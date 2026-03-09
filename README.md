# DataProcessing


# Job Glue — Conversion Parquet → Iceberg

Explication détaillée du script PySpark de migration de tables Parquet vers Apache Iceberg via AWS Glue 4.0.

-----

## Table des matières

- [1. Les imports](#1-les-imports)
- [2. L’initialisation](#2-linitialisation)
- [3. La configuration](#3-la-configuration)
- [4. Les fonctions helpers](#4-les-fonctions-helpers)
- [5. La fonction principale `convert_table`](#5-la-fonction-principale-convert_table)
- [6. Le main](#6-le-main)
- [Résumé du flux](#résumé-du-flux)

-----

## 1. Les imports

```python
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
```

`sys` permet de lire les arguments passés au job (son nom, ses paramètres). Les 4 imports AWS/Spark sont les briques de base de tout job Glue : elles fournissent le contexte d’exécution Spark et les utilitaires spécifiques à Glue.

-----

## 2. L’initialisation

```python
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
```

C’est le **boilerplate obligatoire** de tout job Glue, à copier tel quel. Voici ce que fait chaque ligne :

|Ligne               |Rôle                                                                                  |
|--------------------|--------------------------------------------------------------------------------------|
|`getResolvedOptions`|Récupère le nom du job depuis les arguments AWS (passé automatiquement par Glue)      |
|`SparkContext`      |Démarre le moteur Spark distribué sur le cluster                                      |
|`GlueContext`       |Enveloppe Spark avec les fonctionnalités Glue (accès au Catalog, Lake Formation, etc.)|
|`spark_session`     |Donne accès à l’API SQL/DataFrame de Spark                                            |
|`job.init`          |Enregistre le job auprès de Glue pour activer le suivi d’état et les bookmarks        |

-----

## 3. La configuration

```python
CATALOG           = "glue_catalog"
ICEBERG_WAREHOUSE = "s3://mon-bucket/iceberg-warehouse/"

TABLES = [
    {
        "source_db":      "parquet_db",
        "source_table":   "commandes",
        "target_db":      "iceberg_db",
        "target_table":   "commandes",
        "partition_cols": ["annee", "mois"],
    },
    {
        "source_db":      "parquet_db",
        "source_table":   "clients",
        "target_db":      "iceberg_db",
        "target_table":   "clients",
        "partition_cols": [],
    },
]
```

- **`CATALOG`** — alias du catalogue Spark configuré dans les `--conf` du job. C’est un nom arbitraire qui pointe vers le Glue Data Catalog AWS.
- **`ICEBERG_WAREHOUSE`** — dossier S3 racine où seront écrits les fichiers Iceberg.
- **`TABLES`** — liste de toutes les tables à migrer. C’est **le seul endroit à modifier** pour ajouter ou retirer des tables.

-----

## 4. Les fonctions helpers

### `get_iceberg_type`

```python
def get_iceberg_type(spark_type: str) -> str:
    mapping = {
        "integer": "int",
        "long":    "bigint",
        "short":   "smallint",
        "byte":    "tinyint",
        ...
    }
    return mapping.get(spark_type, spark_type)
```

Spark et Iceberg n’utilisent pas exactement les mêmes noms de types. Par exemple, Spark appelle `integer` ce qu’Iceberg/SQL appelle `int`. Cette fonction fait la traduction. Si un type n’est pas dans le dictionnaire (ex : `decimal(10,2)`), il est gardé tel quel grâce au fallback `spark_type`.

### `build_cols_ddl`

```python
def build_cols_ddl(schema) -> str:
    parts = []
    for field in schema.fields:
        col_type = get_iceberg_type(field.dataType.simpleString())
        nullable = "" if field.nullable else " NOT NULL"
        parts.append(f"`{field.name}` {col_type}{nullable}")
    return ",\n  ".join(parts)
```

Parcourt chaque colonne du DataFrame source et construit la liste des colonnes pour le `CREATE TABLE`. Par exemple, pour une table avec `id (long)` et `nom (string)`, elle produira :

```sql
`id` bigint NOT NULL,
`nom` string
```

-----

## 5. La fonction principale `convert_table`

C’est le cœur du script. Elle effectue **6 opérations** dans l’ordre :

### ① Lecture de la table source

```python
df = spark.table(full_source)
```

Charge la table Parquet depuis le Glue Catalog. Lake Formation vérifie les droits `SELECT` à ce moment. Rien n’est encore chargé en mémoire — Spark est *lazy*, il ne lit réellement les données qu’au moment où c’est nécessaire.

### ② Création de la base cible

```python
spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{target_db}")
```

Crée la base de données Iceberg dans le Glue Catalog si elle n’existe pas encore. Le `IF NOT EXISTS` rend l’opération idempotente (pas d’erreur si elle existe déjà).

### ③ Suppression de l’ancienne table

```python
spark.sql(f"DROP TABLE IF EXISTS {full_target}")
```

Supprime la table Iceberg cible si elle existe déjà. C’est ce qui rend le script **rejouable** : on repart toujours d’une table propre.

> ⚠️ À retirer si tu veux faire des migrations incrémentales plutôt que repartir de zéro.

### ④ Création de la table Iceberg

```python
spark.sql(create_sql)
```

Exécute le `CREATE TABLE ... USING iceberg` avec le schéma construit par `build_cols_ddl`. Détail des `TBLPROPERTIES` :

|Propriété                                          |Effet                                                            |
|---------------------------------------------------|-----------------------------------------------------------------|
|`format-version = 2`                               |Active les fonctionnalités Iceberg v2 (row-level deletes, MERGE) |
|`write.format.default = parquet`                   |Les données sont stockées en Parquet                             |
|`write.parquet.compression-codec = snappy`         |Compression Snappy (bon compromis vitesse/taille)                |
|`write.metadata.delete-after-commit.enabled = true`|Nettoie les anciens fichiers de métadonnées après chaque commit  |
|`write.metadata.previous-versions-max = 5`         |Garde au maximum 5 versions de métadonnées (évite l’accumulation)|

### ⑤ Écriture des données

```python
df.writeTo(full_target).append()
```

Écrit les données du DataFrame dans la table Iceberg. C’est ici que Spark déclenche réellement la lecture du Parquet source et l’écriture sur S3. `.append()` ajoute les données sans écraser ce qui existe (cohérent avec le `DROP` fait juste avant).

### ⑥ Vérification de cohérence

```python
count_iceberg = spark.table(full_target).count()
count_source  = df.count()
if count_iceberg != count_source:
    raise ValueError(...)
```

Compare le nombre de lignes source et cible. Si elles diffèrent, le job lève une erreur et s’arrête — ce qui fait échouer le run Glue et déclenche une alerte. C’est un filet de sécurité basique pour détecter une migration incomplète.

-----

## 6. Le main

```python
for cfg in TABLES:
    convert_table(**cfg)

job.commit()
```

Boucle sur chaque table définie dans `TABLES` et appelle `convert_table` avec les paramètres du dictionnaire (`**cfg` déplie le dict en arguments nommés).

`job.commit()` signale à Glue que le job s’est terminé avec succès — sans cette ligne, Glue considère le job comme échoué même si tout s’est bien passé.

-----

## Résumé du flux

```
Pour chaque table dans TABLES :
  │
  ├── 1. Lire la table Parquet  (Glue Catalog → S3)
  ├── 2. Créer la DB cible      (si absente)
  ├── 3. Supprimer l'ancienne   (rejouabilité)
  ├── 4. Créer la table Iceberg (DDL généré automatiquement)
  ├── 5. Écrire les données     (S3 → fichiers Iceberg)
  └── 6. Vérifier le count      (source == cible ?)

job.commit()  →  Glue marque le job comme "Succeeded"
```