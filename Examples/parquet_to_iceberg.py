import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# ─── Init ──────────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ─── Config ────────────────────────────────────────────────────────────────────
CATALOG          = "glue_catalog"
ICEBERG_WAREHOUSE = "s3://mon-bucket/iceberg-warehouse/"

TABLES = [
    {
        "source_db":      "parquet_db",          # DB Glue Catalog source (Parquet)
        "source_table":   "commandes",
        "target_db":      "iceberg_db",           # DB Glue Catalog cible (Iceberg)
        "target_table":   "commandes",
        "partition_cols": ["annee", "mois"],      # [] si pas de partition
    },
    {
        "source_db":      "parquet_db",
        "source_table":   "clients",
        "target_db":      "iceberg_db",
        "target_table":   "clients",
        "partition_cols": [],
    },
]

# ─── Helpers ───────────────────────────────────────────────────────────────────
def get_iceberg_type(spark_type: str) -> str:
    """Convertit les types Spark en types compatibles Iceberg/DDL."""
    mapping = {
        "integer":   "int",
        "long":      "bigint",
        "short":     "smallint",
        "byte":      "tinyint",
        "double":    "double",
        "float":     "float",
        "boolean":   "boolean",
        "string":    "string",
        "binary":    "binary",
        "date":      "date",
        "timestamp": "timestamp",
    }
    return mapping.get(spark_type, spark_type)  # fallback: on garde le type tel quel

def build_cols_ddl(schema) -> str:
    parts = []
    for field in schema.fields:
        col_type = get_iceberg_type(field.dataType.simpleString())
        nullable = "" if field.nullable else " NOT NULL"
        parts.append(f"`{field.name}` {col_type}{nullable}")
    return ",\n  ".join(parts)

# ─── Conversion ────────────────────────────────────────────────────────────────
def convert_table(source_db, source_table, target_db, target_table, partition_cols):
    full_source = f"`{source_db}`.`{source_table}`"
    full_target = f"{CATALOG}.{target_db}.{target_table}"

    print(f"\n{'='*60}")
    print(f"[INFO] Conversion : {full_source}  →  {full_target}")

    # 1. Lecture depuis le Glue Catalog (Lake Formation gère l'accès)
    df = spark.table(full_source)
    print(f"[INFO] Lignes lues : {df.count()} | Colonnes : {len(df.columns)}")
    df.printSchema()

    # 2. Création de la base cible si elle n'existe pas
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{target_db}")

    # 3. Drop + recréation de la table Iceberg (migration initiale)
    spark.sql(f"DROP TABLE IF EXISTS {full_target}")

    # 4. DDL Iceberg
    cols_ddl = build_cols_ddl(df.schema)
    partition_clause = (
        f"PARTITIONED BY ({', '.join(partition_cols)})"
        if partition_cols else ""
    )

    create_sql = f"""
        CREATE TABLE {full_target} (
          {cols_ddl}
        )
        USING iceberg
        {partition_clause}
        LOCATION '{ICEBERG_WAREHOUSE}{target_db}/{target_table}/'
        TBLPROPERTIES (
          'table_type'       = 'ICEBERG',
          'format-version'   = '2',
          'write.format.default'              = 'parquet',
          'write.parquet.compression-codec'   = 'snappy',
          'write.metadata.delete-after-commit.enabled' = 'true',
          'write.metadata.previous-versions-max'       = '5'
        )
    """
    print(f"[INFO] Création table Iceberg...")
    spark.sql(create_sql)

    # 5. Écriture des données
    print(f"[INFO] Écriture en cours...")
    df.writeTo(full_target).append()

    # 6. Vérification
    count_iceberg = spark.table(full_target).count()
    print(f"[OK] {full_target} — {count_iceberg} lignes écrites.")

    # Optionnel : vérifier la cohérence
    count_source = df.count()
    if count_iceberg != count_source:
        raise ValueError(
            f"[ERREUR] Incohérence : source={count_source} / iceberg={count_iceberg}"
        )

# ─── Main ──────────────────────────────────────────────────────────────────────
for cfg in TABLES:
    convert_table(**cfg)

job.commit()
print("\n[DONE] Toutes les tables converties avec succès.")
