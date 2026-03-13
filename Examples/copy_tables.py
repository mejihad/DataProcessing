import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# ─── Init ─────────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ─── Fix Lake Formation : avoid accessing "default" catalog ───────────────────
spark.conf.set("spark.sql.warehouse.dir", "s3://dev1-bas-gpsw01-518893644482-eu-west-1/parquet-test/")

# ─── Config ───────────────────────────────────────────────────────────────────
# Replace with your actual S3 bucket
S3_OUTPUT = "s3://dev1-bas-gpsw01-518893644482-eu-west-1/parquet-test/"

# Target database in Glue Catalog
TARGET_DB = "parquet_db_test"

# Replace source_db and source_table with your actual values from e_bcbs_db_dev
TABLES = [
    {
        "source_db":    "e_bcbs_db_dev",   # ← ta vraie DB Glue
        "source_table": "ta_table_1",      # ← ton vrai nom de table
        "target_table": "ta_table_1",
        "sample":       500,
    },
]

# ─── Copy function ────────────────────────────────────────────────────────────
def copy_table(source_db, source_table, target_table, sample=None):
    full_source = f"`{source_db}`.`{source_table}`"
    output_path = f"{S3_OUTPUT}{target_table}/"

    print(f"\n{'='*60}")
    print(f"[INFO] Copying : {full_source} -> {output_path}")

    # Read source table — Lake Formation checks SELECT rights here
    df = spark.table(full_source)

    # Limit rows for lightweight test dataset
    if sample:
        df = df.limit(sample)
        print(f"[INFO] Sample mode : limited to {sample} rows")

    total_rows = df.count()
    print(f"[INFO] Rows to write : {total_rows} | Columns : {len(df.columns)}")

    # Write to S3 as Parquet
    df.write \
      .mode("overwrite") \
      .parquet(output_path)

    # Register table in Glue Catalog using glueContext to bypass
    # Lake Formation restrictions on the "default" database
    glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"path": output_path},
        format="parquet"
    )

    spark.sql(f"""
        CREATE DATABASE IF NOT EXISTS {TARGET_DB}
        LOCATION '{S3_OUTPUT}'
    """)

    spark.sql(f"DROP TABLE IF EXISTS {TARGET_DB}.{target_table}")

    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {TARGET_DB}.{target_table}
        STORED AS PARQUET
        LOCATION '{output_path}'
    """)

    # Verify row count
    count_written = spark.table(f"{TARGET_DB}.{target_table}").count()
    print(f"[OK] {TARGET_DB}.{target_table} created — {count_written} rows written.")

    if count_written != total_rows:
        raise ValueError(
            f"[ERROR] Row count mismatch : expected {total_rows}, got {count_written}"
        )

# ─── Main ─────────────────────────────────────────────────────────────────────
for cfg in TABLES:
    copy_table(**cfg)

job.commit()
print("\n[DONE] All tables copied successfully.")
