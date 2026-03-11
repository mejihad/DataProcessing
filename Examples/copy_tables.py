import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# ─── Init ─────────────────────────────────────────────────────────────────────
# Retrieve job name from Glue runtime arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Register the job with Glue for state tracking
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ─── Config ───────────────────────────────────────────────────────────────────
# S3 root path where Parquet files will be written
S3_OUTPUT = "s3://mon-bucket/parquet-test/"

# Target database that will be created in the Glue Catalog
TARGET_DB = "parquet_db_test"

# List of tables to copy from the source database
# - source_db    : Glue Catalog database where you have SELECT rights
# - source_table : table to read from
# - target_table : name of the table in the test database
# - sample        : number of rows to copy (None = copy all rows)
TABLES = [
    {
        "source_db":    "la_db_avec_select",
        "source_table": "ta_table_1",
        "target_table": "ta_table_1",
        "sample":       500,           # limit to 500 rows for testing
    },
    {
        "source_db":    "la_db_avec_select",
        "source_table": "ta_table_2",
        "target_table": "ta_table_2",
        "sample":       None,          # copy all rows
    },
]

# ─── Copy function ────────────────────────────────────────────────────────────
def copy_table(source_db, source_table, target_table, sample=None):
    full_source = f"`{source_db}`.`{source_table}`"
    output_path = f"{S3_OUTPUT}{target_table}/"

    print(f"\n{'='*60}")
    print(f"[INFO] Copying : {full_source} → {output_path}")

    # Read source table from Glue Catalog
    # Lake Formation checks SELECT permissions at this point
    df = spark.table(full_source)

    # Optionally limit the number of rows for lightweight test datasets
    if sample:
        df = df.limit(sample)
        print(f"[INFO] Sample mode : limited to {sample} rows")

    total_rows = df.count()
    print(f"[INFO] Rows to write : {total_rows} | Columns : {len(df.columns)}")

    # Write data to S3 in Parquet format
    # mode("overwrite") ensures the job is replayable
    df.write \
      .mode("overwrite") \
      .parquet(output_path)

    # Register the table in the Glue Catalog under the test database
    # so it can be queried via Athena or used as source for the conversion job
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {TARGET_DB}")
    spark.sql(f"DROP TABLE IF EXISTS {TARGET_DB}.{target_table}")
    spark.sql(f"""
        CREATE TABLE {TARGET_DB}.{target_table}
        USING parquet
        LOCATION '{output_path}'
    """)

    # Verify row count after write
    count_written = spark.table(f"{TARGET_DB}.{target_table}").count()
    print(f"[OK] {TARGET_DB}.{target_table} created — {count_written} rows written.")

    # Sanity check : make sure we didn't lose any rows during the copy
    if count_written != total_rows:
        raise ValueError(
            f"[ERROR] Row count mismatch : expected {total_rows}, got {count_written}"
        )

# ─── Main ─────────────────────────────────────────────────────────────────────
# Iterate over all tables and copy them one by one
for cfg in TABLES:
    copy_table(**cfg)

# Signal Glue that the job completed successfully
job.commit()
print("\n[DONE] All tables copied successfully.")
