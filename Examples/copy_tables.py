import sys
import boto3
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

# ─── Config ───────────────────────────────────────────────────────────────────
# Replace with your actual S3 bucket and path
S3_OUTPUT = "s3://your-bucket/parquet-test/"

# AWS region
REGION = "eu-west-1"

# Target database that will be created in the Glue Catalog
TARGET_DB = "your_target_db"

# Replace source_db and source_table with your actual values
TABLES = [
    {
        "source_db":    "your_source_db",    # ← your Glue source database
        "source_table": "your_source_table", # ← your source table name
        "target_table": "your_target_table",
        "sample":       500,
    },
]

# ─── Glue Catalog helper ──────────────────────────────────────────────────────
def create_glue_table(target_table, output_path, schema):
    """
    Registers the table directly in the Glue Data Catalog via boto3.
    This bypasses Spark SQL and avoids Lake Formation issues on the default DB.
    """
    glue_client = boto3.client("glue", region_name=REGION)

    # Convert Spark schema to Glue column format
    columns = [
        {"Name": field.name, "Type": field.dataType.simpleString()}
        for field in schema.fields
    ]

    # Create target database if it doesn't exist
    try:
        glue_client.create_database(
            DatabaseInput={
                "Name": TARGET_DB,
                "Description": "Test database for Parquet to Iceberg migration",
            }
        )
        print(f"[INFO] Database '{TARGET_DB}' created.")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"[INFO] Database '{TARGET_DB}' already exists, skipping creation.")

    # Drop table if it already exists to ensure clean state
    try:
        glue_client.delete_table(DatabaseName=TARGET_DB, Name=target_table)
        print(f"[INFO] Existing table '{target_table}' dropped.")
    except glue_client.exceptions.EntityNotFoundException:
        print(f"[INFO] Table '{target_table}' does not exist yet, skipping drop.")

    # Register the table in the Glue Catalog
    glue_client.create_table(
        DatabaseName=TARGET_DB,
        TableInput={
            "Name": target_table,
            "Description": f"Parquet test copy of {target_table}",
            "TableType": "EXTERNAL_TABLE",
            "StorageDescriptor": {
                "Columns": columns,
                "Location": output_path,
                "InputFormat":  "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    "Parameters": {"serialization.format": "1"},
                },
            },
            "Parameters": {"classification": "parquet"},
        }
    )
    print(f"[OK] Table '{TARGET_DB}.{target_table}' registered in Glue Catalog.")

# ─── Copy function ────────────────────────────────────────────────────────────
def copy_table(source_db, source_table, target_table, sample=None):
    full_source = f"`{source_db}`.`{source_table}`"
    output_path = f"{S3_OUTPUT}{target_table}/"

    print(f"\n{'='*60}")
    print(f"[INFO] Copying : {full_source} -> {output_path}")

    # Read source table from Glue Catalog
    # Lake Formation checks SELECT permissions at this point
    df = spark.table(full_source)

    # Limit rows for lightweight test dataset
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

    print(f"[INFO] Parquet files written to {output_path}")

    # Register table in Glue Catalog via boto3
    create_glue_table(target_table, output_path, df.schema)

    # Verify row count by reading back from S3
    count_written = spark.read.parquet(output_path).count()
    print(f"[OK] {target_table} — {count_written} rows written.")

    # Sanity check : make sure no rows were lost during the copy
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
