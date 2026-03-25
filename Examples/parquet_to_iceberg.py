import sys
import traceback
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from EDP_Iceberg_Ingestion.EDP_Iceberg_Ingestion import EDPIcebergIngestor

def log_to_s3(message, bucket="dev1-bas-gpsw01-518893644482-eu-west-1", key="glue-logs/account_bal_detail_bert.log"):
    s3 = boto3.client('s3')
    try:
        existing = s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8')
    except:
        existing = ""
    s3.put_object(Bucket=bucket, Key=key, Body=(existing + message + "\n").encode('utf-8'))

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
spark = SparkSession.builder.appName("GlueIcebergTableJob").getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    log_to_s3("=== EXTRACTION START ===")
    base_df = spark.sql("SELECT * FROM e_bcbs_db_dev.account_bal_detail")
    count = base_df.count()
    log_to_s3(f"=== EXTRACTION OK - count: {count} ===")

    if count > 0:
        log_to_s3("=== STARTING INGESTION ===")
        EDPIcebergIngestor.ingestinto_(
            catalog_name="AwsDataCatalog",
            Warehouse_Path="s3://dev1-bas-gpsw01-518893644482-eu-west-1/iceberg-warehouse/e_bcbs_db_dev/account_bal_detail_bert/",
            account_id="518893644482",
            database_name="e_bcbs_db_dev",
            table_name="account_bal_detail_bert",
            write_df_name=base_df,
            write_df_mode="append",
            partition_columns=["dw_bus_dt"],
        )
        log_to_s3("=== INGESTION DONE ===")
    else:
        log_to_s3("=== No Data to Show ===")

except Exception as e:
    log_to_s3(f"=== ERROR: {str(e)}\n{traceback.format_exc()}")
    raise

job.commit()
