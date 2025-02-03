import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import boto3
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment Variables
S3_BUCKET = 'datacraft-retail-dataset'
STAGE_PREFIX = 'data/stage/data/'
TARGET_PREFIX = 'data/curated/data/'
TARGET_PATH = f's3://{S3_BUCKET}/{TARGET_PREFIX}/'

# Retail Data Processing Pipeline
"""
Retail Data Processing Pipeline

Processes retail transaction data from S3, performs data cleaning, 
transformation, and loads it into a Delta Lake target.

Handles:
- Reading parquet files from S3
- Data type conversions and null handling
- Deduplication
- Currency conversion & discount calculations
- Aggregation of sales metrics
- Delta Lake merge operations

Dependencies:
    - boto3
    - pyspark
    - delta
    - python-dotenv
"""

# Function Definitions
def load_env_from_s3(bucket_name='datacraft-retail-dataset', env_file_path='config/.env'):
    s3_client = boto3.client('s3')
    local_env_file = '/tmp/.env'  # Temp directory for AWS Lambda & EC2

    try:
        s3_client.download_file(bucket_name, env_file_path, local_env_file)
        load_dotenv(local_env_file)  # Load environment variables
    except Exception as e:
        print(f"Error loading .env file: {e}")

def create_spark_session():
    """
    Creates and configures a Spark session with Delta Lake support.

    This function initializes a SparkSession, enabling Delta Lake extensions 
    and configuring the catalog to support Delta tables. It logs a success 
    message upon creating the session.

    Returns:
        SparkSession: An active Spark session with Delta Lake configurations.
    """
    spark = SparkSession.builder \
        .appName("DeltaLakeApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    logger.info("Delta Lake Spark Session Created Successfully")
    return spark

def list_parquet_files(bucket_name, prefix):
    """
    Lists parquet files from an S3 bucket using a specified prefix.

    Args:
        bucket_name (str): The name of the S3 bucket.
        prefix (str): The prefix path within the S3 bucket to filter parquet files.

    Returns:
        list: A list of S3 URIs for the found parquet files.
    """
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    parquet_files = ["s3://" + bucket_name + "/" + obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.parquet')]
    if parquet_files:
        logger.info(f"Found {len(parquet_files)} parquet files.")
    else:
        logger.warning("No parquet files found.")
    return parquet_files

def read_parquet(files, spark):
    return spark.read.parquet(*files) if isinstance(files, list) else spark.read.parquet(files)

def read_csv(file_path, spark):
    return spark.read.csv(file_path, header=True, inferSchema=True)

def write_to_delta(source, updates, partition_cols):
    """
    Writes a DataFrame to a Delta Lake table in overwrite mode.

    Args:
        source (DataFrame): The DataFrame to write to Delta Lake.
        updates (str): The target path in Delta Lake where the data will be saved.
        partition_cols (list): List of column names to partition the data by.

    This function uses Delta Lake format, overwriting any existing data
    in the specified path, and partitions the data based on the given columns.
    """
    source.write.format("delta").mode("overwrite").partitionBy(*partition_cols).save(updates)

def update_delta(spark, updates, target, partition_cols, merge_condition):
    if DeltaTable.isDeltaTable(spark, target):
        deltaTable = DeltaTable.forPath(spark, target)
        deltaTable.alias("target") \
            .merge(updates.alias("updates"), merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        logger.info("Data merged successfully.")
    else:
        write_to_delta(updates, target, partition_cols)
        logger.info("Data written to Delta Lake.")

def type_conversion(df):
    """
    Rename columns and convert data types in the DataFrame.
    
    Parameters:
    df (DataFrame): Input DataFrame containing raw transactional data.
    
    Returns:
    DataFrame: Transformed DataFrame with appropriate data types and renamed columns.
    """
    df.createOrReplaceTempView("df_stage")
    
    df_clean = spark.sql("""
        SELECT 
            CAST(TRIM(RetailTxnData_InvoiceNo) AS LONG) AS invoiceno,
            CAST(TRIM(RetailTxnData_InvoiceDateTime) AS TIMESTAMP) AS invoicets,
            DATE(CAST(TRIM(RetailTxnData_InvoiceDateTime) AS TIMESTAMP)) AS invoicedate,
            YEAR(CAST(TRIM(RetailTxnData_InvoiceDateTime) AS TIMESTAMP)) AS invoice_year,
            MONTH(CAST(TRIM(RetailTxnData_InvoiceDateTime) AS TIMESTAMP)) AS invoice_month,
            CAST(TRIM(RetailTxnData_StoreNo) AS STRING) AS storeno,
            CAST(TRIM(RetailTxnData_StorePos) AS STRING) AS storepos,
            CAST(TRIM(RetailTxnData_StoreCountry) AS STRING) AS storecountry,
            CAST(TRIM(RetailTxnData_LineItem_StockCode) AS STRING) AS lineitem_stockcode,
            CAST(TRIM(RetailTxnData_LineItem_Description) AS STRING) AS lineitem_description,
            CAST(TRIM(RetailTxnData_LineItem_Quantity) AS DOUBLE) AS lineitem_quantity,
            CAST(TRIM(REPLACE(RetailTxnData_LineItem_UnitPrice, '$', '')) AS FLOAT) AS lineitem_unitprice,
            CAST(TRIM(REPLACE(RetailTxnData_LineItem_SalePrice, '$', '')) AS FLOAT) AS lineitem_saleprice,
            CAST(TRIM(RetailTxnData_Customer_CustomerID) AS LONG) AS customer_customerid,
            CAST(TRIM(RetailTxnData_Customer_EmailId) AS STRING) AS customer_emailid,
            CAST(TRIM(RetailTxnData_Customer_ContactNo) AS STRING) AS customer_contactno,
            CAST(TRIM(RetailTxnData_Customer_FirstName) AS STRING) AS customer_firstname,
            CAST(TRIM(RetailTxnData_Customer_MiddleName) AS STRING) AS customer_middlename,
            CAST(TRIM(RetailTxnData_Customer_LastName) AS STRING) AS customer_lastname,
            CAST(TRIM(RetailTxnData_Customer_Address) AS STRING) AS customer_address,
            CAST(TRIM(RetailTxnData_Customer_State) AS STRING) AS customer_state,
            CAST(TRIM(RetailTxnData_Customer_Country) AS STRING) AS customer_country,
            CAST(TRIM(RetailTxnData_Customer_ZipCode) AS STRING) AS customer_zipcode,
            CAST(TRIM(RetailTxnData_Discount_DiscountCode) AS STRING) AS discountcode,
            CAST(TRIM(RetailTxnData_Region) AS STRING) AS region,
            CAST(TRIM(RetailTxnData_SourceTimestamp) AS TIMESTAMP) AS sourcets
        FROM df_stage
    """)
    return df_clean

def null_handling(df):
    """
    Handle null values by filling default values for different column types.
    
    Parameters:
    df (DataFrame): Input DataFrame with potential null values.
    
    Returns:
    DataFrame: Cleaned DataFrame with nulls handled.
    """
    fill_numeric_cols = {"invoiceno": -1, "customer_customerid": -1}
    fill_floating_cols = {"lineitem_quantity": "NaN", "lineitem_unitprice": "NaN", "lineitem_saleprice": "NaN"}
    fill_timestamp_cols = {"invoicets": "1900-01-01 00:00:00", "sourcets": "1900-01-01 00:00:00"}
    
    df_clean = df.na.fill("NaN").na.fill(fill_numeric_cols).na.fill(fill_floating_cols).na.fill(fill_timestamp_cols)
    
    df_transformed = df_clean \
        .withColumn('processing_delay', datediff(current_timestamp(), col('sourcets').cast('timestamp'))) \
        .withColumn('customer_status', expr("CASE WHEN customer_customerid IS NULL OR customer_customerid <> '' THEN FALSE ELSE TRUE END")) \
        .withColumn('local_stockcode', substring(col('lineitem_stockcode'), 0, 3)) \
        .withColumn('standard_stockcode', lpad(col('lineitem_stockcode'), 10, '0'))
    
    return df_transformed

def deduplication(df):
    """
    Remove duplicate rows based on region, storepos, storeno, invoicets, and invoiceno.
    
    Parameters:
    df (DataFrame): Input DataFrame with potential duplicates.
    
    Returns:
    DataFrame: De-duplicated DataFrame.
    """
    window_dd = Window.partitionBy('region', 'storepos', 'storeno', 'invoicets', 'invoiceno').orderBy(desc('sourcets'))
    df_deduplicated = df.withColumn('rownumber', row_number().over(window_dd))
    return df_deduplicated

def data_manipulation(df):
    """
    Perform data manipulations by joining with reference datasets.
    
    Parameters:
    df (DataFrame): Input DataFrame with transactions.
    
    Returns:
    DataFrame: Transformed DataFrame with additional columns.
    """
    currency_convert = read_csv('s3://datacraft-retail-dataset/data/source/dim/dim_currency_convert.csv', spark)
    currency = read_csv('s3://datacraft-retail-dataset/data/source/dim/dim_currency.csv', spark)
    discount_code = read_csv('s3://datacraft-retail-dataset/data/source/dim/dim_discount.csv', spark)
    
    currency_convert.createOrReplaceTempView("conversion")
    currency.createOrReplaceTempView("currency")
    discount_code.createOrReplaceTempView("discount")

    df.createOrReplaceTempView("df_dedup")
    df_final = spark.sql("""
        SELECT a.*, 
            COALESCE(b.currency_cd, "USD") AS currency_code, 
            COALESCE(c.conversion_rate, 1) AS conversion_rate, 
            COALESCE(d.discount_per, 5)/100 AS discount_value
        FROM df_dedup a
        LEFT JOIN currency b ON LOWER(a.storecountry) = LOWER(TRIM(b.country))
        LEFT JOIN conversion c ON LOWER(TRIM(b.currency_cd)) = LOWER(TRIM(c.source_currency))
        LEFT JOIN discount d ON LOWER(a.discountcode) = LOWER(TRIM(d.discount_cd))
    """)
    df_final = df_final.withColumn('lineitem_finalsaleprice', expr('ROUND((lineitem_saleprice - (lineitem_saleprice * discount_value)) * conversion_rate, 2)'))
    return df_final

def aggregation(df):
    """
    Aggregate data for dataset summaries.
    
    Parameters:
    df (DataFrame): Input DataFrame containing transactional data.
    
    Returns:
    DataFrame: Aggregated DataFrame with summary statistics.
    """
    df.createOrReplaceTempView("txn_dataset")
    df_agg = spark.sql("""
        SELECT region, storeno, invoice_year, invoice_month,
            COUNT(DISTINCT invoiceno) AS num_invoices,
            COUNT(DISTINCT lineitem_stockcode) AS stock_count,
            SUM(lineitem_quantity) AS total_quantity,
            SUM(lineitem_finalsaleprice) AS total_sales
        FROM txn_dataset
        GROUP BY region, storeno, invoice_year, invoice_month
    """)
    return df_agg

# Main Function

def main():
    """
    Executes the end-to-end data processing workflow for retail transactions.

    Workflow:
    1. Lists parquet files from the specified S3 staging path.
    2. Reads the parquet files into a Spark DataFrame.
    3. Performs data cleaning and type conversions.
    4. Handles null values and adds derived columns.
    5. Deduplicates records based on specific criteria.
    6. Enriches data through joins with dimension tables.
    7. Aggregates sales metrics.
    8. Analyzes data trends using lead/lag functions.
    9. Merges the processed data into a Delta Lake target table.

    Logs progress and errors throughout the workflow.
    """
    # import environment variables
    # load_env_from_s3('datacraft-retail-dataset', 'config/.env')

    parquet_files = list_parquet_files(S3_BUCKET, STAGE_PREFIX)
    if not parquet_files:
        logger.error("No parquet files found. Exiting.")
        return

    df_stage = read_parquet(parquet_files, spark)
    df_clean = type_conversion(df_stage)
    df_transformed = null_handling(df_clean)
    df_deduplicated = deduplication(df_transformed)
    txn_dataset = data_manipulation(df_deduplicated)
    agg_df = aggregation(txn_dataset)
    agg_df.show(10)

    merge_condition = "target.invoiceno=updates.invoiceno and target.storeno=updates.storeno and target.lineitem_stockcode=updates.lineitem_stockcode and target.invoicedate=updates.invoicedate and target.region=updates.region"
    partition_cols = ['region', 'invoice_year']

    update_delta(spark, txn_dataset, TARGET_PATH, partition_cols, merge_condition)

if __name__ == "__main__":
    spark = create_spark_session()
    main()
