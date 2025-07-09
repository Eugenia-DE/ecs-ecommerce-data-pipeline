import os
import sys
import json
from decimal import Decimal
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, sum as _sum, avg, countDistinct, count, when, to_date, lit
    )
    from pyspark.sql.types import DoubleType, IntegerType
except ImportError:
    print("[ERROR] PySpark libraries not found. This script requires PySpark to run.")
    sys.exit(1)

# Global list to store log messages for S3 upload
script_logs = []

# S3 client
s3_client = boto3.client("s3")
# DynamoDB resource for writing KPIs
dynamodb_resource = boto3.resource("dynamodb")

# --- Logging Utilities ---
def log_message(level, message):
    """
    Logs a message to both stdout (for CloudWatch) and a global list (for S3 upload).
    """
    timestamp = datetime.utcnow().isoformat(timespec='milliseconds') + 'Z'
    formatted_message = f"{timestamp} [{level}] {message}"
    print(formatted_message) 
    script_logs.append(formatted_message) 

def write_logs_to_s3(bucket, task_type):
    """
    Writes all collected script logs to a file in the S3 logs folder.
    Args:
        bucket (str): The name of the S3 bucket.
        task_type (str): The type of task.
    """
    log_content = "\n".join(script_logs)
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
    log_key = f"logs/{task_type}/{task_type}_{timestamp}.log"
    try:
        s3_client.put_object(Bucket=bucket, Key=log_key, Body=log_content)
        log_message("INFO", f"Successfully uploaded logs to s3://{bucket}/{log_key}")
    except Exception as e:
        log_message("ERROR", f"Failed to upload logs to s3://{bucket}/{log_key}: {e}")

# --- S3 File Management Utilities ---
def move_file(bucket, key, destination_prefix):
    """
    Moves a file from its current S3 key to a new S3 key under the specified
    destination_prefix. Handles cases where the source file might no longer exist.
    """
    source_key = key
    try:
        # Check if the source object exists before attempting to copy.
        s3_client.head_object(Bucket=bucket, Key=source_key)

        # This function is called after validation, so keys are in 'validated/'
        parts = source_key.split('/')
        if parts[0] == 'validated':
            relative_path = '/'.join(parts[1:])
        else:
            relative_path = source_key 

        target_key = os.path.join(destination_prefix, relative_path)

        # Copy the object to the new location
        s3_client.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': source_key}, Key=target_key)
        # Delete the original object from the source location
        s3_client.delete_object(Bucket=bucket, Key=source_key)
        log_message("INFO", f"Moved file s3://{bucket}/{source_key} -> s3://{bucket}/{target_key}")

    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            log_message("INFO", f"File s3://{bucket}/{source_key} not found. It might have already been moved or does not exist.")
        else:
            log_message("ERROR", f"AWS Client Error moving file {source_key} to {destination_prefix}: {e}")
            raise 
    except Exception as e:
        log_message("ERROR", f"Failed to move file {source_key} to {destination_prefix}: {e}")
        raise 

# --- Spark Utilities ---
def initialize_spark_session():
    """
    Initializes and returns a SparkSession configured for S3a access.
    """
    log_message("INFO", "Initializing SparkSession...")
    spark = SparkSession.builder \
        .appName("KPITransformation") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()
    log_message("INFO", "SparkSession initialized successfully.")
    return spark

def get_s3_paths_for_spark_read(bucket, keys_list, source_prefix, target_prefix):
    """
    Converts a list of S3 keys (from environment variables) into s3a paths
    suitable for Spark, replacing the source prefix with the target prefix.
    """
    s3a_paths = []
    for key in keys_list:
        # Ensure the key starts with the source_prefix before replacing
        if key.startswith(source_prefix):
            processed_key = key.replace(source_prefix, target_prefix)
        else:
            if not key.startswith(target_prefix):
                processed_key = os.path.join(target_prefix, os.path.basename(key))
            else:
                processed_key = key
        s3a_paths.append(f"s3a://{bucket}/{processed_key}")
    return s3a_paths

def load_data_from_s3_to_spark(spark, bucket, s3a_paths, file_type):
    """
    Loads one or more CSV files from S3 into a Spark DataFrame.
    """
    if not s3a_paths:
        log_message("ERROR", f"No {file_type} files found for loading. Paths list is empty.")
        sys.exit(1) # Critical error if no files to process

    log_message("INFO", f"Loading {file_type} data from: {s3a_paths}")
    try:
        df = spark.read.option("header", "true").csv(s3a_paths)
        log_message("INFO", f"Successfully loaded {file_type} data.")
        return df
    except Exception as e:
        log_message("ERROR", f"Failed to load {file_type} data from S3: {e}")
        sys.exit(1) # Exit on critical loading failure

def get_all_validated_files_in_prefix(bucket, prefix):
    """
    Lists all CSV files under a given S3 prefix in the 'validated/' folder.
    """
    log_message("INFO", f"Listing all validated files under prefix: s3://{bucket}/{prefix}")
    all_paths = []
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    
    for page in pages:
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.csv'):
                all_paths.append(f"s3a://{bucket}/{obj['Key']}")
    
    if not all_paths:
        log_message("WARN", f"No CSV files found under s3://{bucket}/{prefix}")
    else:
        log_message("INFO", f"Found {len(all_paths)} validated files under s3://{bucket}/{prefix}")
    return all_paths

# --- Data Preparation and KPI Computation ---
def prepare_data_for_kpis(df_products, df_orders, df_items):
    """
    Prepares the raw Spark DataFrames by renaming columns, casting types,
    and joining them to create a consolidated DataFrame suitable for KPI computation.
    """
    log_message("INFO", "Preparing data for KPI computation (renaming, casting, joining)...")

    # Rename product_id for join consistency
    df_products_renamed = df_products.withColumnRenamed("id", "product_id_product_table")

    # Cast sale_price to DoubleType in df_items
    df_items_casted = df_items.withColumn("sale_price", col("sale_price").cast(DoubleType()))

    # Add order_date to orders DataFrame
    df_orders_dated = df_orders.withColumn("order_date", to_date("created_at"))

    # Handle 'returned_at' columns which might be missing or need renaming
    # For orders: 'returned_at' in orders table
    if "returned_at" in df_orders.columns:
        df_orders_dated = df_orders_dated.withColumnRenamed("returned_at", "returned_at_order")
    else:
        df_orders_dated = df_orders_dated.withColumn("returned_at_order", lit(None).cast(DoubleType())) 

    # For order_items: 'returned_at' in order_items table
    if "returned_at" in df_items.columns:
        df_items_casted = df_items_casted.withColumnRenamed("returned_at", "returned_at_item")
    else:
        df_items_casted = df_items_casted.withColumn("returned_at_item", lit(None).cast(DoubleType())) 

    # Join order_items with products for category information
    df_items_with_category = df_items_casted.join(
        df_products_renamed.select("product_id_product_table", "category"),
        col("product_id") == col("product_id_product_table"),
        how="left"
    ).drop("product_id_product_table") # Drop the renamed column after join

    # Join order_items with orders for order_date, user_id, and order-level return status
    df_consolidated = df_items_with_category.join(
        df_orders_dated.select("order_id", "order_date", "user_id", "returned_at_order"),
        on="order_id",
        how="left"
    )

    # Add return flags
    df_consolidated = df_consolidated.withColumn(
        "returned_item_flag", when(col("returned_at_item").isNotNull(), 1).otherwise(0).cast(IntegerType())
    )
    df_consolidated = df_consolidated.withColumn(
        "returned_order_flag", when(col("returned_at_order").isNotNull(), 1).otherwise(0).cast(IntegerType())
    )

    log_message("INFO", "Data preparation complete. Consolidated DataFrame created.")
    return df_consolidated

def compute_category_kpis(df_consolidated, target_dates=None):
    """
    Computes category-level KPIs from the consolidated DataFrame.
    """
    log_message("INFO", "Computing Category-Level KPIs...")
    df_filtered = df_consolidated
    if target_dates:
        df_filtered = df_consolidated.filter(col("order_date").isin(target_dates))
        log_message("INFO", f"Filtering category KPI computation for dates: {target_dates}")

    category_kpi = df_filtered.groupBy("category", "order_date").agg(
        _sum("sale_price").alias("daily_revenue"),
        avg("sale_price").alias("avg_order_value"), 
        avg("returned_item_flag").alias("avg_return_rate")
    )
    log_message("INFO", "Category-Level KPIs computed.")
    return category_kpi

def compute_order_kpis(df_consolidated, df_orders_full, target_dates=None):
    """
    Computes order-level KPIs from the consolidated DataFrame and full orders DataFrame.
    """
    log_message("INFO", "Computing Order-Level KPIs...")
    
    # Filter orders data by target dates if specified
    df_orders_filtered = df_orders_full
    if target_dates:
        df_orders_filtered = df_orders_full.filter(to_date(col("created_at")).isin(target_dates))
        log_message("INFO", f"Filtering order KPI computation for dates: {target_dates}")

    # Calculate total revenue per order (from order_items)
    total_revenue_per_order = df_consolidated.groupBy("order_id").agg(_sum("sale_price").alias("order_revenue"))

    # Join total revenue back to the filtered orders DataFrame
    df_orders_with_revenue = df_orders_filtered.join(total_revenue_per_order, on="order_id", how="left")
    
    # Ensure order_revenue is not null for aggregation, default to 0
    df_orders_with_revenue = df_orders_with_revenue.withColumn("order_revenue", col("order_revenue").cast(DoubleType()).alias("order_revenue"))
    df_orders_with_revenue = df_orders_with_revenue.fillna(0, subset=["order_revenue"])

    # Calculate total items sold per order_date from df_consolidated (items data)
    total_items_sold_per_date = df_consolidated.groupBy(to_date("created_at").alias("order_date")).agg(
        count("order_id").alias("total_items_sold_daily") # count of items
    )

    order_kpi = df_orders_with_revenue.groupBy(to_date("created_at").alias("date_key")).agg(
        countDistinct("order_id").alias("total_orders"),
        _sum("order_revenue").alias("total_revenue"),
        avg("returned_order_flag").alias("return_rate"),
        countDistinct("user_id").alias("unique_customers")
    )
    
    # Join total_items_sold_daily from the items DataFrame to the order_kpi
    order_kpi = order_kpi.join(total_items_sold_per_date, on="date_key", how="left")
    
    # Rename total_items_sold_daily to total_items_sold and ensure it's integer
    order_kpi = order_kpi.withColumnRenamed("total_items_sold_daily", "total_items_sold")
    order_kpi = order_kpi.withColumn("total_items_sold", col("total_items_sold").cast(IntegerType()))
    order_kpi = order_kpi.fillna(0, subset=["total_items_sold"]) # Fill nulls if no items for a date

    log_message("INFO", "Order-Level KPIs computed.")
    return order_kpi

# --- DynamoDB Write Operations ---
def write_category_kpis_to_dynamodb(category_kpi_df):
    """
    Writes category-level KPIs from a Spark DataFrame to the DynamoDB CategoryKPIs table.

    Args:
        category_kpi_df (pyspark.sql.DataFrame): DataFrame containing category-level KPIs.
    """
    log_message("INFO", "Writing category KPIs to DynamoDB (CategoryKPIs table)...")
    table = dynamodb_resource.Table("CategoryKPIs")
    
    # Convert Spark DataFrame to Pandas DataFrame for batch_writer
    rows = category_kpi_df.toPandas().to_dict(orient="records")
    
    if not rows:
        log_message("WARN", "No category KPIs to write to DynamoDB.")
        return

    try:
        with table.batch_writer() as batch:
            for row in rows:
                batch.put_item(Item={
                    "category_id": row['category'],
                    "date_key": str(row['order_date']),
                    "daily_revenue": Decimal(str(row["daily_revenue"] or 0)),
                    "avg_order_value": Decimal(str(row["avg_order_value"] or 0)),
                    "avg_return_rate": Decimal(str(row["avg_return_rate"] or 0))
                })
        log_message("INFO", f"Finished writing {len(rows)} category KPIs to DynamoDB.")
    except ClientError as e:
        log_message("ERROR", f"DynamoDB Client Error writing category KPIs: {e}")
        raise
    except Exception as e:
        log_message("ERROR", f"Unexpected error writing category KPIs: {e}")
        raise

def write_order_kpis_to_dynamodb(order_kpi_df):
    """
    Writes order-level KPIs from a Spark DataFrame to the DynamoDB DailyKPIs table.

    Args:
        order_kpi_df (pyspark.sql.DataFrame): DataFrame containing order-level KPIs.
    """
    log_message("INFO", "Writing order KPIs to DynamoDB (DailyKPIs table)...")
    table = dynamodb_resource.Table("DailyKPIs")
    
    # Convert Spark DataFrame to Pandas DataFrame for batch_writer
    rows = order_kpi_df.toPandas().to_dict(orient="records")

    if not rows:
        log_message("WARN", "No order KPIs to write to DynamoDB.")
        return

    try:
        with table.batch_writer() as batch:
            for row in rows:
                batch.put_item(Item={
                    "date_key": str(row['date_key']),
                    "total_orders": int(row["total_orders"] or 0),
                    "total_revenue": Decimal(str(row["total_revenue"] or 0)),
                    "total_items_sold": int(row["total_items_sold"] or 0),
                    "return_rate": Decimal(str(row["return_rate"] or 0)),
                    "unique_customers": int(row["unique_customers"] or 0)
                })
        log_message("INFO", f"Finished writing {len(rows)} order KPIs to DynamoDB.")
    except ClientError as e:
        log_message("ERROR", f"DynamoDB Client Error writing order KPIs: {e}")
        raise
    except Exception as e:
        log_message("ERROR", f"Unexpected error writing order KPIs: {e}")
        raise

# --- Main Execution Flow ---
def main():
    """
    Main function to orchestrate the data transformation process.
    """
    log_message("INFO", "Starting transformation script execution.")
    
    # 1. Get Environment Variables
    bucket = os.environ.get("S3_BUCKET")
    products_key_raw = os.environ.get("PRODUCTS_KEY") 
    orders_keys_json = os.environ.get("ORDERS_KEYS", "[]")
    items_keys_json = os.environ.get("ORDER_ITEMS_KEYS", "[]")

    if not all([bucket, products_key_raw]):
        log_message("ERROR", "Missing S3_BUCKET or PRODUCTS_KEY environment variable.")
        sys.exit(1)

    try:
        # These are the specific new files that triggered this batch
        orders_keys_raw = json.loads(orders_keys_json)
        items_keys_raw = json.loads(items_keys_json)
    except json.JSONDecodeError as e:
        log_message("ERROR", f"JSON parsing failed for ORDERS_KEYS or ORDER_ITEMS_KEYS: {e}")
        sys.exit(1)
    except Exception as e:
        log_message("ERROR", f"Unexpected error parsing keys from environment: {e}")
        sys.exit(1)

    if not orders_keys_raw and not items_keys_raw:
        log_message("ERROR", "Both ORDERS_KEYS and ORDER_ITEMS_KEYS are empty. No daily data to process.")
        sys.exit(1)

    log_message("DEBUG", f"S3 Bucket: {bucket}")
    log_message("DEBUG", f"Products key (raw): {products_key_raw}")
    log_message("DEBUG", f"Orders keys (raw): {orders_keys_raw}")
    log_message("DEBUG", f"Order items keys (raw): {items_keys_raw}")

    # 2. Initialize Spark Session
    spark = initialize_spark_session()

    try:
        # 3. Determine Target Dates for Incremental Processing
        # We need to load the NEW orders data to find the dates that need re-computation.
        new_orders_validated_s3a_paths = get_s3_paths_for_spark_read(bucket, orders_keys_raw, "raw/", "validated/")
        log_message("INFO", f"Loading new orders data to extract target dates from: {new_orders_validated_s3a_paths}")
        df_new_orders_for_dates = load_data_from_s3_to_spark(spark, bucket, new_orders_validated_s3a_paths, "new_orders_for_dates")
        
        # Extract distinct dates from the newly arrived orders
        # Ensure 'created_at' is cast to date before distinct collection
        df_new_orders_for_dates = df_new_orders_for_dates.withColumn("order_date", to_date(col("created_at")))
        distinct_new_dates = [row.order_date.strftime("%Y-%m-%d") for row in df_new_orders_for_dates.select("order_date").distinct().collect()]
        log_message("INFO", f"Identified target incremental dates for KPI computation: {distinct_new_dates}")

        if not distinct_new_dates:
            log_message("WARN", "No distinct order dates found in the incoming orders files. Exiting.")
            sys.exit(0) # Exit gracefully if no dates to process

        # 4. Load All Necessary DataFrames (Full historical validated data for accurate KPIs)
        validated_products_s3a_path = get_s3_paths_for_spark_read(bucket, [products_key_raw], "raw/", "validated/")[0]
        df_products_full = load_data_from_s3_to_spark(spark, bucket, [validated_products_s3a_path], "products_full")

        all_orders_validated_s3a_paths = get_all_validated_files_in_prefix(bucket, "validated/orders/")
        df_orders_full = load_data_from_s3_to_spark(spark, bucket, all_orders_validated_s3a_paths, "orders_full")

        all_items_validated_s3a_paths = get_all_validated_files_in_prefix(bucket, "validated/order_items/")
        df_items_full = load_data_from_s3_to_spark(spark, bucket, all_items_validated_s3a_paths, "order_items_full")

        # 5. Prepare Data for KPI Computation
        df_consolidated = prepare_data_for_kpis(df_products_full, df_orders_full, df_items_full)

        # 6. Compute KPIs (incrementally for target dates)
        category_kpi_df = compute_category_kpis(df_consolidated, distinct_new_dates)
        order_kpi_df = compute_order_kpis(df_consolidated, df_orders_full, distinct_new_dates) # Pass df_orders_full for order-level specific aggregations

        # Log a sample of computed KPIs
        log_message("INFO", "Sample of Category KPIs:")
        category_kpi_df.show(5, truncate=False)
        log_message("INFO", "Sample of Order KPIs:")
        order_kpi_df.show(5, truncate=False)

        # 7. Write KPIs to DynamoDB
        write_category_kpis_to_dynamodb(category_kpi_df)
        write_order_kpis_to_dynamodb(order_kpi_df)

        # 8. Archive Processed Files
        log_message("INFO", "Moving processed files from 'validated/' to 'processed/' folder.")
        # Products file
        move_file(bucket, validated_products_s3a_path.replace("s3a://", ""), "processed/")
        # Orders files (only the ones from this batch that triggered the process)
        for key in orders_keys_raw:
            validated_key = key.replace("raw/", "validated/")
            move_file(bucket, validated_key, "processed/")
        # Order items files (only the ones from this batch that triggered the process)
        for key in items_keys_raw:
            validated_key = key.replace("raw/", "validated/")
            move_file(bucket, validated_key, "processed/")

        log_message("INFO", "Transformation complete. Exiting with success.")
        spark.stop()
        sys.exit(0)

    except Exception as e:
        log_message("ERROR", f"An unhandled error occurred during transformation: {e}", exc_info=True)
        spark.stop()
        sys.exit(1)

if __name__ == "__main__":
    main() 
