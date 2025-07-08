import os
import sys
import boto3
import json # Import json for parsing environment variables
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, avg, countDistinct, count, when, to_date, lit
)
from botocore.exceptions import ClientError
from datetime import datetime

# Global list to store log messages for S3 upload
script_logs = []

# Initialize S3 client
s3_client = boto3.client("s3")

def log_message(level, message):
    """
    Logs a message to both stdout (for CloudWatch) and a global list (for S3 upload).

    Args:
        level (str): The log level (e.g., "INFO", "ERROR", "DEBUG").
        message (str): The log message.
    """
    timestamp = datetime.utcnow().isoformat(timespec='milliseconds') + 'Z'
    formatted_message = f"{timestamp} [{level}] {message}"
    print(formatted_message) # Print to stdout for CloudWatch
    script_logs.append(formatted_message) # Append to global list for S3

def write_logs_to_s3(bucket, task_type):
    """
    Writes all collected script logs to a file in the S3 logs folder.

    Args:
        bucket (str): The name of the S3 bucket.
        task_type (str): The type of task (e.g., "validation", "transformation").
    """
    log_content = "\n".join(script_logs)
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
    log_key = f"logs/{task_type}/{task_type}_{timestamp}.log"
    
    try:
        s3_client.put_object(Bucket=bucket, Key=log_key, Body=log_content)
        log_message("INFO", f"Successfully uploaded logs to s3://{bucket}/{log_key}")
    except Exception as e:
        log_message("ERROR", f"Failed to upload logs to s3://{bucket}/{log_key}: {e}")


def move_file(bucket, key, destination_prefix):
    """
    Moves a file from its current S3 key (expected to be in 'validated/' prefix)
    to a new S3 key under the specified destination_prefix.
    This function is idempotent: it handles cases where the source file might
    no longer exist (e.g., already moved by a previous successful run).

    Args:
        bucket (str): The name of the S3 bucket.
        key (str): The current key (path) of the file in S3 (e.g., "validated/orders/order1.csv").
        destination_prefix (str): The new prefix (folder) where the file should be moved
                                  (e.g., "processed/").
    """
    source_key = key
    
    try:
        # Check if the source object exists before attempting to copy.
        s3_client.head_object(Bucket=bucket, Key=source_key)

        relative_path = source_key.replace("validated/", "")
        target_key = os.path.join(destination_prefix, relative_path)

        # Copy the object to the new location
        s3_client.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": source_key}, Key=target_key)
        # Delete the original object from the source location
        s3_client.delete_object(Bucket=bucket, Key=source_key)
        log_message("INFO", f"Moved file s3://{bucket}/{source_key} -> s3://{bucket}/{target_key}")

    except ClientError as e:
        # Handle the specific case where the source file does not exist (already moved or never existed)
        if e.response['Error']['Code'] == 'NoSuchKey':
            log_message("INFO", f"File s3://{bucket}/{source_key} not found. It might have already been moved or does not exist.")
        else:
            # Re-raise other AWS ClientErrors
            log_message("ERROR", f"AWS Client Error moving file {source_key} to {destination_prefix}: {e}")
            raise # Re-raise to ensure the task fails if it's a critical AWS error
    except Exception as e:
        # Catch any other unexpected exceptions during file movement
        log_message("ERROR", f"Failed to move file {source_key} to {destination_prefix}: {e}")
        raise # Re-raise to ensure the task fails on unexpected errors


def load_csv_spark(spark, bucket, key):
    """
    Loads a single CSV file from S3 into a Spark DataFrame.

    Args:
        spark (SparkSession): The active SparkSession.
        bucket (str): The name of the S3 bucket.
        key (str): The key (path) of the CSV file in S3.

    Returns:
        DataFrame: The Spark DataFrame loaded from the CSV.
    """
    path = f"s3a://{bucket}/{key}"
    log_message("INFO", f"Loading CSV from Spark path: {path}")
    return spark.read.option("header", "true").csv(path)


def compute_kpis_spark(df_products, df_orders, df_items, target_dates=None):
    """
    Computes category-level and order-level KPIs using Spark DataFrames.
    Can perform incremental computation by filtering for target_dates.

    Args:
        df_products (DataFrame): Spark DataFrame containing product data.
        df_orders (DataFrame): Spark DataFrame containing order data.
        df_items (DataFrame): Spark DataFrame containing order item data.
        target_dates (list, optional): A list of specific order_date strings (YYYY-MM-DD)
                                        for which to compute KPIs. If None, computes for all data.

    Returns:
        tuple: A tuple containing two Spark DataFrames:
               - category_kpi (DataFrame): Aggregated KPIs by category and order date.
               - order_kpi (DataFrame): Aggregated KPIs by order date.
    """
    log_message("INFO", "Starting KPI computation...")
    # Rename 'id' column in products to 'product_id' for consistent joining
    df_products = df_products.withColumnRenamed("id", "product_id")
    # Convert 'created_at' in orders to a date type
    df_orders = df_orders.withColumn("order_date", to_date("created_at"))

    # Handle 'returned_at' columns for orders and items.
    if "returned_at" in df_orders.columns:
        df_orders = df_orders.withColumnRenamed("returned_at", "returned_at_order")
        log_message("DEBUG", "Renamed 'returned_at' to 'returned_at_order' in orders.")
    else:
        df_orders = df_orders.withColumn("returned_at_order", lit(None))
        log_message("DEBUG", "Added null 'returned_at_order' column to orders.")

    if "returned_at" in df_items.columns:
        df_items = df_items.withColumnRenamed("returned_at", "returned_at_item")
        log_message("DEBUG", "Renamed 'returned_at' to 'returned_at_item' in order_items.")
    else:
        df_items = df_items.withColumn("returned_at_item", lit(None))
        log_message("DEBUG", "Added null 'returned_at_item' column to order_items.")

    # Cast 'sale_price' to double for accurate calculations
    df_items = df_items.withColumn("sale_price", col("sale_price").cast("double"))
    log_message("DEBUG", "Casted 'sale_price' to double in order_items.")

    # Joins:
    # 1. Join order items with products to get category information
    df_items = df_items.join(df_products.select("product_id", "category"), on="product_id", how="left")
    log_message("DEBUG", "Joined order_items with products.")
    # 2. Join order items with orders to get order date, user ID, and order return status
    df_items = df_items.join(
        df_orders.select("order_id", "order_date", "user_id", "returned_at_order"),
        on="order_id", how="left"
    )
    log_message("DEBUG", "Joined order_items with orders.")

    # Apply incremental filtering if target_dates are provided
    if target_dates:
        log_message("INFO", f"Applying incremental filter for dates: {target_dates}")
        # Filter both df_items and df_orders to only include data for the target dates
        df_items_filtered = df_items.filter(col("order_date").isin(target_dates))
        df_orders_filtered = df_orders.filter(col("order_date").isin(target_dates))
    else:
        log_message("INFO", "No target dates provided, computing KPIs for all available data.")
        df_items_filtered = df_items
        df_orders_filtered = df_orders

    # Flags:
    # Create a flag for returned items (1 if returned_at_item is not null, 0 otherwise)
    df_items_filtered = df_items_filtered.withColumn("returned_item_flag", when(col("returned_at_item").isNotNull(), 1).otherwise(0))
    log_message("DEBUG", "Created 'returned_item_flag' on filtered items.")
    # Create a flag for returned orders (1 if returned_at_order is not null, 0 otherwise)
    df_orders_filtered = df_orders_filtered.withColumn("returned_order_flag", when(col("returned_at_order").isNotNull(), 1).otherwise(0))
    log_message("DEBUG", "Created 'returned_order_flag' on filtered orders.")


    # Category KPIs:
    # Group by category and order_date to calculate daily revenue, average order value, and average return rate
    category_kpi = df_items_filtered.groupBy("category", "order_date").agg(
        _sum("sale_price").alias("daily_revenue"),
        avg("sale_price").alias("avg_order_value"),
        avg("returned_item_flag").alias("avg_return_rate")
    )
    log_message("INFO", "Computed Category-Level KPIs.")

    # Order KPIs:
    # Calculate total revenue per order first
    total_revenue_df = df_items_filtered.groupBy("order_id").agg(_sum("sale_price").alias("order_revenue"))
    log_message("DEBUG", "Calculated total revenue per order from filtered items.")
    # Join this back to the orders DataFrame
    df_orders_filtered = df_orders_filtered.join(total_revenue_df, on="order_id", how="left")
    log_message("DEBUG", "Joined filtered orders with order revenue.")

    # Group by order_date to calculate total orders, total revenue, total items sold,
    # return rate, and unique customers
    order_kpi = df_orders_filtered.groupBy("order_date").agg(
        countDistinct("order_id").alias("total_orders"),
        _sum("order_revenue").alias("total_revenue"),
        count("order_id").alias("total_items_sold"), 
        avg("returned_order_flag").alias("return_rate"),
        countDistinct("user_id").alias("unique_customers")
    )
    log_message("INFO", "Computed Order-Level KPIs.")

    return category_kpi, order_kpi


def write_category_kpis_to_dynamodb(category_kpi_df):
    """
    Writes category-level KPIs to the DynamoDB 'CategoryKPIs' table.

    Args:
        category_kpi_df (DataFrame): Spark DataFrame containing category KPIs.
    """
    log_message("INFO", "Writing category KPIs to DynamoDB...")
    # Get the DynamoDB table resource
    table = boto3.resource("dynamodb").Table("CategoryKPIs")
    # Convert Spark DataFrame to Pandas DataFrame, then to a list of dictionaries
    rows = category_kpi_df.toPandas().to_dict(orient="records")

    # Use batch_writer for efficient writing
    with table.batch_writer() as batch:
        for row in rows:
            batch.put_item(Item={
                "category_id": row['category'],  # Partition Key
                "date_key": str(row['order_date']),   # Sort Key 
                "daily_revenue": Decimal(str(row["daily_revenue"] or 0)),
                "avg_order_value": Decimal(str(row["avg_order_value"] or 0)),
                "avg_return_rate": Decimal(str(row["avg_return_rate"] or 0))
            })
    log_message("INFO", "Finished writing category KPIs.")


def write_order_kpis_to_dynamodb(order_kpi_df):
    """
    Writes order-level KPIs to the DynamoDB 'DailyKPIs' table.

    Args:
        order_kpi_df (DataFrame): Spark DataFrame containing order KPIs.
    """
    log_message("INFO", "Writing order KPIs to DynamoDB...")
    # Get the DynamoDB table resource
    table = boto3.resource("dynamodb").Table("DailyKPIs")
    # Convert Spark DataFrame to Pandas DataFrame, then to a list of dictionaries
    rows = order_kpi_df.toPandas().to_dict(orient="records")

    # Use batch_writer for efficient writing
    with table.batch_writer() as batch:
        for row in rows:
            # Ensure all numeric types are converted appropriately for DynamoDB
            # Convert 'order_date' to string as DynamoDB String type expects a string
            batch.put_item(Item={
                "date_key": str(row['order_date']),  
                "total_orders": int(row["total_orders"] or 0),
                "total_revenue": Decimal(str(row["total_revenue"] or 0)),
                "total_items_sold": int(row["total_items_sold"] or 0),
                "return_rate": Decimal(str(row["return_rate"] or 0)),
                "unique_customers": int(row["unique_customers"] or 0)
            })
    log_message("INFO", "Finished writing order KPIs.")


def main():
    """
    Main function to orchestrate the KPI transformation and writing process.
    It loads data from S3, computes KPIs using Spark, and writes them to DynamoDB.
    """
    log_message("INFO", "Starting transformation script execution.")
    
    bucket = os.environ.get("S3_BUCKET")
    # PRODUCTS_KEY is expected to be a single key (e.g., "products/products.csv")
    products_key_raw = os.environ.get("PRODUCTS_KEY") 
    # ORDERS_KEYS and ORDER_ITEMS_KEYS are expected to be JSON strings representing lists of keys
    orders_keys_json = os.environ.get("ORDERS_KEYS", "[]") 
    items_keys_json = os.environ.get("ORDER_ITEMS_KEYS", "[]") 

    # Basic validation for environment variables
    if not bucket or not products_key_raw:
        log_message("ERROR", "Missing S3_BUCKET or PRODUCTS_KEY environment variable.")
        sys.exit(1)

    try:
        orders_keys_raw = json.loads(orders_keys_json)
        items_keys_raw = json.loads(items_keys_json)
    except json.JSONDecodeError as e:
        log_message("ERROR", f"Failed to parse ORDERS_KEYS or ORDER_ITEMS_KEYS from environment (not valid JSON): {e}")
        sys.exit(1)
    except Exception as e:
        log_message("ERROR", f"Unexpected error parsing keys from environment: {e}")
        sys.exit(1)

    if not orders_keys_raw: # Check if the list is empty after parsing
        log_message("ERROR", "No ORDERS_KEYS (order files) provided in environment variable or list is empty.")
        sys.exit(1)

    if not items_keys_raw: # Check if the list is empty after parsing
        log_message("ERROR", "No ORDER_ITEMS_KEYS (order_items files) provided in environment variable or list is empty.")
        sys.exit(1)

    log_message("DEBUG", f"S3 Bucket: {bucket}")
    log_message("DEBUG", f"Products original key: {products_key_raw}")
    log_message("DEBUG", f"Orders original keys: {orders_keys_raw}")
    log_message("DEBUG", f"Order items original keys: {items_keys_raw}")

    # Initialize SparkSession
    log_message("INFO", "Initializing SparkSession...")
    spark = SparkSession.builder \
        .appName("KPITransformation") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()
    log_message("INFO", "SparkSession initialized.")

    # --- Incremental Loading Logic ---
    log_message("INFO", "Identifying distinct order dates from newly validated order files for incremental processing.")
    new_order_validated_paths = []
    for key in orders_keys_raw:
        # Construct the path to the validated folder for the new order files
        validated_key = key.strip().replace("raw/", "validated/")
        new_order_validated_paths.append(f"s3a://{bucket}/{validated_key}")
    
    # Load only the new order files to extract the dates
    df_new_orders_for_dates = spark.read.option("header", "true").csv(new_order_validated_paths)
    df_new_orders_for_dates = df_new_orders_for_dates.withColumn("order_date", to_date("created_at"))
    
    # Collect distinct dates for filtering
    distinct_new_dates = [row.order_date.strftime("%Y-%m-%d") for row in df_new_orders_for_dates.select("order_date").distinct().collect()]
    log_message("INFO", f"Identified distinct dates for incremental processing: {distinct_new_dates}")

    # 2. Load all necessary data from the 'validated/' folder
    validated_products_key = products_key_raw.replace("raw/", "validated/")
    log_message("INFO", f"Loading all products data from: s3a://{bucket}/{validated_products_key}")
    df_products = load_csv_spark(spark, bucket, validated_products_key)

    # All orders data from 'validated/' 
    all_orders_validated_paths = [f"s3a://{bucket}/validated/orders/" + f for f in s3_client.list_objects_v2(Bucket=bucket, Prefix="validated/orders/")['Contents'] if f['Key'].endswith('.csv')]
    if not all_orders_validated_paths:
        log_message("ERROR", "No order files found in s3a://{bucket}/validated/orders/. Exiting.")
        spark.stop()
        sys.exit(1)

    log_message("INFO", f"Loading all orders data from: {len(all_orders_validated_paths)} files in validated/orders/.")
    df_orders_all = spark.read.option("header", "true").csv(all_orders_validated_paths)

    # All order items data from 'validated/' (we will filter this later in compute_kpis_spark)
    all_items_validated_paths = [f"s3a://{bucket}/validated/order_items/" + f for f in s3_client.list_objects_v2(Bucket=bucket, Prefix="validated/order_items/")['Contents'] if f['Key'].endswith('.csv')]
    if not all_items_validated_paths:
        log_message("ERROR", "No order item files found in s3a://{bucket}/validated/order_items/. Exiting.")
        spark.stop()
        sys.exit(1)

    log_message("INFO", f"Loading all order items data from: {len(all_items_validated_paths)} files in validated/order_items/.")
    df_items_all = spark.read.option("header", "true").csv(all_items_validated_paths)

    # Compute KPIs, passing the distinct_new_dates for incremental processing
    category_kpi, order_kpi = compute_kpis_spark(df_products, df_orders_all, df_items_all, target_dates=distinct_new_dates)

    # Display a sample of the computed KPIs
    log_message("INFO", "\n[INFO] Category-Level KPIs Sample (for incremental dates):")
    category_kpi.show(5, truncate=False)

    log_message("INFO", "\n[INFO] Order-Level KPIs Sample (for incremental dates):")
    order_kpi.show(5, truncate=False)

    # Write KPIs to DynamoDB (DynamoDB's put_item is an upsert, handling updates for existing dates)
    write_category_kpis_to_dynamodb(category_kpi)
    write_order_kpis_to_dynamodb(order_kpi)

    # Move processed files from 'validated/' to 'processed/'
    log_message("INFO", "Moving processed files from 'validated/' to 'processed/'...")
    
    # Move products file
    move_file(bucket, validated_products_key, "processed/")
    
    # Move orders files (only the new ones that were just processed)
    for key_raw in orders_keys_raw:
        validated_key = key_raw.strip().replace("raw/", "validated/")
        move_file(bucket, validated_key, "processed/")
    
    # Move order_items files (only the new ones that were just processed)
    for key_raw in items_keys_raw:
        validated_key = key_raw.strip().replace("raw/", "validated/")
        move_file(bucket, validated_key, "processed/")

    # Stop the SparkSession
    log_message("INFO", "Stopping SparkSession.")
    spark.stop()
    log_message("INFO", "Transformation script execution finished successfully.")


if __name__ == "__main__":
    try:
        main()
    finally:
        # Ensure logs are written to S3 even if an unhandled exception occurs
        bucket = os.environ.get("S3_BUCKET")
        if bucket: # Only attempt to write logs if bucket is defined
            write_logs_to_s3(bucket, "transformation")
        else:
            print("[ERROR] S3_BUCKET environment variable not set, cannot write transformation logs to S3.")
