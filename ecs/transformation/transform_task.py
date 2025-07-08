import os
import sys
import boto3
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, avg, countDistinct, count, when, to_date, lit
)

# Initialize S3 client
s3_client = boto3.client("s3")


def move_file(bucket, key, target_prefix):
    """
    Moves a file from one S3 location to another within the same bucket.

    Args:
        bucket (str): The name of the S3 bucket.
        key (str): The current key (path) of the file in S3.
        target_prefix (str): The new prefix (folder) where the file should be moved.
    """
    try:
        # Construct the target key by replacing the 'raw/' prefix with the target_prefix
        # This assumes the original key starts with 'raw/'
        target_key = key.replace("raw/", target_prefix)
        # Copy the object to the new location
        s3_client.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": key}, Key=target_key)
        # Delete the original object
        s3_client.delete_object(Bucket=bucket, Key=key)
        print(f"[INFO] Moved file s3://{bucket}/{key} -> s3://{bucket}/{target_key}")
    except Exception as e:
        print(f"[ERROR] Failed to move file: {e}")


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
    return spark.read.option("header", "true").csv(path)


def compute_kpis_spark(df_products, df_orders, df_items):
    """
    Computes category-level and order-level KPIs using Spark DataFrames.

    Args:
        df_products (DataFrame): Spark DataFrame containing product data.
        df_orders (DataFrame): Spark DataFrame containing order data.
        df_items (DataFrame): Spark DataFrame containing order item data.

    Returns:
        tuple: A tuple containing two Spark DataFrames:
               - category_kpi (DataFrame): Aggregated KPIs by category and order date.
               - order_kpi (DataFrame): Aggregated KPIs by order date.
    """
    # Rename 'id' column in products to 'product_id' for consistent joining
    df_products = df_products.withColumnRenamed("id", "product_id")
    # Convert 'created_at' in orders to a date type
    df_orders = df_orders.withColumn("order_date", to_date("created_at"))

    # Handle 'returned_at' columns for orders and items.
    # If the column exists, rename it; otherwise, add a null column.
    if "returned_at" in df_orders.columns:
        df_orders = df_orders.withColumnRenamed("returned_at", "returned_at_order")
    else:
        df_orders = df_orders.withColumn("returned_at_order", lit(None))

    if "returned_at" in df_items.columns:
        df_items = df_items.withColumnRenamed("returned_at", "returned_at_item")
    else:
        df_items = df_items.withColumn("returned_at_item", lit(None))

    # Cast 'sale_price' to double for accurate calculations
    df_items = df_items.withColumn("sale_price", col("sale_price").cast("double"))

    # Joins:
    # 1. Join order items with products to get category information
    df_items = df_items.join(df_products.select("product_id", "category"), on="product_id", how="left")
    # 2. Join order items with orders to get order date, user ID, and order return status
    df_items = df_items.join(
        df_orders.select("order_id", "order_date", "user_id", "returned_at_order"),
        on="order_id", how="left"
    )

    # Flags:
    # Create a flag for returned items (1 if returned_at_item is not null, 0 otherwise)
    df_items = df_items.withColumn("returned_item_flag", when(col("returned_at_item").isNotNull(), 1).otherwise(0))
    # Create a flag for returned orders (1 if returned_at_order is not null, 0 otherwise)
    df_orders = df_orders.withColumn("returned_order_flag", when(col("returned_at_order").isNotNull(), 1).otherwise(0))

    # Category KPIs:
    # Group by category and order_date to calculate daily revenue, average order value, and average return rate
    category_kpi = df_items.groupBy("category", "order_date").agg(
        _sum("sale_price").alias("daily_revenue"),
        avg("sale_price").alias("avg_order_value"),
        avg("returned_item_flag").alias("avg_return_rate")
    )

    # Order KPIs:
    # Calculate total revenue per order first
    total_revenue_df = df_items.groupBy("order_id").agg(_sum("sale_price").alias("order_revenue"))
    # Join this back to the orders DataFrame
    df_orders = df_orders.join(total_revenue_df, on="order_id", how="left")

    # Group by order_date to calculate total orders, total revenue, total items sold,
    # return rate, and unique customers
    order_kpi = df_orders.groupBy("order_date").agg(
        countDistinct("order_id").alias("total_orders"),
        _sum("order_revenue").alias("total_revenue"),
        count("order_id").alias("total_items_sold"), # This counts all items linked to an order_id
        avg("returned_order_flag").alias("return_rate"),
        countDistinct("user_id").alias("unique_customers")
    )

    return category_kpi, order_kpi


def write_category_kpis_to_dynamodb(category_kpi_df):
    """
    Writes category-level KPIs to the DynamoDB 'CategoryKPIs' table.

    Args:
        category_kpi_df (DataFrame): Spark DataFrame containing category KPIs.
    """
    print("[INFO] Writing category KPIs to DynamoDB...")
    # Get the DynamoDB table resource
    table = boto3.resource("dynamodb").Table("CategoryKPIs")
    # Convert Spark DataFrame to Pandas DataFrame, then to a list of dictionaries
    rows = category_kpi_df.toPandas().to_dict(orient="records")
    
    # Use batch_writer for efficient writing
    with table.batch_writer() as batch:
        for row in rows:
            # Ensure all numeric types are converted to Decimal for DynamoDB
            # Use 'category_id' and 'date_key' as per your DynamoDB schema
            batch.put_item(Item={
                "category_id": row['category'],  # Partition Key
                "date_key": row['order_date'],   # Sort Key
                "daily_revenue": Decimal(str(row["daily_revenue"] or 0)),
                "avg_order_value": Decimal(str(row["avg_order_value"] or 0)),
                "avg_return_rate": Decimal(str(row["avg_return_rate"] or 0))
            })
    print("[INFO] Finished writing category KPIs.")


def write_order_kpis_to_dynamodb(order_kpi_df):
    """
    Writes order-level KPIs to the DynamoDB 'DailyKPIs' table.

    Args:
        order_kpi_df (DataFrame): Spark DataFrame containing order KPIs.
    """
    print("[INFO] Writing order KPIs to DynamoDB...")
    # Get the DynamoDB table resource
    table = boto3.resource("dynamodb").Table("DailyKPIs")
    # Convert Spark DataFrame to Pandas DataFrame, then to a list of dictionaries
    rows = order_kpi_df.toPandas().to_dict(orient="records")
    
    # Use batch_writer for efficient writing
    with table.batch_writer() as batch:
        for row in rows:
            # Ensure all numeric types are converted appropriately for DynamoDB
            # Use 'date_key' as the Partition Key as per your DynamoDB schema
            batch.put_item(Item={
                "date_key": row['order_date'],  # Partition Key
                "total_orders": int(row["total_orders"] or 0),
                "total_revenue": Decimal(str(row["total_revenue"] or 0)),
                "total_items_sold": int(row["total_items_sold"] or 0),
                "return_rate": Decimal(str(row["return_rate"] or 0)),
                "unique_customers": int(row["unique_customers"] or 0)
            })
    print("[INFO] Finished writing order KPIs.")


def main():
    """
    Main function to orchestrate the KPI transformation and writing process.
    It loads data from S3, computes KPIs using Spark, and writes them to DynamoDB.
    """
    # Retrieve environment variables for S3 bucket and file keys
    bucket = os.environ.get("S3_BUCKET")
    products_key = os.environ.get("PRODUCTS_KEY")
    orders_keys = os.environ.get("ORDERS_KEYS", "").split(",")
    items_keys = os.environ.get("ORDER_ITEMS_KEYS", "").split(",")

    # Basic validation for environment variables
    if not bucket or not products_key:
        print("[ERROR] Missing S3_BUCKET or PRODUCTS_KEY environment variable.")
        sys.exit(1)

    if not orders_keys or orders_keys == ['']:
        print("[ERROR] No ORDERS_KEYS (order files) provided in environment variable.")
        sys.exit(1)

    if not items_keys or items_keys == ['']:
        print("[ERROR] No ORDER_ITEMS_KEYS (order_items files) provided in environment variable.")
        sys.exit(1)

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("KPITransformation") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()

    # Load data from S3 into Spark DataFrames
    df_products = load_csv_spark(spark, bucket, products_key)
    # Handle multiple order files
    order_paths = [f"s3a://{bucket}/{key.strip()}" for key in orders_keys]
    df_orders = spark.read.option("header", "true").csv(order_paths)
    # Handle multiple order item files
    item_paths = [f"s3a://{bucket}/{key.strip()}" for key in items_keys]
    df_items = spark.read.option("header", "true").csv(item_paths)

    # Compute KPIs
    category_kpi, order_kpi = compute_kpis_spark(df_products, df_orders, df_items)

    # Display a sample of the computed KPIs
    print("\n[INFO] Category-Level KPIs")
    category_kpi.show(5, truncate=False)

    print("\n[INFO] Order-Level KPIs")
    order_kpi.show(5, truncate=False)

    # Write KPIs to DynamoDB
    write_category_kpis_to_dynamodb(category_kpi)
    write_order_kpis_to_dynamodb(order_kpi)

    # Move processed files to a 'processed/' prefix in S3
    move_file(bucket, products_key, "processed/")
    for key in orders_keys:
        move_file(bucket, key.strip(), "processed/")
    for key in items_keys:
        move_file(bucket, key.strip(), "processed/")

    # Stop the SparkSession
    spark.stop()


if __name__ == "__main__":
    main()
