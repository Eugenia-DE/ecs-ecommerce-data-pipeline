import os
import sys
import boto3
import json
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, avg, countDistinct, count, when, to_date, lit
)
from botocore.exceptions import ClientError
from datetime import datetime

# Global log list
script_logs = []

# S3 client
s3_client = boto3.client("s3")

def log_message(level, message):
    timestamp = datetime.utcnow().isoformat(timespec='milliseconds') + 'Z'
    formatted_message = f"{timestamp} [{level}] {message}"
    print(formatted_message)
    script_logs.append(formatted_message)

def write_logs_to_s3(bucket, task_type):
    log_content = "\n".join(script_logs)
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
    log_key = f"logs/{task_type}/{task_type}_{timestamp}.log"
    try:
        s3_client.put_object(Bucket=bucket, Key=log_key, Body=log_content)
        log_message("INFO", f"Successfully uploaded logs to s3://{bucket}/{log_key}")
    except Exception as e:
        log_message("ERROR", f"Failed to upload logs to s3://{bucket}/{log_key}: {e}")

def move_file(bucket, key, destination_prefix):
    source_key = key
    try:
        s3_client.head_object(Bucket=bucket, Key=source_key)
        relative_path = source_key.replace("validated/", "")
        target_key = os.path.join(destination_prefix, relative_path)
        s3_client.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": source_key}, Key=target_key)
        s3_client.delete_object(Bucket=bucket, Key=source_key)
        log_message("INFO", f"Moved file s3://{bucket}/{source_key} -> s3://{bucket}/{target_key}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            log_message("INFO", f"File s3://{bucket}/{source_key} not found.")
        else:
            log_message("ERROR", f"AWS Client Error moving file: {e}")
            raise
    except Exception as e:
        log_message("ERROR", f"Failed to move file: {e}")
        raise

def load_csv_spark(spark, bucket, key):
    path = f"s3a://{bucket}/{key}"
    log_message("INFO", f"Loading CSV from Spark path: {path}")
    return spark.read.option("header", "true").csv(path)

def compute_kpis_spark(df_products, df_orders, df_items, target_dates=None):
    log_message("INFO", "Starting KPI computation...")
    df_products = df_products.withColumnRenamed("id", "product_id")
    df_orders = df_orders.withColumn("order_date", to_date("created_at"))

    if "returned_at" in df_orders.columns:
        df_orders = df_orders.withColumnRenamed("returned_at", "returned_at_order")
    else:
        df_orders = df_orders.withColumn("returned_at_order", lit(None))

    if "returned_at" in df_items.columns:
        df_items = df_items.withColumnRenamed("returned_at", "returned_at_item")
    else:
        df_items = df_items.withColumn("returned_at_item", lit(None))

    df_items = df_items.withColumn("sale_price", col("sale_price").cast("double"))
    df_items = df_items.join(df_products.select("product_id", "category"), on="product_id", how="left")
    df_items = df_items.join(
        df_orders.select("order_id", "order_date", "user_id", "returned_at_order"),
        on="order_id", how="left"
    )

    if target_dates:
        log_message("INFO", f"Filtering by dates: {target_dates}")
        df_items_filtered = df_items.filter(col("order_date").isin(target_dates))
        df_orders_filtered = df_orders.filter(col("order_date").isin(target_dates))
    else:
        df_items_filtered = df_items
        df_orders_filtered = df_orders

    df_items_filtered = df_items_filtered.withColumn("returned_item_flag", when(col("returned_at_item").isNotNull(), 1).otherwise(0))
    df_orders_filtered = df_orders_filtered.withColumn("returned_order_flag", when(col("returned_at_order").isNotNull(), 1).otherwise(0))

    category_kpi = df_items_filtered.groupBy("category", "order_date").agg(
        _sum("sale_price").alias("daily_revenue"),
        avg("sale_price").alias("avg_order_value"),
        avg("returned_item_flag").alias("avg_return_rate")
    )

    total_revenue_df = df_items_filtered.groupBy("order_id").agg(_sum("sale_price").alias("order_revenue"))
    df_orders_filtered = df_orders_filtered.join(total_revenue_df, on="order_id", how="left")

    order_kpi = df_orders_filtered.groupBy("order_date").agg(
        countDistinct("order_id").alias("total_orders"),
        _sum("order_revenue").alias("total_revenue"),
        count("order_id").alias("total_items_sold"),
        avg("returned_order_flag").alias("return_rate"),
        countDistinct("user_id").alias("unique_customers")
    )

    return category_kpi, order_kpi

def write_category_kpis_to_dynamodb(category_kpi_df):
    log_message("INFO", "Writing category KPIs to DynamoDB...")
    table = boto3.resource("dynamodb").Table("CategoryKPIs")
    rows = category_kpi_df.toPandas().to_dict(orient="records")
    with table.batch_writer() as batch:
        for row in rows:
            batch.put_item(Item={
                "category_id": row['category'],
                "date_key": str(row['order_date']),
                "daily_revenue": Decimal(str(row["daily_revenue"] or 0)),
                "avg_order_value": Decimal(str(row["avg_order_value"] or 0)),
                "avg_return_rate": Decimal(str(row["avg_return_rate"] or 0))
            })
    log_message("INFO", "Finished writing category KPIs.")

def write_order_kpis_to_dynamodb(order_kpi_df):
    log_message("INFO", "Writing order KPIs to DynamoDB...")
    table = boto3.resource("dynamodb").Table("DailyKPIs")
    rows = order_kpi_df.toPandas().to_dict(orient="records")
    with table.batch_writer() as batch:
        for row in rows:
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
    log_message("INFO", "Starting transformation script execution.")
    
    bucket = os.environ.get("S3_BUCKET")
    products_key_raw = os.environ.get("PRODUCTS_KEY")
    orders_keys_json = os.environ.get("ORDERS_KEYS", "[]")
    items_keys_json = os.environ.get("ORDER_ITEMS_KEYS", "[]")

    if not bucket or not products_key_raw:
        log_message("ERROR", "Missing S3_BUCKET or PRODUCTS_KEY.")
        sys.exit(1)

    try:
        orders_keys_raw = json.loads(orders_keys_json)
        items_keys_raw = json.loads(items_keys_json)
    except json.JSONDecodeError as e:
        log_message("ERROR", f"JSON parsing failed: {e}")
        sys.exit(1)

    if not orders_keys_raw:
        log_message("ERROR", "ORDERS_KEYS is empty.")
        sys.exit(1)
    if not items_keys_raw:
        log_message("ERROR", "ORDER_ITEMS_KEYS is empty.")
        sys.exit(1)

    spark = SparkSession.builder \
        .appName("KPITransformation") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()

    new_order_validated_paths = [
        f"s3a://{bucket}/{key.strip().replace('raw/', 'validated/')}"
        for key in orders_keys_raw
    ]

    df_new_orders_for_dates = spark.read.option("header", "true").csv(new_order_validated_paths)
    df_new_orders_for_dates = df_new_orders_for_dates.withColumn("order_date", to_date("created_at"))
    distinct_new_dates = [row.order_date.strftime("%Y-%m-%d") for row in df_new_orders_for_dates.select("order_date").distinct().collect()]
    log_message("INFO", f"Target incremental dates: {distinct_new_dates}")

    validated_products_key = products_key_raw.replace("raw/", "validated/")
    df_products = load_csv_spark(spark, bucket, validated_products_key)

    # FIXED S3 list bug
    response_orders = s3_client.list_objects_v2(Bucket=bucket, Prefix="validated/orders/")
    all_orders_validated_paths = [f"s3a://{bucket}/{f['Key']}" for f in response_orders.get('Contents', []) if f['Key'].endswith('.csv')]
    if not all_orders_validated_paths:
        log_message("ERROR", f"No order files found.")
        spark.stop()
        sys.exit(1)

    df_orders_all = spark.read.option("header", "true").csv(all_orders_validated_paths)

    response_items = s3_client.list_objects_v2(Bucket=bucket, Prefix="validated/order_items/")
    all_items_validated_paths = [f"s3a://{bucket}/{f['Key']}" for f in response_items.get('Contents', []) if f['Key'].endswith('.csv')]
    if not all_items_validated_paths:
        log_message("ERROR", f"No order item files found.")
        spark.stop()
        sys.exit(1)

    df_items_all = spark.read.option("header", "true").csv(all_items_validated_paths)

    category_kpi, order_kpi = compute_kpis_spark(df_products, df_orders_all, df_items_all, distinct_new_dates)

    category_kpi.show(5, truncate=False)
    order_kpi.show(5, truncate=False)

    write_category_kpis_to_dynamodb(category_kpi)
    write_order_kpis_to_dynamodb(order_kpi)

    move_file(bucket, validated_products_key, "processed/")
    for key in orders_keys_raw:
        move_file(bucket, key.replace("raw/", "validated/"), "processed/")
    for key in items_keys_raw:
        move_file(bucket, key.replace("raw/", "validated/"), "processed/")

    log_message("INFO", "Stopping SparkSession.")
    spark.stop()
    log_message("INFO", "Transformation complete.")

if __name__ == "__main__":
    try:
        main()
    finally:
        bucket = os.environ.get("S3_BUCKET")
        if bucket:
            write_logs_to_s3(bucket, "transformation")
        else:
            print("[ERROR] S3_BUCKET not set; cannot write logs.")
