import os
import sys
import boto3
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, avg, countDistinct, count, when, to_date
)

# S3 client for file movement
s3_client = boto3.client("s3")


def move_file(bucket, key, target_prefix):
    try:
        target_key = key.replace("raw/", target_prefix)
        s3_client.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": key}, Key=target_key)
        s3_client.delete_object(Bucket=bucket, Key=key)
        print(f"[INFO] Moved file s3://{bucket}/{key} -> s3://{bucket}/{target_key}")
    except Exception as e:
        print(f"[ERROR] Failed to move file: {e}")


def load_csv_spark(spark, bucket, key):
    path = f"s3a://{bucket}/{key}"
    return spark.read.option("header", "true").csv(path)


def compute_kpis_spark(df_products, df_orders, df_items):
    df_products = df_products.withColumnRenamed("id", "product_id")
    df_orders = df_orders.withColumn("order_date", to_date("created_at"))
    df_items = df_items.withColumn("sale_price", col("sale_price").cast("double"))

    df_items = df_items.join(df_products.select("product_id", "category"), on="product_id", how="left")
    df_items = df_items.join(
        df_orders.select("order_id", "order_date", "user_id", "returned_at"),
        on="order_id", how="left"
    )
    df_items = df_items.withColumn("returned", when(col("returned_at").isNotNull(), 1).otherwise(0))

    category_kpi = df_items.groupBy("category", "order_date").agg(
        _sum("sale_price").alias("daily_revenue"),
        avg("sale_price").alias("avg_order_value"),
        avg("returned").alias("avg_return_rate")
    )

    df_orders = df_orders.withColumn("returned", when(col("returned_at").isNotNull(), 1).otherwise(0))
    total_revenue_df = df_items.groupBy("order_id").agg(_sum("sale_price").alias("order_revenue"))
    df_orders = df_orders.join(total_revenue_df, on="order_id", how="left")

    order_kpi = df_orders.groupBy("order_date").agg(
        countDistinct("order_id").alias("total_orders"),
        _sum("order_revenue").alias("total_revenue"),
        count("order_id").alias("total_items_sold"),
        avg("returned").alias("return_rate"),
        countDistinct("user_id").alias("unique_customers")
    )

    return category_kpi, order_kpi


def write_category_kpis_to_dynamodb(category_kpi_df):
    print("[INFO] Writing category KPIs to DynamoDB...")
    table = boto3.resource("dynamodb").Table("CategoryKPIs")
    rows = category_kpi_df.toPandas().to_dict(orient="records")
    with table.batch_writer() as batch:
        for row in rows:
            batch.put_item(Item={
                "PK": f"CATEGORY#{row['category']}",
                "SK": f"DATE#{row['order_date']}",
                "daily_revenue": Decimal(str(row["daily_revenue"])),
                "avg_order_value": Decimal(str(row["avg_order_value"])),
                "avg_return_rate": Decimal(str(row["avg_return_rate"]))
            })
    print("[INFO] Finished writing category KPIs.")


def write_order_kpis_to_dynamodb(order_kpi_df):
    print("[INFO] Writing order KPIs to DynamoDB...")
    table = boto3.resource("dynamodb").Table("DailyKPIs")
    rows = order_kpi_df.toPandas().to_dict(orient="records")
    with table.batch_writer() as batch:
        for row in rows:
            batch.put_item(Item={
                "PK": f"DATE#{row['order_date']}",
                "total_orders": int(row["total_orders"]),
                "total_revenue": Decimal(str(row["total_revenue"])),
                "total_items_sold": int(row["total_items_sold"]),
                "return_rate": Decimal(str(row["return_rate"])),
                "unique_customers": int(row["unique_customers"])
            })
    print("[INFO] Finished writing order KPIs.")


def main():
    bucket = os.environ.get("S3_BUCKET")
    products_key = os.environ.get("PRODUCTS_KEY")
    orders_keys = os.environ.get("ORDERS_KEYS", "").split(",")
    items_keys = os.environ.get("ORDER_ITEMS_KEYS", "").split(",")

    if not bucket or not products_key:
        print("[ERROR] Missing bucket or products file path.")
        sys.exit(1)

    if not orders_keys or orders_keys == ['']:
        print("[ERROR] No order files provided.")
        sys.exit(1)

    if not items_keys or items_keys == ['']:
        print("[ERROR] No order_items files provided.")
        sys.exit(1)

    spark = SparkSession.builder \
        .appName("KPITransformation") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()

    df_products = load_csv_spark(spark, bucket, products_key)
    order_paths = [f"s3a://{bucket}/{key.strip()}" for key in orders_keys]
    df_orders = spark.read.option("header", "true").csv(*order_paths)
    item_paths = [f"s3a://{bucket}/{key.strip()}" for key in items_keys]
    df_items = spark.read.option("header", "true").csv(*item_paths)

    category_kpi, order_kpi = compute_kpis_spark(df_products, df_orders, df_items)

    print("\n[INFO] Category-Level KPIs")
    category_kpi.show(5, truncate=False)

    print("\n[INFO] Order-Level KPIs")
    order_kpi.show(5, truncate=False)

    write_category_kpis_to_dynamodb(category_kpi)
    write_order_kpis_to_dynamodb(order_kpi)

    move_file(bucket, products_key, "processed/")
    for key in orders_keys:
        move_file(bucket, key.strip(), "processed/")
    for key in items_keys:
        move_file(bucket, key.strip(), "processed/")

    spark.stop()


if __name__ == "__main__":
    main()
