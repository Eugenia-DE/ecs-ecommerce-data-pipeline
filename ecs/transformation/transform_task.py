import os
import sys
import boto3
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, avg, countDistinct, count, when, to_date
)
from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame

s3_client = boto3.client("s3")


def move_file(bucket, key, target_prefix):
    try:
        target_key = key.replace("raw/", target_prefix)
        s3_client.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": key}, Key=target_key)
        s3_client.delete_object(Bucket=bucket, Key=key)
        print(f"[INFO] Moved file s3://{bucket}/{key} -> s3://{bucket}/{target_key}")
    except Exception as e:
        print(f"[ERROR] Failed to move file: {e}")


def load_csv_spark(spark: SparkSession, bucket: str, key: str) -> DataFrame:
    path = f"s3a://{bucket}/{key}"
    return spark.read.option("header", "true").csv(path)


def compute_kpis_spark(df_products: DataFrame, df_orders: DataFrame, df_items: DataFrame):
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

    run_date = datetime.utcnow().strftime("%Y-%m-%d")

    # Delta-aware SparkSession
    builder = SparkSession.builder.appName("KPITransformation") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Load input data
    df_products = load_csv_spark(spark, bucket, products_key)

    order_paths = [f"s3a://{bucket}/{key.strip()}" for key in orders_keys]
    df_orders = spark.read.option("header", "true").csv(*order_paths)

    item_paths = [f"s3a://{bucket}/{key.strip()}" for key in items_keys]
    df_items = spark.read.option("header", "true").csv(*item_paths)

    # Compute KPIs
    category_kpi, order_kpi = compute_kpis_spark(df_products, df_orders, df_items)

    print("\n[INFO] Category-Level KPIs")
    category_kpi.show(5, truncate=False)

    print("\n[INFO] Order-Level KPIs")
    order_kpi.show(5, truncate=False)

    # Add run_date column for partitioning
    category_kpi = category_kpi.withColumn("run_date", to_date(lit(run_date)))
    order_kpi = order_kpi.withColumn("run_date", to_date(lit(run_date)))

    # Save KPIs partitioned by run_date
    category_kpi.write.format("delta").mode("overwrite") \
        .partitionBy("run_date") \
        .save(f"s3a://{bucket}/processed/kpis/category/")

    order_kpi.write.format("delta").mode("overwrite") \
        .partitionBy("run_date") \
        .save(f"s3a://{bucket}/processed/kpis/order/")

    # Move raw input files to processed/
    move_file(bucket, products_key, "processed/")
    for key in orders_keys:
        move_file(bucket, key.strip(), "processed/")
    for key in items_keys:
        move_file(bucket, key.strip(), "processed/")

    spark.stop()


if __name__ == "__main__":
    main()
