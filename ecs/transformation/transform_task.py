import os
import sys
import boto3
import pandas as pd
from io import StringIO

s3 = boto3.client("s3")

def move_file(bucket, key, target_prefix):
    try:
        target_key = key.replace("raw/", target_prefix)
        s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': key}, Key=target_key)
        s3.delete_object(Bucket=bucket, Key=key)
        print(f"[INFO] Moved file s3://{bucket}/{key} -> s3://{bucket}/{target_key}")
    except Exception as e:
        print(f"[ERROR] Failed to move file {key} to {target_prefix}: {e}")

def read_csv_from_s3(bucket, key):
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(obj['Body'])
    except Exception as e:
        print(f"[ERROR] Could not read {key}: {e}")
        sys.exit(1)

def deep_null_check(df, required_columns, file_type):
    nulls = df[required_columns].isnull().sum()
    if nulls.any():
        print(f"[ERROR] {file_type} contains nulls in:\n{nulls[nulls > 0]}")
        sys.exit(1)

def validate_referential(df_products, df_orders, df_items):
    df_products = df_products.rename(columns={"id": "product_id"})
    missing_products = df_items[~df_items["product_id"].isin(df_products["product_id"])]
    missing_orders = df_items[~df_items["order_id"].isin(df_orders["order_id"])]

    if not missing_products.empty:
        print(f"[ERROR] {len(missing_products)} order_items rows have invalid product_id.")
        sys.exit(1)
    if not missing_orders.empty:
        print(f"[ERROR] {len(missing_orders)} order_items rows have invalid order_id.")
        sys.exit(1)

def compute_kpis(df_products, df_orders, df_items):
    df_orders["order_date"] = pd.to_datetime(df_orders["created_at"]).dt.date
    df_items = df_items.merge(df_products[["product_id", "category"]], on="product_id", how="left")
    df_items = df_items.merge(df_orders[["order_id", "order_date", "user_id", "returned_at"]], on="order_id", how="left")
    df_items["returned"] = df_items["returned_at"].notnull().astype(int)

    category_kpi = df_items.groupby(["category", "order_date"]).agg(
        daily_revenue=("sale_price", "sum"),
        avg_order_value=("sale_price", "mean"),
        avg_return_rate=("returned", "mean")
    ).reset_index()

    df_orders["returned"] = df_orders["returned_at"].notnull().astype(int)
    order_kpi = df_orders.groupby("order_date").agg(
        total_orders=("order_id", "nunique"),
        total_revenue=("order_id", lambda x: df_items[df_items["order_id"].isin(x)]["sale_price"].sum()),
        total_items_sold=("order_id", lambda x: df_items[df_items["order_id"].isin(x)].shape[0]),
        return_rate=("returned", "mean"),
        unique_customers=("user_id", "nunique")
    ).reset_index()

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

    df_products = read_csv_from_s3(bucket, products_key)
    df_orders = pd.concat([read_csv_from_s3(bucket, key.strip()) for key in orders_keys], ignore_index=True)
    df_items = pd.concat([read_csv_from_s3(bucket, key.strip()) for key in items_keys], ignore_index=True)

    deep_null_check(df_products, ["id", "sku", "cost", "category", "retail_price"], "products")
    deep_null_check(df_orders, ["order_id", "user_id", "created_at"], "orders")
    deep_null_check(df_items, ["order_id", "product_id", "sale_price"], "order_items")

    validate_referential(df_products, df_orders, df_items)

    category_kpi, order_kpi = compute_kpis(df_products, df_orders, df_items)

    print("\n[INFO] Category-Level KPIs")
    print(category_kpi.head())

    print("\n[INFO] Order-Level KPIs")
    print(order_kpi.head())

    # After successful processing, move all files from raw/ to processed/
    move_file(bucket, products_key, "processed/")
    for key in orders_keys:
        move_file(bucket, key.strip(), "processed/")
    for key in items_keys:
        move_file(bucket, key.strip(), "processed/")

    sys.exit(0)

if __name__ == "__main__":
    main()
