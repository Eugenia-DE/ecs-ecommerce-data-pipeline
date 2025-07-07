import boto3
import os
import sys
import json
import pandas as pd
from io import StringIO

REQUIRED_COLUMNS = {
    "products": ["id", "sku", "cost", "category", "retail_price"],
    "orders": ["order_id", "user_id", "created_at"],
    "order_items": ["order_id", "product_id", "sale_price"]
}

SAMPLE_SIZE = 100
s3 = boto3.client("s3")

def move_file(bucket, key, target_prefix):
    try:
        target_key = key.replace("raw/", target_prefix)
        s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': key}, Key=target_key)
        s3.delete_object(Bucket=bucket, Key=key)
        print(f"[INFO] Moved file s3://{bucket}/{key} -> s3://{bucket}/{target_key}")
    except Exception as e:
        print(f"[ERROR] Failed to move file {key} to {target_prefix}: {e}")

def read_csv_from_s3(bucket, key, sample=False):
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        if sample:
            return pd.read_csv(obj['Body'], nrows=SAMPLE_SIZE)
        return pd.read_csv(obj['Body'])
    except Exception as e:
        print(f"[ERROR] Failed to load s3://{bucket}/{key}: {e}")
        move_file(bucket, key, "rejected/")
        sys.exit(1)

def check_columns(df, required_columns, file_type, bucket, key):
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        print(f"[ERROR] {file_type} missing required columns: {missing}")
        move_file(bucket, key, "rejected/")
        sys.exit(1)

def check_sample_nulls(df, required_columns, file_type, bucket, key):
    nulls = df[required_columns].isnull().sum()
    if nulls.any():
        print(f"[ERROR] {file_type} contains nulls in required fields:\n{nulls[nulls > 0]}")
        move_file(bucket, key, "rejected/")
        sys.exit(1)

def main():
    bucket = os.environ.get("S3_BUCKET")
    products_key = os.environ.get("PRODUCTS_KEY")

    try:
        orders_keys = json.loads(os.environ["ORDERS_KEYS"])
        items_keys = json.loads(os.environ["ORDER_ITEMS_KEYS"])
    except Exception as e:
        print(f"[ERROR] Failed to parse keys from environment: {e}")
        sys.exit(1)

    if not all([bucket, products_key, orders_keys, items_keys]):
        print("[ERROR] Missing one or more required environment variables.")
        sys.exit(1)

    print(f"[DEBUG] Products key: {products_key}")
    print(f"[DEBUG] Orders keys: {orders_keys}")
    print(f"[DEBUG] Order items keys: {items_keys}")

    # Validate products.csv
    print("Validating products.csv...")
    df_products = read_csv_from_s3(bucket, products_key, sample=True)
    check_columns(df_products, REQUIRED_COLUMNS["products"], "products", bucket, products_key)
    check_sample_nulls(df_products, REQUIRED_COLUMNS["products"], "products", bucket, products_key)

    # Validate orders/*.csv
    print("Validating orders/*.csv...")
    for key in orders_keys:
        df_orders = read_csv_from_s3(bucket, key, sample=True)
        check_columns(df_orders, REQUIRED_COLUMNS["orders"], "orders", bucket, key)
        check_sample_nulls(df_orders, REQUIRED_COLUMNS["orders"], "orders", bucket, key)

    # Validate order_items/*.csv
    print("Validating order_items/*.csv...")
    for key in items_keys:
        df_items = read_csv_from_s3(bucket, key, sample=True)
        check_columns(df_items, REQUIRED_COLUMNS["order_items"], "order_items", bucket, key)
        check_sample_nulls(df_items, REQUIRED_COLUMNS["order_items"], "order_items", bucket, key)

    print("Validation PASSED.")
    sys.exit(0)

if __name__ == "__main__":
    main()
