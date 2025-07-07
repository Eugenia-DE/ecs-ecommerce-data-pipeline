import boto3
import os
import sys
import pandas as pd
from io import StringIO

REQUIRED_COLUMNS = {
    "products": ["id", "sku", "cost", "category", "retail_price"],
    "orders": ["order_id", "user_id", "created_at"],
    "order_items": ["order_id", "product_id", "sale_price"]
}

SAMPLE_SIZE = 100
s3 = boto3.client("s3")

def read_csv_from_s3(bucket, key, sample=False):
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        if sample:
            return pd.read_csv(obj['Body'], nrows=SAMPLE_SIZE)
        return pd.read_csv(obj['Body'])
    except Exception as e:
        print(f"[ERROR] Failed to load s3://{bucket}/{key}: {e}")
        sys.exit(1)

def check_columns(df, required_columns, file_type):
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        print(f"[ERROR] {file_type} missing required columns: {missing}")
        sys.exit(1)

def check_sample_nulls(df, required_columns, file_type):
    nulls = df[required_columns].isnull().sum()
    if nulls.any():
        print(f"[ERROR] {file_type} contains nulls in required fields:\n{nulls[nulls > 0]}")
        sys.exit(1)

def main():
    bucket = os.environ.get("S3_BUCKET")
    products_key = os.environ.get("PRODUCTS_KEY")
    orders_keys = os.environ.get("ORDERS_KEYS", "").split(",")
    items_keys = os.environ.get("ORDER_ITEMS_KEYS", "").split(",")

    if not all([bucket, products_key, orders_keys, items_keys]):
        print("[ERROR] Missing one or more required environment variables.")
        sys.exit(1)

    print("Validating products.csv...")
    df_products = read_csv_from_s3(bucket, products_key, sample=True)
    check_columns(df_products, REQUIRED_COLUMNS["products"], "products")
    check_sample_nulls(df_products, REQUIRED_COLUMNS["products"], "products")

    print("Validating orders/*.csv...")
    df_orders = pd.concat(
        [read_csv_from_s3(bucket, key.strip(), sample=True) for key in orders_keys],
        ignore_index=True
    )
    check_columns(df_orders, REQUIRED_COLUMNS["orders"], "orders")
    check_sample_nulls(df_orders, REQUIRED_COLUMNS["orders"], "orders")

    print("Validating order_items/*.csv...")
    df_items = pd.concat(
        [read_csv_from_s3(bucket, key.strip(), sample=True) for key in items_keys],
        ignore_index=True
    )
    check_columns(df_items, REQUIRED_COLUMNS["order_items"], "order_items")
    check_sample_nulls(df_items, REQUIRED_COLUMNS["order_items"], "order_items")

    print("Validation PASSED.")
    sys.exit(0)

if __name__ == "__main__":
    main()
