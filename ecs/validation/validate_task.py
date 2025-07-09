import boto3
import os
import sys
import json
import pandas as pd
from io import StringIO
from botocore.exceptions import ClientError
from datetime import datetime

# Global list to store log messages for S3 upload
script_logs = []

# Define required columns for each file type
REQUIRED_COLUMNS = {
    "products": ["id", "sku", "cost", "category", "retail_price"],
    "orders": ["order_id", "user_id", "created_at"],
    "order_items": ["order_id", "product_id", "sale_price"]
}

# Define critical columns for null checks (subset of required columns)
CRITICAL_NULL_CHECK_COLUMNS = {
    "products": ["id", "sku", "cost", "category", "retail_price"],
    "orders": ["order_id", "user_id", "created_at"],
    "order_items": ["order_id", "product_id", "sale_price"]
}

# Define sample size for initial data checks (e.g., header and initial nulls)
SAMPLE_SIZE = 100
# Initialize S3 client
s3 = boto3.client("s3")

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
        s3.put_object(Bucket=bucket, Key=log_key, Body=log_content)
        log_message("INFO", f"Successfully uploaded logs to s3://{bucket}/{log_key}")
    except Exception as e:
        log_message("ERROR", f"Failed to upload logs to s3://{bucket}/{log_key}: {e}")


def move_file(bucket, key, destination_prefix, reason=None):
    """
    Moves a file from its current S3 key (expected to be in 'raw/' prefix)
    to a new S3 key under the specified destination_prefix.
    This function is idempotent: it handles cases where the source file might
    no longer exist (e.g., already moved by a previous successful run).
    If a reason is provided, a corresponding JSON file is uploaded.

    Args:
        bucket (str): The name of the S3 bucket.
        key (str): The current key (path) of the file in S3 (e.g., "raw/orders/order1.csv").
        destination_prefix (str): The new prefix (folder) where the file should be moved
                                  (e.g., "validated/" or "invalid/").
        reason (str, optional): The reason for moving the file, especially for 'invalid/' moves.
    """
    source_key = key
    
    try:
        # Check if the source object exists before attempting to copy.
        s3.head_object(Bucket=bucket, Key=source_key)

        # Determine the relative path to maintain folder structure
        # Find the part of the key after the initial 'raw/' or 'validated/'
        parts = source_key.split('/')
        if parts[0] == 'raw' or parts[0] == 'validated':
            relative_path = '/'.join(parts[1:])
        else:
            relative_path = source_key # Fallback if no known prefix

        target_key = os.path.join(destination_prefix, relative_path)

        # Copy the object to the new location
        s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': source_key}, Key=target_key)
        # Delete the original object from the source location
        s3.delete_object(Bucket=bucket, Key=source_key)
        log_message("INFO", f"Moved file s3://{bucket}/{source_key} -> s3://{bucket}/{target_key}")

        if reason and destination_prefix == "invalid/":
            reason_key = target_key.replace(".csv", "_reason.json")
            reason_content = json.dumps({
                "original_key": source_key,
                "rejected_key": target_key,
                "reason": reason,
                "timestamp": datetime.utcnow().isoformat()
            }, indent=2)
            s3.put_object(Bucket=bucket, Key=reason_key, Body=reason_content, ContentType="application/json")
            log_message("INFO", f"Uploaded rejection reason to s3://{bucket}/{reason_key}")

    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            log_message("INFO", f"File s3://{bucket}/{source_key} not found. It might have already been moved or does not exist.")
        else:
            log_message("ERROR", f"AWS Client Error moving file {source_key} to {destination_prefix}: {e}")
            raise # Re-raise other AWS ClientErrors
    except Exception as e:
        log_message("ERROR", f"Failed to move file {source_key} to {destination_prefix}: {e}")
        raise # Re-raise to ensure the task fails on unexpected errors

def read_csv_from_s3(bucket, key, file_type, sample=False):
    """
    Reads a CSV file from S3 into a Pandas DataFrame.
    If reading fails, logs an error and returns None.
    """
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        csv_content = obj['Body'].read().decode('utf-8')
        if sample:
            df = pd.read_csv(StringIO(csv_content), nrows=SAMPLE_SIZE)
        else:
            df = pd.read_csv(StringIO(csv_content))
        log_message("INFO", f"Successfully loaded {file_type} file: s3://{bucket}/{key} (sample={sample})")
        return df
    except Exception as e:
        log_message("ERROR", f"Failed to load {file_type} file s3://{bucket}/{key}: {e}")
        return None # Return None on failure

def validate_schema_and_nulls(df, required_cols, critical_null_cols, file_type, bucket, key):
    """
    Performs schema conformity (column presence) and critical null checks on a DataFrame.
    If checks fail, moves the file to 'invalid/' and exits.

    Args:
        df (pandas.DataFrame): The DataFrame to check.
        required_cols (list): List of column names that must be present.
        critical_null_cols (list): List of column names to check for nulls.
        file_type (str): Type of file for logging.
        bucket (str): S3 bucket name.
        key (str): S3 key of the file.
    """
    # 1. Schema Conformity Check
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        reason = f"Missing required columns: {', '.join(missing_cols)}"
        log_message("ERROR", f"{file_type} file s3://{bucket}/{key} failed schema conformity: {reason}")
        move_file(bucket, key, "invalid/", reason=reason)
        sys.exit(1)

    # 2. Missing/Null Fields Check (on relevant columns in the loaded sample)
    # Ensure critical_null_cols are actually present in the DataFrame before checking
    cols_to_check = [col for col in critical_null_cols if col in df.columns]
    
    if not cols_to_check:
        log_message("WARN", f"No critical null check columns found in {file_type} file {key}. Skipping null check.")
    else:
        null_counts = df[cols_to_check].isnull().sum()
        columns_with_nulls = null_counts[null_counts > 0]

        if not columns_with_nulls.empty:
            reason = f"Contains null values in critical fields: {columns_with_nulls.to_dict()}"
            log_message("ERROR", f"{file_type} file s3://{bucket}/{key} failed null check on sample: {reason}")
            move_file(bucket, key, "invalid/", reason=reason)
            sys.exit(1)
        else:
            log_message("INFO", f"Schema and sample null checks passed for {file_type} file: s3://{bucket}/{key}")

def check_referential_integrity(df_orders, df_products, df_order_items, bucket, all_raw_keys):
    """
    Checks referential integrity between order_items, orders, and products DataFrames.
    - order_id in order_items must exist in orders.
    - product_id in order_items must exist in products.
    If integrity checks fail, logs an error, moves all raw files for the batch to 'invalid/', and exits.
    """
    log_message("INFO", "--- Starting Referential Integrity Checks ---")

    validation_failed = False
    rejection_reasons = []

    # Check 1: order_id in order_items must exist in orders
    # Use .isin() for efficient lookup
    missing_order_ids_in_orders = df_order_items[~df_order_items['order_id'].isin(df_orders['order_id'])]['order_id'].unique()
    if len(missing_order_ids_in_orders) > 0:
        reason = f"Order IDs in order_items not found in orders: {missing_order_ids_in_orders[:5].tolist()}"
        log_message("ERROR", f"Referential Integrity Error: {reason}")
        rejection_reasons.append(reason)
        validation_failed = True

    # Check 2: product_id in order_items must exist in products
    # Use .isin() for efficient lookup
    missing_product_ids_in_products = df_order_items[~df_order_items['product_id'].isin(df_products['id'])]['product_id'].unique()
    if len(missing_product_ids_in_products) > 0:
        reason = f"Product IDs in order_items not found in products: {missing_product_ids_in_products[:5].tolist()}"
        log_message("ERROR", f"Referential Integrity Error: {reason}")
        rejection_reasons.append(reason)
        validation_failed = True

    if validation_failed:
        full_reason = "Referential integrity checks failed: " + " | ".join(rejection_reasons)
        log_message("ERROR", f"Referential integrity checks failed for the batch: {full_reason}. Moving all raw files to 'invalid/'.")
        # Move all files related to this batch to invalid/
        for key in all_raw_keys:
            move_file(bucket, key, "invalid/", reason=full_reason)
        sys.exit(1)
    else:
        log_message("INFO", "Referential integrity checks passed successfully.")

def main():
    """
    Main function to orchestrate the data validation process.
    It retrieves S3 paths from environment variables, performs validation checks,
    and moves files to 'validated/' or 'invalid/' accordingly.
    """
    bucket = os.environ.get("S3_BUCKET")
    products_key = os.environ.get("PRODUCTS_KEY")

    try:
        # Environment variables for keys are expected to be JSON strings (lists of strings)
        orders_keys = json.loads(os.environ.get("ORDERS_KEYS", "[]"))
        items_keys = json.loads(os.environ.get("ORDER_ITEMS_KEYS", "[]"))
    except json.JSONDecodeError as e:
        log_message("ERROR", f"Failed to parse ORDERS_KEYS or ORDER_ITEMS_KEYS from environment (not valid JSON): {e}")
        sys.exit(1)
    except Exception as e:
        log_message("ERROR", f"Unexpected error parsing keys from environment: {e}")
        sys.exit(1)

    # Validate that all necessary environment variables are provided
    if not all([bucket, products_key, orders_keys, items_keys]):
        log_message("ERROR", "Missing one or more required environment variables (S3_BUCKET, PRODUCTS_KEY, ORDERS_KEYS, ORDER_ITEMS_KEYS).")
        sys.exit(1)

    log_message("DEBUG", f"S3 Bucket: {bucket}")
    log_message("DEBUG", f"Products key: {products_key}")
    log_message("DEBUG", f"Orders keys: {orders_keys}")
    log_message("DEBUG", f"Order items keys: {items_keys}")

    # --- Individual File Validation (Schema & Sample Nulls) ---
    # Load products file (full for referential integrity later)
    log_message("INFO", "\n--- Validating products.csv (Schema & Sample Nulls) ---")
    df_products = read_csv_from_s3(bucket, products_key, "products", sample=False) # Read full for RI
    if df_products is None: # Check if read_csv_from_s3 failed
        sys.exit(1) # Exit if products file couldn't be loaded
    validate_schema_and_nulls(df_products, REQUIRED_COLUMNS["products"], CRITICAL_NULL_CHECK_COLUMNS["products"], "products", bucket, products_key)
    log_message("INFO", f"Products file s3://{bucket}/{products_key} passed individual validation checks.")

    # Load and validate all orders files
    log_message("INFO", "\n--- Validating orders/*.csv (Schema & Sample Nulls) ---")
    all_orders_dfs = []
    for key in orders_keys:
        df_orders_part = read_csv_from_s3(bucket, key, "orders", sample=False)
        if df_orders_part is None:
            sys.exit(1) # Exit if any orders file couldn't be loaded
        validate_schema_and_nulls(df_orders_part, REQUIRED_COLUMNS["orders"], CRITICAL_NULL_CHECK_COLUMNS["orders"], "orders", bucket, key)
        log_message("INFO", f"Orders file s3://{bucket}/{key} passed individual validation checks.")
        all_orders_dfs.append(df_orders_part)
    
    # Concatenate all orders parts into a single DataFrame for referential integrity
    df_orders_full = pd.concat(all_orders_dfs, ignore_index=True) if all_orders_dfs else pd.DataFrame(columns=REQUIRED_COLUMNS["orders"])
    log_message("INFO", f"Consolidated {len(all_orders_dfs)} orders files into a single DataFrame for referential integrity.")

    # Load and validate all order_items files
    log_message("INFO", "\n--- Validating order_items/*.csv (Schema & Sample Nulls) ---")
    all_items_dfs = []
    for key in items_keys:
        df_items_part = read_csv_from_s3(bucket, key, "order_items", sample=False) 
        if df_items_part is None:
            sys.exit(1) # Exit if any order_items file couldn't be loaded
        validate_schema_and_nulls(df_items_part, REQUIRED_COLUMNS["order_items"], CRITICAL_NULL_CHECK_COLUMNS["order_items"], "order_items", bucket, key)
        log_message("INFO", f"Order items file s3://{bucket}/{key} passed individual validation checks.")
        all_items_dfs.append(df_items_part)

    # Concatenate all order_items parts into a single DataFrame for referential integrity
    df_order_items_full = pd.concat(all_items_dfs, ignore_index=True) if all_items_dfs else pd.DataFrame(columns=REQUIRED_COLUMNS["order_items"])
    log_message("INFO", f"Consolidated {len(all_items_dfs)} order_items files into a single DataFrame for referential integrity.")

    # --- Referential Integrity Check (Batch-level) ---
    # Collect all original raw keys to move to invalid/ if RI fails
    all_raw_keys_for_batch = [products_key] + orders_keys + items_keys
    check_referential_integrity(df_orders_full, df_products, df_order_items_full, bucket, all_raw_keys_for_batch)

    # --- Move Validated Files to 'validated/' Folder ---
    log_message("INFO", "\n--- All validations passed. Moving files from 'raw/' to 'validated/' folder ---")
    
    # Move products file
    move_file(bucket, products_key, "validated/")
    # Move orders files
    for key in orders_keys:
        move_file(bucket, key, "validated/")
    # Move order_items files
    for key in items_keys:
        move_file(bucket, key, "validated/")

    log_message("INFO", "\nValidation PASSED for all files and referential integrity.")
    sys.exit(0) # Exit with success code

if __name__ == "__main__":
    try:
        main()
    finally:
        # Ensure logs are written to S3 even if an unhandled exception occurs
        bucket = os.environ.get("S3_BUCKET")
        if bucket: # Only attempt to write logs if bucket is defined
            write_logs_to_s3(bucket, "validation")
        else:
            print("[ERROR] S3_BUCKET environment variable not set, cannot write logs to S3.")