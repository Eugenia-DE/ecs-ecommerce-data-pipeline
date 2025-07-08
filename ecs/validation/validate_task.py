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

# Define sample size for initial data checks
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


def move_file(bucket, key, destination_prefix):
    """
    Moves a file from its current S3 key (expected to be in 'raw/' prefix)
    to a new S3 key under the specified destination_prefix.
    This function is idempotent: it handles cases where the source file might
    no longer exist (e.g., already moved by a previous successful run).

    Args:
        bucket (str): The name of the S3 bucket.
        key (str): The current key (path) of the file in S3 (e.g., "raw/orders/order1.csv").
        destination_prefix (str): The new prefix (folder) where the file should be moved
                                  (e.g., "validated/" or "invalid/").
    """
    source_key = key
    
    try:
        # Check if the source object exists before attempting to copy.
        # If it doesn't exist, a ClientError with 'NoSuchKey' code will be raised.
        s3.head_object(Bucket=bucket, Key=source_key)

        relative_path = source_key.replace("raw/", "")
        target_key = os.path.join(destination_prefix, relative_path)

        # Copy the object to the new location
        s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': source_key}, Key=target_key)
        # Delete the original object from the source location
        s3.delete_object(Bucket=bucket, Key=source_key)
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

def read_csv_from_s3(bucket, key, file_type, sample=False):
    """
    Reads a CSV file from S3 into a Pandas DataFrame.
    If reading fails, the file is moved to the 'invalid/' folder and the script exits.

    Args:
        bucket (str): The name of the S3 bucket.
        key (str): The key (path) of the CSV file in S3.
        file_type (str): A string indicating the type of file (e.g., "products", "orders").
                         Used for logging and error messages.
        sample (bool): If True, reads only a sample of rows (defined by SAMPLE_SIZE).

    Returns:
        pandas.DataFrame: The DataFrame loaded from the CSV.
    """
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        # Decode the body content from bytes to string before passing to StringIO
        csv_content = obj['Body'].read().decode('utf-8')
        if sample:
            df = pd.read_csv(StringIO(csv_content), nrows=SAMPLE_SIZE)
        else:
            df = pd.read_csv(StringIO(csv_content))
        log_message("INFO", f"Successfully loaded {file_type} file: s3://{bucket}/{key}")
        return df
    except Exception as e:
        log_message("ERROR", f"Failed to load {file_type} file s3://{bucket}/{key}: {e}")
        # Move the problematic file to the 'invalid/' folder
        move_file(bucket, key, "invalid/")
        sys.exit(1) # Exit with error code to signal failure in Step Functions

def check_columns(df, required_columns, file_type, bucket, key):
    """
    Checks if all required columns are present in the DataFrame.
    If missing, the file is moved to 'invalid/' and the script exits.

    Args:
        df (pandas.DataFrame): The DataFrame to check.
        required_columns (list): A list of column names that must be present.
        file_type (str): Type of file for logging.
        bucket (str): S3 bucket name.
        key (str): S3 key of the file.
    """
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        log_message("ERROR", f"{file_type} file s3://{bucket}/{key} missing required columns: {missing}")
        # Move the problematic file to the 'invalid/' folder
        move_file(bucket, key, "invalid/")
        sys.exit(1) # Exit with error code

def check_sample_nulls(df, required_columns, file_type, bucket, key):
    """
    Checks for null values in required columns within the DataFrame sample.
    If nulls are found, the file is moved to 'invalid/' and the script exits.

    Args:
        df (pandas.DataFrame): The DataFrame sample to check.
        required_columns (list): A list of column names to check for nulls.
        file_type (str): Type of file for logging.
        bucket (str): S3 bucket name.
        key (str): S3 key of the file.
    """
    # Select only the required columns and check for nulls
    null_counts = df[required_columns].isnull().sum()
    # Filter for columns that have at least one null value
    columns_with_nulls = null_counts[null_counts > 0]

    if not columns_with_nulls.empty:
        log_message("ERROR", f"{file_type} file s3://{bucket}/{key} contains nulls in required fields:\n{columns_with_nulls}")
        # Move the problematic file to the 'invalid/' folder
        move_file(bucket, key, "invalid/")
        sys.exit(1) # Exit with error code
    else:
        log_message("INFO", f"No nulls found in required columns for {file_type} file: s3://{bucket}/{key}")


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

    # --- Validation Steps ---

    log_message("INFO", "\n--- Validating products.csv ---")
    df_products = read_csv_from_s3(bucket, products_key, "products", sample=True)
    check_columns(df_products, REQUIRED_COLUMNS["products"], "products", bucket, products_key)
    check_sample_nulls(df_products, REQUIRED_COLUMNS["products"], "products", bucket, products_key)
    log_message("INFO", f"Products file s3://{bucket}/{products_key} passed validation checks.")

    log_message("INFO", "\n--- Validating orders/*.csv ---")
    for key in orders_keys:
        df_orders = read_csv_from_s3(bucket, key, "orders", sample=True)
        check_columns(df_orders, REQUIRED_COLUMNS["orders"], "orders", bucket, key)
        check_sample_nulls(df_orders, REQUIRED_COLUMNS["orders"], "orders", bucket, key)
        log_message("INFO", f"Orders file s3://{bucket}/{key} passed validation checks.")

    log_message("INFO", "\n--- Validating order_items/*.csv ---")
    for key in items_keys:
        df_items = read_csv_from_s3(bucket, key, "order_items", sample=True)
        check_columns(df_items, REQUIRED_COLUMNS["order_items"], "order_items", bucket, key)
        check_sample_nulls(df_items, REQUIRED_COLUMNS["order_items"], "order_items", bucket, key)
        log_message("INFO", f"Order items file s3://{bucket}/{key} passed validation checks.")

    # --- Move Validated Files to 'validated/' Folder ---
    log_message("INFO", "\n--- All validations passed. Moving files to 'validated/' folder ---")
    # Move products file
    move_file(bucket, products_key, "validated/")
    # Move orders files
    for key in orders_keys:
        move_file(bucket, key, "validated/")
    # Move order_items files
    for key in items_keys:
        move_file(bucket, key, "validated/")

    log_message("INFO", "\nValidation PASSED for all files.")
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

