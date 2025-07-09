import json
import boto3
import os
from datetime import datetime, timedelta
import re
from botocore.exceptions import ClientError

s3 = boto3.client("s3")
stepfunctions = boto3.client("stepfunctions")
dynamodb = boto3.client("dynamodb")

# Environment variables
PIPELINE_BATCH_TRACKER_TABLE = os.environ.get("PIPELINE_BATCH_TRACKER_TABLE", "PipelineBatchTracker")
STEP_FUNCTION_ARN = os.environ.get("STEP_FUNCTION_ARN")

# Define the expected S3 structure for files to be polled
RAW_S3_BUCKET = os.environ.get("RAW_S3_BUCKET") 

EXPECTED_FILE_TYPES = {
    "products": "raw/products/products.csv",
    "orders": "raw/orders/",              
    "order_items": "raw/order_items/"     
}

# Batch ID for the products master data
PRODUCTS_BATCH_ID = "products_master_data"

def get_target_date(event):
    """
    Determines the target date for which to process data.
    If 'target_date' is provided in the event, use it (for manual re-runs).
    Otherwise, default to yesterday's date (UTC).
    """
    if 'target_date' in event:
        try:
            # Validate format if provided
            datetime.strptime(event['target_date'], '%Y-%m-%d')
            return event['target_date']
        except ValueError:
            print(f"[WARN] Invalid target_date format in event: {event['target_date']}. Expected YYYY-MM-DD. Falling back to yesterday.")

    yesterday_utc = datetime.utcnow() - timedelta(days=1)
    return yesterday_utc.strftime('%Y-%m-%d')

def lambda_handler(event, context):
    print(f"Received event: {json.dumps(event, indent=2)}")

    if not STEP_FUNCTION_ARN or not PIPELINE_BATCH_TRACKER_TABLE or not RAW_S3_BUCKET:
        print("[ERROR] Environment variables (STEP_FUNCTION_ARN, PIPELINE_BATCH_TRACKER_TABLE, RAW_S3_BUCKET) not set.")
        return { 'statusCode': 500, 'body': json.dumps('Configuration error: Missing environment variables') }

    # Determine the date for the batch we are checking
    target_date_batch_id = get_target_date(event)
    print(f"[INFO] Polling for Daily Batch ID: {target_date_batch_id}")

    # --- Step 1: Scan S3 for relevant files ---
    found_files = {
        "products": None,
        "orders": [],
        "order_items": []
    }

    try:
        # Check for products master data
        try:
            s3.head_object(Bucket=RAW_S3_BUCKET, Key=EXPECTED_FILE_TYPES["products"])
            found_files["products"] = EXPECTED_FILE_TYPES["products"]
            print(f"[INFO] Found products master data: {EXPECTED_FILE_TYPES['products']}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'NotFound':
                print(f"[INFO] Products master data not found at {EXPECTED_FILE_TYPES['products']}")
            else:
                print(f"[ERROR] S3 error checking products file: {e}")
                raise # Re-raise for other S3 errors

        # List objects for orders for the target date
        orders_prefix = f"{EXPECTED_FILE_TYPES['orders']}{target_date_batch_id}/"
        orders_response = s3.list_objects_v2(Bucket=RAW_S3_BUCKET, Prefix=orders_prefix)
        if 'Contents' in orders_response:
            for obj in orders_response['Contents']:
                if obj['Key'].endswith('.csv'): # Ensure it's a CSV file
                    found_files["orders"].append(obj['Key'])
            print(f"[INFO] Found {len(found_files['orders'])} order files for {target_date_batch_id} under {orders_prefix}")

        # List objects for order_items for the target date
        order_items_prefix = f"{EXPECTED_FILE_TYPES['order_items']}{target_date_batch_id}/"
        order_items_response = s3.list_objects_v2(Bucket=RAW_S3_BUCKET, Prefix=order_items_prefix)
        if 'Contents' in order_items_response:
            for obj in order_items_response['Contents']:
                if obj['Key'].endswith('.csv'): # Ensure it's a CSV file
                    found_files["order_items"].append(obj['Key'])
            print(f"[INFO] Found {len(found_files['order_items'])} order item files for {target_date_batch_id} under {order_items_prefix}")

    except Exception as e:
        print(f"[ERROR] Error during S3 file listing: {e}")
        raise

    # --- Step 2: Update DynamoDB Batch Tracker for PRODUCTS_BATCH_ID ---
    try:
        # Update products master data item if found
        if found_files["products"]:
            dynamodb.update_item(
                TableName=PIPELINE_BATCH_TRACKER_TABLE,
                Key={'BatchId': {'S': PRODUCTS_BATCH_ID}},
                UpdateExpression="SET products_file_arrived = :true_val, products_key = :key, last_updated = :lu",
                ExpressionAttributeValues={
                    ":true_val": {"BOOL": True},
                    ":key": {"S": found_files["products"]},
                    ":lu": {"S": datetime.utcnow().isoformat()}
                }
            )
            print(f"[INFO] Updated DynamoDB for {PRODUCTS_BATCH_ID}.")
        else:
            print(f"[INFO] Products file not found, skipping DynamoDB update for {PRODUCTS_BATCH_ID}.")

        # --- Step 3: Update DynamoDB Batch Tracker for target_date_batch_id ---
        update_expression_parts = ["SET last_updated = :lu"]
        expression_attribute_values = {":lu": {"S": datetime.utcnow().isoformat()}}
        expression_attribute_names = {"#status": "status"} # for status field

        if found_files["orders"]:
            update_expression_parts.append("orders_file_arrived = :true_val")
            expression_attribute_values[":true_val"] = {"BOOL": True}
            # Replace the list of keys found on each poll (not append like event-driven)
            expression_attribute_values[":orders_keys_list"] = {"L": [{"S": k} for k in found_files["orders"]]}
            update_expression_parts.append("orders_keys = :orders_keys_list")
        else:
            update_expression_parts.append("orders_file_arrived = :false_val")
            expression_attribute_values[":false_val"] = {"BOOL": False}
            update_expression_parts.append("orders_keys = :empty_list")
            expression_attribute_values[":empty_list"] = {"L": []}

        if found_files["order_items"]:
            update_expression_parts.append("order_items_file_arrived = :true_val")
            expression_attribute_values[":true_val"] = {"BOOL": True}
            expression_attribute_values[":order_items_keys_list"] = {"L": [{"S": k} for k in found_files["order_items"]]}
            update_expression_parts.append("order_items_keys = :order_items_keys_list")
        else:
            update_expression_parts.append("order_items_file_arrived = :false_val")
            expression_attribute_values[":false_val"] = {"BOOL": False} 
            update_expression_parts.append("order_items_keys = :empty_list")
            expression_attribute_values[":empty_list"] = {"L": []}

        if found_files["orders"] or found_files["order_items"]:
             update_expression_parts.append("#status = :in_progress_status")
             expression_attribute_values[":in_progress_status"] = {"S": "IN_PROGRESS"}
        else:
            try:
                response = dynamodb.get_item(
                    TableName=PIPELINE_BATCH_TRACKER_TABLE,
                    Key={'BatchId': {'S': target_date_batch_id}},
                    ProjectionExpression="#status, step_function_triggered",
                    ExpressionAttributeNames={"#status": "status"}
                )
                current_status = response.get('Item', {}).get('status', {}).get('S')
                is_triggered = response.get('Item', {}).get('step_function_triggered', {}).get('BOOL', False)

                if not is_triggered and current_status != "NOT_TRIGGERED":
                     update_expression_parts.append("#status = :not_triggered_status")
                     expression_attribute_values[":not_triggered_status"] = {"S": "NOT_TRIGGERED"}
            except ClientError as e:
                print(f"[WARN] Failed to get current status for {target_date_batch_id}: {e}. Proceeding without status adjustment for no files found.")
                pass

        update_expression = ", ".join(update_expression_parts)

        # ConditionExpression ensures we only update if the SF hasn't been triggered yet for this batch
        dynamodb.update_item(
            TableName=PIPELINE_BATCH_TRACKER_TABLE,
            Key={'BatchId': {'S': target_date_batch_id}},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ExpressionAttributeNames=expression_attribute_names,
            ConditionExpression="attribute_not_exists(step_function_triggered) OR step_function_triggered = :false_val_check",
            ReturnValues="ALL_NEW" 
        )
        print(f"[INFO] Updated DynamoDB for Daily Batch ID {target_date_batch_id}.")

    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            print(f"[INFO] Daily Batch ID {target_date_batch_id} already triggered. Skipping update for this poll cycle.")
            # Fetch the current state since update failed due to condition check
            current_batch_item_response = dynamodb.get_item(
                TableName=PIPELINE_BATCH_TRACKER_TABLE,
                Key={'BatchId': {'S': target_date_batch_id}}
            )
            current_batch_item = current_batch_item_response.get('Item', {})
        else:
            print(f"[ERROR] DynamoDB Client Error during update for {target_date_batch_id}: {e}")
            raise # Re-raise to signal a critical error
    except Exception as e:
        print(f"[ERROR] Unexpected error updating DynamoDB for {target_date_batch_id}: {e}")
        raise

    # --- Step 4: Retrieve the latest batch state and check for completion ---
    try:
        # Get products master data status (always required)
        products_master_response = dynamodb.get_item(
            TableName=PIPELINE_BATCH_TRACKER_TABLE,
            Key={'BatchId': {'S': PRODUCTS_BATCH_ID}}
        )
        products_master_item = products_master_response.get('Item', {})
        products_master_ready = products_master_item.get('products_file_arrived', {}).get('BOOL', False)
        products_key_for_sf = products_master_item.get('products_key', {}).get('S') if products_master_ready else None

        print(f"[DEBUG] Global Products Master Data status: Ready={products_master_ready}, Key={products_key_for_sf}")

        # Get the current daily batch status
        current_daily_batch_response = dynamodb.get_item(
            TableName=PIPELINE_BATCH_TRACKER_TABLE,
            Key={'BatchId': {'S': target_date_batch_id}}
        )
        current_daily_batch_item = current_daily_batch_response.get('Item', {})

        orders_arrived = current_daily_batch_item.get('orders_file_arrived', {}).get('BOOL', False)
        order_items_arrived = current_daily_batch_item.get('order_items_file_arrived', {}).get('BOOL', False)
        step_function_already_triggered = current_daily_batch_item.get('step_function_triggered', {}).get('BOOL', False)

        print(f"[DEBUG] Daily Batch {target_date_batch_id} status: Orders Found={orders_arrived}, Order Items Found={order_items_arrived}, SF Triggered={step_function_already_triggered}")

        # Check if all required files for the daily batch have been found
        if orders_arrived and order_items_arrived and products_master_ready and not step_function_already_triggered:
            print(f"[INFO] All required files for Daily Batch ID {target_date_batch_id} and Products Master Data are ready. Preparing to trigger Step Function.")

            # Retrieve all collected keys for the payload
            orders_keys_for_sf = [item['S'] for item in current_daily_batch_item.get('orders_keys', {}).get('L', [])]
            order_items_keys_for_sf = [item['S'] for item in current_daily_batch_item.get('order_items_keys', {}).get('L', [])]

            # Construct payload with all necessary keys
            input_payload = {
                "bucket": RAW_S3_BUCKET,
                "products_key": products_key_for_sf,
                "orders_keys": orders_keys_for_sf,
                "order_items_keys": order_items_keys_for_sf,
                "batch_id": target_date_batch_id # Pass batch ID to Step Function for consistent tracking
            }

            print("[INFO] Triggering Step Function with input:")
            print(json.dumps(input_payload, indent=2))

            # --- Step 5: Trigger Step Function ---
            stepfunctions.start_execution(
                stateMachineArn=STEP_FUNCTION_ARN,
                input=json.dumps(input_payload),
                name=f"ecommerce-kpi-pipeline-{target_date_batch_id}-{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}" # Unique name
            )
            print(f"[INFO] Step Function triggered for Daily Batch ID {target_date_batch_id}.")

            # --- Step 6: Atomically mark daily batch as triggered in DynamoDB ---
            dynamodb.update_item(
                TableName=PIPELINE_BATCH_TRACKER_TABLE,
                Key={'BatchId': {'S': target_date_batch_id}},
                UpdateExpression="SET step_function_triggered = :true_val, #s = :triggered_status",
                ExpressionAttributeValues={
                    ":true_val": {"BOOL": True},
                    ":triggered_status": {"S": "TRIGGERED"},
                    ":false_val_check": {"BOOL": False} 
                },
                ExpressionAttributeNames={
                    "#s": "status"
                },
                ConditionExpression="step_function_triggered = :false_val_check OR attribute_not_exists(step_function_triggered)",
                ReturnValues="UPDATED_NEW"
            )
            print(f"[INFO] Marked Daily Batch ID {target_date_batch_id} as triggered in DynamoDB.")
            
            return { 'statusCode': 200, 'body': json.dumps(f'Step Function triggered for Batch ID {target_date_batch_id}') }
        else:
            print(f"[INFO] Daily Batch ID {target_date_batch_id} not yet complete, or Products Master Data not ready, or already triggered. Waiting for next poll cycle.")
            return { 'statusCode': 200, 'body': json.dumps(f'Batch {target_date_batch_id} incomplete or already triggered.') }

    except ClientError as e:
        print(f"[ERROR] DynamoDB Client Error during get/update for {target_date_batch_id}: {e}")
        raise
    except Exception as e:
        print(f"[ERROR] Unexpected error during batch completion check or SF trigger for {target_date_batch_id}: {e}")
        raise