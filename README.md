### Real-Time Event-Driven Data Pipeline for E-Commerce Analytics
This project details the design and implementation of a robust, real-time, event-driven data pipeline for an e-commerce platform. It leverages a suite of AWS-native services in a containerized environment to automate the ingestion, validation, transformation, and storage of transactional data for operational analytics.

### Project Objective
The primary objective is to establish an automated, scalable data pipeline that processes incoming e-commerce data files (products, orders, order items) in near real-time. Key goals include:

Automated Ingestion: Detect new data files arriving in Amazon S3.

Data Quality Assurance: Validate incoming data for structural integrity and completeness using containerized services.

KPI Generation: Transform validated raw data into actionable business Key Performance Indicators (KPIs).

Real-time Accessibility: Store computed KPIs in Amazon DynamoDB for low-latency querying and operational dashboards.

Workflow Orchestration: Automate the entire data flow, including error handling and retries, using AWS Step Functions.

Operational Visibility: Implement comprehensive logging and alerting mechanisms.

### Architecture Overview
The pipeline operates on an event-driven paradigm, initiated by new file uploads to S3.

+----------------+       +---------------------+       +---------------------------+
| Amazon S3      |       | AWS Lambda          |       | AWS Step Functions        |
| (raw/ prefix)  | ----> | (S3 Event Trigger)  | ----> | (Workflow Orchestration)  |
|                |       | (Batch Coordination)|       |                           |
+----------------+       +----------^----------+       +------------|--------------+
                                   |                                 |
                                   | (State Tracking)                |
                                   |                                 |
                          +--------+--------+                        |
                          | Amazon DynamoDB |                        |
                          | (Batch Tracker) |                        |
                          +-----------------+                        |
                                                                     |
                                                                     v
                                                          +---------------------+
                                                          | ECS Fargate Task    |
                                                          | (Data Validation)   |
                                                          +---------------------+
                                                                     |
                                                                     v
                                                          +---------------------+
                                                          | ECS Fargate Task    |
                                                          | (KPI Transformation)|
                                                          +---------------------+
                                                                     |
                                                                     v
                                                          +---------------------+
                                                          | Amazon DynamoDB     |
                                                          | (CategoryKPIs,      |
                                                          |  DailyKPIs)         |
                                                          +---------------------+

### Core AWS Services Utilized
Service

Purpose

Amazon S3

Scalable object storage for raw, validated, invalid, and processed files.

AWS Lambda

Serverless compute for S3 event handling and intelligent batch coordination.

Amazon ECS/Fargate

Container orchestration for running data validation and transformation tasks without server management.

AWS Step Functions

State machine for orchestrating the multi-step workflow, managing state, retries, and error paths.

Amazon DynamoDB

High-performance NoSQL database for real-time KPI storage and batch metadata tracking.

Amazon CloudWatch

Centralized logging and monitoring for all pipeline components.

Amazon SNS

Notification service for critical pipeline failures.

### Data Format
Incoming data files are expected in CSV format and are uploaded to the raw/ prefix within the designated S3 bucket.

S3 File Structure and Naming Convention
To support robust batching and incremental processing, orders and order_items files are expected to be organized into date-partitioned subfolders.

Products (Static Master Data):
s3://[YOUR_BUCKET_NAME]/raw/products/products.csv

Orders (Daily Batches):
s3://[YOUR_BUCKET_NAME]/raw/orders/YYYY-MM-DD/orders_partX.csv
(e.g., raw/orders/2025-07-09/orders_part1.csv)

Order Items (Daily Batches):
s3://[YOUR_BUCKET_NAME]/raw/order_items/YYYY-MM-DD/order_items_partX.csv
(e.g., raw/order_items/2025-07-09/order_items_part1.csv)

Note: The Lambda trigger is designed to automatically assign the current UTC date as the batch ID if orders or order_items files are uploaded without the YYYY-MM-DD folder structure (e.g., directly into raw/orders/). While this provides flexibility, adhering to the date-partitioned structure is recommended for clearer data organization and historical traceability.

Required Columns per Dataset
Dataset

Required Columns

products

id, sku, cost, category, retail_price

orders

order_id, user_id, created_at

order_items

order_id, product_id, sale_price

### Key Performance Indicators (KPIs)
The transformation logic computes two primary types of KPIs, stored in dedicated DynamoDB tables.

### Category-Level KPIs (Stored in CategoryKPIs table)
These KPIs provide daily insights into product category performance.

Field

Description

DynamoDB Key/Type

category_id

Product category (e.g., "Electronics")

Partition Key (String)

date_key

Date of the summarized orders (YYYY-MM-DD)

Sort Key (String)

daily_revenue

Total revenue from that category for the day

Attribute (Number)

avg_order_value

Average value of individual orders in the category

Attribute (Number)

avg_return_rate

Percentage of returned orders for the category

Attribute (Number)

### Order-Level KPIs (Stored in DailyKPIs table)
These KPIs provide a summarized view of overall daily order activity.

Field

Description

DynamoDB Key/Type

date_key

Date of the summarized orders (YYYY-MM-DD)

Partition Key (String)

total_orders

Count of unique orders

Attribute (Number)

total_revenue

Total revenue from all orders

Attribute (Number)

total_items_sold

Total number of items sold

Attribute (Number)

return_rate

Percentage of orders that were returned

Attribute (Number)

unique_customers

Number of distinct customers who placed orders

Attribute (Number)

### Pipeline Components and Flow
5.1. S3 Event Trigger & Batch Coordination (AWS Lambda & DynamoDB)
Mechanism: An S3 event notification (e.g., s3:ObjectCreated:Put) on the raw/ prefix triggers an AWS Lambda function (trigger-ecommerce-kpi-pipeline).

Batch Tracking: This Lambda function acts as a coordinator. It uses a dedicated DynamoDB table (BatchFileTracker) to atomically track the arrival of products.csv (as a products_master_data batch) and all parts of orders and order_items for a specific date (e.g., 2025-07-09).

BatchFileTracker DynamoDB Table Schema:

Partition Key: BatchId (String) - e.g., "2025-07-09" or "products_master_data"

Attributes: products_key, orders_keys (List of Strings), order_items_keys (List of Strings), product_file_arrived (Boolean), orders_files_arrived (Boolean), order_items_files_arrived (Boolean), step_function_triggered (Boolean), last_updated (String), status (String).

Conditional Triggering: The Lambda only initiates an AWS Step Functions workflow if and only if all required data components (the products.csv file and all orders and order_items files for a specific date) are detected as arrived in the BatchFileTracker DynamoDB table, and the workflow for that batch has not been previously triggered. This prevents premature or duplicate processing.

### Workflow Orchestration (AWS Step Functions)
The Step Functions state machine (ecommerce-pipeline-workflow) orchestrates the end-to-end data processing.

{
  "Comment": "Real-time e-commerce KPI pipeline using ECS Fargate",
  "StartAt": "ValidateData",
  "States": {
    "ValidateData": {
      "Type": "Task",
      "Resource": "arn:aws:states:::ecs:runTask.sync",
      "Parameters": {
        "LaunchType": "FARGATE",
        "Cluster": "ecommerce-cluster",
        "TaskDefinition": "ecommerce-validation-task",
        "NetworkConfiguration": {
          "AwsvpcConfiguration": {
            "Subnets": [
              "subnet-xxxxxxxxxxxxxxxxx",
              "subnet-yyyyyyyyyyyyyyyyy"
            ],
            "SecurityGroups": [
              "sg-zzzzzzzzzzzzzzzzz"
            ],
            "AssignPublicIp": "ENABLED"
          }
        },
        "Overrides": {
          "ContainerOverrides": [
            {
              "Name": "validation",
              "Environment": [
                {
                  "Name": "S3_BUCKET",
                  "Value.$": "$.bucket"
                },
                {
                  "Name": "PRODUCTS_KEY",
                  "Value.$": "$.products_key"
                },
                {
                  "Name": "ORDERS_KEYS",
                  "Value.$": "States.JsonToString($.orders_keys)"
                },
                {
                  "Name": "ORDER_ITEMS_KEYS",
                  "Value.$": "States.JsonToString($.order_items_keys)"
                }
              ]
            }
          ]
        }
      },
      "InputPath": "$",
      "ResultPath": "$.validation",
      "Retry": [
        {
          "ErrorEquals": [
            "States.TaskFailed",
            "States.Timeout"
          ],
          "IntervalSeconds": 3,
          "MaxAttempts": 2,
          "BackoffRate": 2.0
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Next": "PublishValidationFailureNotification",
          "ResultPath": "$.error"
        }
      ],
      "TimeoutSeconds": 120,
      "Next": "CheckValidationResult"
    },
    "CheckValidationResult": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.validation.Containers[0].ExitCode",
          "NumericEquals": 0,
          "Next": "TransformData"
        }
      ],
      "Default": "PublishValidationFailureNotification"
    },
    "PublishValidationFailureNotification": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "arn:aws:sns:[YOUR_REGION]:[YOUR_ACCOUNT_ID]:ecommerce-pipeline-alerts",
        "Message": {
          "Input.$": "$",
          "ErrorDetails.$": "$.error"
        },
        "Subject": "E-commerce Pipeline Alert: Validation Failed"
      },
      "Next": "ValidationFailed"
    },
    "TransformData": {
      "Type": "Task",
      "Resource": "arn:aws:states:::ecs:runTask.sync",
      "Parameters": {
        "LaunchType": "FARGATE",
        "Cluster": "ecommerce-cluster",
        "TaskDefinition": "ecommerce-transformation-task",
        "NetworkConfiguration": {
          "AwsvpcConfiguration": {
            "Subnets": [
              "subnet-xxxxxxxxxxxxxxxxx",
              "subnet-yyyyyyyyyyyyyyyyy"
            ],
            "SecurityGroups": [
              "sg-zzzzzzzzzzzzzzzzz"
            ],
            "AssignPublicIp": "ENABLED"
          }
        },
        "Overrides": {
          "ContainerOverrides": [
            {
              "Name": "transformation",
              "Environment": [
                {
                  "Name": "S3_BUCKET",
                  "Value.$": "$.bucket"
                },
                {
                  "Name": "PRODUCTS_KEY",
                  "Value.$": "$.products_key"
                },
                {
                  "Name": "ORDERS_KEYS",
                  "Value.$": "States.JsonToString($.orders_keys)"
                },
                {
                  "Name": "ORDER_ITEMS_KEYS",
                  "Value.$": "States.JsonToString($.order_items_keys)"
                }
              ]
            }
          ]
        }
      },
      "InputPath": "$",
      "ResultPath": "$.transformation",
      "Retry": [
        {
          "ErrorEquals": [
            "States.TaskFailed",
            "States.Timeout"
          ],
          "IntervalSeconds": 3,
          "MaxAttempts": 2,
          "BackoffRate": 2.0
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Next": "PublishTransformationFailureNotification",
          "ResultPath": "$.error"
        }
      ],
      "TimeoutSeconds": 300,
      "End": true
    },
    "PublishTransformationFailureNotification": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "arn:aws:sns:[YOUR_REGION]:[YOUR_ACCOUNT_ID]:ecommerce-pipeline-alerts",
        "Message": {
          "Input.$": "$",
          "ErrorDetails.$": "$.error"
        },
        "Subject": "E-commerce Pipeline Alert: Transformation Failed"
      },
      "Next": "TransformationFailed"
    },
    "ValidationFailed": {
      "Type": "Fail",
      "Error": "ValidationError",
      "Cause": "Data validation task failed."
    },
    "TransformationFailed": {
      "Type": "Fail",
      "Error": "TransformationError",
      "Cause": "Data transformation task failed."
    }
  }
}

### Containerized ECS Tasks (Validation & Transformation)
Your data processing logic is encapsulated within Docker containers, deployed to Amazon ECS using the Fargate launch type. This eliminates the need to manage underlying EC2 instances.

Validation Task (ecommerce-validation-task):

Reads incoming CSV files from the raw/ S3 prefix.

Performs schema validation (checks for required columns) and basic data quality checks (e.g., null values in critical fields).

On success, files are moved to validated/. On failure, files are moved to invalid/, and the task exits with a non-zero code, signaling Step Functions to trigger error handling.

Transformation Task (ecommerce-transformation-task):

Reads validated CSV files from the validated/ S3 prefix.

Utilizes Apache Spark (running within the container) for distributed data processing.

Implements incremental loading: It identifies the specific order_dates present in the newly processed files and recalculates KPIs only for those dates, optimizing compute resources.

Computes Category-Level and Order-Level KPIs.

Writes the resulting KPIs to the respective DynamoDB tables (CategoryKPIs, DailyKPIs).

Archives successfully processed files from validated/ to processed/ in S3.

ECS Task Environment Variables (Example for Step Functions Input):

The Step Functions workflow dynamically passes S3 paths to the ECS tasks via environment variables.

S3_BUCKET=ecommerce-event-pipeline
PRODUCTS_KEY=raw/products/products.csv
ORDERS_KEYS=["raw/orders/2025-07-09/orders_part1.csv", "raw/orders/2025-07-09/orders_part2.csv"]
ORDER_ITEMS_KEYS=["raw/order_items/2025-07-09/order_items_part1.csv"]

### Robustness and Error Handling
The pipeline is designed with fault tolerance and observability as core principles:

Retry Mechanism: Step Functions tasks (ValidateData, TransformData) are configured with retry logic (MaxAttempts, IntervalSeconds, BackoffRate) for transient failures (e.g., States.TaskFailed, States.Timeout).

Centralized Logging: All Lambda and ECS task stdout/stderr are streamed to Amazon CloudWatch Logs for real-time monitoring and debugging. Additionally, ECS tasks write detailed execution logs to S3 (logs/validation/, logs/transformation/) for long-term storage and analysis.

Failure Notifications (SNS): Upon unrecoverable errors (after retries are exhausted) in the ValidateData or TransformData steps, an Amazon SNS topic (ecommerce-pipeline-alerts) is published. This can be configured to send email or other notifications to alert operations teams.

Dedicated Fail States: Step Functions includes ValidationFailed and TransformationFailed states, which explicitly terminate the workflow on critical errors, preserving the execution history for investigation.

Data Segregation: Malformed or incomplete data files are automatically moved to an invalid/ S3 prefix by the validation task, preventing them from polluting downstream processes.

Idempotent File Movement: The file movement logic (move_file function) is idempotent, ensuring that concurrent or retried operations do not lead to data duplication or corruption.

Atomic Batch Tracking: The DynamoDB BatchFileTracker uses atomic updates and conditional expressions to ensure consistent state management, preventing race conditions and duplicate Step Functions triggers.

### IAM and Security
Fine-grained IAM policies are applied to each AWS service role to adhere to the principle of least privilege:

Lambda Role: Permissions for s3:GetObject (implicitly via event), dynamodb:UpdateItem, dynamodb:GetItem on the BatchFileTracker table, and states:StartExecution on the Step Functions state machine.

ECS Task Roles: Permissions for s3:GetObject, s3:PutObject, s3:DeleteObject on the ecommerce-event-pipeline bucket (specifically raw/, validated/, invalid/, processed/, logs/ prefixes), and dynamodb:PutItem (for KPI tables).

Step Functions Role: Permissions to invoke ECS tasks (ecs:RunTask), publish to SNS (sns:Publish), and manage its own executions.

### Simulation and Testing Instructions
To manually simulate the pipeline's functionality:

Ensure all AWS resources are deployed: S3 bucket, DynamoDB tables (CategoryKPIs, DailyKPIs, BatchFileTracker), Lambda function (trigger-ecommerce-kpi-pipeline), ECS Cluster, ECS Task Definitions, and the Step Functions State Machine.

Verify Lambda Environment Variables: Confirm PIPELINE_BATCH_TRACKER_TABLE is set to BatchFileTracker and STEP_FUNCTION_ARN is set to your Step Functions ARN.

Verify DynamoDB Schema: Ensure BatchFileTracker has BatchId (String) as its sole Partition Key.

Upload products.csv: Place your products.csv file into s3://[YOUR_BUCKET_NAME]/raw/products/products.csv.

Expected Lambda Log: You should see [INFO] Products master data batch updated. No Step Function trigger from this file directly.

Upload orders files: Place your orders_partX.csv files into a date-partitioned folder, e.g., s3://[YOUR_BUCKET_NAME]/raw/orders/2025-07-09/orders_part1.csv.

Expected Lambda Log: [WARN] No date partition found... Assigning current UTC date... (if you don't use date partitions) or [INFO] Processing incoming file... for Batch ID: 2025-07-09 (if you use date partitions). DynamoDB BatchFileTracker will be updated.

Upload order_items files: Place your order_items_partX.csv files into the same date-partitioned folder as the orders, e.g., s3://[YOUR_BUCKET_NAME]/raw/order_items/2025-07-09/order_items_part1.csv.

Expected Lambda Log: Similar to orders.

Once the last required file for a batch arrives (and products master data is ready): The Lambda logs should show [INFO] All required files for Daily Batch ID [DATE] and Products Master Data are ready. Preparing to trigger Step Function. followed by [INFO] Triggering Step Function with input: and the full payload.

Monitor Step Functions: Go to the AWS Step Functions console and observe the execution of your workflow.

Verify ValidateData and TransformData tasks run successfully.

Check their CloudWatch Logs for detailed execution output.

Verify Data in S3:

s3://[YOUR_BUCKET_NAME]/validated/ should contain the files after validation.

s3://[YOUR_BUCKET_NAME]/processed/ should contain the files after successful transformation.

s3://[YOUR_BUCKET_NAME]/invalid/ (if any validation failures occur).

s3://[YOUR_BUCKET_NAME]/logs/validation/ and logs/transformation/ should contain custom script logs.

Verify KPIs in DynamoDB: Query your CategoryKPIs and DailyKPIs tables to confirm the computed KPIs are present and correctly structured.

Test Failure Paths: Intentionally upload a malformed CSV (e.g., missing a required column) to raw/ and observe the pipeline's behavior (validation failure, SNS alert, file moved to invalid/).