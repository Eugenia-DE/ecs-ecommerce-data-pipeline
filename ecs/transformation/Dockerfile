# Base Spark image from Bitnami
FROM bitnami/spark:3.4.1

# Switch to root user to install dependencies
USER root

# Set working directory
WORKDIR /app

# Install boto3 and pandas (avoid reinstalling Spark dependencies)
RUN pip install --no-cache-dir boto3 pandas

# Copy your transformation script
COPY transform_task.py .

# Default command: run the Spark job
CMD ["spark-submit", "--master", "local[*]", "transform_task.py"]
