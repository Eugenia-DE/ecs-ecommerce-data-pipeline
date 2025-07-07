FROM bitnami/spark:3.4.1

USER root

# Set working directory
WORKDIR /app

# Install required Python packages
RUN pip install boto3 pandas

# Copy your transformation script
COPY transform_task.py .

# Set command
CMD ["spark-submit", "transform_task.py"]
