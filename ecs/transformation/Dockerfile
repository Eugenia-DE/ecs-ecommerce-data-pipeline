FROM apache/spark-py:3.4.1

# Set working directory
WORKDIR /app

# Install required Python packages
RUN pip install boto3 pandas

# Copy your PySpark script
COPY transform_task.py .

# Set the command
CMD ["python", "transform_task.py"]
