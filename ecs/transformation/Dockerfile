FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy your code into the container
COPY transform_task.py .

# Install required dependencies
RUN pip install --no-cache-dir pyspark pandas boto3

# Set the command
CMD ["python", "transform_task.py"]
