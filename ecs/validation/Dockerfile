# Use slim Python image
FROM python:3.9-slim

# Install required libraries
RUN pip install pandas boto3

# Set working directory
WORKDIR /app

# Copy validation script
COPY validate_task.py .

# Set entrypoint
CMD ["python", "validate_task.py"]
