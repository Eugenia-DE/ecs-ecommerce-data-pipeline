# Use slim Python image
FROM python:3.9-slim

# Install required libraries
RUN pip install pandas boto3

# Set working directory
WORKDIR /app

# Copy transformation script
COPY transform_task.py .

# Set entrypoint
CMD ["python", "transform_task.py"]
