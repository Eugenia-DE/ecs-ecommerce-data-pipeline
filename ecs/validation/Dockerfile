FROM python:3.11-slim

WORKDIR /app

COPY validate_task.py .

RUN pip install pandas boto3

CMD ["python", "validate_task.py"]
