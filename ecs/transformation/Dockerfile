FROM python:3.10-slim

WORKDIR /app

COPY transform_task.py .

RUN pip install pandas boto3

CMD ["python", "transform_task.py"]
