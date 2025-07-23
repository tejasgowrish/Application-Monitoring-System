FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir kafka-python psycopg2-binary

COPY log_consumer.py .

CMD ["python", "log_consumer.py"]