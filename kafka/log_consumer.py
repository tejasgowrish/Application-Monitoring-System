import json
import time
import os
from datetime import datetime
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import Json

# Database connection parameters
DB_HOST = os.environ.get('DB_HOST', 'postgres')
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_NAME = os.environ.get('DB_NAME', 'logs')
DB_USER = os.environ.get('DB_USER', 'postgres')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'postgres')

# Kafka connection parameters
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
API_LOGS_TOPIC = 'api-logs'
ERROR_LOGS_TOPIC = 'error-logs'

def connect_to_postgres():
    """Connect to PostgreSQL database"""
    retries = 0
    max_retries = 30
    
    while retries < max_retries:
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            print("Connected to PostgreSQL database")
            return conn
        except psycopg2.OperationalError as e:
            retries += 1
            print(f"Failed to connect to PostgreSQL (Attempt {retries}/{max_retries}): {e}")
            time.sleep(5)
    
    raise Exception("Failed to connect to PostgreSQL after maximum retries")

def create_tables(conn):
    """Create necessary tables in the database if they don't exist"""
    with conn.cursor() as cur:
        # Create API logs table
        cur.execute('''
        CREATE TABLE IF NOT EXISTS api_logs (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            request_id TEXT NOT NULL,
            method TEXT NOT NULL,
            path TEXT NOT NULL,
            status_code INTEGER NOT NULL,
            success BOOLEAN NOT NULL,
            response_time FLOAT NOT NULL,
            client_ip TEXT,
            raw_data JSONB
        )
        ''')
        
        # Create error logs table
        cur.execute('''
        CREATE TABLE IF NOT EXISTS error_logs (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            error_type TEXT NOT NULL,
            message TEXT NOT NULL,
            severity TEXT NOT NULL,
            raw_data JSONB
        )
        ''')
        
        # Create indices for faster queries
        cur.execute('CREATE INDEX IF NOT EXISTS idx_api_logs_timestamp ON api_logs (timestamp)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_api_logs_path ON api_logs (path)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_api_logs_method ON api_logs (method)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_api_logs_status_code ON api_logs (status_code)')
        
        cur.execute('CREATE INDEX IF NOT EXISTS idx_error_logs_timestamp ON error_logs (timestamp)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_error_logs_error_type ON error_logs (error_type)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_error_logs_severity ON error_logs (severity)')
        
        conn.commit()
        print("Database tables created successfully")

def connect_to_kafka():
    """Connect to Kafka topics"""
    retries = 0
    max_retries = 30
    
    while retries < max_retries:
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                group_id='log-consumer-group',
                enable_auto_commit=True,
                auto_commit_interval_ms=5000
            )
            print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return consumer
        except Exception as e:
            retries += 1
            print(f"Failed to connect to Kafka (Attempt {retries}/{max_retries}): {e}")
            time.sleep(5)
    
    raise Exception("Failed to connect to Kafka after maximum retries")

def store_api_log(conn, log_data):
    """Store API log in the database"""
    with conn.cursor() as cur:
        cur.execute('''
        INSERT INTO api_logs (
            timestamp, request_id, method, path, status_code, 
            success, response_time, client_ip, raw_data
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            datetime.fromisoformat(log_data.get('timestamp', datetime.now().isoformat())),
            log_data.get('request_id', ''),
            log_data.get('method', ''),
            log_data.get('path', ''),
            log_data.get('status_code', 0),
            log_data.get('success', False),
            log_data.get('response_time', 0.0),
            log_data.get('client_ip', ''),
            Json(log_data)
        ))
        conn.commit()

def store_error_log(conn, log_data):
    """Store error log in the database"""
    with conn.cursor() as cur:
        cur.execute('''
        INSERT INTO error_logs (
            timestamp, error_type, message, severity, raw_data
        ) VALUES (%s, %s, %s, %s, %s)
        ''', (
            datetime.fromisoformat(log_data.get('timestamp', datetime.now().isoformat())),
            log_data.get('error_type', 'unknown'),
            log_data.get('message', ''),
            log_data.get('severity', 'error'),
            Json(log_data)
        ))
        conn.commit()

def process_logs():
    """Process logs from Kafka and store them in the database"""
    # Connect to PostgreSQL
    db_conn = connect_to_postgres()
    create_tables(db_conn)
    
    # Connect to Kafka
    consumer = connect_to_kafka()
    consumer.subscribe([API_LOGS_TOPIC, ERROR_LOGS_TOPIC])
    
    print(f"Subscribed to topics: {API_LOGS_TOPIC}, {ERROR_LOGS_TOPIC}")
    print("Starting log consumption...")
    
    try:
        for message in consumer:
            topic = message.topic
            log_data = message.value
            
            try:
                if topic == API_LOGS_TOPIC:
                    store_api_log(db_conn, log_data)
                    print(f"Stored API log: {log_data.get('request_id')}")
                elif topic == ERROR_LOGS_TOPIC:
                    store_error_log(db_conn, log_data)
                    print(f"Stored error log: {log_data.get('error_type')}")
            except Exception as e:
                print(f"Error processing log message: {e}")
    except KeyboardInterrupt:
        print("Stopping log consumer...")
    finally:
        db_conn.close()
        consumer.close()

if __name__ == "__main__":
    process_logs()