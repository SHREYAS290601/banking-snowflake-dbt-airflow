import boto3
from confluent_kafka import Consumer
import json
import pandas as pd
from datetime import datetime
import os
from urllib.parse import urlparse, urlunparse
from dotenv import load_dotenv

# -----------------------------
# Load secrets from .env
# -----------------------------
load_dotenv()

# Kafka consumer settings
consumer = Consumer({
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP"),
    'group.id': os.getenv("KAFKA_GROUP"),
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'max.partition.fetch.bytes': 10485760  # 10MB max per message
})

consumer.subscribe([
    'banking_server.public.customers',
    'banking_server.public.accounts',
    'banking_server.public.transactions'
])

# MinIO client
def resolve_minio_endpoint() -> str:
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    endpoint_local = os.getenv("MINIO_ENDPOINT_LOCAL")

    # When running from host, Docker DNS name `minio` will not resolve.
    if endpoint_local:
        return endpoint_local

    parsed = urlparse(endpoint)
    if parsed.hostname == "minio":
        netloc = "localhost"
        if parsed.port:
            netloc = f"localhost:{parsed.port}"
        return urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))

    return endpoint


s3 = boto3.client(
    's3',
    endpoint_url=resolve_minio_endpoint(),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
)

bucket = os.getenv("MINIO_BUCKET")

# Create bucket if not exists
if bucket not in [b['Name'] for b in s3.list_buckets()['Buckets']]:
    s3.create_bucket(Bucket=bucket)

# Consume and write function
def write_to_minio(table_name, records):
    if not records:
        return
    df = pd.DataFrame(records)
    date_str = datetime.now().strftime('%Y-%m-%d')
    file_path = f'{table_name}_{date_str}.parquet'
    df.to_parquet(file_path, engine='fastparquet', index=False)
    s3_key = f'{table_name}/date={date_str}/{table_name}_{datetime.now().strftime("%H%M%S%f")}.parquet'
    s3.upload_file(file_path, bucket, s3_key)
    os.remove(file_path)
    print(f'✅ Uploaded {len(records)} records to s3://{bucket}/{s3_key}')

# Batch consume
batch_size = 50
buffer = {
    'banking_server.public.customers': [],
    'banking_server.public.accounts': [],
    'banking_server.public.transactions': []
}

print("✅ Connected to Kafka. Listening for messages...")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
        
        if msg.error():
            print(f"❌ Consumer error: {msg.error()}")
            continue
        
        topic = msg.topic()
        
        try:
            event = json.loads(msg.value().decode('utf-8'))
            payload = event.get("payload", {})
            record = payload.get("after")  # Only take the actual row

            if record:
                buffer[topic].append(record)
                print(f"[{topic}] -> {record}")

            if len(buffer[topic]) >= batch_size:
                write_to_minio(topic.split('.')[-1], buffer[topic])
                buffer[topic] = []
        
        except json.JSONDecodeError as e:
            print(f"❌ JSON decode error for topic {topic}: {e}")
        except Exception as e:
            print(f"❌ Error processing message from {topic}: {e}")

except KeyboardInterrupt:
    print("\n🛑 Shutting down gracefully...")
finally:
    # Flush remaining buffers before closing
    for topic, records in buffer.items():
        if records:
            write_to_minio(topic.split('.')[-1], records)
            print(f"✅ Flushed {len(records)} remaining records from {topic}")
    
    consumer.close()
    print("✅ Consumer closed")