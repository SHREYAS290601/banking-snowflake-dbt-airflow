import os
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv(filename=".env"))

# -------- MinIO Config --------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET = os.getenv("MINIO_BUCKET",'raw')
LOCAL_DIR = os.getenv("MINIO_LOCAL_DIR", "/tmp/minio_downloads")

# -------- Snowflake Config --------
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

TABLES = ["customers", "accounts", "transactions"]

def download_from_minio():
    try:
        os.makedirs(LOCAL_DIR, exist_ok=True)
    except Exception as e:
        print(f"Error creating local directory: {e}")
        return

    s3=boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    local_files ={}
    for table in TABLES:
        prefix=f"{table}/"
        print(f"Listing objects in bucket '{BUCKET}' with prefix '{prefix}'")
        response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        objects=response.get('Contents', [])
        local_files[table]=[]
        for obj in objects:
            key=obj['Key']
            # this gets the filename from the key and joins it with the local directory to create the full path for the downloaded file
            local_file=os.path.join(LOCAL_DIR,os.path.basename(key))
            s3.download_file(BUCKET, key, local_file)
            print(f"Downloaded {key} to {local_file}")
            local_files[table].append(local_file)
    return local_files

def load_into_snowflake(**kwargs):
    local_files=kwargs['ti'].xcom_pull(task_ids='download_from_minio')
    if not local_files:
        print(f"{'-'*10}No Files to Load into Snowflake{'-'*10}")
        return
    conn=snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA
    )
    cur=conn.cursor()

    for table_name, files in local_files.items():
        if not files:
            print(f"{'-'*10}No Files for Table {table_name} to Load into Snowflake{'-'*10}")
            continue
        for file in files:
            cur.execute(
                f"""
                PUT file:///{file} @%{table_name}
            """)
            print(f"Uploaded {file} to Snowflake stage for table {table_name}")
        
        ingest_all_data_from_files_sql=f"""
        COPY into {table_name}
        FROM @%{table_name}
        FILE_FORMAT=(TYPE=PARQUET)
        ON_ERROR='CONTINUE'
        """
        cur.execute(ingest_all_data_from_files_sql)
        print(f"Ingested data from files into Snowflake table {table_name}")
    cur.close()
    conn.close()

# Define the DAG
# ---- DAG Config ----
default_args={
    'owner': 'airflow',
    'retries':1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='minio_to_snowflake',
    default_args=default_args,
    description='A DAG to transfer data from MinIO to Snowflake',
    schedule_interval="*/1 * * * *",
    start_date=datetime.now(),
    catchup=False
) as dag:
    
    task1=PythonOperator(
        task_id='download_from_minio',
        python_callable=download_from_minio
    )

    task2=PythonOperator(
        task_id='load_into_snowflake',
        python_callable=load_into_snowflake,
        provide_context=True
    )

    task1 >> task2