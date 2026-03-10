# Banking CDC Pipeline with Kafka, MinIO, Snowflake, dbt, and Airflow

End-to-end local data platform for banking data that demonstrates:

- Synthetic data generation in Postgres
- CDC streaming with Debezium + Kafka
- Raw landing to MinIO as Parquet
- Ingestion into Snowflake
- Transformations, snapshots (SCD2), facts/marts in dbt
- Orchestration with Airflow

## Architecture

`Postgres -> Debezium Connect -> Kafka topics -> Python consumer -> MinIO (Parquet) -> Airflow DAG -> Snowflake RAW -> dbt staging/facts/marts + snapshots`

## Project Structure

- `data-generation/fake_sql_data.py`: generates customers/accounts/transactions in Postgres
- `kafka-ingestion/kafka-debezium_config_conn.py`: creates Debezium connector
- `consumer/kafka_to_minio.py`: consumes Kafka CDC topics and writes Parquet to MinIO
- `docker/dags/minio_to_snowflake.py`: Airflow DAG to move MinIO files into Snowflake tables
- `docker/dags/scd_airflow.py`: Airflow DAG to run dbt snapshots and marts
- `postgres/ingestion.sql`: source table DDL
- `banking_dbt/`: dbt project (staging, facts, marts, snapshots)

## Prerequisites

- Docker + Docker Compose
- Python 3.12+
- `uv` (recommended) or pip
- Snowflake account and credentials

## Environment Variables

Create a `.env` file at repo root with values for:

- Postgres: `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
- Kafka: `KAFKA_BOOTSTRAP`, `KAFKA_GROUP`
- MinIO: `MINIO_ENDPOINT`, `MINIO_ENDPOINT_LOCAL`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET`, `MINIO_LOCAL_DIR`
- Airflow DB: `AIRFLOW_DB_USER`, `AIRFLOW_DB_PASSWORD`, `AIRFLOW_DB_NAME`
- Snowflake: `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DB`, `SNOWFLAKE_SCHEMA`

Note:
- For host-side Python runs, use `MINIO_ENDPOINT_LOCAL=http://localhost:9000`.
- Inside Docker services, `MINIO_ENDPOINT=http://minio:9000` is valid.

## Setup

### 1. Start infrastructure

```bash
docker compose up -d
docker compose ps
```

Services include Zookeeper, Kafka, Debezium Connect, Postgres, MinIO, Airflow, and Airflow Postgres.

### 2. Install Python dependencies

Using `uv`:

```bash
uv sync
```

or pip:

```bash
pip install -r requirements.txt
```

### 3. Initialize Postgres source tables

```bash
psql -h localhost -p 5432 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f postgres/ingestion.sql
```

### 4. Seed/generate source data

Continuous mode:

```bash
uv run python -m data-generation.fake_sql_data
```

Run once:

```bash
uv run python -m data-generation.fake_sql_data --once
```

### 5. Create Debezium connector

```bash
uv run python -m kafka-ingestion.kafka-debezium_config_conn
```

Check connector:

```bash
curl -s http://localhost:8083/connectors
curl -s http://localhost:8083/connectors/postgres-connector/status
```

### 6. Start Kafka to MinIO consumer

```bash
uv run python -m consumer.kafka_to_minio
```

Expected:
- Subscribes to:
	- `banking_server.public.customers`
	- `banking_server.public.accounts`
	- `banking_server.public.transactions`
- Uploads parquet files to `s3://<MINIO_BUCKET>/<table>/date=YYYY-MM-DD/...`

### 7. Run dbt

```bash
cd banking_dbt
dbt debug
dbt run --select staging
dbt snapshot
dbt run --select facts
dbt run --select marts
```

## Airflow

- UI: `http://localhost:8080`
- Default local creds in compose: `admin` / `admin`
- DAGs:
	- `minio_to_snowflake`: runs every minute
	- `SCD2_SNAPSHOT`: daily snapshot -> facts -> marts

## Common Issues and Fixes

### 1. `Could not connect to endpoint URL: http://minio:9000/`

Cause: running consumer on host where `minio` DNS is not resolvable.
Fix: set `MINIO_ENDPOINT_LOCAL=http://localhost:9000`.

### 2. Kafka `UNKNOWN_TOPIC_OR_PART`

Cause: Debezium connector not created/running.
Fix:
- Create connector via `kafka-debezium_config_conn.py`
- Verify connector status is `RUNNING`
- Verify topics exist.

### 3. dbt duplicate row errors in incremental/snapshot

Usually caused by wrong `unique_key` or missing dedup in staging models.

- Ensure account snapshots use `account_id` (not `customer_id`)
- Ensure CDC staging models keep latest record per business key using `row_number() ... where rn = 1`
- Ensure incremental facts use `is_incremental()` filters

## Quick Validation Checklist

- `docker compose ps` shows all services healthy
- Connector status is `RUNNING`
- Kafka topics for `banking_server.public.*` exist
- MinIO bucket has parquet files
- `dbt snapshot` succeeds
- `dbt run --select facts` succeeds

## Tech Stack

- Postgres 15
- Kafka + Zookeeper (Confluent images)
- Debezium Connect
- MinIO
- Snowflake
- dbt (Snowflake adapter)
- Apache Airflow
- Python (`confluent-kafka`, `boto3`, `pandas`, `fastparquet`, `psycopg2`, `python-dotenv`)
