from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=1),
    'retries': 1,
}

with DAG(
    dag_id='SCD2_SNAPSHOT',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['dbt', 'scd2', 'snapshot'],
    description='A DAG to run dbt snapshot for SCD2 tables',
) as dag:
    
    run_dbt_snapshot = BashOperator(
        task_id='run_dbt_snapshot',
        bash_command='cd /opt/airflow/banking_dbt && dbt snapshot --profiles-dir /home/airflow/.dbt'
    )

    run_facts = BashOperator(
        task_id='run_facts',
        bash_command='cd /opt/airflow/banking_dbt && dbt run --select facts --profiles-dir /home/airflow/.dbt'
    )

    run_marts = BashOperator(
        task_id='run_marts',
        bash_command='cd /opt/airflow/banking_dbt && dbt run --select marts --profiles-dir /home/airflow/.dbt'
    )

    run_dbt_snapshot >> run_facts >> run_marts