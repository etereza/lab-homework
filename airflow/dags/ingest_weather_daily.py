from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {"owner": "admin", "retries": 2, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="ingest_weather_daily",
    description="Daily weather ingest from source to bronze/silver",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["ingestion","weather"],
) as dag:
    fetch = BashOperator(task_id="fetch_weather", bash_command="echo '[weather] fetching for {{ ds }}'")
    load = BashOperator(task_id="load_weather", bash_command="echo '[weather] loading to DB for {{ ds }}'")
    fetch >> load
