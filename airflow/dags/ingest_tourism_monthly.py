from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {"owner": "admin", "retries": 2, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id="ingest_tourism_monthly",
    description="Monthly tourism ingest (orchestrated)",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["ingestion","tourism"],
) as dag:
    fetch = BashOperator(task_id="fetch_tourism", bash_command="echo '[tourism] fetching for month {{ ds }}'")
    load = BashOperator(task_id="load_tourism", bash_command="echo '[tourism] loading to DB for month {{ ds }}'")
    fetch >> load
