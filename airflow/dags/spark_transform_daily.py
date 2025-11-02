from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {"owner": "admin", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="spark_transform_daily",
    description="Daily Spark transforms from bronze/silver to silver/gold",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["spark","silver"],
) as dag:
    transform = BashOperator(task_id="spark_transform", bash_command="echo '[spark] transform for {{ ds }}'")
    transform
