from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime

default_args = {"owner": "admin", "retries": 0}

DBT_CMD = (
    "cd /opt/airflow/dbt_project && "
    "dbt deps && "
    "DBT_PROFILES_DIR=/opt/airflow/dbt_project "
    "dbt build --project-dir . --profiles-dir . --target prod --fail-fast -x"
)

with DAG(
    dag_id="dbt_warehouse_build",
    description="Run dbt models & tests for DWH",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["dbt", "gold"],
) as dag:
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f'echo "Running: {DBT_CMD}" && {DBT_CMD}',
    )

    dbt_build