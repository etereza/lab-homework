from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook

def _probe():
    hook = PostgresHook(postgres_conn_id="pg_dwh")
    ts = hook.get_first("select now()")
    print(f"PG says now(): {ts[0]}")

with DAG(
    dag_id="_check_pg_conn",
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    tags=["healthcheck"],
) as dag:
    PythonOperator(task_id="probe_pg", python_callable=_probe)
