from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

PIPELINE_SCHEDULE = Variable.get("pipeline_cron", default_var="0 3 * * *")
BLOCK_ON_FAILURE = Variable.get("block_on_failure", default_var="true").lower() == "true"

default_args = {"owner": "admin", "retries": 1, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id="tourism_weather_pipeline",
    description="Top-level orchestrator for tourism & weather",
    start_date=datetime(2025, 1, 1),
    schedule=PIPELINE_SCHEDULE,
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["orchestrator"],
) as dag:
    start = EmptyOperator(task_id="start")

    def _is_monthly_run(logical_date: datetime, **_):
        return logical_date.day == 1

    is_monthly = ShortCircuitOperator(task_id="is_monthly", python_callable=_is_monthly_run)

    t_ingest_weather = TriggerDagRunOperator(
        task_id="run_ingest_weather_daily",
        trigger_dag_id="ingest_weather_daily",
        conf={"runtime": "{{ ds }}"},
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    t_ingest_tourism = TriggerDagRunOperator(
        task_id="run_ingest_tourism_monthly",
        trigger_dag_id="ingest_tourism_monthly",
        conf={"runtime_month": "{{ ds }}"},
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    t_spark = TriggerDagRunOperator(
        task_id="run_spark_transform_daily",
        trigger_dag_id="spark_transform_daily",
        conf={"runtime": "{{ ds }}"},
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    t_dbt = TriggerDagRunOperator(
        task_id="run_dbt_warehouse_build",
        trigger_dag_id="dbt_warehouse_build",
        conf={"runtime": "{{ ds }}"},
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        trigger_rule=TriggerRule.ALL_SUCCESS if BLOCK_ON_FAILURE else TriggerRule.ALL_DONE,
    )

    end = EmptyOperator(task_id="end")

    start >> t_ingest_weather
    start >> is_monthly >> t_ingest_tourism
    [t_ingest_weather, t_ingest_tourism] >> t_spark
    t_spark >> t_dbt >> end
