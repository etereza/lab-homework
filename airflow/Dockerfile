FROM apache/airflow:2.9.2

# Переходимо під користувача airflow (не root)
USER airflow

# Встановлюємо dbt-core та адаптер для Postgres
RUN pip install --no-cache-dir "dbt-core==1.7.14" "dbt-postgres==1.7.14"
