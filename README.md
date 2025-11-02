Проєкт: "Вплив погоди на туристичний потік у Дрогобичі"

Мета:
- зібрати погодні дані (середня температура, опади, вологість) по Дрогобичу за період.
- об’єднати їх з даними про кількість туристів (місячна статистика).
- побудувати аналітичні таблиці для BI (Power BI / Tableau).

Архітектура:
1. ingestion (скрипти Python) -> пишуть у staging.* у віддаленій БД Postgres
2. dbt -> будує marts.fact_tourism_weather, dim_date у тій же БД
3. BI -> читає marts

Кроки запуску:
1. pip install -r requirements.txt
2. python .\ingestion\weather_fetch_to_staging.py
3. python .\ingestion\tourism_load_to_staging.py
4. cd dbt_project && python -m dbt debug && python -m dbt run && python -m dbt test
