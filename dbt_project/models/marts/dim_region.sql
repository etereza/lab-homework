{{ config(schema='marts', materialized='table') }}
select distinct region
from {{ ref('fact_tourism_weather') }}
