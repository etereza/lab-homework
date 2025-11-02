{{ config(materialized='table') }}
with t as (select * from {{ ref('int_tourism') }}),
     w as (select * from {{ ref('int_weather_monthly') }})
select
  t.month_start,
  t.region,
  t.tourists_total,
  w.avg_temp_c,
  w.humidity_avg,
  w.wind_speed_avg_ms,
  w.precipitation_mm_sum
from t left join w using(month_start)
