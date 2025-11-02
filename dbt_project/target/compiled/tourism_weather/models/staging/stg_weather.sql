with src as (select * from "tereza_tourist"."core"."weather_daily_clean")
select
  dt,
  month_start,
  avg_temp_c,
  min_temp_c,
  max_temp_c,
  humidity_avg,
  wind_speed_avg_ms,
  precipitation_mm,
  temp_bucket
from src