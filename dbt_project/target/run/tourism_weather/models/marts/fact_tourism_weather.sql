
  
    

  create  table "tereza_tourist"."marts"."fact_tourism_weather__dbt_tmp"
  
  
    as
  
  (
    
with t as (select * from "tereza_tourist"."core"."int_tourism"),
     w as (select * from "tereza_tourist"."core"."int_weather_monthly")
select
  t.month_start,
  t.region,
  t.tourists_total,
  w.avg_temp_c,
  w.humidity_avg,
  w.wind_speed_avg_ms,
  w.precipitation_mm_sum
from t left join w using(month_start)
  );
  