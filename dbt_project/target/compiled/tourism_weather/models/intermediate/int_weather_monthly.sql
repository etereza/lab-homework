with w as (select * from "tereza_tourist"."core"."stg_weather")
select
  date_trunc('month', month_start)::date as month_start,
  avg(avg_temp_c) as avg_temp_c,
  avg(humidity_avg) as humidity_avg,
  avg(wind_speed_avg_ms) as wind_speed_avg_ms,
  sum(precipitation_mm) as precipitation_mm_sum
from w
group by 1