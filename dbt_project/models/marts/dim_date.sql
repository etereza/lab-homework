{{ config(schema='marts', materialized='table') }}

with bounds as (
  select
    date_trunc('month', min(month_start)) as dmin,
    date_trunc('month', max(month_start)) as dmax
  from {{ ref('fact_tourism_weather') }}
),
months as (
  select generate_series(dmin, dmax, interval '1 month')::date as m
  from bounds
)
select
  m                                         as month_start,
  extract(year  from m)::int                as year,
  to_char(m, 'TMMonth')                     as month_name,
  extract(month from m)::int                as month_num,
  case
    when extract(month from m) in (12,1,2) then 'winter'
    when extract(month from m) in (3,4,5)  then 'spring'
    when extract(month from m) in (6,7,8)  then 'summer'
    else 'autumn'
  end                                       as season
from months
