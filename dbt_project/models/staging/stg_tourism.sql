
with src as (select * from {{ source('core','tourism_monthly_clean') }})
select
  month_start,
  region,
  tourists_total,
  year,
  month
from src
