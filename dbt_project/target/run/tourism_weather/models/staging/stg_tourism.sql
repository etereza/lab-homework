
  create view "tereza_tourist"."core"."stg_tourism__dbt_tmp"
    
    
  as (
    with src as (select * from "tereza_tourist"."core"."tourism_monthly_clean")
select
  month_start,
  region,
  tourists_total,
  year,
  month
from src
  );