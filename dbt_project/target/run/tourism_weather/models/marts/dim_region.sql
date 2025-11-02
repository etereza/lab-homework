
  
    

  create  table "tereza_tourist"."marts"."dim_region__dbt_tmp"
  
  
    as
  
  (
    
select distinct region
from "tereza_tourist"."marts"."fact_tourism_weather"
  );
  