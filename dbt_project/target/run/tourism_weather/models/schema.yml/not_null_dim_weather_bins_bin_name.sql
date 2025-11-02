select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select bin_name
from "tereza_tourist"."marts"."dim_weather_bins"
where bin_name is null



      
    ) dbt_internal_test