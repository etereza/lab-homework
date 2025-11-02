select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    bin_name as unique_field,
    count(*) as n_records

from "tereza_tourist"."marts"."dim_weather_bins"
where bin_name is not null
group by bin_name
having count(*) > 1



      
    ) dbt_internal_test