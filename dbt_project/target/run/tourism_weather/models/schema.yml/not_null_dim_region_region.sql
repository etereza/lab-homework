select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select region
from "tereza_tourist"."marts"."dim_region"
where region is null



      
    ) dbt_internal_test