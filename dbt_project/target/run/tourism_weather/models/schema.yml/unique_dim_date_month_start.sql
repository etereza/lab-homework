select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    month_start as unique_field,
    count(*) as n_records

from "tereza_tourist"."marts"."dim_date"
where month_start is not null
group by month_start
having count(*) > 1



      
    ) dbt_internal_test