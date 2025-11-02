select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select month_start as from_field
    from "tereza_tourist"."marts"."fact_tourism_weather"
    where month_start is not null
),

parent as (
    select month_start as to_field
    from "tereza_tourist"."marts"."dim_date"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test