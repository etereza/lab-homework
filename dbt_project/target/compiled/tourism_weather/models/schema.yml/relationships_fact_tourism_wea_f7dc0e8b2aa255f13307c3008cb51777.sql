
    
    

with child as (
    select region as from_field
    from "tereza_tourist"."marts"."fact_tourism_weather"
    where region is not null
),

parent as (
    select region as to_field
    from "tereza_tourist"."marts"."dim_region"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


