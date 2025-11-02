
    
    

select
    region as unique_field,
    count(*) as n_records

from "tereza_tourist"."marts"."dim_region"
where region is not null
group by region
having count(*) > 1


