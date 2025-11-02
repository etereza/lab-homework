
    
    

select
    month_start as unique_field,
    count(*) as n_records

from "tereza_tourist"."marts"."dim_date"
where month_start is not null
group by month_start
having count(*) > 1


