
    
    

select
    bin_name as unique_field,
    count(*) as n_records

from "tereza_tourist"."marts"."dim_weather_bins"
where bin_name is not null
group by bin_name
having count(*) > 1


