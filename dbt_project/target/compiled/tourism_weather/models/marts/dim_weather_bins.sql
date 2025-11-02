

-- dbt-модель, яка віддає рівно ті колонки, які очікують тести
select
    bin_name,
    t_min,
    t_max
from marts.dim_weather_bins