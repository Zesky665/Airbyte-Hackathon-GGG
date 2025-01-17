WITH obt as (
    SELECT 
    source_name, 
    source_value, 
    date_value, 
    year_value, 
    month_value,
    iso_week_num, 
    day_value,
    hour_value 
    FROM {{ ref('obt_fct_tbl') }}
)

SELECT 
    source_name, 
    date_value, 
    year_value, 
    month_value,
    iso_week_num, 
    day_value,
    hour_value,
    AVG(source_value) as source_value_hourly
    FROM obt
    GROUP BY 
        source_name, 
        date_value, 
        year_value, 
        month_value,
        iso_week_num, 
        day_value,
        hour_value