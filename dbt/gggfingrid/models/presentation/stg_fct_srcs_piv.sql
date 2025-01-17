{{ config(materialized='table') }}
WITH basetable AS (

    SELECT
        source_name,
        source_value,
        date_value,
        year_value,
        month_value,
        iso_week_num,
        day_value,
        hour_value,
    FROM {{ ref('obt_fct_tbl') }}
) 
PIVOT basetable
ON source_name 
USING AVG(source_value)
GROUP BY 
        date_value,
        year_value,
        month_value,
        iso_week_num,
        day_value,
        hour_value