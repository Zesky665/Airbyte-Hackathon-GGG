{{ config(
    schema = 'analytics'
) }}

WITH basetable AS (

    SELECT
        source_name,
        source_value,
        date_value,
        time_value 
    FROM {{ ref('obt_fct_tbl') }}
) 
PIVOT basetable
ON source_name 
USING SUM(source_value)
