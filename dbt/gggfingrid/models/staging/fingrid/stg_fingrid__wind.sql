WITH base as (SELECT * FROM {{ ref('base__wind') }}),
renamed as (
    SELECT 
    endTime::DATE as generated_at_date,
    endTime::TIME as generated_at_time,
    "Wind power production - real time data" as source_value,
    'Wind Power Production' as source_name
    FROM base
)
SELECT * 
FROM renamed 