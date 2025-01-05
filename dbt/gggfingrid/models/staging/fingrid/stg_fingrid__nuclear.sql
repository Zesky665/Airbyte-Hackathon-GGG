WITH base as (SELECT * FROM {{ ref('base__nuclear') }}),
renamed as (
    SELECT 
    endTime::DATE as generated_at_date,
    endTime::TIME as generated_at_time,
    "Nuclear power production - real time data" as source_value,
    'Nuclear Power Production' as source_name
    FROM base
)
SELECT * 
FROM renamed 