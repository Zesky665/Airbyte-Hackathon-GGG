WITH base as (SELECT * FROM {{ ref('base__hydro') }}),
renamed as (
    SELECT 
    endTime::DATE as generated_at_date,
    endTime::TIME as generated_at_time,
    "Hydro power production - real time data" as source_value,
    'Hydroelectric Power Production' as source_name
)
SELECT * 
FROM renamed 