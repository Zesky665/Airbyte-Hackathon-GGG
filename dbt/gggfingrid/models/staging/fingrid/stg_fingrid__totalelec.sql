WITH base as (SELECT * FROM {{ ref('base__totalelec') }}),
renamed as (
    SELECT 
    endTime::DATE as generated_at_date,
    endTime::TIME as generated_at_time,
    "Electricity production in Finland - real time data" as source_value,
    'Total Electricity Production' as source_name  
)
SELECT * 
FROM renamed 