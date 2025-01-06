WITH base as (SELECT * FROM {{ ref('base__nuclear') }}),
renamed as (
    SELECT 
    endTime::DATE as generated_at_date,
    endTime::TIME as generated_at_time,
    "Nuclear power production - real time data" as source_value,
    'Nuclear Power Production' as source_name
    FROM base
), 
null_filter_and_deduped as (
SELECT DISTINCT 
generated_at_date,
generated_at_time,
source_value,
source_name
FROM renamed 
WHERE generated_at_date IS NOT NULL 
AND generated_at_time IS NOT NULL 
AND source_value IS NOT NULL 
AND source_name IS NOT NULL 
)
SELECT * 
FROM null_filter_and_deduped 