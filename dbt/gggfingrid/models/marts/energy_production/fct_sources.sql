with 
dates as (SELECT date_key, date_value FROM {{ ref('dim_date') }}),
times as (SELECT time_key, time_value FROM {{ ref('dim_time') }}),
sources as (SELECT * FROM {{ ref('dim_sources') }}),
uniontable as (SELECT * FROM {{ ref('int_fct_sources_unioned') }}),
joinedtable as (
    SELECT 
    {{dbt_utils.generate_surrogate_key(['time_value', 'date_value', 'sources.source_name'])}} as fact_key,
    dates.date_key, times.time_key, sources.source_key, uniontable.source_value
    FROM uniontable
    INNER JOIN dates
    ON uniontable.generated_at_date = dates.date_value
    INNER JOIN times
    ON uniontable.generated_at_time = times.time_value
    INNER JOIN sources
    ON uniontable.source_name = sources.source_name 
)
SELECT *
FROM joinedtable
