with hourly as (
    SELECT {{ dbt_utils.star(from=ref('hourly_statistics'), except=["source_value_hourly", "hour_value"])}}, SUM(source_value_hourly) as daily_source_value
    FROM {{ ref('hourly_statistics') }}
    GROUP BY {{ dbt_utils.star(from=ref('hourly_statistics'), except=["source_value_hourly", "hour_value"])}}
)
SELECT *
FROM hourly
