WITH base AS (
    SELECT
        *
    FROM
        {{ ref('base__consumption') }}
),
renamed AS (
    SELECT
        endTime :: DATE AS generated_at_date,
        endTime :: TIME AS generated_at_time,
        "Electricity consumption in Finland - real time data" AS source_value,
        'Total Power Consumption' AS source_name
    FROM
        base
),
null_filter AS (
    SELECT
        generated_at_date,
        generated_at_time,
        source_value,
        source_name
    FROM
        renamed
    WHERE
        generated_at_date IS NOT NULL
        AND generated_at_time IS NOT NULL
        AND source_value IS NOT NULL
        AND source_name IS NOT NULL
),
null_filter_and_deduped AS (
    SELECT
        generated_at_date,
        generated_at_time,
        source_name,
        AVG(source_value) AS source_value
    FROM
        null_filter
    GROUP BY
        generated_at_date,
        generated_at_time,
        source_name
)
SELECT
    *
FROM
    null_filter_and_deduped
