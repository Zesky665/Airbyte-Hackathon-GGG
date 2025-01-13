WITH dates AS (
    SELECT
*
    FROM
        {{ ref('dim_date') }}
),
times AS (
    SELECT
*
    FROM
        {{ ref('dim_time') }}
),
sources AS (
    SELECT
        *
    FROM
        {{ ref('dim_sources') }}
),
fcttable AS (
    SELECT
        *
    FROM
        {{ ref('fct_sources') }}
),
joinedtable AS (
    SELECT
sources.source_name,
fcttable.source_value,
{{ dbt_utils.star(from=ref('dim_date'), except=["date_key"], relation_alias='dates')}},
{{ dbt_utils.star(from=ref('dim_time'), except=["time_key"], relation_alias='times')}}
    FROM
        fcttable
        INNER JOIN dates
        ON fcttable.date_key = dates.date_key
        INNER JOIN times
        ON fcttable.time_key = times.time_key
        INNER JOIN sources
        ON fcttable.source_key = sources.source_key
)
SELECT
    *
FROM
    joinedtable
