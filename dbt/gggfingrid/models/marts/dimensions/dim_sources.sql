WITH sourcename AS (
    SELECT
        DISTINCT source_name
    FROM
        {{ ref('int_fct_sources_unioned') }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['source_name']) }} AS source_key,
    source_name AS source_name
FROM
    sourcename
