with sourcename as ( SELECT DISTINCT source_name FROM {{ ref('int_fct_sources_unioned') }})
SELECT
{{dbt_utils.generate_surrogate_key(['source_name'])}} as source_key,
source_name as source_name
FROM sourcename 