with sourcename as ( SELECT DISTINCT SOURCE FROM {{ ref('stg_piv_fingrid') }})
SELECT
{{dbt_utils.generate_surrogate_key(['Source'])}} as SourceKey,
SOURCE as SourceName
FROM sourcename 