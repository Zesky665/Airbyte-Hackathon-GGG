{{ config(
  materialized = 'view',
) }}

WITH uniontable AS (

  SELECT
    *
  FROM
    {{ ref('stg_fingrid__hydro') }}
  UNION
  SELECT
    *
  FROM
    {{ ref('stg_fingrid__totalelec') }}
  UNION
  SELECT
    *
  FROM
    {{ ref('stg_fingrid__nuclear') }}
  UNION
  SELECT
    *
  FROM
    {{ ref('stg_fingrid__wind') }}
)
SELECT
  *
FROM
  uniontable
