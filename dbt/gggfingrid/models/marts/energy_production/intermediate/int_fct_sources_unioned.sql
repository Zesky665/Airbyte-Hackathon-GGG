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
  UNION 
  SELECT
    *
  FROM 
    {{ ref('stg_fingrid__consumption') }}
)
SELECT
  *
FROM
  uniontable
