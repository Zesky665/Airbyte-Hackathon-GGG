{{
  config(
    materialized = 'ephemeral',
    )
}}
SELECT * 
FROM {{ source('fingrid', 'ElectrictyConsumptionRaw') }}