WITH unpiv as (
    SELECT * FROM {{ ref('stg_fingrid') }}
),
pivottable as (
UNPIVOT unpiv
ON COLUMNS(* EXCLUDE (GeneratedAtDate, GeneratedAtTime))
INTO
    NAME Source
    VALUE GeneratedPower
)

SELECT * FROM pivottable 