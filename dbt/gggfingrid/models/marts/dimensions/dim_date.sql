WITH dategentable AS (
    SELECT
        DISTINCT dates :: DATE AS date_value,
        EXTRACT(
            YEAR
            FROM
                dates
        ) :: SMALLINT AS year_value,
        EXTRACT(
            MONTH
            FROM
                dates
        ) :: tinyint AS month_value,
        MONTHNAME(dates) AS month_name,
        EXTRACT(
            DAY
            FROM
                dates
        ) :: SMALLINT AS day_value,
        DAYNAME(dates) AS week_day_name,
        strftime(
            dates,
            '%u'
        ) :: SMALLINT AS week_day_value,
        strftime(
            dates,
            '%V'
        ) :: SMALLINT AS iso_week_num,
        CASE
            WHEN EXTRACT(
                MONTH
                FROM
                    dates
            ) BETWEEN 1
            AND 3 THEN 1 :: tinyint
            WHEN EXTRACT(
                MONTH
                FROM
                    dates
            ) BETWEEN 4
            AND 6 THEN 2 :: tinyint
            WHEN EXTRACT(
                MONTH
                FROM
                    dates
            ) BETWEEN 7
            AND 9 THEN 3 :: tinyint
            WHEN EXTRACT(
                MONTH
                FROM
                    dates
            ) BETWEEN 9
            AND 12 THEN 4 :: tinyint
        END AS quarter_value
    FROM
        (
            SELECT
                unnest(
                    generate_series(
                        '2014-01-01' :: DATE,
                        '2025-12-31' :: DATE,
                        '1 day'
                    )
                ) AS dates
        ) AS dategen
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['date_value']) }} AS date_key,
    dategentable.*
FROM
    dategentable
