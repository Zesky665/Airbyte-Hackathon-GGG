WITH timegentable AS (
    SELECT
        DISTINCT hourminute :: TIME AS time_value,
        strftime(
            hourminute,
            '%H:%M'
        ) AS time_of_day_str,
        strftime(
            hourminute,
            '%H:%M %p'
        ) AS time_of_day_str_ext,
        EXTRACT(
            HOUR
            FROM
                hourminute
        ) :: SMALLINT AS hour_value,
        CAST(
            EXTRACT(
                HOUR
                FROM
                    hourminute
            ) * 60 + EXTRACT(
                MINUTE
                FROM
                    hourminute
            ) AS SMALLINT
        ) AS minute_value,
        strftime(
            hourminute - (
                EXTRACT(
                    MINUTE
                    FROM
                        hourminute
                ) :: INTEGER % 15 || 'minutes'
            ) :: INTERVAL,
            '%H:%M'
        ) || ' – ' || strftime(
            hourminute - (
                EXTRACT(
                    MINUTE
                    FROM
                        hourminute
                ) :: INTEGER % 15 || 'minutes'
            ) :: INTERVAL + '14 minutes' :: INTERVAL,
            '%H:%M'
        ) AS quarter_hour,
        CASE
            WHEN strftime(
                hourminute,
                '%H:%M'
            ) BETWEEN '06:00'
            AND '11:59' THEN 'Morning'
            WHEN strftime(
                hourminute,
                '%H:%M'
            ) BETWEEN '12:00'
            AND '17:59' THEN 'Afternoon'
            WHEN strftime(
                hourminute,
                '%H:%M'
            ) BETWEEN '18:00'
            AND '23:59' THEN 'Evening'
            ELSE 'Night'
        END AS day_period_name,
        CASE
            WHEN strftime(
                hourminute,
                '%H:%M'
            ) BETWEEN '06:00'
            AND '17:59' THEN 'Day'
            ELSE 'Night'
        END AS day_or_night,
        strftime(
            hourminute,
            '%p'
        ) AS ampm
    FROM
        (
            SELECT
                unnest(
                    generate_series(
                        TIMESTAMP without TIME ZONE '2016-10-16',
                        TIMESTAMP without TIME ZONE '2016-10-17',
                        '1 minute'
                    )
                ) AS hourminute
        ) AS tq
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['time_value']) }} AS time_key,
    timegentable.*
FROM
    timegentable
