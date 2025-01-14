WITH hourly as (SELECT * FROM {{ ref('grid_pivot') }}),
groupbytable as (
SELECT 
date_value, 
SUM(wind) as wind,
SUM(hydro) as hydro, 
SUM(nuclear) as nuclear, 
SUM(production) as production,
SUM(consumption) as consumption
FROM hourly 
GROUP BY date_value
)
SELECT *, (production - (wind + hydro + nuclear)) as nongreen
FROM groupbytable
ORDER BY date_value 