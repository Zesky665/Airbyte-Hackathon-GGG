with dategentable as (
SELECT DISTINCT
dates::DATE as date_value,
EXTRACT(YEAR FROM dates)::SMALLINT as year_value,
EXTRACT(MONTH FROM dates)::TINYINT as month_value,
monthname(dates) as month_name,
EXTRACT(DAY FROM dates)::SMALLINT as day_value,
dayname(dates) as week_day_name,
strftime(dates, '%u')::SMALLINT as week_day_value,
strftime(dates, '%V')::SMALLINT as iso_week_num,
CASE 
WHEN EXTRACT(MONTH FROM dates) BETWEEN 1 and 3 THEN 1::TINYINT 
WHEN EXTRACT(MONTH FROM dates) BETWEEN 4 and 6 THEN 2::TINYINT  
WHEN EXTRACT(MONTH FROM dates) BETWEEN 7 and 9 THEN 3::TINYINT 
WHEN EXTRACT(MONTH FROM dates) BETWEEN 9 and 12 THEN 4::TINYINT 
END as quarter_value
FROM (
SELECT unnest(generate_series('2014-01-01'::DATE,'2025-12-31'::DATE, '1 day')) as dates
) as dategen
)
SELECT 
{{dbt_utils.generate_surrogate_key(['date_value'])}} as date_key,
dategentable.*
FROM dategentable 