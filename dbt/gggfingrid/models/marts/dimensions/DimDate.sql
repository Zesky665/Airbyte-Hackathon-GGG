with dategentable as (
SELECT 
--strftime(dates, '%Y%m%d')::INT as DateKey, -- date key using dbt generate surrogate key
--{{dbt_utils.generate_surrogate_key(['DateValue'])}} as DateKey,
dates::DATE as DateValue,
EXTRACT(YEAR FROM dates)::SMALLINT as YearVal,
EXTRACT(MONTH FROM dates)::TINYINT as MonthVal,
monthname(dates) as MonthName,
EXTRACT(DAY FROM dates)::SMALLINT as DayVal,
dayname(dates) as WeekDayName,
strftime(dates, '%u')::SMALLINT as WeekDayVal,
strftime(dates, '%V')::SMALLINT as IsoWeekNum,
CASE 
WHEN EXTRACT(MONTH FROM dates) BETWEEN 1 and 3 THEN 1::TINYINT 
WHEN EXTRACT(MONTH FROM dates) BETWEEN 4 and 6 THEN 2::TINYINT  
WHEN EXTRACT(MONTH FROM dates) BETWEEN 7 and 9 THEN 3::TINYINT 
WHEN EXTRACT(MONTH FROM dates) BETWEEN 9 and 12 THEN 4::TINYINT 
END as QuarterValue
FROM (
SELECT unnest(generate_series('2022-01-01'::DATE,'2023-12-31'::DATE, '1 day')) as dates
) as dategen
)
SELECT 
{{dbt_utils.generate_surrogate_key(['DateValue'])}} as DateKey,
dategentable.*
FROM dategentable 