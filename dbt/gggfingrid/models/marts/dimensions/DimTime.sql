WITH timegentable as (
select DISTINCT
--strftime(hourminute, '%H%M')::SMALLINT AS TimeKey, -- generating key using dbt utils
hourminute::TIME as TimeValue,    
strftime(hourminute, '%H:%M') AS TimeOfDayStr,
strftime(hourminute, '%H:%M %p') AS TimeOfDayStrExt,
extract(hour from hourminute)::SMALLINT as Hour, 
CAST(extract(hour from hourminute)*60 + extract(minute from hourminute) as SMALLINT) as Minute,
strftime(hourminute - (extract(minute from hourminute)::integer % 15 || 'minutes')::interval, '%H:%M') || ' – ' ||
strftime(hourminute - (extract(minute from hourminute)::integer % 15 || 'minutes')::interval + '14 minutes'::interval, '%H:%M')
		as QuarterHour,
	case when strftime(hourminute, '%H:%M') between '06:00' and '11:59'
		then 'Morning'
	     when strftime(hourminute, '%H:%M') between '12:00' and '17:59'
		then 'Afternoon'
	     when strftime(hourminute, '%H:%M') between '18:00' and '23:59'
		then 'Evening'
	     else 'Night'
	end as DaytimeName,
	case when strftime(hourminute, '%H:%M') between '06:00' and '17:59' then 'Day'
	     else 'Night'
	end AS DayNight,
    strftime(hourminute, '%p') as AMPM
FROM (
	SELECT unnest(generate_series(TIMESTAMP without TIME zone '2016-10-16', TIMESTAMP without TIME zone '2016-10-17', '1 minute')) as hourminute
	) AS TQ
ORDER BY TimeValue
)
SELECT 
{{dbt_utils.generate_surrogate_key(['TimeValue'])}} as TimeKey,
timegentable.*
FROM timegentable