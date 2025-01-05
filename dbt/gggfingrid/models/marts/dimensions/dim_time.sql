WITH timegentable as (
select DISTINCT
hourminute::TIME as time_value,    
strftime(hourminute, '%H:%M') AS time_of_day_str,
strftime(hourminute, '%H:%M %p') AS time_of_day_str_ext,
extract(hour from hourminute)::SMALLINT as hour_value, 
CAST(extract(hour from hourminute)*60 + extract(minute from hourminute) as SMALLINT) as minute_value,
strftime(hourminute - (extract(minute from hourminute)::integer % 15 || 'minutes')::interval, '%H:%M') || ' – ' ||
strftime(hourminute - (extract(minute from hourminute)::integer % 15 || 'minutes')::interval + '14 minutes'::interval, '%H:%M')
		as quarter_hour,
	case when strftime(hourminute, '%H:%M') between '06:00' and '11:59'
		then 'Morning'
	     when strftime(hourminute, '%H:%M') between '12:00' and '17:59'
		then 'Afternoon'
	     when strftime(hourminute, '%H:%M') between '18:00' and '23:59'
		then 'Evening'
	     else 'Night'
	end as day_period_name,
	case when strftime(hourminute, '%H:%M') between '06:00' and '17:59' then 'Day'
	     else 'Night'
	end AS day_or_night,
    strftime(hourminute, '%p') as AMPM
FROM (
	SELECT unnest(generate_series(TIMESTAMP without TIME zone '2016-10-16', TIMESTAMP without TIME zone '2016-10-17', '1 minute')) as hourminute
	) AS TQ
)
SELECT 
{{dbt_utils.generate_surrogate_key(['time_value'])}} as time_key,
timegentable.*
FROM timegentable