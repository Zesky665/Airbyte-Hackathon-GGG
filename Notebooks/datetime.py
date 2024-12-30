import marimo

__generated_with = "0.10.8"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        ATTACH IF NOT EXISTS 'md:FG_DWH'
        """
    )
    return (FG_DWH,)


@app.cell
def _(generate_series, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM generate_series(1,5)
        """
    )
    return


@app.cell(disabled=True)
def _(minute, mo):
    _df = mo.sql(
        f"""
        select to_char(minute, 'hh24:mi') AS TimeOfDay,
        	-- Hour of the day (0 - 23)
        	extract(hour from minute) as Hour, 
        	-- Extract and format quarter hours
        	to_char(minute - (extract(minute from minute)::integer % 15 || 'minutes')::interval, 'hh24:mi') ||
        	' – ' ||
        	to_char(minute - (extract(minute from minute)::integer % 15 || 'minutes')::interval + '14 minutes'::interval, 'hh24:mi')
        		as QuarterHour,
        	-- Minute of the day (0 - 1439)
        	extract(hour from minute)*60 + extract(minute from minute) as minute,
        	-- Names of day periods
        	case when to_char(minute, 'hh24:mi') between '06:00' and '08:29'
        		then 'Morning'
        	     when to_char(minute, 'hh24:mi') between '08:30' and '11:59'
        		then 'AM'
        	     when to_char(minute, 'hh24:mi') between '12:00' and '17:59'
        		then 'PM'
        	     when to_char(minute, 'hh24:mi') between '18:00' and '22:29'
        		then 'Evening'
        	     else 'Night'
        	end as DaytimeName,
        	-- Indicator of day or night
        	case when to_char(minute, 'hh24:mi') between '07:00' and '19:59' then 'Day'
        	     else 'Night'
        	end AS DayNight
        from (SELECT '0:00'::time + (sequence.minute || ' minutes')::interval AS minute
        	FROM generate_series(0,1439) AS sequence(minute)
        	GROUP BY sequence.minute
             ) DQ
        order by 1
        """
    )
    return


@app.cell
def _(generate_series, minute, mo):
    _df = mo.sql(
        f"""
        SELECT extract(hour from minute)
        from (SELECT '0:00'::time + (sequence.minute || ' minutes')::interval AS minute
        	FROM generate_series(0,1439) AS sequence(minute)
        	GROUP BY sequence.minute
             ) DQ
        order by 1
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
