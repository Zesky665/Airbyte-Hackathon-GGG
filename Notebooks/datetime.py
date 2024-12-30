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
def _(minute, mo):
    _df = mo.sql(
        f"""
        -- added in an AM/PM Column
        select DISTINCT
        strftime(minute, '%H%M')::BIGINT AS TimeKey,
        strftime(minute, '%H:%M') AS TimeOfDay,
        strftime(minute, '%H:%M %p') AS TimeOfDayExt,
        	-- Hour of the day (0 - 23)
        extract(hour from minute) as Hour, 
        	-- Minute of the day (0 - 1439)
        	extract(hour from minute)*60 + extract(minute from minute) as Minute,
        	-- Extract and format quarter hours
        	strftime(minute - (extract(minute from minute)::integer % 15 || 'minutes')::interval, '%H:%M') ||
        	' – ' ||
        	strftime(minute - (extract(minute from minute)::integer % 15 || 'minutes')::interval + '14 minutes'::interval, '%H:%M')
        		as QuarterHour,
        	-- Names of day periods
        	case when strftime(minute, '%H:%M') between '06:00' and '11:59'
        		then 'Morning'
        	     when strftime(minute, '%H:%M') between '12:00' and '17:59'
        		then 'Afternoon'
        	     when strftime(minute, '%H:%M') between '18:00' and '22:29'
        		then 'Evening'
        	     else 'Night'
        	end as DaytimeName,
        	-- Indicator of day or night
        	case when strftime(minute, '%H:%M') between '07:00' and '19:59' then 'Day'
        	     else 'Night'
        	end AS DayNight,
            strftime(minute, '%p') as AMPM
        FROM (
        	SELECT unnest(generate_series(TIMESTAMP without TIME zone '2016-10-16', TIMESTAMP without TIME zone '2016-10-17', '1 minute')) as minute
        	) AS DQ
        ORDER BY TimeOfDay
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""Test with the Raw table.""")
    return


@app.cell
def _(minute, mo, totalpowerraw2022_2023):
    _df = mo.sql(
        f"""
        WITH generated_table as
        (select DISTINCT 
        strftime(minute, '%H:%M') AS TimeOfDay,
        strftime(minute, '%H:%M %p') AS TimeOfDayExt,
        	-- Hour of the day (0 - 23)
        extract(hour from minute) as Hour, 
        	-- Minute of the day (0 - 1439)
        	extract(hour from minute)*60 + extract(minute from minute) as Minute,
        	-- Extract and format quarter hours
        	strftime(minute - (extract(minute from minute)::integer % 15 || 'minutes')::interval, '%H:%M') ||
        	' – ' ||
        	strftime(minute - (extract(minute from minute)::integer % 15 || 'minutes')::interval + '14 minutes'::interval, '%H:%M')
        		as QuarterHour,
        	-- Names of day periods
        	case when strftime(minute, '%H:%M') between '06:00' and '11:59'
        		then 'Morning'
        	     when strftime(minute, '%H:%M') between '12:00' and '17:59'
        		then 'Afternoon'
        	     when strftime(minute, '%H:%M') between '18:00' and '22:29'
        		then 'Evening'
        	     else 'Night'
        	end as DaytimeName,
        	-- Indicator of day or night
        	case when strftime(minute, '%H:%M') between '07:00' and '19:59' then 'Day'
        	     else 'Night'
        	end AS DayNight,
            strftime(minute, '%p') as AMPM
        FROM (
        	SELECT unnest(generate_series(TIMESTAMP without TIME zone '2016-10-16', TIMESTAMP without TIME zone '2016-10-17', '1 minute')) as minute
        	) AS DQ
        ORDER BY TimeOfDay),
        rawtable as (SELECT *, strftime(endTime, '%H:%M') as endtimefrmt FROM "FG_DWH".mockdashraw.totalpowerraw2022_2023 LIMIT 50)
        SELECT * FROM rawtable
        INNER JOIN generated_table ON rawtable.endtimefrmt = generated_table.TimeOfDay
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Okay so this generates the Time Dimension Table. I need to create a time dimension key.

        I need to break out the startTime and endTime columns into 4 columns, two date and two time columns.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Interesting findings, DuckDB does support generate_series() [link](https://stackoverflow.com/questions/75159150/in-duckdb-how-do-i-generate-a-range-of-timestamps-between-a-start-date-column-a). Also this is the source of the date and time dimension table in postgres [link](https://wiki.postgresql.org/wiki/Date_and_Time_dimensions) [link2](https://dba.stackexchange.com/questions/175963/how-do-i-generate-a-date-series-in-postgresql)

        Here's a link using the RANGE function in DuckDB to generate a Date Dimension Table [link](https://gist.github.com/adityawarmanfw/0612333605d351f2f1fe5c87e1af20d2)

        Finally, Kimball has an excel file we can use for the date dimension table [link](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/books/data-warehouse-dw-toolkit/)

        Actually [Microsoft](https://learn.microsoft.com/en-us/fabric/data-warehouse/dimensional-modeling-dimension-tables) mentions just using a date dimension generator [like this one](https://dimdates.com/)
        """
    )
    return


@app.cell
def _(FG_DWH, mo, totalpowerraw2022_2023):
    _df = mo.sql(
        f"""
        SELECT * FROM FG_DWH.mockdashraw.totalpowerraw2022_2023 ORDER BY endTime LIMIT 10 OFFSET 100
        """
    )
    return


@app.cell(disabled=True)
def _(generate_series, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM generate_series(1,5)
        """
    )
    return


@app.cell(disabled=True)
def _(mo):
    _df = mo.sql(
        f"""
        SELECT RANGE(DATE '2009-01-01', DATE '2013-12-31', INTERVAL 1 DAY)
        """
    )
    return


@app.cell(disabled=True)
def _(mo):
    _df = mo.sql(
        f"""
        SELECT unnest(generate_series(TIMESTAMP without TIME zone '2016-10-16', TIMESTAMP without TIME zone '2016-10-17', '1 minute'))
        """
    )
    return


@app.cell(disabled=True)
def _(minute, mo):
    _df = mo.sql(
        f"""
        -- https://wiki.postgresql.org/wiki/Date_and_Time_dimensions
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


@app.cell(disabled=True)
def _(generate_series, minute, mo):
    _df = mo.sql(
        f"""
        SELECT 
        strftime(minute, 'hh24:mi') AS TimeOfDay,
        extract(hour from minute) as Hour, 
        extract(hour from minute)*60 + extract(minute from minute) as minute
        from (SELECT '0:00'::time + (sequence.minute || ' minutes')::interval AS minute
        	FROM generate_series(0,1439) AS sequence(minute)
        	GROUP BY sequence.minute
             ) DQ
        order by 1, 2
        """
    )
    return


@app.cell(disabled=True)
def _(mo):
    _df = mo.sql(
        f"""
        SELECT TIME '00:01:00'
        """
    )
    return


@app.cell(disabled=True)
def _(mo):
    _df = mo.sql(
        f"""
        SELECT strftime('1992-01-01 02:01:00'::TIMESTAMP, '%H:%M')
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""DuckDB doesn't support converting a time data type to text. the function strftime only takes DATE or TIMESTAMP but not Time""")
    return


@app.cell(disabled=True)
def _(mo):
    _df = mo.sql(
        f"""
        -- source: https://dba.stackexchange.com/questions/175963/how-do-i-generate-a-date-series-in-postgresql, I added unnest. 
        SELECT *
        FROM (
        	SELECT unnest(generate_series(TIMESTAMP without TIME zone '2016-10-16', TIMESTAMP without TIME zone '2016-10-17', '1 minute'))
        	) AS a
        """
    )
    return


@app.cell(disabled=True)
def _(generate_series, mo):
    _df = mo.sql(
        f"""
        --alternative way using GROUP BY instead of UNNEST
        SELECT '2016-10-16 00:00'::timestamp + (sequence.minute || ' minutes')::interval AS minute
        	FROM generate_series(0,1439) AS sequence(minute)
        	GROUP BY sequence.minute
        """
    )
    return


@app.cell(disabled=True)
def _(generate_series, mo):
    _df = mo.sql(
        f"""
        EXPLAIN SELECT '2016-10-16 00:00'::timestamp + (sequence.minute || ' minutes')::interval AS minute
        	FROM generate_series(0,1439) AS sequence(minute)
        	GROUP BY sequence.minute
        """
    )
    return


@app.cell(disabled=True)
def _(generate_series, mo):
    _df = mo.sql(
        f"""
        SELECT sequence.minute
        	FROM generate_series(0,1439) AS sequence(minute)
        	GROUP BY sequence.minute
        """
    )
    return


@app.cell(disabled=True)
def _(mo):
    _df = mo.sql(
        f"""
        --testing out why group by alternative to unnest isn't working for a Timestamp based series vs an int based one from above. 
        SELECT sequence.timestamp FROM (SELECT generate_series(TIMESTAMP without TIME zone '2016-10-16', TIMESTAMP without TIME zone '2016-10-17', '1 minute')) as sequence(timestamp)
        GROUP BY sequence.timestamp
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
