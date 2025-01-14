import marimo

__generated_with = "0.10.9"
app = marimo.App(width="full")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Time Dimension Notebook""")
    return


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


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Time Dimension Table Create

        The following is the SQL Query used to create the Time Dimension Table
        """
    )
    return


@app.cell
def _(hourminute, mo):
    timedf = mo.sql(
        f"""
        -- This is the Query to generate a Times Table
        -- added in an AM/PM Column and changed the DaytimeName column
        -- changed the ints to smallints to be more efficient
        -- Added the TimeKey
        -- Added the timeofdayExt
        -- Changed the from clause Generate Series
        -- Changed the Number of day periods column to be in Quarters so Quarter of a day
        -- source: https://wiki.postgresql.org/wiki/Date_and_Time_dimensions
        select DISTINCT
        strftime(hourminute, '%H%M')::SMALLINT AS TimeKey,
        hourminute::TIME as TimeValue,    
        strftime(hourminute, '%H:%M') AS TimeOfDayStr,
        strftime(hourminute, '%H:%M %p') AS TimeOfDayStrExt,
        	-- Hour of the day (0 - 23)
        extract(hour from hourminute)::SMALLINT as Hour, 
        	-- Minute of the day (0 - 1439)
        CAST(extract(hour from hourminute)*60 + extract(minute from hourminute) as SMALLINT) as Minute,
        	-- Extract and format quarter hours
        strftime(hourminute - (extract(minute from hourminute)::integer % 15 || 'minutes')::interval, '%H:%M') || ' – ' ||
        strftime(hourminute - (extract(minute from hourminute)::integer % 15 || 'minutes')::interval + '14 minutes'::interval, '%H:%M')
        		as QuarterHour,
        -- Names of day periods, split into quarters
        	case when strftime(hourminute, '%H:%M') between '06:00' and '11:59'
        		then 'Morning'
        	     when strftime(hourminute, '%H:%M') between '12:00' and '17:59'
        		then 'Afternoon'
        	     when strftime(hourminute, '%H:%M') between '18:00' and '23:59'
        		then 'Evening'
        	     else 'Night'
        	end as DaytimeName,
        -- Indicator of day or night, split by half
        	case when strftime(hourminute, '%H:%M') between '06:00' and '17:59' then 'Day'
        	     else 'Night'
        	end AS DayNight,
            strftime(hourminute, '%p') as AMPM
        FROM (
        	SELECT unnest(generate_series(TIMESTAMP without TIME zone '2016-10-16', TIMESTAMP without TIME zone '2016-10-17', '1 minute')) as hourminute
        	) AS TQ
        ORDER BY TimeValue
        """
    )
    return (timedf,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### Notes and Sources
        Interesting findings, DuckDB does support generate_series() [link](https://stackoverflow.com/questions/75159150/in-duckdb-how-do-i-generate-a-range-of-timestamps-between-a-start-date-column-a). Also this is the source of the date and time dimension table in postgres [link](https://wiki.postgresql.org/wiki/Date_and_Time_dimensions) [link2](https://dba.stackexchange.com/questions/175963/how-do-i-generate-a-date-series-in-postgresql)

        Here's a link using the RANGE function in DuckDB to generate a Date Dimension Table [link](https://gist.github.com/adityawarmanfw/0612333605d351f2f1fe5c87e1af20d2)

        Finally, Kimball has an excel file we can use for the date dimension table [link](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/books/data-warehouse-dw-toolkit/)

        Actually [Microsoft](https://learn.microsoft.com/en-us/fabric/data-warehouse/dimensional-modeling-dimension-tables) mentions just using a date dimension generator [like this one](https://dimdates.com/)

        Kimball has some posts about the Time Dimension Table: [note 1](https://www.kimballgroup.com/2004/02/design-tip-51-latest-thinking-on-time-dimension-tables/) and [note 2](https://www.kimballgroup.com/1997/07/its-time-for-time/) among others that can be googled.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        DuckDB doesn't support converting a time data type to text. the function strftime only takes DATE or TIMESTAMP but not Time

        Generate Series does not take the TIME Data Type only TIMESTAMP or DATE
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        --Table Creation DDL
        /*
        CREATE TABLE IF NOT EXISTS "FG_DWH".mockdashstaging.TimeDim AS 
        select DISTINCT
        strftime(hourminute, '%H%M')::SMALLINT AS TimeKey,
        hourminute::TIME as TimeValue,    
        strftime(hourminute, '%H:%M') AS TimeOfDayStr,
        strftime(hourminute, '%H:%M %p') AS TimeOfDayStrExt,
        	-- Hour of the day (0 - 23)
        extract(hour from hourminute)::SMALLINT as Hour, 
        	-- Minute of the day (0 - 1439)
        CAST(extract(hour from hourminute)*60 + extract(minute from hourminute) as SMALLINT) as Minute,
        	-- Extract and format quarter hours
        strftime(hourminute - (extract(minute from hourminute)::integer % 15 || 'minutes')::interval, '%H:%M') || ' – ' ||
        strftime(hourminute - (extract(minute from hourminute)::integer % 15 || 'minutes')::interval + '14 minutes'::interval, '%H:%M')
        		as QuarterHour,
        -- Names of day periods, split into quarters
        	case when strftime(hourminute, '%H:%M') between '06:00' and '11:59'
        		then 'Morning'
        	     when strftime(hourminute, '%H:%M') between '12:00' and '17:59'
        		then 'Afternoon'
        	     when strftime(hourminute, '%H:%M') between '18:00' and '23:59'
        		then 'Evening'
        	     else 'Night'
        	end as DaytimeName,
        -- Indicator of day or night, split by half
        	case when strftime(hourminute, '%H:%M') between '06:00' and '17:59' then 'Day'
        	     else 'Night'
        	end AS DayNight,
            strftime(hourminute, '%p') as AMPM
        FROM (
        	SELECT unnest(generate_series(TIMESTAMP without TIME zone '2016-10-16', TIMESTAMP without TIME zone '2016-10-17', '1 minute')) as hourminute
        	) AS DQ
        ORDER BY TimeValue
        */
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        DDL for TimeDimension Table
        ```sql
        CREATE TABLE IF NOT EXISTS "FG_DWH".mockdashstaging.TimeDim AS 
        select DISTINCT
        strftime(hourminute, '%H%M')::SMALLINT AS TimeKey,
        hourminute::TIME as TimeValue,    
        strftime(hourminute, '%H:%M') AS TimeOfDayStr,
        strftime(hourminute, '%H:%M %p') AS TimeOfDayStrExt,
        	-- Hour of the day (0 - 23)
        extract(hour from hourminute)::SMALLINT as Hour, 
        	-- Minute of the day (0 - 1439)
        CAST(extract(hour from hourminute)*60 + extract(minute from hourminute) as SMALLINT) as Minute,
        	-- Extract and format quarter hours
        strftime(hourminute - (extract(minute from hourminute)::integer % 15 || 'minutes')::interval, '%H:%M') || ' – ' ||
        strftime(hourminute - (extract(minute from hourminute)::integer % 15 || 'minutes')::interval + '14 minutes'::interval, '%H:%M')
        		as QuarterHour,
        -- Names of day periods, split into quarters
        	case when strftime(hourminute, '%H:%M') between '06:00' and '11:59'
        		then 'Morning'
        	     when strftime(hourminute, '%H:%M') between '12:00' and '17:59'
        		then 'Afternoon'
        	     when strftime(hourminute, '%H:%M') between '18:00' and '23:59'
        		then 'Evening'
        	     else 'Night'
        	end as DaytimeName,
        -- Indicator of day or night, split by half
        	case when strftime(hourminute, '%H:%M') between '06:00' and '17:59' then 'Day'
        	     else 'Night'
        	end AS DayNight,
            strftime(hourminute, '%p') as AMPM
        FROM (
        	SELECT unnest(generate_series(TIMESTAMP without TIME zone '2016-10-16', TIMESTAMP without TIME zone '2016-10-17', '1 minute')) as hourminute
        	) AS DQ
        ORDER BY TimeValue
        ```
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""Test to see if Table was created""")
    return


@app.cell(disabled=True)
def _(FG_DWH, TimeDim, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM "FG_DWH".mockdashstaging."TimeDim"
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""Test with the Raw table.""")
    return


@app.cell(hide_code=True)
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


@app.cell
def _(FG_DWH, mo, totalpowerraw2022_2023):
    rawdatedf = mo.sql(
        f"""
        -- basic transformations to breakout the start and end timestamp into a date and time seperate column 
        WITH rawtable AS (
        SELECT startTime as startDateTime, endTime as endDateTime FROM "FG_DWH".mockdashraw.totalpowerraw2022_2023 USING SAMPLE 500
            )
        SELECT startDateTime, 
        startDateTime::DATE as startDate,
        strftime(startDateTime, '%H:%M') as startTime,
        endDateTime, 
        endDateTime::DATE as endDate, 
        strftime(endDateTime, '%H:%M') as endTime
        FROM rawtable
        """
    )
    return (rawdatedf,)


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        -- Quick Exploration setting the previous two outputs assigning to variables. Used to double check the Join of the Date Dimension to the Fact Table. 
        --SUMMARIZE SELECT startTime, startDate, timedf."TimeKey" 
        --from rawdatedf 
        --inner join timedf ON rawdatedf."startTime" = timedf."TimeOfDay"
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### Tasks
        Okay so this generates the Time Dimension Table. I need to create a time dimension key.

        I need to break out the startTime and endTime columns into 4 columns, two date and two time columns.

        - [x] Create Time Dimension Key 
        - [ ] Breakout Columns in the Fact Table to prepare to add the keys
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        SELECT hourminute::TIME
        FROM (
        	SELECT unnest(generate_series(TIMESTAMP without TIME zone '2016-10-16', TIMESTAMP without TIME zone '2016-10-17', '1 minute')) as hourminute
        	) AS DQ
        """
    )
    return


if __name__ == "__main__":
    app.run()
