import marimo

__generated_with = "0.10.9"
app = marimo.App(width="full")


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        # Energy FACT TABLE AD HOC work
        An area to work on the Raw data table and work on the star schema as well as to do some data explorations
        """
    )
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


@app.cell
def _(mo):
    mo.md(r"""Let's start investigating the RAW table for the Schema and how to transform it into an intermediate table then a fact table""")
    return


@app.cell
def _():
    return


@app.cell
def _(FG_DWH, mo, totalpowerraw2022_2023):
    _df = mo.sql(
        f"""
        SELECT * FROM FG_DWH.mockdashraw.totalpowerraw2022_2023 
        USING SAMPLE 100
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        DESCRIBE "FG_DWH".mockdashraw.totalpowerraw2022_2023
        """
    )
    return


@app.cell
def _(FG_DWH, mo, totalpowerraw2022_2023):
    _df = mo.sql(
        f"""
        SUMMARIZE SELECT * FROM "FG_DWH".mockdashraw.totalpowerraw2022_2023
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### Initial Draft of the intermediate table

        This will eventually become the fact table. I'm working on the Date and Time dimensions first. 

        After that I will move the sources to their own dimension table.

        - [x] Work on Time Dimension Table
        - [x] Transform Raw Table for the Time Dimension Table
        - [ ] Create Intermediate Table from Raw Table with TimeDim Key
        - [ ] Work on Date Dimension Table
        - [x] Transform Raw Table for Date Dimension Table
        - [ ] Create Intermediate Table from previous intermediate table for DateDim Key
        - [ ] Create Source Dimension Table
        """
    )
    return


@app.cell
def _(FG_DWH, mo, totalpowerraw2022_2023):
    testdf = mo.sql(
        f"""
        SELECT 
        --startTime::DATE as StartDate,
        --startTime::TIME as StartTime,
        endTime::DATE as GeneratedAtTime,
        endTime::TIME as GeneratedAtDate, 
        "Wind power production - real time data" as WindPowerGenerated,
        "Nuclear power production - real time data" as NuclearPowerGenerated,
        "Hydro power production - real time data" as HydroPowerGenerated,
        "Electricity production in Finland - real time data" as AllPowerGenerated
        FROM FG_DWH.mockdashraw.totalpowerraw2022_2023
        USING SAMPLE 100
        """
    )
    return (testdf,)


@app.cell
def _(FG_DWH, TimeDim, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM "FG_DWH".mockdashstaging."TimeDim" USING SAMPLE 100;
        """
    )
    return


@app.cell
def _(mo, testdf):
    _df = mo.sql(
        f"""
        DESCRIBE TABLE testdf;
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""Confirming that StartTime and EndTime from the RawTable is the same value.""")
    return


@app.cell
def _(FG_DWH, mo, totalpowerraw2022_2023):
    _df = mo.sql(
        f"""
        SELECT * 
        FROM "FG_DWH".mockdashraw.totalpowerraw2022_2023
        WHERE 1=1 
        AND startTime NOT IN (SELECT endTime FROM "FG_DWH".mockdashraw.totalpowerraw2022_2023)
        """
    )
    return


@app.cell
def _(FG_DWH, mo, totalpowerraw2022_2023):
    _df = mo.sql(
        f"""
        SELECT * 
        FROM "FG_DWH".mockdashraw.totalpowerraw2022_2023
        WHERE 1=1 
        AND endTime NOT IN (SELECT startTime FROM "FG_DWH".mockdashraw.totalpowerraw2022_2023)
        """
    )
    return


@app.cell
def _(FG_DWH, mo, totalpowerraw2022_2023):
    _df = mo.sql(
        f"""
        --logic check
        SELECT * 
        FROM "FG_DWH".mockdashraw.totalpowerraw2022_2023
        WHERE 1=1 
        AND endTime IN (SELECT startTime FROM "FG_DWH".mockdashraw.totalpowerraw2022_2023)
        LIMIT 5
        """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        Creating a temporary table for the visualizations dropping the StartTime column, breaking out the EndTime column into EndTime into an endtime and enddate column. 

        Also renaming the source generated column names
        """
    )
    return


@app.cell
def _(FG_DWH, mo, totalpowerraw2022_2023):
    _df = mo.sql(
        f"""
        -- Select portion of the CTAS Query 
        SELECT 
        endTime::DATE as GeneratedAtDate,
        endTime::TIME as GeneratedAtTime, 
        "Wind power production - real time data" as WindPowerGenerated,
        "Nuclear power production - real time data" as NuclearPowerGenerated,
        "Hydro power production - real time data" as HydroPowerGenerated,
        "Electricity production in Finland - real time data" as AllPowerGenerated
        FROM FG_DWH.mockdashraw.totalpowerraw2022_2023
        USING SAMPLE 100
        """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        Temp Table DDL 
        ```sql
        CREATE TABLE IF NOT EXISTS "FG_DWH".mockdashstaging.tempallpower2022_2023 AS 
        SELECT 
        endTime::DATE as GeneratedAtDate,
        endTime::TIME as GeneratedAtTime, 
        "Wind power production - real time data" as WindPowerGenerated,
        "Nuclear power production - real time data" as NuclearPowerGenerated,
        "Hydro power production - real time data" as HydroPowerGenerated,
        "Electricity production in Finland - real time data" as AllPowerGenerated
        FROM FG_DWH.mockdashraw.totalpowerraw2022_2023
        ```
        """
    )
    return


@app.cell
def _(FG_DWH, mo, tempallpower2022_2023):
    _df = mo.sql(
        f"""
        SELECT * FROM "FG_DWH".mockdashstaging.tempallpower2022_2023 USING SAMPLE 100
        """
    )
    return


@app.cell
def _(mo, tempallpower2022_2023):
    _df = mo.sql(
        f"""
        DESCRIBE TABLE "FG_DWH".mockdashstaging.tempallpower2022_2023
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""Working to replace the GeneratedAtTime with the DimTable""")
    return


@app.cell
def _(FG_DWH, TimeDim, mo, viewdf):
    _df = mo.sql(
        f"""
        SUMMARIZE
        SELECT viewdf."GeneratedAtDate", viewdf."GeneratedAtTime", timedim."TimeKey", viewdf."AllPowerGenerated"
        FROM viewdf 
        INNER JOIN "FG_DWH".mockdashstaging."TimeDim" as timedim
        ON viewdf."GeneratedAtTime" = timedim."TimeValue"
        """
    )
    return


@app.cell
def _(mo):
    mo.md("""I'm going to know create an intermediate view to include the time key""")
    return


@app.cell
def _(FG_DWH, TimeDim, mo, mockdashstaging, tempallpower2022_2023):
    _df = mo.sql(
        f"""
        CREATE OR REPLACE VIEW "FG_DWH".mockdashstaging.v_powerandtime AS 
        SELECT 
        power."GeneratedAtDate",
        timedim."TimeKey",
        power."HydroPowerGenerated",
        power."NuclearPowerGenerated",
        power."WindPowerGenerated",
        power."AllPowerGenerated"
        FROM "FG_DWH".mockdashstaging.tempallpower2022_2023 as power 
        INNER JOIN "FG_DWH".mockdashstaging."TimeDim" as timedim
        ON power."GeneratedAtTime" = timedim."TimeValue"
        """
    )
    return (v_powerandtime,)


@app.cell
def _(mo, v_powerandtime):
    _df = mo.sql(
        f"""
        DESCRIBE TABLE "FG_DWH".mockdashstaging.v_powerandtime
        """
    )
    return


@app.cell
def _(FG_DWH, mo, v_powerandtime):
    _df = mo.sql(
        f"""
        SELECT * 
        FROM "FG_DWH".mockdashstaging.v_powerandtime
        USING SAMPLE 100
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""Now the same process for the Date Key""")
    return


@app.cell
def _(DateDim, FG_DWH, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM "FG_DWH".mockdashstaging.DateDim USING SAMPLE 100
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""Now to create an intermediate level including the date dimension table""")
    return


@app.cell
def _(DateDim, FG_DWH, mo, v_powerandtime):
    _df = mo.sql(
        f"""
        WITH powertime as (
            SELECT * FROM "FG_DWH".mockdashstaging.v_powerandtime USING SAMPLE 100 
        )
        SELECT *
        FROM "FG_DWH".mockdashstaging.DateDim dates
        INNER JOIN powertime 
        ON dates."DateValue" = powertime.GeneratedAtDate
        """
    )
    return


@app.cell
def _(DateDim, FG_DWH, mo, mockdashstaging, v_powerandtime):
    _df = mo.sql(
        f"""
        -- DDL for view creation
        CREATE OR REPLACE VIEW "FG_DWH".mockdashstaging.v_powerdatetime AS 
        SELECT 
        dates."DateKey",
        powertime."TimeKey",
        powertime."WindPowerGenerated",
        powertime."HydroPowerGenerated",
        powertime."NuclearPowerGenerated",
        powertime."AllPowerGenerated"
        FROM "FG_DWH".mockdashstaging.DateDim dates
        INNER JOIN "FG_DWH".mockdashstaging.v_powerandtime as powertime
        ON dates."DateValue" = powertime."GeneratedAtDate"
        """
    )
    return (v_powerdatetime,)


@app.cell
def _(mo, v_powerdatetime):
    _df = mo.sql(
        f"""
        DESCRIBE TABLE "FG_DWH".mockdashstaging.v_powerdatetime
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""Just noticed I didn't make the datekey a small int. Just went and changed it in the DDL.""")
    return


@app.cell
def _(mo):
    mo.md(r"""Test for joins""")
    return


@app.cell
def _(DateDim, FG_DWH, TimeDim, mo, v_powerdatetime):
    _df = mo.sql(
        f"""
        WITH power as ( SELECT * FROM "FG_DWH".mockdashstaging.v_powerdatetime USING SAMPLE 100)
        SELECT *
        FROM power
        INNER JOIN "FG_DWH".mockdashstaging."DateDim" dates
        ON dates."DateKey" = power.DateKey
        INNER JOIN "FG_DWH".mockdashstaging."TimeDim" times
        ON times."TimeKey" = power.TimeKey
        """
    )
    return


@app.cell
def _():
    return


@app.cell
def _(mo):
    mo.md(r"""### Create the Unpivot of the raw table and create a new source dim key connection""")
    return


@app.cell
def _(FG_DWH, mo, mockdashstaging):
    _df = mo.sql(
        f"""
        CREATE OR REPLACE VIEW "FG_DWH".mockdashstaging.v_powerunpivot AS
        WITH unpiv as (
        UNPIVOT "FG_DWH".mockdashstaging.v_powerdatetime
        ON COLUMNS(* EXCLUDE (DateKey, TimeKey))
        INTO
            NAME Source
            VALUE GeneratedPower)
        SELECT * 
        FROM unpiv
        """
    )
    return (v_powerunpivot,)


@app.cell
def _(FG_DWH, mo, sourcedim, v_powerunpivot):
    _df = mo.sql(
        f"""
        SELECT 
        vpower."DateKey", vpower."TimeKey", source."SourceKey", vpower."GeneratedPower"
        FROM "FG_DWH".mockdashstaging.v_powerunpivot as vpower
        INNER JOIN "FG_DWH".mockdashstaging.sourcedim as source 
        ON vpower."Source" = source."SourceName"
        USING SAMPLE 100
        """
    )
    return


@app.cell
def _(FG_DWH, mo, sourcedim, v_powerunpivot):
    _df = mo.sql(
        f"""
        --CREATE OR REPLACE SEQUENCE test_seq START 1; 
        --CREATE TEMPORARY TABLE tempfct AS 
        SELECT 
        --nextval('test_seq') as Power_PK,
        vpower."DateKey", vpower."TimeKey", source."SourceKey", vpower."GeneratedPower"
        FROM "FG_DWH".mockdashstaging.v_powerunpivot as vpower
        INNER JOIN "FG_DWH".mockdashstaging.sourcedim as source 
        ON vpower."Source" = source."SourceName"
        USING SAMPLE 100
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        The following chain creates a series of Temp Tables to get around the SEQUENCE issues of a local DuckDB installation and MotherDuck for the AutoIncrement. This is just temporary since it will not
        autoincrement with new values. 

        Pivot Table in memory sourced from the view that includes power, date and time
        ```sql
        CREATE TEMPORARY TABLE temppivot AS 
        SELECT * FROM "FG_DWH".mockdashstaging.v_powerdatetime
        ```
        Unpivoted Temporary Table
        ```sql
        CREATE TEMPORARY TABLE tempunpivot AS
        WITH unpiv as (
        UNPIVOT temppivot
        ON COLUMNS(* EXCLUDE (DateKey, TimeKey))
        INTO
            NAME Source
            VALUE GeneratedPower)
        SELECT * 
        FROM unpiv 
        ```
        Source Dimension Table loaded into memory
        ```sql
        CREATE TEMPORARY TABLE tempsource AS 
        SELECT * FROM "FG_DWH".mockdashstaging.sourcedim
        ```
        Temporary Table that will be the future fact table joining in the Unpivoted table and the SourceDim table 
        ```sql
        CREATE OR REPLACE SEQUENCE test_seq START 1; 
        CREATE TEMPORARY TABLE tempfact AS 
        SELECT 
        nextval('test_seq') as Power_PK,
        vpower."DateKey", vpower."TimeKey", source."SourceKey", vpower."GeneratedPower"
        FROM tempunpivot as vpower
        INNER JOIN tempsource as source 
        ON vpower."Source" = source."SourceName";
        ```
        Uploading the fact table to motherduck 
        ```sql
        CREATE TABLE IF NOT EXISTS "FG_DWH".mockdashstaging.fctpower AS 
        SELECT * FROM tempfact
        ```
        """
    )
    return


@app.cell
def _(FG_DWH, fctpower, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM "FG_DWH".mockdashstaging.fctpower USING SAMPLE 100 ORDER BY Power_PK
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""This should be most of the analysis for the fact table""")
    return


if __name__ == "__main__":
    app.run()
