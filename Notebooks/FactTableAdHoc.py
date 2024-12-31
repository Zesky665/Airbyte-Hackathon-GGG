import marimo

__generated_with = "0.10.9"
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
    mo.md(r"""Confirming that StartTime and EndTime from the RawTable is the same value. """)
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
def _():
    return


if __name__ == "__main__":
    app.run()
