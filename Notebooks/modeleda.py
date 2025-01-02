import marimo

__generated_with = "0.10.9"
app = marimo.App(width="full")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    import pandas as pd
    return mo, pd


@app.cell(hide_code=True)
def _(mo):
    _df = mo.sql(
        f"""
        ATTACH IF NOT EXISTS 'md:FG_DWH'
        """
    )
    return (FG_DWH,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""## Base Table Data Exploration""")
    return


@app.cell
def _(FG_DWH, mo, tempallpower2022_2023):
    sourcedf = mo.sql(
        f"""
        SELECT * FROM "FG_DWH".mockdashstaging.tempallpower2022_2023 USING SAMPLE 10000
        """
    )
    return (sourcedf,)


@app.cell
def _(FG_DWH, mo, tempallpower2022_2023):
    _df = mo.sql(
        f"""
        SUMMARIZE SELECT * FROM "FG_DWH".mockdashstaging.tempallpower2022_2023
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
def _(mo, sourcedf):
    mo.ui.data_explorer(sourcedf)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""##NULL investigation""")
    return


@app.cell
def _(FG_DWH, mo, tempallpower2022_2023):
    _df = mo.sql(
        f"""
        SELECT * FROM "FG_DWH".mockdashstaging.tempallpower2022_2023
        WHERE NuclearPowerGenerated IS NULL 
        OR AllPowerGenerated IS NULL 
        OR Windpowergenerated is null
        or hydropowergenerated is null
        """
    )
    return


@app.cell
def _(FG_DWH, mo, tempallpower2022_2023):
    _df = mo.sql(
        f"""
        SELECT DISTINCT
        (SELECT COUNT(*) FROM "FG_DWH".mockdashstaging.tempallpower2022_2023 WHERE NuclearPowerGenerated IS NULL) as NuclearPowerNulls,
        (SELECT COUNT(*) FROM "FG_DWH".mockdashstaging.tempallpower2022_2023 WHERE AllPowerGenerated IS NULL) as AllPowerNulls,
        (SELECT COUNT(*) FROM "FG_DWH".mockdashstaging.tempallpower2022_2023 WHERE Windpowergenerated IS NULL) as Windpowernulls,
        (SELECT COUNT(*) FROM "FG_DWH".mockdashstaging.tempallpower2022_2023 WHERE hydropowergenerated IS NULL) as Hydropowernulls
        FROM "FG_DWH".mockdashstaging.tempallpower2022_2023
        """
    )
    return


@app.cell
def _(FG_DWH, mo, tempallpower2022_2023):
    _df = mo.sql(
        f"""
        SELECT * FROM "FG_DWH".mockdashstaging.tempallpower2022_2023
        WHERE NuclearPowerGenerated IS NULL 
        --OR AllPowerGenerated IS NULL 
        --OR Windpowergenerated is null
        --or hydropowergenerated is null
        """
    )
    return


@app.cell
def _(FG_DWH, mo, tempallpower2022_2023):
    _df = mo.sql(
        f"""
        SELECT * FROM "FG_DWH".mockdashstaging.tempallpower2022_2023
        WHERE 
        --NuclearPowerGenerated IS NULL 
        --OR 
        AllPowerGenerated IS NULL 
        --OR 
        --Windpowergenerated is null
        --or 
        --hydropowergenerated is null
        """
    )
    return


@app.cell
def _(FG_DWH, mo, tempallpower2022_2023):
    _df = mo.sql(
        f"""
        SELECT * FROM "FG_DWH".mockdashstaging.tempallpower2022_2023
        WHERE 
        --NuclearPowerGenerated IS NULL 
        --OR 
        --AllPowerGenerated IS NULL 
        --OR 
        Windpowergenerated is null
        --or 
        --hydropowergenerated is null
        """
    )
    return


@app.cell
def _(FG_DWH, mo, tempallpower2022_2023):
    _df = mo.sql(
        f"""
        SELECT * FROM "FG_DWH".mockdashstaging.tempallpower2022_2023
        WHERE 
        --NuclearPowerGenerated IS NULL 
        --OR 
        --AllPowerGenerated IS NULL 
        --OR 
        --Windpowergenerated is null
        --or 
        hydropowergenerated is null
        """
    )
    return


@app.cell
def _(FG_DWH, mo, tempallpower2022_2023):
    _df = mo.sql(
        f"""
        WITH nulltable as (SELECT * FROM "FG_DWH".mockdashstaging.tempallpower2022_2023
        WHERE NuclearPowerGenerated IS NULL 
        OR AllPowerGenerated IS NULL 
        OR Windpowergenerated is null
        or hydropowergenerated is null)
        SELECT GeneratedAtDate, COUNT(*)
        FROM nulltable
        GROUP BY GeneratedAtDate
        """
    )
    return


@app.cell
def _(datenulldf, mo):
    mo.ui.table(data=datenulldf)
    return


@app.cell
def _(mo):
    mo.md(r"""## Star Schema Joins""")
    return


@app.cell
def _(
    DateDim,
    FG_DWH,
    TimeDim,
    dates,
    fctpower,
    mo,
    sourcedim,
    sources,
    times,
):
    _df = mo.sql(
        f"""
        -- Predicate Pushdown
        WITH facts as (SELECT * FROM "FG_DWH".mockdashstaging.fctpower USING SAMPLE 1000),
        dates as (SELECT * FROM "FG_DWH".mockdashstaging."DateDim"),
        times as (SELECT * FROM "FG_DWH".mockdashstaging."TimeDim"),
        sources as (select * FROM "FG_DWH".mockdashstaging.sourcedim)
        SELECT *
        FROM facts
        INNER JOIN dates 
        ON facts.DateKey = dates.DateKey
        INNER JOIN times
        ON facts.TimeKey = times.TimeKey 
        INNER JOIN sources
        ON facts.SourceKey = sources.SourceKey
        """
    )
    return


@app.cell
def _(FG_DWH, fctpower, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM "FG_DWH".mockdashstaging.fctpower WHERE GeneratedPower IS NULL  
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
