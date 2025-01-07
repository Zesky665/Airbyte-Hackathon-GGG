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
def _(FG_DWH, HydroProductionRaw, mo):
    tempdf = mo.sql(
        f"""
        CREATE temporary table HydroRaw AS 
        SELECT *
        FROM "FG_DWH".landing."HydroProductionRaw"
        """
    )
    return HydroRaw, tempdf


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        ALTER TABLE HydroRaw RENAME "Hydro power production - real time data" to HydroPowerVal
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        DESCRIBE HydroRaw
        """
    )
    return


@app.cell
def _(HYDRORAW, mo):
    _df = mo.sql(
        f"""
        SUMMARIZE SELECT * FROM HYDRORAW
        """
    )
    return


@app.cell
def _(HydroRaw, mo):
    _df = mo.sql(
        f"""
        SUMMARIZE SELECT * 
        FROM HydroRaw
        WHERE startTime != endTime
        """
    )
    return


@app.cell
def _(HydroRaw, mo):
    _df = mo.sql(
        f"""
        SELECT * 
        FROM HydroRaw
        WHERE startTime != endTime
        ORDER BY startTime
        """
    )
    return


@app.cell
def _(HydroRaw, mo, startTime):
    _df = mo.sql(
        f"""
        with blah as (
        SELECT  
        count(*) as counttotal, 
        (SELECT count(*) 
        FROM HydroRaw
        WHERE startTime != endTime) as counterror
        FROM HydroRaw
        WHERE EXTRACT(YEAR FROM startTime) >= 2024
            )
        SELECT 
        counttotal, counterror, counterror/counttotal
        FROM blah
        """
    )
    return


@app.cell
def _(HydroRaw, mo):
    _df = mo.sql(
        f"""
        SELECT COUNT(*)
        FROM HydroRaw
        WHERE HydroPowerVal = 0
        """
    )
    return


@app.cell
def _(mo, unnest):
    _df = mo.sql(
        f"""
        SELECT *
        FROM unnest(generate_series('2024-01-01 00:00:00'::Timestamp, '2025-01-01 00:00:00'::Timestamp, '3 minutes'::Interval))
        """
    )
    return


@app.cell
def _(
    HydroRaw,
    ajmm,
    antijoin,
    mismatch,
    mo,
    nn,
    notnext,
    notthreemin,
    startTime,
    threemindf,
):
    _df = mo.sql(
        f"""
        with starttable2024 as (
            SELECT startTime, endTime
            FROM HydroRaw
            WHERE EXTRACT(YEAR FROM startTime) = 2024
        ),
        mismatch as (
            SELECT startTime
            FROM HydroRaw
            WHERE EXTRACT(YEAR FROM startTime) = 2024
            AND startTime != endTime 
        ),
        antijoin as (
            SELECT startTime
            from starttable2024
            WHERE startTime NOT IN (SELECT * FROM threemindf)
        ),
        ajmm as (
            SELECT startTime
            from mismatch
            WHERE startTime NOT IN (SELECT * FROM threemindf)
        ),
        notthreemin as (
            SELECT startTime 
            FROM starttable2024 
            WHERE startTime != endTime - '3 minutes'::INTERVAL 
        ), 
        notnext as (
            SELECT startTime, Lead(startTime, 1) OVER (ORDER BY startTime) = startTime + '3 minutes'::Interval as truthtable
            FROM starttable2024
        ),
        nn as (
            SELECT count(*) 
            From notnext 
            WHERE not truthtable
        )
        SELECT 
        (SELECT count(startTime) FROM starttable2024),
        (SELECT count(*) FROM mismatch),
        (SELECT count(*) FROM antijoin),
        (SELECT COUNT(*) FROM ajmm),
        (SELECT count(*) FROM notthreemin),
        (SELECT * FROM nn)
        """
    )
    return


@app.cell
def _(HydroRaw, mo, startTime):
    _df = mo.sql(
        f"""
        SELECT startTime, Lead(startTime, 1) OVER (ORDER BY startTime) = startTime + '3 minutes'::Interval as truthtable
        FROM HydroRaw
        WHERE EXTRACT(YEAR FROM startTime) = 2024
        """
    )
    return


@app.cell
def _(HydroRaw, mo, startTime):
    _df = mo.sql(
        f"""
        with basetable as (SELECT startTime, Lead(startTime, 1) OVER (ORDER BY startTime) = startTime + '3 minutes'::Interval as truthtable
        FROM HydroRaw
        WHERE EXTRACT(YEAR FROM startTime) = 2024)
        SELECT * 
        FROM basetable 
        WHERE not truthtable 
        ORDER BY starttime
        """
    )
    return


@app.cell
def _(HydroRaw, hydroraw, mo):
    _df = mo.sql(
        f"""
        WITH subq as ( 
            SELECT endTime, COUNT(*) as cnt FROM HydroRaw GROUP BY endTime HAVING COUNT(*) > 1
            )
        SELECT * 
        From hydroraw
        WHERE endTime IN (SELECT endTime FROM subq)
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
