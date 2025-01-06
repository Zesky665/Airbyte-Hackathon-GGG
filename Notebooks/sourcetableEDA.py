import marimo

__generated_with = "0.10.9"
app = marimo.App(width="full")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Descriptive Statistics and Exploratory Analysis of Raw FinGrid Tables""")
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
    _df = mo.sql(
        f"""
        USE "FG_DWH".landing;
        SHOW TABLES
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Total Electricity Production""")
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        DESCRIBE TotalElectricityProductionRaw
        """
    )
    return


@app.cell
def _(TotalElectricityProductionRaw, mo):
    _df = mo.sql(
        f"""
        SUMMARIZE SELECT * FROM TotalElectricityProductionRaw
        """
    )
    return


@app.cell
def _(TotalElectricityProductionRaw, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM TotalElectricityProductionRaw USING SAMPLE 100 ORDER BY startTime;
        """
    )
    return


@app.cell
def _(WindProductionRaw, landing, mo):
    _df = mo.sql(
        f"""
        SELECT *
        FROM landing.WindProductionRaw
        WHERE startTime not in (SELECT endTime from landing.WindProductionRaw)
        UNION 
        SELECT *
        FROM landing.WindProductionRaw
        WHERE endTime not in (SELECT startTime from landing.WindProductionRaw)
        ORDER BY startTime
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Wind Energy Production""")
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        DESCRIBE WindProductionRaw
        """
    )
    return


@app.cell
def _(WindProductionRaw, mo):
    _df = mo.sql(
        f"""
        SUMMARIZE SELECT * FROM WindProductionRaw
        """
    )
    return


@app.cell
def _(WindProductionRaw, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM WindProductionRaw USING SAMPLE 100 ORDER BY startTime;
        """
    )
    return


@app.cell
def _(WindProductionRaw, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM WindProductionRaw ORDER BY startTime LIMIT 100 OFFSET 100000;
        """
    )
    return


@app.cell
def _(WindProductionRaw, landing, mo):
    _df = mo.sql(
        f"""
        WITH x AS (
           SELECT
              *,
              CASE
                 WHEN startTime = endTime THEN endTime - INTERVAL '3 MINUTES'
                 ELSE startTime
              END AS adj_start_time,
              LAG(endTime) OVER (ORDER BY endTime) > startTime AS overlap_with_previous,
              LEAD(startTime) OVER (ORDER BY endTime) < endTime AS overlap_with_next
           FROM landing.WindProductionRaw
           ORDER BY endTime
        )
        SELECT *
        FROM x
        WHERE overlap_with_previous OR overlap_with_next;
        """
    )
    return


@app.cell
def _(WindProductionRaw, landing, mo, overlap):
    _df = mo.sql(
        f"""
        WITH adj_time AS (
           SELECT
              *,
              CASE
                 -- assume the previous record's endTime as startTime
                 WHEN startTime = endTime THEN LAG(endTime) OVER (ORDER BY endTime)
                 ELSE startTime
              END AS adj_start_time
           FROM landing.WindProductionRaw
           ORDER BY endTime
        )
        , overlap AS (
          SELECT
              *,
              LAG(endTime) OVER (ORDER BY endTime) > adj_start_time AS overlap_with_previous,
              LEAD(adj_start_time) OVER (ORDER BY endTime) < endTime AS overlap_with_next
              FROM adj_time
        )
        SELECT *
        FROM overlap
        WHERE overlap_with_previous OR overlap_with_next
        ORDER BY endTime;
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Nuclear Energy Production""")
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        DESCRIBE NuclearProductionRaw
        """
    )
    return


@app.cell
def _(NuclearProductionRaw, mo):
    _df = mo.sql(
        f"""
        SUMMARIZE SELECT * FROM NuclearProductionRaw
        """
    )
    return


@app.cell
def _(NuclearProductionRaw, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM NuclearProductionRaw USING SAMPLE 100 ORDER BY startTime;
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Hydroelectric Energy Production""")
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        DESCRIBE HydroProductionRaw
        """
    )
    return


@app.cell
def _(HydroProductionRaw, mo):
    _df = mo.sql(
        f"""
        SUMMARIZE SELECT * FROM HydroProductionRaw
        """
    )
    return


@app.cell
def _(HydroProductionRaw, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM HydroProductionRaw USING SAMPLE 100 ORDER BY startTime;
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Total Energy Consumption in Finland""")
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        DESCRIBE HydroProductionRaw
        """
    )
    return


@app.cell
def _(HydroProductionRaw, mo):
    _df = mo.sql(
        f"""
        SUMMARIZE SELECT * FROM HydroProductionRaw
        """
    )
    return


@app.cell
def _(HydroProductionRaw, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM HydroProductionRaw USING SAMPLE 100 ORDER BY startTime;
        """
    )
    return


@app.cell
def _(TotalElectricityProductionRaw, mo):
    _df = mo.sql(
        f"""
        SELECT 
            endTime::DATE as generated_at_date,
            endTime::TIME as generated_at_time,
            "Electricity production in Finland - real time data" as total_electricity_produced,
            'Total Electricity Production' as SourceName  
        FROM TotalElectricityProductionRaw
        USING SAMPLE 10
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
