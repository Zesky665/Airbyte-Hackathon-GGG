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


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Wind Energy Production """)
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
