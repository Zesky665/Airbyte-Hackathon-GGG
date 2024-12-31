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
    testdf = mo.sql(
        f"""
        SELECT startTime::DATE as StartDate,
        startTime::TIME as StartTime,
        endTime::DATE as EndDate,
        endTime::TIME as EndTime, 
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
def _():
    return


if __name__ == "__main__":
    app.run()
