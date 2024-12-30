import marimo

__generated_with = "0.10.8"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""##Testing the connection to MotherDuck""")
    return


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
    mo.md(r"""### So this requires you to sign in everytime you want to use the database. If we didn't want to be prompted everytime we would save the token and configure our enviroment variables. """)
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        DESCRIBE FG_DWH.mockdashraw.totalpowerraw2022_2023
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""###Okay we're able to connect. One thing to note is we have to use full qualified names. As in Database.Schema.Table""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Let's do some summary statistics on the raw dataset.""")
    return


@app.cell
def _(FG_DWH, mo, totalpowerraw2022_2023):
    _df = mo.sql(
        f"""
        SELECT * FROM FG_DWH.mockdashraw.totalpowerraw2022_2023 USING SAMPLE 1%
        """
    )
    return


@app.cell
def _(FG_DWH, mo, totalpowerraw2022_2023):
    _df = mo.sql(
        f"""
        SUMMARIZE SELECT * FROM FG_DWH.mockdashraw.totalpowerraw2022_2023
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
