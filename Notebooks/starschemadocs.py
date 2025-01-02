import marimo

__generated_with = "0.10.9"
app = marimo.App(width="full")


@app.cell(hide_code=True)
def _(mo):
    _df = mo.sql(
        f"""
        ATTACH IF NOT EXISTS 'md:FG_DWH'
        """,
        output=False,
    )
    return (FG_DWH,)


@app.cell(hide_code=True)
def _():
    import marimo as mo
    import pandas
    return mo, pandas


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Star Schema Documentation""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""## Summary Statistics and Schema Information""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Schema Tables""")
    return


@app.cell(hide_code=True)
def _(datedimschema, fctschema, mo, sourcedimschema, timedimschema):
    tab1 = fctschema
    tab3 = timedimschema
    tab4 = sourcedimschema
    tab2 = datedimschema
    tabs = mo.ui.tabs({
        "Fact Table Schema": tab1,
        "Date Dimension Schema": tab2,
        "Time Dimension Schema": tab3,
        "Source Dimension Schema": tab4
    })
    mo.md(f"""
    #### All Schema Tables
    {tabs}
    """)
    return tab1, tab2, tab3, tab4, tabs


@app.cell(hide_code=True)
def _(fctpower, mo):
    fddf = mo.sql(
        f"""
        DESCRIBE TABLE "FG_DWH".mockdashstaging.fctpower
        """,
        output=False,
    )
    return (fddf,)


@app.cell(hide_code=True)
def _(fddf, mo):
    fctschema = mo.ui.table(data=fddf)
    mo.md(f"""#### Fact Table Schema
        {fctschema}""")
    return (fctschema,)


@app.cell(hide_code=True)
def _(mo, sourcedim):
    sddf = mo.sql(
        f"""
        DESCRIBE TABLE "FG_DWH".mockdashstaging.sourcedim
        """,
        output=False,
    )
    return (sddf,)


@app.cell(hide_code=True)
def _(mo, sddf):
    sourcedimschema = mo.ui.table(data=sddf)
    mo.md(
            f"""
            #### Source Dimension Table Schema
            {sourcedimschema}""")
    return (sourcedimschema,)


@app.cell(hide_code=True)
def _(DateDim, mo):
    dddf = mo.sql(
        f"""
        DESCRIBE TABLE "FG_DWH".mockdashstaging."DateDim"
        """,
        output=False,
    )
    return (dddf,)


@app.cell(hide_code=True)
def _(dddf, mo):
    datedimschema = mo.ui.table(data=dddf)
    mo.md(
            f"""
            #### Date Dimension Table Schema
            {datedimschema}""")
    return (datedimschema,)


@app.cell(hide_code=True)
def _(TimeDim, mo):
    tddf = mo.sql(
        f"""
        DESCRIBE TABLE "FG_DWH".mockdashstaging."TimeDim"
        """,
        output=False,
    )
    return (tddf,)


@app.cell(hide_code=True)
def _(mo, tddf):
    timedimschema = mo.ui.table(data=tddf)
    mo.md(
            f"""
            #### Time Dimension Table Schema
            {timedimschema}""")
    return (timedimschema,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""### Summary Statistic Tables""")
    return


@app.cell(hide_code=True)
def _(datesumstat, fctsumstat, mo, sourcesumstat, timesumstat):
    _tab1 = fctsumstat
    _tab3 = timesumstat
    _tab4 = sourcesumstat
    _tab2 = datesumstat
    sumtabs = mo.ui.tabs({
        "Fact Table Summary Statistics": _tab1,
        "Date Dimension Summary Statistics": _tab2,
        "Time Dimension Summary Statistics": _tab3,
        "Source Dimension Summary Statistics": _tab4
    })
    mo.md(f"""
    #### All Schema Tables Summary Statistics
    {sumtabs}
    """)
    return (sumtabs,)


@app.cell(hide_code=True)
def _(FG_DWH, fctpower, mo):
    fsdf = mo.sql(
        f"""
        SUMMARIZE SELECT * FROM "FG_DWH".mockdashstaging.fctpower
        """,
        output=False,
    )
    return (fsdf,)


@app.cell(hide_code=True)
def _(fsdf, mo):
    fctsumstat = mo.ui.table(fsdf)
    mo.md(f"""
    #### Power Fact Table Summary Statistics
    {fctsumstat}""")
    return (fctsumstat,)


@app.cell(hide_code=True)
def _(DateDim, FG_DWH, mo):
    dsdf = mo.sql(
        f"""
        SUMMARIZE SELECT * FROM "FG_DWH".mockdashstaging."DateDim"
        """,
        output=False,
    )
    return (dsdf,)


@app.cell(hide_code=True)
def _(dsdf, mo):
    datesumstat = mo.ui.table(dsdf)
    mo.md(f"""
    #### Date Dimension Table Summary Statistics
    {datesumstat}""")
    return (datesumstat,)


@app.cell(hide_code=True)
def _(FG_DWH, Timedim, mo):
    tsdf = mo.sql(
        f"""
        SUMMARIZE SELECT * FROM "FG_DWH".mockdashstaging."Timedim"
        """,
        output=False,
    )
    return (tsdf,)


@app.cell(hide_code=True)
def _(mo, tsdf):
    timesumstat= mo.ui.table(tsdf)
    mo.md(f"""
        #### Time Dimension Table Summary Statistics
        {timesumstat}""")
    return (timesumstat,)


@app.cell(hide_code=True)
def _(FG_DWH, mo, sourcedim):
    sourcesumdf = mo.sql(
        f"""
        SUMMARIZE SELECT * FROM "FG_DWH".mockdashstaging."sourcedim"
        """,
        output=False,
    )
    return (sourcesumdf,)


@app.cell(hide_code=True)
def _(mo, sourcesumdf):
    sourcesumstat = mo.ui.table(sourcesumdf)
    mo.md(f"""
    #### Source Dimension Table Summary Statistics
    {sourcesumstat}""")
    return (sourcesumstat,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### Sample of the individual tables

        Helps get a feel for the data

        The first element is a dataframe explorer loaded with the Fact Table
        """
    )
    return


@app.cell
def _(fctdf, mo):
    mo.ui.dataframe(df=fctdf)
    return


@app.cell
def _(FG_DWH, fctpower, mo):
    fctdf = mo.sql(
        f"""
        SELECT * FROM "FG_DWH".mockdashstaging.fctpower USING SAMPLE 100;
        """
    )
    return (fctdf,)


@app.cell
def _(DateDim, FG_DWH, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM "FG_DWH".mockdashstaging."DateDim" USING SAMPLE 100 ORDER BY DateKey;
        """
    )
    return


@app.cell
def _(FG_DWH, TimeDim, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM "FG_DWH".mockdashstaging."TimeDim" USING SAMPLE 100 ORDER BY TimeKey;
        """
    )
    return


@app.cell
def _(FG_DWH, mo, sourcedim):
    _df = mo.sql(
        f"""
        SELECT * FROM "FG_DWH".mockdashstaging.sourcedim;
        """
    )
    return


if __name__ == "__main__":
    app.run()
