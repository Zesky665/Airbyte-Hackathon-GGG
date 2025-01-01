import marimo

__generated_with = "0.10.9"
app = marimo.App(width="full")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Star Schema Documentation""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""## Summary Statistics and Schema Information """)
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
def _(datedimschema, fctsumstat, mo, sourcesumstat, timesumstat):
    _tab1 = fctsumstat
    _tab3 = timesumstat
    _tab4 = sourcesumstat
    _tab2 = datedimschema
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
def _(fctpower, mo):
    _fctdf = mo.sql(
        f"""
        DESCRIBE TABLE "FG_DWH".mockdashstaging.fctpower
        """
    )
    fctschema = mo.ui.table(data=_fctdf)
    mo.md(f"""### Fact Table Schema
    {fctschema} """)
    return (fctschema,)


@app.cell(hide_code=True)
def _(mo, sourcedim):
    _df = mo.sql(
        f"""
        DESCRIBE TABLE "FG_DWH".mockdashstaging.sourcedim
        """
    )
    sourcedimschema = mo.ui.table(data=_df)
    mo.md(
        f"""
        ### Source Dimension Table Schema
        {sourcedimschema}
        """
    )
    return (sourcedimschema,)


@app.cell(hide_code=True)
def _(DateDim, mo):
    _df = mo.sql(
        f"""
        DESCRIBE TABLE "FG_DWH".mockdashstaging."DateDim"
        """
    )
    datedimschema = mo.ui.table(data=_df)
    mo.md(
        f"""
        ### Date Dimension Table Schema
        {datedimschema}
        """
    )
    return (datedimschema,)


@app.cell(hide_code=True)
def _(TimeDim, mo):
    _df = mo.sql(
        f"""
        DESCRIBE TABLE "FG_DWH".mockdashstaging."TimeDim"
        """
    )
    timedimschema = mo.ui.table(data=_df)
    mo.md(
        f"""
        ### Time Dimension Table Schema
        {timedimschema}
        """
    )
    return (timedimschema,)


@app.cell(hide_code=True)
def _(FG_DWH, fctpower, mo):
    _df = mo.sql(
        f"""
        SUMMARIZE SELECT * FROM "FG_DWH".mockdashstaging.fctpower
        """
    )
    fctsumstat = mo.ui.table(_df)
    mo.md(f"""
    #### Power Fact Table Summary Statistics
    {fctsumstat}
    """)
    return (fctsumstat,)


@app.cell(hide_code=True)
def _(DateDim, FG_DWH, mo):
    _df = mo.sql(
        f"""
        SUMMARIZE SELECT * FROM "FG_DWH".mockdashstaging."DateDim"
        """
    )
    datesumstat = mo.ui.table(_df)
    mo.md(f"""
    #### Power Fact Table Summary Statistics
    {datesumstat}
    """)
    return (datesumstat,)


@app.cell(hide_code=True)
def _(FG_DWH, Timedim, mo):
    _df = mo.sql(
        f"""
        SUMMARIZE SELECT * FROM "FG_DWH".mockdashstaging."Timedim"
        """
    )
    timesumstat= mo.ui.table(_df)
    mo.md(f"""
    #### Power Fact Table Summary Statistics
    {timesumstat}
    """)
    return (timesumstat,)


@app.cell(hide_code=True)
def _(FG_DWH, mo, sourcedim):
    _df = mo.sql(
        f"""
        SUMMARIZE SELECT * FROM "FG_DWH".mockdashstaging."sourcedim"
        """
    )
    sourcesumstat = mo.ui.table(_df)
    mo.md(f"""
    #### Power Fact Table Summary Statistics
    {sourcesumstat}
    """)
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
    _df = mo.sql(
        f"""
        SELECT * FROM "FG_DWH".mockdashstaging.fctpower USING SAMPLE 100;
        """
    )
    return


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


@app.cell
def _():
    return


@app.cell
def _():
    import marimo as mo
    import pandas
    return mo, pandas


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        ATTACH IF NOT EXISTS 'md:FG_DWH'
        """
    )
    return (FG_DWH,)


if __name__ == "__main__":
    app.run()
