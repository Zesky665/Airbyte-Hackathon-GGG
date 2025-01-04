import marimo

__generated_with = "0.10.8"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md(r"""Testing how this will work""")
    return


@app.cell
def _(mo):
    sqlout = mo.sql(
        f"""
        SELECT 1
        """
    )
    return (sqlout,)


@app.cell
def _(sqlout):
    print(sqlout)
    return


if __name__ == "__main__":
    app.run()
