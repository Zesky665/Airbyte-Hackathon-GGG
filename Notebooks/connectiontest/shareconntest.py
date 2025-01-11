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
        ATTACH 
        """
    )
    return


if __name__ == "__main__":
    app.run()
