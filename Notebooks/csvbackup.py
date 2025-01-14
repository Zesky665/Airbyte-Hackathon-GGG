import marimo

__generated_with = "0.10.12"
app = marimo.App(width="full")


@app.cell
def _(mo):
    mo.md(r"""#Notebook to backup database""")
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        ATTACH 'md:FG_DWH'
        """
    )
    return (FG_DWH,)


@app.cell
def _(grid_pivot, mo):
    _df = mo.sql(
        f"""
        --export hourly csv
        COPY "FG_DWH".analytics.grid_pivot to 'hourly.csv' (HEADER, DELIMITER ',');
        """
    )
    return


@app.cell
def _(daily_grid, mo):
    _df = mo.sql(
        f"""
        --export daily csv
        COPY "FG_DWH".analytics.daily_grid to 'daily.csv' (HEADER, DELIMITER ',');
        """
    )
    return


@app.cell
def _(grid_pivot, mo):
    _df = mo.sql(
        f"""
        --export hourly parquet
        COPY "FG_DWH".analytics.grid_pivot to 'hourly.parquet' (FORMAT PARQUET);
        """
    )
    return


@app.cell
def _(daily_grid, mo):
    _df = mo.sql(
        f"""
        --export daily parquet
        COPY "FG_DWH".analytics.daily_grid to 'daily.parquet' (FORMAT PARQUET);
        """
    )
    return


@app.cell
def _(hourly_electric_data_union, mo):
    _df = mo.sql(
        f"""
        --export referencetable csv
        COPY "FG_DWH".reference.hourly_electric_data_union to 'ref.csv' (HEADER, DELIMITER ',');
        """
    )
    return


@app.cell
def _(hourly_electric_data_union, mo):
    _df = mo.sql(
        f"""
        --export referencetable parquet
        COPY "FG_DWH".reference.hourly_electric_data_union to 'ref.parquet' (FORMAT PARQUET);
        """
    )
    return


if __name__ == "__main__":
    app.run()
