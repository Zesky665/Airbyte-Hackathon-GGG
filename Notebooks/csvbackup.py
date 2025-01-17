import marimo

__generated_with = "0.10.13"
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
def _(hourly_grid_pivot, mo):
    _df = mo.sql(
        f"""
        --export hourly csv
        COPY "FG_DWH".analytics.hourly_grid_pivot to 'databackup/hourly_piv.csv' (HEADER, DELIMITER ',');
        """
    )
    return


@app.cell
def _(daily_grid_piv, mo):
    _df = mo.sql(
        f"""
        --export daily csv
        COPY "FG_DWH".analytics.daily_grid_piv to 'databackup/daily_piv.csv' (HEADER, DELIMITER ',');
        """
    )
    return


@app.cell
def _(hourly_grid_pivot, mo):
    _df = mo.sql(
        f"""
        --export hourly parquet
        COPY "FG_DWH".analytics.hourly_grid_pivot to 'databackup/hourly_piv.parquet' (FORMAT PARQUET);
        """
    )
    return


@app.cell
def _(daily_grid_piv, mo):
    _df = mo.sql(
        f"""
        --export daily parquet
        COPY "FG_DWH".analytics.daily_grid_piv to 'databackup/daily_piv.parquet' (FORMAT PARQUET);
        """
    )
    return


@app.cell
def _(hourly_electric_data_union, mo):
    _df = mo.sql(
        f"""
        --export referencetable csv
        COPY "FG_DWH".reference.hourly_electric_data_union to 'databackup/ref_piv.csv' (HEADER, DELIMITER ',');
        """
    )
    return


@app.cell
def _(hourly_electric_data_union, mo):
    _df = mo.sql(
        f"""
        --export referencetable parquet
        COPY "FG_DWH".reference.hourly_electric_data_union to 'databackup/ref_piv.parquet' (FORMAT PARQUET);
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""Base non pivoted tables""")
    return


@app.cell
def _(hourly_statistics, mo):
    _df = mo.sql(
        f"""
        COPY "FG_DWH".analytics.hourly_statistics to 'databackup/hourly.csv' (HEADER, DELIMITER ',');
        """
    )
    return


@app.cell
def _(daily_statistics_rollup, mo):
    _df = mo.sql(
        f"""
        COPY "FG_DWH".analytics.daily_statistics_rollup to 'databackup/daily.csv' (HEADER, DELIMITER ',');
        """
    )
    return


@app.cell
def _(hourly_statistics, mo):
    _df = mo.sql(
        f"""
        COPY "FG_DWH".analytics.hourly_statistics to 'databackup/hourly.parquet' (FORMAT PARQUET);
        """
    )
    return


@app.cell
def _(daily_statistics_rollup, mo):
    _df = mo.sql(
        f"""
        COPY "FG_DWH".analytics.daily_statistics_rollup to 'databackup/daily.parquet' (FORMAT PARQUET);
        """
    )
    return


if __name__ == "__main__":
    app.run()
