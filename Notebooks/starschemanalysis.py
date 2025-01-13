import marimo

__generated_with = "0.10.9"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    import seaborn as sns
    return (sns,)


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        ATTACH IF NOT EXISTS 'md:FG_DWH'
        """
    )
    return (FG_DWH,)


@app.cell
def _(FG_DWH, mo, obt_fct_tbl):
    _df = mo.sql(
        f"""
        SELECT * 
        FROM "FG_DWH".dbt_staging.obt_fct_tbl
        USING SAMPLE 10
        """
    )
    return


@app.cell
def _(FG_DWH, mo, obt_fct_tbl):
    _df = mo.sql(
        f"""
        SELECT source_name, year_value, ROUND(min(source_value)) as min_value, ROUND(max(source_value)) as max_value, ROUND(sum(source_value)) as total_energy_produced, ROUND(avg(source_value)) as average_value, ROUND(median(source_value)) as median_value
        FROM "FG_DWH".dbt_staging.obt_fct_tbl
        GROUP BY source_name, year_value
        ORDER BY year_value, source_name
        """
    )
    return


@app.cell
def _(FG_DWH, mo, obt_fct_tbl):
    sourcesum = mo.sql(
        f"""
        SELECT source_name, year_value, ROUND(sum(source_value)) as total_energy_produced
        FROM "FG_DWH".dbt_staging.obt_fct_tbl
        WHERE year_value < 2025
        GROUP BY source_name, year_value
        ORDER BY year_value, source_name
        """
    )
    return (sourcesum,)


@app.cell
def _(sns, sourcesum):
    sns.relplot(data=sourcesum, x="year_value", y="total_energy_produced", kind="line", hue="source_name")
    return


@app.cell
def _(FG_DWH, mo, obt_fct_tbl):
    _df = mo.sql(
        f"""
        SELECT source_name, year_value, month_value, ROUND(min(source_value)) as min_value, ROUND(max(source_value)) as max_value, ROUND(sum(source_value)) as total_energy_produced, ROUND(avg(source_value)) as average_value, ROUND(median(source_value)) as median_value
        FROM "FG_DWH".dbt_staging.obt_fct_tbl
        WHERE year_value < 2025
        GROUP BY source_name, year_value, month_value
        ORDER BY year_value, month_value, source_name
        """
    )
    return


@app.cell
def _(crosstab, sns):
    sns.relplot(data=crosstab, x="month_value", y="total_energy_produced", kind="line", hue="year_value", style="source_name")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
