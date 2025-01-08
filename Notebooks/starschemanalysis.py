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
        ATTACH IF NOT EXISTS 'md:FG_DWH'
        """
    )
    return (FG_DWH,)


@app.cell
def _(mo):
    mo.md(r"""Testing out the joined view""")
    return


@app.cell
def _(
    FG_DWH,
    dim_date,
    dim_sources,
    dim_time,
    fct_sources,
    fcttable,
    joinedtable,
    mo,
    sources,
    times,
):
    _df = mo.sql(
        f"""


        WITH dates AS (
            SELECT
        *
            FROM
                "FG_DWH"."dbt_staging"."dim_date"
        ),
        times AS (
            SELECT
        *
            FROM
                "FG_DWH"."dbt_staging"."dim_time"
        ),
        sources AS (
            SELECT
                *
            FROM
                "FG_DWH"."dbt_staging"."dim_sources"
        ),
        fcttable AS (
            SELECT
                *
            FROM
                "FG_DWH"."dbt_staging"."fct_sources"
            USING SAMPLE 100
        ),
        joinedtable AS (
            SELECT
        *
            FROM
                fcttable
                INNER JOIN dates
                ON fcttable.date_key = dates.date_key
                INNER JOIN times
                ON fcttable.time_key = times.time_key
                INNER JOIN sources
                ON fcttable.source_key = sources.source_key
        )
        SELECT
            *
        FROM
            joinedtable
        """
    )
    return


@app.cell
def _(
    FG_DWH,
    dim_date,
    dim_sources,
    dim_time,
    fct_sources,
    fcttable,
    joinedtable,
    mo,
    sources,
    times,
):
    _df = mo.sql(
        f"""
        SUMMARIZE 


        WITH dates AS (
            SELECT
        *
            FROM
                "FG_DWH"."dbt_staging"."dim_date"
        ),
        times AS (
            SELECT
        *
            FROM
                "FG_DWH"."dbt_staging"."dim_time"
        ),
        sources AS (
            SELECT
                *
            FROM
                "FG_DWH"."dbt_staging"."dim_sources"
        ),
        fcttable AS (
            SELECT
                *
            FROM
                "FG_DWH"."dbt_staging"."fct_sources"
            USING SAMPLE 100
        ),
        joinedtable AS (
            SELECT
        *
            FROM
                fcttable
                INNER JOIN dates
                ON fcttable.date_key = dates.date_key
                INNER JOIN times
                ON fcttable.time_key = times.time_key
                INNER JOIN sources
                ON fcttable.source_key = sources.source_key
        )
        SELECT
            *
        FROM
            joinedtable
        """
    )
    return


@app.cell
def _(
    FG_DWH,
    dim_date,
    dim_sources,
    dim_time,
    fct_sources,
    fcttable,
    joinedtable,
    mo,
    sources,
    times,
):
    _df = mo.sql(
        f"""


        WITH dates AS (
            SELECT
        *
            FROM
                "FG_DWH"."dbt_staging"."dim_date"
        ),
        times AS (
            SELECT
        *
            FROM
                "FG_DWH"."dbt_staging"."dim_time"
        ),
        sources AS (
            SELECT
                *
            FROM
                "FG_DWH"."dbt_staging"."dim_sources"
        ),
        fcttable AS (
            SELECT
                *
            FROM
                "FG_DWH"."dbt_staging"."fct_sources"
                USING SAMPLE 100
        ),
        joinedtable AS (
            SELECT
        sources.source_name,
        fcttable.source_value,
        dates."date_value",
          dates."year_value",
          dates."month_value",
          dates."month_name",
          dates."day_value",
          dates."week_day_name",
          dates."week_day_value",
          dates."iso_week_num",
          dates."quarter_value",
        times."time_value",
          times."time_of_day_str",
          times."time_of_day_str_ext",
          times."hour_value",
          times."minute_value",
          times."quarter_hour",
          times."day_period_name",
          times."day_or_night",
          times."AMPM"
            FROM
                fcttable
                INNER JOIN dates
                ON fcttable.date_key = dates.date_key
                INNER JOIN times
                ON fcttable.time_key = times.time_key
                INNER JOIN sources
                ON fcttable.source_key = sources.source_key
        )
        SELECT
            *
        FROM
            joinedtable
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
