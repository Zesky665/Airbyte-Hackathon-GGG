import marimo

__generated_with = "0.10.8"
app = marimo.App(width="full")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""#Windpower 2023 Creation DDL""")
    return


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


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### Schema Creation
        This is how I started the process to make the tables for the mockup dashboard

        First let's create the schema's

        I'm choosing for now to go with a Raw -> Staging -> Presentation so I'm going to create the three schema's accordingly

        I did these in the notebook editior on MotherDuck. I could replicate here with CREATE IF NOT EXISTS. Additionally I would need to fully qualify the tables with database.schema.table
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        """
        Raw Schema
        ```sql 
        USE FG_DWH;
        CREATE SCHEMA mockdashraw; 
        ```

        Staging 
        ```sql 
        USE FG_DWH;
        CREATE SCHEMA mockdashstaging; 
        ```

        Presentation
        ```sql
        USE FG_DWH;
        CREATE SCHEMA mockdashpresentation; 
        ```
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### Initial Load

        I did the initial data load from the [FinGrid Dataset Downloader](https://data.fingrid.fi/en/data?datasets=75)

        The DDL for this is:
        ```sql
        CREATE OR REPLACE TABLE mockdashstaging.windpower2023RAW AS SELECT * FROM read_csv_auto(['<insert-file-here.csv>']);
        ```

        I realized afterwards that I used the wrong schema but for testing purposes I just continued
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        DESCRIBE "FG_DWH".mockdashstaging."windpower2023RAW"
        """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Creating the staging tables and data exploration

        First I'll make the Query for the table before doing a Create-Table-AS-SELECT (CTAS)

        Looking at the extract functions, they really weren't necessary but I was still figuring out what to model
        """
    )
    return


@app.cell
def _(FG_DWH, endTime, mo, windpower2023RAW):
    _df = mo.sql(
        f"""
        SELECT startTime, endTime, datetrunc('day', endTime) as "DATE", EXTRACT(DAY FROM endTime) as Day, EXTRACT(MONTH FROM endTime) as Month, "Wind power generation - 15 min data" as WindPowerGenerated
        FROM "FG_DWH".mockdashstaging.windpower2023RAW
        ORDER BY startTime ASC
        LIMIT 5;
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        I ran the following in Motherduck: 
        ```sql
        USE FG_DWH.mockdashstaging;
        CREATE TABLE IF NOT EXISTS windpower2023Staging AS 
        SELECT startTime, endTime, datetrunc('day', endTime) as "DATE", EXTRACT(DAY FROM endTime) as Day, EXTRACT(MONTH FROM endTime) as Month, "Wind power generation - 15 min data" as WindPowerGenerated
        FROM windpower2023RAW
        ORDER BY startTime ASC;
        ```
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Summary Statistics""")
    return


@app.cell
def _(FG_DWH, mo, windpower2023Staging):
    _df = mo.sql(
        f"""
        SUMMARIZE SELECT * FROM "FG_DWH".mockdashstaging."windpower2023Staging"
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Some simple data exploration""")
    return


@app.cell
def _(mo, windpower2023Staging):
    lowhighdf = mo.sql(
        f"""
        -- Find the lowest and highest power generated days
        USE FG_DWH.mockdashstaging;
        SELECT *
        FROM windpower2023Staging
        WHERE WindPowerGenerated = (SELECT min(WindPowerGenerated) FROM windpower2023Staging)
        UNION ALL 
        SELECT *
        FROM windpower2023Staging
        WHERE WindPowerGenerated = (SELECT max(WindPowerGenerated) FROM windpower2023Staging)
        """
    )
    return (lowhighdf,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Creating the Aggregate Rollup Table""")
    return


@app.cell
def _(DATE, mo, windpower2023Staging):
    _df = mo.sql(
        f"""
        USE FG_DWH.mockdashstaging;
        WITH basetable AS (
        SELECT 
        --"DATE", 
        EXTRACT(DAY FROM "DATE") as DAY, 
        EXTRACT(MONTH FROM "DATE") as MONTH, 
        EXTRACT(YEAR FROM "DATE") as YEAR, 
        WindPowerGenerated
        FROM windpower2023Staging
        WHERE YEAR = 2023
          )
        SELECT YEAR, 
               MONTH, 
               DAY, 
               SUM(WindPowerGenerated), 
               AVG(WindPowerGenerated), 
               min(WindPowerGenerated), 
               max(WindPowerGenerated),
               GROUPING_ID(YEAR, MONTH, DAY) as AggLevel
        FROM basetable 
        --GROUP BY CUBE(YEAR, MONTH, DAY)
        GROUP BY GROUPING SETS (
          (YEAR, MONTH, DAY),
          (YEAR, MONTH), 
          (YEAR),
          ()
        )
        ORDER BY AggLevel desc, YEAR, MONTH, DAY
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        The DDL for the Agg Table 
        ```sql
        USE FG_DWH.mockdashstaging;
        CREATE TABLE windpower2023Agg AS 
        WITH basetable AS (
        SELECT 
        EXTRACT(DAY FROM "DATE") as DAY, EXTRACT(MONTH FROM "DATE") as MONTH, EXTRACT(YEAR FROM "DATE") as YEAR, WindPowerGenerated
        FROM windpower2023Staging
        WHERE YEAR = 2023
          )
        SELECT YEAR, MONTH, DAY, SUM(WindPowerGenerated), AVG(WindPowerGenerated), min(WindPowerGenerated), max(WindPowerGenerated),
        GROUPING_ID(YEAR, MONTH, DAY) as AggLevel
        FROM basetable 
        GROUP BY GROUPING SETS (
          (YEAR, MONTH, DAY),
          (YEAR, MONTH), 
          (YEAR),
          ()
        )
        ORDER BY AggLevel desc, YEAR, MONTH, DAY
        ```
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""And a decode table for the Rollup Levels""")
    return


@app.cell
def _(mo, windpower2023Agg):
    _df = mo.sql(
        f"""
        USE FG_DWH.mockdashstaging;
        WITH decodetable as (
        SELECT DISTINCT AggLevel
        FROM windpower2023Agg
        )
        SELECT AggLevel, CASE WHEN AggLevel = 7 THEN 'Grand Total' WHEN AggLevel = 3 THEN 'Year Total' WHEN AggLevel = 1 THEN 'Year and Month Total'
        WHEN AggLevel = 0 THEN 'Year, Month, and Day Total' END as Description 
        FROM decodetable 
        ORDER BY AggLevel
        """
    )
    return


@app.cell
def _(mo, windpower2023Agg):
    _df = mo.sql(
        f"""
        USE FG_DWH.mockdashstaging;
        SELECT *
        FROM windpower2023Agg
        LIMIT 15;
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
