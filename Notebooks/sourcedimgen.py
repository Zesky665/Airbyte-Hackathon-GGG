import marimo

__generated_with = "0.10.9"
app = marimo.App(width="full")


@app.cell
def _(mo):
    mo.md(r"""# Energy Sources Dimension Table Generation""")
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
        Figuring out how to unpivot the raw table and then create the energy sources. 

        ~~The most manual way would be unions probably.~~

        Looking at the DuckDB documentation there is an UnPivot operator: [UNPIVOT DuckDB Docs](https://duckdb.org/docs/sql/statements/unpivot.html)
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        DESCRIBE "FG_DWH".mockdashstaging.tempallpower2022_2023
        """
    )
    return


@app.cell
def _(FG_DWH, mo, tempallpower2022_2023):
    _df = mo.sql(
        f"""
        -- need to limit the amount of data
        WITH power as (SELECT * FROM "FG_DWH".mockdashstaging.tempallpower2022_2023 USING SAMPLE 1000)
        UNPIVOT power
        ON COLUMNS(* EXCLUDE (GeneratedAtDate, GeneratedAtTime))
        INTO
            NAME Source
            VALUE GeneratedPower;
        """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        I did not limit the number on the UNPIVOT. 

        Let me calculate how much the unpivot will be (number of rows) * (number of sources). Currently we have 4 sources and 353702 rows 
        """
    )
    return


@app.cell
def _():
    4 * 353702
    return


@app.cell
def _(mo):
    mo.md(r"""Creating a SEQUENCE is complicated on motherduck I would need to create the table in memory here then upload it to MotherDuck. I'm going to hardcode the ID field for now""")
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        ---DROP SEQUENCE id_sequence
        """
    )
    return


@app.cell
def _(FG_DWH, mo, mockdashstaging):
    _df = mo.sql(
        f"""
        -- worked on it in motherduck 
        CREATE TABLE IF NOT EXISTS "FG_DWH".mockdashstaging.sourcedim (
           SourceKey UBIGINT PRIMARY KEY,
            SourceName VARCHAR
        )
        --CREATE OR REPLACE TABLE "FG_DWH".mockdashstaging.sourcedim (
        --   SourceKey INTEGER PRIMARY KEY DEFAULT nextval(id_sequence),
        --    SourceName VARCHAR
        --)
        """
    )
    return (sourcedim,)


@app.cell
def _(mo):
    iddf = mo.sql(
        f"""
        --CREATE SEQUENCE id_sequence START 1;
        --INSERT INTO "FG_DWH".mockdashstaging.sourcedim
        """
    )
    return (iddf,)


@app.cell
def _(mo):
    mo.md(r"""Having problems with the ID Sequence Generation """)
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        --Insert the source value from the pivot table into the source table 
        -- Use insert into by name 
        --INSERT INTO "FG_DWH".mockdashstaging.sourcedim BY NAME (SELECT DISTINCT Source as SourceName, md5_number_lower(Source) as SourceKey 
        --FROM unpivotdf)
        """
    )
    return


@app.cell
def _(FG_DWH, mo, sourcedim):
    _df = mo.sql(
        f"""
        SELECT * FROM "FG_DWH".mockdashstaging.sourcedim
        """
    )
    return


@app.cell
def _(mo, unpivotdf):
    _df = mo.sql(
        f"""
        -- SELECT Source for a CTAS 
        SELECT DISTINCT 
        md5_number_lower(Source) as SourceKey,
        Source as SourceName
        FROM unpivotdf
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
