import marimo

__generated_with = "0.10.9"
app = marimo.App(width="full")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Construction of the Date Dimension Table""")
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


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        --SELECT unnest(generate_series(TIMESTAMP without TIME zone '2016-10-16', TIMESTAMP without TIME zone '2016-10-17', '1 minute')) as hourminute
        SELECT unnest(generate_series('2022-01-01'::DATE,'2023-12-31'::DATE, '1 day')) as hourminute
        """
    )
    return


@app.cell
def _(dates, mo):
    _df = mo.sql(
        f"""
        -- can't remember if I should use modulo 3 for this or not for the quarter calculations
        SELECT 
        strftime(dates, '%Y%m%d')::INT as DateKey,
        dates::DATE as DateValue,
        EXTRACT(YEAR FROM dates)::SMALLINT as YearVal,
        EXTRACT(MONTH FROM dates)::TINYINT as MonthVal,
        monthname(dates) as MonthName,
        EXTRACT(DAY FROM dates)::SMALLINT as DayVal,
        dayname(dates) as WeekDayName,
        strftime(dates, '%u')::SMALLINT as WeekDayVal,
        strftime(dates, '%V')::SMALLINT as IsoWeekNum,
        CASE 
        WHEN EXTRACT(MONTH FROM dates) BETWEEN 1 and 3 THEN 1::TINYINT 
        WHEN EXTRACT(MONTH FROM dates) BETWEEN 4 and 6 THEN 2::TINYINT  
        WHEN EXTRACT(MONTH FROM dates) BETWEEN 7 and 9 THEN 3::TINYINT 
        WHEN EXTRACT(MONTH FROM dates) BETWEEN 9 and 12 THEN 4::TINYINT 
        END as QuarterValue
        FROM (
        SELECT unnest(generate_series('2022-01-01'::DATE,'2023-12-31'::DATE, '1 day')) as dates
        ) as dategen
        """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        Much simpler thanks to what I learned from the TimeDim table and using DuckDBs date format functions

        Changed the DDL to include for more efficient data types 
        """
    )
    return


@app.cell(disabled=True)
def _(FG_DWH, dates, mo, mockdashstaging):
    _df = mo.sql(
        f"""
        --DDL For the DateDim Table 
        CREATE OR REPLACE TABLE "FG_DWH".mockdashstaging.DateDim AS 
        SELECT 
        strftime(dates, '%Y%m%d')::INT as DateKey,
        dates::DATE as DateValue,
        EXTRACT(YEAR FROM dates)::SMALLINT as YearVal,
        EXTRACT(MONTH FROM dates)::TINYINT as MonthVal,
        monthname(dates) as MonthName,
        EXTRACT(DAY FROM dates)::SMALLINT as DayVal,
        dayname(dates) as WeekDayName,
        strftime(dates, '%u')::SMALLINT as WeekDayVal,
        strftime(dates, '%V')::SMALLINT as IsoWeekNum,
        CASE 
        WHEN EXTRACT(MONTH FROM dates) BETWEEN 1 and 3 THEN 1::TINYINT 
        WHEN EXTRACT(MONTH FROM dates) BETWEEN 4 and 6 THEN 2::TINYINT  
        WHEN EXTRACT(MONTH FROM dates) BETWEEN 7 and 9 THEN 3::TINYINT 
        WHEN EXTRACT(MONTH FROM dates) BETWEEN 9 and 12 THEN 4::TINYINT 
        END as QuarterValue
        FROM (
        SELECT unnest(generate_series('2022-01-01'::DATE,'2023-12-31'::DATE, '1 day')) as dates
        ) as dategen
        """
    )
    return (DateDim,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        DDL for Date Dimension Table Generation, change the Create section to Create IF NOT EXISTS
        ```sql
        CREATE OR REPLACE TABLE "FG_DWH".mockdashstaging.DateDim AS 
        SELECT 
        strftime(dates, '%Y%m%d')::INT as DateKey,
        dates::DATE as DateValue,
        EXTRACT(YEAR FROM dates)::SMALLINT as YearVal,
        EXTRACT(MONTH FROM dates)::TINYINT as MonthVal,
        monthname(dates) as MonthName,
        EXTRACT(DAY FROM dates)::SMALLINT as DayVal,
        dayname(dates) as WeekDayName,
        strftime(dates, '%u')::SMALLINT as WeekDayVal,
        strftime(dates, '%V')::SMALLINT as IsoWeekNum,
        CASE 
        WHEN EXTRACT(MONTH FROM dates) BETWEEN 1 and 3 THEN 1::TINYINT 
        WHEN EXTRACT(MONTH FROM dates) BETWEEN 4 and 6 THEN 2::TINYINT  
        WHEN EXTRACT(MONTH FROM dates) BETWEEN 7 and 9 THEN 3::TINYINT 
        WHEN EXTRACT(MONTH FROM dates) BETWEEN 9 and 12 THEN 4::TINYINT 
        END as QuarterValue
        FROM (
        SELECT unnest(generate_series('2022-01-01'::DATE,'2023-12-31'::DATE, '1 day')) as dates
        ) as dategen
        ```
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
