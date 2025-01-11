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
    _df = mo.sql(
        f"""
        SELECT *
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        DESCRIBE FG_DWH.landing.ElectrictyConsumptionRaw
        """
    )
    return


@app.cell
def _(mo):
    dateleveldf = mo.sql(
        f"""
        --SELECT datetrunc('day', endTime) as generated_at_date,"Electricity consumption in Finland - real time data" as source_value, 'Total Energy Consumption' as source_name
        --FROM FG_DWH.landing.ElectrictyConsumptionRaw 
        --UNION ALL
        --SELECT generated_at_date, source_value, source_name 
        --FROM "FG_DWH".dbt_staging.int_fct_sources_unioned
        """
    )
    return (dateleveldf,)


@app.cell
def _(ElectrictyConsumptionRaw, FG_DWH, dedup, houragg, hourtable, mo):
    consumptiondf = mo.sql(
        f"""
        with rename as (
        SELECT endTime,
        'Total Energy Consumption' as source_name, 
        "Electricity consumption in Finland - real time data" as source_value
        FROM FG_DWH.landing.ElectrictyConsumptionRaw 
        ), dedup as (
            SELECT endTime, 
            source_name, 
            AVG(source_value) as source_value
            FROM rename
            GROUP BY 
            endTime, 
            source_name
        ), hourtable as (
            SELECT 
            datetrunc('hour', endTime) as generated_at_hour,
            source_name, 
            source_value
            FROM dedup
        ), houragg as (
            SELECT 
            generated_at_hour, source_name, avg(source_value) as hourly_source_value
            FROM hourtable 
            GROUP BY generated_at_hour, source_name
        )
        SELECT 
        datetrunc('day', generated_at_hour) as generated_at_date, 
        source_name, 
        ROUND(SUM(hourly_source_value)) as daily_cumulative_source_value
        FROM houragg
        GROUP BY datetrunc('day', generated_at_hour), source_name
        ORDER BY generated_at_date
        """
    )
    return (consumptiondf,)


@app.cell
def _(FG_DWH, HydroProductionRaw, dedup, houragg, hourtable, mo):
    _df = mo.sql(
        f"""
        with rename as (
        SELECT endTime,
        'Hydroelectric Power Production' as source_name, 
        "Hydro power production - real time data" as source_value
        FROM FG_DWH.landing."HydroProductionRaw"
        ), dedup as (
            SELECT endTime, 
            source_name, 
            AVG(source_value) as source_value
            FROM rename
            GROUP BY 
            endTime, 
            source_name
        ), hourtable as (
            SELECT 
            datetrunc('hour', endTime) as generated_at_hour,
            source_name, 
            source_value
            FROM dedup
        ), houragg as (
            SELECT 
            generated_at_hour, source_name, avg(source_value) as hourly_source_value
            FROM hourtable 
            GROUP BY generated_at_hour, source_name
        )
        SELECT 
        datetrunc('day', generated_at_hour) as generated_at_date, 
        source_name, 
        ROUND(SUM(hourly_source_value)) as daily_cumulative_source_value
        FROM houragg
        GROUP BY datetrunc('day', generated_at_hour), source_name
        ORDER BY generated_at_date
        """
    )
    return


@app.cell
def _(FG_DWH, NuclearProductionRaw, dedup, houragg, hourtable, mo):
    nuclear = mo.sql(
        f"""
        with rename as (
        SELECT endTime,
        'Nuclear Power Production' as source_name, 
        "Nuclear power production - real time data" as source_value
        FROM FG_DWH.landing."NuclearProductionRaw"
        ), dedup as (
            SELECT endTime, 
            source_name, 
            AVG(source_value) as source_value
            FROM rename
            GROUP BY 
            endTime, 
            source_name
        ), hourtable as (
            SELECT 
            datetrunc('hour', endTime) as generated_at_hour,
            source_name, 
            source_value
            FROM dedup
        ), houragg as (
            SELECT 
            generated_at_hour, source_name, avg(source_value) as hourly_source_value
            FROM hourtable 
            GROUP BY generated_at_hour, source_name
        )
        SELECT 
        datetrunc('day', generated_at_hour) as generated_at_date, 
        source_name, 
        ROUND(SUM(hourly_source_value)) as daily_cumulative_source_value
        FROM houragg
        GROUP BY datetrunc('day', generated_at_hour), source_name
        ORDER BY generated_at_date
        """
    )
    return (nuclear,)


@app.cell
def _(FG_DWH, WindProductionRaw, dedup, houragg, hourtable, mo):
    _df = mo.sql(
        f"""
        with rename as (
        SELECT endTime,
        'Wind Power Production' as source_name, 
        "Wind power production - real time data" as source_value
        FROM FG_DWH.landing."WindProductionRaw"
        ), dedup as (
            SELECT endTime, 
            source_name, 
            AVG(source_value) as source_value
            FROM rename
            GROUP BY 
            endTime, 
            source_name
        ), hourtable as (
            SELECT 
            datetrunc('hour', endTime) as generated_at_hour,
            source_name, 
            source_value
            FROM dedup
        ), houragg as (
            SELECT 
            generated_at_hour, source_name, avg(source_value) as hourly_source_value
            FROM hourtable 
            GROUP BY generated_at_hour, source_name
        )
        SELECT 
        datetrunc('day', generated_at_hour) as generated_at_date, 
        source_name, 
        ROUND(SUM(hourly_source_value)) as daily_cumulative_source_value
        FROM houragg
        GROUP BY datetrunc('day', generated_at_hour), source_name
        ORDER BY generated_at_date
        """
    )
    return


@app.cell
def _(
    FG_DWH,
    TotalElectricityProductionRaw,
    dedup,
    houragg,
    hourtable,
    mo,
):
    _df = mo.sql(
        f"""
        with rename as (
        SELECT endTime,
        'Total Electricity Production' as source_name, 
        "Electricity production in Finland - real time data" as source_value
        FROM FG_DWH.landing."TotalElectricityProductionRaw"
        ), dedup as (
            SELECT endTime, 
            source_name, 
            AVG(source_value) as source_value
            FROM rename
            GROUP BY 
            endTime, 
            source_name
        ), hourtable as (
            SELECT 
            datetrunc('hour', endTime) as generated_at_hour,
            source_name, 
            source_value
            FROM dedup
        ), houragg as (
            SELECT 
            generated_at_hour, source_name, avg(source_value) as hourly_source_value
            FROM hourtable 
            GROUP BY generated_at_hour, source_name
        )
        SELECT 
        datetrunc('day', generated_at_hour) as generated_at_date, 
        source_name, 
        ROUND(SUM(hourly_source_value)) as daily_cumulative_source_value
        FROM houragg
        GROUP BY datetrunc('day', generated_at_hour), source_name
        ORDER BY generated_at_date
        """
    )
    return


@app.cell
def _(
    consumptiondf,
    generated_at_date,
    hydro,
    mo,
    nuclear,
    production,
    wind,
):
    alltables = mo.sql(
        f"""
        with basetable as (
        SELECT *
        FROM consumptiondf
        UNION ALL 
        SELECT * 
        FROM hydro 
        UNION ALL 
        SELECT * 
        FROM nuclear 
        UNION ALL 
        SELECT * 
        FROM wind 
        UNION ALL 
        SELECT * 
        FROM production 
        )
        SELECT * 
        FROM basetable
        WHERE extract(YEAR FROM generated_at_date) < 2025
        """
    )
    return (alltables,)


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        --COPY alltables TO 'basetable.csv' (HEADER, DELIMITER ',');
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        --COPY alltables TO 'basetable.parquet' (FORMAT PARQUET);
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        --COPY 
        --(SELECT generated_at_date, source_name, SUM(source_value), min(source_value), max(source_value), avg(source_value)
        --FROM dateleveldf
        --GROUP BY generated_at_date, source_name)
        --TO 'rolluptable.parquet' (FORMAT PARQUET);
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        --COPY 
        --(SELECT generated_at_date, source_name, SUM(source_value), min(source_value), max(source_value), avg(source_value)
        --FROM dateleveldf
        --GROUP BY generated_at_date, source_name)
        --TO 'rolluptable.csv' (HEADER, DELIMITER ',');
        """
    )
    return


@app.cell
def _(ElectrictyConsumptionRaw, FG_DWH, hourtable, mo):
    _df = mo.sql(
        f"""
        WITH basetable as (
        SELECT datetrunc('hour', endTime) as endHour, "Electricity consumption in Finland - real time data" as source_value
        FROM FG_DWH.landing.ElectrictyConsumptionRaw 
        LIMIT 10000
        ), hourtable as (
        SELECT endHour, AVG(source_value) as source_value
        FROM basetable
        GROUP BY endHour
        )
        SELECT * 
        FROM hourtable
        """
    )
    return


@app.cell
def _(hourtable, mo):
    _df = mo.sql(
        f"""
        SELECT datetrunc('day', endHour), AVG(source_value), SUM(source_value)
        FROM hourtable 
        GROUP BY datetrunc('day', endHour)
        ORDER BY datetrunc('day', endHour)
        """
    )
    return


@app.cell
def _():
    return


@app.cell
def _(FG_DWH, mo, stg_fingrid__nuclear):
    _df = mo.sql(
        f"""
        SELECT * FROM FG_DWH.dbt_staging.stg_fingrid__nuclear LIMIT 100
        """
    )
    return


@app.cell
def _():
    return


@app.cell
def _(FG_DWH, NuclearProductionRaw, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM FG_DWH.landing.NuclearProductionRaw LIMIT 100
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
