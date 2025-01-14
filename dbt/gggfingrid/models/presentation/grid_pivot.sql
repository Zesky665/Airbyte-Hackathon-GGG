with basepiv as (
    SELECT *
    FROM {{ ref('stg_fct_srcs_piv') }}
),
rename as
(
    SELECT
        date_value,
        year_value,
        month_value,
        iso_week_num,
        day_value,
        hour_value,
        "Hydroelectric Power Production" as hydro, 
        "Nuclear Power Production" as nuclear, 
        "Total Electricity Production" as production, 
        "Total Power Consumption" as consumption, 
        "Wind Power Production" as wind
        FROM basepiv
)
SELECT * 
FROM rename 