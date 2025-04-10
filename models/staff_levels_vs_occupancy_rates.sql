{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'PROVNUM || \'-\' || month',
    pre_hook = load_raw_pbj_daily()
) }}

WITH source_data AS (
    SELECT
        "PROVNUM" AS provnum,
        DATE_TRUNC('month', TO_DATE(TO_VARCHAR("WorkDate"), 'YYYYMMDD')) AS month,
        "Hrs_RN" + "Hrs_LPN" + "Hrs_CNA" AS daily_total_hours,
        "MDScensus" AS daily_census
    FROM {{ source('healthcare_raw', 'RAW_PBJ_DAILY_NURSE_STAFFING') }}
    WHERE "Hrs_RN" IS NOT NULL AND "MDScensus" IS NOT NULL
),

aggregated AS (
    SELECT
        provnum,
        month,
        SUM(daily_total_hours) AS total_hours_worked,
        AVG(daily_census) AS avg_daily_census
    FROM source_data
    GROUP BY provnum, month
)

SELECT * FROM aggregated
