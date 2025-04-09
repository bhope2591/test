{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'PROVNUM || \'-\' || TO_VARCHAR("WorkDate")',
    pre_hook = load_raw_pbj_daily()
) }}

-- Notes: Fixing WorkDate conversion from YYYYMMDD integer to DATE

WITH source_data AS (
    SELECT 
        "PROVNUM" AS provnum,
        "STATE" AS state,
        TO_DATE("WorkDate", 'YYYYMMDD') AS work_date,
        "Hrs_RN",
        "Hrs_LPN"
    FROM {{ source('healthcare_raw', 'RAW_PBJ_DAILY_NURSE_STAFFING') }}
),

aggregated AS (
    SELECT
        provnum,
        state,
        DATE_TRUNC('month', work_date) AS month,
        SUM("Hrs_RN" + "Hrs_LPN") AS total_hours_worked
    FROM source_data
    GROUP BY provnum, state, month
)

SELECT * FROM aggregated
