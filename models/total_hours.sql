{{ config(
    materialized = 'incremental',
    incremental_strategy='delete+insert',
    unique_key = "provnum || '-' || workdate",
    pre_hook= load_raw_pbj_daily()
) }}
-- notes: using delete+insert for scd1 type table. using prehook to pull in raw data from s3 from snowflake before transformations happen below.

WITH source_data AS (

    SELECT 
        provnum,
        state,
        workdate,
        hours_worked
    FROM {{ source('healthcare_raw', 'RAW_PBJ_DAILY_NURSE_STAFFING') }}

),

aggregated AS (

    SELECT
        provnum,
        state,
        DATE_TRUNC('month', workdate) AS month,
        SUM(hours_worked) AS total_hours_worked
    FROM source_data
    GROUP BY provnum, state, month

)

SELECT * FROM aggregated