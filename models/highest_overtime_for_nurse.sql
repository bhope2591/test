{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'provnum || \'-\' || work_date',
    pre_hook = load_raw_pbj_daily()
) }}

WITH source_data AS (
    SELECT 
        "PROVNUM" AS provnum,
        TO_DATE(TO_VARCHAR("WorkDate"), 'YYYYMMDD') AS work_date,
        COALESCE("Hrs_RN_emp", 0) AS rn_emp_hours,
        COALESCE("Hrs_RN_ctr", 0) AS rn_ctr_hours,
        COALESCE("Hrs_LPN_emp", 0) AS lpn_emp_hours,
        COALESCE("Hrs_LPN_ctr", 0) AS lpn_ctr_hours
    FROM {{ source('healthcare_raw', 'RAW_PBJ_DAILY_NURSE_STAFFING') }}
)

SELECT
    provnum,
    work_date,

    GREATEST(0, rn_emp_hours - (ROUND(rn_emp_hours / 8.0, 1) * 8)) AS rn_emp_overtime_hours, --ENSURE OVERTIME CANNOT BE GREATER TAHN ZERO
    GREATEST(0, rn_ctr_hours - (ROUND(rn_ctr_hours / 8.0, 1) * 8)) AS rn_ctr_overtime_hours,
    GREATEST(0, lpn_emp_hours - (ROUND(lpn_emp_hours / 8.0, 1) * 8)) AS lpn_emp_overtime_hours,
    GREATEST(0, lpn_ctr_hours - (ROUND(lpn_ctr_hours / 8.0, 1) * 8)) AS lpn_ctr_overtime_hours

FROM source_data
