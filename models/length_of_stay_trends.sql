{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'reporting_period || \'-\' || stay_type',
    pre_hook = load_raw_quality_mds()
) }}

WITH base AS (
    SELECT
        "CMS_Certification_Number" AS cms_certification_number,
        "Measure_Description",
        "Stay_Type",  -- expected values: 'Short Stay' or 'Long Stay'
        "Reporting_Period",       -- e.g. 'Q1 2024' or a month value
        "Measure_Value"  -- percentage or rate
    FROM {{ source('healthcare_raw', 'RAW_NH_QUALITYMSR_MDS_OCT2024') }}
    WHERE "Stay_Type" IS NOT NULL
),

aggregated AS (
    SELECT
        Reporting_Period AS reporting_period,
        Stay_Type AS stay_type,
        COUNT(*) AS total_measures,
        AVG(Measure_Value) AS avg_measure_value
    FROM base
    GROUP BY reporting_period, stay_type
)

SELECT *
FROM aggregated
ORDER BY reporting_period, stay_type
