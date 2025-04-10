{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'MEASURE_PERIOD || \'-\' || stay_type',
    pre_hook = load_raw_quality_msr_claims()
) }}

WITH base AS (
    SELECT
        CMS_CERTIFICATION_NUMBER AS cms_certification_number,
        MEASURE_DESCRIPTION,
        MEASURE_PERIOD,
        ADJUSTED_SCORE,
        CASE 
            WHEN MEASURE_DESCRIPTION ILIKE '%short-stay%' THEN 'Short Stay'
            WHEN MEASURE_DESCRIPTION ILIKE '%long-stay%' THEN 'Long Stay'
            ELSE 'Other'
        END AS stay_type
    FROM {{ source('healthcare_raw', 'RAW_NH_QUALITYMSR_CLAIMS_OCT2024') }}
    WHERE MEASURE_DESCRIPTION ILIKE '%stay%'
),

aggregated AS (
    SELECT
        MEASURE_PERIOD AS reporting_period,
        stay_type,
        COUNT(*) AS total_measures,
        AVG(ADJUSTED_SCORE) AS avg_adjusted_score
    FROM base
    GROUP BY MEASURE_PERIOD, stay_type
)

SELECT *
FROM aggregated
ORDER BY reporting_period, stay_type
