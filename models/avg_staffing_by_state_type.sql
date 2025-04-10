{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'STATE || \'-\' || PROVIDER_TYPE',
    pre_hook = load_raw_provider_info()
) }}

SELECT
    STATE AS state,
    PROVIDER_TYPE AS provider_type,

    -- Reported average staffing levels
    AVG(REPORTED_RN_HOURS_PER_RESIDENT_PER_DAY) AS avg_rn_staffing,
    AVG(REPORTED_LPN_HOURS_PER_RESIDENT_PER_DAY) AS avg_lpn_staffing,
    AVG(REPORTED_NURSE_AIDE_HOURS_PER_RESIDENT_PER_DAY) AS avg_aide_staffing,
    AVG(REPORTED_TOTAL_NURSE_HOURS_PER_RESIDENT_PER_DAY) AS avg_total_staffing

FROM {{ source('healthcare_raw', 'RAW_NH_PROVIDERINFO_OCT2024') }}
WHERE REPORTED_TOTAL_NURSE_HOURS_PER_RESIDENT_PER_DAY IS NOT NULL
GROUP BY STATE, PROVIDER_TYPE
ORDER BY STATE, PROVIDER_TYPE
