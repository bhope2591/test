{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'STATE',
    pre_hook = load_raw_provider_info()
) }}

SELECT
    STATE AS state,
    AVG(REPORTED_TOTAL_NURSE_HOURS_PER_RESIDENT_PER_DAY) AS avg_hppd
FROM {{ source('healthcare_raw', 'RAW_NH_PROVIDERINFO_OCT2024') }}
WHERE REPORTED_TOTAL_NURSE_HOURS_PER_RESIDENT_PER_DAY IS NOT NULL
GROUP BY STATE
ORDER BY STATE
