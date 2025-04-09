{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'CMS_Certification_Number || \'-\' || MEASURE_DESCRIPTION',
    pre_hook = [load_raw_snf_vbp_facility_performance(), load_raw_quality_msr_claims()]
) }}

WITH claims AS (
    SELECT
        "CMS_Certification_Number" AS cms_certification_number,
        "MEASURE_DESCRIPTION" AS diagnosis_category
    FROM {{ source('healthcare_raw', 'RAW_NH_QUALITYMSR_CLAIMS_OCT2024') }}
),

vbp AS (
    SELECT
        "CMS_Certification_Number" AS cms_certification_number,
        "Provider_Name" AS provider_name,
        "State" AS state,
        "Performance_Period_FY2022_RSRR" AS readmission_rate
    FROM {{ source('healthcare_raw', 'RAW_FY_2024_SNF_VBP_FACILITY_PERFORMANCE') }}
    WHERE "Performance_Period_FY2022_RSRR" IS NOT NULL
)

SELECT
    vbp.cms_certification_number,
    vbp.provider_name,
    vbp.state,
    claims.diagnosis_category,
    vbp.readmission_rate
FROM vbp
JOIN claims
  ON vbp.cms_certification_number = claims.cms_certification_number
