{% macro load_raw_provider_info() %}
    {% set sql %}
        TRUNCATE TABLE BH_DB.PUBLIC.RAW_NH_PROVIDERINFO_OCT2024;
        COPY INTO BH_DB.PUBLIC.RAW_NH_PROVIDERINFO_OCT2024
        FROM @s3_stage/NH_ProviderInfo_Oct2024.csv
        FILE_FORMAT = (
            TYPE = 'CSV',
            FIELD_OPTIONALLY_ENCLOSED_BY = '"',
            SKIP_HEADER = 1,
            ENCODING = 'ISO-8859-1'
        )
        ON_ERROR = 'CONTINUE'
        FORCE = TRUE;
    {% endset %}
    {{ return(sql) }}
{% endmacro %}

{% macro load_raw_quality_msr_claims() %}
    {% set sql %}
        TRUNCATE TABLE BH_DB.PUBLIC.RAW_NH_QUALITYMSR_CLAIMS_OCT2024;
        COPY INTO BH_DB.PUBLIC.RAW_NH_QUALITYMSR_CLAIMS_OCT2024
        FROM @s3_stage/NH_QualityMsr_Claims_Oct2024.csv
        FILE_FORMAT = (
            TYPE = 'CSV',
            FIELD_OPTIONALLY_ENCLOSED_BY = '"',
            SKIP_HEADER = 1,
            ENCODING = 'ISO-8859-1'
        )
        ON_ERROR = 'CONTINUE'
        FORCE = TRUE;
    {% endset %}
    {{ return(sql) }}
{% endmacro %}

{% macro load_raw_pbj_daily() %}
    {% set sql %}
        TRUNCATE TABLE BH_DB.PUBLIC.RAW_PBJ_DAILY_NURSE_STAFFING;
        COPY INTO BH_DB.PUBLIC.RAW_PBJ_DAILY_NURSE_STAFFING
        FROM @s3_stage/PBJ_Daily_Nurse_Staffing_Q2_2024.csv
        FILE_FORMAT = (
            TYPE = 'CSV',
            FIELD_OPTIONALLY_ENCLOSED_BY = '"',
            SKIP_HEADER = 1,
            ENCODING = 'ISO-8859-1'
        )
        ON_ERROR = 'CONTINUE'
        FORCE = TRUE;
    {% endset %}
    {{ return(sql) }}
{% endmacro %}

{% macro load_raw_snf_vbp_facility_performance() %}
    {% set sql %}
        TRUNCATE TABLE BH_DB.PUBLIC.RAW_FY_2024_SNF_VBP_FACILITY_PERFORMANCE;
        COPY INTO BH_DB.PUBLIC.RAW_FY_2024_SNF_VBP_FACILITY_PERFORMANCE
        FROM @s3_stage/FY_2024_SNF_VBP_Facility_Performance.csv
        FILE_FORMAT = (
            TYPE = 'CSV',
            FIELD_OPTIONALLY_ENCLOSED_BY = '"',
            SKIP_HEADER = 1,
            ENCODING = 'ISO-8859-1'
        )
        ON_ERROR = 'CONTINUE'
        FORCE = TRUE;
    {% endset %}
    {{ return(sql) }}
{% endmacro %}

{% macro load_US_ave_info() %}
    {% set sql %}
        TRUNCATE TABLE BH_DB.PUBLIC.NH_STATE_US_AVERAGES_INFO_DIM;
        COPY INTO BH_DB.PUBLIC.NH_STATE_US_AVERAGES_INFO_DIM
        FROM @s3_stage/NH_StateUSAverages_Oct2024.csv
        FILE_FORMAT = (
            TYPE = 'CSV',
            FIELD_OPTIONALLY_ENCLOSED_BY = '"',
            SKIP_HEADER = 1,
            ENCODING = 'ISO-8859-1'
        )
        ON_ERROR = 'CONTINUE'
        FORCE = TRUE;
    {% endset %}
    {{ return(sql) }}
{% endmacro %}