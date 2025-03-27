{% macro load_raw_department() %}
TRUNCATE TABLE BH_DB.PUBLIC.RAW_DEPARTMENT;

COPY INTO BH_DB.PUBLIC.RAW_DEPARTMENT
FROM @s3_stage/department.csv
FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1
)
FORCE = TRUE;
{% endmacro %}

{% macro load_raw_fact() %}
TRUNCATE TABLE BH_DB.PUBLIC.RAW_FACT;

COPY INTO BH_DB.PUBLIC.RAW_FACT
FROM @s3_stage/fact.csv
FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    NULL_IF = ('NA', '')
)
FORCE = TRUE;
{% endmacro %}

{% macro load_raw_stores() %}
TRUNCATE TABLE BH_DB.PUBLIC.RAW_STORES;

COPY INTO BH_DB.PUBLIC.RAW_STORES
FROM @s3_stage/stores.csv
FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1
)
FORCE = TRUE;
{% endmacro %}
