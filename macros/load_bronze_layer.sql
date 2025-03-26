{% macro load_raw_department() %}
    {% do log("Running COPY INTO RAW_DEPARTMENT", info=True) %}
    COPY INTO BH_DB.PUBLIC.RAW_DEPARTMENT
    FROM @BH_DB.PUBLIC.S3_STAGE/department.csv
    FILE_FORMAT = (
        TYPE = 'CSV',
        FIELD_OPTIONALLY_ENCLOSED_BY = '"',
        SKIP_HEADER = 1
    );
{% endmacro %}



{% macro load_raw_fact() %}
    COPY INTO raw_fact
    FROM @s3_stage/data/fact.csv
    FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1 NULL_IF=('NA', ''));
{% endmacro %}

{% macro load_raw_stores() %}
    COPY INTO raw_stores
    FROM @s3_stage/data/stores.csv
    FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
{% endmacro %}
