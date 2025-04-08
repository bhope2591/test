{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='date_id',
    pre_hook= load_raw_department() 
) }}
-- notes: using delete+insert for scd1 type table. using prehook to pull in raw data from s3 from snowflake before transformations happen below.
WITH source AS (
    SELECT 
        DISTINCT date,
        isholiday,
        MD5(TO_VARCHAR(date)) AS date_id
    FROM {{ source('walmart_raw', 'RAW_DEPARTMENT') }}
)

SELECT
    s.date_id,
    s.date,
    s.isholiday,
    COALESCE(t.insert_date, CURRENT_TIMESTAMP) AS insert_date,
    CURRENT_TIMESTAMP AS update_date
FROM source s
LEFT JOIN {{ this }} t -- pointer existing version of model in snowflake (which we incrementally update)
    ON s.date_id = t.date_id

{% if is_incremental() %}
WHERE (s.date_id, s.date, s.isholiday) NOT IN (
    SELECT date_id, date, isholiday FROM {{ this }} -- filter only rows that are new or updated
)
{% endif %}
