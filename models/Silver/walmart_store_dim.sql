{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['store_id', 'dept_id'],
    pre_hook=[load_raw_department(), load_raw_stores()]
) }}
-- notes: using delete+insert for scd1 type table. using prehook to pull in raw data from s3 from snowflake before transformations happen below.
SELECT DISTINCT
    d.store AS store_id,
    d.dept AS dept_id,
    s.type AS store_type,
    s.size AS store_size,
    COALESCE(t.insert_date, CURRENT_TIMESTAMP) AS insert_date,
    CURRENT_TIMESTAMP AS update_date
FROM {{ source('walmart_raw', 'RAW_DEPARTMENT') }} d
JOIN {{ source('walmart_raw', 'RAW_STORES') }} s
    ON d.store = s.store
LEFT JOIN {{ this }} t
    ON d.store = t.store_id AND d.dept = t.dept_id

{% if is_incremental() %}
WHERE (d.store, d.dept, s.size) NOT IN (
    SELECT store_id, dept_id, store_size FROM {{ this }}
)
{% endif %}
