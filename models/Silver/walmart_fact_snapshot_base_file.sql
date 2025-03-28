{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['date_id', 'store_id', 'dept_id'],
    pre_hook=[load_raw_fact(), load_raw_department()]
) }}

SELECT DISTINCT
    d.date_id,                                      -- FK from date_dim
    s.store_id,                                     -- FK from store_dim
    rd.dept AS dept_id,                             -- from RAW_DEPARTMENT
    f.date,
    rd.weekly_sales,                                -- from RAW_DEPARTMENT
    f.fuel_price,
    f.temperature,
    f.unemployment,
    f.cpi,
    f.markdown1,
    f.markdown2,
    f.markdown3,
    f.markdown4,
    f.markdown5,
    CURRENT_TIMESTAMP       AS vrsn_start_date,
    NULL                    AS vrsn_end_date,
    CURRENT_TIMESTAMP       AS insert_date,
    CURRENT_TIMESTAMP       AS update_date

FROM {{ source('walmart_raw', 'RAW_FACT') }} f
JOIN {{ ref('walmart_store_dim') }} s
    ON f.store = s.store_id
JOIN {{ ref('walmart_date_dim') }} d
    ON f.date = d.date
JOIN {{ source('walmart_raw', 'RAW_DEPARTMENT') }} rd
    ON f.store = rd.store AND f.date = rd.date
