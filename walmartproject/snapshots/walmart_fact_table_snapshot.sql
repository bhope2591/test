{% snapshot walmart_fact_snapshot %}

{{
    config(
        target_schema='PUBLIC',
        unique_key=['date_id', 'store_id', 'dept_id'],
        strategy='timestamp',
        updated_at='update_date'
    )
}}

SELECT *
FROM {{ ref('walmart_fact_snapshot_base_file') }}

{% endsnapshot %}