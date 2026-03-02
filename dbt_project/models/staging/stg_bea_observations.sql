-- models/staging/stg_bea_observations.sql
-- Deduplicates and type-casts raw BEA NIPA observations loaded by dlt.
-- The BEA pipeline already normalises period strings to ISO dates in Python;
-- this model casts them to DATE and deduplicates on the compound key.

{{ config(materialized='view', schema='staging') }}

with source as (
    select * from {{ source('raw', 'bea_nipa_observations') }}
),

-- Deduplicate on (table_key, series_name, period_date).
-- BEA revises past periods; ingested_at captures the most recent vintage.
deduped as (
    select
        *,
        row_number() over (
            partition by table_key, series_name, period_date
            order by ingested_at desc
        ) as _row_num
    from source
),

final as (
    select
        -- Primary key dimensions
        table_key,
        series_name,
        cast(period_date as date)                                       as period_date,

        -- Value
        cast(value as {{ dbt.type_float() }})                           as value,
        coalesce(is_missing, value is null)                             as is_missing,

        -- BEA metadata
        bea_table_name,
        bea_line_number,
        bea_line_description,
        bea_period_str,
        frequency,
        units,
        description,
        source,

        -- Audit
        ingested_at,
        pipeline_version

    from deduped
    where _row_num = 1
)

select * from final
