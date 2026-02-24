-- models/staging/stg_fred_observations.sql
-- Deduplicates and type-casts raw FRED observations loaded by dlt.
-- Adds observation_month and observation_quarter columns used by
-- fct_macro_indicators_monthly for spine-based joins.

{{ config(materialized='view', schema='staging') }}

with source as (
    select * from {{ source('raw', 'fred_observations') }}
),

-- Deduplicate: keep the most-recently-ingested row per (series_id, observation_date).
-- dlt may load the same observation twice on retries; ingested_at is the tiebreaker.
deduped as (
    select
        *,
        row_number() over (
            partition by series_id, observation_date
            order by ingested_at desc
        ) as _row_num
    from source
),

final as (
    select
        -- Primary key dimensions
        series_id,

        -- Date columns: cast to DATE and derive spine columns
        cast(observation_date as date)                                  as observation_date,
        cast(date_trunc('month',  cast(observation_date as date)) as date) as observation_month,
        cast(date_trunc('quarter', cast(observation_date as date)) as date) as observation_quarter,

        -- Value
        cast(value as float)                                            as value,
        coalesce(value_is_missing, value is null)                       as is_missing,

        -- Series metadata
        description,
        category,
        units,
        frequency,
        source,
        series_url,

        -- Audit
        ingested_at,
        pipeline_version

    from deduped
    where _row_num = 1
)

select * from final
