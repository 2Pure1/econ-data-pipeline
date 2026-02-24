-- models/staging/stg_market_prices.sql
-- Deduplicates and type-casts raw market prices loaded by yfinance_pipeline via dlt.
-- Renames OHLCV columns to snake_case with _price suffix for clarity.
-- Adds trade_month for monthly aggregation in fct_macro_indicators_monthly.

{{ config(materialized='view', schema='staging') }}

with source as (
    select * from {{ source('raw', 'market_prices') }}
),

-- Deduplicate on (ticker, trade_date). yfinance may be re-ingested for recent history.
deduped as (
    select
        *,
        row_number() over (
            partition by ticker, trade_date
            order by ingested_at desc
        ) as _row_num
    from source
),

final as (
    select
        -- Primary key dimensions
        ticker,
        cast(trade_date as date)                                                as trade_date,
        cast(date_trunc('month', cast(trade_date as date)) as date)             as trade_month,

        -- OHLCV (renamed for clarity; avoid shadowing SQL reserved words)
        cast(open   as float)                                                   as open_price,
        cast(high   as float)                                                   as high_price,
        cast(low    as float)                                                   as low_price,
        cast(close  as float)                                                   as close_price,
        cast(volume as float)                                                   as volume,

        -- Derived features (computed in yfinance_pipeline.py)
        cast(daily_return     as float)                                         as daily_return,
        cast(log_return       as float)                                         as log_return,
        cast(rolling_vol_20d  as float)                                         as rolling_vol_20d,
        cast(ma_50            as float)                                         as ma_50,
        cast(ma_200           as float)                                         as ma_200,
        cast(above_200ma      as boolean)                                       as above_200ma,

        -- Ticker metadata
        ticker_name,
        category,
        asset_class,
        source,

        -- Audit
        ingested_at

    from deduped
    where _row_num = 1
      and close is not null   -- exclude rows with no price data
)

select * from final
