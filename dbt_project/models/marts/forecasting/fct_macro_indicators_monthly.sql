-- models/marts/forecasting/fct_macro_indicators_monthly.sql
-- Wide monthly macro table for ML feature engineering.
-- Pivots FRED series into columns; joins market data.
-- Used by econ-forecast-engine as primary feature source.

{{
  config(
    materialized='table',
    schema='marts',
    indexes=[
      {'columns': ['observation_month'], 'unique': false},
    ]
  )
}}

with fred as (
    select * from {{ ref('stg_fred_observations') }}
    where not is_missing
),

-- Pivot key macro series into columns
gdp as (
    select observation_quarter as period, value as gdp_billions_usd
    from fred where series_id = 'GDP'
),

real_gdp as (
    select observation_quarter as period, value as real_gdp_billions
    from fred where series_id = 'GDPC1'
),

cpi as (
    select observation_month as period, value as cpi_all_urban
    from fred where series_id = 'CPIAUCSL'
),

core_cpi as (
    select observation_month as period, value as core_cpi
    from fred where series_id = 'CPILFESL'
),

pce as (
    select observation_month as period, value as pce_price_index
    from fred where series_id = 'PCEPI'
),

core_pce as (
    select observation_month as period, value as core_pce
    from fred where series_id = 'PCEPILFE'
),

unemployment as (
    select observation_month as period, value as unemployment_rate
    from fred where series_id = 'UNRATE'
),

u6 as (
    select observation_month as period, value as u6_unemployment_rate
    from fred where series_id = 'U6RATE'
),

payrolls as (
    select observation_month as period, value as nonfarm_payrolls_thousands
    from fred where series_id = 'PAYEMS'
),

lfpr as (
    select observation_month as period, value as labor_force_participation_rate
    from fred where series_id = 'CIVPART'
),

fed_funds as (
    select observation_month as period, value as fed_funds_rate
    from fred where series_id = 'FEDFUNDS'
),

m2 as (
    select observation_month as period, value as m2_money_supply_billions
    from fred where series_id = 'M2SL'
),

consumer_sentiment as (
    select observation_month as period, value as umich_consumer_sentiment
    from fred where series_id = 'UMCSENT'
),

industrial_production as (
    select observation_month as period, value as industrial_production_index
    from fred where series_id = 'INDPRO'
),

housing_starts as (
    select observation_month as period, value as housing_starts_thousands
    from fred where series_id = 'HOUST'
),

trade_balance as (
    select observation_month as period, value as trade_balance_millions
    from fred where series_id = 'BOPGSTB'
),

-- Monthly average of daily market data
market_monthly as (
    select
        trade_month                                                     as period,
        avg(case when ticker = '^GSPC' then close_price end)           as sp500_close_avg,
        avg(case when ticker = '^VIX'  then close_price end)           as vix_avg,
        avg(case when ticker = 'DX-Y.NYB' then close_price end)        as usd_index_avg,
        avg(case when ticker = 'CL=F' then close_price end)            as wti_oil_avg,
        avg(case when ticker = 'GC=F' then close_price end)            as gold_avg,
        avg(case when ticker = '^GSPC' then daily_return end)          as sp500_monthly_return_avg,
        avg(case when ticker = '^GSPC' then rolling_vol_20d end)       as sp500_realised_vol_avg
    from {{ ref('stg_market_prices') }}
    group by 1
),

-- Spine: all months since 1990
spine as (
    select
        generate_series(
            '1990-01-01'::date,
            date_trunc('month', current_date)::date,
            interval '1 month'
        )::date as observation_month
),

-- Joined wide table
joined as (
    select
        s.observation_month,
        extract(year from s.observation_month)::int         as year,
        extract(month from s.observation_month)::int        as month,
        date_trunc('quarter', s.observation_month)::date    as quarter,

        -- Output
        g.gdp_billions_usd,
        rg.real_gdp_billions,

        -- Inflation
        c.cpi_all_urban,
        cc.core_cpi,
        p.pce_price_index,
        cp.core_pce,

        -- Computed: YoY inflation rates
        round(
            100.0 * (c.cpi_all_urban - lag(c.cpi_all_urban, 12) over (order by s.observation_month))
            / nullif(lag(c.cpi_all_urban, 12) over (order by s.observation_month), 0),
            2
        )                                                   as cpi_yoy_pct,

        round(
            100.0 * (cp.core_pce - lag(cp.core_pce, 12) over (order by s.observation_month))
            / nullif(lag(cp.core_pce, 12) over (order by s.observation_month), 0),
            2
        )                                                   as core_pce_yoy_pct,

        -- Labor
        u.unemployment_rate,
        u6.u6_unemployment_rate,
        prl.nonfarm_payrolls_thousands,
        lf.labor_force_participation_rate,

        -- Computed: MoM payroll change
        prl.nonfarm_payrolls_thousands
            - lag(prl.nonfarm_payrolls_thousands, 1) over (order by s.observation_month)
                                                            as nonfarm_payrolls_mom_change,

        -- Monetary
        ff.fed_funds_rate,
        m2.m2_money_supply_billions,

        -- Sentiment & Production
        cs.umich_consumer_sentiment,
        ip.industrial_production_index,
        hs.housing_starts_thousands,
        tb.trade_balance_millions,

        -- Markets
        mm.sp500_close_avg,
        mm.vix_avg,
        mm.usd_index_avg,
        mm.wti_oil_avg,
        mm.gold_avg,
        mm.sp500_monthly_return_avg,
        mm.sp500_realised_vol_avg,

        -- Data completeness flag
        case
            when c.cpi_all_urban is null
              or u.unemployment_rate is null
              or ff.fed_funds_rate is null
            then false
            else true
        end                                                 as is_complete_record,

        current_timestamp                                   as dbt_updated_at

    from spine s
    left join gdp g                 on s.observation_month = g.period
    left join real_gdp rg           on s.observation_month = rg.period
    left join cpi c                 on s.observation_month = c.period
    left join core_cpi cc           on s.observation_month = cc.period
    left join pce p                 on s.observation_month = p.period
    left join core_pce cp           on s.observation_month = cp.period
    left join unemployment u        on s.observation_month = u.period
    left join u6                    on s.observation_month = u6.period
    left join payrolls prl          on s.observation_month = prl.period
    left join lfpr lf               on s.observation_month = lf.period
    left join fed_funds ff          on s.observation_month = ff.period
    left join m2                    on s.observation_month = m2.period
    left join consumer_sentiment cs on s.observation_month = cs.period
    left join industrial_production ip on s.observation_month = ip.period
    left join housing_starts hs     on s.observation_month = hs.period
    left join trade_balance tb      on s.observation_month = tb.period
    left join market_monthly mm     on s.observation_month = mm.period
)

select * from joined
order by observation_month
