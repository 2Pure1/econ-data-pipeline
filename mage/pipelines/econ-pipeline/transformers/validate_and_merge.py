if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def validate_and_merge(bea_df, fred_df, *args, **kwargs):

    """
    TRANSFORMER BLOCK
    Validate each source independently, then merge into a monthly summary.
    Mage shows a diff of the DataFrame before/after this block.
    """
    import pandas as pd
    from loguru import logger

    # Auto-detect argument order: Mage may wire upstream blocks in any sequence.
    # BEA has 'series_name'; FRED has 'series_id'.
    if "series_name" not in bea_df.columns and "series_name" in fred_df.columns:
        bea_df, fred_df = fred_df, bea_df

    # ── Validate BEA ──────────────────────────────────────────────────────────
    assert not bea_df.empty,                    "BEA DataFrame is empty"
    assert "series_name" in bea_df.columns,     "BEA missing series_name column"
    assert bea_df["value"].notna().mean() > 0.8, "BEA has too many null values (>20%)"

    # ── Validate FRED ─────────────────────────────────────────────────────────
    assert not fred_df.empty,                      "FRED DataFrame is empty"
    assert set(["FEDFUNDS", "M2SL"]).issubset(
        fred_df["series_id"].unique()
    ), "Expected FRED series missing"

    # ── Pivot BEA to wide ─────────────────────────────────────────────────────
    bea_wide = (
        bea_df[bea_df["series_name"].isin(["gdp", "personal_consumption_expenditures"])]
        .pivot_table(index="period_date", columns="series_name", values="value", aggfunc="last")
        .reset_index()
        .rename(columns={"period_date": "month", "gdp": "gdp_billions",
                         "personal_consumption_expenditures": "pce_billions"})
    )
    bea_wide["month"] = bea_wide["month"].dt.to_period("M").dt.to_timestamp()

    # ── Pivot FRED to wide ────────────────────────────────────────────────────
    fred_wide = (
        fred_df
        .assign(month=lambda x: x["observation_date"].dt.to_period("M").dt.to_timestamp())
        .pivot_table(index="month", columns="series_id", values="value", aggfunc="last")
        .reset_index()
        .rename(columns={
            "FEDFUNDS": "fed_funds_rate",
            "M2SL":     "m2_billions",
            "T10Y2Y":   "yield_curve_spread",
            "CPIAUCSL": "cpi",
        })
    )

    # ── Merge on month ────────────────────────────────────────────────────────
    merged = bea_wide.merge(fred_wide, on="month", how="outer").sort_values("month")

    # Derived features
    merged["cpi_yoy_pct"] = merged["cpi"].pct_change(12) * 100
    merged["yield_curve_inverted"] = merged["yield_curve_spread"] < 0

    logger.info(f"Merged DataFrame: {len(merged):,} rows × {len(merged.columns)} columns")
    return merged



