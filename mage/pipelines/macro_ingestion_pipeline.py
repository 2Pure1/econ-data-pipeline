"""
mage/pipelines/macro_ingestion_pipeline.py
------------------------------------------
Mage.ai alternative orchestration for the macro data pipeline.

Mage vs Airflow — key differences:
  Airflow: mature, battle-tested, verbose DAG syntax, strong ecosystem
  Mage:    modern UI, notebook-style blocks, built-in data preview,
           easier local development, native dbt integration, faster iteration

Both are included in this project to demonstrate awareness of the
current orchestration landscape. Production choice depends on:
  - Team size (Mage is faster for small teams)
  - Existing infrastructure (Airflow if already invested)
  - Need for notebook-style debugging (Mage wins here)

To run Mage locally:
    pip install mage-ai
    mage start econ-data-pipeline
    # Then open http://localhost:6789

This file follows Mage's block-based pipeline structure:
  @data_loader   → extract
  @transformer   → transform / validate
  @data_exporter → load to destination
"""

from __future__ import annotations

import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import pandas as pd
from loguru import logger

# Mage decorators — only available inside a Mage runtime
# Wrapped in try/except so the file is importable outside Mage too
try:
    from mage_ai.data_preparation.decorators import (
        data_loader,
        transformer,
        data_exporter,
        test,
    )
    MAGE_AVAILABLE = True
except ImportError:
    # Define no-op decorators for standalone use
    def data_loader(f):   return f
    def transformer(f):   return f
    def data_exporter(f): return f
    def test(f):          return f
    MAGE_AVAILABLE = False


# ── Block 1: Load BEA GDP Data ───────────────────────────────────────────────

@data_loader
def load_bea_gdp(*args, **kwargs) -> pd.DataFrame:
    """
    DATA LOADER BLOCK
    Fetch BEA GDP and components from the NIPA API.
    In Mage UI: shows a live data preview after execution.
    """
    from ingestion.bea_pipeline import BEAClient, NIPA_TABLES, parse_bea_period
    from ingestion.base_pipeline import safe_float
    from datetime import date

    client = BEAClient()
    table_cfg = NIPA_TABLES["gdp_and_components"]
    line_map  = table_cfg["lines"]

    current_year = date.today().year
    year_range   = ",".join(str(y) for y in range(2010, current_year + 1))

    results = client.get_nipa(
        table_name=table_cfg["table_name"],
        frequency=table_cfg["frequency"],
        year=year_range,
    )

    records = []
    for obs in results.get("Data", []):
        line_num = int(obs.get("LineNumber", -1))
        if line_num not in line_map:
            continue
        period_date = parse_bea_period(obs["TimePeriod"], table_cfg["frequency"])
        if not period_date:
            continue
        raw_val = obs.get("DataValue", "")
        value   = None if raw_val in ("", "(D)", "(NA)") else safe_float(raw_val.replace(",", ""))
        records.append({
            "series_name":  line_map[line_num],
            "period_date":  pd.to_datetime(period_date),
            "value":        value,
            "source":       "bea",
            "frequency":    table_cfg["frequency"],
        })

    df = pd.DataFrame(records)
    logger.info(f"Loaded {len(df):,} BEA GDP records")
    return df


# ── Block 2: Load FRED Monetary Data ─────────────────────────────────────────

@data_loader
def load_fred_monetary(*args, **kwargs) -> pd.DataFrame:
    """
    DATA LOADER BLOCK
    Fetch Fed Funds Rate, M2, and yield curve from FRED.
    Runs in parallel with load_bea_gdp in the Mage DAG.
    """
    import requests
    from ingestion.base_pipeline import cfg, safe_float, normalize_date

    series = ["FEDFUNDS", "M2SL", "T10Y2Y", "CPIAUCSL"]
    records = []

    for series_id in series:
        resp = requests.get(
            "https://api.stlouisfed.org/fred/series/observations",
            params={
                "series_id":          series_id,
                "api_key":            cfg.FRED_API_KEY,
                "file_type":          "json",
                "observation_start":  "2010-01-01",
                "sort_order":         "asc",
            },
            timeout=30,
        )
        resp.raise_for_status()
        for obs in resp.json().get("observations", []):
            val = obs.get("value", ".")
            records.append({
                "series_id":        series_id,
                "observation_date": pd.to_datetime(obs["date"]),
                "value":            None if val == "." else safe_float(val),
                "source":           "fred",
            })

    df = pd.DataFrame(records)
    logger.info(f"Loaded {len(df):,} FRED monetary records")
    return df


# ── Block 3: Validate & Merge ─────────────────────────────────────────────────

@transformer
def validate_and_merge(bea_df: pd.DataFrame, fred_df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """
    TRANSFORMER BLOCK
    Validate each source independently, then merge into a monthly summary.
    Mage shows a diff of the DataFrame before/after this block.
    """
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


# ── Block 4: Load to PostgreSQL ───────────────────────────────────────────────

@data_exporter
def export_to_postgres(df: pd.DataFrame, *args, **kwargs) -> None:
    """
    DATA EXPORTER BLOCK
    Write the merged DataFrame to PostgreSQL.
    In Mage: shows row counts and column types after export.
    """
    from sqlalchemy import create_engine

    engine = create_engine(
        f"postgresql://{os.getenv('POSTGRES_USER','econ_user')}:"
        f"{os.getenv('POSTGRES_PASSWORD','')}@"
        f"{os.getenv('POSTGRES_HOST','localhost')}:"
        f"{os.getenv('POSTGRES_PORT','5432')}/"
        f"{os.getenv('POSTGRES_DB','econ_warehouse')}"
    )

    df.to_sql(
        name="mage_macro_summary",
        schema="raw",
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=1000,
    )
    logger.success(f"Exported {len(df):,} rows → raw.mage_macro_summary")


# ── Block 5: Load to Delta Lake ───────────────────────────────────────────────

@data_exporter
def export_to_delta(df: pd.DataFrame, *args, **kwargs) -> None:
    """
    DATA EXPORTER BLOCK
    Write the merged DataFrame to Delta Lake (alongside the Postgres export).
    Demonstrates Mage's ability to fan-out to multiple destinations.
    """
    delta_path = os.path.join(
        os.getenv("DELTA_S3_PATH", "/tmp/delta_lake"), "mage/macro_summary"
    )
    df.to_parquet(f"{delta_path}/data.parquet", index=False)
    logger.success(f"Exported {len(df):,} rows → {delta_path}")


# ── Tests ─────────────────────────────────────────────────────────────────────

@test
def test_output_not_empty(df: pd.DataFrame, *args) -> None:
    """Assert the merged output is not empty."""
    assert df is not None and len(df) > 0, "Output DataFrame is empty"


@test
def test_required_columns_present(df: pd.DataFrame, *args) -> None:
    """Assert key columns exist in the output."""
    required = {"month", "gdp_billions", "fed_funds_rate", "yield_curve_inverted"}
    missing  = required - set(df.columns)
    assert not missing, f"Missing columns: {missing}"


@test
def test_no_future_dates(df: pd.DataFrame, *args) -> None:
    """Assert no observation dates are in the future (look-ahead bias check)."""
    from datetime import date
    if "month" in df.columns:
        max_date = pd.to_datetime(df["month"]).max().date()
        assert max_date <= date.today(), f"Future date found: {max_date}"
