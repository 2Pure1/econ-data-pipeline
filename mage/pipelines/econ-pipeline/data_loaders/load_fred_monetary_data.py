import os
import sys
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_fred_monetary(*args, **kwargs) -> pd.DataFrame:
    """
    DATA LOADER BLOCK
    Fetch Fed Funds Rate, M2, and yield curve from FRED.
    Runs in parallel with load_bea_gdp in the Mage DAG.
    """
    import requests
    from loguru import logger

    # Resolve project root for ingestion.* imports (handles Mage exec() context)
    _proj = os.environ.get("PYTHONPATH", "").split(":")[0]
    if _proj and _proj not in sys.path:
        sys.path.insert(0, _proj)

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
