import os
import sys
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_bea_gdp(*args, **kwargs) -> pd.DataFrame:
    """
    DATA LOADER BLOCK
    Fetch BEA GDP and components from the NIPA API.
    In Mage UI: shows a live data preview after execution.
    """
    from loguru import logger
    from datetime import date

    # Resolve project root for ingestion.* imports (handles Mage exec() context)
    _proj = os.environ.get("PYTHONPATH", "").split(":")[0]
    if _proj and _proj not in sys.path:
        sys.path.insert(0, _proj)

    from ingestion.bea_pipeline import BEAClient, NIPA_TABLES, parse_bea_period
    from ingestion.base_pipeline import safe_float

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
