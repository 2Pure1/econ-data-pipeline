"""
fred_pipeline.py
----------------
Ingests macroeconomic indicators from the Federal Reserve Economic Data (FRED) API.

Indicators covered:
  GDP, CPI, Core CPI, PCE, Fed Funds Rate, Unemployment Rate,
  10Y Treasury Yield, 2Y Treasury Yield, M2 Money Supply,
  Consumer Sentiment, Industrial Production, Housing Starts

Usage:
    python ingestion/fred_pipeline.py --destination postgres
    python ingestion/fred_pipeline.py --destination bigquery
    python ingestion/fred_pipeline.py --indicators GDP,CPIAUCSL --start 2000-01-01
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from typing import Generator

import requests
from loguru import logger

from base_pipeline import BasePipeline, cfg, normalize_date, safe_float


# ── FRED Series Config ────────────────────────────────────────────────────────

FRED_SERIES = {
    # Output & Growth
    "GDP": {
        "description": "Gross Domestic Product",
        "category": "output",
        "units": "billions_usd",
        "frequency": "quarterly",
    },
    "GDPC1": {
        "description": "Real GDP (Chained 2017 Dollars)",
        "category": "output",
        "units": "billions_chained_2017_usd",
        "frequency": "quarterly",
    },
    "GDPPOT": {
        "description": "Real Potential GDP",
        "category": "output",
        "units": "billions_chained_2012_usd",
        "frequency": "quarterly",
    },
    # Inflation
    "CPIAUCSL": {
        "description": "Consumer Price Index (All Urban Consumers)",
        "category": "inflation",
        "units": "index_1982_84_100",
        "frequency": "monthly",
    },
    "CPILFESL": {
        "description": "Core CPI (Excluding Food and Energy)",
        "category": "inflation",
        "units": "index_1982_84_100",
        "frequency": "monthly",
    },
    "PCEPI": {
        "description": "PCE Price Index",
        "category": "inflation",
        "units": "index_2017_100",
        "frequency": "monthly",
    },
    "PCEPILFE": {
        "description": "Core PCE Price Index (Fed's preferred measure)",
        "category": "inflation",
        "units": "index_2017_100",
        "frequency": "monthly",
    },
    # Labor Market
    "UNRATE": {
        "description": "Unemployment Rate",
        "category": "labor",
        "units": "percent",
        "frequency": "monthly",
    },
    "PAYEMS": {
        "description": "Total Nonfarm Payroll Employment",
        "category": "labor",
        "units": "thousands_of_persons",
        "frequency": "monthly",
    },
    "CIVPART": {
        "description": "Labor Force Participation Rate",
        "category": "labor",
        "units": "percent",
        "frequency": "monthly",
    },
    "U6RATE": {
        "description": "U-6 Unemployment Rate (incl. underemployment)",
        "category": "labor",
        "units": "percent",
        "frequency": "monthly",
    },
    # Monetary Policy & Interest Rates
    "FEDFUNDS": {
        "description": "Effective Federal Funds Rate",
        "category": "monetary",
        "units": "percent",
        "frequency": "monthly",
    },
    "DGS10": {
        "description": "10-Year Treasury Constant Maturity Rate",
        "category": "monetary",
        "units": "percent",
        "frequency": "daily",
    },
    "DGS2": {
        "description": "2-Year Treasury Constant Maturity Rate",
        "category": "monetary",
        "units": "percent",
        "frequency": "daily",
    },
    "T10Y2Y": {
        "description": "10-Year minus 2-Year Treasury Spread (Yield Curve)",
        "category": "monetary",
        "units": "percent",
        "frequency": "daily",
    },
    "M2SL": {
        "description": "M2 Money Supply",
        "category": "monetary",
        "units": "billions_usd",
        "frequency": "monthly",
    },
    # Consumer & Business Sentiment
    "UMCSENT": {
        "description": "University of Michigan Consumer Sentiment",
        "category": "sentiment",
        "units": "index_1966_q1_100",
        "frequency": "monthly",
    },
    # Industrial & Housing
    "INDPRO": {
        "description": "Industrial Production Index",
        "category": "production",
        "units": "index_2017_100",
        "frequency": "monthly",
    },
    "HOUST": {
        "description": "Housing Starts",
        "category": "housing",
        "units": "thousands_of_units",
        "frequency": "monthly",
    },
    "MORTGAGE30US": {
        "description": "30-Year Fixed Rate Mortgage Average",
        "category": "housing",
        "units": "percent",
        "frequency": "weekly",
    },
    # Trade & External
    "BOPGSTB": {
        "description": "Trade Balance: Goods and Services",
        "category": "trade",
        "units": "millions_usd",
        "frequency": "monthly",
    },
    "DEXUSEU": {
        "description": "USD/EUR Exchange Rate",
        "category": "fx",
        "units": "usd_per_eur",
        "frequency": "daily",
    },
}

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"


# ── Pipeline ──────────────────────────────────────────────────────────────────

class FREDPipeline(BasePipeline):
    """
    Ingests FRED series data into PostgreSQL or BigQuery via dlt.

    Each record represents one observation: {series_id, date, value, metadata}.
    Supports incremental loads via observation_start parameter.
    """

    def __init__(
        self,
        destination: str = "postgres",
        series_ids: list[str] | None = None,
        start_date: str = "1990-01-01",
        end_date: str | None = None,
        **kwargs,
    ):
        super().__init__(destination=destination, write_disposition="merge", **kwargs)
        self.series_ids = series_ids or list(FRED_SERIES.keys())
        self.start_date = start_date
        self.end_date = end_date or date.today().isoformat()

        if not cfg.FRED_API_KEY:
            raise ValueError(
                "FRED_API_KEY not set. Get a free key at "
                "https://fred.stlouisfed.org/docs/api/api_key.html"
            )

    def get_pipeline_name(self) -> str:
        return "fred_pipeline"

    def get_resource_name(self) -> str:
        return "fred_observations"

    def _fetch_series(self, series_id: str) -> list[dict]:
        """Fetch all observations for a single FRED series."""
        params = {
            "series_id": series_id,
            "api_key": cfg.FRED_API_KEY,
            "file_type": "json",
            "observation_start": self.start_date,
            "observation_end": self.end_date,
            "sort_order": "asc",
        }

        response = requests.get(FRED_BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if "observations" not in data:
            logger.warning(f"No observations returned for {series_id}: {data}")
            return []

        # Archive raw response to S3
        if self.archive_to_s3 and self.s3:
            self.s3.archive(
                data=data,
                source="fred",
                indicator=series_id.lower(),
                partition_date=date.today(),
            )

        return data["observations"]

    def extract(self) -> Generator[dict, None, None]:
        """Yield normalized observation records for all configured series."""
        series_meta = {s: FRED_SERIES.get(s, {}) for s in self.series_ids}

        for series_id in self.series_ids:
            meta = series_meta.get(series_id, {})
            logger.info(f"Fetching FRED series: {series_id} ({meta.get('description', '')})")

            try:
                observations = self._fetch_series(series_id)
                logger.info(f"  → {len(observations)} observations")

                for obs in observations:
                    # FRED uses "." for missing values
                    raw_value = obs.get("value", ".")
                    value = None if raw_value == "." else safe_float(raw_value)

                    yield {
                        # Primary key: series + date
                        "series_id": series_id,
                        "observation_date": normalize_date(obs.get("date")),
                        # Value
                        "value": value,
                        "value_is_missing": value is None,
                        # Metadata
                        "description": meta.get("description", ""),
                        "category": meta.get("category", "unknown"),
                        "units": meta.get("units", ""),
                        "frequency": meta.get("frequency", ""),
                        "source": "fred",
                        "series_url": f"https://fred.stlouisfed.org/series/{series_id}",
                        # Audit fields
                        "ingested_at": datetime.utcnow().isoformat(),
                        "pipeline_version": "1.0.0",
                    }

            except requests.HTTPError as e:
                logger.error(f"HTTP error fetching {series_id}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error fetching {series_id}: {e}")
                raise


# ── CLI Entry Point ───────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="Run FRED data pipeline")
    parser.add_argument(
        "--destination",
        choices=["postgres", "bigquery"],
        default="postgres",
        help="Target destination",
    )
    parser.add_argument(
        "--indicators",
        type=str,
        default=None,
        help="Comma-separated FRED series IDs (default: all configured)",
    )
    parser.add_argument(
        "--start",
        type=str,
        default="1990-01-01",
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="End date (YYYY-MM-DD, default: today)",
    )
    parser.add_argument(
        "--no-s3",
        action="store_true",
        help="Skip S3 archival",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    series = args.indicators.split(",") if args.indicators else None

    pipeline = FREDPipeline(
        destination=args.destination,
        series_ids=series,
        start_date=args.start,
        end_date=args.end,
        archive_to_s3=not args.no_s3,
    )

    result = pipeline.run()
    logger.info(f"Result: {result}")
