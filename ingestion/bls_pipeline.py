"""
bls_pipeline.py
---------------
Ingests labor market data from the Bureau of Labor Statistics (BLS) Public Data API v2.
BLS is the authoritative source for employment, wages, productivity, and price data.

Why BLS directly instead of FRED?
  - BLS releases employment situation data (nonfarm payrolls, unemployment) at 8:30am ET
    on the first Friday of each month — FRED republishes with a delay
  - BLS provides CPI micro-series and seasonal adjustment variants not in FRED
  - Direct access demonstrates understanding of the primary data source

Series covered:
  CES0000000001  — Total Nonfarm Payrolls (seasonally adjusted, monthly, thousands)
  LNS14000000    — Unemployment Rate U-3 (SA, monthly, %)
  LNS13327709    — U-6 Underemployment Rate (SA, monthly, %)
  CIU1010000000000A — Employment Cost Index (SA, quarterly, % change)
  CUSR0000SA0    — CPI-U All Items (SA, monthly, 1982-84=100)
  CUSR0000SA0L1E — Core CPI ex Food & Energy (SA, monthly, 1982-84=100)
  PRS85006092    — Nonfarm Business Productivity (SA, quarterly, % change)
  CES0500000003  — Average Hourly Earnings, Private (SA, monthly, $)

API Docs: https://www.bls.gov/developers/api_signature_v2.htm
Registration (free, raises rate limit): https://data.bls.gov/registrationEngine/

Usage:
    python ingestion/bls_pipeline.py --destination postgres
    python ingestion/bls_pipeline.py --destination bigquery
    python ingestion/bls_pipeline.py --series CES0000000001,LNS14000000
    python ingestion/bls_pipeline.py --start 2010 --end 2024 --no-s3
"""

from __future__ import annotations

import argparse
from datetime import date, datetime
from typing import Generator

import requests
from loguru import logger

from base_pipeline import BasePipeline, cfg, normalize_date, safe_float


# ── BLS Series Config ─────────────────────────────────────────────────────────

BLS_SERIES: dict[str, dict] = {
    # ── Employment ────────────────────────────────────────────────────────────
    "CES0000000001": {
        "description": "Total Nonfarm Payrolls",
        "category": "labor",
        "units": "thousands_of_persons",
        "frequency": "monthly",
        "seasonal_adjustment": "seasonally_adjusted",
        "survey": "Current Employment Statistics",
    },
    "LNS14000000": {
        "description": "Unemployment Rate (U-3)",
        "category": "labor",
        "units": "percent",
        "frequency": "monthly",
        "seasonal_adjustment": "seasonally_adjusted",
        "survey": "Current Population Survey",
    },
    "LNS13327709": {
        "description": "U-6 Underemployment Rate (total unemployed + marginally attached + part-time for economic reasons)",
        "category": "labor",
        "units": "percent",
        "frequency": "monthly",
        "seasonal_adjustment": "seasonally_adjusted",
        "survey": "Current Population Survey",
    },
    "CES0500000003": {
        "description": "Average Hourly Earnings, All Private Employees",
        "category": "labor",
        "units": "dollars_per_hour",
        "frequency": "monthly",
        "seasonal_adjustment": "seasonally_adjusted",
        "survey": "Current Employment Statistics",
    },
    # ── Compensation ──────────────────────────────────────────────────────────
    "CIU1010000000000A": {
        "description": "Employment Cost Index, Wages and Salaries, All Workers",
        "category": "compensation",
        "units": "percent_change",
        "frequency": "quarterly",
        "seasonal_adjustment": "seasonally_adjusted",
        "survey": "Employment Cost Index",
    },
    # ── Prices ────────────────────────────────────────────────────────────────
    "CUSR0000SA0": {
        "description": "CPI-U All Items (Urban Consumers, Seasonally Adjusted)",
        "category": "inflation",
        "units": "index_1982_84_100",
        "frequency": "monthly",
        "seasonal_adjustment": "seasonally_adjusted",
        "survey": "Consumer Price Index",
    },
    "CUSR0000SA0L1E": {
        "description": "Core CPI-U Excluding Food and Energy",
        "category": "inflation",
        "units": "index_1982_84_100",
        "frequency": "monthly",
        "seasonal_adjustment": "seasonally_adjusted",
        "survey": "Consumer Price Index",
    },
    # ── Productivity ──────────────────────────────────────────────────────────
    "PRS85006092": {
        "description": "Nonfarm Business Productivity, Percent Change from Prior Quarter",
        "category": "productivity",
        "units": "percent_change",
        "frequency": "quarterly",
        "seasonal_adjustment": "seasonally_adjusted",
        "survey": "Productivity and Costs",
    },
}

BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
BLS_SERIES_URL_TEMPLATE = "https://data.bls.gov/timeseries/{series_id}"

# BLS API limits: registered key = 500 req/day, 50 series/req, 20 years/req
# Unregistered: 25 req/day, 25 series/req, 10 years/req
MAX_SERIES_PER_REQUEST = 50
MAX_YEARS_PER_REQUEST = 20


# ── BLS API Client ────────────────────────────────────────────────────────────

class BLSClient:
    """
    HTTP wrapper for the BLS Public Data API v2.

    Handles:
      - Multi-series bulk fetches (up to 50 series per request)
      - Year-range batching (20-year window per request for registered keys)
      - BLS-specific error parsing (errors in JSON body, not HTTP status)
    """

    def __init__(self, api_key: str = ""):
        self.api_key = api_key or cfg.BLS_API_KEY
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

        if not self.api_key:
            logger.warning(
                "BLS_API_KEY not set. Rate limits apply: 25 req/day, 10 years history. "
                "Register free at https://data.bls.gov/registrationEngine/"
            )

    def fetch_series(
        self,
        series_ids: list[str],
        start_year: int,
        end_year: int,
    ) -> list[dict]:
        """
        Fetch data for multiple BLS series in a single API call.

        The BLS v2 API accepts POST with a JSON body. Returns the raw
        Results.series list from the response.

        Args:
            series_ids:  List of BLS series IDs (max 50 per request)
            start_year:  First year of data to retrieve
            end_year:    Last year of data to retrieve (max 20-year window)

        Returns:
            List of series dicts, each containing seriesID + data list.
        """
        if len(series_ids) > MAX_SERIES_PER_REQUEST:
            raise ValueError(
                f"BLS API allows max {MAX_SERIES_PER_REQUEST} series per request; "
                f"got {len(series_ids)}"
            )

        payload: dict = {
            "seriesid":  series_ids,
            "startyear": str(start_year),
            "endyear":   str(end_year),
            "calculations": True,   # include percent change, net change
            "annualaverage": False,
        }

        if self.api_key:
            payload["registrationkey"] = self.api_key

        response = self.session.post(BLS_API_URL, json=payload, timeout=60)
        response.raise_for_status()

        data = response.json()

        status = data.get("status", "")
        if status != "REQUEST_SUCCEEDED":
            messages = data.get("message", [])
            raise ValueError(
                f"BLS API request failed (status={status}): {messages}"
            )

        results = data.get("Results", {})
        series_list = results.get("series", [])

        if not series_list:
            logger.warning(f"BLS returned empty series list for {series_ids}")

        return series_list


# ── Period Parsing ────────────────────────────────────────────────────────────

def parse_bls_period(year: str, period: str) -> str | None:
    """
    Convert BLS year + period codes to ISO date strings.

    BLS period formats:
      Monthly:   M01–M12  →  YYYY-01-01 through YYYY-12-01
      Quarterly: Q01–Q04  →  YYYY-01-01, YYYY-04-01, YYYY-07-01, YYYY-10-01
      Annual:    A01      →  YYYY-01-01
      Semi-ann:  S01/S02  →  YYYY-01-01, YYYY-07-01

    Returns ISO date string or None if unparseable.
    """
    if not year or not period or len(period) < 2:
        return None

    period_type = period[0].upper()
    try:
        period_num = int(period[1:])
    except ValueError:
        return None

    try:
        if period_type == "M":
            if 1 <= period_num <= 12:
                return f"{year}-{period_num:02d}-01"

        elif period_type == "Q":
            quarter_to_month = {1: 1, 2: 4, 3: 7, 4: 10}
            month = quarter_to_month.get(period_num)
            if month:
                return f"{year}-{month:02d}-01"

        elif period_type == "A":
            return f"{year}-01-01"

        elif period_type == "S":
            semi_to_month = {1: 1, 2: 7}
            month = semi_to_month.get(period_num)
            if month:
                return f"{year}-{month:02d}-01"

    except Exception:
        pass

    return None


def infer_frequency_from_period(period: str) -> str:
    """Infer frequency label from BLS period code."""
    if not period:
        return "unknown"
    p = period[0].upper()
    return {"M": "monthly", "Q": "quarterly", "A": "annual", "S": "semi_annual"}.get(p, "unknown")


# ── Pipeline ──────────────────────────────────────────────────────────────────

class BLSPipeline(BasePipeline):
    """
    Ingests BLS labor market data into PostgreSQL or BigQuery via dlt.

    Each record represents one observation:
      {series_id, observation_date, value, year, period, period_name, ...metadata}

    The output mirrors the FRED pipeline's normalized long format so that
    dbt staging models and the mart follow consistent patterns.

    Year-range batching: BLS allows max 20 years per request with a registered key.
    For long historical pulls (e.g. 1990–2024), the pipeline automatically splits
    into 20-year windows and merges results.
    """

    def __init__(
        self,
        destination: str = "postgres",
        series_ids: list[str] | None = None,
        start_year: int = 2000,
        end_year: int | None = None,
        **kwargs,
    ):
        super().__init__(destination=destination, write_disposition="merge", **kwargs)
        self.series_ids = series_ids or list(BLS_SERIES.keys())
        self.start_year = start_year
        self.end_year = end_year or date.today().year
        self.client = BLSClient()

    def get_pipeline_name(self) -> str:
        return "bls_pipeline"

    def get_resource_name(self) -> str:
        return "bls_observations"

    def _year_windows(self) -> list[tuple[int, int]]:
        """
        Split the requested year range into MAX_YEARS_PER_REQUEST windows.

        Example: 1990–2024 → [(1990, 2009), (2010, 2024)]
        """
        windows = []
        start = self.start_year
        while start <= self.end_year:
            end = min(start + MAX_YEARS_PER_REQUEST - 1, self.end_year)
            windows.append((start, end))
            start = end + 1
        return windows

    def _fetch_all_series(self) -> list[dict]:
        """
        Fetch all configured series across all year windows.

        BLS allows up to 50 series per request, so all 8 configured series
        can be fetched in a single call per year window.
        """
        all_series_data: dict[str, list] = {sid: [] for sid in self.series_ids}
        windows = self._year_windows()

        for start_yr, end_yr in windows:
            logger.info(f"Fetching BLS series for years {start_yr}–{end_yr}")
            try:
                series_list = self.client.fetch_series(
                    series_ids=self.series_ids,
                    start_year=start_yr,
                    end_year=end_yr,
                )
                for series in series_list:
                    sid = series.get("seriesID", "")
                    data_points = series.get("data", [])
                    logger.info(f"  {sid}: {len(data_points)} observations")
                    if sid in all_series_data:
                        all_series_data[sid].extend(data_points)

                # Archive raw response to S3
                if self.archive_to_s3 and self.s3:
                    self.s3.archive(
                        data={"series": series_list, "start_year": start_yr, "end_year": end_yr},
                        source="bls",
                        indicator=f"bulk_{start_yr}_{end_yr}",
                        partition_date=date.today(),
                    )

            except requests.HTTPError as e:
                logger.error(f"HTTP error fetching BLS batch {start_yr}–{end_yr}: {e}")
            except ValueError as e:
                logger.error(f"BLS API error for batch {start_yr}–{end_yr}: {e}")

        return all_series_data

    def extract(self) -> Generator[dict, None, None]:
        """Yield normalized observation records for all configured BLS series."""
        all_series_data = self._fetch_all_series()

        for series_id in self.series_ids:
            meta = BLS_SERIES.get(series_id, {})
            data_points = all_series_data.get(series_id, [])
            logger.info(
                f"Processing {series_id} — {meta.get('description', '')} "
                f"({len(data_points)} total observations)"
            )

            for point in data_points:
                year = point.get("year", "")
                period = point.get("period", "")
                period_name = point.get("periodName", "")

                # Skip annual averages (period M13 = annual avg in BLS)
                if period == "M13":
                    continue

                observation_date = parse_bls_period(year, period)
                if observation_date is None:
                    logger.warning(
                        f"Could not parse period '{period}' year '{year}' "
                        f"for series {series_id}"
                    )
                    continue

                raw_value = point.get("value", "")
                # BLS uses "-" for missing/suppressed values
                if raw_value in ("-", "", "N/A"):
                    value = None
                    is_missing = True
                else:
                    value = safe_float(raw_value)
                    is_missing = value is None

                # BLS footnote codes (e.g. "P" = preliminary, "R" = revised)
                footnotes = point.get("footnotes", [])
                footnote_codes = ",".join(
                    f.get("code", "") for f in footnotes if f.get("code")
                )

                yield {
                    # Primary key: series + date
                    "series_id":         series_id,
                    "observation_date":  normalize_date(observation_date),
                    # Value
                    "value":             value,
                    "is_missing":        is_missing,
                    # BLS period metadata
                    "year":              int(year) if year.isdigit() else None,
                    "period":            period,
                    "period_name":       period_name,
                    "footnote_codes":    footnote_codes or None,
                    # Series metadata
                    "description":       meta.get("description", ""),
                    "category":          meta.get("category", "unknown"),
                    "units":             meta.get("units", ""),
                    "frequency":         meta.get("frequency", infer_frequency_from_period(period)),
                    "seasonal_adjustment": meta.get("seasonal_adjustment", ""),
                    "survey":            meta.get("survey", ""),
                    "source":            "bls",
                    "series_url":        BLS_SERIES_URL_TEMPLATE.format(series_id=series_id),
                    # Audit
                    "ingested_at":       datetime.utcnow().isoformat(),
                    "pipeline_version":  "1.0.0",
                }


# ── CLI Entry Point ───────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run BLS labor market data pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python ingestion/bls_pipeline.py --destination postgres
  python ingestion/bls_pipeline.py --series CES0000000001,LNS14000000 --start 2010
  python ingestion/bls_pipeline.py --destination bigquery --no-s3
        """,
    )
    parser.add_argument(
        "--destination",
        choices=["postgres", "bigquery"],
        default="postgres",
        help="Target destination (default: postgres)",
    )
    parser.add_argument(
        "--series",
        type=str,
        default=None,
        help="Comma-separated BLS series IDs (default: all configured)",
    )
    parser.add_argument(
        "--start",
        type=int,
        default=2000,
        help="Start year (default: 2000; use 1990 for full history — slower)",
    )
    parser.add_argument(
        "--end",
        type=int,
        default=None,
        help="End year (default: current year)",
    )
    parser.add_argument(
        "--no-s3",
        action="store_true",
        help="Skip S3 archival of raw API responses",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    series = args.series.split(",") if args.series else None

    pipeline = BLSPipeline(
        destination=args.destination,
        series_ids=series,
        start_year=args.start,
        end_year=args.end,
        archive_to_s3=not args.no_s3,
    )

    result = pipeline.run()
    logger.info(f"Result: {result}")
