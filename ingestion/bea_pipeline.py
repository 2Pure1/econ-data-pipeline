"""
bea_pipeline.py
---------------
Ingests macroeconomic data directly from the Bureau of Economic Analysis (BEA) API.
BEA is the PRIMARY source for GDP, PCE, national income, and trade data.

Why BEA over FRED for these series?
  - BEA releases data first; FRED republishes it with a lag
  - BEA provides more granular breakdowns (GDP by component, PCE by category)
  - Direct access demonstrates understanding of the data lineage

Datasets covered:
  NIPA  — National Income and Product Accounts (GDP, PCE, GDI, savings rate)
  MNE   — Multinational Enterprise data
  ITA   — International Transactions (trade balance, current account)
  GDPbyIndustry — Value added, gross output by industry (sectoral analysis)

API Docs: https://apps.bea.gov/api/signup/

Usage:
    python ingestion/bea_pipeline.py --destination postgres
    python ingestion/bea_pipeline.py --dataset NIPA --start 2000 --end 2024
    python ingestion/bea_pipeline.py --list-tables  # show all available NIPA tables
"""

from __future__ import annotations

import argparse
import os
from datetime import date, datetime, UTC
from typing import Generator

import requests
from loguru import logger

from ingestion.base_pipeline import BasePipeline, cfg, normalize_date, safe_float

BEA_BASE_URL = "https://apps.bea.gov/api/data"
BEA_API_KEY  = os.getenv("BEA_API_KEY", "")


# ── BEA NIPA Table Definitions ────────────────────────────────────────────────
# Each entry maps a human-readable name → BEA TableName + key series LineNumbers.
# Line numbers come from BEA's NIPA Handbook; pinning them ensures stable pulls.

NIPA_TABLES = {

    # ── GDP & Components (Table 1.1.5) ───────────────────────────────────────
    "gdp_and_components": {
        "table_name": "T10105",         # GDP and Components of GDP, Levels
        "description": "GDP and major demand-side components (quarterly, $B SAAR)",
        "frequency": "Q",
        "lines": {
            1:  "gdp",
            2:  "personal_consumption_expenditures",
            6:  "gross_private_domestic_investment",
            11: "net_exports_goods_services",
            12: "exports",
            16: "imports",
            19: "government_consumption_investment",
            20: "federal_government",
            25: "state_local_government",
        },
    },

    # ── Real GDP Growth (Table 1.1.1) ────────────────────────────────────────
    "real_gdp_growth": {
        "table_name": "T10101",         # % change from preceding period, SAAR
        "description": "Real GDP percent change from preceding period (quarterly)",
        "frequency": "Q",
        "lines": {
            1: "real_gdp_pct_change",
            2: "real_pce_pct_change",
            6: "real_gross_private_investment_pct_change",
            11: "real_net_exports_pct_change",
            19: "real_government_pct_change",
        },
    },

    # ── GDP Price Indexes (Table 1.1.4) ──────────────────────────────────────
    "gdp_price_indexes": {
        "table_name": "T10104",         # Price indexes for GDP components
        "description": "GDP chain-type price indexes (quarterly, 2017=100)",
        "frequency": "Q",
        "lines": {
            1: "gdp_price_index",
            2: "pce_price_index",
            9: "gross_private_investment_price_index",
            13: "exports_price_index",
            17: "imports_price_index",
        },
    },

    # ── PCE by Major Category (Table 2.8.5) ──────────────────────────────────
    "pce_by_category": {
        "table_name": "T20805",         # PCE by major type, levels
        "description": "Personal Consumption Expenditures by category ($B SAAR)",
        "frequency": "M",              # Monthly — more granular than FRED
        "lines": {
            1:  "pce_total",
            2:  "pce_goods",
            3:  "pce_durable_goods",
            6:  "pce_nondurable_goods",
            10: "pce_services",
            11: "pce_housing_utilities",
            12: "pce_healthcare",
            17: "pce_financial_services",
            20: "pce_recreation",
            24: "pce_food_beverages",
            26: "pce_transportation",
        },
    },

    # ── Personal Income & Saving (Table 2.1) ─────────────────────────────────
    "personal_income_saving": {
        "table_name": "T20600",         # Personal income, outlays, saving
        "description": "Personal income, disposable income, and saving",
        "frequency": "M",
        "lines": {
            1:  "personal_income",
            2:  "compensation_of_employees",
            3:  "wages_salaries",
            12: "proprietors_income",
            16: "rental_income",
            17: "personal_income_receipts_assets",
            20: "transfer_payments",
            27: "personal_outlays",
            28: "personal_consumption_expenditures",
            34: "personal_saving",
            35: "personal_saving_rate",
        },
    },

    # ── Corporate Profits (Table 6.16) ───────────────────────────────────────
    "corporate_profits": {
        "table_name": "T61600D",        # Corporate profits by industry
        "description": "Corporate profits by industry",
        "frequency": "Q",
        "lines": {
            1: "corporate_profits_total",
            2: "domestic_industries_profits",
            3: "financial_sector_profits",
            13: "nonfinancial_sector_profits",
            30: "rest_of_world_profits",
        },
    },

    # ── International Trade (ITA Table 1) ────────────────────────────────────
    "international_transactions": {
        "table_name": "T40100",
        "description": "Current account: trade, income, transfers (quarterly, $B)",
        "frequency": "Q",
        "lines": {
            1:  "current_account_balance",
            2:  "goods_exports",
            3:  "goods_imports",
            5:  "services_exports",
            6:  "services_imports",
            17: "primary_income_receipts",
            18: "primary_income_payments",
        },
    },

    # ── Fixed Assets (Table 5.1) ──────────────────────────────────────────────
    "fixed_assets": {
        "table_name": "T50100",
        "description": "Net stock of fixed assets (annual)",
        "frequency": "A",
        "lines": {
            1: "net_stock_fixed_assets_total",
            2: "private_fixed_assets",
            3: "residential_fixed_assets",
            4: "nonresidential_fixed_assets",
            31: "government_fixed_assets",
        },
    },
}


# ── BEA API Client ────────────────────────────────────────────────────────────

class BEAClient:
    """Thin client for the BEA Data API v2."""

    def __init__(self, api_key: str = BEA_API_KEY):
        if not api_key:
            raise ValueError(
                "BEA_API_KEY not set in environment. "
                "Get a free key at https://apps.bea.gov/api/signup/"
            )
        self.api_key = api_key
        self.session = requests.Session()

    def get_nipa(
        self,
        table_name: str,
        frequency: str,
        year: str = "ALL",
    ) -> dict:
        """
        Fetch a NIPA table from BEA.

        Args:
            table_name: BEA table ID (e.g. 'T10105')
            frequency:  'A' (annual), 'Q' (quarterly), 'M' (monthly)
            year:       Comma-separated years, or 'ALL'
        """
        params = {
            "UserID":      self.api_key,
            "method":      "GetData",
            "DataSetName": "NIPA",
            "TableName":   table_name,
            "Frequency":   frequency,
            "Year":        year,
            "ResultFormat": "JSON",
        }

        response = self.session.get(BEA_BASE_URL, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        # BEA wraps errors inside the JSON body (not HTTP status)
        if "BEAAPI" not in data:
            raise ValueError(f"Unexpected BEA response structure: {data}")

        results = data["BEAAPI"].get("Results", {})
        if "Error" in results:
            raise ValueError(f"BEA API error for {table_name}: {results['Error']}")

        return results

    def list_nipa_tables(self) -> list[dict]:
        """Return all available NIPA table names and descriptions."""
        params = {
            "UserID":      self.api_key,
            "method":      "GetParameterValues",
            "DataSetName": "NIPA",
            "ParameterName": "TableName",
            "ResultFormat": "JSON",
        }
        resp = self.session.get(BEA_BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data["BEAAPI"]["Results"]["ParamValue"]


# ── Parsing Utilities ─────────────────────────────────────────────────────────

def parse_bea_period(period_str: str, frequency: str) -> date | None:
    """
    Convert BEA period strings to ISO dates.

    BEA formats:
      Annual:    '2023'         → date(2023, 1, 1)
      Quarterly: '2023Q1'       → date(2023, 1, 1)
      Monthly:   '2023M01'      → date(2023, 1, 1)
    """
    try:
        if frequency == "A":
            return date(int(period_str), 1, 1)

        elif frequency == "Q":
            year, q = int(period_str[:4]), period_str[5]
            month = {"1": 1, "2": 4, "3": 7, "4": 10}[q]
            return date(year, month, 1)

        elif frequency == "M":
            year  = int(period_str[:4])
            month = int(period_str[5:7])
            return date(year, month, 1)

    except Exception:
        pass
    return None


# ── Pipeline ──────────────────────────────────────────────────────────────────

class BEAPipeline(BasePipeline):
    """
    Ingests BEA NIPA data into PostgreSQL or BigQuery via dlt.

    Each record represents one data point:
      {table_key, series_name, period_date, value, frequency, ...metadata}

    The output is intentionally in a normalized (long) format so that
    dbt can pivot it into wide feature tables for ML.
    """

    def __init__(
        self,
        destination: str = "postgres",
        tables: list[str] | None = None,
        start_year: int = 1990,
        end_year: int | None = None,
        **kwargs,
    ):
        super().__init__(destination=destination, write_disposition="merge", **kwargs)
        self.tables    = tables or list(NIPA_TABLES.keys())
        self.start_year = start_year
        self.end_year   = end_year or date.today().year
        self.client     = BEAClient()

    def get_pipeline_name(self) -> str:
        return "bea_pipeline"

    def get_resource_name(self) -> str:
        return "bea_nipa_observations"

    def _year_range_str(self) -> str:
        """Build comma-separated year string for BEA API (e.g. '2020,2021,2022')."""
        return ",".join(str(y) for y in range(self.start_year, self.end_year + 1))

    def _fetch_table(self, table_key: str, table_cfg: dict) -> list[dict]:
        """Fetch one NIPA table and return raw BEA data records."""
        logger.info(
            f"Fetching BEA table: {table_key} "
            f"({table_cfg['table_name']}, {table_cfg['frequency']})"
        )

        year_param = "X" if table_cfg["frequency"] == "M" else self._year_range_str()

        results = self.client.get_nipa(
            table_name=table_cfg["table_name"],
            frequency=table_cfg["frequency"],
            year=year_param,
        )

        raw_data = results.get("Data", [])
        logger.info(f"  → {len(raw_data)} raw records")

        # Archive to S3
        if self.archive_to_s3 and self.s3:
            self.s3.archive(
                data={"table_key": table_key, "data": raw_data},
                source="bea",
                indicator=table_key,
                partition_date=date.today(),
            )

        return raw_data

    def extract(self) -> Generator[dict, None, None]:
        """
        Yield normalized records for all configured NIPA tables.

        BEA returns ALL line numbers for a table; we filter to only the
        lines defined in NIPA_TABLES to keep the dataset focused.
        """
        for table_key in self.tables:
            table_cfg = NIPA_TABLES[table_key]
            line_map  = table_cfg.get("lines", {})  # {line_num: series_name}

            try:
                raw_data = self._fetch_table(table_key, table_cfg)

                for record in raw_data:
                    line_num = int(record.get("LineNumber", -1))

                    # Skip lines we haven't explicitly mapped
                    if line_num not in line_map:
                        continue

                    series_name = line_map[line_num]
                    period_str  = record.get("TimePeriod", "")
                    frequency   = table_cfg["frequency"]

                    period_date = parse_bea_period(period_str, frequency)
                    if period_date is None:
                        logger.warning(f"Could not parse period '{period_str}' for {table_key}")
                        continue

                    raw_value = record.get("DataValue", "")
                    # BEA uses empty string or "(D)" for suppressed/missing values
                    if raw_value in ("", "(D)", "(NA)", "..."):
                        value = None
                    else:
                        # BEA values often have commas: "27,360.4"
                        value = safe_float(raw_value.replace(",", ""))

                    yield {
                        # Primary key
                        "table_key":    table_key,
                        "series_name":  series_name,
                        "period_date":  period_date,
                        # Value
                        "value":        value,
                        "is_missing":   value is None,
                        # BEA metadata
                        "bea_table_name":  table_cfg["table_name"],
                        "bea_line_number": line_num,
                        "bea_line_description": record.get("LineDescription", ""),
                        "bea_period_str":  period_str,
                        "frequency":       frequency,
                        "units":           record.get("METRIC_NAME", ""),
                        "description":     table_cfg["description"],
                        "source":          "bea",
                        # Audit
                        "ingested_at":     datetime.now(UTC).isoformat(),
                        "pipeline_version": "1.0.0",
                    }

            except Exception as e:
                logger.error(f"Error fetching BEA table {table_key}: {e}")
                raise


# ── CLI Entry Point ───────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="Run BEA NIPA data pipeline")
    parser.add_argument(
        "--destination",
        choices=["postgres", "bigquery"],
        default="postgres",
    )
    parser.add_argument(
        "--dataset",
        choices=list(NIPA_TABLES.keys()),
        nargs="+",
        default=None,
        help="Which NIPA table(s) to fetch (default: all)",
    )
    parser.add_argument(
        "--start",
        type=int,
        default=1990,
        help="Start year (default: 1990)",
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
        help="Skip S3 archival",
    )
    parser.add_argument(
        "--list-tables",
        action="store_true",
        help="List all available BEA NIPA tables and exit",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.list_tables:
        client = BEAClient()
        tables = client.list_nipa_tables()
        for t in tables:
            print(f"{t.get('TableName', ''):12s}  {t.get('Description', '')}")
        raise SystemExit(0)

    pipeline = BEAPipeline(
        destination=args.destination,
        tables=args.dataset,
        start_year=args.start,
        end_year=args.end,
        archive_to_s3=not args.no_s3,
    )

    result = pipeline.run()
    logger.info(f"Result: {result}")
