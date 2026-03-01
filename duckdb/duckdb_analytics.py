"""
duckdb/duckdb_analytics.py
---------------------------
DuckDB as a fast local analytics layer on top of the Delta Lake tables.

Why DuckDB here?
  - Runs entirely in-process (no server needed) — instant startup
  - Reads Delta Lake / Parquet files directly from disk or S3
  - Executes analytical SQL 10-100x faster than Postgres for column scans
  - Replaces Postgres as the dbt development target (see profiles.yml)
  - Standard in modern data stacks: Motherduck, MotherDuck Cloud,
    and most new "lakehouse" tooling is built on DuckDB

This module provides:
  1. DuckDBAnalytics — query Delta/Parquet files directly, no ETL needed
  2. CLI — run any SQL query against the local data lake
  3. dbt profile snippet — drop-in replacement for Postgres in dev

Usage:
    # Run a query
    python duckdb/duckdb_analytics.py --query "SELECT * FROM macro_monthly LIMIT 10"

    # Export a table to CSV
    python duckdb/duckdb_analytics.py --export macro_monthly --output /tmp/macro.csv

    # Benchmark vs Postgres
    python duckdb/duckdb_analytics.py --benchmark
"""

from __future__ import annotations

import argparse
import os
import time
from pathlib import Path

import duckdb
import pandas as pd
from loguru import logger

DELTA_BASE_PATH = os.getenv("DELTA_S3_PATH", "/tmp/delta_lake")
DUCKDB_PATH     = os.getenv("DUCKDB_PATH", "/tmp/econ_analytics.duckdb")

# Table paths (Delta Lake = Parquet files under the hood)
TABLES = {
    "fred_obs":      f"{DELTA_BASE_PATH}/fred/observations",
    "bea_obs":       f"{DELTA_BASE_PATH}/bea/nipa_observations",
    "market_prices": f"{DELTA_BASE_PATH}/market/prices",
    "macro_monthly": f"{DELTA_BASE_PATH}/marts/macro_indicators_monthly",
}


class DuckDBAnalytics:
    """
    Fast local analytics on Delta Lake tables using DuckDB.

    DuckDB reads the Parquet files that Delta Lake manages — it doesn't
    need to know about Delta specifically, just the Parquet layer.
    For full Delta support (time travel, schema evolution), use the
    delta-rs Python library alongside DuckDB.
    """

    def __init__(self, db_path: str = DUCKDB_PATH):
        self.conn = duckdb.connect(db_path)
        
        # Enable S3 support
        if DELTA_BASE_PATH.startswith("s3"):
            self.conn.execute("INSTALL httpfs;")
            self.conn.execute("LOAD httpfs;")
            # Replaces s3a:// with s3:// for DuckDB globbing
            self.conn.execute("SET s3_endpoint='s3.us-east-1.amazonaws.com';")
            self.conn.execute(f"SET s3_access_key_id='{os.getenv('AWS_ACCESS_KEY_ID')}';")
            self.conn.execute(f"SET s3_secret_access_key='{os.getenv('AWS_SECRET_ACCESS_KEY')}';")
            self.conn.execute(f"SET s3_region='{os.getenv('AWS_DEFAULT_REGION', 'us-east-1')}';")
        self._register_tables()
        logger.info(f"DuckDB connected: {db_path}")

    def _register_tables(self) -> None:
        """Register Delta Lake Parquet directories as DuckDB views."""
        for name, path in TABLES.items():
            # DuckDB httpfs expects s3:// instead of Spark's s3a://
            duckdb_path = path.replace("s3a://", "s3://")
            parquet_glob = f"{duckdb_path}/**/*.parquet"
            if Path(path).exists() or path.startswith("s3://") or path.startswith("s3a://"):
                try:
                    self.conn.execute(f"""
                        CREATE OR REPLACE VIEW {name} AS
                        SELECT * FROM read_parquet('{parquet_glob}', hive_partitioning=true)
                    """)
                    logger.debug(f"Registered view: {name} → {parquet_glob}")
                except Exception as e:
                    logger.warning(f"Could not register view {name}: {e}")
            else:
                logger.warning(f"Path not found, skipping view: {name} ({path})")

    def query(self, sql: str) -> pd.DataFrame:
        """Execute SQL and return a Pandas DataFrame."""
        start = time.perf_counter()
        result = self.conn.execute(sql).df()
        elapsed = time.perf_counter() - start
        logger.info(f"Query returned {len(result):,} rows in {elapsed:.3f}s")
        return result

    def export_csv(self, table: str, output_path: str) -> None:
        """Export a full table to CSV."""
        logger.info(f"Exporting {table} → {output_path}")
        self.conn.execute(f"""
            COPY (SELECT * FROM {table})
            TO '{output_path}' (HEADER, DELIMITER ',')
        """)
        logger.success(f"Exported: {output_path}")

    def benchmark_vs_postgres(self) -> None:
        """
        Run the same analytical query against DuckDB and Postgres,
        printing a side-by-side timing comparison.
        """
        import psycopg2

        sql = """
            SELECT
                year,
                ROUND(AVG(unemployment_rate), 3)              AS avg_unemployment,
                ROUND(AVG(cpi_yoy_pct), 3)                    AS avg_cpi_yoy,
                ROUND(AVG(fed_funds_rate), 3)                  AS avg_fed_funds,
                ROUND(AVG(core_pce_yoy_pct), 3)               AS avg_core_pce,
                ROUND(AVG(nonfarm_payrolls_mom_change), 0)     AS avg_payrolls_mom,
                COUNT(*)                                       AS months
            FROM macro_monthly
            WHERE is_complete_record = true
            GROUP BY year
            ORDER BY year
        """

        # DuckDB
        t0 = time.perf_counter()
        duck_result = self.query(sql)
        duck_time = time.perf_counter() - t0

        # Postgres
        pg_sql = sql.replace("macro_monthly", "marts.fct_macro_indicators_monthly")
        try:
            conn_str = (
                f"host={os.getenv('POSTGRES_HOST','localhost')} "
                f"port={os.getenv('POSTGRES_PORT','5432')} "
                f"dbname={os.getenv('POSTGRES_DB','econ_warehouse')} "
                f"user={os.getenv('POSTGRES_USER','econ_user')} "
                f"password={os.getenv('POSTGRES_PASSWORD','')}"
            )
            with psycopg2.connect(conn_str) as pg_conn:
                t0 = time.perf_counter()
                pg_result = pd.read_sql(pg_sql, pg_conn)
                pg_time = time.perf_counter() - t0
        except Exception as e:
            pg_time = None
            logger.warning(f"Postgres benchmark failed: {e}")

        print(f"\n{'─'*50}")
        print(f"Benchmark: Annual macro aggregation ({duck_result.shape[0]} years)")
        print(f"{'─'*50}")
        print(f"DuckDB:    {duck_time*1000:.1f}ms")
        if pg_time:
            print(f"PostgreSQL: {pg_time*1000:.1f}ms")
            speedup = pg_time / duck_time
            print(f"Speedup:    {speedup:.1f}x  {'🦆' * min(int(speedup), 10)}")
        print(f"{'─'*50}\n")
        print(duck_result.to_string(index=False))

    def recession_analysis(self) -> pd.DataFrame:
        """
        Identify recession-adjacent periods using classic indicators:
        rising unemployment, falling real GDP, and elevated CPI.
        Note: yield curve / VIX columns are not in the current mart schema.
        """
        sql = """
            WITH signals AS (
                SELECT
                    observation_month,
                    year,
                    real_gdp_billions,
                    real_gdp_billions - LAG(real_gdp_billions, 1)
                        OVER (ORDER BY observation_month)          AS gdp_qoq_change,
                    unemployment_rate,
                    unemployment_rate - LAG(unemployment_rate, 3)
                        OVER (ORDER BY observation_month)          AS unemployment_3m_change,
                    cpi_yoy_pct,
                    core_pce_yoy_pct,
                    fed_funds_rate,
                    nonfarm_payrolls_mom_change
                FROM macro_monthly
                WHERE is_complete_record = true
            )
            SELECT *,
                CASE
                    WHEN unemployment_3m_change > 0.5
                     AND gdp_qoq_change < 0
                    THEN true ELSE false
                END AS recession_risk_flag
            FROM signals
            WHERE unemployment_3m_change > 0.5
               OR gdp_qoq_change < 0
            ORDER BY observation_month
        """
        return self.query(sql)

    def close(self) -> None:
        self.conn.close()


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="DuckDB analytics on Delta Lake")
    parser.add_argument("--query",     type=str, help="Run a SQL query")
    parser.add_argument("--export",    type=str, help="Table name to export")
    parser.add_argument("--output",    type=str, default="/tmp/export.csv")
    parser.add_argument("--benchmark", action="store_true")
    parser.add_argument("--recession", action="store_true", help="Run recession analysis")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    analytics = DuckDBAnalytics()

    if args.query:
        df = analytics.query(args.query)
        print(df.to_string(index=False))

    elif args.export:
        analytics.export_csv(args.export, args.output)

    elif args.benchmark:
        analytics.benchmark_vs_postgres()

    elif args.recession:
        df = analytics.recession_analysis()
        print(df.to_string(index=False))

    else:
        print("DuckDB Analytics ready. Use --query, --export, --benchmark, or --recession.")
        print(f"Registered tables: {list(TABLES.keys())}")

    analytics.close()
