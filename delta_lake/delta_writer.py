"""
delta_lake/delta_writer.py
--------------------------
Writes ingested macroeconomic data to Delta Lake tables on AWS S3.

Delta Lake (open-sourced by Databricks) adds ACID transactions, schema
enforcement, time-travel, and upsert (MERGE) semantics on top of raw
Parquet files in S3 — giving the raw data lake the reliability of a
database without the cost of one.

Why Delta Lake here?
  - ACID upserts: economic data gets revised (BEA revises GDP quarterly).
    Delta's MERGE lets us upsert revised values without full reloads.
  - Time travel: `VERSION AS OF` lets us reconstruct the dataset as it
    existed on any past date — critical for avoiding look-ahead bias in
    ML backtests.
  - Schema evolution: new BEA/FRED series can be added without breaking
    existing readers.
  - Databricks-compatible: these same Delta tables can be mounted directly
    in a Databricks workspace via Unity Catalog with zero ETL.

Local dev (no Databricks account needed):
  pip install delta-spark pyspark
  The DeltaLakeWriter runs on local PySpark — same Delta protocol,
  same files, just without the Databricks managed runtime.

Production path:
  Point DELTA_S3_PATH to s3://your-bucket/delta/ and run on
  Databricks or EMR with the Delta Lake JAR on the classpath.

Usage:
    python delta_lake/delta_writer.py --table fred_observations
    python delta_lake/delta_writer.py --table all --mode overwrite
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime
from typing import Literal

from loguru import logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, StringType, BooleanType, TimestampType, StructType, StructField
)
from delta import configure_spark_with_delta_pip

# ── Config ────────────────────────────────────────────────────────────────────

DELTA_BASE_PATH  = os.getenv("DELTA_S3_PATH", "/tmp/delta_lake")   # swap for s3:// in prod
POSTGRES_URL     = (
    f"jdbc:postgresql://{os.getenv('POSTGRES_HOST','localhost')}:"
    f"{os.getenv('POSTGRES_PORT','5432')}/"
    f"{os.getenv('POSTGRES_DB','econ_warehouse')}"
)
POSTGRES_PROPS = {
    "user":     os.getenv("POSTGRES_USER", "econ_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "localdev"),
    "driver":   "org.postgresql.Driver",
}

# Table definitions: source Postgres table → Delta path → merge key(s)
TABLE_CONFIG = {
    "fred_observations": {
        "source_table": "raw.fred_observations",
        "delta_path":   f"{DELTA_BASE_PATH}/fred/observations",
        "merge_keys":   ["series_id", "observation_date"],
        "partition_by": ["category"],
        "description":  "FRED macroeconomic series observations",
    },
    "bea_nipa_observations": {
        "source_table": "raw.bea_nipa_observations",
        "delta_path":   f"{DELTA_BASE_PATH}/bea/nipa_observations",
        "merge_keys":   ["table_key", "series_name", "period_date"],
        "partition_by": ["frequency"],
        "description":  "BEA NIPA GDP, PCE, income, trade data",
    },
    "market_prices": {
        "source_table": "raw.market_prices",
        "delta_path":   f"{DELTA_BASE_PATH}/market/prices",
        "merge_keys":   ["ticker", "trade_date"],
        "partition_by": ["asset_class"],
        "description":  "Daily market OHLCV prices",
    },
    "macro_indicators_monthly": {
        "source_table": "marts.fct_macro_indicators_monthly",
        "delta_path":   f"{DELTA_BASE_PATH}/marts/macro_indicators_monthly",
        "merge_keys":   ["observation_month"],
        "partition_by": ["year"],
        "description":  "Wide ML feature table (gold layer)",
    },
}


# ── Spark Session ─────────────────────────────────────────────────────────────

def get_spark() -> SparkSession:
    """
    Build a local PySpark session with Delta Lake support.

    In production (Databricks/EMR), replace with SparkSession.builder
    .getOrCreate() — the cluster runtime already has Delta configured.
    """
    builder = (
        SparkSession.builder
        .appName("econ-delta-lake")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # S3 config (no-op locally, required for AWS)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        # Performance tuning
        .config("spark.sql.shuffle.partitions", "8")   # reduce for local dev
        .config("spark.driver.memory", "4g")
    )
    return configure_spark_with_delta_pip(
        builder, extra_packages=["org.postgresql:postgresql:42.6.0"]
    ).getOrCreate()


# ── Delta Writer ──────────────────────────────────────────────────────────────

class DeltaLakeWriter:
    """
    Reads from PostgreSQL and writes/upserts to Delta Lake tables.

    Supports two write modes:
      'merge'    — UPSERT on merge_keys. Handles data revisions correctly.
                   Use for incremental runs.
      'overwrite' — Full table replace. Use for backfills/schema changes.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_from_postgres(self, table: str) -> DataFrame:
        """Read a full table from PostgreSQL via JDBC."""
        logger.info(f"Reading from Postgres: {table}")
        df = (
            self.spark.read
            .format("jdbc")
            .option("url", POSTGRES_URL)
            .option("dbtable", table)
            .option("user", POSTGRES_PROPS["user"])
            .option("password", POSTGRES_PROPS["password"])
            .option("driver", POSTGRES_PROPS["driver"])
            .option("fetchsize", "10000")
            .load()
        )
        logger.info(f"  → {df.count():,} rows, {len(df.columns)} columns")
        return df

    def write_delta(
        self,
        df: DataFrame,
        delta_path: str,
        merge_keys: list[str],
        partition_by: list[str] | None = None,
        mode: Literal["merge", "overwrite"] = "merge",
    ) -> None:
        """Write DataFrame to a Delta table."""

        # Add ingestion audit column
        df = df.withColumn("delta_written_at", F.current_timestamp())

        if mode == "overwrite":
            logger.info(f"Overwriting Delta table at: {delta_path}")
            writer = df.write.format("delta").mode("overwrite")
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            writer.save(delta_path)
            logger.success(f"Overwrote {df.count():,} rows → {delta_path}")

        elif mode == "merge":
            logger.info(f"Merging into Delta table at: {delta_path}")
            from delta.tables import DeltaTable

            # Create table if it doesn't exist yet
            if not DeltaTable.isDeltaTable(self.spark, delta_path):
                logger.info("Delta table does not exist — creating via initial write")
                writer = df.write.format("delta").mode("overwrite")
                if partition_by:
                    writer = writer.partitionBy(*partition_by)
                writer.save(delta_path)
                logger.success(f"Created Delta table with {df.count():,} rows")
                return

            delta_table = DeltaTable.forPath(self.spark, delta_path)

            # Build merge condition from composite keys
            merge_condition = " AND ".join(
                f"target.{k} = source.{k}" for k in merge_keys
            )

            (
                delta_table.alias("target")
                .merge(df.alias("source"), merge_condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

            # Log operation metrics
            history = delta_table.history(1).select(
                "version", "timestamp", "operation", "operationMetrics"
            ).collect()[0]
            logger.success(
                f"Merge complete → version {history['version']} | "
                f"metrics: {history['operationMetrics']}"
            )

    def optimize(self, delta_path: str, z_order_cols: list[str] | None = None) -> None:
        """
        Run OPTIMIZE (compaction) + optional Z-ORDER on a Delta table.

        OPTIMIZE compacts small files → faster queries.
        Z-ORDER co-locates related data → skip more files on filtered reads.
        Run after bulk loads, not on every incremental update.
        """
        logger.info(f"Optimizing Delta table: {delta_path}")
        z_order_clause = ""
        if z_order_cols:
            cols = ", ".join(z_order_cols)
            z_order_clause = f"ZORDER BY ({cols})"

        self.spark.sql(f"""
            OPTIMIZE delta.`{delta_path}`
            {z_order_clause}
        """)
        logger.success(f"Optimized: {delta_path}")

    def show_history(self, delta_path: str, n: int = 5) -> None:
        """Print the last n operations on a Delta table (time travel audit log)."""
        from delta.tables import DeltaTable
        dt = DeltaTable.forPath(self.spark, delta_path)
        print(f"\n{'─'*60}")
        print(f"Delta History: {delta_path}")
        print(f"{'─'*60}")
        dt.history(n).select(
            "version", "timestamp", "operation", "operationMetrics"
        ).show(truncate=False)

    def time_travel_read(self, delta_path: str, version: int) -> DataFrame:
        """
        Read a Delta table AS OF a specific version.

        Critical for ML backtesting — ensures training data reflects
        only what was known at a given point in time (no look-ahead bias).

        Example:
            # Read the macro feature table as it existed in Jan 2023
            df = writer.time_travel_read(path, version=42)
        """
        logger.info(f"Time-travel read: {delta_path} @ version {version}")
        return (
            self.spark.read
            .format("delta")
            .option("versionAsOf", version)
            .load(delta_path)
        )

    def run_table(
        self,
        table_key: str,
        mode: Literal["merge", "overwrite"] = "merge",
    ) -> None:
        """Run the full read → write pipeline for one configured table."""
        cfg = TABLE_CONFIG[table_key]
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing: {table_key}")
        logger.info(f"  Source:      {cfg['source_table']}")
        logger.info(f"  Delta path:  {cfg['delta_path']}")
        logger.info(f"  Merge keys:  {cfg['merge_keys']}")
        logger.info(f"  Mode:        {mode}")

        df = self.read_from_postgres(cfg["source_table"])
        
        # Deduplicate incoming source data to prevent Delta MULTIPLE_SOURCE_ROW_MATCHING errors
        from pyspark.sql import Window
        from pyspark.sql.functions import row_number, col
        
        w = Window.partitionBy(*cfg["merge_keys"])
        if "ingested_at" in df.columns:
            w = w.orderBy(col("ingested_at").desc())
        else:
            w = w.orderBy(*[col(k) for k in cfg["merge_keys"]])
            
        df = df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")

        self.write_delta(
            df=df,
            delta_path=cfg["delta_path"],
            merge_keys=cfg["merge_keys"],
            partition_by=cfg.get("partition_by"),
            mode=mode,
        )


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="Write data to Delta Lake")
    parser.add_argument(
        "--table",
        choices=list(TABLE_CONFIG.keys()) + ["all"],
        default="all",
        help="Which table to write (default: all)",
    )
    parser.add_argument(
        "--mode",
        choices=["merge", "overwrite"],
        default="merge",
        help="Write mode: merge (upsert) or overwrite (full replace)",
    )
    parser.add_argument(
        "--history",
        action="store_true",
        help="Print Delta history for the specified table and exit",
    )
    parser.add_argument(
        "--optimize",
        action="store_true",
        help="Run OPTIMIZE after writing",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark()
    writer = DeltaLakeWriter(spark)

    tables = list(TABLE_CONFIG.keys()) if args.table == "all" else [args.table]

    for table_key in tables:
        if args.history:
            cfg = TABLE_CONFIG[table_key]
            writer.show_history(cfg["delta_path"])
            continue

        writer.run_table(table_key, mode=args.mode)

        if args.optimize:
            cfg = TABLE_CONFIG[table_key]
            z_cols = cfg["merge_keys"][:2]   # Z-order on primary keys
            writer.optimize(cfg["delta_path"], z_order_cols=z_cols)

    spark.stop()
    logger.success("Delta Lake write complete.")
