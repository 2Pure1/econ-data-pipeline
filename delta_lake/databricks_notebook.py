# Databricks notebook source
# ============================================================
# Econ Data Pipeline — Databricks Notebook
# Macro Indicator Analysis & Delta Lake Exploration
#
# This notebook demonstrates how the Delta Lake tables built
# by delta_writer.py can be consumed directly in Databricks.
#
# To run on Databricks:
#   1. Mount your S3 bucket via dbutils.fs.mount() or Unity Catalog
#   2. Set DELTA_BASE_PATH to your mounted path
#   3. Attach to any Databricks cluster (DBR 13.0+ recommended)
#
# Locally with Databricks Community Edition (free):
#   Upload this notebook and run against a community cluster.
# ============================================================

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Setup & Configuration

# COMMAND ----------

import os
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import pandas as pd

# Path to Delta tables — update for your environment
DELTA_BASE_PATH = os.getenv("DELTA_S3_PATH", "/tmp/delta_lake")

FRED_PATH    = f"{DELTA_BASE_PATH}/fred/observations"
BEA_PATH     = f"{DELTA_BASE_PATH}/bea/nipa_observations"
MARKET_PATH  = f"{DELTA_BASE_PATH}/market/prices"
MART_PATH    = f"{DELTA_BASE_PATH}/marts/macro_indicators_monthly"

print(f"Delta base path: {DELTA_BASE_PATH}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Register Delta Tables in the Spark Catalog

# COMMAND ----------

# Register as temp views for SQL querying
spark.read.format("delta").load(MART_PATH).createOrReplaceTempView("macro_monthly")
spark.read.format("delta").load(FRED_PATH).createOrReplaceTempView("fred_obs")
spark.read.format("delta").load(MARKET_PATH).createOrReplaceTempView("market_prices")

print("Tables registered.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Exploratory Analysis with Spark SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recession periods: yield curve inversion + rising unemployment
# MAGIC SELECT
# MAGIC     observation_month,
# MAGIC     year,
# MAGIC     real_gdp_qoq_pct,
# MAGIC     unemployment_rate,
# MAGIC     fed_funds_rate,
# MAGIC     yield_curve_spread_10y2y,
# MAGIC     yield_curve_inverted,
# MAGIC     cpi_yoy_pct,
# MAGIC     vix_avg
# MAGIC FROM macro_monthly
# MAGIC WHERE yield_curve_inverted = true
# MAGIC   AND observation_month >= '2000-01-01'
# MAGIC ORDER BY observation_month

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PCE composition shift over time (services vs goods)
# MAGIC SELECT
# MAGIC     year,
# MAGIC     ROUND(AVG(pce_services_goods_ratio), 3) AS avg_services_goods_ratio,
# MAGIC     ROUND(AVG(pce_healthcare_billions / pce_total_monthly_billions * 100), 2) AS healthcare_pct_of_pce,
# MAGIC     ROUND(AVG(personal_saving_rate_pct), 2) AS avg_saving_rate
# MAGIC FROM macro_monthly
# MAGIC WHERE pce_total_monthly_billions IS NOT NULL
# MAGIC GROUP BY year
# MAGIC ORDER BY year

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Feature Correlation Analysis

# COMMAND ----------

# Pull the mart into Pandas for correlation analysis
df = spark.read.format("delta").load(MART_PATH).toPandas()
df = df[df["is_complete_record"] == True].copy()

feature_cols = [
    "real_gdp_qoq_pct",
    "cpi_yoy_pct",
    "core_cpi_yoy_pct",
    "fed_funds_rate",
    "real_fed_funds_rate",
    "unemployment_rate",
    "yield_curve_spread_10y2y",
    "personal_saving_rate_pct",
    "pce_services_goods_ratio",
    "vix_avg",
    "sp500_realised_vol",
    "wti_oil_avg",
]

corr_matrix = df[feature_cols].corr()

# Heatmap
fig, ax = plt.subplots(figsize=(12, 10))
im = ax.imshow(corr_matrix, cmap="RdBu_r", vmin=-1, vmax=1)
plt.colorbar(im)
ax.set_xticks(range(len(feature_cols)))
ax.set_yticks(range(len(feature_cols)))
ax.set_xticklabels(feature_cols, rotation=45, ha="right", fontsize=9)
ax.set_yticklabels(feature_cols, fontsize=9)
ax.set_title("Macro Feature Correlation Matrix", fontsize=14, fontweight="bold")
plt.tight_layout()
plt.savefig("/tmp/correlation_matrix.png", dpi=150, bbox_inches="tight")
plt.show()
print("Saved: /tmp/correlation_matrix.png")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Delta Lake Time Travel — Backtest Data Integrity

# COMMAND ----------

# Show Delta table history (every revision is tracked)
from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, MART_PATH)
display(dt.history(10).select("version", "timestamp", "operation", "operationMetrics"))

# COMMAND ----------

# Read the table AS OF version 0 (initial load)
# This is how you guarantee no look-ahead bias in ML backtests
df_v0 = (
    spark.read
    .format("delta")
    .option("versionAsOf", 0)
    .load(MART_PATH)
)

print(f"Current table: {spark.read.format('delta').load(MART_PATH).count():,} rows")
print(f"Version 0:     {df_v0.count():,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Streaming Readiness Check

# COMMAND ----------

# Delta Lake supports streaming reads — any new rows written to the
# Delta table will be picked up automatically by a streaming query.
# This is how econ-streaming-pipeline will consume market data updates.

streaming_df = (
    spark.readStream
    .format("delta")
    .load(MARKET_PATH)
    .filter(F.col("ticker") == "^GSPC")
    .select("ticker", "trade_date", "close_price", "daily_return")
)

print("Streaming schema:")
streaming_df.printSchema()
print("\nStreaming source is Delta Lake — all ACID guarantees apply.")
print("Connect econ-streaming-pipeline here for real-time model scoring.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Export Feature Table for ML Training

# COMMAND ----------

# Write the clean, complete feature set to a CSV for econ-forecast-engine
ml_features = (
    spark.read.format("delta").load(MART_PATH)
    .filter(F.col("is_complete_record") == True)
    .filter(F.col("observation_month") >= "1994-01-01")  # enough history for lags
    .orderBy("observation_month")
)

print(f"ML-ready rows: {ml_features.count():,}")
print(f"Features:      {len(ml_features.columns)}")

# Save as CSV for downstream use
(
    ml_features
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv("/tmp/macro_features_ml_ready")
)

print("\nExported to /tmp/macro_features_ml_ready/")
print("Feed this into econ-forecast-engine for model training.")
