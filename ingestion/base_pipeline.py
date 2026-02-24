"""
base_pipeline.py
----------------
Shared utilities and base class for all econ data pipelines.
Handles: config loading, S3 archival, logging, retry logic.
"""

from __future__ import annotations

import os
import json
import hashlib
from datetime import datetime, date
from pathlib import Path
from typing import Any, Generator, Optional

import boto3
import dlt
import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

load_dotenv()


# ── Config ────────────────────────────────────────────────────────────────────

class PipelineConfig:
    """Centralised config loaded from environment variables."""

    # PostgreSQL
    PG_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    PG_PORT: int = int(os.getenv("POSTGRES_PORT", 5432))
    PG_DB: str = os.getenv("POSTGRES_DB", "econ_warehouse")
    PG_USER: str = os.getenv("POSTGRES_USER", "econ_user")
    PG_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "")
    PG_SCHEMA: str = os.getenv("POSTGRES_SCHEMA", "raw")

    # BigQuery
    GCP_PROJECT: str = os.getenv("GCP_PROJECT_ID", "")
    BQ_DATASET: str = os.getenv("BIGQUERY_DATASET", "econ_warehouse")

    # AWS S3
    S3_RAW_BUCKET: str = os.getenv("S3_BUCKET_RAW", "econ-pipeline-raw-data")
    S3_PROCESSED_BUCKET: str = os.getenv("S3_BUCKET_PROCESSED", "econ-pipeline-processed")
    AWS_REGION: str = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    # API keys
    FRED_API_KEY: str = os.getenv("FRED_API_KEY", "")
    BLS_API_KEY: str = os.getenv("BLS_API_KEY", "")


cfg = PipelineConfig()


# ── S3 Utilities ─────────────────────────────────────────────────────────────

class S3Archiver:
    """Archives raw API responses to S3 for auditability and reprocessing."""

    def __init__(self, bucket: str = cfg.S3_RAW_BUCKET):
        self.bucket = bucket
        self.client = boto3.client("s3", region_name=cfg.AWS_REGION)

    def archive(
        self,
        data: Any,
        source: str,
        indicator: str,
        partition_date: Optional[date] = None,
    ) -> str:
        """
        Upload raw data to S3 with a partitioned key.

        Key format: raw/{source}/{indicator}/year={Y}/month={M}/day={D}/{hash}.json
        Returns the S3 key.
        """
        dt = partition_date or date.today()
        payload = json.dumps(data, default=str)
        content_hash = hashlib.md5(payload.encode()).hexdigest()[:8]

        key = (
            f"raw/{source}/{indicator}/"
            f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
            f"{content_hash}.json"
        )

        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=payload,
            ContentType="application/json",
            Metadata={
                "source": source,
                "indicator": indicator,
                "ingested_at": datetime.utcnow().isoformat(),
            },
        )
        logger.info(f"Archived to s3://{self.bucket}/{key}")
        return key

    def archive_dataframe(self, df: pd.DataFrame, source: str, indicator: str) -> str:
        """Archive a DataFrame as Parquet to S3."""
        import io
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)

        dt = date.today()
        key = (
            f"processed/{source}/{indicator}/"
            f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
            f"data.parquet"
        )

        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )
        logger.info(f"Archived DataFrame to s3://{self.bucket}/{key}")
        return key


# ── Base Pipeline ─────────────────────────────────────────────────────────────

class BasePipeline:
    """
    Base class for all economic data pipelines.

    Subclasses implement:
        - `extract()` → yields records
        - `get_pipeline_name()` → str
        - `get_resource_name()` → str
    """

    def __init__(
        self,
        destination: str = "postgres",
        write_disposition: str = "merge",
        archive_to_s3: bool = True,
    ):
        self.destination = destination
        self.write_disposition = write_disposition
        self.archive_to_s3 = archive_to_s3
        self.s3 = S3Archiver() if archive_to_s3 else None
        self.config = cfg

        # Configure dlt destination
        if destination == "postgres":
            self._dlt_destination = dlt.destinations.postgres(
                f"postgresql://{cfg.PG_USER}:{cfg.PG_PASSWORD}"
                f"@{cfg.PG_HOST}:{cfg.PG_PORT}/{cfg.PG_DB}"
            )
        elif destination == "bigquery":
            self._dlt_destination = dlt.destinations.bigquery(
                project=cfg.GCP_PROJECT
            )
        else:
            raise ValueError(f"Unsupported destination: {destination}")

    def get_pipeline_name(self) -> str:
        raise NotImplementedError

    def get_resource_name(self) -> str:
        raise NotImplementedError

    def extract(self) -> Generator[dict, None, None]:
        raise NotImplementedError

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=30),
        reraise=True,
    )
    def run(self) -> dict:
        """Execute the full pipeline: extract → archive → load."""
        logger.info(f"Starting pipeline: {self.get_pipeline_name()}")
        start = datetime.utcnow()

        # Build dlt resource
        @dlt.resource(
            name=self.get_resource_name(),
            write_disposition=self.write_disposition,
        )
        def _resource():
            for record in self.extract():
                yield record

        # Build and run dlt pipeline
        pipeline = dlt.pipeline(
            pipeline_name=self.get_pipeline_name(),
            destination=self._dlt_destination,
            dataset_name=cfg.PG_SCHEMA if self.destination == "postgres" else cfg.BQ_DATASET,
        )

        load_info = pipeline.run(_resource())

        duration = (datetime.utcnow() - start).total_seconds()
        logger.success(
            f"Pipeline {self.get_pipeline_name()} completed in {duration:.1f}s | "
            f"rows: {load_info.metrics}"
        )

        return {
            "pipeline": self.get_pipeline_name(),
            "destination": self.destination,
            "duration_seconds": duration,
            "load_info": str(load_info),
            "completed_at": datetime.utcnow().isoformat(),
        }


# ── Helpers ───────────────────────────────────────────────────────────────────

def normalize_date(d: Any) -> Optional[str]:
    """Normalize various date formats to ISO string."""
    if d is None:
        return None
    if isinstance(d, (date, datetime)):
        return d.isoformat()
    try:
        return pd.to_datetime(d).isoformat()
    except Exception:
        return str(d)


def safe_float(value: Any) -> Optional[float]:
    """Safely convert to float, returning None for missing/invalid values."""
    try:
        f = float(value)
        return None if pd.isna(f) else f
    except (TypeError, ValueError):
        return None
