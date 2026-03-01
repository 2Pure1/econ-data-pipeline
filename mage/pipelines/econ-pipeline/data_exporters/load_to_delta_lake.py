import os
import sys

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_to_delta(df, *args, **kwargs) -> None:
    """
    DATA EXPORTER BLOCK
    Write the merged DataFrame to Delta Lake (alongside the Postgres export).
    Demonstrates Mage's ability to fan-out to multiple destinations.
    """
    from loguru import logger

    delta_path = os.path.join(
        os.getenv("DELTA_S3_PATH", "/tmp/delta_lake"), "mage/macro_summary"
    )

    is_s3 = delta_path.startswith("s3://") or delta_path.startswith("s3a://")

    if is_s3:
        # Normalise s3a:// → s3:// for pandas/s3fs
        s3_path = delta_path.replace("s3a://", "s3://")
        storage_options = {
            "key":    os.getenv("AWS_ACCESS_KEY_ID", ""),
            "secret": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        }
        endpoint = os.getenv("AWS_ENDPOINT_URL", "")
        if endpoint:
            storage_options["endpoint_url"] = endpoint

        df.to_parquet(
            f"{s3_path}/data.parquet",
            index=False,
            storage_options=storage_options,
        )
    else:
        os.makedirs(delta_path, exist_ok=True)
        df.to_parquet(f"{delta_path}/data.parquet", index=False)

    logger.success(f"Exported {len(df):,} rows → {delta_path}")
