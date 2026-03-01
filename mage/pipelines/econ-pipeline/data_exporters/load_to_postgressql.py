import os
import sys

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_to_postgres(df, *args, **kwargs) -> None:
    """
    DATA EXPORTER BLOCK
    Write the merged DataFrame to PostgreSQL.
    In Mage: shows row counts and column types after export.
    """
    from sqlalchemy import create_engine
    from loguru import logger

    engine = create_engine(
        f"postgresql://{os.getenv('POSTGRES_USER','econ_user')}:"
        f"{os.getenv('POSTGRES_PASSWORD','')}@"
        f"{os.getenv('POSTGRES_HOST','localhost')}:"
        f"{os.getenv('POSTGRES_PORT','5432')}/"
        f"{os.getenv('POSTGRES_DB','econ_warehouse')}"
    )

    df.to_sql(
        name="mage_macro_summary",
        schema="raw",
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=1000,
    )
    logger.success(f"Exported {len(df):,} rows → raw.mage_macro_summary")
