"""
yfinance_pipeline.py
--------------------
Ingests daily market data via yfinance:
  S&P 500, NASDAQ, VIX, USD Index, Oil (WTI), Gold,
  10Y/2Y Treasury ETFs, Investment Grade & HY Credit spreads.

Usage:
    python ingestion/yfinance_pipeline.py --destination postgres
    python ingestion/yfinance_pipeline.py --tickers SPY,QQQ --start 2000-01-01
"""

from __future__ import annotations

import argparse
from datetime import date, datetime
from typing import Generator

import pandas as pd
import yfinance as yf
from loguru import logger

from base_pipeline import BasePipeline, cfg, normalize_date, safe_float


# ── Ticker Config ─────────────────────────────────────────────────────────────

MARKET_TICKERS = {
    # Equity Indices
    "^GSPC": {"name": "S&P 500", "category": "equity_index", "asset_class": "equity"},
    "^IXIC": {"name": "NASDAQ Composite", "category": "equity_index", "asset_class": "equity"},
    "^DJI":  {"name": "Dow Jones Industrial Average", "category": "equity_index", "asset_class": "equity"},
    "^RUT":  {"name": "Russell 2000", "category": "equity_index", "asset_class": "equity"},
    # Volatility
    "^VIX":  {"name": "CBOE Volatility Index", "category": "volatility", "asset_class": "volatility"},
    # Fixed Income ETFs
    "IEF":   {"name": "iShares 7-10Y Treasury ETF", "category": "fixed_income", "asset_class": "bonds"},
    "SHY":   {"name": "iShares 1-3Y Treasury ETF", "category": "fixed_income", "asset_class": "bonds"},
    "TLT":   {"name": "iShares 20+Y Treasury ETF", "category": "fixed_income", "asset_class": "bonds"},
    "LQD":   {"name": "iShares Investment Grade Corp Bond ETF", "category": "credit", "asset_class": "bonds"},
    "HYG":   {"name": "iShares High Yield Corp Bond ETF", "category": "credit", "asset_class": "bonds"},
    # Commodities
    "CL=F":  {"name": "WTI Crude Oil Futures", "category": "commodity", "asset_class": "commodity"},
    "GC=F":  {"name": "Gold Futures", "category": "commodity", "asset_class": "commodity"},
    "SI=F":  {"name": "Silver Futures", "category": "commodity", "asset_class": "commodity"},
    # Currency
    "DX-Y.NYB": {"name": "US Dollar Index", "category": "fx", "asset_class": "currency"},
    "EURUSD=X":  {"name": "EUR/USD", "category": "fx", "asset_class": "currency"},
    "GBPUSD=X":  {"name": "GBP/USD", "category": "fx", "asset_class": "currency"},
    "JPY=X":     {"name": "USD/JPY", "category": "fx", "asset_class": "currency"},
    # Crypto (macro relevance)
    "BTC-USD": {"name": "Bitcoin USD", "category": "crypto", "asset_class": "crypto"},
}


# ── Pipeline ──────────────────────────────────────────────────────────────────

class YFinancePipeline(BasePipeline):
    """
    Downloads OHLCV + derived features for configured tickers.

    Derived features computed per ticker:
      - daily_return: (close - prev_close) / prev_close
      - log_return: log(close / prev_close)
      - rolling_vol_20d: 20-day annualised realised volatility
      - above_200ma: boolean, price > 200-day MA
    """

    def __init__(
        self,
        destination: str = "postgres",
        tickers: list[str] | None = None,
        start_date: str = "1990-01-01",
        end_date: str | None = None,
        **kwargs,
    ):
        super().__init__(destination=destination, write_disposition="merge", **kwargs)
        self.tickers = tickers or list(MARKET_TICKERS.keys())
        self.start_date = start_date
        self.end_date = end_date or date.today().isoformat()

    def get_pipeline_name(self) -> str:
        return "yfinance_pipeline"

    def get_resource_name(self) -> str:
        return "market_prices"

    def _fetch_ticker(self, ticker: str) -> pd.DataFrame:
        """Download historical OHLCV data and compute derived features."""
        logger.info(f"Downloading {ticker}...")
        df = yf.download(
            ticker,
            start=self.start_date,
            end=self.end_date,
            progress=False,
            auto_adjust=True,
        )

        if df.empty:
            logger.warning(f"No data returned for {ticker}")
            return pd.DataFrame()

        df = df.reset_index()
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]

        # Derived features
        df["daily_return"] = df["close"].pct_change()
        df["log_return"] = (df["close"] / df["close"].shift(1)).apply(
            lambda x: None if pd.isna(x) else __import__("math").log(x)
        )
        df["rolling_vol_20d"] = df["daily_return"].rolling(20).std() * (252 ** 0.5)
        df["ma_50"] = df["close"].rolling(50).mean()
        df["ma_200"] = df["close"].rolling(200).mean()
        df["above_200ma"] = df["close"] > df["ma_200"]

        logger.info(f"  → {len(df)} rows for {ticker}")
        return df

    def extract(self) -> Generator[dict, None, None]:
        for ticker in self.tickers:
            meta = MARKET_TICKERS.get(ticker, {})

            try:
                df = self._fetch_ticker(ticker)
                if df.empty:
                    continue

                # Archive to S3
                if self.archive_to_s3 and self.s3:
                    self.s3.archive_dataframe(
                        df=df,
                        source="yfinance",
                        indicator=ticker.lower().replace("^", "").replace("=", "_"),
                    )

                for _, row in df.iterrows():
                    yield {
                        # Primary key
                        "ticker": ticker,
                        "trade_date": normalize_date(row.get("date")),
                        # OHLCV
                        "open": safe_float(row.get("open")),
                        "high": safe_float(row.get("high")),
                        "low": safe_float(row.get("low")),
                        "close": safe_float(row.get("close")),
                        "volume": safe_float(row.get("volume")),
                        # Derived
                        "daily_return": safe_float(row.get("daily_return")),
                        "log_return": safe_float(row.get("log_return")),
                        "rolling_vol_20d": safe_float(row.get("rolling_vol_20d")),
                        "ma_50": safe_float(row.get("ma_50")),
                        "ma_200": safe_float(row.get("ma_200")),
                        "above_200ma": bool(row.get("above_200ma")),
                        # Metadata
                        "ticker_name": meta.get("name", ""),
                        "category": meta.get("category", ""),
                        "asset_class": meta.get("asset_class", ""),
                        "source": "yfinance",
                        # Audit
                        "ingested_at": datetime.utcnow().isoformat(),
                    }

            except Exception as e:
                logger.error(f"Error processing {ticker}: {e}")
                raise


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="Run Yahoo Finance market data pipeline")
    parser.add_argument("--destination", choices=["postgres", "bigquery"], default="postgres")
    parser.add_argument("--tickers", type=str, default=None, help="Comma-separated tickers")
    parser.add_argument("--start", type=str, default="1990-01-01")
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument("--no-s3", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    tickers = args.tickers.split(",") if args.tickers else None

    pipeline = YFinancePipeline(
        destination=args.destination,
        tickers=tickers,
        start_date=args.start,
        end_date=args.end,
        archive_to_s3=not args.no_s3,
    )

    result = pipeline.run()
    logger.info(f"Result: {result}")
