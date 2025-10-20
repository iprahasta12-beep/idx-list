"""Fetch hourly OHLCV data for IDX tickers from Yahoo Finance."""

from __future__ import annotations

from pathlib import Path
from typing import List

import pandas as pd
import yfinance as yf

from .utils import (
    ensure_csv,
    ensure_directories,
    load_config,
    load_tickers,
    setup_logger,
    timestamp_now,
)


COLUMNS = ["ticker", "datetime", "open", "high", "low", "close", "volume"]


def to_timezone_string(value: pd.Timestamp, timezone: str | None) -> str:
    if not isinstance(value, pd.Timestamp):
        return str(value)

    if value.tzinfo is None:
        value = value.tz_localize("UTC")

    target_timezone = timezone or "UTC"
    try:
        localized = value.tz_convert(target_timezone)
    except Exception:
        localized = value

    return localized.strftime("%Y-%m-%d %H:%M:%S%z")


def fetch_ticker_history(ticker: str, period: str, interval: str) -> pd.DataFrame:
    data = yf.download(ticker, period=period, interval=interval, auto_adjust=False, progress=False)
    if data.empty:
        return data

    data = data.reset_index()
    data["Ticker"] = ticker
    return data


def main() -> None:
    config = load_config()
    ensure_directories([config["data_dir"], config["snapshots_dir"], Path(config["log_file"]).parent])

    logger = setup_logger("fetch_quotes", config["log_file"])

    tickers: List[str] = load_tickers(config["tickers_file"])
    if not tickers:
        logger.warning("No tickers found in %s", config["tickers_file"])
        return

    master_path = ensure_csv(config["quotes_file"], COLUMNS)
    master_df = pd.read_csv(master_path)

    all_records = []
    timezone = config.get("timezone", "UTC")

    logger.info("Fetching %s tickers (period=%s, interval=%s)", len(tickers), config["quote_period"], config["quote_interval"])

    for ticker in tickers:
        history = fetch_ticker_history(ticker, config["quote_period"], config["quote_interval"])
        if history.empty:
            logger.warning("No data returned for %s", ticker)
            continue

        for _, row in history.iterrows():
            all_records.append(
                {
                    "ticker": ticker,
                    "datetime": to_timezone_string(row["Datetime"], timezone),
                    "open": row["Open"],
                    "high": row["High"],
                    "low": row["Low"],
                    "close": row["Close"],
                    "volume": int(row["Volume"]),
                }
            )

    if not all_records:
        logger.warning("No records fetched; nothing to update.")
        return

    new_df = pd.DataFrame(all_records)
    new_df.drop_duplicates(subset=["ticker", "datetime"], keep="last", inplace=True)

    combined = (
        pd.concat([master_df, new_df], ignore_index=True)
        .drop_duplicates(subset=["ticker", "datetime"], keep="last")
        .sort_values(["ticker", "datetime"])
    )

    combined.to_csv(master_path, index=False)

    snapshot_name = f"quotes_{timestamp_now(config['snapshot_format'], config.get('timezone'))}.csv"
    snapshot_path = Path(config["snapshots_dir"]) / snapshot_name
    new_df.to_csv(snapshot_path, index=False)

    logger.info("Stored %s new rows", len(new_df))
    logger.info("Snapshot saved to %s", snapshot_path)
    print(f"✅ Hourly data updated for {len(tickers)} tickers")


if __name__ == "__main__":
    main()
