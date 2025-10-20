"""Calculate technical indicators for IDX tickers."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import numpy as np
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


RESULT_COLUMNS = [
    "TICKER",
    "CLOSE",
    "VOLUME",
    "MA20",
    "MA50",
    "RSI14",
    "MAX30",
    "MAX30_DATE",
    "NEW30HIGHWITHIN5",
    "PULLBACK2DNSTAY>MA20",
    "BULLISH REVERSAL",
    "VOL SURGE 130",
    "INDICATOR",
]


def calc_rsi(series: pd.Series, period: int) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi


def evaluate_ticker(ticker: str, cfg: Dict[str, object]) -> Dict[str, object] | None:
    df = yf.download(
        ticker,
        period=cfg["indicator_period"],
        interval=cfg["indicator_interval"],
        auto_adjust=False,
        progress=False,
    )

    if df.empty or len(df) < cfg["indicator_ma_short"]:
        return None

    df = df.dropna(subset=["Close", "Volume"])  # ensure data quality
    if df.empty:
        return None

    df["MA20"] = df["Close"].rolling(window=cfg["indicator_ma_short"]).mean()
    df["MA50"] = df["Close"].rolling(window=cfg["indicator_ma_long"]).mean()
    df["RSI14"] = calc_rsi(df["Close"], cfg["indicator_rsi_period"])
    df["MAX30"] = df["Close"].rolling(window=cfg["indicator_max_window"]).max()

    latest = df.iloc[-1]
    last5 = df.tail(5)

    max30_value = df["MAX30"].iloc[-1]
    if pd.isna(max30_value):
        max30_value = df["Close"].iloc[-1]

    new_high = bool((last5["Close"] >= max30_value).any())

    latest_close = df["Close"].iloc[-1]
    latest_ma20 = latest["MA20"]

    pullback = (
        len(df) >= 3
        and not pd.isna(latest_ma20)
        and latest_close > latest_ma20
        and latest_close < df["Close"].iloc[-2]
        and df["Close"].iloc[-2] < df["Close"].iloc[-3]
    )

    recent_volume = df["Volume"].tail(10)
    vol_surge = bool(latest["Volume"] >= 1.3 * recent_volume.mean()) if not recent_volume.empty else False

    indicator_score = sum(
        [
            latest["Close"] > latest["MA20"],
            latest["Close"] > latest["MA50"],
            latest["MA20"] > latest["MA50"],
            latest["RSI14"] > 55,
            new_high,
            pullback,
            vol_surge,
        ]
    )

    max30_date = df[df["Close"] >= max30_value].index.max()
    max30_date_str = max30_date.strftime("%Y-%m-%d") if not pd.isna(max30_date) else ""

    return {
        "TICKER": ticker,
        "CLOSE": round(float(latest["Close"]), 2),
        "VOLUME": int(latest["Volume"]),
        "MA20": round(float(latest["MA20"]), 2),
        "MA50": round(float(latest["MA50"]), 2),
        "RSI14": round(float(latest["RSI14"]), 2),
        "MAX30": round(float(df["MAX30"].iloc[-1]), 2),
        "MAX30_DATE": max30_date_str,
        "NEW30HIGHWITHIN5": bool(new_high),
        "PULLBACK2DNSTAY>MA20": bool(pullback),
        "BULLISH REVERSAL": None,
        "VOL SURGE 130": bool(vol_surge),
        "INDICATOR": int(indicator_score),
    }


def main() -> None:
    config = load_config()
    ensure_directories([config["data_dir"], config["snapshots_dir"], Path(config["log_file"]).parent])

    logger = setup_logger("calculate_indicators", config["log_file"])

    tickers: List[str] = load_tickers(config["tickers_file"])
    if not tickers:
        logger.warning("No tickers available for indicator calculation.")
        return

    results: List[Dict[str, object]] = []
    for ticker in tickers:
        metrics = evaluate_ticker(ticker, config)
        if metrics is None:
            logger.warning("Skipping %s due to insufficient data", ticker)
            continue
        results.append(metrics)

    if not results:
        logger.warning("No indicators calculated; nothing to save.")
        return

    indicators_path = ensure_csv(config["indicators_file"], RESULT_COLUMNS)
    df = pd.DataFrame(results)
    df.to_csv(indicators_path, index=False)

    snapshot_name = f"indicators_{timestamp_now(config['snapshot_format'], config.get('timezone'))}.csv"
    snapshot_path = Path(config["snapshots_dir"]) / snapshot_name
    df.to_csv(snapshot_path, index=False)

    logger.info("Indicators calculated for %s tickers", len(results))
    logger.info("Snapshot saved to %s", snapshot_path)
    print(f"✅ Indicator table updated for {len(results)} tickers")


if __name__ == "__main__":
    main()
