from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from ..core.config import get_settings
from ..data.repository import get_repository
from .fetcher import fetch_daily, fetch_intraday
from .indicators import compute_indicators

logger = logging.getLogger(__name__)


def fetch_and_compute(days: int = 7, include_intraday: bool = True) -> None:
    settings = get_settings()
    tickers = settings.load_tickers()
    repo = get_repository()
    end = datetime.now(timezone.utc)
    start_daily = end - timedelta(days=max(days, settings.high_lookback + 60))
    logger.info(
        "Fetching daily candles",
        extra={"symbols": len(tickers), "start": start_daily.isoformat(), "end": end.isoformat()},
    )
    daily_rows = fetch_daily(tickers, start_daily, end, interval="1d")
    repo.upsert_prices(daily_rows)

    if include_intraday:
        start_intra = end - timedelta(days=7)
        logger.info(
            "Fetching intraday candles",
            extra={"symbols": len(tickers), "start": start_intra.isoformat(), "end": end.isoformat()},
        )
        intra_rows = fetch_intraday(tickers, start_intra, end, interval="60m")
        repo.upsert_prices(intra_rows)

    history_days = max(settings.high_lookback + 60, 120)
    df_prices = repo.load_prices(days=history_days)
    if df_prices.empty:
        logger.warning("No price data available to compute indicators")
        return
    indicators = compute_indicators(df_prices)
    repo.upsert_indicators(indicators)
    logger.info("Indicators updated", extra={"count": len(indicators)})


def backfill(days: int) -> None:
    settings = get_settings()
    tickers = settings.load_tickers()
    repo = get_repository()
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days + 5)
    logger.info(
        "Backfilling historical data",
        extra={"symbols": len(tickers), "days": days},
    )
    rows = fetch_daily(tickers, start, end, interval="1d")
    repo.upsert_prices(rows)
    df_prices = repo.load_prices(days=max(days, settings.high_lookback + 60))
    if df_prices.empty:
        return
    indicators = compute_indicators(df_prices)
    repo.upsert_indicators(indicators)
    logger.info("Backfill complete", extra={"count": len(indicators)})
