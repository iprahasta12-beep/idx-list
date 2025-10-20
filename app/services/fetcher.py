from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Iterable, List

import requests

from ..data.models import PriceRow

logger = logging.getLogger(__name__)

BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"


def build_chart_url(symbol: str, interval: str, start: int, end: int) -> str:
    return f"{BASE_URL.format(symbol=symbol)}?interval={interval}&period1={start}&period2={end}"


def _to_timestamp(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp())


def _parse_chart(symbol: str, data: dict) -> List[PriceRow]:
    result = data.get("chart", {}).get("result")
    if not result:
        return []
    result = result[0]
    timestamps = result.get("timestamp") or []
    quote = result.get("indicators", {}).get("quote", [{}])[0]
    opens = quote.get("open", [])
    highs = quote.get("high", [])
    lows = quote.get("low", [])
    closes = quote.get("close", [])
    volumes = quote.get("volume", [])
    now_ts = int(datetime.now(timezone.utc).timestamp())
    rows: List[PriceRow] = []
    for ts, o, h, l, c, v in zip(timestamps, opens, highs, lows, closes, volumes):
        if ts is None:
            continue
        if ts > now_ts:
            logger.debug("Skipping future candle", extra={"symbol": symbol, "ts": ts})
            continue
        if None in (o, h, l, c):
            continue
        rows.append(
            PriceRow(
                symbol=symbol,
                ts_utc=int(ts),
                open=float(o),
                high=float(h),
                low=float(l),
                close=float(c),
                volume=float(v) if v is not None else 0.0,
            )
        )
    return rows


def fetch_daily(symbols: Iterable[str], start: datetime, end: datetime, interval: str = "1d") -> List[PriceRow]:
    start_ts = _to_timestamp(start)
    end_ts = _to_timestamp(end)
    rows: List[PriceRow] = []
    for symbol in symbols:
        attempt = 0
        backoff = 1
        while attempt < 3:
            attempt += 1
            try:
                url = build_chart_url(symbol, interval, start_ts, end_ts)
                resp = requests.get(url, timeout=10)
                if resp.status_code != 200:
                    raise RuntimeError(f"HTTP {resp.status_code}")
                data = resp.json()
                parsed = _parse_chart(symbol, data)
                rows.extend(parsed)
                break
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Fetch failed", extra={"symbol": symbol, "attempt": attempt, "error": str(exc)}
                )
                time.sleep(backoff)
                backoff *= 2
        time.sleep(0.2)
    return rows


def fetch_intraday(symbols: Iterable[str], start: datetime, end: datetime, interval: str = "60m") -> List[PriceRow]:
    return fetch_daily(symbols, start, end, interval=interval)
