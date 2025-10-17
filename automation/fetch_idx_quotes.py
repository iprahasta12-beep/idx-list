#!/usr/bin/env python3
"""Automate fetching IDX ticker data from Yahoo Finance.

This module fetches quote data for a predefined set of IDX tickers from the
public Yahoo Finance quote API and stores each snapshot inside a SQLite
database. The task runs every hour by default, but you can override the
interval or run it once for testing with command-line flags.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Sequence
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python < 3.9 not expected
    ZoneInfo = None  # type: ignore


DEFAULT_INTERVAL_SECONDS = 60 * 60  # 1 hour
IDX_SUFFIX = ".JK"
YAHOO_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"
DEFAULT_TICKERS = ["CMRY", "MAIN", "BBCA", "BMRI", "ARCI"]
JAKARTA_TZ = ZoneInfo("Asia/Jakarta") if ZoneInfo else None


@dataclass
class Quote:
    symbol: str
    short_name: str | None
    price: float | None
    currency: str | None
    regular_market_time: datetime | None

    @classmethod
    def from_yahoo_payload(cls, payload: dict) -> "Quote":
        """Create a quote instance from a Yahoo Finance response payload."""

        symbol = payload.get("symbol", "").upper()
        short_name = payload.get("shortName")
        price = payload.get("regularMarketPrice")
        currency = payload.get("currency")
        market_time_epoch = payload.get("regularMarketTime")
        market_time = None
        if market_time_epoch:
            market_time = datetime.fromtimestamp(
                market_time_epoch, tz=timezone.utc
            )
        return cls(
            symbol=symbol,
            short_name=short_name,
            price=price,
            currency=currency,
            regular_market_time=market_time,
        )


def ticker_with_suffix(ticker: str) -> str:
    """Append the IDX suffix for Yahoo Finance queries."""

    ticker = ticker.strip().upper()
    if ticker.endswith(IDX_SUFFIX):
        return ticker
    return f"{ticker}{IDX_SUFFIX}"


def build_quote_url(tickers: Sequence[str]) -> str:
    """Build the Yahoo Finance query URL."""

    symbols = ",".join(ticker_with_suffix(t) for t in tickers)
    return f"{YAHOO_QUOTE_URL}?symbols={symbols}"


def fetch_quotes(tickers: Sequence[str]) -> List[Quote]:
    """Fetch quote data from Yahoo Finance."""

    url = build_quote_url(tickers)
    request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urlopen(request, timeout=30) as response:
            payload = json.load(response)
    except HTTPError as exc:  # pragma: no cover - relies on external API
        raise RuntimeError(f"Yahoo Finance returned HTTP {exc.code}: {exc.reason}") from exc
    except URLError as exc:  # pragma: no cover - relies on network conditions
        raise RuntimeError(f"Failed to fetch Yahoo Finance data: {exc}") from exc

    results = payload.get("quoteResponse", {}).get("result", [])
    quotes = [Quote.from_yahoo_payload(result) for result in results]
    return quotes


def ensure_schema(connection: sqlite3.Connection) -> None:
    """Ensure the SQLite schema exists."""

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS idx_quotes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            short_name TEXT,
            price REAL,
            currency TEXT,
            regular_market_time TEXT,
            fetch_time_utc TEXT NOT NULL,
            latest_update_label TEXT NOT NULL
        )
        """
    )
    connection.commit()


def store_quotes(connection: sqlite3.Connection, quotes: Iterable[Quote]) -> None:
    """Store quotes in the SQLite database."""

    now_utc = datetime.now(timezone.utc)
    if JAKARTA_TZ:
        jakarta_now = now_utc.astimezone(JAKARTA_TZ)
    else:  # pragma: no cover - fallback when zoneinfo missing
        jakarta_now = now_utc
    latest_update_label = f"Latest update {jakarta_now.strftime('%H:%M')}"

    rows = [
        (
            quote.symbol,
            quote.short_name,
            quote.price,
            quote.currency,
            quote.regular_market_time.isoformat() if quote.regular_market_time else None,
            now_utc.isoformat(),
            latest_update_label,
        )
        for quote in quotes
    ]

    if not rows:
        raise RuntimeError("No quote data returned from Yahoo Finance.")

    connection.executemany(
        """
        INSERT INTO idx_quotes (
            symbol,
            short_name,
            price,
            currency,
            regular_market_time,
            fetch_time_utc,
            latest_update_label
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    connection.commit()


def open_database(path: Path) -> sqlite3.Connection:
    """Open the SQLite database, creating directories as needed."""

    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    return connection


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_INTERVAL_SECONDS,
        help="Interval between fetches in seconds (default: 3600).",
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "data" / "idx_quotes.db",
        help="Path to the SQLite database file.",
    )
    parser.add_argument(
        "--tickers",
        nargs="*",
        default=DEFAULT_TICKERS,
        help="List of IDX tickers to fetch (default: %(default)s).",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Fetch once and exit instead of running continuously.",
    )
    return parser.parse_args(argv)


def run_fetch_cycle(connection: sqlite3.Connection, tickers: Sequence[str]) -> None:
    quotes = fetch_quotes(tickers)
    store_quotes(connection, quotes)
    print(f"Stored quotes for {len(quotes)} tickers at {datetime.now()}")


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])

    connection = open_database(args.database)
    ensure_schema(connection)

    tickers = args.tickers
    interval = max(1, args.interval)

    try:
        while True:
            try:
                run_fetch_cycle(connection, tickers)
            except Exception as exc:
                print(f"Error during fetch cycle: {exc}", file=sys.stderr)
            if args.once:
                break
            time.sleep(interval)
    finally:
        connection.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
