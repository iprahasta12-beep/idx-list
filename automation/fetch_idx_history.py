#!/usr/bin/env python3
"""Fetch historical IDX OCHLV data and store it in a SQLite database.

This script queries the public Yahoo Finance chart API for one or more IDX
tickers starting from the requested date (1 August 2024 by default). The
returned daily candles are written into a local SQLite database, allowing the
dashboard to display historical trading data. Optionally the stored data can be
exported into a JSON snapshot that powers the Trading tab filter.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Iterable, List, Sequence
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DEFAULT_TICKERS = ("CMRY", "MAIN", "BBCA", "BMRI", "ARCI")
DEFAULT_START_DATE = date(2024, 8, 1)
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
IDX_SUFFIX = ".JK"


@dataclass(slots=True)
class Candle:
    """A single daily candle."""

    symbol: str
    trade_date: date
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: int | None

    def as_row(self) -> tuple[str, str, float | None, float | None, float | None, float | None, int | None]:
        return (
            self.symbol,
            self.trade_date.isoformat(),
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
        )


def ticker_with_suffix(ticker: str) -> str:
    ticker = ticker.strip().upper()
    if ticker.endswith(IDX_SUFFIX):
        return ticker
    return f"{ticker}{IDX_SUFFIX}"


def ensure_schema(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_quotes (
            symbol TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            PRIMARY KEY (symbol, trade_date)
        )
        """
    )


def epoch_seconds(value: date, *, end_of_day: bool = False) -> int:
    dt = datetime.combine(value, time.max if end_of_day else time.min, tzinfo=timezone.utc)
    return int(dt.timestamp())


def fetch_history(symbol: str, start: date, end: date) -> List[Candle]:
    yahoo_symbol = ticker_with_suffix(symbol)
    params = f"?period1={epoch_seconds(start)}&period2={epoch_seconds(end, end_of_day=True)}&interval=1d"
    url = YAHOO_CHART_URL.format(symbol=yahoo_symbol) + params
    request = Request(url, headers={"User-Agent": "Mozilla/5.0"})

    try:
        with urlopen(request, timeout=30) as response:
            payload = json.load(response)
    except HTTPError as exc:  # pragma: no cover - depends on remote API
        raise RuntimeError(f"Yahoo Finance returned HTTP {exc.code}: {exc.reason}") from exc
    except URLError as exc:  # pragma: no cover - depends on network availability
        raise RuntimeError(f"Failed to fetch Yahoo Finance data: {exc}") from exc

    result = (payload.get("chart", {}).get("result") or [None])[0]
    if not result:
        return []

    timestamps = result.get("timestamp", [])
    indicators = result.get("indicators", {}).get("quote", [{}])[0]
    opens = indicators.get("open", [])
    highs = indicators.get("high", [])
    lows = indicators.get("low", [])
    closes = indicators.get("close", [])
    volumes = indicators.get("volume", [])

    candles: List[Candle] = []
    for index, ts in enumerate(timestamps):
        # The Yahoo API returns seconds since epoch in UTC.
        trade_date = datetime.fromtimestamp(ts, tz=timezone.utc).date()
        candles.append(
            Candle(
                symbol=ticker_with_suffix(symbol),
                trade_date=trade_date,
                open=_safe_get(opens, index),
                high=_safe_get(highs, index),
                low=_safe_get(lows, index),
                close=_safe_get(closes, index),
                volume=_safe_get(volumes, index, cast=int),
            )
        )
    return candles


def _safe_get(sequence: Sequence[float | None] | Sequence[int | None], index: int, cast: type | None = None):
    try:
        value = sequence[index]
    except IndexError:
        return None
    if value is None:
        return None
    if cast:
        try:
            return cast(value)
        except (TypeError, ValueError):
            return None
    return value


def store_candles(connection: sqlite3.Connection, candles: Iterable[Candle]) -> int:
    with connection:
        rows = [candle.as_row() for candle in candles]
        connection.executemany(
            """
            INSERT INTO daily_quotes (symbol, trade_date, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, trade_date) DO UPDATE SET
                open=excluded.open,
                high=excluded.high,
                low=excluded.low,
                close=excluded.close,
                volume=excluded.volume
            """,
            rows,
        )
    return len(rows)


def export_json(connection: sqlite3.Connection, destination: Path) -> None:
    cursor = connection.execute(
        """
        SELECT symbol, trade_date, open, high, low, close, volume
        FROM daily_quotes
        ORDER BY trade_date DESC, symbol ASC
        """
    )
    payload = [
        {
            "symbol": row[0],
            "date": row[1],
            "open": row[2],
            "high": row[3],
            "low": row[4],
            "close": row[5],
            "volume": row[6],
        }
        for row in cursor.fetchall()
    ]
    destination.write_text(json.dumps(payload, indent=2))


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tickers", nargs="*", default=DEFAULT_TICKERS, help="Tickers to download")
    parser.add_argument(
        "--start-date",
        type=_parse_date,
        default=DEFAULT_START_DATE,
        help="Start date (inclusive) in YYYY-MM-DD format. Default: %(default)s",
    )
    parser.add_argument(
        "--end-date",
        type=_parse_date,
        default=date.today,
        help="End date (inclusive). Default: today",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("data/idx_ochv.sqlite3"),
        help="SQLite database file to create or update",
    )
    parser.add_argument(
        "--export-json",
        type=Path,
        default=None,
        help="Optional path to export the stored data as JSON",
    )
    return parser.parse_args(argv)


def _parse_date(value: str | date):
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    start_date: date = args.start_date
    end_date: date = args.end_date if not callable(args.end_date) else args.end_date()
    if end_date < start_date:
        raise SystemExit("End date must be on or after start date")

    args.db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(args.db_path)
    ensure_schema(connection)

    total_rows = 0
    for ticker in args.tickers:
        candles = fetch_history(ticker, start_date, end_date)
        if not candles:
            continue
        inserted = store_candles(connection, candles)
        total_rows += inserted
        print(f"Stored {inserted} rows for {ticker.upper()}")

    if args.export_json:
        export_json(connection, args.export_json)
        print(f"Exported JSON snapshot to {args.export_json}")

    connection.close()
    print(f"Finished. {total_rows} rows written.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
