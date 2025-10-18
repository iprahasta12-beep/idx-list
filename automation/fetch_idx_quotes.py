#!/usr/bin/env python3
"""Automate fetching IDX ticker data from Yahoo Finance.

This module fetches quote data for a predefined set of IDX tickers from the
public Yahoo Finance quote API and stores each snapshot inside a Google Sheets
worksheet. The task runs every hour by default, but you can override the
interval or run it once for testing with command-line flags.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Sequence
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

try:
    import gspread
    from gspread.exceptions import WorksheetNotFound
except ImportError as exc:  # pragma: no cover - dependency injection
    raise SystemExit(
        "The 'gspread' package is required to run this script. Install it with"
        " 'pip install gspread google-auth'."
    ) from exc

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python < 3.9 not expected
    ZoneInfo = None  # type: ignore


DEFAULT_INTERVAL_SECONDS = 60 * 60  # 1 hour
IDX_SUFFIX = ".JK"
YAHOO_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"
DEFAULT_TICKERS = ["CMRY", "MAIN", "BBCA", "BMRI", "ARCI"]
JAKARTA_TZ = ZoneInfo("Asia/Jakarta") if ZoneInfo else None

HEADER_ROW = [
    "symbol",
    "short_name",
    "price",
    "currency",
    "regular_market_time",
    "open_price",
    "day_high",
    "day_low",
    "previous_close",
    "fetch_time_utc",
    "latest_update_label",
]


@dataclass
class Quote:
    symbol: str
    short_name: str | None
    price: float | None
    currency: str | None
    regular_market_time: datetime | None
    open_price: float | None
    day_high: float | None
    day_low: float | None
    previous_close: float | None

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
            open_price=payload.get("regularMarketOpen"),
            day_high=payload.get("regularMarketDayHigh"),
            day_low=payload.get("regularMarketDayLow"),
            previous_close=payload.get("regularMarketPreviousClose"),
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


def ensure_header_row(worksheet: gspread.Worksheet) -> None:
    """Ensure the worksheet has the expected header row."""

    existing_header = worksheet.row_values(1)
    if existing_header != HEADER_ROW:
        worksheet.update(f"A1:K1", [HEADER_ROW])


def store_quotes(worksheet: gspread.Worksheet, quotes: Iterable[Quote]) -> None:
    """Store quotes in the Google Sheets worksheet."""

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
            quote.open_price,
            quote.day_high,
            quote.day_low,
            quote.previous_close,
            now_utc.isoformat(),
            latest_update_label,
        )
        for quote in quotes
    ]

    if not rows:
        raise RuntimeError("No quote data returned from Yahoo Finance.")

    ensure_header_row(worksheet)
    worksheet.append_rows(rows, value_input_option="USER_ENTERED")


def open_worksheet(
    spreadsheet_id: str,
    worksheet_title: str,
    service_account_path: Path | None,
) -> gspread.Worksheet:
    """Open the target Google Sheets worksheet, creating it if needed."""

    if service_account_path is not None:
        client = gspread.service_account(filename=str(service_account_path))
    else:
        client = gspread.service_account()

    spreadsheet = client.open_by_key(spreadsheet_id)
    try:
        worksheet = spreadsheet.worksheet(worksheet_title)
    except WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=worksheet_title, rows="1000", cols="20")
    return worksheet


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_INTERVAL_SECONDS,
        help="Interval between fetches in seconds (default: 3600).",
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
    parser.add_argument(
        "--spreadsheet-id",
        required=True,
        help="ID of the Google Spreadsheet to update.",
    )
    parser.add_argument(
        "--worksheet",
        default="Quotes",
        help="Worksheet title to update or create (default: %(default)s).",
    )
    parser.add_argument(
        "--service-account",
        type=Path,
        default=None,
        help=(
            "Path to a Google service account JSON key. If omitted,"
            " gspread's default discovery is used."
        ),
    )
    return parser.parse_args(argv)


def run_fetch_cycle(worksheet: gspread.Worksheet, tickers: Sequence[str]) -> None:
    quotes = fetch_quotes(tickers)
    store_quotes(worksheet, quotes)
    print(f"Stored quotes for {len(quotes)} tickers at {datetime.now()}")


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])

    worksheet = open_worksheet(
        spreadsheet_id=args.spreadsheet_id,
        worksheet_title=args.worksheet,
        service_account_path=args.service_account,
    )

    tickers = args.tickers
    interval = max(1, args.interval)

    while True:
        try:
            run_fetch_cycle(worksheet, tickers)
        except Exception as exc:
            print(f"Error during fetch cycle: {exc}", file=sys.stderr)
        if args.once:
            break
        time.sleep(interval)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
