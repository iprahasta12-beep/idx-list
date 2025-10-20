#!/usr/bin/env python3
"""Automate fetching IDX ticker data from Yahoo Finance.

This module fetches quote data for a predefined set of IDX tickers from the
public Yahoo Finance quote API and stores each snapshot inside a local CSV
file. The task runs every hour by default, but you can override the interval or
run it once for testing with command-line flags.
"""

from __future__ import annotations

import argparse
import csv
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
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python < 3.9 not expected
    ZoneInfo = None  # type: ignore


DEFAULT_INTERVAL_SECONDS = 60 * 60  # 1 hour
IDX_SUFFIX = ".JK"
YAHOO_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"
DEFAULT_TICKERS = [
    "BBCA",
    "TPIA",
    "BREN",
    "BBRI",
    "AMMN",
    "BMRI",
    "DSSA",
    "TLKM",
    "PANI",
    "ASII",
    "BBNI",
    "BRPT",
    "CUAN",
    "ICBP",
    "BRIS",
    "AMRT",
    "BNLI",
    "ANTM",
    "UNTR",
    "CPIN",
    "KLBF",
    "INDF",
    "ISAT",
    "PGEO",
    "ADRO",
    "UNVR",
    "AADI",
    "MDKA",
    "CASA",
    "TBIG",
    "MYOR",
    "BNGA",
    "ADMR",
    "PGAS",
    "EXCL",
    "CMRY",
    "INCO",
    "MEDC",
    "CBDK",
    "MIKA",
    "PTBA",
    "INKP",
    "NISP",
    "PTRO",
    "PNBN",
    "SILO",
    "JSMR",
    "TCPI",
    "ITMG",
    "AKRA",
    "ARTO",
    "BDMN",
    "FILM",
    "SRTG",
    "RATU",
    "HEAL",
    "INTP",
    "MAPI",
    "SMGR",
    "TAPG",
    "GGRM",
    "BSDE",
    "TKIM",
    "JPFA",
    "CTRA",
    "BBTN",
    "CMNT",
    "APIC",
    "ULTJ",
    "SSMS",
    "NICL",
    "BANK",
    "BFIN",
    "HRUM",
    "RAJA",
    "AALI",
    "SMSM",
    "TSPC",
    "BSSR",
    "RISE",
    "BTPS",
    "AUTO",
    "JRPT",
    "BJBR",
    "LSIP",
    "STAA",
    "ABMM",
    "TINS",
    "DSNG",
    "CMNP",
    "INDY",
    "PACK",
    "TOBA",
    "SSIA",
    "CYBR",
    "BALI",
    "ANJT",
    "NOBU",
    "ROTI",
    "BIRD",
    "WIFI",
    "CBUT",
    "DRMA",
    "SGRO",
    "MSTI",
    "HEXA",
    "TBLA",
    "MPMX",
    "MBSS",
    "SMDM",
    "PNIN",
    "GJTL",
    "LPPF",
    "TUGU",
    "IMAS",
    "AGII",
    "KEEN",
    "BNBA",
    "BISI",
    "BOLT",
    "ASSA",
    "PRDA",
    "ARKO",
    "SUNI",
    "IPCC",
    "MTMH",
    "DATA",
    "CEKA",
    "WIIM",
    "ASGR",
    "PANR",
    "PKPK",
    "ITMA",
    "FORU",
    "PNSE",
    "KARW",
    "RIGS",
    "CLPI",
    "MREI",
    "IMPC",
    "CFIN",
    "NSSS",
    "SMDR",
    "NICE",
    "ISSP",
    "SGER",
    "BPFI",
    "ENRG",
    "MKAP",
    "AIMS",
    "OBAT",
    "WOOD",
    "DMMX",
    "GPSO",
    "PBSA",
    "JARR",
    "KKGI",
    "PWON",
    "SMRA",
    "PTMR",
    "WINS",
    "MBMA",
    "KOPI",
    "BRMS",
    "UNIQ",
    "RALS",
    "RSCH",
    "MIDI",
    "DOID",
    "IRRA",
    "AVIA",
    "DKFT",
    "PTPP",
    "PSAB",
    "AREA",
    "LPGI",
    "SIMP",
    "DGWG",
    "MICE",
    "ELSA",
    "EMTK",
    "BJTM",
    "SIDO",
    "TOWR",
    "MSIN",
    "UCID",
    "PDPP",
    "ERAA",
    "MMLP",
    "RMKE",
    "ACES",
    "MTEL",
    "MTDL",
    "SNLK",
    "MINE",
    "ARCI",
    "TPMA",
    "HRTA",
    "ARNA",
    "ESSA",
    "JIHD",
    "BIKE",
    "HMSP",
    "MAIN",
    "ALII",
    "TEBE",
    "VICI",
    "MAPA",
    "FORE",
    "NCKL",
    "SPTO",
    "BHAT",
    "CLEO",
    "TOTL",
    "MGRO",
    "KAEF",
    "POWR",
    "BBLD",
]
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


def ensure_output_file(path: Path) -> None:
    """Ensure the CSV output file exists and has the expected header row."""

    if path.exists():
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(HEADER_ROW)


def store_quotes(csv_path: Path, quotes: Iterable[Quote]) -> None:
    """Store quotes in the CSV file."""

    rows = list(quotes)
    if not rows:
        raise RuntimeError("No quote data returned from Yahoo Finance.")

    now_utc = datetime.now(timezone.utc)
    if JAKARTA_TZ:
        jakarta_now = now_utc.astimezone(JAKARTA_TZ)
    else:  # pragma: no cover - fallback when zoneinfo missing
        jakarta_now = now_utc
    latest_update_label = f"Latest update {jakarta_now.strftime('%H:%M')}"

    ensure_output_file(csv_path)
    with csv_path.open("a", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        for quote in rows:
            writer.writerow(
                [
                    quote.symbol,
                    quote.short_name,
                    quote.price,
                    quote.currency,
                    quote.regular_market_time.isoformat()
                    if quote.regular_market_time
                    else None,
                    quote.open_price,
                    quote.day_high,
                    quote.day_low,
                    quote.previous_close,
                    now_utc.isoformat(),
                    latest_update_label,
                ]
            )


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
        "--output",
        type=Path,
        default=Path("data/idx_data.csv"),
        help="Path to the CSV file where data snapshots are stored.",
    )
    return parser.parse_args(argv)


def run_fetch_cycle(csv_path: Path, tickers: Sequence[str]) -> None:
    quotes = fetch_quotes(tickers)
    store_quotes(csv_path, quotes)
    print(f"Stored quotes for {len(quotes)} tickers at {datetime.now()}")


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])

    tickers = args.tickers
    interval = max(1, args.interval)
    output_path = args.output

    while True:
        try:
            run_fetch_cycle(output_path, tickers)
        except Exception as exc:
            print(f"Error during fetch cycle: {exc}", file=sys.stderr)
        if args.once:
            break
        time.sleep(interval)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
