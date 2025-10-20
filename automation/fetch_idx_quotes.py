#!/usr/bin/env python3
"""Automate fetching IDX ticker data from Yahoo Finance.

This module fetches quote data for a predefined set of IDX tickers from the
public Yahoo Finance quote API and stores each snapshot inside a local CSV
file. The task runs every hour on weekdays by default, but you can override the
interval or run it once for testing with command-line flags.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, List, Sequence
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python < 3.9 not expected
    ZoneInfo = None  # type: ignore


DEFAULT_INTERVAL_SECONDS = 60 * 60  # 1 hour
IDX_SUFFIX = ".JK"
YAHOO_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"
MAX_TICKERS_PER_REQUEST = 50
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

OUTPUT_HEADER = [
    "ticker",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "last_updated_at",
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
    volume: int | None

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
        volume = payload.get("regularMarketVolume")
        try:
            parsed_volume = int(volume) if volume is not None else None
        except (TypeError, ValueError):
            parsed_volume = None

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
            volume=parsed_volume,
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


def _chunked(sequence: Sequence[str], size: int) -> Iterator[Sequence[str]]:
    """Yield consecutive chunks from *sequence* with a maximum length."""

    for index in range(0, len(sequence), size):
        yield sequence[index : index + size]


def fetch_quotes(tickers: Sequence[str]) -> List[Quote]:
    """Fetch quote data from Yahoo Finance."""

    quotes: List[Quote] = []
    for batch in _chunked(list(tickers), MAX_TICKERS_PER_REQUEST):
        url = build_quote_url(batch)
        request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            with urlopen(request, timeout=30) as response:
                payload = json.load(response)
        except HTTPError as exc:  # pragma: no cover - relies on external API
            raise RuntimeError(
                f"Yahoo Finance returned HTTP {exc.code}: {exc.reason}"
            ) from exc
        except URLError as exc:  # pragma: no cover - relies on network conditions
            raise RuntimeError(f"Failed to fetch Yahoo Finance data: {exc}") from exc

        results = payload.get("quoteResponse", {}).get("result", [])
        quotes.extend(Quote.from_yahoo_payload(result) for result in results)

    return quotes


def prepare_output_path(path: Path) -> None:
    """Ensure the destination directory exists."""

    path.parent.mkdir(parents=True, exist_ok=True)


def _normalize_tickers(tickers: Sequence[str]) -> List[str]:
    normalized: List[str] = []
    seen: set[str] = set()
    for ticker in tickers:
        symbol = ticker_with_suffix(ticker)
        if symbol in seen:
            continue
        seen.add(symbol)
        normalized.append(symbol)
    return normalized


def _load_previous_rows(csv_path: Path) -> dict[str, dict[str, str]]:
    if not csv_path.exists():
        return {}

    try:
        with csv_path.open("r", newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            rows: dict[str, dict[str, str]] = {}
            for row in reader:
                symbol = (row.get("ticker") or "").strip().upper()
                if not symbol:
                    continue
                rows[symbol] = {key: (value or "") for key, value in row.items() if key}
            return rows
    except FileNotFoundError:
        return {}


def _to_float(value: object | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        number = float(value)
        if not math.isfinite(number):
            return None
        return number

    text = str(value).strip()
    if not text:
        return None

    try:
        return float(text.replace(",", ""))
    except ValueError:
        return None


def _format_price(value: object | None) -> str:
    number = _to_float(value)
    if number is None:
        return ""
    if number.is_integer():
        return str(int(number))
    return f"{number:.2f}"


def _format_volume(value: object | None) -> str:
    number = _to_float(value)
    if number is None:
        return ""
    return str(int(round(number)))


def _first_value(*values: object | None) -> object | None:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def store_quotes(csv_path: Path, tickers: Sequence[str], quotes: Iterable[Quote]) -> None:
    """Store quotes in the CSV file, replacing the previous snapshot."""

    normalized_tickers = _normalize_tickers(tickers)
    quote_map = {quote.symbol.upper(): quote for quote in quotes}
    previous_rows = _load_previous_rows(csv_path)

    if not quote_map and not previous_rows:
        raise RuntimeError("No quote data returned from Yahoo Finance.")
    now_utc = datetime.now(timezone.utc)
    jakarta_now = now_utc.astimezone(JAKARTA_TZ) if JAKARTA_TZ else now_utc
    prepare_output_path(csv_path)

    with csv_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(OUTPUT_HEADER)

        for ticker in normalized_tickers:
            symbol = ticker_with_suffix(ticker)
            quote = quote_map.get(symbol) or quote_map.get(symbol.upper())
            previous = previous_rows.get(symbol)

            if quote and quote.regular_market_time:
                trade_reference = quote.regular_market_time
            else:
                trade_reference = now_utc

            trade_local = (
                trade_reference.astimezone(JAKARTA_TZ)
                if JAKARTA_TZ
                else trade_reference
            )
            trade_date = trade_local.date().isoformat()
            if previous and previous.get("date") and (quote is None or quote.regular_market_time is None):
                trade_date = previous["date"].strip() or trade_date

            open_value = _format_price(
                _first_value(
                    quote.open_price if quote else None,
                    quote.price if quote else None,
                    quote.previous_close if quote else None,
                    previous.get("open") if previous else None,
                )
            )
            high_value = _format_price(
                _first_value(
                    quote.day_high if quote else None,
                    quote.price if quote else None,
                    previous.get("high") if previous else None,
                )
            )
            low_value = _format_price(
                _first_value(
                    quote.day_low if quote else None,
                    quote.price if quote else None,
                    previous.get("low") if previous else None,
                )
            )
            close_value = _format_price(
                _first_value(
                    quote.price if quote else None,
                    quote.previous_close if quote else None,
                    quote.open_price if quote else None,
                    previous.get("close") if previous else None,
                )
            )
            volume_value = _format_volume(
                _first_value(
                    quote.volume if quote else None,
                    previous.get("volume") if previous else None,
                )
            )

            writer.writerow(
                [
                    symbol,
                    trade_date,
                    open_value,
                    high_value,
                    low_value,
                    close_value,
                    volume_value,
                    jakarta_now.isoformat(),
                ]
            )

    return None


def should_run_now(moment: datetime) -> bool:
    """Return True when the automation should perform a fetch cycle."""

    # Weekday values: Monday=0, Sunday=6
    jakarta_moment = moment.astimezone(JAKARTA_TZ) if JAKARTA_TZ else moment
    return jakarta_moment.weekday() < 5


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
        default=Path("data/idx-data.csv"),
        help="Path to the CSV file where data snapshots are stored.",
    )
    return parser.parse_args(argv)


def run_fetch_cycle(csv_path: Path, tickers: Sequence[str]) -> None:
    normalized_tickers = _normalize_tickers(tickers)
    quotes = fetch_quotes(normalized_tickers)
    store_quotes(csv_path, normalized_tickers, quotes)
    print(
        f"Stored quotes for {len(quotes)} tickers at {datetime.now(timezone.utc).isoformat()}"
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])

    tickers = args.tickers
    interval = max(1, args.interval)
    output_path = args.output

    while True:
        now = datetime.now(timezone.utc)
        should_fetch = args.once or should_run_now(now)
        if should_fetch:
            try:
                run_fetch_cycle(output_path, tickers)
            except Exception as exc:
                print(f"Error during fetch cycle: {exc}", file=sys.stderr)
        else:
            print("Skipping fetch cycle outside of weekday trading hours.")

        if args.once:
            break

        try:
            time.sleep(interval)
        except KeyboardInterrupt:  # pragma: no cover - manual interruption
            break

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
