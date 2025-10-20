from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


def to_timestamp(dt: datetime) -> int:
    return int(dt.timestamp())


@dataclass(frozen=True)
class PriceRow:
    symbol: str
    ts_utc: int
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(frozen=True)
class IndicatorRow:
    symbol: str
    ts_utc: int
    ma20: float | None
    ma50: float | None
    rsi14: float | None
    is_30d_high: int
    signal: int
    updated_at_utc: int


@dataclass(frozen=True)
class SummaryRow:
    symbol: str
    last_close: float | None
    pct_change_1d: float | None
    ma20: float | None
    ma50: float | None
    rsi14: float | None
    is_30d_high: int
    signal: int
    updated_wib: str
