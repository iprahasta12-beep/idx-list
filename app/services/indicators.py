from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Iterable, List

import numpy as np
import pandas as pd

from ..core.config import get_settings
from ..data.models import IndicatorRow

logger = logging.getLogger(__name__)


def ma(series: pd.Series, n: int) -> pd.Series:
    return series.rolling(window=n, min_periods=1).mean()


def rsi_wilder(series: pd.Series, n: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / n, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / n, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(0)


def rolling_high(series: pd.Series, n: int) -> pd.Series:
    return series.rolling(window=n, min_periods=1).max()


def compute_indicators(df_prices: pd.DataFrame) -> List[IndicatorRow]:
    if df_prices.empty:
        return []
    settings = get_settings()
    records: List[IndicatorRow] = []
    df_prices = df_prices.sort_values(["symbol", "ts_utc"]).copy()
    for symbol, df_symbol in df_prices.groupby("symbol", sort=False):
        close = df_symbol["close"].astype(float)
        ma20 = ma(close, 20)
        ma50 = ma(close, 50)
        rsi14 = rsi_wilder(close, 14)
        high_flag = rolling_high(close, settings.high_lookback)
        is_high = (close == high_flag).astype(int)
        recent_high = is_high.rolling(window=settings.high_within_days, min_periods=1).max()
        latest = df_symbol.iloc[-1]
        idx = latest.name
        latest_ma20 = ma20.loc[idx]
        latest_ma50 = ma50.loc[idx]
        latest_rsi = rsi14.loc[idx]
        latest_is_high = int(is_high.loc[idx])
        latest_recent_high = int(recent_high.loc[idx])
        signal = (
            latest["close"] > latest_ma20
            and latest["close"] > latest_ma50
            and (latest_ma20 is not None and latest_ma50 is not None and latest_ma20 > latest_ma50)
            and latest_rsi >= settings.rsi_min
            and latest_recent_high == 1
        )
        records.append(
            IndicatorRow(
                symbol=symbol,
                ts_utc=int(latest["ts_utc"]),
                ma20=float(latest_ma20) if pd.notna(latest_ma20) else None,
                ma50=float(latest_ma50) if pd.notna(latest_ma50) else None,
                rsi14=float(latest_rsi) if pd.notna(latest_rsi) else None,
                is_30d_high=int(latest_is_high),
                signal=int(bool(signal)),
                updated_at_utc=int(datetime.now(timezone.utc).timestamp()),
            )
        )
    return records
