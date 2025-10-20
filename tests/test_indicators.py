import math

import pandas as pd

from app.services.indicators import ma, rolling_high, rsi_wilder


def test_ma_simple():
    series = pd.Series([1, 2, 3, 4, 5], dtype=float)
    result = ma(series, 3)
    assert result.round(2).tolist() == [1.0, 1.5, 2.0, 3.0, 4.0]


def manual_rsi(values, period):
    gains = []
    losses = []
    for prev, cur in zip(values[:-1], values[1:]):
        change = cur - prev
        gains.append(max(change, 0))
        losses.append(max(-change, 0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    rs_values = [0.0] * period
    for gain, loss in zip(gains[period:], losses[period:]):
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        if avg_loss == 0:
            rs = math.inf
        else:
            rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        rs_values.append(rsi)
    rs_series = pd.Series([0.0] + rs_values, index=range(len(values)))
    return rs_series


def test_rsi_matches_manual():
    closes = pd.Series(
        [
            44.00,
            44.15,
            43.90,
            43.60,
            44.00,
            44.15,
            43.95,
            44.35,
            44.45,
            44.20,
            44.10,
            44.35,
            44.40,
            45.85,
            46.20,
        ],
        dtype=float,
    )
    ours = rsi_wilder(closes, 14).round(2)
    manual = manual_rsi(closes.tolist(), 14).round(2)
    assert ours.iloc[-1] == manual.iloc[-1]


def test_rolling_high_flag():
    series = pd.Series([1, 3, 2, 5, 4, 6], dtype=float)
    result = rolling_high(series, 3)
    assert result.tolist() == [1.0, 3.0, 3.0, 5.0, 5.0, 6.0]
