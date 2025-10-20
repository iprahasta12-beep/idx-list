"""Utility helpers for IDX automation scripts."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Iterable, List, Sequence

import pandas as pd

DEFAULT_CONFIG = {
    "data_dir": "data",
    "tickers_file": "data/tickers.csv",
    "quotes_file": "data/quotes.csv",
    "indicators_file": "data/indicators.csv",
    "snapshots_dir": "data/snapshots",
    "log_file": "logs/automation.log",
    "snapshot_format": "%Y%m%d_%H%M",
    "timezone": "Asia/Jakarta",
    "quote_period": "1d",
    "quote_interval": "1h",
    "indicator_period": "120d",
    "indicator_interval": "1d",
    "indicator_ma_short": 20,
    "indicator_ma_long": 50,
    "indicator_rsi_period": 14,
    "indicator_max_window": 30,
}


def load_config(path: str | Path = "config/settings.json") -> dict:
    """Load configuration JSON, falling back to defaults when missing."""

    config_path = Path(path)
    if not config_path.exists():
        return DEFAULT_CONFIG.copy()

    with config_path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)

    merged = DEFAULT_CONFIG.copy()
    merged.update(data)
    return merged


def ensure_directories(paths: Sequence[str | Path]) -> None:
    for path in paths:
        Path(path).mkdir(parents=True, exist_ok=True)


def ensure_csv(path: str | Path, columns: Sequence[str]) -> Path:
    csv_path = Path(path)
    if not csv_path.exists():
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=list(columns)).to_csv(csv_path, index=False)
    return csv_path


def load_tickers(tickers_file: str | Path) -> List[str]:
    df = pd.read_csv(tickers_file)
    column = df.columns[0]
    tickers: Iterable[str] = df[column].dropna().astype(str).str.strip()
    return [t for t in tickers if t]


def setup_logger(name: str, log_file: str | Path) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger


def timestamp_now(fmt: str, timezone: str | None = None) -> str:
    from datetime import datetime
    from zoneinfo import ZoneInfo

    tz = ZoneInfo(timezone) if timezone else ZoneInfo("UTC")
    return datetime.now(tz=tz).strftime(fmt)
