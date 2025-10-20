import json
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import List

from dotenv import load_dotenv

load_dotenv()


def _bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


@dataclass
class Settings:
    timezone: str = os.getenv("TZ", "Asia/Jakarta")
    storage: str = os.getenv("STORAGE", "sqlite")
    db_path: Path = Path(os.getenv("DB_PATH", "data/idx_quotes.db"))
    csv_dir: Path = Path(os.getenv("CSV_DIR", "data"))
    tickers_path: Path = Path(os.getenv("TICKERS_PATH", "config/tickers.json"))
    rsi_min: float = float(os.getenv("RSI_MIN", "55"))
    high_lookback: int = int(os.getenv("HIGH_LOOKBACK", "30"))
    high_within_days: int = int(os.getenv("HIGH_WITHIN_DAYS", "5"))
    fetch_interval_min: int = int(os.getenv("FETCH_INTERVAL_MIN", "60"))
    enable_scheduler: bool = _bool(os.getenv("ENABLE_SCHEDULER", "false"))

    def load_tickers(self) -> List[str]:
        if not self.tickers_path.exists():
            raise FileNotFoundError(f"Tickers file not found: {self.tickers_path}")
        with self.tickers_path.open("r", encoding="utf-8") as f:
            tickers = json.load(f)
        if not isinstance(tickers, list):
            raise ValueError("Tickers file must contain a JSON array")
        return [str(t).upper() for t in tickers]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    if settings.storage not in {"sqlite", "csv"}:
        raise ValueError("STORAGE must be either 'sqlite' or 'csv'")
    return settings
