from __future__ import annotations

import logging
import sqlite3
from dataclasses import asdict
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd
from zoneinfo import ZoneInfo

from ..core.config import get_settings
from .models import IndicatorRow, PriceRow

logger = logging.getLogger(__name__)


class BaseRepository:
    def upsert_prices(self, rows: Iterable[PriceRow]) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    def upsert_indicators(self, rows: Iterable[IndicatorRow]) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    def get_latest_summary(self, target_date: Optional[date]) -> List[dict]:  # pragma: no cover - interface
        raise NotImplementedError

    def get_symbol(self, symbol: str, limit: int) -> List[dict]:  # pragma: no cover - interface
        raise NotImplementedError

    def load_prices(self, days: Optional[int] = None) -> pd.DataFrame:  # pragma: no cover - interface
        raise NotImplementedError


class SQLiteRepository(BaseRepository):
    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        schema_path = Path(__file__).parent / "schema.sql"
        with schema_path.open("r", encoding="utf-8") as f:
            self.conn.executescript(f.read())
        self.conn.commit()

    def upsert_prices(self, rows: Iterable[PriceRow]) -> None:
        data = [
            (r.symbol, r.ts_utc, r.open, r.high, r.low, r.close, r.volume)
            for r in rows
        ]
        if not data:
            return
        with self.conn:
            self.conn.executemany(
                """
                INSERT OR REPLACE INTO prices(symbol, ts_utc, open, high, low, close, volume)
                VALUES(?,?,?,?,?,?,?)
                """,
                data,
            )

    def upsert_indicators(self, rows: Iterable[IndicatorRow]) -> None:
        data = [
            (
                r.symbol,
                r.ts_utc,
                r.ma20,
                r.ma50,
                r.rsi14,
                r.is_30d_high,
                r.signal,
                r.updated_at_utc,
            )
            for r in rows
        ]
        if not data:
            return
        with self.conn:
            self.conn.executemany(
                """
                INSERT OR REPLACE INTO indicators(symbol, ts_utc, ma20, ma50, rsi14, is_30d_high, signal, updated_at_utc)
                VALUES(?,?,?,?,?,?,?,?)
                """,
                data,
            )

    def get_latest_summary(self, target_date: Optional[date]) -> List[dict]:
        params: dict[str, int | None] = {"end_utc": None}
        end_utc = None
        if target_date:
            settings = get_settings()
            tz = ZoneInfo(settings.timezone)
            end_dt = datetime.combine(target_date, datetime.max.time()).replace(tzinfo=tz)
            end_utc = int(end_dt.timestamp())
            params["end_utc"] = end_utc
        sql = """
        WITH latest_price AS (
            SELECT symbol, MAX(ts_utc) AS ts_utc
            FROM prices
            WHERE (:end_utc IS NULL OR ts_utc <= :end_utc)
            GROUP BY symbol
        ), latest_indicator AS (
            SELECT i.* FROM indicators i
            INNER JOIN (
                SELECT symbol, MAX(ts_utc) AS ts_utc
                FROM indicators
                WHERE (:end_utc IS NULL OR ts_utc <= :end_utc)
                GROUP BY symbol
            ) latest ON latest.symbol = i.symbol AND latest.ts_utc = i.ts_utc
        )
        SELECT p.symbol,
               p.close,
               p.ts_utc,
               li.ma20,
               li.ma50,
               li.rsi14,
               li.is_30d_high,
               li.signal,
               COALESCE(li.updated_at_utc, p.ts_utc) AS updated_at_utc
        FROM latest_price lp
        JOIN prices p ON p.symbol = lp.symbol AND p.ts_utc = lp.ts_utc
        LEFT JOIN latest_indicator li ON li.symbol = p.symbol
        ORDER BY p.symbol
        """
        cur = self.conn.execute(sql, params)
        rows = []
        settings = get_settings()
        tz = ZoneInfo(settings.timezone)
        for row in cur.fetchall():
            updated = datetime.fromtimestamp(row["updated_at_utc"], tz=ZoneInfo("UTC")).astimezone(tz)
            local_dt = datetime.fromtimestamp(row["ts_utc"], tz=ZoneInfo("UTC")).astimezone(tz)
            start_local = datetime.combine(local_dt.date(), datetime.min.time(), tz)
            start_utc = int(start_local.astimezone(ZoneInfo("UTC")).timestamp())
            prev_cur = self.conn.execute(
                "SELECT close FROM prices WHERE symbol = ? AND ts_utc < ? ORDER BY ts_utc DESC LIMIT 1",
                (row["symbol"], start_utc),
            )
            prev_row = prev_cur.fetchone()
            prev_close = prev_row[0] if prev_row else None
            pct = None
            if prev_close and prev_close != 0:
                pct = (row["close"] - prev_close) / prev_close * 100
            rows.append(
                {
                    "symbol": row["symbol"],
                    "last_close": row["close"],
                    "pct_change_1d": pct,
                    "ma20": row["ma20"],
                    "ma50": row["ma50"],
                    "rsi14": row["rsi14"],
                    "is_30d_high": bool(row["is_30d_high"]) if row["is_30d_high"] is not None else False,
                    "signal": bool(row["signal"]) if row["signal"] is not None else False,
                    "updated_wib": updated.strftime("%Y-%m-%d %H:%M"),
                }
            )
        return rows

    def get_symbol(self, symbol: str, limit: int) -> List[dict]:
        sql = """
        SELECT p.symbol, p.ts_utc, p.open, p.high, p.low, p.close, p.volume,
               i.ma20, i.ma50, i.rsi14, i.is_30d_high, i.signal
        FROM prices p
        LEFT JOIN indicators i ON i.symbol = p.symbol AND i.ts_utc = (
            SELECT MAX(ts_utc) FROM indicators i2 WHERE i2.symbol = p.symbol AND i2.ts_utc <= p.ts_utc
        )
        WHERE p.symbol = ?
        ORDER BY p.ts_utc DESC
        LIMIT ?
        """
        cur = self.conn.execute(sql, (symbol, limit))
        rows = []
        for row in cur.fetchall():
            rows.append({k: row[k] for k in row.keys()})
        return rows

    def load_prices(self, days: Optional[int] = None) -> pd.DataFrame:
        sql = "SELECT symbol, ts_utc, open, high, low, close, volume FROM prices"
        params: tuple = ()
        if days is not None:
            cutoff = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
            sql += " WHERE ts_utc >= ?"
            params = (cutoff,)
        df = pd.read_sql_query(sql, self.conn, params=params)
        return df


class CSVRepository(BaseRepository):
    def __init__(self, directory: Path) -> None:
        self.dir = directory
        self.dir.mkdir(parents=True, exist_ok=True)
        self.prices_path = self.dir / "prices.csv"
        self.indicators_path = self.dir / "indicators.csv"

    def _load_df(self, path: Path, columns: list[str]) -> pd.DataFrame:
        if not path.exists():
            return pd.DataFrame(columns=columns)
        return pd.read_csv(path)

    def upsert_prices(self, rows: Iterable[PriceRow]) -> None:
        records = [asdict(r) for r in rows]
        if not records:
            return
        df_new = pd.DataFrame(records)
        df = self._load_df(self.prices_path, list(df_new.columns))
        combined = pd.concat([df, df_new], ignore_index=True)
        combined.sort_values(["symbol", "ts_utc"], inplace=True)
        combined.drop_duplicates(subset=["symbol", "ts_utc"], keep="last", inplace=True)
        combined.to_csv(self.prices_path, index=False)

    def upsert_indicators(self, rows: Iterable[IndicatorRow]) -> None:
        records = [asdict(r) for r in rows]
        if not records:
            return
        df_new = pd.DataFrame(records)
        df = self._load_df(self.indicators_path, list(df_new.columns))
        combined = pd.concat([df, df_new], ignore_index=True)
        combined.sort_values(["symbol", "ts_utc"], inplace=True)
        combined.drop_duplicates(subset=["symbol", "ts_utc"], keep="last", inplace=True)
        combined.to_csv(self.indicators_path, index=False)

    def get_latest_summary(self, target_date: Optional[date]) -> List[dict]:
        if not self.prices_path.exists():
            return []
        df_prices = pd.read_csv(self.prices_path)
        if df_prices.empty:
            return []
        df_prices.sort_values(["symbol", "ts_utc"], inplace=True)
        if target_date:
            settings = get_settings()
            tz = ZoneInfo(settings.timezone)
            end_dt = datetime.combine(target_date, datetime.max.time()).replace(tzinfo=tz)
            end_utc = int(end_dt.timestamp())
            df_prices = df_prices[df_prices["ts_utc"] <= end_utc]
        latest = df_prices.groupby("symbol").tail(1).copy()
        latest.rename(columns={"close": "last_close"}, inplace=True)
        if self.indicators_path.exists():
            df_ind = pd.read_csv(self.indicators_path)
            if target_date:
                df_ind = df_ind[df_ind["ts_utc"] <= end_utc]
            df_ind.sort_values(["symbol", "ts_utc"], inplace=True)
            df_ind = df_ind.groupby("symbol").tail(1)
            latest = latest.merge(df_ind, on=["symbol", "ts_utc"], how="left")
        settings = get_settings()
        tz = ZoneInfo(settings.timezone)
        rows: List[dict] = []
        for _, row in latest.iterrows():
            symbol = row["symbol"]
            symbol_df = df_prices[df_prices["symbol"] == symbol]
            local_dt = datetime.fromtimestamp(row["ts_utc"], tz=ZoneInfo("UTC")).astimezone(tz)
            start_local = datetime.combine(local_dt.date(), datetime.min.time(), tz)
            start_utc = int(start_local.astimezone(ZoneInfo("UTC")).timestamp())
            prev_df = symbol_df[symbol_df["ts_utc"] < start_utc].tail(1)
            prev_close = prev_df["close"].iloc[0] if not prev_df.empty else None
            pct = None
            if pd.notna(prev_close) and prev_close:
                pct = (row["last_close"] - prev_close) / prev_close * 100
            updated = datetime.fromtimestamp(row["ts_utc"], tz=ZoneInfo("UTC")).astimezone(tz)
            rows.append(
                {
                    "symbol": row["symbol"],
                    "last_close": row["last_close"],
                    "pct_change_1d": pct,
                    "ma20": row.get("ma20"),
                    "ma50": row.get("ma50"),
                    "rsi14": row.get("rsi14"),
                    "is_30d_high": bool(row.get("is_30d_high", 0)),
                    "signal": bool(row.get("signal", 0)),
                    "updated_wib": updated.strftime("%Y-%m-%d %H:%M"),
                }
            )
        return rows

    def get_symbol(self, symbol: str, limit: int) -> List[dict]:
        if not self.prices_path.exists():
            return []
        df_prices = pd.read_csv(self.prices_path)
        df_prices = df_prices[df_prices["symbol"] == symbol].sort_values("ts_utc", ascending=False)
        df_prices = df_prices.head(limit)
        if self.indicators_path.exists():
            df_ind = pd.read_csv(self.indicators_path)
            df_ind = df_ind[df_ind["symbol"] == symbol]
            df_ind = df_ind.sort_values("ts_utc")
            df_prices = df_prices.merge(df_ind, on=["symbol", "ts_utc"], how="left")
        return df_prices.to_dict(orient="records")

    def load_prices(self, days: Optional[int] = None) -> pd.DataFrame:
        if not self.prices_path.exists():
            return pd.DataFrame(columns=["symbol", "ts_utc", "open", "high", "low", "close", "volume"])
        df = pd.read_csv(self.prices_path)
        if days is not None and not df.empty:
            cutoff = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
            df = df[df["ts_utc"] >= cutoff]
        return df


def get_repository() -> BaseRepository:
    settings = get_settings()
    if settings.storage == "sqlite":
        return SQLiteRepository(settings.db_path)
    return CSVRepository(settings.csv_dir)
