from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from ..core.config import get_settings
from ..data.repository import get_repository

router = APIRouter()
settings = get_settings()
repository = get_repository()


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/tickers")
def tickers() -> list[str]:
    return settings.load_tickers()


@router.get("/summary")
def summary(date: Optional[str] = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$")) -> list[dict]:
    target_date = None
    if date:
        target_date = datetime.strptime(date, "%Y-%m-%d").date()
    return repository.get_latest_summary(target_date)


@router.get("/symbol/{symbol}")
def symbol_detail(symbol: str, limit: int = Query(60, ge=1, le=500)) -> list[dict]:
    data = repository.get_symbol(symbol.upper(), limit)
    if not data:
        raise HTTPException(status_code=404, detail="Symbol not found or no data")
    return data
