from __future__ import annotations

import logging

import math
from typing import Callable

from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.api import router as api_router
from app.core import configure_logging, get_settings
from app.core.scheduler import setup_scheduler
from app.data.repository import get_repository
from app.services.aggregator import fetch_and_compute

configure_logging()
settings = get_settings()
repository = get_repository()
PAGE_SIZE = 50

app = FastAPI(title="IDX Watchlist", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router, prefix="/api", tags=["api"])
app.mount("/static", StaticFiles(directory="app/web/static"), name="static")

templates = Jinja2Templates(directory="app/web/templates")


@app.on_event("startup")
async def startup_event() -> None:
    logging.getLogger(__name__).info("Application startup")
    setup_scheduler(app, fetch_and_compute)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("index.html", {"request": request})


def _sort_key(field: str, direction: str) -> Callable[[dict], tuple]:
    def key(row: dict) -> tuple:
        value = row.get(field)
        if isinstance(value, bool):
            value = int(value)
        if isinstance(value, (int, float)) or value is None:
            normalized = float(value) if value is not None else 0.0
            return (value is None, normalized)
        return (value is None, value)

    return key


@app.get("/partials/summary", response_class=HTMLResponse)
async def summary_partial(
    request: Request,
    page: int = Query(1, ge=1),
    sort: str = Query("symbol"),
    direction: str = Query("asc"),
    search: str = Query(""),
) -> HTMLResponse:
    direction = direction.lower()
    if direction not in {"asc", "desc"}:
        direction = "asc"
    allowed_fields = {
        "symbol",
        "last_close",
        "pct_change_1d",
        "ma20",
        "ma50",
        "rsi14",
        "is_30d_high",
        "signal",
        "updated_wib",
    }
    if sort not in allowed_fields:
        sort = "symbol"
    data = repository.get_latest_summary(None)
    if search:
        search_upper = search.upper()
        data = [row for row in data if search_upper in row["symbol"].upper()]
    reverse = direction == "desc"
    data.sort(key=_sort_key(sort, direction), reverse=reverse)
    total = len(data)
    total_pages = max(1, math.ceil(total / PAGE_SIZE))
    page = min(page, total_pages)
    start = (page - 1) * PAGE_SIZE
    end = start + PAGE_SIZE
    page_rows = data[start:end]
    context = {
        "request": request,
        "rows": page_rows,
        "page": page,
        "total_pages": total_pages,
        "sort": sort,
        "direction": direction,
        "search": search,
        "total": total,
        "page_size": PAGE_SIZE,
    }
    return templates.TemplateResponse("partials/summary_table.html", context)
