from __future__ import annotations

import logging
from datetime import datetime

import click

from app.core import configure_logging, get_settings
from app.data.repository import get_repository
from app.services.aggregator import backfill, fetch_and_compute

configure_logging()
logger = logging.getLogger(__name__)


@click.group()
def cli() -> None:
    """Management commands for IDX Watchlist."""


@cli.command()
def seed() -> None:
    """Initialize storage and ensure ticker list is readable."""
    settings = get_settings()
    repo = get_repository()
    tickers = settings.load_tickers()
    logger.info("Seed completed", extra={"storage": settings.storage, "tickers": len(tickers)})


@cli.command()
@click.option("--once", is_flag=True, help="Run a single refresh with default settings")
@click.option("--days", default=7, show_default=True, help="Days of history to refresh")
@click.option("--intraday/--no-intraday", default=True, help="Include intraday snapshots")
def fetch(once: bool, days: int, intraday: bool) -> None:
    """Fetch latest prices and compute indicators."""
    if once:
        fetch_and_compute(days=days, include_intraday=intraday)
    else:
        fetch_and_compute(days=days, include_intraday=intraday)


@cli.command()
@click.option("--days", default=120, show_default=True, help="Number of days to backfill")
def backfill_cmd(days: int) -> None:
    """Backfill historical data."""
    backfill(days)


if __name__ == "__main__":
    cli()
