from __future__ import annotations

import logging
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from .config import get_settings

logger = logging.getLogger(__name__)


class SchedulerService:
    def __init__(self) -> None:
        self.scheduler: AsyncIOScheduler | None = None

    def start(self, job_func) -> None:
        settings = get_settings()
        tz = ZoneInfo(settings.timezone)
        scheduler = AsyncIOScheduler(timezone=tz)
        trigger = CronTrigger(minute=5, timezone=tz)
        scheduler.add_job(
            job_func,
            trigger,
            id="hourly-fetch",
            max_instances=1,
            misfire_grace_time=900,
            coalesce=True,
        )
        scheduler.start()
        self.scheduler = scheduler
        logger.info("Scheduler started", extra={"timezone": settings.timezone})

    def shutdown(self) -> None:
        if self.scheduler and self.scheduler.running:
            self.scheduler.shutdown(wait=False)
            logger.info("Scheduler stopped")


def setup_scheduler(app, job_func) -> SchedulerService | None:
    settings = get_settings()
    if not settings.enable_scheduler:
        logger.info("Scheduler disabled")
        return None

    service = SchedulerService()
    service.start(job_func)

    @app.on_event("shutdown")
    async def _shutdown_scheduler() -> None:
        service.shutdown()

    return service
