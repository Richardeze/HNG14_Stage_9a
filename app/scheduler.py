import asyncio
import logging
from datetime import datetime, timezone

from sqlalchemy import select, update

from app.database import AsyncSessionLocal
from app.models import Job, JobStatus
from app.queue import QueueItem, priority_queue, timing_wheel

logger = logging.getLogger(__name__)


class Scheduler:
    """
    Responsible for two things:
    1. Moving scheduled jobs into the heap when their time is due
    2. Ticking the timing wheel every second

    The worker handles actually processing jobs.
    The scheduler just decides WHEN jobs enter the queue.
    """

    def __init__(self):
        self.running = False

    async def start(self):
        """Start all scheduler loops."""
        self.running = True
        logger.info("Scheduler started")

        await asyncio.gather(
            self.scheduled_job_loader(),
            self.timing_wheel_loop(),
            self.starvation_prevention_loop(),
        )

    async def scheduled_job_loader(self):
        """
        Runs every 5 seconds.
        Checks the database for any pending jobs whose scheduled_at
        time has now passed, and pushes them into the heap.

        This is the bridge between the database and the heap for
        scheduled jobs.
        """
        while self.running:
            try:
                async with AsyncSessionLocal() as session:
                    now = datetime.now(timezone.utc)

                    result = await session.execute(
                        select(Job).where(
                            Job.status == JobStatus.pending,
                            Job.scheduled_at <= now,
                        )
                    )
                    due_jobs = result.scalars().all()

                    for job in due_jobs:
                        pushed = await priority_queue.push(QueueItem(
                            effective_priority=job.effective_priority,
                            scheduled_at=job.scheduled_at or job.created_at,
                            created_at=job.created_at,
                            job_id=job.id,
                            job_type=job.type,
                            payload=job.payload,
                        ))
                        if pushed:
                            logger.info(
                                "Scheduled job due, added to heap: job_id=%s type=%s",
                                job.id,
                                job.type,
                            )

            except Exception as e:
                logger.error("Scheduled job loader error: %s", str(e))

            await asyncio.sleep(5)

    async def timing_wheel_loop(self):
        """
        Ticks the timing wheel every second.
        Any jobs that come out of the wheel get pushed into the main heap.

        This is the alternative scheduling algorithm running alongside
        the heap. Jobs added to the timing wheel get moved to the heap
        exactly when their time is due.
        """
        while self.running:
            try:
                due_jobs = await timing_wheel.tick()

                for item in due_jobs:
                    pushed = await priority_queue.push(item)
                    if pushed:
                        logger.info(
                            "Timing wheel released job to heap: job_id=%s",
                            item.job_id,
                        )

            except Exception as e:
                logger.error("Timing wheel loop error: %s", str(e))

            await asyncio.sleep(1)

    async def starvation_prevention_loop(self):
        """
        Runs every 60 seconds.
        Boosts the effective priority of jobs that have been
        waiting too long so low priority jobs never starve.
        Also syncs updated priorities back to the database.
        """
        while self.running:
            await asyncio.sleep(60)
            try:
                await priority_queue.update_priorities()

                async with AsyncSessionLocal() as session:
                    for item in priority_queue._heap:
                        await session.execute(
                            update(Job)
                            .where(Job.id == item.job_id)
                            .values(effective_priority=item.effective_priority)
                        )
                    await session.commit()

                logger.info("Starvation prevention: priorities synced to DB")

            except Exception as e:
                logger.error("Starvation prevention loop error: %s", str(e))

    def stop(self):
        self.running = False
        logger.info("Scheduler stopped")


scheduler = Scheduler()