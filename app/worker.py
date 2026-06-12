import asyncio
import logging
import random
from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import AsyncSessionLocal
from app.handlers import run_handler
from app.models import DLQEntry, Job, JobLog, JobStatus
from app.queue import QueueItem, priority_queue

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
POLL_INTERVAL = 2  # seconds between polls
DLQ_THRESHOLD = 10  # alert when DLQ hits this number


async def log_event(session: AsyncSession, job_id: str, event: str, message: str = None):
    entry = JobLog(
        job_id=job_id,
        event=event,
        message=message,
        timestamp=datetime.now(timezone.utc),
    )
    session.add(entry)
    await session.commit()


def get_backoff_delay(retry_count: int) -> float:
    """
    Attempt 1 → ~1s
    Attempt 2 → ~5s
    Attempt 3 → ~25s
    Jitter adds randomness so multiple workers don't retry at the same time.
    """
    base_delays = [1, 5, 25]
    base = base_delays[min(retry_count, len(base_delays) - 1)]
    jitter = random.uniform(0, base * 0.3)
    return base + jitter


async def move_to_dlq(session: AsyncSession, job: Job, error: str):
    dlq_entry = DLQEntry(
        job_id=job.id,
        type=job.type,
        payload=job.payload,
        priority=job.priority,
        error=error,
        retry_count=job.retry_count,
    )
    session.add(dlq_entry)

    await session.execute(
        update(Job)
        .where(Job.id == job.id)
        .values(status=JobStatus.failed, error=error)
    )
    await session.commit()

    await log_event(session, job.id, "job_failed", f"Moved to DLQ after {job.retry_count} retries. Error: {error}")
    logger.error("Job moved to DLQ: job_id=%s error=%s", job.id, error)

    result = await session.execute(select(DLQEntry))
    dlq_count = len(result.scalars().all())
    if dlq_count >= DLQ_THRESHOLD:
        logger.critical(
            "DLQ ALERT: %d jobs in dead-letter queue. Threshold is %d. Immediate attention required.",
            dlq_count,
            DLQ_THRESHOLD,
        )


async def dependencies_met(session: AsyncSession, job: Job) -> bool:
    """
    A job only runs when all its dependencies have completed successfully.
    """
    if not job.dependencies:
        return True

    result = await session.execute(
        select(Job).where(Job.id.in_(job.dependencies))
    )
    dep_jobs = result.scalars().all()

    for dep in dep_jobs:
        if dep.status != JobStatus.completed:
            logger.info(
                "Job waiting on dependency: job_id=%s waiting_on=%s status=%s",
                job.id,
                dep.id,
                dep.status,
            )
            return False
    return True


async def schedule_next_run(session: AsyncSession, job: Job):
    """
    When a recurring job completes, create the next run automatically.
    """
    from datetime import timedelta

    intervals = {
        "every_1_minute": timedelta(minutes=1),
        "every_5_minutes": timedelta(minutes=5),
        "every_1_hour": timedelta(hours=1),
    }

    delta = intervals.get(job.interval)
    if not delta:
        return

    next_run_time = datetime.now(timezone.utc) + delta

    new_job = Job(
        type=job.type,
        payload=job.payload,
        priority=job.priority,
        status=JobStatus.pending,
        scheduled_at=next_run_time,
        interval=job.interval,
        is_recurring=True,
        dependencies=job.dependencies,
        effective_priority=float(job.priority),
    )
    session.add(new_job)
    await session.commit()

    await priority_queue.push(QueueItem(
        effective_priority=float(new_job.priority),
        scheduled_at=next_run_time,
        created_at=new_job.created_at,
        job_id=new_job.id,
        job_type=new_job.type,
        payload=new_job.payload,
    ))

    logger.info(
        "Next recurring run scheduled: job_type=%s next_run=%s",
        job.type,
        next_run_time.isoformat(),
    )


async def process_job(job_id: str):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Job).where(Job.id == job_id).with_for_update(skip_locked=True)
        )
        job = result.scalar_one_or_none()

        if not job:
            return 

        if job.status == JobStatus.cancelled:
            logger.info("Skipping cancelled job: job_id=%s", job.id)
            return

        if not await dependencies_met(session, job):
            await session.execute(
                update(Job).where(Job.id == job.id).values(status=JobStatus.pending)
            )
            await session.commit()
            await priority_queue.push(QueueItem(
                effective_priority=job.effective_priority,
                scheduled_at=job.scheduled_at or job.created_at,
                created_at=job.created_at,
                job_id=job.id,
                job_type=job.type,
                payload=job.payload,
            ))
            return

        await session.execute(
            update(Job).where(Job.id == job.id).values(
                status=JobStatus.processing,
                started_at=datetime.now(timezone.utc),
            )
        )
        await session.commit()
        await log_event(session, job.id, "job_started", f"Processing job type={job.type}")

        try:
            await run_handler(job.type, job.payload)

            await session.execute(
                update(Job).where(Job.id == job.id).values(
                    status=JobStatus.completed,
                    completed_at=datetime.now(timezone.utc),
                )
            )
            await session.commit()
            await log_event(session, job.id, "job_completed", "Job finished successfully")
            logger.info("Job completed: job_id=%s type=%s", job.id, job.type)

            if job.is_recurring and job.interval:
                await schedule_next_run(session, job)

        except Exception as e:
            error_msg = str(e)
            new_retry_count = job.retry_count + 1

            if new_retry_count >= MAX_RETRIES:
                await move_to_dlq(session, job, error_msg)
            else:
                delay = get_backoff_delay(new_retry_count)
                await session.execute(
                    update(Job).where(Job.id == job.id).values(
                        status=JobStatus.pending,
                        retry_count=new_retry_count,
                        error=error_msg,
                    )
                )
                await session.commit()
                await log_event(
                    session, job.id, "retry_attempted",
                    f"Attempt {new_retry_count} failed: {error_msg}. Retrying in {delay:.1f}s"
                )
                logger.warning(
                    "Job retry scheduled: job_id=%s attempt=%d delay=%.1fs error=%s",
                    job.id, new_retry_count, delay, error_msg
                )
                await asyncio.sleep(delay)

                await priority_queue.push(QueueItem(
                    effective_priority=job.effective_priority,
                    scheduled_at=job.scheduled_at or job.created_at,
                    created_at=job.created_at,
                    job_id=job.id,
                    job_type=job.type,
                    payload=job.payload,
                ))


async def worker_loop():
    """
    Runs forever. Every POLL_INTERVAL seconds it checks the queue,
    picks the highest priority due job, and processes it.
    Runs independently from the main FastAPI app.
    """
    logger.info("Worker started")

    while True:
        try:
            now = datetime.now(timezone.utc)
            item = await priority_queue.peek()

            if item is None:
                await load_pending_jobs()
                await asyncio.sleep(POLL_INTERVAL)
                continue

            if item.scheduled_at and item.scheduled_at > now:
                await asyncio.sleep(POLL_INTERVAL)
                continue

            item = await priority_queue.pop()
            if item:
                asyncio.create_task(process_job(item.job_id))

        except Exception as e:
            logger.error("Worker loop error: %s", str(e))
            await asyncio.sleep(POLL_INTERVAL)


async def load_pending_jobs():
    """
    On startup (or when queue is empty), pull all pending jobs
    from the database and push them into the heap.
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Job).where(Job.status == JobStatus.pending)
        )
        jobs = result.scalars().all()

        for job in jobs:
            await priority_queue.push(QueueItem(
                effective_priority=job.effective_priority,
                scheduled_at=job.scheduled_at or job.created_at,
                created_at=job.created_at,
                job_id=job.id,
                job_type=job.type,
                payload=job.payload,
            ))

        if jobs:
            logger.info("Loaded %d pending jobs into queue from DB", len(jobs))


async def priority_update_loop():
    """
    Runs every 60 seconds to boost waiting jobs (starvation prevention).
    """
    while True:
        await asyncio.sleep(60)
        await priority_queue.update_priorities()