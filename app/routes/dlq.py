import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import DLQEntry, Job, JobLog, JobStatus
from app.queue import QueueItem, priority_queue

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/dlq", tags=["dlq"])


@router.get("/")
async def list_dlq(db: AsyncSession = Depends(get_db)):
    """
    List all jobs currently in the dead-letter queue.
    Shows error details so engineers can investigate.
    """
    result = await db.execute(
        select(DLQEntry).order_by(DLQEntry.moved_at.desc())
    )
    entries = result.scalars().all()

    return [
        {
            "id": entry.id,
            "job_id": entry.job_id,
            "type": entry.type,
            "payload": entry.payload,
            "priority": entry.priority,
            "error": entry.error,
            "retry_count": entry.retry_count,
            "created_at": entry.created_at.isoformat(),
            "moved_at": entry.moved_at.isoformat(),
        }
        for entry in entries
    ]


@router.get("/stats")
async def dlq_stats(db: AsyncSession = Depends(get_db)):
    """
    Returns DLQ count and whether threshold has been crossed.
    Threshold is 10 jobs — documented in worker.py DLQ_THRESHOLD.
    """
    result = await db.execute(select(DLQEntry))
    entries = result.scalars().all()
    count = len(entries)

    return {
        "count": count,
        "threshold": 10,
        "threshold_exceeded": count >= 10,
        "message": (
            "ALERT: DLQ threshold exceeded. Manual intervention required."
            if count >= 10
            else "DLQ within normal range."
        ),
    }


@router.post("/{dlq_id}/retry")
async def retry_dlq_job(dlq_id: str, db: AsyncSession = Depends(get_db)):
    """
    Manually retry a job from the DLQ.
    Creates a fresh job with reset retry count.
    If it fails again, it goes back to the DLQ.
    Removes the entry from DLQ on retry.
    """
    result = await db.execute(
        select(DLQEntry).where(DLQEntry.id == dlq_id)
    )
    entry = result.scalar_one_or_none()

    if not entry:
        raise HTTPException(status_code=404, detail="DLQ entry not found")

    new_job = Job(
        type=entry.type,
        payload=entry.payload,
        priority=entry.priority,
        status=JobStatus.pending,
        retry_count=0,
        effective_priority=float(entry.priority),
    )
    db.add(new_job)
    await db.commit()
    await db.refresh(new_job)

    log = JobLog(
        job_id=new_job.id,
        event="retry_attempted",
        message=f"Manual retry from DLQ. Original job_id={entry.job_id}",
        timestamp=datetime.now(timezone.utc),
    )
    db.add(log)

    await db.execute(
        delete(DLQEntry).where(DLQEntry.id == dlq_id)
    )
    await db.commit()

    await priority_queue.push(QueueItem(
        effective_priority=float(new_job.priority),
        scheduled_at=new_job.created_at,
        created_at=new_job.created_at,
        job_id=new_job.id,
        job_type=new_job.type,
        payload=new_job.payload,
    ))

    logger.info(
        "DLQ job manually retried: original_job_id=%s new_job_id=%s",
        entry.job_id,
        new_job.id,
    )

    return {
        "message": "Job requeued from DLQ",
        "original_job_id": entry.job_id,
        "new_job_id": new_job.id,
    }


@router.delete("/{dlq_id}")
async def delete_dlq_entry(dlq_id: str, db: AsyncSession = Depends(get_db)):
    """
    Permanently delete a DLQ entry.
    Use when you've investigated and decided the job should not be retried.
    """
    result = await db.execute(
        select(DLQEntry).where(DLQEntry.id == dlq_id)
    )
    entry = result.scalar_one_or_none()

    if not entry:
        raise HTTPException(status_code=404, detail="DLQ entry not found")

    await db.execute(
        delete(DLQEntry).where(DLQEntry.id == dlq_id)
    )
    await db.commit()

    logger.info("DLQ entry deleted: dlq_id=%s job_id=%s", dlq_id, entry.job_id)
    return {"message": "DLQ entry deleted", "dlq_id": dlq_id}