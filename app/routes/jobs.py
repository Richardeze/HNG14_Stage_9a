import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.dag import dag_validator
from app.database import get_db
from app.models import Job, JobLog, JobStatus
from app.queue import QueueItem, priority_queue, timing_wheel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/jobs", tags=["jobs"])

class JobCreate(BaseModel):
    type: str
    payload: dict = {}
    priority: int = 2
    scheduled_at: Optional[datetime] = None
    interval: Optional[str] = None
    is_recurring: bool = False
    dependencies: list[str] = []


class JobResponse(BaseModel):
    id: str
    type: str
    payload: dict
    priority: int
    status: str
    retry_count: int
    max_retries: int
    scheduled_at: Optional[datetime]
    interval: Optional[str]
    is_recurring: bool
    dependencies: list
    effective_priority: float
    created_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    error: Optional[str]

    class Config:
        from_attributes = True



@router.post("/", response_model=JobResponse, status_code=201)
async def create_job(data: JobCreate, db: AsyncSession = Depends(get_db)):
    """
    Create a new job and add it to the queue.
    Validates dependencies before creating.
    """
    valid_intervals = {"every_1_minute", "every_5_minutes", "every_1_hour", None}
    if data.interval not in valid_intervals:
        raise HTTPException(status_code=400, detail=f"Invalid interval: {data.interval}")

    if data.priority not in (1, 2, 3):
        raise HTTPException(status_code=400, detail="Priority must be 1 (high), 2 (medium), or 3 (low)")

    valid_types = {"send_email", "webhook_delivery", "log_processing"}
    if data.type not in valid_types:
        raise HTTPException(status_code=400, detail=f"Unknown job type: {data.type}")

    if data.dependencies:
        is_valid, error_msg = await dag_validator.validate(db, "new_job", data.dependencies)
        if not is_valid:
            raise HTTPException(status_code=400, detail=error_msg)

    job = Job(
        type=data.type,
        payload=data.payload,
        priority=data.priority,
        status=JobStatus.pending,
        scheduled_at=data.scheduled_at,
        interval=data.interval,
        is_recurring=data.is_recurring,
        dependencies=data.dependencies,
        effective_priority=float(data.priority),
    )
    db.add(job)
    await db.commit()
    await db.refresh(job)

    log = JobLog(
        job_id=job.id,
        event="job_created",
        message=f"Job created: type={job.type} priority={job.priority}",
        timestamp=datetime.now(timezone.utc),
    )
    db.add(log)
    await db.commit()

    logger.info("Job created: job_id=%s type=%s priority=%d", job.id, job.type, job.priority)

    now = datetime.now(timezone.utc)
    scheduled = job.scheduled_at

    if scheduled and scheduled > now:
        await timing_wheel.add(QueueItem(
            effective_priority=float(job.priority),
            scheduled_at=scheduled,
            created_at=job.created_at,
            job_id=job.id,
            job_type=job.type,
            payload=job.payload,
        ))
    elif not job.dependencies:
        await priority_queue.push(QueueItem(
            effective_priority=float(job.priority),
            scheduled_at=scheduled or job.created_at,
            created_at=job.created_at,
            job_id=job.id,
            job_type=job.type,
            payload=job.payload,
        ))

    return job


@router.get("/", response_model=list[JobResponse])
async def list_jobs(
    status: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """List all jobs, optionally filtered by status."""
    query = select(Job)
    if status:
        query = query.where(Job.status == status)
    query = query.order_by(Job.created_at.desc())

    result = await db.execute(query)
    return result.scalars().all()


@router.get("/stats")
async def get_stats(db: AsyncSession = Depends(get_db)):
    """
    Dashboard stats — count of jobs by status.
    Used by the UI dashboard.
    """
    statuses = ["pending", "processing", "completed", "failed", "cancelled"]
    stats = {}

    for status in statuses:
        result = await db.execute(
            select(Job).where(Job.status == status)
        )
        stats[status] = len(result.scalars().all())

    stats["queue_size"] = priority_queue.size()
    return stats


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(job_id: str, db: AsyncSession = Depends(get_db)):
    """Get a single job by ID."""
    result = await db.execute(select(Job).where(Job.id == job_id))
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.patch("/{job_id}/cancel")
async def cancel_job(job_id: str, db: AsyncSession = Depends(get_db)):
    """
    Cancel a job.
    If it's pending, remove from queue and mark cancelled.
    If it's already processing, we mark it cancelled in DB.
    The worker checks this flag before processing.
    """
    result = await db.execute(select(Job).where(Job.id == job_id))
    job = result.scalar_one_or_none()

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status in (JobStatus.completed, JobStatus.failed, JobStatus.cancelled):
        raise HTTPException(
            status_code=400,
            detail=f"Cannot cancel a job with status: {job.status}"
        )

    await db.execute(
        update(Job).where(Job.id == job_id).values(status=JobStatus.cancelled)
    )
    await db.commit()

    await priority_queue.remove(job_id)

    log = JobLog(
        job_id=job_id,
        event="job_cancelled",
        message="Job cancelled by user",
        timestamp=datetime.now(timezone.utc),
    )
    db.add(log)
    await db.commit()

    logger.info("Job cancelled: job_id=%s", job_id)
    return {"message": "Job cancelled", "job_id": job_id}


@router.get("/{job_id}/logs")
async def get_job_logs(job_id: str, db: AsyncSession = Depends(get_db)):
    """Get all log entries for a specific job."""
    result = await db.execute(
        select(JobLog)
        .where(JobLog.job_id == job_id)
        .order_by(JobLog.timestamp.asc())
    )
    logs = result.scalars().all()
    return [
        {
            "id": log.id,
            "job_id": log.job_id,
            "event": log.event,
            "message": log.message,
            "timestamp": log.timestamp.isoformat(),
        }
        for log in logs
    ]


@router.get("/{job_id}/dependencies")
async def get_dependency_chain(job_id: str, db: AsyncSession = Depends(get_db)):
    """Get the full dependency tree for a job."""
    chain = await dag_validator.get_dependency_chain(db, job_id)
    if not chain:
        raise HTTPException(status_code=404, detail="Job not found")
    return chain