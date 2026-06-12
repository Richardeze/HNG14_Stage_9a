import asyncio
import logging
import logging.config
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from app.database import init_db
from app.routes.jobs import router as jobs_router
from app.routes.dlq import router as dlq_router
from app.worker import worker_loop, load_pending_jobs, priority_update_loop
from app.scheduler import scheduler


logging.config.dictConfig({
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "structured": {
            "format": "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            "datefmt": "%Y-%m-%dT%H:%M:%S",
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "structured",
        }
    },
    "root": {
        "level": "INFO",
        "handlers": ["console"],
    },
})

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up scheduler application")
    await init_db()
    await load_pending_jobs()

    worker_task = asyncio.create_task(worker_loop())
    scheduler_task = asyncio.create_task(scheduler.start())
    priority_task = asyncio.create_task(priority_update_loop())

    logger.info("Worker and scheduler running")

    yield

    logger.info("Shutting down")
    worker_task.cancel()
    scheduler_task.cancel()
    priority_task.cancel()


app = FastAPI(
    title="Background Job Scheduler",
    description="A production-grade background job scheduler with priority queue, DAG workflows, and DLQ.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(jobs_router)
app.include_router(dlq_router)

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def serve_ui():
    return FileResponse("static/index.html")

from fastapi.responses import StreamingResponse
from app.database import AsyncSessionLocal
from app.models import Job
from sqlalchemy import select
import json

@app.get("/events")
async def sse_events():
    """
    Server-Sent Events endpoint.
    The UI connects here and receives live job status updates
    every 2 seconds without polling manually.
    """
    async def event_stream():
        while True:
            try:
                async with AsyncSessionLocal() as session:
                    result = await session.execute(
                        select(Job).order_by(Job.created_at.desc()).limit(50)
                    )
                    jobs = result.scalars().all()
                    data = [
                        {
                            "id": job.id,
                            "type": job.type,
                            "status": job.status,
                            "priority": job.priority,
                            "retry_count": job.retry_count,
                            "created_at": job.created_at.isoformat(),
                            "scheduled_at": job.scheduled_at.isoformat() if job.scheduled_at else None,
                            "completed_at": job.completed_at.isoformat() if job.completed_at else None,
                            "error": job.error,
                        }
                        for job in jobs
                    ]
                    yield f"data: {json.dumps(data)}\n\n"
            except Exception as e:
                logger.error("SSE error: %s", str(e))
                yield f"data: {json.dumps([])}\n\n"

            await asyncio.sleep(2)

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.get("/health")
async def health():
    return {"status": "ok", "queue_size": __import__('app.queue', fromlist=['priority_queue']).priority_queue.size()}