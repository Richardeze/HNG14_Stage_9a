# Architecture Document — Background Job Scheduler

## Overview

A production-grade background job scheduler built with FastAPI and PostgreSQL.
Jobs are created via API or UI, queued in a heap-based priority queue,
processed by an independent worker, and tracked through their full lifecycle.

## System Components
┌─────────────┐     HTTP      ┌─────────────────┐

│   Browser   │ ←──────────── │   FastAPI App   │

│     UI      │ ──────────── →│   (main.py)     │

└─────────────┘     SSE       └────────┬────────┘

│

┌────────▼────────┐

│   PostgreSQL    │

│   Database      │

└────────┬────────┘

│

┌──────────────────┼──────────────┐

│                  │              │

┌────────▼──────┐  ┌───────▼──────┐  ┌───▼────────────┐

│    Worker     │  │  Scheduler   │  │ Priority Queue │

│  (worker.py)  │  │(scheduler.py)│  │  (queue.py)    │

└───────────────┘  └──────────────┘  └────────────────┘

## Job Lifecycle
CREATE → pending → processing → completed

→ failed (retry up to 3x) → DLQ

→ cancelled

## Heap-Based Priority Queue

Jobs are stored in a Python `heapq` min-heap. Each job is wrapped
in a `QueueItem` dataclass with fields compared in this order:

1. `effective_priority` — 1 (High), 2 (Medium), 3 (Low)
2. `scheduled_at` — earlier scheduled time comes first
3. `created_at` — older jobs come first as tiebreaker

**Complexity:**
- Push: O(log n)
- Pop: O(log n)
- Peek: O(1)

Scheduled jobs only enter the heap when their `scheduled_at` time
has passed. Recurring jobs re-enter the heap after each completion.

## Starvation Prevention

Jobs waiting more than **5 minutes** get their `effective_priority`
reduced by 0.1 per minute of waiting. This runs every 60 seconds.

Example:
- Low priority job (3.0) waits 10 minutes → effective priority 2.0
- Low priority job (3.0) waits 20 minutes → effective priority 1.0
- Floor is 0.1 — a job can never go below this

## Alternative Algorithm — Timing Wheel

A timing wheel with **3600 slots** (one per second = one hour) runs
alongside the heap. Jobs scheduled for a future time are placed in
the slot matching their scheduled second. Every second, the wheel
checks only the current slot — O(1) regardless of total job count.

Jobs released by the wheel are pushed into the main heap for
priority-ordered processing.

## Benchmark Results

| Operation | Jobs | Heap | Timing Wheel |
|-----------|------|------|--------------|
| Insert | 1,000 | 0.0141s | 0.0135s |
| Extract/Tick | 1,000 | 0.0072s | 0.0080s |
| Insert | 10,000 | 0.1660s | 0.1305s |
| Extract/Tick | 10,000 | 0.1119s | 0.0092s |
| Insert | 50,000 | 0.7713s | 0.7592s |
| Extract/Tick | 50,000 | 0.8401s | 0.0316s |

**Key finding:** At 50,000 jobs the timing wheel tick is **26x faster**
than heap extraction (0.0316s vs 0.8401s). This is because the wheel
checks one slot per tick regardless of job count, while the heap must
heapify after each pop.

**Tradeoffs:**
- Heap gives strict priority ordering — most urgent job always runs first
- Timing wheel is faster for scheduled dispatch but has no priority awareness
- Heap memory grows with jobs O(n); wheel is fixed at O(3600 slots)
- Jobs scheduled more than 1 hour ahead wrap around in the wheel

## DAG Workflow

Jobs can declare dependencies as a list of job IDs. A job will not
enter the heap until all its dependencies have status `completed`.

Cycle detection uses DFS — if walking the dependency tree ever
encounters the original job ID, the job is rejected at creation time.

Example:
Generate Report (no deps) → completed

↓

Upload File (depends on Generate Report) → completed

↓

Send Email (depends on Upload File) → runs last

## Retry Logic

Failed jobs retry automatically with exponential backoff and jitter:

| Attempt | Base Delay | With Jitter |
|---------|-----------|-------------|
| 1 | 1s | ~1.0-1.3s |
| 2 | 5s | ~5.0-6.5s |
| 3 | 25s | ~25.0-32.5s |

After 3 failed attempts the job moves to the Dead-Letter Queue.

## Dead-Letter Queue

Failed jobs land in the DLQ with full error details. Engineers can:
- View the error that caused failure
- Manually retry the job (resets retry count to 0)
- Delete the entry permanently

**DLQ Threshold: 10 jobs**
When the DLQ reaches 10 entries a CRITICAL alert is logged automatically.

## Duplicate Protection

The worker uses `SELECT ... FOR UPDATE SKIP LOCKED` when claiming a job.
This is a PostgreSQL-level lock that prevents two workers from picking
up the same job simultaneously, even under concurrent load.

## Cancellation Policy

- **Pending jobs** — removed from the heap immediately, marked cancelled
- **Processing jobs** — marked cancelled in the database. The worker
  checks the cancelled flag before processing and skips the job

## Live Updates

The UI connects to `/events` via Server-Sent Events (SSE). The server
streams the latest 50 jobs every 2 seconds. No page refresh needed.

## Logging

All events are logged in structured format:
2026-06-12T10:16:02 | INFO | app.worker | Job created: job_id=xxx type=send_email

Events logged:
- `job_created`
- `job_started`
- `retry_attempted`
- `job_failed`
- `job_cancelled`
- `job_completed`

## Tech Stack

| Layer | Technology |
|-------|-----------|
| API | FastAPI (Python 3.12) |
| Database | PostgreSQL + asyncpg |
| ORM | SQLAlchemy (async) |
| Queue | Custom heapq implementation |
| Server | Uvicorn |
| Frontend | Vanilla HTML/CSS/JS |
| Reverse Proxy | Nginx |