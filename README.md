# Background Job Scheduler

A production-grade background job scheduler built with FastAPI and PostgreSQL.

## Features

- **Priority Queue** — Heap-based scheduler that orders jobs by priority, scheduled time, and creation time
- **DAG Workflows** — Jobs can depend on other jobs. A job will not run until all its dependencies complete
- **Recurring Jobs** — Jobs reschedule themselves automatically after completion
- **Scheduled Jobs** — Jobs with a future `scheduled_at` time wait until that time before running
- **Dead-Letter Queue** — Failed jobs after 3 retries move to DLQ for manual inspection and retry
- **Starvation Prevention** — Low priority jobs get boosted after waiting 5+ minutes
- **Retry with Backoff** — Failed jobs retry up to 3 times with jitter (1s, 5s, 25s)
- **Duplicate Protection** — `SELECT FOR UPDATE SKIP LOCKED` prevents two workers picking the same job
- **Live UI** — Dashboard updates in real time via Server-Sent Events
- **Alternative Algorithm** — Timing wheel runs alongside the heap for O(1) scheduled job dispatch
- **Structured Logging** — Every significant event is logged in structured format

## Tech Stack

- **Backend** — FastAPI, Python 3.12
- **Database** — PostgreSQL with async SQLAlchemy
- **Queue** — Custom heap-based priority queue + timing wheel
- **Frontend** — Vanilla HTML, CSS, JavaScript
- **Server** — Uvicorn

## Project Structure
app/

├── main.py          # FastAPI app, startup, SSE endpoint

├── models.py        # SQLAlchemy models (Job, DLQEntry, JobLog)

├── database.py      # Database connection and session

├── queue.py         # Min-heap priority queue + timing wheel

├── worker.py        # Background worker loop

├── scheduler.py     # Scheduled job loader + starvation prevention

├── handlers.py      # Job handlers (email, webhook, log processing)

├── dag.py           # DAG validator and dependency resolver

├── routes/

│   ├── jobs.py      # Job CRUD endpoints

│   └── dlq.py       # DLQ endpoints

static/

├── index.html       # UI

├── style.css        # Styles

└── app.js           # Frontend logic

## Setup

**1. Clone the repo**
```bash
git clone https://github.com/YOURUSERNAME/YOURREPONAME.git
cd YOURREPONAME
```

**2. Create virtual environment**
```bash
python3 -m venv venv
source venv/bin/activate
```

**3. Install dependencies**
```bash
pip install -r requirements.txt
```

**4. Create PostgreSQL database**
```bash
psql -U postgres
CREATE DATABASE scheduler_db;
ALTER USER postgres PASSWORD 'yourpassword';
\q
```

**5. Create `.env` file**
DATABASE_URL=postgresql+asyncpg://postgres:yourpassword@localhost:5432/scheduler_db

**6. Run the app**
```bash
uvicorn app.main:app --reload
```

**7. Open the UI**
http://127.0.0.1:8000/

**API Docs**
http://127.0.0.1:8000/docs

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/jobs/` | Create a new job |
| GET | `/jobs/` | List all jobs |
| GET | `/jobs/stats` | Dashboard stats |
| GET | `/jobs/{id}` | Get a single job |
| PATCH | `/jobs/{id}/cancel` | Cancel a job |
| GET | `/jobs/{id}/logs` | Get job logs |
| GET | `/jobs/{id}/dependencies` | Get dependency chain |
| GET | `/dlq/` | List DLQ entries |
| GET | `/dlq/stats` | DLQ stats |
| POST | `/dlq/{id}/retry` | Retry a DLQ job |
| DELETE | `/dlq/{id}` | Delete a DLQ entry |
| GET | `/events` | SSE live updates |
| GET | `/health` | Health check |

## Job Types

| Type | Required Payload Fields |
|------|------------------------|
| `send_email` | `to`, `subject`, `body` |
| `webhook_delivery` | `url`, `data` |
| `log_processing` | `log_entry`, `severity` |

## Priority Levels

| Value | Level |
|-------|-------|
| 1 | High |
| 2 | Medium |
| 3 | Low |

## Recurring Intervals

- `every_1_minute`
- `every_5_minutes`
- `every_1_hour`

## How the Heap Works

Jobs are stored in a min-heap ordered by:
1. `effective_priority` — starts as raw priority (1/2/3), decreases over time for starvation prevention
2. `scheduled_at` — earlier scheduled time comes first
3. `created_at` — older jobs come first as tiebreaker

Operations are O(log n) for push and pop, O(1) for peek.

## Starvation Prevention

Jobs waiting more than 5 minutes get their `effective_priority` reduced by 0.1 per minute. A Low priority job (3.0) waiting 20 minutes reaches effective priority 1.0, same as a fresh High priority job.

## Dead-Letter Queue

Jobs that fail 3 times move to the DLQ automatically. The DLQ threshold is **10 jobs**. When exceeded, a CRITICAL alert is logged. Engineers can manually retry or delete entries from the UI.

## Cancellation Policy

- **Pending jobs** — removed from queue immediately, marked cancelled
- **Processing jobs** — marked cancelled in the database. The worker checks this flag before processing and skips the job if cancelled