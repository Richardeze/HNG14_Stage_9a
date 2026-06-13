import heapq
import asyncio
import logging
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class QueueItem:
    """
    Represents one item in the heap.
    The heap sorts by the fields in order — so priority first,
    then scheduled_at, then created_at, then job_id as a tiebreaker.
    
    We use effective_priority instead of raw priority so that
    starvation prevention works — low priority jobs gradually
    become more urgent the longer they wait.
    """
    effective_priority: float
    scheduled_at: datetime
    created_at: datetime
    job_id: str
    job_type: str = field(compare=False)
    payload: dict = field(compare=False, default_factory=dict)

    def __lt__(self, other: "QueueItem") -> bool:
        return (
            self.effective_priority,
            self.scheduled_at,
            self.created_at,
        ) < (
            other.effective_priority,
            other.scheduled_at,
            other.created_at,
        )

    def __le__(self, other: "QueueItem") -> bool:
        return self == other or self < other

    def __gt__(self, other: "QueueItem") -> bool:
        return not self <= other

    def __ge__(self, other: "QueueItem") -> bool:
        return not self < other

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, QueueItem):
            return False
        return self.job_id == other.job_id


class PriorityQueue:
    """
    Heap-based priority queue.
    
    heapq in Python is a min-heap — the smallest value always
    comes out first. Since priority 1 = high and 3 = low,
    smaller numbers naturally mean higher priority. Perfect.
    
    Operations:
    - push: O(log n) — insert a job and maintain heap order
    - pop:  O(log n) — remove and return the highest priority job
    - peek: O(1)     — look at the highest priority job without removing it
    """

    def __init__(self):
        self._heap: list[QueueItem] = []
        self._lock = asyncio.Lock()
        self._job_ids: set[str] = set() 

    async def push(self, item: QueueItem) -> bool:
        """
        Add a job to the queue.
        Returns False if the job is already in the queue (duplicate protection).
        Returns True if successfully added.
        """
        async with self._lock:
            if item.job_id in self._job_ids:
                logger.warning(
                    "Duplicate job rejected from queue: job_id=%s", item.job_id
                )
                return False
            heapq.heappush(self._heap, item)
            self._job_ids.add(item.job_id)
            logger.info(
                "Job pushed to queue: job_id=%s type=%s priority=%s",
                item.job_id,
                item.job_type,
                item.effective_priority,
            )
            return True

    async def pop(self) -> Optional[QueueItem]:
        """
        Remove and return the highest priority job.
        Returns None if the queue is empty.
        """
        async with self._lock:
            if not self._heap:
                return None
            item = heapq.heappop(self._heap)
            self._job_ids.discard(item.job_id)
            logger.info(
                "Job popped from queue: job_id=%s type=%s",
                item.job_id,
                item.job_type,
            )
            return item

    async def peek(self) -> Optional[QueueItem]:
        """
        Look at the next job without removing it.
        Useful for checking if the next job is scheduled for the future.
        """
        async with self._lock:
            if not self._heap:
                return None
            return self._heap[0]

    async def remove(self, job_id: str) -> bool:
        """
        Remove a specific job from the queue by ID.
        Used when a job is cancelled.
        Returns True if found and removed, False if not in queue.
        """
        async with self._lock:
            if job_id not in self._job_ids:
                return False
            self._heap = [item for item in self._heap if item.job_id != job_id]
            heapq.heapify(self._heap)  # rebuild heap after removal
            self._job_ids.discard(job_id)
            logger.info("Job removed from queue (cancelled): job_id=%s", job_id)
            return True

    async def update_priorities(self) -> None:
        """
        Starvation prevention — called periodically by the scheduler.
        
        Every job that has been waiting more than 5 minutes gets its
        effective_priority reduced by 0.1 per minute of waiting.
        This means a priority-3 (low) job waiting 10 minutes will have
        effective_priority = 3 - (10 * 0.1) = 2.0, same as a fresh medium job.
        After 20 minutes it becomes 1.0, same as a fresh high priority job.
        """
        async with self._lock:
            now = datetime.now(timezone.utc)
            for item in self._heap:
                wait_minutes = (now - item.created_at).total_seconds() / 60
                if wait_minutes > 5:
                    
                    item.effective_priority = max(
                        0.1,  
                        item.effective_priority - (wait_minutes * 0.1),
                    )
            heapq.heapify(self._heap) 
            logger.info("Queue priorities updated for starvation prevention")

    def size(self) -> int:
        return len(self._heap)

    def is_empty(self) -> bool:
        return len(self._heap) == 0


class TimingWheel:
    """
    Alternative scheduling algorithm — timing wheel.
    
    Imagine a clock face with slots. Each slot represents one second.
    Jobs scheduled for a future time are placed in the slot matching
    their scheduled second. Every tick, the wheel checks the current
    slot and moves any due jobs into the main priority queue.
    
    This is O(1) for both inserting and checking — much faster than
    scanning all pending jobs every second.
    
    We use 3600 slots = 1 hour wheel. Jobs scheduled more than
    1 hour ahead wrap around using modulo arithmetic.
    """

    def __init__(self, size: int = 3600):
        self.size = size
        self.slots: list[list[QueueItem]] = [[] for _ in range(size)]
        self.current_slot = 0
        self._lock = asyncio.Lock()

    def _get_slot(self, scheduled_at: datetime) -> int:
        """
        Calculate which slot a job belongs in based on its scheduled time.
        Uses modulo so jobs more than 1 hour away wrap around.
        """
        now = datetime.now(timezone.utc)
        seconds_until = max(0, int((scheduled_at - now).total_seconds()))
        return (self.current_slot + seconds_until) % self.size

    async def add(self, item: QueueItem) -> None:
        """Add a job to the timing wheel."""
        async with self._lock:
            slot = self._get_slot(item.scheduled_at)
            self.slots[slot].append(item)
            logger.info(
                "Job added to timing wheel: job_id=%s slot=%d",
                item.job_id,
                slot,
            )

    async def tick(self) -> list[QueueItem]:
        """
        Advance the wheel by one second.
        Returns all jobs that are now due to run.
        Called every second by the scheduler.
        """
        async with self._lock:
            due_jobs = self.slots[self.current_slot]
            self.slots[self.current_slot] = []  # clear the slot
            self.current_slot = (self.current_slot + 1) % self.size
            if due_jobs:
                logger.info(
                    "Timing wheel tick: slot=%d jobs_due=%d",
                    self.current_slot,
                    len(due_jobs),
                )
            return due_jobs


# Global instances used across the app
priority_queue = PriorityQueue()
timing_wheel = TimingWheel()