import asyncio
import time
from datetime import datetime, timezone, timedelta
from app.queue import PriorityQueue, TimingWheel, QueueItem


def make_item(i: int) -> QueueItem:
    now = datetime.now(timezone.utc)
    return QueueItem(
        effective_priority=float((i % 3) + 1),
        scheduled_at=now + timedelta(seconds=i % 3600),
        created_at=now,
        job_id=f"job-{i}",
        job_type="send_email",
        payload={},
    )


async def benchmark_heap(n: int):
    queue = PriorityQueue()

    start = time.perf_counter()
    for i in range(n):
        await queue.push(make_item(i))
    insert_time = time.perf_counter() - start

    start = time.perf_counter()
    while not queue.is_empty():
        await queue.pop()
    pop_time = time.perf_counter() - start

    return insert_time, pop_time


async def benchmark_wheel(n: int):
    wheel = TimingWheel(size=3600)

    start = time.perf_counter()
    for i in range(n):
        await wheel.add(make_item(i))
    insert_time = time.perf_counter() - start

    start = time.perf_counter()
    for _ in range(3600):
        await wheel.tick()
    tick_time = time.perf_counter() - start

    return insert_time, tick_time


async def main():
    sizes = [1_000, 10_000, 50_000]

    print("=" * 60)
    print(f"{'Operation':<30} {'Heap':>10} {'TimingWheel':>15}")
    print("=" * 60)

    for n in sizes:
        heap_insert, heap_pop = await benchmark_heap(n)
        wheel_insert, wheel_tick = await benchmark_wheel(n)

        print(f"\n--- {n:,} jobs ---")
        print(f"{'Insert':<30} {heap_insert:.4f}s {wheel_insert:.4f}s")
        print(f"{'Extract/Tick':<30} {heap_pop:.4f}s {wheel_tick:.4f}s")

    print("\n" + "=" * 60)
    print("NOTES:")
    print("Heap insert:  O(log n) — slower as n grows")
    print("Wheel insert: O(1)     — constant regardless of n")
    print("Heap pop:     O(log n) — always gets highest priority first")
    print("Wheel tick:   O(1)     — checks one slot per second")
    print("Heap memory:  O(n)     — grows with jobs")
    print("Wheel memory: O(slots) — fixed at 3600 slots")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())