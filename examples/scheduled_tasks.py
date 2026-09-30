"""Delayed tasks, ETAs, cancellation and periodic tasks.

    python examples/scheduled_tasks.py

In production, periodic tasks are dispatched by a scheduler process (you may
run several for availability; each occurrence is dispatched once):

    blitzq scheduler examples.scheduled_tasks:queue
    blitzq worker examples.scheduled_tasks:queue
"""

import asyncio
from datetime import UTC, datetime, timedelta

from blitzq import Every, Queue, Worker
from blitzq.scheduler import Scheduler

queue = Queue("default", "redis://localhost:6379/0", namespace="example-sched")


@queue.task
async def remind(text: str) -> str:
    return f"{datetime.now(UTC):%H:%M:%S.%f} {text}"


@queue.periodic(Every(1), name="heartbeat")  # every second, aligned to the epoch
async def heartbeat() -> None:
    print(f"  heartbeat at {datetime.now(UTC):%H:%M:%S.%f}")


@queue.periodic("0 9 * * 1-5", tz="Europe/Berlin", name="weekday-report")
async def weekday_report() -> None:
    print("  generating the 09:00 Berlin weekday report")


async def main() -> None:
    worker = Worker(queue, schedule_poll_interval=0.1)
    scheduler = Scheduler(queue, poll_interval=0.2)
    await worker.start()
    sched_task = asyncio.create_task(scheduler.run())
    try:
        print("now:", datetime.now(UTC).strftime("%H:%M:%S.%f"))
        h1 = await remind.options(delay=1.5).enqueue("delayed by 1.5s")
        h2 = await remind.options(eta=datetime.now(UTC) + timedelta(seconds=1)).enqueue("at an ETA")
        h3 = await remind.options(delay=60).enqueue("never runs")
        print("status of h3:", await h3.status())
        print("cancelled h3:", await h3.cancel())
        print(await h2.result(timeout=10))
        print(await h1.result(timeout=10))
        await asyncio.sleep(2.2)
    finally:
        scheduler.stop()
        await sched_task
        worker.stop()
        await worker.shutdown()
        await queue.close()


if __name__ == "__main__":
    asyncio.run(main())
