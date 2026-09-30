"""Multiple queues, routing and per-queue concurrency.

    python examples/multiple_queues.py

Equivalent separate worker processes (scaled independently):

    blitzq worker examples.multiple_queues:queue --queues default,emails --concurrency 200
    blitzq worker examples.multiple_queues:queue --queues images --concurrency 8
"""

import asyncio
import time

from blitzq import Queue, Worker

queue = Queue(
    "default",
    "redis://localhost:6379/0",
    namespace="example-mq",
    # Glob routing rules; a queue= on the decorator or on .options() wins.
    routes={"*.send_*": "emails"},
)


@queue.task(queue="images")
async def make_thumbnail(image_id: int) -> int:
    await asyncio.sleep(0.2)  # slow work
    return image_id


@queue.task  # routed to "emails" by the rule above
async def send_welcome(user_id: int) -> int:
    return user_id


async def main() -> None:
    worker = Worker(
        queue,
        queues=["default", "emails", "images"],
        concurrency=50,
        queue_concurrency={"images": 2},  # images never take more than 2 slots
    )
    await worker.start()
    try:
        print("send_welcome routes to:", send_welcome.queue)
        await make_thumbnail.enqueue_many((i,) for i in range(20))  # 2 s of image backlog
        t0 = time.monotonic()
        handles = [await send_welcome.enqueue(i) for i in range(10)]
        for h in handles:
            await h.result(timeout=10)
        print(f"10 emails done in {time.monotonic() - t0:.3f}s despite the image backlog")
        for stats in await queue.queue_stats(["default", "emails", "images"]):
            print(f"  {stats.name:<8} waiting={stats.waiting} in_progress={stats.in_progress}")
    finally:
        worker.stop()
        await worker.shutdown()
        await queue.purge("images")
        await queue.close()


if __name__ == "__main__":
    asyncio.run(main())
