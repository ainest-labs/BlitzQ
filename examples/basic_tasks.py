"""Basic usage: define tasks, run a worker in-process, enqueue and wait for results.

    docker compose up -d redis
    python examples/basic_tasks.py

In production run the worker as its own process instead:

    blitzq worker examples.basic_tasks:queue
"""

import asyncio

from blitzq import Queue, RetryPolicy, Worker

queue = Queue(name="default", redis_url="redis://localhost:6379/0")


@queue.task(
    retries=3,
    retry_policy=RetryPolicy(initial_delay=1, max_delay=60, backoff=2, jitter=True),
)
async def process_order(order_id: str) -> dict:
    await asyncio.sleep(0.01)  # e.g. an HTTP call with an async client
    return {"order_id": order_id, "processed": True}


@queue.task
def resize_image(path: str, width: int) -> str:
    # Plain functions run on the worker's thread pool, so blocking I/O is fine.
    return f"{path}@{width}px"


async def main() -> None:
    worker = Worker(queue, concurrency=50)
    await worker.start()
    try:
        task = await process_order.enqueue("ORD-123")
        print("enqueued", task.id)
        result = await queue.get_result(task.id, timeout=10)
        print("result", result)

        handle = await resize_image.enqueue("cat.png", width=320)
        print("resized", await handle.result(timeout=10))
    finally:
        worker.stop()
        await worker.shutdown()
        await queue.close()


if __name__ == "__main__":
    asyncio.run(main())
