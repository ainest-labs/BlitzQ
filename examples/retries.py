"""Retries: automatic backoff, exception classification, explicit Retry, dead letters.

python examples/retries.py
"""

import asyncio

from blitzq import Queue, Retry, RetryPolicy, TaskFailed, Worker, current_task

queue = Queue("default", "redis://localhost:6379/0", namespace="example-retries")


class PaymentDeclined(Exception):
    """A permanent business error: retrying cannot help."""


@queue.task(
    retries=4,  # up to 5 attempts in total
    retry_policy=RetryPolicy(
        initial_delay=0.2,
        backoff=2,
        max_delay=5,
        jitter=True,
        retry_on=(ConnectionError, TimeoutError),
        dont_retry_on=(PaymentDeclined,),
    ),
    timeout=5,
)
async def charge(amount: int) -> str:
    ctx = current_task()
    assert ctx is not None
    print(f"  charge attempt {ctx.attempt}/{ctx.max_attempts}")
    if amount < 0:
        raise PaymentDeclined("negative amount")
    if ctx.attempt < 3:
        raise ConnectionError("payment gateway unavailable")  # transient -> retried
    return f"charged {amount}"


@queue.task(retries=10)
async def wait_for_export(export_id: str) -> str:
    ctx = current_task()
    assert ctx is not None
    if ctx.attempt < 3:
        raise Retry(delay=0.3, reason="export not ready yet")  # explicit, custom delay
    return f"export {export_id} ready after {ctx.retries} retries"


async def main() -> None:
    worker = Worker(queue, schedule_poll_interval=0.1)
    await worker.start()
    try:
        print(await (await charge.enqueue(100)).result(timeout=30))
        print(await (await wait_for_export.enqueue("e-1")).result(timeout=30))

        declined = await charge.enqueue(-5)
        try:
            await declined.result(timeout=30)
        except TaskFailed as exc:
            print(
                f"failed permanently: state={exc.state} error={exc.error_type}: {exc.error_message}"
            )
        for dead in await queue.dead_letters():
            print(f"dead letter {dead.id}: reason={dead.reason!r} attempts={dead.attempt}")
            # After fixing the cause: await queue.retry(dead.id)  (or `blitzq task retry ID`)
    finally:
        worker.stop()
        await worker.shutdown()
        await queue.broker.purge_dead_letters()
        await queue.close()


if __name__ == "__main__":
    asyncio.run(main())
