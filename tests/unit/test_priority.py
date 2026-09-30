"""Task priority within a queue: physically separate broker sub-queues,
checked in order (high, normal, low), sharing the base queue's concurrency
budget. See PRIORITY_LEVELS in worker.py and docs/architecture.md.
"""

import asyncio

from blitzq import Queue
from blitzq.worker import physical_queue
from conftest import running, wait_for


def test_physical_queue_naming():
    assert physical_queue("default", "normal") == "default"
    assert physical_queue("default", "") == "default"
    assert physical_queue("default", "high") == "default:high"
    assert physical_queue("default", "low") == "default:low"


async def test_high_priority_enqueue_uses_a_separate_physical_queue(memory_app: Queue):
    @memory_app.task
    async def job():
        pass

    h_high = await job.options(priority="high").enqueue()
    h_low = await job.options(priority="low").enqueue()
    h_normal = await job.enqueue()
    assert h_high.queue == "default:high"
    assert h_low.queue == "default:low"
    assert h_normal.queue == "default"
    stats = {
        s.name: s.waiting
        for s in await memory_app.queue_stats(["default", "default:high", "default:low"])
    }
    assert stats == {"default": 1, "default:high": 1, "default:low": 1}


async def test_worker_drains_high_before_normal_before_low(memory_app: Queue):
    order: list[str] = []

    @memory_app.task
    async def job(tag: str):
        order.append(tag)

    for i in range(3):
        await job.options(priority="low").enqueue(f"low{i}")
    for i in range(3):
        await job.enqueue(f"normal{i}")
    for i in range(3):
        await job.options(priority="high").enqueue(f"high{i}")

    # concurrency=1 makes draining strictly serial, exposing the order.
    async with running(memory_app, concurrency=1):
        await wait_for(lambda: len(order) == 9)
    assert order == [
        "high0", "high1", "high2",
        "normal0", "normal1", "normal2",
        "low0", "low1", "low2",
    ]  # fmt: skip


async def test_priority_levels_share_one_concurrency_budget(memory_app: Queue):
    active = 0
    peak = 0
    done = 0

    @memory_app.task
    async def slow():
        nonlocal active, peak, done
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.05)
        active -= 1
        done += 1

    for i in range(5):
        await slow.options(priority="high").enqueue()
    for i in range(5):
        await slow.enqueue()
    for i in range(5):
        await slow.options(priority="low").enqueue()

    async with running(memory_app, concurrency=4):
        await wait_for(lambda: done == 15, timeout=10)
    # Never more than the worker's single concurrency limit, even though
    # three physical queues are being drained at once.
    assert peak == 4


async def test_decorator_level_default_priority(memory_app: Queue):
    @memory_app.task(priority="high")
    async def urgent():
        pass

    h = await urgent.enqueue()
    assert h.queue == "default:high"
    # Per-call override wins over the decorator default.
    h2 = await urgent.options(priority="low").enqueue()
    assert h2.queue == "default:low"


async def test_priority_message_still_delivered_without_backlog(memory_app: Queue):
    """A lone high-priority message with nothing else queued is not stranded."""
    got = []

    @memory_app.task
    async def job(tag):
        got.append(tag)

    async with running(memory_app, block_timeout=0.2):
        await job.options(priority="high").enqueue("only-one")
        await wait_for(lambda: got == ["only-one"], timeout=3)
