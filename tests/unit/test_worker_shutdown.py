"""Shutdown edge cases and task bookkeeping (in-memory broker)."""

import asyncio
import gc

from blitzq import Queue, Worker
from conftest import running, wait_for


async def test_message_held_for_a_slot_is_requeued_at_shutdown(memory_app: Queue):
    """A fetcher holding a received message while waiting for a free slot must not drop it.

    Setup: global concurrency 1, queues "a" and "b". The fetcher for "b" is
    blocked on its empty queue (a global slot was free when it started
    waiting); then a long task on "a" takes the only global slot; then a
    message arrives on "b" and is received, but must wait for a slot.
    """
    started = asyncio.Event()
    ran = []

    @memory_app.task(queue="a")
    async def blocker():
        started.set()
        await asyncio.sleep(30)

    @memory_app.task(queue="b")
    async def second():
        ran.append(1)

    w = Worker(memory_app, queues=["a", "b"], concurrency=1, block_timeout=5, shutdown_timeout=0.2)
    await w.start()
    await asyncio.sleep(0.1)  # both fetchers now block on their empty queues
    await blocker.enqueue()
    await asyncio.wait_for(started.wait(), 5)
    await second.enqueue()  # received by b's blocked fetch; no global slot free
    await asyncio.sleep(0.2)
    assert (await memory_app.queue_stats(["b"]))[0].waiting == 0  # held in worker memory
    w.stop()
    await w.shutdown()
    stats = {s.name: s.waiting for s in await memory_app.queue_stats(["a", "b"])}
    # The held message and the cancelled running task are both back in their queues.
    assert stats == {"a": 1, "b": 1}
    assert ran == []


async def test_inflight_tasks_survive_garbage_collection(memory_app: Queue):
    done = []

    @memory_app.task
    async def work(i):
        await asyncio.sleep(0.05)
        gc.collect()
        done.append(i)

    async with running(memory_app, concurrency=50) as w:
        await work.enqueue_many((i,) for i in range(100))
        await wait_for(lambda: len(done) == 100)
        await wait_for(lambda: not w._inflight)
    assert sorted(done) == list(range(100))


async def test_duplicate_ids_do_not_clobber_running_registry(memory_app: Queue):
    """Two deliveries of one task id: finishing the first must not unregister the second."""
    gates = [asyncio.Event(), asyncio.Event()]
    started = []

    @memory_app.task
    async def job(i):
        started.append(i)
        await gates[i].wait()

    async with running(memory_app, concurrency=4) as w:
        await job.options(task_id="same").enqueue(0)
        await wait_for(lambda: started == [0])
        await job.options(task_id="same").enqueue(1)
        await wait_for(lambda: started == [0, 1])
        gates[0].set()
        await wait_for(lambda: w.global_limit.used == 1)
        assert "same" in w._running  # the second execution is still tracked
        gates[1].set()
        await wait_for(lambda: w.global_limit.used == 0)
        assert w._running == {}


def test_heartbeat_must_be_well_within_visibility_timeout():
    import pytest

    from blitzq.broker import MemoryBroker

    app = Queue(broker=MemoryBroker(), visibility_timeout=0.6)
    assert Worker(app).heartbeat_interval == pytest.approx(0.2)
    with pytest.raises(ValueError, match="half of the visibility timeout"):
        Worker(app, heartbeat_interval=0.3)
