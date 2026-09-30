"""The maintenance loop retries quickly after a transient failure, not after
waiting a full `tick` - a fixed-interval retry there was silently adding
seconds to crash/connection-loss recovery in reliable mode (worker maintenance
is what renews leases and recovers abandoned messages).
"""

import time

from blitzq import Queue
from blitzq.broker import MemoryBroker
from blitzq.worker import Worker
from conftest import wait_for


async def test_maintenance_retries_quickly_after_a_failure(monkeypatch):
    app = Queue(broker=MemoryBroker())
    calls: list[float] = []
    failures_left = 3

    real_register = app.broker.register_worker

    async def flaky_register(*args, **kwargs):
        nonlocal failures_left
        calls.append(time.monotonic())
        if failures_left > 0:
            failures_left -= 1
            raise ConnectionError("simulated")
        return await real_register(*args, **kwargs)

    monkeypatch.setattr(app.broker, "register_worker", flaky_register)

    # stats_interval/heartbeat_interval both large: a fixed-tick retry would
    # take seconds; the fix must retry within a couple hundred ms regardless.
    w = Worker(app, stats_interval=5.0, heartbeat_interval=5.0)
    await w.start()
    try:
        await wait_for(lambda: failures_left == 0, timeout=3)
        # 3 failed calls should not have taken anywhere near 3 x 5s of ticks.
        assert calls[-1] - calls[0] < 2.0
    finally:
        w.stop()
        await w.shutdown()


async def test_maintenance_backoff_resets_after_success(monkeypatch):
    app = Queue(broker=MemoryBroker())
    calls: list[float] = []
    real_register = app.broker.register_worker
    state = {"fail_next": True}

    async def once_flaky(*args, **kwargs):
        calls.append(time.monotonic())
        if state["fail_next"]:
            state["fail_next"] = False
            raise ConnectionError("simulated")
        return await real_register(*args, **kwargs)

    monkeypatch.setattr(app.broker, "register_worker", once_flaky)
    w = Worker(app, stats_interval=0.2, heartbeat_interval=0.2)
    await w.start()
    try:
        await wait_for(lambda: len(calls) >= 3, timeout=3)
        # After the one failure, spacing should return to the normal tick
        # (~0.2s), not stay doubled from the backoff.
        assert 0.1 < calls[2] - calls[1] < 0.5
    finally:
        w.stop()
        await w.shutdown()
