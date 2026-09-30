from __future__ import annotations

import asyncio
import os
import time
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any

import pytest

from blitzq import Queue, Worker
from blitzq.broker import MemoryBroker

REDIS_URL = os.environ.get("BLITZQ_TEST_REDIS_URL", "redis://localhost:6379/15")

_redis_ok: bool | None = None


def redis_available() -> bool:
    global _redis_ok
    if _redis_ok is None:
        import redis

        try:
            redis.Redis.from_url(REDIS_URL, socket_connect_timeout=1).ping()
            _redis_ok = True
        except Exception:
            _redis_ok = False
    return _redis_ok


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if redis_available():
        return
    if os.environ.get("BLITZQ_REQUIRE_REDIS"):
        raise pytest.UsageError(f"Redis is required but not reachable at {REDIS_URL}")
    skip = pytest.mark.skip(reason=f"Redis not reachable at {REDIS_URL}")
    for item in items:
        if "redis" in item.keywords:
            item.add_marker(skip)


def unique_ns() -> str:
    return f"bqtest-{uuid.uuid4().hex[:10]}"


@pytest.fixture
def memory_app() -> Queue:
    return Queue("default", broker=MemoryBroker())


@pytest.fixture(params=["fast", "reliable"])
def mode(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@pytest.fixture
async def redis_app_factory() -> AsyncIterator[Callable[..., Queue]]:
    created: list[Queue] = []

    def make(mode: str = "reliable", **kwargs: Any) -> Queue:
        kwargs.setdefault("namespace", unique_ns())
        q = Queue(kwargs.pop("name", "default"), REDIS_URL, mode=mode, **kwargs)  # type: ignore[arg-type]
        created.append(q)
        return q

    yield make
    for q in created:
        try:
            await q.broker.flush_namespace()  # type: ignore[attr-defined]
        except Exception:
            pass
        await q.close()
        q.close_sync()


@pytest.fixture
def redis_app(redis_app_factory: Callable[..., Queue], mode: str) -> Queue:
    return redis_app_factory(mode)


@asynccontextmanager
async def running(app: Queue, **kwargs: Any) -> AsyncIterator[Worker]:
    kwargs.setdefault("schedule_poll_interval", 0.05)
    kwargs.setdefault("block_timeout", 0.2)
    kwargs.setdefault("revocation_interval", 0.1)
    w = Worker(app, **kwargs)
    await w.start()
    try:
        yield w
    finally:
        w.stop()
        await w.shutdown()


async def wait_for(
    predicate: Callable[[], Any], timeout: float = 10.0, interval: float = 0.02
) -> None:
    deadline = time.monotonic() + timeout
    while True:
        res = predicate()
        if asyncio.iscoroutine(res):
            res = await res
        if res:
            return
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(interval)
