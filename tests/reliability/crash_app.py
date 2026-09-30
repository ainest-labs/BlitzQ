"""Worker application run in a subprocess by the crash tests.

Each execution records its task argument in Redis lists (``<ns>:started`` and
``<ns>:done``) so the test can count lost and duplicated executions after the
worker process is killed.
"""

import asyncio
import os

import redis.asyncio as aioredis

from blitzq import Queue

NS = os.environ["BQ_NS"]
URL = os.environ["BQ_URL"]

app = Queue(
    redis_url=URL,
    mode=os.environ.get("BQ_MODE", "reliable"),  # type: ignore[arg-type]
    namespace=NS,
    visibility_timeout=float(os.environ.get("BQ_VIS", "2")),
)

_client: aioredis.Redis | None = None


def _r() -> aioredis.Redis:
    global _client
    if _client is None:
        _client = aioredis.Redis.from_url(URL)
    return _client


@app.task(name="crash.work")
async def work(i: int, sleep: float) -> None:
    await _r().rpush(f"{NS}:started", i)
    await asyncio.sleep(sleep)
    await _r().rpush(f"{NS}:done", i)
