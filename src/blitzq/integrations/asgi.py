"""Lifespan integration for ASGI frameworks (FastAPI, Starlette, Litestar, ...).

No framework imports are needed: the helper only opens and closes BlitzQ's
connections around the application's lifespan.

FastAPI / Starlette::

    from fastapi import FastAPI
    from blitzq.integrations.asgi import lifespan

    app = FastAPI(lifespan=lifespan(queue))

    @app.post("/orders/{order_id}")
    async def create(order_id: str):
        handle = await process_order.enqueue(order_id)
        return {"task_id": handle.id}

Wrap your own lifespan with ``lifespan(queue, inner=my_lifespan)``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any

from ..client import Queue

Lifespan = Callable[[Any], AbstractAsyncContextManager[Any]]


def lifespan(*queues: Queue, inner: Lifespan | None = None) -> Lifespan:
    """Return an ASGI lifespan function that connects and closes ``queues``.

    Connections are opened at startup (so a misconfigured Redis fails fast)
    and closed at shutdown after the inner lifespan has finished.
    """

    @asynccontextmanager
    async def _lifespan(app: Any) -> AsyncIterator[Any]:
        for q in queues:
            await q.connect()
        try:
            if inner is None:
                yield None
            else:
                async with inner(app) as state:
                    yield state
        finally:
            for q in queues:
                await q.close()

    return _lifespan
