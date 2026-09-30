"""Background event loop that backs the synchronous API.

Synchronous callers (Flask, Django, scripts) submit coroutines to one private
event loop running in a daemon thread and block on the result. This avoids
creating a new event loop per call, never nests event loops, and keeps a
single pool of broker connections for all sync callers of a ``Queue``.

Calling the sync API from a thread that is already running an event loop
raises ``RuntimeError`` instead of blocking that loop.
"""

from __future__ import annotations

import asyncio
import os
import threading
from collections.abc import Callable, Coroutine
from typing import Any, TypeVar

T = TypeVar("T")


def ensure_no_running_loop(what: str) -> None:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return
    raise RuntimeError(
        f"{what} is a blocking call and cannot be used inside a running event loop; "
        "use the async API (await ...) instead"
    )


class Portal:
    def __init__(self, name: str = "blitzq-sync") -> None:
        self._name = name
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._pid: int | None = None

    def _ensure(self) -> asyncio.AbstractEventLoop:
        with self._lock:
            # Re-create after fork: threads do not survive into the child.
            if self._loop is None or self._pid != os.getpid() or not self._thread_alive():
                loop = asyncio.new_event_loop()
                ready = threading.Event()

                def run() -> None:
                    asyncio.set_event_loop(loop)
                    loop.call_soon(ready.set)
                    loop.run_forever()

                thread = threading.Thread(target=run, name=self._name, daemon=True)
                thread.start()
                ready.wait()
                self._loop, self._thread, self._pid = loop, thread, os.getpid()
            return self._loop

    def _thread_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def call(self, fn: Callable[..., Coroutine[Any, Any, T]], *args: Any, what: str = "this") -> T:
        ensure_no_running_loop(what)
        loop = self._ensure()
        return asyncio.run_coroutine_threadsafe(fn(*args), loop).result()

    def stop(self, cleanup: Callable[[], Coroutine[Any, Any, Any]] | None = None) -> None:
        with self._lock:
            loop, thread = self._loop, self._thread
            if loop is None or thread is None or self._pid != os.getpid():
                self._loop = self._thread = None
                return
            self._loop = self._thread = None
        try:
            if cleanup is not None and thread.is_alive():
                asyncio.run_coroutine_threadsafe(cleanup(), loop).result(timeout=5)
        finally:
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=5)
            if not thread.is_alive():
                loop.close()
