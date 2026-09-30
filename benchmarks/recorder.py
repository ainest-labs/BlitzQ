"""Per-execution timing records, shared verbatim by the BlitzQ and Celery task code.

Each task execution appends one line to an in-memory buffer; a daemon thread
per process ships batches to a *separate* stats Redis every 50 ms, so
accounting traffic never touches the broker being measured. Appending to a
list is atomic under the GIL, so this is safe from async tasks, thread pools,
prefork children and gevent greenlets.

Timestamps are ``time.perf_counter()`` values. On Linux this is
CLOCK_MONOTONIC (on Windows, QueryPerformanceCounter), which is system-wide,
so values taken in the producer and in worker processes on the same host are
directly comparable.

Record format (CSV line)::

    uid,outcome,attempt,t_enqueue,t_start,t_end,queue,pid
"""

from __future__ import annotations

import atexit
import os
import threading
import time

import redis

RECORDS_KEY = "bench:records"

_buf: list[str] = []
_pid: int | None = None
_lock = threading.Lock()
_client: redis.Redis | None = None


def _url() -> str:
    return os.environ.get("BLITZQ_BENCH_STATS_URL", "redis://localhost:6380/0")


def _flush() -> None:
    global _buf
    if not _buf:
        return
    batch, _buf = _buf, []
    assert _client is not None
    _client.rpush(RECORDS_KEY, *batch)


def _loop() -> None:
    while True:
        time.sleep(0.05)
        try:
            _flush()
        except Exception:  # stats Redis hiccup: keep records for next attempt
            time.sleep(0.5)


def _ensure() -> None:
    global _pid, _client, _buf
    if _pid == os.getpid():
        return
    with _lock:
        if _pid == os.getpid():
            return
        _buf = []  # never ship a parent's buffer from a forked child
        _client = redis.Redis.from_url(_url())
        threading.Thread(target=_loop, name="bench-recorder", daemon=True).start()
        atexit.register(_flush)
        _pid = os.getpid()


#: Crash workloads write each record synchronously before the task returns, so a
#: killed worker cannot take already-completed executions' records with it
#: (which would otherwise be miscounted as lost tasks). Costs one stats-Redis
#: round-trip per task for both systems equally.
SYNC = os.environ.get("BENCH_RECORD_SYNC") == "1"


def record(uid: int, outcome: str, attempt: int, t_enq: float, t_start: float, queue: str) -> None:
    _ensure()
    t_end = time.perf_counter()
    line = f"{uid},{outcome},{attempt},{t_enq!r},{t_start!r},{t_end!r},{queue},{os.getpid()}"
    if SYNC:
        assert _client is not None
        _client.rpush(RECORDS_KEY, line)
    else:
        _buf.append(line)
