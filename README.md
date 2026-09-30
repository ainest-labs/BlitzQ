# BlitzQ

[![tests](https://github.com/ainest-labs/BlitzQ/actions/workflows/tests.yml/badge.svg)](https://github.com/ainest-labs/BlitzQ/actions/workflows/tests.yml)
[![PyPI](https://img.shields.io/pypi/v/blitzq.svg)](https://pypi.org/project/blitzq/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue)](docs/installation.md#requirements)

A high-performance, framework-agnostic task queue for Python, built on asyncio
and Redis. Part of [AiNest Labs](https://github.com/ainest-labs).

- **Async-native workers:** thousands of concurrent `async def` tasks per process;
  sync functions on a bounded thread pool; CPU-bound functions on a process pool.
- **Two explicit delivery modes:** *reliable* (Redis Streams, at-least-once, crash
  recovery) and *fast* (Redis lists, at-most-once, fewest round-trips).
- **Retries with backoff and jitter, dead letters, delayed tasks, periodic tasks,
  timeouts, cancellation, results, multiple queues with per-queue concurrency,
  task priority within a queue, cross-worker rate limiting.**
- **Framework-agnostic core** with optional FastAPI/Starlette, Django and Flask
  helpers.
- **Measured against Celery** with a reproducible benchmark suite; results below,
  including where BlitzQ is slower.

> Status: 1.0.0, published on PyPI.

## Contents

[Requirements](#requirements) · [Installation](#installation) ·
[Quick start](#quick-start) · [Tasks](#creating-and-executing-tasks) ·
[Workers](#starting-workers) · [Queues](#multiple-queues-and-routing) ·
[Retries](#retries) · [Scheduling](#scheduling) · [Results](#results-and-task-inspection) ·
[Frameworks](#framework-integrations) · [Configuration](#configuration) ·
[Reliability](#reliability-modes-and-delivery-guarantees) · [Tests](#running-tests) ·
[Benchmarks](#benchmarks) · [Limitations](#current-limitations) ·
[Roadmap](docs/roadmap.md)

## Requirements

- Python **3.12 or 3.13**
- Redis **7.0+** (tested with 7.4). Redis Cluster is not supported.
- Runtime dependencies: `redis` (redis-py ≥ 5), `msgspec`, `typer`

## Installation

```bash
pip install blitzq
pip install "blitzq[django]"      # optional: Django integration dependency
pip install "blitzq[flask]"       # optional: Flask integration dependency
pip install "blitzq[monitoring]"  # optional: Prometheus exporter
```

Or install from source:

```bash
git clone https://github.com/ainest-labs/BlitzQ.git && cd BlitzQ
pip install .
```

Start a local Redis with `docker compose up -d redis`. More in
[docs/installation.md](docs/installation.md).

## Quick start

```python
# app.py
import asyncio
from blitzq import Queue, RetryPolicy

queue = Queue(name="default", redis_url="redis://localhost:6379/0")

@queue.task(retries=3, retry_policy=RetryPolicy(initial_delay=1, max_delay=60, backoff=2, jitter=True))
async def process_order(order_id: str) -> dict:
    return {"order_id": order_id, "processed": True}

async def main():
    task = await process_order.enqueue("ORD-123")
    print(task.id)
    result = await queue.get_result(task.id, timeout=10)
    print(result)            # {'order_id': 'ORD-123', 'processed': True}

if __name__ == "__main__":
    asyncio.run(main())
```

```bash
blitzq worker app:queue          # terminal 1: executes tasks
python app.py                    # terminal 2: enqueues and waits for the result
```

Runnable examples live in [`examples/`](examples/): basic tasks, multiple queues,
retries, scheduling, FastAPI, Flask and Django.

## Creating and executing tasks

```python
@queue.task                                    # async: runs on the worker's event loop
async def fetch(url: str) -> int: ...

@queue.task(timeout=30)                        # sync: runs on the worker's thread pool
def resize(path: str, width: int) -> str: ...

@queue.task(executor="process")                # CPU-bound: runs on a process pool
def crunch(n: int) -> int: ...
```

| Call | Kind | Notes |
|---|---|---|
| `await task.enqueue(*args, **kwargs)` | async, non-blocking | returns a `TaskHandle` once Redis accepted the message |
| `task.enqueue_sync(*args, **kwargs)` | sync, **blocking** | for sync code; raises `RuntimeError` inside a running event loop |
| `await task.enqueue_many([(a,), (b,)])` / `enqueue_many_sync` | async / blocking | one pipelined round-trip for many calls |
| `task.options(queue=, delay=, eta=, task_id=, correlation_id=, headers=, timeout=)` | - | returns a bound task with `.enqueue`, `.enqueue_sync`, `.enqueue_many` |
| `await queue.send("name", args, kwargs)` / `send_sync` | async / blocking | enqueue by name without importing the task code |
| `task(*args)` | local call | runs the function directly in the current process |

**Serialization.** Arguments and results are encoded with msgspec (MessagePack
by default; `Serializer("json")` is also available). Supported: `None`, `bool`,
`int`, `float`, `str`, `bytes`, lists, tuples, dicts and `datetime`. `UUID`,
`Decimal`, dataclasses and `msgspec.Struct` are accepted but arrive as plain data
(`str`/`dict`). Anything else is rejected at enqueue time; `Serializer(enc_hook=...)`
adds conversions. Data from Redis is never unpickled. Pass identifiers, not ORM
objects, requests or sessions.

Inside a task, `blitzq.current_task()` returns a `TaskContext` with `id`,
`attempt`, `max_attempts`, `queue`, `correlation_id` and `headers`. Correlation ids
and headers (e.g. `traceparent`) propagate automatically to tasks enqueued from
within a task.

## Starting workers

Workers are separate processes. They never share memory, request context or
database sessions with your web processes.

```bash
blitzq worker app:queue --queues default,emails --concurrency 200
blitzq worker app:queue --queues images --concurrency 8 --processes 8
blitzq worker app:queue --queue-concurrency images=4 --threads 32 --log-format json --metrics-port 9100
```

`SIGTERM`/`SIGINT` stop fetching, wait up to `--shutdown-timeout` seconds for
running tasks, then requeue unfinished ones; a second signal forces the stop. In
Python you can also run `await Worker(queue, concurrency=50).run()`.

**Async concurrency, threads, processes:** `--concurrency` bounds how many tasks
run at once in one worker process. Async tasks are cheap coroutines (use
hundreds). Sync tasks occupy one of `--threads` pool threads. `executor="process"`
tasks occupy one of `--processes` pool processes. Scale CPU capacity by running
more worker processes - either yourself (multiple `blitzq worker` invocations,
e.g. under systemd/k8s replicas) or with `--workers N`, which forks and
supervises N of them from one command (a prefork-style pool: each child still
runs `--concurrency` async tasks internally, `SIGTERM`/`SIGINT` to the
supervisor drains all N, and a child that dies unexpectedly is restarted). See
[docs/performance_tuning.md](docs/performance_tuning.md).

## Multiple queues and routing

```python
queue = Queue("default", routes={"app.images.*": "images", "*.send_*": "emails"})

@queue.task(queue="reports")        # explicit queue beats routing rules
def build_report(report_id: int): ...

await build_report.options(queue="urgent").enqueue(7)   # per call beats both
```

Each subscribed queue has its own fetch loop, and fetching reserves capacity
first, so a flooded queue cannot starve a quiet one. Cap busy queues with
`--queue-concurrency`, and run separate worker pools per queue to scale them
independently (tested in `tests/integration/test_redis_basic.py`).

### Priority within a queue

```python
@queue.task(priority="high")                       # decorator default
async def urgent(): ...

await task.options(priority="low").enqueue(x)       # per-call override
```

`"high"`/`"normal"` (default)/`"low"`. Every worker checks a queue's levels in
that order on every batch, always - there's no separate config to remember, so
a `priority="high"` call is never silently unheard. All three levels share the
queue's one concurrency budget (priority reorders what runs next, it doesn't
add capacity), and crash recovery/at-least-once semantics apply identically to
every level. Full detail, including the one latency trade-off it makes (a
lone `low`-priority message can wait up to `block_timeout` longer when the
queue is otherwise idle): [docs/architecture.md#task-priority](docs/architecture.md#task-priority).

### Rate limiting

```python
@queue.task(rate_limit="10/s")          # or "100/m", "1000/hour"
async def call_downstream(): ...
```

Caps how often the task *starts*, enforced with a Redis-backed token bucket
shared across every worker process - `"10/s"` means 10/s total, not per
worker. A task over its limit is not executed and not counted as a retry or a
failure; it's rescheduled for when a slot should be free, so a rate-limited
backlog shows up as queue depth, not as a worker sleeping.
[docs/architecture.md#rate-limiting](docs/architecture.md#rate-limiting).

## Retries

```python
@queue.task(
    retries=4,                                  # up to 5 attempts
    retry_policy=RetryPolicy(initial_delay=1, backoff=2, max_delay=300, jitter=True,
                             retry_on=(ConnectionError,), dont_retry_on=(PermissionError,)),
)
async def call_api(): ...

@queue.task(retries=10)
async def poll_export(export_id: str):
    if not ready(export_id):
        raise Retry(delay=30)                   # explicit retry; still bounded by retries=
```

Delays are `min(max_delay, initial_delay * backoff**(n-1))`, drawn from
`[d/2, d]` with jitter. Retries are scheduled in Redis, not slept, so workers
keep processing other tasks. Retries keep the same task id and pass through normal
queue limits. After the last attempt the task is **dead-lettered**. Inspect and
replay with `blitzq dead-letter list` and `blitzq task retry ID`, or
`await queue.retry(id)`.

## Scheduling

```python
await remind.options(delay=60).enqueue("stand up")                 # relative
await remind.options(eta=datetime(2026, 10, 1, 9, tzinfo=UTC)).enqueue("meeting")
await handle.cancel()                                               # certain while still scheduled

@queue.periodic("0 9 * * 1-5", tz="Europe/Berlin", missed="run_once")
async def weekday_report(): ...

@queue.periodic(Every(300))                                         # every 5 min, epoch-aligned
async def cleanup(): ...
```

Periodic tasks are dispatched by `blitzq scheduler app:queue`. Run two or more
for availability: each occurrence is claimed atomically, so it is dispatched once.
After downtime, `missed="run_once"` (default) runs the latest missed occurrence,
`"run_all"` runs each (up to 100) and `"skip"` runs none.

## Results and task inspection

```python
result = await queue.get_result(task_id, timeout=10)   # raises TaskFailed / ResultTimeout
info = await queue.inspect(task_id)                    # TaskInfo: state, attempt, timings, error
state = await queue.status(task_id)                    # TaskState or None
```

States: `scheduled`, `queued`, `running`, `retrying`, `succeeded`, `failed`,
`dead_lettered`, `cancelled`. Final states and results are stored by default
(`store_results=True`, `result_ttl=86400`). `queued`/`running`/`retrying` are
recorded only with `track_state=True`, which costs one extra write per
transition. Disable result storage per task with `@queue.task(store_result=False)`.
Protect result access in your application: results may be sensitive.

```bash
blitzq task inspect <id> --app app:queue
blitzq queue stats --app app:queue
```

## Framework integrations

| Framework | Publish with | Helper |
|---|---|---|
| FastAPI, Starlette, Litestar | `await task.enqueue(...)` | `FastAPI(lifespan=blitzq.integrations.asgi.lifespan(queue))` |
| Django | `enqueue_on_commit(task, ...)` (after commit) or `enqueue_sync` | `blitzq.integrations.django.setup(queue)` (`pip install "blitzq[django]"`) |
| Flask | `task.enqueue_sync(...)` | `blitzq.integrations.flask.init_app(app, queue)` (`pip install "blitzq[flask]"`) |
| aiohttp, Sanic, Quart, scripts, notebooks | async or sync API | none needed |

Deploy web apps and workers as separate processes: web apps publish, workers
execute. Details in [docs/framework_integration.md](docs/framework_integration.md).

## Configuration

| `Queue(...)` parameter | Default | Meaning |
|---|---|---|
| `name` | `"default"` | default queue |
| `redis_url` | `$BLITZQ_REDIS_URL` or `redis://localhost:6379/0` | `rediss://` for TLS |
| `mode` | `"reliable"` | `"reliable"` or `"fast"` |
| `namespace` | `"blitzq"` | Redis key prefix |
| `store_results` / `result_ttl` | `True` / `86400` | result storage |
| `track_state` | `False` | record non-final states |
| `routes` | none | glob patterns → queue, or a callable |
| `default_retries` / `default_retry_policy` / `default_timeout` | `0` / `RetryPolicy()` / none | task defaults |
| `visibility_timeout` | `60` | reliable mode: seconds before an abandoned message is redelivered |
| `max_deliveries` | `5` | reliable mode: dead-letter after this many deliveries |
| `dead_letter_max` | `100000` | dead letters kept (oldest dropped) |
| `serializer` | `Serializer()` | `Serializer("json")`, `enc_hook=`, `max_message_size=` |
| `redis_options` | none | extra redis-py connection options (TLS certs, timeouts, ...) |

Worker options: `blitzq worker --help`. Operational guidance, including Redis
persistence, memory and security: [docs/operations.md](docs/operations.md).

## Reliability modes and delivery guarantees

| | reliable (default) | fast |
|---|---|---|
| Guarantee | **at-least-once** | **at-most-once** |
| Worker killed mid-task | redelivered after `visibility_timeout` | task lost |
| Graceful shutdown | unfinished tasks requeued | unfinished tasks requeued |
| Redis ops per task (no results) | fetch (batched) + ack (group-committed) | fetch (batched) |

Neither mode provides exactly-once execution. In reliable mode a task can run
twice, for example when a worker dies after finishing a task but before its ack
reached Redis, so **make side effects idempotent** (use `current_task().id` as an
idempotency key). Everything is only as durable as your Redis persistence. Full
details: [docs/delivery_guarantees.md](docs/delivery_guarantees.md).

## Running tests

```bash
pip install -e ".[dev]"
docker compose up -d redis
pytest -q                     # 212 tests: unit, Redis integration (both modes), crash/recovery
pytest -q tests/unit          # no Redis needed
ruff check src tests && mypy
```

## Benchmarks

The suite in [`benchmarks/`](benchmarks/) runs identical workloads against
BlitzQ, Celery 5.6 and Huey 3.4 on the same Redis and reports raw per-task data,
medians over 5 repetitions, variability and resource usage. Methodology, profiles
and the full results are in [docs/benchmarking.md](docs/benchmarking.md) and
[benchmarks/results/published/](benchmarks/results/published/).

### Results (Linux container, 12 CPUs, Redis 7.4, Celery 5.6.3, 5 repetitions, medians)

Profile **A** = equal settings: the same sync task code, 16 execution slots, one
worker process, one producer making one sync call per task. Profile **B** = each
system tuned: 4 worker and 4 producer processes each. All rows compare matched
delivery guarantees (at-least-once unless marked early-ack).

| Workload | Profile | BlitzQ tasks/s | Celery tasks/s | Ratio |
|---|---|---:|---:|---:|
| no-op, producer and workers concurrent | A | 2,604 | 855 | 3.0x |
| no-op, pre-loaded backlog (worker capacity) | A | 5,503 | 967 | 5.7x |
| no-op, pre-loaded backlog | B | 70,696 | 3,738 | 18.9x |
| 1 KiB payload | B | 59,270 | 3,129 | 18.9x |
| 100 KiB payload | B | 3,812 | 1,265 | 3.0x |
| 20 ms I/O task | A | 755 | 720 | 1.05x |
| 20 ms I/O task (BlitzQ async, Celery prefork) | B | 41,807 | 2,632 | 15.9x |
| retry-heavy (50% fail once) | B | 8,577 | 1,370 | 6.3x |
| result storage on | B | 49,521 | 2,748 | 18.0x |
| **CPU-bound** (threads vs prefork) | A | **73** | **486** | **0.15x** |
| CPU-bound (process pools, 8 each) | B | 472 | 445 | 1.06x |

Other measured results:

- **Where BlitzQ is worse.** CPU-bound work on BlitzQ's default thread executor is
  GIL-bound (0.15x); use `executor="process"`. Recovery after all Redis
  connections were dropped took 5.1 s vs 4.4 s for Celery. With tuned settings,
  delayed tasks started 13 ms late at p50 vs 0.6 ms for Celery, which keeps ETA
  tasks in worker memory. The scheduled workload also costs BlitzQ more Redis
  commands per task (11–16 vs 10) because of promoter polling.
- **Worker crash (SIGKILL at 30%, at-least-once).** 0 tasks lost in both systems.
  The last task completed 16.9 s after the kill with BlitzQ vs 101.6 s with Celery,
  whose Redis transport restores unacknowledged messages on a ~100 s cycle. BlitzQ
  re-executed 1 task across 5 runs.
- **Early-ack crash.** Both lost 15–16 of 2,000 tasks per crash, the tasks that
  were executing.
- **Redis connections dropped (at-least-once).** 0 lost in both. Duplicate
  executions: 0 for BlitzQ, 77 over 5 runs for Celery.
- **Footprint.** Peak worker RSS was 39 MB for BlitzQ (1 process, 16 threads) vs
  about 780 MB for Celery (prefork, 16 children). Worker CPU per task was 2.7–21x
  lower for BlitzQ on the no-op, payload, burst, retry and result-storage workloads.

These are single-machine numbers with trivial task bodies; real tasks that do
meaningful work shrink the differences. See the
[full report](benchmarks/results/published/suite/REPORT.md), which lists every
run, variability, settings and limitations.

### BlitzQ vs Huey (same machine, 5 repetitions, medians)

Huey has no at-least-once mode of its own - it pops a message before executing
it, like Celery's early-ack mode or BlitzQ's fast mode - so it is only compared
against those, never against BlitzQ's reliable mode.

| Workload | Profile | BlitzQ tasks/s | Huey tasks/s | Ratio |
|---|---|---:|---:|---:|
| no-op | A (equal, 16 threads each) | 2,847 | 1,499 | 1.9x |
| no-op | B (tuned, 4 processes each) | 126,462 | 6,024 | 21.0x |
| 20 ms I/O task | A | 767 | 762 | 1.01x |
| 20 ms I/O (BlitzQ async, Huey greenlet) | B | 62,120 | 8,657 | 7.2x |
| retry-heavy | A | 1,914 | 609 | 3.1x |
| **CPU-bound** (threads vs threads) | A | 73 | 70 | 1.0x |
| CPU-bound (process pools, 8 each) | B | 467 | 478 | **0.98x** |

- **Worker crash (early-ack).** Both lost 15–16 of 2,000 tasks per crash (the
  tasks executing at the kill), and both recovered in about 5 s.
- **Footprint.** Huey used 4 Redis commands per task on every workload (vs
  BlitzQ's 1–7, workload-dependent) but had similar or higher worker CPU per
  task than BlitzQ on most workloads.
- **Scheduled tasks** started 1.0–2.1 s late at p50 on a 2 s delay (Huey's
  default 1 s scheduler poll interval, left at its shipped default).
- A bug found while building this comparison: Huey's `greenlet` worker type
  does not monkey-patch the standard library itself, so a synchronous task
  using `time.sleep` blocked every greenlet on that worker instead of
  yielding, measuring 48 tasks/s on a 20 ms I/O workload until
  `gevent.monkey.patch_all()` was applied before Redis/Huey are imported
  (`benchmarks/apps/huey_app.py`); afterwards it measured 2,750 tasks/s on the
  same workload.

Full report: [benchmarks/results/published/huey_suite/REPORT.md](benchmarks/results/published/huey_suite/REPORT.md).

Reproduce:

```bash
docker compose up -d redis redis-stats
docker compose --profile bench run --rm bench python -m benchmarks.runner --config benchmarks/configs/suite.yaml
docker compose --profile bench run --rm bench python -m benchmarks.runner --config benchmarks/configs/huey_suite.yaml
python -m benchmarks.report --input benchmarks/results/<run-dir>
```

## Current limitations

- Redis only (plus an in-memory test broker). Redis Cluster is not supported.
  Valkey, Dragonfly and KeyDB are untested.
- No exactly-once execution. Reliable mode is at-least-once; fast mode loses
  in-flight tasks when a worker process dies.
- Thread and process tasks cannot be interrupted on timeout or cancellation.
- Cancelling a queued or running task is best effort (revocation synced about
  every second). Only scheduled tasks are cancelled with certainty.
- Without `track_state`, queued and running tasks have no inspectable record.
- Task chains/groups/chords and a web dashboard are not implemented. Priority
  and rate limiting within a queue are (see above).
- Delayed-task precision is bounded by the promoter poll interval (0.5 s default)
  for tasks scheduled earlier than anything already pending.

See [docs/roadmap.md](docs/roadmap.md) for why Redis Cluster support isn't
there yet, and what's already been fixed (`get_result()` used to poll; it's
now notification-driven).

## License

MIT, see [LICENSE](LICENSE).

---

Built by [AiNest Labs](https://github.com/ainest-labs). Issues and pull
requests welcome - see [CONTRIBUTING.md](CONTRIBUTING.md).
