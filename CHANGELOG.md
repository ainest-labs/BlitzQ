# Changelog

All notable changes to this project are documented here. The project follows
[Semantic Versioning](https://semver.org/).

## [1.1.0] - 2026-09-30

### Added

- `blitzq worker --workers N`: forks and supervises N worker processes from
  one command (a prefork-style pool), each still running its own
  `--concurrency` async tasks. `SIGTERM`/`SIGINT` to the supervisor drains
  and stops all N; a child that exits unexpectedly is restarted. Uses a
  `multiprocessing.Event` rather than forwarding OS signals to children,
  because `os.kill(pid, SIGTERM)` doesn't deliver a catchable signal on
  Windows (it hard-kills). Equivalent to running `blitzq worker` N times
  yourself; this is the same scaling lever the benchmark report's "Profile
  B" already uses. See
  [docs/performance_tuning.md](docs/performance_tuning.md#scale-out-with-processes).

### Changed

- `get_result()`'s push notification (added in 1.0.1) now shares one
  pub/sub connection per process instead of opening one per waiting call.
  Redis brokers `PSUBSCRIBE` the whole notification pattern once and fan
  incoming messages out to local waiters in-process; a workload with
  thousands of tasks in flight at once now costs a handful of Redis
  connections instead of thousands. Also closes a race that could delay a
  result by the full fallback poll interval: a task finishing and
  publishing its notification in the gap between a caller's last check and
  its registration as a waiter is now caught by one extra check right
  after registering, instead of only being recovered on the next fallback
  poll. No API change. See
  [docs/delivery_guarantees.md](docs/delivery_guarantees.md#results-and-task-state).

## [1.0.1] - 2026-09-30

### Changed

- `get_result()` no longer polls on a fixed backoff. Redis brokers now
  publish on a per-task pub/sub channel in the same transaction as the final
  record write; `get_result()` waits on that instead, with a 5 s safety-net
  poll in case a notification is dropped. `MemoryBroker` uses an in-process
  `asyncio.Condition` for the same effect. No API change — same signature,
  same exceptions, just lower latency and less Redis traffic under load. See
  [docs/delivery_guarantees.md](docs/delivery_guarantees.md#results-and-task-state).

## [1.0.0] - 2026-09-30

First release, published to PyPI: `pip install blitzq`.

### Added

- `Queue` application object: task registry, routing rules, per-task options,
  async and sync client APIs (`enqueue`/`enqueue_sync`, `enqueue_many`,
  `get_result`, `inspect`, `status`, `cancel`, `retry`, `dead_letters`,
  `queue_stats`, `purge`, `send` by name).
- Redis brokers: **reliable mode** (Streams consumer groups, at-least-once,
  lease renewal, crash recovery, poison-message protection) and **fast mode**
  (lists, at-most-once). `MemoryBroker` for tests (ephemeral).
- Broker abstraction (`blitzq.broker.base.Broker`); all Redis commands are
  isolated in `blitzq.broker`.
- Worker: bounded global and per-queue concurrency, backpressure, async tasks,
  thread executor for sync tasks, process executor for CPU-bound tasks, timeouts,
  graceful shutdown with requeue, group-committed acknowledgements, revocation,
  structured logging, metrics, optional Prometheus exporter. Warns once per
  task name when a thread-executor task looks CPU-bound (high thread CPU/wall
  ratio for a non-trivial duration), suggesting `executor="process"`;
  disable with `warn_cpu_bound=False` / `--no-warn-cpu-bound`.
- Retries: `RetryPolicy` (exponential backoff, max delay, jitter,
  retryable/non-retryable exceptions), explicit `Retry`, scheduled (not sleeping)
  retries, dead-letter store with replay.
- Scheduling: delays and ETAs via an atomically promoted sorted set; periodic
  tasks (`Every`, timezone-aware `Cron`) with deterministic occurrence ids,
  compare-and-set dispatch for multiple schedulers, and `skip`/`run_once`/`run_all`
  missed-run policies.
- Task priority within a queue (`@queue.task(priority=...)` /
  `task.options(priority=...)`, `"high"`/`"normal"`/`"low"`): physically
  separate broker sub-queues checked in order by every worker on every batch,
  sharing the base queue's concurrency budget; crash recovery and
  at-least-once semantics apply identically to every level.
- Rate limiting (`@queue.task(rate_limit="10/s")`): a Redis-backed token
  bucket per task name, shared across every worker process. A task over its
  limit is not executed and not counted as a retry; it's rescheduled for
  when a slot should be free.
- Task state records with optional state tracking and result TTL.
- CLI: `worker`, `scheduler`, `queue stats|purge`, `task inspect|retry|cancel`,
  `dead-letter list|purge`, `benchmark run|compare`.
- Integrations: ASGI lifespan (FastAPI/Starlette/Litestar), Django
  (`enqueue_on_commit`, worker setup), Flask (`init_app`, app context in workers).
- Benchmark suite comparing BlitzQ with Celery and Huey (12 workload types,
  equivalent and tuned profiles, raw CSV/JSON output, charts, Markdown report).
