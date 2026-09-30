# Benchmarking BlitzQ against Celery

The benchmark suite is a first-class part of the project. It runs the same
workload definitions against BlitzQ and Celery, records every task execution,
and generates reports from raw data. **No number in the published reports is
typed by hand.**

## Running

```bash
docker compose up -d redis redis-stats                  # broker (6379) + accounting Redis (6380)
docker compose --profile bench build bench              # Linux image with Celery, gevent, psutil
docker compose --profile bench run --rm bench \
    python -m benchmarks.runner --config benchmarks/configs/suite.yaml
python -m benchmarks.report --input benchmarks/results/suite-<stamp>     # or: blitzq benchmark compare
```

Useful options: `--only noop,worker-crash`, `--systems blitzq`,
`--repetitions 1`, `--scale 0.1` (for quick harness checks; not for published
numbers).

Benchmarks run inside a Linux container on purpose. Celery's default prefork pool
is not supported on Windows, and benchmarking it there would be unfair to Celery.
`blitzq benchmark run` works on the host too if Redis is on ports 6379/6380 and
`pip install -e ".[bench]"` is done.

## How a run is measured

For every (workload, profile, repetition, system) the runner (`benchmarks/runner.py`):

1. `FLUSHDB`s the broker database and clears the accounting list, so every run
   starts from an empty, isolated namespace.
2. Starts the system's worker processes.
3. **Warm-up**: publishes 500 no-op tasks, waits for all of them, then discards
   their records.
4. Starts samplers every 250 ms: worker process-tree CPU and RSS (psutil),
   broker backlog, Redis memory. It also snapshots Redis `INFO` counters (CPU,
   commands, network bytes).
5. Runs producer **subprocesses**. Each task carries its own enqueue timestamp,
   taken immediately before the enqueue call.
6. Optionally disrupts the run: kills a worker's whole process tree with SIGKILL
   and starts a replacement, or drops all Redis client connections with
   `CLIENT KILL TYPE normal`.
7. Waits until every task id has a successful execution record. It stops early
   only if the run is quiescent (no new records for 5 s *and* nothing left in the
   broker) or the timeout expires. Outstanding work is always reported.
8. Stops the workers and writes raw per-task CSV, the time series and a summary
   row.

Systems alternate order between repetitions (ABBA...) to spread drift evenly.

### Accounting

Task bodies are shared by both systems (`benchmarks/workloads.py`). Each
execution appends `uid,outcome,attempt,t_enqueue,t_start,t_end,queue,pid` to an
in-process buffer that a background thread ships to a **separate Redis**
(`redis-stats`) every 50 ms, so accounting traffic never touches the broker being
measured. For crash and interruption workloads records are written synchronously
before the task returns. Otherwise a killed worker would take the records of
tasks that had really completed with it, and they would be miscounted as lost.
This costs one extra round-trip per task for both systems.

Timestamps are `time.perf_counter()` (Linux: CLOCK_MONOTONIC, system-wide), so
producer and worker timestamps on the same host are directly comparable.
Execution time is measured inside the task, so it is never counted as queue
latency.

| Metric | Definition |
|---|---|
| throughput | unique successfully completed tasks / (last task end - first enqueue). For *drain* workloads: / (last task end - first task start) |
| enqueue rate | tasks / (last enqueue returned - first enqueue started), across producer processes |
| start latency | task start - enqueue timestamp (queueing + dispatch) |
| e2e latency | task end - enqueue timestamp |
| not completed | tasks with no successful execution record at the end |
| outstanding | messages still in the broker (queued, unacknowledged, scheduled) at the end |
| lost | `not completed` when `outstanding == 0`; otherwise unknown and reported as outstanding |
| duplicates | successful executions beyond the first per task id |
| worker CPU | CPU seconds of the worker process trees (including prefork children) during the measured phase |
| Redis CPU / cmds / net | deltas of Redis `INFO` counters over the measured phase (includes producer traffic) |
| recovery | disruption time until the last task completed |

Speedup is BlitzQ median throughput / Celery median throughput, computed only
when both systems completed every task in every repetition of that workload and
profile.

## Systems

BlitzQ is compared against **Celery 5.6.3** and **Huey 3.4.0** (both installed
in the benchmark image; see `benchmarks/apps/` and `benchmarks/adapters/` for
each system's task definitions and worker/producer wiring). Huey has no
at-least-once mode of its own - it pops a message from Redis before executing
it, the same as Celery's early-ack mode or BlitzQ's fast mode - so it is only
ever compared against those, never against BlitzQ's reliable mode. Huey's
`greenlet` worker type requires `gevent.monkey.patch_all()` to be applied
before Redis or Huey are imported, or synchronous task bodies (e.g.
`time.sleep`) block every greenlet on that worker instead of yielding; this is
done in `benchmarks/apps/huey_app.py` when `BENCH_HUEY_WORKER_TYPE=greenlet`.

## Profiles

A benchmark that compares a non-durable configuration with a durable one proves
nothing, so every comparison is paired by delivery guarantee:

- **alo (at-least-once)**: BlitzQ `mode="reliable"` vs Celery
  `task_acks_late=True` + `task_reject_on_worker_lost=True`. Both acknowledge
  after execution and redeliver after a crash.
- **amo (early ack)**: BlitzQ `mode="fast"` vs Celery defaults (ack before
  execution). Neither redelivers a task that was executing when its worker died.
  Celery's default still recovers prefetched-but-unstarted messages; BlitzQ fast
  mode does not, so here BlitzQ runs the *weaker* configuration.

Two resource and tuning profiles:

- **Profile A, equivalent.** Identical plain synchronous task functions, 16
  concurrent execution slots, one consumer process, one producer process making one
  synchronous enqueue call per task (`enqueue_sync` vs `apply_async`). Each system
  runs sync functions in its default way: Celery's prefork pool (16 child
  processes), BlitzQ's thread executor (16 threads). Celery's `threads` pool was
  evaluated for this role and rejected because on this stack it stalled on I/O
  tasks (about 5 tasks/s on 50 ms sleeps; see `configs/diag_celery_io.yaml`).
- **Profile B, independently tuned.** Four consumer processes and four producer
  processes for both. Settings come from a measured sweep
  (`configs/tuning.yaml`, `configs/tuning2.yaml`): BlitzQ uses async tasks, 4×200
  slots (4×500 for I/O) and pipelined `enqueue_many`. Celery uses 4 worker
  instances with prefork-8 (prefork-16 for I/O). Celery has no batch publish API.
  Celery's gevent pool was also measured and was slower (230–237 tasks/s for both
  no-op and 20 ms I/O tasks, independent of pool size, vs 722/s for prefork-16;
  see `configs/diag_celery_gevent*.yaml` and `results/published/diagnostics/`).

## Workloads

| Workload | What it stresses |
|---|---|
| noop | per-task overhead with producers and workers running concurrently |
| noop-drain | pure worker capacity: the whole backlog is published first, then workers start |
| small-payload / large-payload | 1 KiB / 100 KiB string argument |
| async-io | 20 ms `asyncio.sleep` (BlitzQ B) / `time.sleep` (A, Celery) |
| cpu-bound | 200k-iteration integer loop |
| burst-publish | 30k tasks published as fast as possible |
| multi-queue-uneven | 80% / 15% / 5% traffic over three queues; per-queue latency |
| retry-heavy | every second task fails its first attempt; 0.1 s retry delay |
| scheduled | every task due 2 s after enqueue; dispatch lateness |
| worker-crash | 50 ms tasks; SIGKILL of the worker tree at 30% progress; replacement started immediately |
| redis-interruption | all Redis client connections dropped at 30% progress (after publishing) |
| result-storage | no-op tasks with result storage on in both systems |

## Limitations

- Single machine: Redis, workers, producers and the harness share one 12-CPU
  Docker VM (WSL2 on Windows 11). There is no network latency, and absolute numbers
  on dedicated hardware or across a network will differ. Relative results for
  round-trip-heavy designs may shift when network latency is added.
- Redis runs without persistence (`appendonly no`) for both systems. With AOF,
  per-command cost rises for both.
- Only Redis 7.4, Python 3.12, Celery 5.6.3 / kombu 5.6.2 and redis-py 6.4 were
  measured.
- Profile A's single producer bounds throughput for fast consumers. The
  `noop-drain` workload isolates consumer capacity.
- Task bodies are trivial or synthetic. Real tasks dominated by their own work
  shrink every queue-overhead difference shown here.
- The Celery `threads` and `gevent` observations were reproduced but not
  root-caused inside Celery/kombu.

## Published results

Two suites were run against the same BlitzQ code and Redis:
[`suite/`](../benchmarks/results/published/suite/REPORT.md) compares BlitzQ
against Celery (reliable-mode and fast-mode profiles);
[`huey_suite/`](../benchmarks/results/published/huey_suite/REPORT.md) compares
BlitzQ against Huey (fast-mode-only profiles, since Huey has no at-least-once
mode). Both were run with 5 repetitions per workload/profile/system and 0
harness errors.

[`benchmarks/results/published/suite/REPORT.md`](../benchmarks/results/published/suite/REPORT.md)
is the full report of the run summarised in the README: 290 runs with 5
repetitions per system, workload and profile, and 0 harness errors. The
directory also holds per-run (`runs.csv`, `summary.jsonl`) and aggregated
(`aggregate.csv`, `summary.json`) data, charts, the exact config, the environment
and the command. Tuning and diagnostic run summaries are in `published/tuning/`
and `published/diagnostics/`. Raw per-task CSVs (about 260 MB) are not committed;
rerunning the command regenerates them.

Observations from the run, stated with their evidence:

- **Celery `threads` pool:** 4–5 tasks/s on 50 ms I/O tasks with or without
  `acks_late`, vs 153/s for prefork-8 (`diagnostics/diag-celery-io-*`). Not
  root-caused.
- **Celery crash recovery:** kombu's Redis transport calls `restore_visible` every
  10 s but acts only on every 10th call. Unacknowledged messages of a killed worker
  were therefore redelivered about 100 s after the crash in every repetition,
  regardless of `visibility_timeout` (10 s here). This was read from kombu 5.6.2
  source (`kombu/transport/redis.py`) and matches the measured 101.6 s.
- **Celery duplicates under connection loss:** 77 duplicate executions over 5
  runs with `acks_late` (0 for BlitzQ), and 2 with early ack.
- **One Celery loss in a harness check:** in a scaled-down harness check (100
  tasks), Celery with `acks_late` lost 1 task after a worker kill. This was not
  reproduced in the 5 full repetitions (0 lost), so it is not claimed as a
  result. A plausible cause is that kombu moves a message from the queue into its
  unacked set in two steps, so a kill between them loses the message. That is a
  hypothesis, not verified.
- **Measurement artifact found and fixed during development:** buffered accounting
  lost the records of completed tasks when a worker was killed, which showed up as
  6 false "lost" tasks for BlitzQ. Crash workloads now record synchronously.
- **Code version:** the suite measured the code before three post-benchmark
  fixes (strong references to in-flight worker tasks, one Redis client per event
  loop, and a heartbeat default that keeps leases within a third of the visibility
  timeout). A BlitzQ-only re-run of `noop` and `noop-drain` after the fixes
  (3 repetitions, `published/postfix-check/`) matched the published medians within
  run-to-run variation; for example, tuned drain was 70,217 vs 70,696 tasks/s.
- **Huey observations (`huey_suite/REPORT.md`):** at equal settings (16
  threads each), BlitzQ was 1.0-1.9x faster on all workloads except
  retry-heavy (3.1x) and worker-crash (comparable: both lost 15-16 of 2,000
  tasks per crash, both recovered in about 5s). With each system tuned (4
  processes, Huey's greenlet workers for I/O, process workers for CPU-bound),
  BlitzQ was 7-29x faster except CPU-bound, where the two process pools
  performed the same (0.98x). Huey's Redis usage per task (4 commands, mostly
  independent of workload) was lower than Celery's (9-17) but its worker CPU
  per task was similar to or higher than BlitzQ's on most workloads. Huey's
  scheduled-task lateness (p50 1.0-2.1s on a 2s delay) reflects its default
  1s scheduler poll interval, which is configurable but was left at its
  default to match how each system ships.
- The host machine was lightly used (file editing, a 12-second unit-test run once)
  while the suite ran.
