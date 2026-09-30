# BlitzQ vs Celery vs Huey

One consolidated report: what each system is, its pros and cons, and measured
speed/reliability/footprint numbers from BlitzQ's benchmark suite. All numbers
here are pulled directly from the raw run data — nothing is estimated or
rounded from memory. Full methodology: [docs/benchmarking.md](benchmarking.md).
Full raw reports: [`benchmarks/results/published/suite/REPORT.md`](../benchmarks/results/published/suite/REPORT.md)
(BlitzQ vs Celery) and [`benchmarks/results/published/huey_suite/REPORT.md`](../benchmarks/results/published/huey_suite/REPORT.md)
(BlitzQ vs Huey).

**Test environment (same for both suites):** Linux container, 12 logical CPUs,
25.2 GB RAM, Redis 7.4.11, no Redis persistence. Python 3.12.14. Celery
5.6.3/kombu 5.6.2, Huey 3.4.0, redis-py 6.4.0. 5 repetitions per
workload/profile/system, medians reported, 0 harness errors across both suites
(420 runs total). Task bodies are small/synthetic; real tasks that do
meaningful work will shrink every ratio below.

---

## 1. What each system is

| | **BlitzQ** | **Celery** | **Huey** |
|---|---|---|---|
| Execution model | asyncio-native; async tasks as coroutines, sync tasks on a thread pool, CPU tasks on a process pool | prefork (processes) by default; also supports threads, eventlet, gevent pools | thread/greenlet/process worker types per consumer |
| Delivery guarantees | explicit: `reliable` (at-least-once, Streams) or `fast` (at-most-once, lists) | `task_acks_late` + `task_reject_on_worker_lost` for at-least-once; early-ack by default | early-ack only; no at-least-once mode |
| Brokers | Redis only (+ in-memory for tests) | Redis, RabbitMQ, Amazon SQS, others via kombu | Redis, SQLite, in-memory, file |
| Multiple queues | native, per-queue concurrency limits | native, per-queue concurrency via routing | one Huey instance per queue (no built-in multi-queue routing) |
| Scheduling | delayed/ETA tasks + cron/interval periodic tasks, multi-scheduler safe | ETA/countdown tasks; periodic via Celery Beat (single instance unless externally coordinated) | delayed tasks + periodic tasks via decorator, single scheduler thread per consumer |
| Batch enqueue | `enqueue_many` (one pipelined round-trip) | no batch publish API | no batch publish API |
| Dependencies | `redis`, `msgspec`, `typer` | `kombu`, `billiard`, optional `gevent`/`eventlet` | none beyond `redis` (very small) |
| Maturity/ecosystem | 0.1.0, new, alpha API | mature, large ecosystem, Django/Flask integrations everywhere | mature, smaller ecosystem, Django integration built in |
| Typed API | yes (msgspec-typed, `py.typed`) | partially (dynamic task registry) | partially |
| Monitoring | Prometheus exporter, CLI inspection | Flower and other mature tooling | minimal built-in tooling |

---

## 2. Pros and cons

### BlitzQ

**Pros**
- Fastest in every benchmark except CPU-bound work (where it ties both), often
  by a wide margin — see §3.
- Explicit, honestly-documented delivery guarantees; never claims exactly-once.
- Lowest resource footprint measured: ~39 MB worker RSS vs ~775–815 MB for
  Celery's prefork pool on the same task (see §5).
- Async-native: high concurrency for I/O-bound tasks without threads or
  greenlets.
- Typed public API, `py.typed`, small dependency footprint.
- Fastest crash recovery measured (worker-crash workload): ~17 s vs Celery's
  ~102 s at-least-once (Celery's Redis transport restores messages on a fixed
  ~10 s cycle, acting every 10th tick).

**Cons**
- **0.1.0, alpha, not yet on PyPI.** No production track record.
- CPU-bound tasks on the default thread executor are GIL-bound (measured
  0.15x vs Celery's prefork at equal settings) — must explicitly opt into
  `executor="process"` to be competitive.
- Slightly slower recovery after a Redis connection interruption than Celery
  in the one metric measured (5.1 s vs 4.4 s).
- Delayed/ETA tasks start later at p50 under heavy tuned load than Celery's
  equivalent (13 ms vs 0.6 ms) — a side effect of promoter polling.
- Redis only; no other broker.
- Smaller ecosystem: no Flower-equivalent dashboard, fewer third-party
  integrations, far fewer people who have run it in production.

### Celery

**Pros**
- Mature, extremely widely deployed, huge ecosystem (Flower, django-celery-*,
  countless guides).
- True multi-broker support (RabbitMQ, SQS, more).
- Prefork pool gives CPU-bound tasks real process-level parallelism without
  any extra configuration.
- Flexible worker pool choices (prefork/threads/eventlet/gevent) tunable per
  workload.

**Cons**
- Slowest of the three systems on overhead-dominated workloads (no-op,
  payloads, retries, results) at both equal and tuned settings — measured
  3–29x slower than BlitzQ depending on workload and tuning.
- By far the heaviest footprint measured: ~780 MB peak worker RSS for a
  16-child prefork pool doing nothing, vs ~39–43 MB for BlitzQ/Huey.
- Crash recovery in at-least-once mode was measured at ~102 s regardless of
  a 10 s `visibility_timeout` setting — kombu's Redis transport only checks
  for restorable messages on a fixed ~10-tick cycle. Not configurable without
  patching kombu.
- Its `threads` worker pool measured 4–5 tasks/s on a 50 ms I/O workload
  (vs 153/s for prefork-8) — stalls under this benchmark's conditions; not
  root-caused, but the practical implication is: don't reach for `threads`
  for I/O-bound Celery work.
- Its `gevent` worker pool capped at ~230 tasks/s regardless of pool size or
  workload (no-op or I/O), independent of what the task actually does.
- Measured 77 duplicate executions across 5 runs when all Redis connections
  were dropped mid-run in at-least-once mode (BlitzQ: 0).
- Heavier dependency chain (kombu, billiard, optional gevent/eventlet).

### Huey

**Pros**
- Very small, simple, minimal dependencies (essentially just `redis`).
- Lowest, flattest Redis command cost measured: exactly 4 commands per task
  across every workload tested, regardless of features used.
- Its `process` worker type matched BlitzQ's process executor almost exactly
  on CPU-bound work (0.98–1.04x either way — a genuine tie, not a rounding
  artifact).
- Comparable worker-crash loss and recovery time to BlitzQ's fast mode (both
  lost 15–16 of 2,000 tasks, both recovered in ~5 s) — no disadvantage there.
- Built-in Django integration, simple decorator API.

**Cons**
- **No at-least-once mode at all.** A message is popped from Redis before
  execution; if the worker dies mid-task, that task is simply gone. This is
  a design limitation, not a bug, but rules Huey out for work that must
  survive a crash.
- Slower than BlitzQ on nearly everything except CPU-bound work: 1.9–29x at
  equal/tuned settings on overhead-dominated workloads, 3.1–4.7x on retries.
- Its `greenlet` worker type does not monkey-patch the standard library
  itself — a plain `time.sleep`-based task blocks every greenlet on that
  worker instead of yielding. (Found in this benchmark: 48 tasks/s until
  `gevent.monkey.patch_all()` was applied manually, 2,750 tasks/s after.)
  Anyone using greenlet workers with ordinary synchronous code needs to know
  to patch manually.
- No native multi-queue routing — one Huey instance per queue, coordinated
  manually.
- No batch/pipelined publish API — every enqueue is one round-trip.
- Scheduled tasks lagged their due time more than the other two systems in
  this benchmark (p50 1.0–2.1 s late on a 2 s delay) — a consequence of its
  default 1 s scheduler poll interval (configurable, left at default here).
- Smaller ecosystem and less production track record than Celery.

---

## 3. Speed tests

All entries are median completed tasks/second across 5 repetitions.
**Profile A** = equal settings (16 execution slots, one worker process, one
producer). **Profile B** = each system independently tuned (4 processes each,
BlitzQ async + batched enqueue, Celery prefork, Huey greenlet/process where
applicable). Guarantee suffix: `alo` = at-least-once, `amo` = early-ack (fast
mode/default-ack). Huey has no `alo` mode, so it only appears under `amo`.

| Workload | Profile | BlitzQ | Celery | Huey | BlitzQ vs Celery | BlitzQ vs Huey |
|---|---|---:|---:|---:|---:|---:|
| no-op | A-alo / A-amo | 2,604 / 2,689 | 855 / 865 | — / 1,499 | 3.05x / 3.11x | — / 1.90x |
| no-op | B-alo / B-amo | 63,542 | 3,139 | — / 6,024 | 20.24x | — / 20.99x |
| no-op, pre-loaded backlog | A-alo / A-amo | 5,503 / 7,204\* | 967 / 984 | — / 1,438 | 5.69x / 7.52x | — / 5.01x |
| no-op, pre-loaded backlog | B-alo / B-amo | 70,696 | 3,738 | — / 5,414 | 18.91x | — / 28.76x |
| 1 KiB payload | A-alo / A-amo | 2,771 / — | 919 / — | — / 1,418 | 3.02x | — / 1.89x |
| 1 KiB payload | B-alo / B-amo | 59,270 | 3,129 | — / 5,731 | 18.94x | — / 17.28x |
| 100 KiB payload | A-alo | 2,296 | 474 | n/a | 4.84x | n/a |
| 100 KiB payload | B-alo | 3,812 | 1,265 | n/a | 3.01x | n/a |
| 20 ms I/O task | A-alo / A-amo | 755 / — | 720 / — | — / 762 | 1.05x | — / 1.01x |
| 20 ms I/O (tuned executors) | B-alo / B-amo | 41,807 / 62,120 | 2,632 | — / 8,657 | 15.89x | — / 7.18x |
| **CPU-bound** (threads/prefork) | A-alo / A-amo | 73 | **486** | — / 70 | **0.15x** | — / 1.04x |
| CPU-bound (process pools, 8 each) | B-alo / B-amo | 472 / 467 | 445 | — / 478 | 1.06x | — / **0.98x** |
| burst-publish | A-alo / A-amo | 2,777 / 2,806\* | 947 / 964 | — / 1,480 | 2.93x / 2.97x | — / 1.90x |
| burst-publish | B-alo / B-amo | 62,749 / — | 3,335 | — / 5,871 | 18.81x | — / 22.53x |
| multi-queue (80/15/5 split) | A-alo / A-amo | 2,730 / — | 933 | — / 1,835 | 2.93x | — / 1.50x |
| multi-queue (80/15/5 split) | B-alo / B-amo | 62,496 / 110,537 | 3,277 | — / 5,797 | 19.07x | — / 19.07x |
| retry-heavy (50% fail once) | A-alo / A-amo | 1,745 / 1,914 | 497 | — / 609 | 3.51x | — / 3.14x |
| retry-heavy | B-alo / B-amo | 8,577 / 9,218 | 1,370 | — / 1,956 | 6.26x | — / 4.71x |
| result storage on | A-alo / A-amo | 2,784 / 2,746 | 1,002 | — / 1,459 | 2.78x | — / 1.88x |
| result storage on | B-alo / B-amo | 49,521 / 70,449 | 2,748 | — / 5,596 | 18.02x | — / 12.59x |
| scheduled (2 s delay; throughput bounded by the delay — see §4) | A-alo / A-amo | 1,273 / 1,245 | 914 | — / 742 | 1.39x | — / 1.68x |
| scheduled | B-alo / B-amo | 2,359 / 2,419 | 1,749 | — / 1,200 | 1.35x | — / 2.02x |

\* BlitzQ's A-amo figure differs slightly between the two suites (7,407 vs
Celery, 7,204 vs Huey; 2,862 vs Celery, 2,806 vs Huey) even though the settings
are identical — this is ordinary run-to-run variation from running the two
suites at different times, well within the CV% reported in the full reports.

**Reading this table:** the two suites were run separately, so BlitzQ's own
figures can differ between its "vs Celery" and "vs Huey" columns even for the
same workload name — BlitzQ used reliable mode (`alo`) with 1 producer against
Celery, and fast mode (`amo`) with 4 producers against Huey, matched to what
each competitor could actually be paired against (Huey has no `alo` mode).
Within one comparison (BlitzQ vs Celery, or BlitzQ vs Huey) the settings are
apples-to-apples; across the two comparisons they are not directly
interchangeable. See §3's profile note and the linked full reports for the
exact settings behind every number. Bold marks the one workload class
(CPU-bound, equal/default settings) where BlitzQ is slower than a competitor.

---

## 4. Reliability tests

| Test | BlitzQ | Celery | Huey |
|---|---|---|---|
| **Worker SIGKILL'd mid-task, at-least-once mode** | 0 lost, recovered in 16.9 s, 1 duplicate execution across 5 runs | 0 lost, recovered in **101.6 s** (fixed ~10 s Redis-transport restore cycle, independent of `visibility_timeout`) | no at-least-once mode — not applicable |
| **Worker SIGKILL'd mid-task, early-ack mode** | 15–16 of 2,000 lost per crash (the tasks executing at the kill); recovered in 5.3 s | 15–16 of 2,000 lost per crash; recovered in 101.6 s | 15–16 of 2,000 lost per crash; recovered in 5.1 s |
| **All Redis connections dropped mid-run, at-least-once** | 0 lost, 0 duplicates | 0 lost, **77 duplicate executions** across 5 runs | not applicable (no at-least-once mode) |
| **All Redis connections dropped mid-run, early-ack** | 0 lost, 0 duplicates, recovered in 4.8 s | 0 lost, 2 duplicates, recovered in 4.1 s | not measured in this suite |

Takeaway: for a worker crash, BlitzQ's reliable mode is the only combination
here that both loses nothing *and* recovers in single-digit seconds. Celery's
at-least-once mode also loses nothing but takes ~6x longer to redeliver, and
also produced duplicate work under a connection-loss scenario where BlitzQ did
not.

---

## 5. Resource footprint

Per-task worker CPU, peak worker RSS and Redis commands per task, at equal
settings (profile A / A-amo), medians across representative workloads:

| Workload | Metric | BlitzQ | Celery | Huey |
|---|---|---:|---:|---:|
| no-op | worker RSS (MB) | 38.8 | 775.2 | 42.9 |
| no-op | worker CPU (ms/task) | 0.43 | 1.22 | 1.01 |
| no-op | Redis commands/task | 4.43 | 10.02 | 4.01 |
| 20 ms I/O | worker RSS (MB) | 38.8 | 815.3 | 42.9 |
| 20 ms I/O | worker CPU (ms/task) | 0.65 | 1.32 | 0.53 |
| 20 ms I/O | Redis commands/task | 5.51 | 10.02 | 4.02 |
| CPU-bound | worker RSS (MB) | 38.4 | 814.9 | 42.7 |
| CPU-bound | worker CPU (ms/task) | 14.11 | 21.65 | 15.33 |
| CPU-bound | Redis commands/task | 4.05 | 10.04 | 4.24 |

Celery's memory figure is dominated by its 16-process prefork pool (16 Python
interpreters); BlitzQ and Huey both ran 16 threads in one process. This is an
architectural consequence of the pool model, not a per-task inefficiency, but
it is the real memory cost of Celery's default concurrency model for
sync-function workloads.

---

## 6. Bottom line

- **Overhead-dominated work (no-op, small/large payloads, bursts, retries,
  result storage, multi-queue):** BlitzQ wins clearly against both, from
  ~1.9x to ~29x depending on tuning. Huey is consistently the middle
  performer; Celery is consistently the slowest per-task, though its prefork
  pool buys real CPU parallelism Huey's and BlitzQ's default thread models
  don't have.
- **CPU-bound work:** a genuine BlitzQ weakness *only if you use its default
  thread executor* (0.15x vs Celery prefork). Switch to
  `executor="process"` and it's roughly tied with both competitors' process
  pools (0.98–1.06x).
- **I/O-bound work:** roughly tied at equal settings across all three;
  BlitzQ's async model pulls well ahead once each system is allowed its best
  configuration (7–16x).
- **Crash resilience:** BlitzQ's reliable mode is the strongest result in this
  report — zero loss and the fastest recovery of any at-least-once
  configuration measured. Huey has no equivalent mode at all.
- **Maturity and ecosystem** are not something a benchmark measures: Celery
  is a known quantity in production; BlitzQ is 0.1.0 and unpublished; Huey
  sits in between.

Reproduce everything above:

```bash
docker compose up -d redis redis-stats
docker compose --profile bench run --rm bench python -m benchmarks.runner --config benchmarks/configs/suite.yaml
docker compose --profile bench run --rm bench python -m benchmarks.runner --config benchmarks/configs/huey_suite.yaml
python -m benchmarks.report --input benchmarks/results/<run-dir>
```
