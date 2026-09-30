# Performance tuning

Measure before and after any change. `blitzq queue stats` and the Prometheus
histograms show whether you are limited by producers, workers or Redis.

## Choose the right execution model

| Work | Use | Why |
|---|---|---|
| Network I/O with async clients (httpx, asyncpg, aioboto3, ...) | `async def` tasks, `--concurrency` in the hundreds | Thousands of in-flight tasks on one thread |
| Blocking I/O libraries (requests, psycopg2, boto3) | plain `def` tasks, `--threads` sized to the parallelism you want | Threads release the GIL during I/O |
| CPU-bound Python | `executor="process"` and `--processes` ≈ cores, or several single-threaded worker processes | The GIL serialises CPU work on threads |

Never call blocking functions inside `async def` tasks. They stall every other
task on that worker, and in reliable mode they can delay lease renewal long
enough for another worker to redeliver the message.

**The worker warns you when it catches this.** If a thread-executor task spends
almost all of a non-trivial wall-clock duration on the CPU rather than blocked
in I/O, the worker logs one warning per task name suggesting
`executor="process"`:

```
task 'myapp.tasks.crunch' looks CPU-bound (210ms wall, 97% on CPU) but is
running on the thread executor, where Python's GIL serialises it against
every other task on this worker; consider @queue.task(executor="process")
instead
```

This is exactly the gap that shows up as a 0.15x result in the benchmark
report when a CPU-bound task is left on the thread executor - switching it to
`executor="process"` closed that gap to roughly 1.0x in the same benchmark.
Disable it with `Worker(warn_cpu_bound=False)` if you have a task that is
genuinely, deliberately CPU-heavy but short enough (or rare enough) that a
dedicated process pool isn't worth it.

## Scale out with processes

One worker process runs one event loop on one core. In the tuning sweep,
no-op drain throughput went from about 17k tasks/s with 1 worker process to about
66–70k with 4 on a 12-CPU machine
(`benchmarks/results/published/tuning/`). Run roughly one worker process per core you
want to dedicate, and size `--concurrency` per process.

## Knobs

| Setting | Default | Effect |
|---|---|---|
| `--concurrency` | 100 | Max tasks executing per process. Higher helps I/O-bound work; it doesn't help CPU-bound work. |
| `--batch-size` | `min(concurrency, 100)` | Max messages per fetch round-trip. Larger means fewer round-trips but more work claimed at once. |
| `--queue-concurrency q=N` | none | Cap a queue within a worker (protects downstream services, keeps capacity for other queues). |
| `mode="fast"` | reliable | Removes the ack write (one fewer Redis command per task) at the cost of losing in-flight tasks on crashes. |
| `store_results=False` / `@task(store_result=False)` | True | Skips one `SET` per task. |
| `track_state=True` | False | Adds one write per state transition. Enable only if you need `queued`/`running` visibility. |
| `task.enqueue_many(...)` | - | Publishes many tasks in one pipelined round-trip. In the benchmark, one `enqueue_sync` call per task from one process reached about 2.7k tasks/s; `enqueue_many` batches of 500 from four processes reached 80k–196k tasks/s. |
| `schedule_poll_interval` | 0.5 s | Upper bound on extra latency for newly scheduled tasks. Lower means more precise at the cost of more Redis calls. |
| `visibility_timeout` | 60 s | Reliable mode: lower means faster crash recovery but less tolerance for long event-loop stalls. |
| `--warn-cpu-bound` / `Worker(warn_cpu_bound=)` | on | Warn once per task name when a thread-executor task looks CPU-bound (see above). Free to leave on; it only logs, never changes behavior. |

## Producers

- Prefer the async API in async applications. `enqueue_sync` adds a thread hop
  (a few tens of µs) per call.
- Reuse the `Queue` object; it pools connections. Don't create one per request.
- Burst publishing: use `enqueue_many`.

## Redis

- Keep Redis close to the workers (same availability zone). Every round-trip is
  on the critical path.
- Redis executes commands on one core. In the tuned no-op benchmark (about 63k
  tasks/s) Redis used about 0.013 ms of CPU per task, which is about 0.8 of a core,
  so Redis is the next ceiling beyond that rate. Check `redis_cpu_ms_per_task` in
  the benchmark report.
- Use separate namespaces or Redis databases for unrelated applications. Split
  heavy applications across Redis instances.
