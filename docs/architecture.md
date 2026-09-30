# Architecture

```
 web / script process                     Redis                          worker process
┌──────────────────────┐      ┌──────────────────────────────┐      ┌──────────────────────────┐
│ task.enqueue()       │─────▶│ {ns}:s:<queue>  (stream)     │─────▶│ fetch loop per queue     │
│  └ Serializer        │      │ {ns}:l:<queue>  (list, fast) │      │  └ limiter (global+queue)│
│  └ Broker.publish    │      │ {ns}:sched      (zset)       │◀─────│ task coroutine / thread  │
│                      │      │ {ns}:t:<id>     (records)    │◀─────│ group-commit flusher     │
│ queue.get_result()   │◀─────│ {ns}:dlq        (dead)       │◀─────│ promoter (sched → queue) │
└──────────────────────┘      │ {ns}:periodic   (last run)   │◀─────│ lease renew / recovery   │
                              └──────────────────────────────┘      └──────────────────────────┘
                                             ▲
                              blitzq scheduler (periodic tasks + promotion; run 1..n)
```

## Layers

| Module | Responsibility | Knows about Redis? |
|---|---|---|
| `client.Queue` | configuration, task registry, client operations (results, cancel, retry, stats) | no |
| `task.Task` | building envelopes, enqueue (async, sync, batched) | no |
| `worker.Worker` | fetching, bounded execution, timeouts, retries, dead letters, shutdown | no |
| `scheduler.Scheduler` | periodic occurrences, missed-run policy | no |
| `serialization` | envelopes and records (msgspec, MessagePack/JSON) | no |
| `broker.base.Broker` | the contract between the engine and a backend | defines it |
| `broker.redis_fast`, `broker.redis_reliable`, `broker.redis_base`, `broker._lua` | Redis implementations | yes (only here) |
| `broker.memory` | in-process ephemeral backend | no |

Every Redis command lives in `blitzq/broker/`. The worker, client and scheduler call
only `Broker` methods and exchange opaque `bytes` payloads with the broker.

## The broker contract

`Broker` (see `broker/base.py`) groups operations by concern:

- **Queues:** `publish`, `prepare_consumer`, `fetch(queue, count, timeout)`,
  `complete(completions)`, `heartbeat`, `recover`.
- **Schedule:** `promote_due(now, limit) -> (count, next_eta)`, `scheduled`,
  `is_scheduled`, `scheduled_count`.
- **Cancellation:** `cancel(task_id, ttl)` and `revoked()`.
- **Records:** `get_record` and `set_record`.
- **Dead letters:** list, get, count, `replay_dead_letter` (atomic) and purge.
- **Periodic coordination:** `claim_periodic` (compare-and-set + publish) and
  `periodic_last`.
- **Inspection:** `queue_stats`, `known_queues`, worker registration, `purge_queue`.

A `Completion` bundles everything that must happen when a worker finishes with a
delivery: ack, record write, reschedule (retry), dead letter or requeue. Backends
must apply one completion atomically. Redis wraps a whole batch of completions in
one `MULTI`/`EXEC`.

Brokers declare `guarantee` (`at-most-once`, `at-least-once` or `ephemeral`) and
`supports_recovery`. The worker enables lease renewal and recovery only for brokers
that support it.

### Adding a backend

Implement `Broker` and pass an instance as `Queue(broker=...)`. The task API,
retry policy, scheduler and worker need no changes. Redis-compatible servers
(Valkey, Dragonfly, KeyDB) should work with the Redis brokers if they support
Streams consumer groups, `XAUTOCLAIM`, Lua scripting and `MULTI`. **They have
not been tested and are not declared supported.**

## Messages

The envelope (`serialization.Envelope`) is a positional MessagePack array:
`id, task, queue, args, kwargs, attempt, created_at, enqueued_at,
correlation_id, headers, timeout`. A no-argument task message is about 60 bytes.
New fields can only be appended with defaults, so messages from older producers
keep decoding.

Only plain data is accepted (see [serialization](../README.md#serialization)).
Nothing received from Redis is ever unpickled. Malformed messages are dead-lettered
with reason `malformed message`, and oversized messages are rejected on both
enqueue and receipt.

## Worker internals

- **One fetch loop per subscribed queue.** Each loop reserves capacity from its
  queue's limiter and from the global limiter *before* taking messages. It fetches
  up to `min(free slots, batch_size)` messages without blocking. When the queue is
  empty, it blocks for one message for at most `block_timeout` seconds while
  holding only a queue slot. Excess work therefore stays in Redis (backpressure),
  and a busy queue cannot consume a quiet queue's fetch capacity.
- **Execution:** `async` tasks run as coroutines, sync tasks on a bounded thread
  pool, and `executor="process"` tasks on a process pool. A timed-out thread or
  process call keeps its concurrency slot until it really returns, so the bound on
  concurrently running code is never exceeded.
- **Group commit:** completions go to a single flusher coroutine. Whatever has
  accumulated while the previous `MULTI` was in flight is written in the next
  one. No timer is involved, so latency under light load is one round-trip, while
  under heavy load many acks share a round-trip.
- **Background loops:** the promoter (scheduled → queue), revocation sync (1 s),
  and maintenance. Maintenance registers the worker with metrics every 2 s, and in
  reliable mode renews leases every `visibility_timeout / 3` and recovers
  abandoned messages.
- **Lease renewal** uses an ownership-checked Lua script: an entry's idle time is
  reset only if this consumer still owns it. That prevents a slow worker from
  stealing back a message another worker already recovered.
- **Graceful shutdown:** stop fetching (fetch loops are not cancelled mid-command,
  so a popped message is never dropped), wait up to `shutdown_timeout` for running
  tasks, cancel the rest and requeue their messages, flush completions, then run
  shutdown hooks.

## Task priority

```python
@queue.task(priority="high")          # decorator-level default
async def urgent(): ...

await task.options(priority="low").enqueue(x)   # per-call override
```

Priority (`"high"`, `"normal"` (default), `"low"`) is implemented as physically
separate broker queues, not a field on the message: enqueuing with
`priority="high"` on queue `"default"` publishes to the queue named
`"default:high"` (`TaskHandle.queue`, `TaskInfo.queue`, dead-letter listings and
metrics all show that name - there's no hidden name mangling to reverse).

Every worker fetch loop for queue `"default"` checks `"default:high"`, then
`"default"`, then `"default:low"`, in that order, on every batch - always, with
no config flag to remember, so a message published at a priority the worker
"wasn't told about" is never silently stranded. When nobody uses priority, the
extra two checks per batch are non-blocking and return empty, costing a
couple of cheap round-trips per *batch* (not per task) - not separately
benchmarked, but the same shape of negligible overhead as the existing
multi-queue fetch loop's per-queue polling.

The three levels **share the queue's one concurrency budget** (`concurrency=`
and `queue_concurrency={"default": N}`, keyed by the base name): priority
reorders which message runs next, it does not add capacity. A worker with
`concurrency=4` running only high-priority backlog never exceeds 4 concurrent
executions just because normal/low levels also have work waiting.

Leases, heartbeat renewal, abandoned-message recovery and reliable mode's
consumer-group setup all operate per physical queue (`"default:high"` has its
own Streams consumer group, its own in-flight lease tracking), so crash
recovery and at-least-once semantics apply identically to every priority
level - a priority message dropped by a crashed worker is redelivered exactly
like any other message.

**Trade-off, not a bug:** when a queue's higher-priority levels are empty and
the worker blocks waiting for new work, it blocks on the *normal* level only.
A message published only to `:low` while the queue is otherwise idle can wait
up to `block_timeout` (default 1 s) longer than it would if published to
`:high` or `:normal` under the same conditions - the same bounded latency
trade-off `block_timeout` already makes for a single queue, not a new one.

Periodic tasks do not currently support priority.

## Periodic tasks

- Interval schedules (`Every`) are aligned to the Unix epoch, so every scheduler
  instance computes identical occurrence times.
- Cron schedules (`Cron`) are evaluated in an IANA timezone. Wall times that don't
  exist because of a DST gap are skipped; repeated wall times fire once.
- On each tick a scheduler reads the last dispatched occurrence of every periodic
  task and applies the missed-run policy to occurrences between that time and now.
  It then calls `claim_periodic`, a Lua compare-and-set that records the occurrence
  and publishes the message atomically. Concurrent schedulers race safely: exactly
  one wins each occurrence.
- Occurrence id: `periodic:<name>:<timestamp>`.
- The scheduler re-evaluates at least every `poll_interval` (1 s), so wall-clock
  jumps take effect within a second. A backwards jump never re-dispatches; a
  forward jump is handled like downtime.

## Sync API

Synchronous methods (`enqueue_sync`, `get_result_sync`, `cancel_sync`, ...) run
coroutines on one private background event loop per `Queue`, in a daemon thread
with its own broker connections. They raise `RuntimeError` if called from a thread
that is running an event loop, rather than blocking it. After `fork()` the
background loop is recreated lazily.
