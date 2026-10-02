# Delivery guarantees

BlitzQ never promises exactly-once delivery or exactly-once execution. Nothing
built on Redis (or on most brokers) can promise that for tasks with external side
effects. This page states what each mode actually guarantees, where tasks can be
duplicated or lost, and how retries, results and cancellation interact with that.

## The two Redis modes

| | `mode="reliable"` (default) | `mode="fast"` |
|---|---|---|
| Redis structure | Stream per queue, consumer group `blitzq` | List per queue |
| Claiming a message | `XREADGROUP`: atomic hand-off into the pending-entries list (PEL) | `RPOP`/`BRPOP`: message removed from Redis |
| Acknowledgement | `XACK`+`XDEL` **after** the task finishes, in one `MULTI` with its result/retry/dead-letter write | none needed |
| Worker crash while executing | message redelivered to another worker after `visibility_timeout` | **message lost** |
| Worker crash with prefetched (unstarted) messages | redelivered | **lost** (at most `batch_size` per queue) |
| Graceful shutdown (SIGTERM/SIGINT) | unfinished tasks are returned to the queue | unfinished tasks are returned to the queue |
| Guarantee | **at-least-once** | **at-most-once** |
| Per-task Redis round-trips on success (no results) | fetch (amortised) + ack (group-committed) | fetch (amortised) |

`MemoryBroker` is **ephemeral**: everything lives in one Python process and is lost
when it exits. Use it only for tests, notebooks and scripts.

### When can a task run twice (reliable mode)?

- The worker executed the task but died, hung or lost Redis before its ack was
  written. The message is redelivered and runs again.
- A worker stopped renewing its lease for longer than `visibility_timeout`. This
  happens if its event loop was blocked by CPU-heavy or blocking code in an `async`
  task, or if it was partitioned from Redis. Another worker then recovers the message
  while the first may still be running it. The first worker logs
  `lease lost; message may be executed again`.
- A graceful shutdown timed out and the message was requeued while a
  thread-executor call was still running in the background.
- A connection dropped after a publish reached Redis but before the reply
  arrived, and the caller retried the enqueue.

**Tasks with external side effects must be idempotent.** BlitzQ gives you the
tools for that, see [Idempotency keys](#idempotency-keys) below, and you can also
use the task id (`current_task().id`), which stays the same across retries and
redeliveries, as a key towards payment providers, or record completion in your
database under a unique constraint.

## Idempotency keys

An idempotency key names one *logical job*, so it takes effect once however many
times it is enqueued or delivered:

```python
queue = Queue(idempotency_ttl=24 * 3600)            # how long a key is remembered

@queue.task(idempotency_key=lambda order: f"charge:{order['id']}")
async def charge(order):
    # forward the key to systems that accept their own idempotency key
    await gateway.charge(order, idempotency_key=current_task().idempotency_key)

await charge.enqueue(order)                         # derived from the arguments
await charge.options(idempotency_key="inv-9").enqueue(order)   # or set per call
```

Two things use the key, both through the broker, so they work across every
producer and worker process:

1. **At enqueue.** The key is claimed atomically before the message is published.
   A second enqueue with the same key (a retried HTTP request, a double click, a
   producer that restarted) publishes nothing and returns a handle to the first
   task, so `handle.result()` works for both callers. This also applies inside one
   `enqueue_many` batch.
2. **At execution.** Before the task body runs, the worker takes a leased lock for
   the key:
   - If the job already **succeeded**, the body is skipped and the recorded result
     is returned. This covers a redelivery after the result was recorded but before
     the ack, and a duplicate that reached a worker because its claim had expired.
   - If another execution is **still running**, the delivery is rescheduled
     (about a second later) instead of running concurrently. This covers a lost
     lease with the first worker still running, and two workers racing.
   - If the previous owner **crashed**, its lock lapses after the lease
     (`visibility_timeout`, renewed while the task runs) and the key can run again.

Behaviour worth knowing:

- Keys are **scoped to the task**: `charge` and `notify` can both use `order-1`
  without colliding. The key you pass is what `current_task().idempotency_key`
  returns. Keys are non-empty strings up to 512 characters.
- A **failed attempt releases the lock**, so retries run normally. A job that ends
  without succeeding (dead-lettered, or cancelled) **releases the key**, so you can
  submit it again. A job that **succeeded** keeps its key for `idempotency_ttl`
  (counted again from completion); it is never released early.
- The first recorded result wins, so every later duplicate sees one consistent
  answer. The result is stored with the key only when the task stores results
  (`store_result`); otherwise duplicates are acknowledged with no result.
- Cost: nothing for tasks without a key. With a key, one atomic Redis call at
  enqueue, and two at execution (gate and record), plus a lock renewal every
  `visibility_timeout / 3` while the task runs. Each key is a small hash and a
  lock in Redis, so use bounded, meaningful ids rather than random values.
- Works in both modes. In fast mode (at-most-once) a lost message is still lost,
  but a key can never cause a duplicate effect.

What this **cannot** do: if a worker performs the side effect and then crashes
*before* the result is recorded, the next owner runs the body again. No queue can
close that window, because the effect and the record live in different systems.
Close it by passing the key to the system that holds the effect, which is why
`current_task().idempotency_key` exists (payment providers, email and SMS APIs and
most cloud APIs accept an idempotency key), or by writing your effect and your own
completion marker in one database transaction.

A task that **times out on the thread or process executor** cannot be killed (see
[Timeouts and hung tasks](operations.md#timeouts-and-hung-tasks)), so its side
effect may still happen after its lock is released and the retry has started. The
same remedy applies.

### When can a task be lost?

- Fast mode: any message a worker has popped (executing or prefetched) when that
  worker process dies.
- Both modes: Redis loses data. BlitzQ is exactly as durable as your Redis
  persistence configuration (see [operations.md](operations.md)). With no
  persistence (`appendonly no`, no RDB) a Redis restart loses every queued,
  scheduled and in-flight message.
- Both modes: `purge` or a dead-letter store trimmed at `dead_letter_max` entries
  (oldest are dropped).

### Poison messages

A message that crashes its worker every time would otherwise be redelivered forever.
In reliable mode each redelivery increments the stream's delivery counter; once it
exceeds `max_deliveries` (default 5) the message is dead-lettered with reason
`max deliveries exceeded`.

## Retries

- `retries=N` allows N retries, which is N + 1 *attempts*. `TaskContext.attempt` is
  1-based and `TaskContext.retries == attempt - 1`.
- A retry keeps the **same task id**. It is a new message with `attempt + 1` written
  to the schedule (a Redis sorted set). In reliable mode this happens in the same
  `MULTI` transaction that acknowledges the failed delivery, so a retry is never both
  lost and acknowledged.
- Workers never sleep during backoff. Retries become runnable when the scheduler
  promotes them, and they then pass through the normal queue, so they respect
  per-queue concurrency limits and backpressure like any other message.
- An explicit `raise Retry(delay=...)` counts toward the attempt limit, so tasks
  cannot retry forever.
- After the last attempt (or a non-retryable exception) the task is dead-lettered
  (`dead_letter=True`, the default) or recorded as `failed`.
- Retrying a task does **not** make its side effects happen exactly once. An
  attempt that timed out may have completed its side effect.

## Results and task state

- With `store_results=True` (default) the worker writes the final record
  (`succeeded`, `failed`, `dead_lettered` or `cancelled`, plus result or error) in
  the same transaction as the acknowledgement. A reader that sees a final state
  therefore knows the message was acknowledged.
- Records expire after `result_ttl` seconds (default 24 h).
- With `track_state=True` producers write `queued`/`scheduled` and workers write
  `running`/`retrying`. Without it, a task that is queued or running has **no
  record**, and `inspect()` returns `None` unless the task is scheduled or
  dead-lettered.
- Records reflect the last write. During concurrent transitions a reader can briefly
  see a stale state, for example `queued` just after a worker started the task, or
  the state of the first execution while a redelivered duplicate is running. When a
  task runs twice, the record of the last execution to finish wins.
- `get_result()` is notification-driven on Redis brokers: the worker
  publishes on a per-task channel in the same transaction as the final
  record write, and one shared `PSUBSCRIBE` connection per process (per
  event loop) fans incoming notifications out to whichever local
  `get_result()` calls are waiting, so it wakes up close to immediately
  rather than on a poll interval - however many tasks are in flight at
  once, this costs one Redis connection, not one per waiting call. It
  re-checks the record itself after every wake-up (a missed notification
  just means it waits again), and falls back to a 5 s safety-net poll so a
  dropped one costs seconds, not an indefinite hang. `MemoryBroker` uses an
  in-process `asyncio.Condition` instead of pub/sub, with the same
  behavior. It needs result storage for that task; otherwise it waits until
  its timeout.

## Cancellation

- `cancel(task_id)` on a **scheduled** task (delayed, ETA or waiting for a retry)
  removes it atomically. That cancellation is certain and returns `True`.
- For a queued or running task, `cancel()` records a revocation (kept for
  `revoke_ttl`, default 1 h) and returns `False`. Workers load revocations at
  startup and re-sync them every `revocation_interval` (1 s). A queued task
  received after the sync is skipped and recorded as `cancelled`. A running
  **async** task is cancelled with `asyncio.CancelledError`. Running thread and
  process tasks cannot be interrupted; their result is still recorded.
- A task that finished before the revocation was seen is unaffected.

## Scheduling and periodic tasks

- Delayed tasks and retries live in one sorted set scored by due time. Due
  entries are moved into their queues by a Lua script, which is atomic. Every
  worker (and every `blitzq scheduler`) runs this promotion, so there is no single
  point of failure and no entry is promoted twice.
- Promotion latency: a promoter sleeps until the earliest known due time, but
  never longer than `schedule_poll_interval` (default 0.5 s). A newly scheduled
  task that is due earlier than anything already known can therefore be up to that
  long late.
- Periodic tasks: see [architecture.md](architecture.md#periodic-tasks). Each
  occurrence has a deterministic id and is dispatched through a compare-and-set, so
  any number of scheduler instances dispatch it **at most once**. If every scheduler
  is down at an occurrence's time, the missed-run policy decides what happens on
  restart.
