# Framework integration

The core package imports no web framework. Integrations are thin and optional:
they manage connection lifecycle, application context and transaction timing.
They never provide a different queue implementation.

**Deployment model:** web processes *publish* tasks; separate `blitzq worker`
processes *execute* them. Workers import the same module that defines your
`Queue` and tasks, but do not inherit HTTP request context, database sessions,
authenticated users or any application memory of the web process. Pass primitive
arguments (ids, strings, numbers) and load what the task needs inside the task.
BlitzQ never serializes request objects, sessions or ORM instances for you, and
rejects arguments it cannot encode as plain data.

## Async frameworks (FastAPI, Starlette, Litestar, Quart, aiohttp, Sanic)

Use the async API (`await task.enqueue(...)`). Never call `*_sync` methods inside
an async view. They raise `RuntimeError` rather than block the event loop.

```python
from fastapi import FastAPI
from blitzq.integrations.asgi import lifespan

app = FastAPI(lifespan=lifespan(queue))            # or lifespan(queue, inner=your_lifespan)
```

`lifespan` works with any ASGI framework that accepts a lifespan callable
(Starlette, FastAPI, Litestar via `lifespan=[...]`). It connects at startup, so
a wrong Redis URL fails fast, and closes connections at shutdown. For aiohttp use
`app.on_startup`/`app.on_cleanup` with `queue.connect()`/`queue.close()`; for
Sanic use `before_server_start`/`after_server_stop`.

Tested: FastAPI and Starlette (lifespan, publishing, connection cleanup).

## Django

```python
# myproject/tasks_app.py
from blitzq import Queue
from blitzq.integrations import django as bq_django

app = Queue("default", redis_url="redis://localhost:6379/0")
bq_django.setup(app, settings_module="myproject.settings")  # django.setup() + autodiscovery

# myapp/tasks.py
from myproject.tasks_app import app

@app.task
def send_receipt(order_id: int) -> None:
    order = Order.objects.get(pk=order_id)   # load by id inside the task
    ...
```

Publish after the transaction commits, so the worker never sees uncommitted or
rolled-back rows:

```python
from django.db import transaction
from blitzq.integrations.django import enqueue_on_commit

with transaction.atomic():
    order = Order.objects.create(...)
    task_id = enqueue_on_commit(send_receipt, order.pk)   # id returned immediately
```

If the transaction rolls back, nothing is published. Outside a transaction the
task is published immediately.

Run workers with `blitzq worker myproject.tasks_app:app`. `setup()`:

- initialises Django if needed (`DJANGO_SETTINGS_MODULE`);
- imports `<app>.tasks` for every installed app (`autodiscover=True`);
- wraps every sync task in `close_old_connections()` inside the task's own thread,
  the same connection hygiene Django applies around requests.

Async tasks must use Django's async ORM API or `sync_to_async`.

Tested: `enqueue_on_commit` (commit, rollback, autocommit) and worker-side
connection management.

## Flask

```python
from blitzq.integrations.flask import init_app

queue = Queue("default", redis_url="redis://localhost:6379/0")
app = Flask(__name__)
init_app(app, queue)

@app.post("/reports")
def create_report():
    handle = build_report.enqueue_sync(request.json["report_id"])
    return {"task_id": handle.id}, 202
```

Views use the synchronous API. On the worker, sync tasks run inside
`app.app_context()` (so `current_app` and extensions such as Flask-SQLAlchemy
work), never inside a request context.

Tested: publishing from a view and app-context access in the worker.

## Plain scripts, CLIs, data pipelines

```python
handle = my_task.enqueue_sync(42)          # no event loop needed
print(handle.result_sync(timeout=30))
```

or `asyncio.run(...)` with the async API. Call `queue.close_sync()` at the end of
long-running sync programs (it also runs automatically at interpreter exit).

## Jupyter

Notebooks already run an event loop, so use `await` directly in cells:
`await task.enqueue(...)` and `await queue.get_result(id)`. The `*_sync` methods
raise `RuntimeError` there by design.

## Other frameworks (Pyramid, Bottle, Litestar, ...)

Any synchronous framework can use the `*_sync` API, and any asyncio framework the
async API. The only integration work is closing connections at shutdown
(`queue.close()` / `queue.close_sync()`).
