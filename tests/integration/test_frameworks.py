"""Framework integrations: FastAPI/Starlette lifespan, Django on_commit, Flask."""

import asyncio

import pytest
import redis

from blitzq import Queue
from blitzq.broker import MemoryBroker
from blitzq.integrations.asgi import lifespan
from conftest import REDIS_URL, running, unique_ns

pytestmark = pytest.mark.redis


# -- FastAPI / Starlette -----------------------------------------------------------
async def test_fastapi_lifespan_publish_and_cleanup(redis_app_factory):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    queue = redis_app_factory("reliable")

    @queue.task
    async def process_order(order_id: str) -> dict:
        return {"order_id": order_id, "processed": True}

    inner_events = []

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def my_lifespan(app):
        inner_events.append("start")
        yield
        inner_events.append("stop")

    api = FastAPI(lifespan=lifespan(queue, inner=my_lifespan))

    @api.post("/orders/{order_id}")
    async def create(order_id: str):
        handle = await process_order.enqueue(order_id)
        return {"task_id": handle.id}

    def call_api():
        with TestClient(api) as client:
            assert len(queue.broker._conns) == 1  # type: ignore[attr-defined]  # connected
            return client.post("/orders/ORD-1").json()["task_id"]

    task_id = await asyncio.to_thread(call_api)
    assert len(queue.broker._conns) == 0  # type: ignore[attr-defined]  # closed at shutdown
    assert inner_events == ["start", "stop"]
    async with running(queue):
        assert await queue.get_result(task_id, timeout=10) == {
            "order_id": "ORD-1",
            "processed": True,
        }


async def test_starlette_lifespan(redis_app_factory):
    from starlette.applications import Starlette
    from starlette.responses import JSONResponse
    from starlette.routing import Route
    from starlette.testclient import TestClient

    queue = redis_app_factory("fast")

    @queue.task
    async def ping():
        return "pong"

    async def endpoint(request):
        h = await ping.enqueue()
        return JSONResponse({"id": h.id})

    app = Starlette(routes=[Route("/", endpoint, methods=["POST"])], lifespan=lifespan(queue))

    def call():
        with TestClient(app) as c:
            return c.post("/").json()["id"]

    tid = await asyncio.to_thread(call)
    async with running(queue):
        assert await queue.get_result(tid, timeout=10) == "pong"


# -- Django ------------------------------------------------------------------------
@pytest.fixture(scope="module")
def django_setup():
    import django
    from django.conf import settings

    if not settings.configured:
        settings.configure(
            DATABASES={"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}},
            INSTALLED_APPS=[],
            USE_TZ=True,
        )
        django.setup()


def _queue_len(ns: str) -> int:
    return int(redis.Redis.from_url(REDIS_URL).llen(f"{ns}:l:default"))


def test_django_enqueue_on_commit(django_setup):
    from django.db import transaction

    from blitzq.integrations.django import enqueue_on_commit

    ns = unique_ns()
    queue = Queue(redis_url=REDIS_URL, mode="fast", namespace=ns)

    @queue.task
    def send_receipt(order_id):
        return order_id

    try:
        with transaction.atomic():
            tid = enqueue_on_commit(send_receipt, 42)
            assert isinstance(tid, str)
            assert _queue_len(ns) == 0  # nothing published inside the transaction
        assert _queue_len(ns) == 1  # published on commit

        try:
            with transaction.atomic():
                enqueue_on_commit(send_receipt.options(queue="default"), 43)
                raise RuntimeError("rollback")
        except RuntimeError:
            pass
        assert _queue_len(ns) == 1  # rolled back: nothing published

        enqueue_on_commit(send_receipt, 44)  # autocommit: immediate
        assert _queue_len(ns) == 2
    finally:
        queue.close_sync()
        r = redis.Redis.from_url(REDIS_URL)
        for key in r.scan_iter(f"{ns}:*"):
            r.delete(key)


async def test_django_worker_setup_manages_connections(django_setup):
    from django.db import connection

    from blitzq.integrations import django as dj

    app = Queue(broker=MemoryBroker())
    dj.setup(app, autodiscover=False)
    dj.setup(app, autodiscover=False)  # idempotent
    assert len(app.sync_wrappers) == 1

    @app.task
    def query():
        with connection.cursor() as cur:
            cur.execute("SELECT 1")
            return cur.fetchone()[0]

    async with running(app):
        assert await (await query.enqueue()).result(10) == 1


# -- Flask -------------------------------------------------------------------------
async def test_flask_publish_and_app_context(redis_app_factory):
    from flask import Flask, current_app, request

    from blitzq.integrations.flask import get_queue, init_app

    queue = redis_app_factory("reliable")
    flask_app = Flask("testapp")
    flask_app.config["GREETING"] = "hello"
    init_app(flask_app, queue)

    @queue.task
    def greet(name):
        # Runs in the worker with an application context, never a request context.
        from flask import has_request_context

        assert not has_request_context()
        return f"{current_app.config['GREETING']} {name}"

    @flask_app.post("/greet")
    def view():
        h = greet.enqueue_sync(request.json["name"])
        assert get_queue() is queue
        return {"task_id": h.id}

    def call():
        return flask_app.test_client().post("/greet", json={"name": "ada"}).json["task_id"]

    tid = await asyncio.to_thread(call)
    async with running(queue):
        assert await queue.get_result(tid, timeout=10) == "hello ada"
