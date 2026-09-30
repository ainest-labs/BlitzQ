"""Flask integration (``pip install "blitzq[flask]"``).

::

    from flask import Flask
    from blitzq import Queue
    from blitzq.integrations.flask import init_app

    queue = Queue("default")
    flask_app = Flask(__name__)
    init_app(flask_app, queue)

    @flask_app.post("/reports")
    def create_report():
        handle = build_report.enqueue_sync(request.json["report_id"])
        return {"task_id": handle.id}

Views publish with the synchronous API (``enqueue_sync``). Tasks never see a
request context - pass the data they need as arguments. When a worker runs
tasks for this queue, sync tasks execute inside ``flask_app.app_context()`` so
extensions that need ``current_app`` (Flask-SQLAlchemy, config access) work.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ..client import Queue


def init_app(flask_app: Any, queue: Queue, *, app_context_in_worker: bool = True) -> None:
    """Register ``queue`` on ``flask_app`` (as ``app.extensions["blitzq"]``)."""
    flask_app.extensions["blitzq"] = queue
    if app_context_in_worker:

        def with_app_context(call: Callable[[], Any]) -> Any:
            with flask_app.app_context():
                return call()

        queue.add_sync_wrapper(with_app_context)


def get_queue(flask_app: Any = None) -> Queue:
    """The queue registered on ``flask_app`` (default: ``flask.current_app``)."""
    if flask_app is None:
        from flask import current_app

        flask_app = current_app
    queue: Queue = flask_app.extensions["blitzq"]
    return queue
