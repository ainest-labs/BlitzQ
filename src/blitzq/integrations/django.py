"""Django integration (``pip install "blitzq[django]"``).

Publishing after commit
-----------------------
A task enqueued inside a transaction can be picked up by a worker before the
transaction commits (the worker would not see the new rows) or after it rolls
back. :func:`enqueue_on_commit` defers publishing until the transaction
commits and does nothing on rollback::

    from django.db import transaction
    from blitzq.integrations.django import enqueue_on_commit

    with transaction.atomic():
        order = Order.objects.create(...)
        task_id = enqueue_on_commit(send_receipt, order.pk)

Workers
-------
Call :func:`setup` in the module that defines your ``Queue`` so that
``blitzq worker myproject.tasks:app`` initialises Django before importing task
modules. It also wraps every sync task in ``close_old_connections()`` (in the
task's own thread), the same connection hygiene Django applies around
requests. Async tasks must use Django's async ORM API or ``sync_to_async``.
"""

from __future__ import annotations

import importlib
import os
from collections.abc import Callable
from typing import Any

from ..client import Queue
from ..task import BoundTask, Task, new_task_id


def setup(queue: Queue, *, settings_module: str | None = None, autodiscover: bool = True) -> None:
    """Initialise Django (if needed) and install connection handling for sync tasks."""
    import django
    from django.apps import apps

    if settings_module:
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", settings_module)
    if not apps.ready:
        django.setup()
    if not getattr(queue, "_blitzq_django", False):
        queue.add_sync_wrapper(_with_connection_cleanup)
        queue._blitzq_django = True  # type: ignore[attr-defined]
    if autodiscover:
        autodiscover_tasks()


def _with_connection_cleanup(call: Callable[[], Any]) -> Any:
    from django.db import close_old_connections

    close_old_connections()
    try:
        return call()
    finally:
        close_old_connections()


def autodiscover_tasks(module: str = "tasks") -> list[str]:
    """Import ``<app>.<module>`` for every installed Django app that has one."""
    from django.apps import apps

    imported = []
    for cfg in apps.get_app_configs():
        name = f"{cfg.name}.{module}"
        try:
            importlib.import_module(name)
        except ModuleNotFoundError as exc:
            if exc.name != name:
                raise
            continue
        imported.append(name)
    return imported


def enqueue_on_commit(
    task: Task[Any, Any] | BoundTask[Any, Any],
    *args: Any,
    using: str | None = None,
    **kwargs: Any,
) -> str:
    """Publish ``task(*args, **kwargs)`` when the current transaction commits.

    Returns the task id immediately (it is generated up front). Outside a
    transaction (autocommit) Django runs the callback immediately. If the
    transaction rolls back nothing is published. Uses the synchronous enqueue
    API, which is safe in async views because Django runs ORM transactions in
    worker threads.
    """
    from django.db import transaction

    if isinstance(task, BoundTask):
        base, call = task.task, task.call
        task_id = call.task_id or new_task_id()
        bound = base.options(
            queue=call.queue,
            eta=call.eta,
            task_id=task_id,
            correlation_id=call.correlation_id,
            headers=call.headers,
            timeout=call.timeout,
        )
    else:
        task_id = new_task_id()
        bound = task.options(task_id=task_id)

    transaction.on_commit(lambda: bound.enqueue_sync(*args, **kwargs), using=using)
    return task_id
