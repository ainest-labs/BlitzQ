"""Task-to-queue routing."""

from __future__ import annotations

import fnmatch
from collections.abc import Callable, Mapping

RouteFunction = Callable[[str], str | None]
Routes = Mapping[str, str] | RouteFunction | None


class Router:
    """Resolves the queue for a task name.

    Precedence (highest first):

    1. ``queue=`` passed to ``task.options(...)`` at enqueue time
    2. ``queue=`` given to the ``@app.task`` decorator
    3. routing rules configured on the application (this class)
    4. the application's default queue

    Rules are either a mapping of glob patterns to queue names, evaluated in
    insertion order (``{"app.images.*": "images"}``), or a callable returning a
    queue name or ``None``.
    """

    def __init__(self, routes: Routes, default: str) -> None:
        self.default = default
        self._fn: RouteFunction | None = None
        self._patterns: list[tuple[str, str]] = []
        if callable(routes):
            self._fn = routes
        elif routes:
            self._patterns = list(routes.items())
        self._cache: dict[str, str | None] = {}

    def route(self, task_name: str) -> str | None:
        """Queue selected by routing rules, or ``None`` if no rule matches."""
        if self._fn is not None:
            return self._fn(task_name)
        try:
            return self._cache[task_name]
        except KeyError:
            pass
        found = None
        for pattern, queue in self._patterns:
            if fnmatch.fnmatchcase(task_name, pattern):
                found = queue
                break
        self._cache[task_name] = found
        return found

    def resolve(self, task_name: str, decorator_queue: str | None, call_queue: str | None) -> str:
        return call_queue or decorator_queue or self.route(task_name) or self.default
