"""Structured logging helpers.

BlitzQ logs through the standard ``logging`` module under the ``blitzq``
logger and attaches context (task id, task name, queue, attempt) as record
attributes. Task arguments and results are never logged.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from typing import Any

logger = logging.getLogger("blitzq")

_STANDARD_ATTRS = frozenset(
    logging.LogRecord("", 0, "", 0, "", (), None).__dict__.keys() | {"message", "asctime"}
)


class JSONFormatter(logging.Formatter):
    """One JSON object per line with extra fields merged in."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname.lower(),
            "logger": record.name,
            "msg": record.getMessage(),
        }
        payload.update(
            (k, v)
            for k, v in record.__dict__.items()
            if k not in _STANDARD_ATTRS and not k.startswith("_")
        )
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class _KeyValueFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        extras = [
            f"{k}={v}"
            for k, v in record.__dict__.items()
            if k not in _STANDARD_ATTRS and not k.startswith("_")
        ]
        return f"{base} {' '.join(extras)}" if extras else base


def configure_logging(level: str = "INFO", fmt: str = "text") -> None:
    """Configure the ``blitzq`` logger for CLI processes (``fmt``: text|json)."""
    handler = logging.StreamHandler(sys.stderr)
    if fmt == "json":
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(
            _KeyValueFormatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s")
        )
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
