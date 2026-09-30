from typing import Any

from ..workloads import Workload
from .base import Adapter


def get_adapter(
    system: str, settings: dict[str, Any], workload: Workload, broker_url: str
) -> Adapter:
    if system == "blitzq":
        from .blitzq import BlitzQAdapter

        return BlitzQAdapter(settings, workload, broker_url)
    if system == "celery":
        from .celery import CeleryAdapter

        return CeleryAdapter(settings, workload, broker_url)
    if system == "huey":
        from .huey import HueyAdapter

        return HueyAdapter(settings, workload, broker_url)
    raise ValueError(f"unknown system {system!r}")
