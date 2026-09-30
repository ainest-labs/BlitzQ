import subprocess
import sys
from pathlib import Path

import blitzq


def test_version_and_public_api():
    assert blitzq.__version__.count(".") == 2
    for name in blitzq.__all__:
        assert hasattr(blitzq, name), name


def test_py_typed_marker_present():
    assert (Path(blitzq.__file__).parent / "py.typed").exists()


def test_core_import_does_not_pull_in_web_frameworks():
    code = (
        "import sys, blitzq, blitzq.worker, blitzq.scheduler, blitzq.cli, "
        "blitzq.integrations.asgi\n"
        "bad = [m for m in ('fastapi', 'starlette', 'django', 'flask', 'celery') "
        "if m in sys.modules]\n"
        "print(','.join(bad))"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == ""


def test_cli_entry_point_help():
    out = subprocess.run(
        [sys.executable, "-m", "blitzq", "--help"], capture_output=True, text=True, check=True
    )
    for cmd in ("worker", "scheduler", "queue", "task", "dead-letter", "benchmark"):
        assert cmd in out.stdout
