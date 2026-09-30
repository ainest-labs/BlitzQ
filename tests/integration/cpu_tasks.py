"""Module-level functions for process-executor tests (importable by child processes)."""

import os


def fib(n: int) -> tuple[int, int]:
    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b
    return a, os.getpid()
