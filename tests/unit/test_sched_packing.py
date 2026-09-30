"""The scheduled-entry pack/unpack helpers (one Redis hash field, not two)."""

import pytest

from blitzq.broker.redis_base import _pack_sched, _unpack_sched


@pytest.mark.parametrize(
    ("queue", "data"),
    [
        ("default", b"hello"),
        ("emails", b""),
        ("q", b"\x00\x01\xff binary payload with weird bytes \x00"),
        ("a-very-long-queue-name-1234567890", b"x" * 500),
    ],
)
def test_roundtrip(queue: str, data: bytes) -> None:
    assert _unpack_sched(_pack_sched(queue, data)) == (queue, data)


def test_data_containing_nul_bytes_survives():
    # Only the FIRST NUL is the separator; the message itself may contain any bytes.
    packed = _pack_sched("q", b"a\x00b\x00c")
    assert _unpack_sched(packed) == ("q", b"a\x00b\x00c")
