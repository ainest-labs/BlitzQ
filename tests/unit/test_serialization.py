import datetime as dt
import uuid
from decimal import Decimal

import msgspec
import pytest

from blitzq import MessageTooLarge, SerializationError, Serializer
from blitzq.serialization import DeadLetter, Envelope
from blitzq.state import ErrorInfo, TaskInfo, TaskState


def env(**kw):
    base = dict(id="t1", task="m.f", queue="default", args=[1, "a"], kwargs={"k": [1, 2]})
    base.update(kw)
    return Envelope(**base)


@pytest.mark.parametrize("fmt", ["msgpack", "json"])
def test_envelope_roundtrip(fmt):
    s = Serializer(fmt)
    e = env(attempt=3, correlation_id="c", headers={"traceparent": "x"}, timeout=2.5)
    assert s.decode_envelope(s.encode_envelope(e)) == e


def test_msgpack_is_compact():
    s = Serializer()
    data = s.encode_envelope(env(args=[], kwargs={}))
    # Positional encoding: no field names on the wire.
    assert b"attempt" not in data
    assert len(data) < 80


def test_older_short_envelope_still_decodes():
    # Trailing fields are optional so messages from older producers decode.
    raw = msgspec.msgpack.encode(["t1", "m.f", "q", [], {}])
    e = Serializer().decode_envelope(raw)
    assert e.attempt == 1 and e.headers is None


def test_supported_argument_types_arrive_as_plain_data():
    s = Serializer()
    when = dt.datetime(2026, 1, 2, 3, 4, 5, tzinfo=dt.UTC)
    u = uuid.uuid4()
    e = s.decode_envelope(s.encode_envelope(env(args=[when, u, Decimal("1.5"), b"\x00"])))
    assert e.args == [when, str(u), "1.5", b"\x00"]


def test_unsupported_argument_type_rejected_at_enqueue():
    s = Serializer()
    with pytest.raises(SerializationError):
        s.encode_envelope(env(args=[object()]))


def test_enc_hook_allows_custom_types():
    class Point:
        def __init__(self, x, y):
            self.x, self.y = x, y

    s = Serializer(enc_hook=lambda o: {"x": o.x, "y": o.y})
    e = s.decode_envelope(s.encode_envelope(env(args=[Point(1, 2)])))
    assert e.args == [{"x": 1, "y": 2}]


def test_max_message_size_enforced_both_ways():
    s = Serializer(max_message_size=100)
    with pytest.raises(MessageTooLarge):
        s.encode_envelope(env(args=["x" * 200]))
    big = Serializer().encode_envelope(env(args=["x" * 200]))
    with pytest.raises(MessageTooLarge):
        s.decode_envelope(big)


@pytest.mark.parametrize(
    "payload",
    [b"", b"\xff\xff\xff", b"not msgpack at all", msgspec.msgpack.encode({"id": 1})],
)
def test_malformed_messages_raise_serialization_error(payload):
    with pytest.raises(SerializationError):
        Serializer().decode_envelope(payload)


def test_pickle_payload_is_not_executed():
    import pickle

    class Evil:
        def __reduce__(self):
            return (exec, ("raise SystemExit('pwned')",))

    with pytest.raises(SerializationError):
        Serializer().decode_envelope(pickle.dumps(Evil()))


def test_task_info_and_dead_letter_roundtrip():
    s = Serializer()
    info = TaskInfo(
        id="x",
        state=TaskState.FAILED,
        attempt=2,
        started_at=1.0,
        finished_at=3.5,
        error=ErrorInfo("ValueError", "bad"),
        result={"a": 1},
    )
    back = s.decode_info(s.encode_info(info))
    assert back == info
    assert back.duration == 2.5 and back.retries == 1
    d = DeadLetter(id="x", queue="q", task="t", reason="r", failed_at=1.0, message=b"raw")
    assert s.decode_dead(s.encode_dead(d)) == d


def test_unknown_format():
    with pytest.raises(ValueError):
        Serializer("pickle")  # type: ignore[arg-type]
