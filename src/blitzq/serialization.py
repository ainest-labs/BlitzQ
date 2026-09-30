"""Message envelopes and safe serialization.

BlitzQ never unpickles data received from a broker. Messages are encoded with
msgspec as MessagePack (default) or JSON, which can only produce plain data:
``None``, ``bool``, ``int``, ``float``, ``str``, ``bytes``, lists and dicts
(plus ``datetime`` for MessagePack). Values such as ``uuid.UUID``,
``decimal.Decimal``, dataclasses and ``msgspec.Struct`` instances are accepted
when enqueuing but arrive in the worker in their plain-data form (``str`` or
``dict``). Pass identifiers and primitive data rather than live objects.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal

import msgspec

from .exceptions import MessageTooLarge, SerializationError
from .state import ErrorInfo, TaskInfo

DEFAULT_MAX_MESSAGE_SIZE = 8 * 1024 * 1024


class Envelope(msgspec.Struct, array_like=True):
    """The on-the-wire task message.

    Encoded positionally (``array_like``) for compactness; new fields must be
    appended with defaults so older messages keep decoding.
    """

    id: str
    task: str
    queue: str
    args: list[Any]
    kwargs: dict[str, Any]
    attempt: int = 1
    created_at: float = 0.0
    # Wall-clock time this attempt became runnable (publish time, or the ETA
    # for scheduled work). Used for queue-latency metrics.
    enqueued_at: float = 0.0
    correlation_id: str | None = None
    headers: dict[str, str] | None = None
    timeout: float | None = None


class DeadLetter(msgspec.Struct, omit_defaults=True):
    """A terminally failed message kept for inspection and manual replay."""

    id: str
    queue: str
    task: str
    reason: str
    failed_at: float
    attempt: int = 0
    error: ErrorInfo | None = None
    # The original encoded envelope (may be undecodable if the message was malformed).
    message: bytes = b""


class Serializer:
    """Encodes envelopes, task records and results.

    Parameters
    ----------
    format:
        ``"msgpack"`` (compact, default) or ``"json"`` (human-readable in Redis).
    enc_hook:
        Optional ``msgspec`` encode hook to convert otherwise unsupported
        argument types into supported ones (for example ``lambda o: o.to_dict()``).
    max_message_size:
        Upper bound for an encoded message, enforced on both enqueue and receipt.
    """

    def __init__(
        self,
        format: Literal["msgpack", "json"] = "msgpack",
        *,
        enc_hook: Callable[[Any], Any] | None = None,
        max_message_size: int = DEFAULT_MAX_MESSAGE_SIZE,
    ) -> None:
        self.format = format
        self.max_message_size = max_message_size
        mod: Any
        if format == "msgpack":
            mod = msgspec.msgpack
        elif format == "json":
            mod = msgspec.json
        else:
            raise ValueError(f"unsupported serializer format: {format!r}")
        self._encoder = mod.Encoder(enc_hook=enc_hook)
        self._env_decoder = mod.Decoder(Envelope)
        self._info_decoder = mod.Decoder(TaskInfo)
        self._dead_decoder = mod.Decoder(DeadLetter)
        self._any_decoder = mod.Decoder()

    # -- envelopes -----------------------------------------------------------------
    def encode_envelope(self, env: Envelope) -> bytes:
        try:
            data: bytes = self._encoder.encode(env)
        except (TypeError, ValueError, OverflowError) as exc:
            raise SerializationError(
                f"cannot serialize arguments for task {env.task!r}: {exc}"
            ) from exc
        if len(data) > self.max_message_size:
            raise MessageTooLarge(
                f"message for task {env.task!r} is {len(data)} bytes, "
                f"exceeding max_message_size={self.max_message_size}"
            )
        return data

    def decode_envelope(self, data: bytes) -> Envelope:
        if len(data) > self.max_message_size:
            raise MessageTooLarge(f"received message of {len(data)} bytes")
        try:
            env: Envelope = self._env_decoder.decode(data)
        except (msgspec.DecodeError, msgspec.ValidationError) as exc:
            raise SerializationError(f"malformed task message: {exc}") from exc
        return env

    # -- records -------------------------------------------------------------------
    def encode_info(self, info: TaskInfo) -> bytes:
        try:
            return self._encoder.encode(info)  # type: ignore[no-any-return]
        except (TypeError, ValueError, OverflowError) as exc:
            raise SerializationError(f"cannot serialize result of task {info.id}: {exc}") from exc

    def decode_info(self, data: bytes) -> TaskInfo:
        try:
            return self._info_decoder.decode(data)  # type: ignore[no-any-return]
        except (msgspec.DecodeError, msgspec.ValidationError) as exc:
            raise SerializationError(f"malformed task record: {exc}") from exc

    def encode_dead(self, dead: DeadLetter) -> bytes:
        return self._encoder.encode(dead)  # type: ignore[no-any-return]

    def decode_dead(self, data: bytes) -> DeadLetter:
        try:
            return self._dead_decoder.decode(data)  # type: ignore[no-any-return]
        except (msgspec.DecodeError, msgspec.ValidationError) as exc:
            raise SerializationError(f"malformed dead-letter record: {exc}") from exc

    def check_value(self, value: Any) -> None:
        """Raise SerializationError if ``value`` cannot be stored as a result."""
        try:
            self._encoder.encode(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise SerializationError(f"result is not serializable: {exc}") from exc
