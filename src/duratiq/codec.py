"""Pluggable payload codec for the JSON columns that hold workflow data.

Workflow inputs, results, errors, step inputs/results, and signal payloads are all
memoized in Postgres. Large payloads bloat the history; some shouldn't sit in the
database at all. A :class:`PayloadCodec` is the seam to intervene — compress them,
or offload big blobs to S3 and store only a reference — applied transparently at the
SQLAlchemy type layer, so neither the engine nor workflow code changes.

The default is :class:`IdentityCodec` (a pass-through), so behaviour is unchanged
until you install one. The active codec is process-global; set it once at startup:

    from duratiq import set_payload_codec
    set_payload_codec(MyS3OffloadingCodec())

A codec must round-trip: ``decode(encode(value)) == value``, and ``encode`` must
return something JSON-serialisable.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from sqlalchemy import JSON
from sqlalchemy.types import TypeDecorator


@runtime_checkable
class PayloadCodec(Protocol):
    """Transforms a payload on the way into and out of the database."""

    def encode(self, value: Any) -> Any: ...

    def decode(self, value: Any) -> Any: ...


class IdentityCodec:
    """The default no-op codec: payloads are stored verbatim."""

    def encode(self, value: Any) -> Any:
        return value

    def decode(self, value: Any) -> Any:
        return value


_codec: PayloadCodec = IdentityCodec()


def set_payload_codec(codec: PayloadCodec) -> None:
    """Install the process-global payload codec (call once at startup)."""
    global _codec
    _codec = codec


def get_payload_codec() -> PayloadCodec:
    """Return the active payload codec."""
    return _codec


class CodecJSON(TypeDecorator):
    """A JSON column type that runs values through the active payload codec.

    ``encode`` on the way to the database, ``decode`` on the way back — so the codec
    sees every persisted payload while the rest of the code keeps using plain Python
    values. ``None`` (SQL NULL) bypasses the codec.
    """

    impl = JSON
    cache_ok = True

    def process_bind_param(self, value: Any, dialect: Any) -> Any:
        if value is None:
            return None
        return get_payload_codec().encode(value)

    def process_result_value(self, value: Any, dialect: Any) -> Any:
        if value is None:
            return None
        return get_payload_codec().decode(value)
