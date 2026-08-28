"""Canonical encodings used by schema, configuration, and content hashes."""

from __future__ import annotations

import hashlib
import json
import struct
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def canonical_json_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _timestamp_microseconds(value: datetime) -> int:
    if value.tzinfo is None:
        raise ValueError("canonical timestamps must be timezone-aware")
    utc_value = value.astimezone(UTC)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = utc_value - epoch
    return ((delta.days * 86_400 + delta.seconds) * 1_000_000) + delta.microseconds


def canonical_scalar(field: pa.Field, value: object) -> bytes:
    """Encode a scalar with an explicit type tag and no locale dependence."""

    if value is None:
        if not field.nullable:
            raise ValueError(f"non-nullable field {field.name!r} received null")
        return b"N"
    data_type = field.type
    if pa.types.is_integer(data_type):
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"field {field.name!r} expected integer, got {type(value).__name__}")
        return b"I" + str(value).encode("ascii")
    if pa.types.is_string(data_type):
        if not isinstance(value, str):
            raise TypeError(f"field {field.name!r} expected string, got {type(value).__name__}")
        return b"S" + value.encode("utf-8")
    if pa.types.is_decimal(data_type):
        if not isinstance(value, Decimal):
            raise TypeError(f"field {field.name!r} expected Decimal, got {type(value).__name__}")
        quantum = Decimal(1).scaleb(-data_type.scale)
        quantized = value.quantize(quantum)
        return b"D" + f"{quantized:.{data_type.scale}f}".encode("ascii")
    if pa.types.is_timestamp(data_type):
        if not isinstance(value, datetime):
            raise TypeError(f"field {field.name!r} expected datetime, got {type(value).__name__}")
        return b"T" + str(_timestamp_microseconds(value)).encode("ascii")
    raise TypeError(f"unsupported canonical type for field {field.name!r}: {data_type}")


def canonical_row(schema: pa.Schema, row: dict[str, Any]) -> bytes:
    expected_names = schema.names
    if set(row) != set(expected_names):
        raise ValueError(
            f"row fields differ from schema; missing={sorted(set(expected_names) - set(row))}, "
            f"unknown={sorted(set(row) - set(expected_names))}"
        )
    encoded = bytearray(b"lakehouse-canonical-row-v1\0")
    for field in schema:
        scalar = canonical_scalar(field, row[field.name])
        encoded.extend(struct.pack(">I", len(scalar)))
        encoded.extend(scalar)
    return bytes(encoded)


def manifest_scalar(field: pa.Field, value: object) -> object:
    """Convert a scalar to a stable JSON-compatible min/max representation."""

    if value is None:
        return None
    if pa.types.is_integer(field.type) or pa.types.is_string(field.type):
        return value
    if pa.types.is_decimal(field.type):
        if not isinstance(value, Decimal):
            raise TypeError(f"expected Decimal for {field.name}")
        return f"{value:.{field.type.scale}f}"
    if pa.types.is_timestamp(field.type):
        if not isinstance(value, datetime):
            raise TypeError(f"expected datetime for {field.name}")
        return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    raise TypeError(f"unsupported manifest scalar type for {field.name}: {field.type}")
