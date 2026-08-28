"""Counter-based SHA-256 pseudo-random field derivation."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from datetime import datetime, timedelta

from data.generator.constants import GENERATOR_VERSION

PrimaryKey = int | str | tuple[int, ...]


def _primary_key_text(primary_key: PrimaryKey) -> str:
    if isinstance(primary_key, tuple):
        return ",".join(str(component) for component in primary_key)
    return str(primary_key)


def field_digest(seed: int, table: str, primary_key: PrimaryKey, field: str) -> bytes:
    """Return SHA-256(version|seed|table|primary_key|field).

    There is no process-global RNG state, so generation order, file size, and
    future partitioning changes cannot alter a row.
    """

    material = (
        f"{GENERATOR_VERSION}|{seed}|{table}|{_primary_key_text(primary_key)}|{field}"
    ).encode()
    return hashlib.sha256(material).digest()


def field_uint64(seed: int, table: str, primary_key: PrimaryKey, field: str) -> int:
    return int.from_bytes(field_digest(seed, table, primary_key, field)[:8], "big")


def integer_inclusive(
    seed: int,
    table: str,
    primary_key: PrimaryKey,
    field: str,
    minimum: int,
    maximum: int,
) -> int:
    if maximum < minimum:
        raise ValueError(f"invalid integer interval [{minimum}, {maximum}]")
    width = maximum - minimum + 1
    return minimum + field_uint64(seed, table, primary_key, field) % width


def choose[T](
    seed: int,
    table: str,
    primary_key: PrimaryKey,
    field: str,
    values: Sequence[T],
) -> T:
    if not values:
        raise ValueError("cannot choose from an empty sequence")
    return values[field_uint64(seed, table, primary_key, field) % len(values)]


def weighted_choice[T](
    seed: int,
    table: str,
    primary_key: PrimaryKey,
    field: str,
    weighted_values: Sequence[tuple[T, int]],
) -> T:
    total_weight = sum(weight for _, weight in weighted_values)
    if (
        not weighted_values
        or total_weight <= 0
        or any(weight <= 0 for _, weight in weighted_values)
    ):
        raise ValueError("weighted choices require positive weights")
    selection = field_uint64(seed, table, primary_key, field) % total_weight
    cumulative = 0
    for value, weight in weighted_values:
        cumulative += weight
        if selection < cumulative:
            return value
    raise AssertionError("weighted choice did not select a value")


def token(
    seed: int,
    table: str,
    primary_key: PrimaryKey,
    field: str,
    *,
    length: int = 16,
) -> str:
    if length <= 0 or length > 64:
        raise ValueError("token length must be between 1 and 64")
    return field_digest(seed, table, primary_key, field).hex()[:length]


def timestamp_inclusive(
    seed: int,
    table: str,
    primary_key: PrimaryKey,
    field: str,
    start: datetime,
    end: datetime,
) -> datetime:
    """Derive a whole-second timestamp in the inclusive UTC interval."""

    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("timestamp boundaries must be timezone-aware")
    span_seconds = int((end - start).total_seconds())
    if span_seconds < 0:
        raise ValueError("timestamp end precedes start")
    offset = integer_inclusive(seed, table, primary_key, field, 0, span_seconds)
    return start + timedelta(seconds=offset)
