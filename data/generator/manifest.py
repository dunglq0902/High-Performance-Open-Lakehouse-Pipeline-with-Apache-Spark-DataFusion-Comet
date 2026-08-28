"""Streaming table statistics and PK-range Merkle content hashes."""

from __future__ import annotations

import hashlib
import struct
from dataclasses import dataclass
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]

from data.generator.canonical import canonical_row, manifest_scalar
from data.generator.constants import (
    CANONICAL_ROW_ENCODING,
    CONTENT_HASH_ALGORITHM,
    PRIMARY_KEY_RANGE_SIZE,
)


@dataclass(frozen=True, slots=True)
class RangeHash:
    range_start: int
    range_end: int
    row_count: int
    sha256: str

    def as_mapping(self) -> dict[str, int | str]:
        return {
            "range_start": self.range_start,
            "range_end": self.range_end,
            "row_count": self.row_count,
            "sha256": self.sha256,
        }


class PrimaryKeyRangeMerkle:
    """Hash sorted rows into fixed first-PK-component ranges."""

    def __init__(self, schema: pa.Schema, range_size: int = PRIMARY_KEY_RANGE_SIZE) -> None:
        if range_size <= 0:
            raise ValueError("range_size must be positive")
        self._schema = schema
        self._range_size = range_size
        self._current_start: int | None = None
        self._current_count = 0
        self._current_hash: Any = None
        self._leaves: list[RangeHash] = []
        self._finished: tuple[str, list[RangeHash]] | None = None

    def _start_range(self, range_start: int) -> None:
        range_end = range_start + self._range_size - 1
        digest = hashlib.sha256()
        digest.update(b"lakehouse-pk-range-leaf-v1\0")
        digest.update(struct.pack(">QQ", range_start, range_end))
        self._current_start = range_start
        self._current_count = 0
        self._current_hash = digest

    def _finish_range(self) -> None:
        if self._current_start is None or self._current_hash is None:
            return
        self._current_hash.update(struct.pack(">Q", self._current_count))
        self._leaves.append(
            RangeHash(
                range_start=self._current_start,
                range_end=self._current_start + self._range_size - 1,
                row_count=self._current_count,
                sha256=self._current_hash.hexdigest(),
            )
        )
        self._current_start = None
        self._current_count = 0
        self._current_hash = None

    def add(self, first_primary_key: int, row: dict[str, Any]) -> None:
        if self._finished is not None:
            raise RuntimeError("cannot add rows after finishing a content hash")
        if first_primary_key <= 0:
            raise ValueError("primary-key range hashing requires positive integer keys")
        range_start = ((first_primary_key - 1) // self._range_size) * self._range_size + 1
        if self._current_start is None:
            self._start_range(range_start)
        elif range_start != self._current_start:
            if range_start < self._current_start:
                raise ValueError("rows are not sorted by the range-key component")
            self._finish_range()
            self._start_range(range_start)
        row_bytes = canonical_row(self._schema, row)
        if self._current_hash is None:
            raise AssertionError("range hash was not initialized")
        self._current_hash.update(struct.pack(">Q", len(row_bytes)))
        self._current_hash.update(row_bytes)
        self._current_count += 1

    def finish(self) -> tuple[str, list[RangeHash]]:
        if self._finished is not None:
            cached_root, cached_leaves = self._finished
            return cached_root, list(cached_leaves)
        self._finish_range()
        if not self._leaves:
            raise ValueError("cannot create a content hash for an empty table")
        root_digest = hashlib.sha256()
        root_digest.update(b"lakehouse-pk-range-merkle-root-v1\0")
        root_digest.update(struct.pack(">Q", self._range_size))
        for leaf in self._leaves:
            root_digest.update(
                struct.pack(">QQQ", leaf.range_start, leaf.range_end, leaf.row_count)
            )
            root_digest.update(bytes.fromhex(leaf.sha256))
        self._finished = (root_digest.hexdigest(), list(self._leaves))
        return self._finished[0], list(self._finished[1])


class TableAudit:
    """Accumulate deterministic content identity and table-level statistics."""

    def __init__(self, schema: pa.Schema, primary_key: tuple[str, ...]) -> None:
        if not primary_key:
            raise ValueError("primary_key cannot be empty")
        self.schema = schema
        self.primary_key = primary_key
        self.row_count = 0
        self._previous_key: tuple[int, ...] | None = None
        self._minimum: dict[str, object] = {}
        self._maximum: dict[str, object] = {}
        self._null_counts = {field.name: 0 for field in schema}
        self._merkle = PrimaryKeyRangeMerkle(schema)
        self._finished: dict[str, Any] | None = None

    def add(self, row: dict[str, Any]) -> None:
        if self._finished is not None:
            raise RuntimeError("cannot add rows after finishing a table audit")
        raw_key = tuple(row[name] for name in self.primary_key)
        if not all(
            isinstance(component, int) and not isinstance(component, bool) for component in raw_key
        ):
            raise TypeError("primary-key components must be integers")
        key = tuple(int(component) for component in raw_key)
        if self._previous_key is not None and key <= self._previous_key:
            raise ValueError(f"primary key is duplicate or unsorted: {key}")
        self._previous_key = key

        for field in self.schema:
            value = row[field.name]
            if value is None:
                self._null_counts[field.name] += 1
                continue
            if field.name not in self._minimum or value < self._minimum[field.name]:
                self._minimum[field.name] = value
            if field.name not in self._maximum or value > self._maximum[field.name]:
                self._maximum[field.name] = value

        self._merkle.add(key[0], row)
        self.row_count += 1

    def finish(self) -> dict[str, Any]:
        if self._finished is not None:
            return self._finished
        content_sha256, range_hashes = self._merkle.finish()
        min_max: dict[str, list[object | None]] = {}
        for field in self.schema:
            minimum = self._minimum.get(field.name)
            maximum = self._maximum.get(field.name)
            min_max[field.name] = [
                manifest_scalar(field, minimum),
                manifest_scalar(field, maximum),
            ]
        self._finished = {
            "row_count": self.row_count,
            "content_sha256": content_sha256,
            "content_hash_algorithm": CONTENT_HASH_ALGORITHM,
            "canonical_row_encoding": CANONICAL_ROW_ENCODING,
            "primary_key_range_size": PRIMARY_KEY_RANGE_SIZE,
            "range_key": self.primary_key[0],
            "range_hashes": [leaf.as_mapping() for leaf in range_hashes],
            "min_max": min_max,
            "null_counts": dict(self._null_counts),
        }
        return self._finished
