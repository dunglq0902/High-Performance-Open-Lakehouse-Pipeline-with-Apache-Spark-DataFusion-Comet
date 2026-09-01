"""Strict generator-profile loading and semantic validation."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from data.generator.constants import TABLE_ORDER

_PROFILE_KEYS = {
    "schema_version",
    "dataset_revision",
    "profile_id",
    "seed",
    "benchmark_eligible",
    "skew_profile",
    "timezone",
    "currency",
    "rounding_mode",
    "dataset_start",
    "dataset_end",
    "counts",
    "rows_per_file",
}
_REQUIRED_PROFILE_KEYS = _PROFILE_KEYS - {"dataset_revision"}

_BENCHMARK_MINIMUM_COUNTS = {
    "customers": 100_000,
    "products": 10_000,
    "orders": 1_000_000,
    "order_items": 4_000_000,
    "events": 4_000_000,
}

_BENCHMARK_MINIMUM_ROWS_PER_FILE = {
    "orders": 250_000,
    "order_items": 1_000_000,
    "events": 500_000,
}


class _UniqueKeyLoader(yaml.SafeLoader):
    """Safe YAML loader that rejects silently overwritten mapping keys."""


def _construct_unique_mapping(
    loader: _UniqueKeyLoader, node: yaml.MappingNode, deep: bool = False
) -> dict[object, object]:
    loader.flatten_mapping(node)
    result: dict[object, object] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in result:
            raise ValueError(f"duplicate YAML mapping key: {key!r}")
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


@dataclass(frozen=True, slots=True)
class GeneratorProfile:
    """Fully validated, immutable generator configuration."""

    schema_version: int
    dataset_revision: int
    profile_id: str
    seed: int
    benchmark_eligible: bool
    skew_profile: str
    timezone: str
    currency: str
    rounding_mode: str
    dataset_start: datetime
    dataset_end: datetime
    counts: Mapping[str, int]
    rows_per_file: Mapping[str, int]

    @property
    def dataset_id(self) -> str:
        return (
            f"ecommerce-{self.profile_id}-{self.skew_profile}-seed-"
            f"{self.seed}-v{self.dataset_revision}"
        )

    def as_canonical_mapping(self) -> dict[str, Any]:
        """Return the normalized mapping whose hash identifies this config."""

        result: dict[str, Any] = {
            "schema_version": self.schema_version,
            "profile_id": self.profile_id,
            "seed": self.seed,
            "benchmark_eligible": self.benchmark_eligible,
            "skew_profile": self.skew_profile,
            "timezone": self.timezone,
            "currency": self.currency,
            "rounding_mode": self.rounding_mode,
            "dataset_start": _format_utc(self.dataset_start),
            "dataset_end": _format_utc(self.dataset_end),
            "counts": dict(sorted(self.counts.items())),
            "rows_per_file": dict(sorted(self.rows_per_file.items())),
        }
        if self.dataset_revision != 1:
            result["dataset_revision"] = self.dataset_revision
        return result


def _format_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse_utc(value: object, field_name: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError(f"{field_name} must be an RFC 3339 UTC timestamp ending in 'Z'")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise ValueError(f"{field_name} is not a valid timestamp: {value!r}") from error
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ValueError(f"{field_name} must use UTC")
    return parsed.astimezone(UTC)


def _strict_positive_counts(value: object, field_name: str) -> dict[str, int]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a mapping")
    unknown = set(value) - set(TABLE_ORDER)
    missing = set(TABLE_ORDER) - set(value)
    if unknown or missing:
        raise ValueError(
            f"{field_name} table mismatch; missing={sorted(missing)}, unknown={sorted(unknown)}"
        )
    normalized: dict[str, int] = {}
    for table_name in TABLE_ORDER:
        count = value[table_name]
        if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
            raise ValueError(f"{field_name}.{table_name} must be a positive integer")
        normalized[table_name] = count
    return normalized


def profile_from_mapping(value: Mapping[str, object]) -> GeneratorProfile:
    """Validate a decoded YAML mapping without accepting undeclared fields."""

    unknown = set(value) - _PROFILE_KEYS
    missing = _REQUIRED_PROFILE_KEYS - set(value)
    if unknown or missing:
        raise ValueError(
            f"generator profile fields mismatch; missing={sorted(missing)}, "
            f"unknown={sorted(unknown)}"
        )

    schema_version = value["schema_version"]
    dataset_revision = value.get("dataset_revision", 1)
    seed = value["seed"]
    benchmark_eligible = value["benchmark_eligible"]
    if (
        isinstance(schema_version, bool)
        or not isinstance(schema_version, int)
        or schema_version != 1
    ):
        raise ValueError("only generator profile schema_version 1 is supported")
    if (
        isinstance(dataset_revision, bool)
        or not isinstance(dataset_revision, int)
        or dataset_revision < 1
    ):
        raise ValueError("dataset_revision must be a positive integer")
    if isinstance(seed, bool) or not isinstance(seed, int) or seed < 0:
        raise ValueError("seed must be a non-negative integer")
    if not isinstance(benchmark_eligible, bool):
        raise ValueError("benchmark_eligible must be a boolean")

    profile_id = value["profile_id"]
    if (
        not isinstance(profile_id, str)
        or not profile_id.replace("-", "").replace("_", "").isalnum()
    ):
        raise ValueError("profile_id must contain only letters, digits, '-' or '_'")
    if value["skew_profile"] != "uniform":
        raise ValueError("the first generator slice supports only skew_profile='uniform'")
    if value["timezone"] != "UTC":
        raise ValueError("timezone must be UTC")
    if value["currency"] != "USD":
        raise ValueError("currency must be USD")
    if value["rounding_mode"] != "HALF_UP":
        raise ValueError("rounding_mode must be HALF_UP")

    counts = _strict_positive_counts(value["counts"], "counts")
    rows_per_file = _strict_positive_counts(value["rows_per_file"], "rows_per_file")
    if counts["order_items"] < counts["orders"]:
        raise ValueError("order_items count must be at least the orders count")
    if counts["order_items"] > counts["orders"] * 20:
        raise ValueError("order_items count would exceed the line_number <= 20 contract")
    if counts["events"] < counts["orders"] * 4:
        raise ValueError("events count must allow a four-step session for every order")
    if benchmark_eligible:
        if profile_id != "small":
            raise ValueError("benchmark-eligible E-commerce data must use profile_id='small'")
        undersized = {
            table_name: (counts[table_name], minimum)
            for table_name, minimum in _BENCHMARK_MINIMUM_COUNTS.items()
            if counts[table_name] < minimum
        }
        if undersized:
            raise ValueError(f"benchmark-eligible profile is undersized: {undersized}")
        fragmented = {
            table_name: (rows_per_file[table_name], minimum)
            for table_name, minimum in _BENCHMARK_MINIMUM_ROWS_PER_FILE.items()
            if rows_per_file[table_name] < minimum
        }
        if fragmented:
            raise ValueError(
                f"benchmark-eligible profile has undersized fact-file row targets: {fragmented}"
            )

    dataset_start = _parse_utc(value["dataset_start"], "dataset_start")
    dataset_end = _parse_utc(value["dataset_end"], "dataset_end")
    if dataset_end <= dataset_start:
        raise ValueError("dataset_end must be later than dataset_start")
    if (dataset_end - dataset_start).total_seconds() < 600:
        raise ValueError("dataset interval must be at least ten minutes")

    return GeneratorProfile(
        schema_version=schema_version,
        dataset_revision=dataset_revision,
        profile_id=profile_id,
        seed=seed,
        benchmark_eligible=benchmark_eligible,
        skew_profile="uniform",
        timezone="UTC",
        currency="USD",
        rounding_mode="HALF_UP",
        dataset_start=dataset_start,
        dataset_end=dataset_end,
        counts=counts,
        rows_per_file=rows_per_file,
    )


def load_profile(path: str | Path) -> GeneratorProfile:
    """Load a UTF-8 YAML profile using PyYAML's non-executable safe loader."""

    profile_path = Path(path)
    with profile_path.open("r", encoding="utf-8") as stream:
        decoded = yaml.load(stream, Loader=_UniqueKeyLoader)
    if not isinstance(decoded, dict) or not all(isinstance(key, str) for key in decoded):
        raise ValueError("generator profile root must be a string-keyed mapping")
    return profile_from_mapping(decoded)
