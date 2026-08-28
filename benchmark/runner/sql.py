"""Typed SQL parameter rendering and canonical result identity."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from benchmark.runner.canonical import canonical_json_bytes, normalize
from benchmark.runner.config import ConfigurationError

PLACEHOLDER = re.compile(r"{{\s*([A-Za-z_][A-Za-z0-9_]*)\s*}}")
TIMESTAMP = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$")
DATE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
SAFE_STRING = re.compile(r"^[A-Za-z0-9_.:/ -]+$")


def _render_value(name: str, definition: Mapping[str, Any], value: Any) -> str:
    kind = definition["type"]
    if kind == "integer":
        if isinstance(value, bool) or not isinstance(value, int):
            raise ConfigurationError(f"parameter {name!r} must be an integer")
        rendered = str(value)
    elif kind == "number":
        if isinstance(value, bool) or not isinstance(value, int | float):
            raise ConfigurationError(f"parameter {name!r} must be numeric")
        if not math.isfinite(value):
            raise ConfigurationError(f"parameter {name!r} must be finite")
        rendered = str(value)
    elif kind == "boolean":
        if not isinstance(value, bool):
            raise ConfigurationError(f"parameter {name!r} must be boolean")
        rendered = "TRUE" if value else "FALSE"
    elif kind == "timestamp":
        if not isinstance(value, str) or not TIMESTAMP.fullmatch(value):
            raise ConfigurationError(f"parameter {name!r} must use YYYY-MM-DD HH:MM:SS")
        try:
            datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except ValueError as error:
            raise ConfigurationError(f"parameter {name!r} is not a valid timestamp") from error
        rendered = value
    elif kind == "date":
        if not isinstance(value, str) or not DATE.fullmatch(value):
            raise ConfigurationError(f"parameter {name!r} must use YYYY-MM-DD")
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError as error:
            raise ConfigurationError(f"parameter {name!r} is not a valid date") from error
        rendered = value
    elif kind == "string":
        if not isinstance(value, str) or not SAFE_STRING.fullmatch(value):
            raise ConfigurationError(f"parameter {name!r} is not a safe string literal")
        rendered = value.replace("'", "''")
    else:
        raise ConfigurationError(f"unsupported parameter type for {name!r}: {kind}")

    allowed = definition.get("allowed_values")
    if allowed is not None and value not in allowed:
        raise ConfigurationError(f"parameter {name!r} is outside allowed_values")
    return rendered


def render_sql(
    sql: str,
    definitions: Mapping[str, Mapping[str, Any]],
    values: Mapping[str, Any],
) -> str:
    expected = set(definitions)
    supplied = set(values)
    missing = {name for name in expected - supplied if definitions[name].get("required", False)}
    extra = supplied - expected
    if missing:
        raise ConfigurationError(f"missing SQL parameters: {sorted(missing)}")
    if extra:
        raise ConfigurationError(f"unknown SQL parameters: {sorted(extra)}")

    rendered = sql
    for name, definition in definitions.items():
        if name in values:
            rendered = re.sub(
                r"{{\s*" + re.escape(name) + r"\s*}}",
                _render_value(name, definition, values[name]),
                rendered,
            )
    leftovers = sorted(set(PLACEHOLDER.findall(rendered)))
    if leftovers:
        raise ConfigurationError(f"unresolved SQL parameters: {leftovers}")
    return rendered


def canonical_result_hash(rows: Sequence[Any], *, ordered: bool) -> str:
    canonical_rows: list[bytes] = []
    for row in rows:
        value = row.asDict(recursive=True) if hasattr(row, "asDict") else row
        canonical_rows.append(canonical_json_bytes(normalize(value)))
    if not ordered:
        canonical_rows.sort()
    framed = b"".join(len(item).to_bytes(8, "big") + item for item in canonical_rows)
    return hashlib.sha256(framed).hexdigest()


def _canonical_schema_value(value: object) -> object:
    if isinstance(value, dict):
        if value.get("type") == "struct" and isinstance(value.get("fields"), list):
            return _canonical_struct(value)
        return {
            str(key): _canonical_schema_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, list):
        return [_canonical_schema_value(item) for item in value]
    return value


def _canonical_struct(value: Mapping[str, object]) -> dict[str, object]:
    fields = value.get("fields")
    if value.get("type") != "struct" or not isinstance(fields, list):
        raise ValueError("Spark schema must be a struct with a fields array")
    canonical_fields: list[object] = []
    for raw_field in fields:
        if not isinstance(raw_field, dict):
            raise ValueError("Spark schema field must be an object")
        name = raw_field.get("name")
        nullable = raw_field.get("nullable")
        if not isinstance(name, str) or not isinstance(nullable, bool) or "type" not in raw_field:
            raise ValueError("Spark schema field is missing name, type, or nullable")
        metadata = raw_field.get("metadata", {})
        if not isinstance(metadata, dict):
            raise ValueError("Spark schema field metadata must be an object")
        canonical_fields.append(
            {
                "name": name,
                "type": _canonical_schema_value(raw_field["type"]),
                "nullable": nullable,
                "metadata": _canonical_schema_value(metadata),
            }
        )
    return {"type": "struct", "fields": canonical_fields}


def canonical_schema_json(schema_json: str) -> str:
    """Apply the workload contract's fixed Spark-schema key order."""

    value: object = json.loads(schema_json)
    if not isinstance(value, dict):
        raise ValueError("Spark schema JSON must be an object")
    canonical = _canonical_struct(value)
    return json.dumps(canonical, ensure_ascii=False, separators=(",", ":"), sort_keys=False)


def schema_hash(schema_json: str) -> str:
    payload = canonical_schema_json(schema_json).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
