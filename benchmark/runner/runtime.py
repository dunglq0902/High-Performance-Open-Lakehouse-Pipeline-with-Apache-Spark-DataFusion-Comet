"""Offline validation for the immutable runtime lock."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, cast

from jsonschema import Draft202012Validator, FormatChecker

from benchmark.runner.config import ConfigurationError

SHA256 = re.compile(r"^(?:sha256:)?[0-9a-f]{64}$")
PLACEHOLDER = re.compile(r"(?:<[^>]+>|\b(?:todo|tbd|placeholder|unresolved)\b)", re.IGNORECASE)


def validate_runtime_lock(path: Path, schema_path: Path) -> dict[str, Any]:
    lock = json.loads(path.read_text(encoding="utf-8"))
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    errors = sorted(
        Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(lock),
        key=lambda item: list(item.path),
    )
    if errors:
        messages = [
            f"{'/'.join(map(str, item.path)) or '<root>'}: {item.message}" for item in errors
        ]
        raise ConfigurationError(
            "runtime lock schema validation failed:\n- " + "\n- ".join(messages)
        )

    names: set[str] = set()
    for component in lock["components"]:
        name = component["name"]
        if name in names:
            raise ConfigurationError(f"duplicate runtime component: {name}")
        names.add(name)
        coordinate = component["coordinate_or_image"]
        digest = component["sha256_or_digest"]
        joined = " ".join(str(value) for value in component.values())
        if PLACEHOLDER.search(joined):
            raise ConfigurationError(f"runtime component {name!r} contains a placeholder")
        if ":latest" in coordinate or coordinate.endswith("/latest"):
            raise ConfigurationError(f"runtime component {name!r} uses a mutable latest reference")
        if not SHA256.fullmatch(digest):
            raise ConfigurationError(f"runtime component {name!r} has no SHA-256/OCI digest")

    required = {
        "apache-spark",
        "scala",
        "java",
        "datafusion-comet",
        "apache-iceberg-runtime",
        "apache-iceberg-aws-bundle",
        "iceberg-rest-fixture",
        "minio",
        "python",
        "tpch-dbgen",
    }
    missing = required - names
    if missing:
        raise ConfigurationError(f"runtime lock misses required components: {sorted(missing)}")
    return cast(dict[str, Any], lock)
