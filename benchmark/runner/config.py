"""Strict experiment/workload configuration loading and validation."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from copy import deepcopy
from pathlib import Path
from typing import Any, cast

import yaml
from jsonschema import Draft202012Validator

from benchmark.runner.canonical import sha256_file, sha256_value
from benchmark.runner.dataset_attestation import VerifiedDataset
from benchmark.runner.schedule import paired_randomized_schedule

ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?}")
SECRET_KEY_PATTERN = re.compile(r"(?:secret|password|token|access[_.-]?key)", re.IGNORECASE)

ALLOWED_COMET_DELTAS = frozenset(
    {
        "spark.plugins",
        "spark.shuffle.manager",
        "spark.comet.enabled",
        "spark.comet.exec.enabled",
        "spark.comet.nativeLoadRequired",
        "spark.comet.shuffle.enabled",
        "spark.comet.exec.memoryPool",
        "spark.comet.exec.memoryPool.fraction",
        "spark.comet.exec.strictFloatingPoint",
        "spark.comet.scan.icebergNative.enabled",
        "spark.comet.explain.fallback.enabled",
        "spark.comet.explain.format",
        "spark.comet.parquet.write.enabled",
        "spark.comet.metrics.enabled",
    }
)


class ConfigurationError(ValueError):
    """A configuration violates its schema or cross-field protocol contract."""


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
            raise ConfigurationError(f"duplicate YAML mapping key: {key!r}")
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def _expand_string(value: str, environment: Mapping[str, str]) -> str:
    def replace(match: re.Match[str]) -> str:
        name, default = match.group(1), match.group(2)
        if name in environment:
            return environment[name]
        if default is not None:
            return default
        raise ConfigurationError(f"unresolved environment variable: {name}")

    return ENV_PATTERN.sub(replace, value)


def resolve_environment(value: Any, environment: Mapping[str, str] | None = None) -> Any:
    source = os.environ if environment is None else environment
    if isinstance(value, str):
        return _expand_string(value, source)
    if isinstance(value, list):
        return [resolve_environment(item, source) for item in value]
    if isinstance(value, dict):
        return {key: resolve_environment(item, source) for key, item in value.items()}
    return value


def redact(value: Any, *, parent_key: str = "") -> Any:
    """Redact values whose key names can carry credentials."""

    if isinstance(value, dict):
        return {
            key: "<redacted>"
            if SECRET_KEY_PATTERN.search(str(key))
            else redact(item, parent_key=str(key))
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact(item, parent_key=parent_key) for item in value]
    return value


def load_document(
    path: Path, schema_path: Path, environment: Mapping[str, str] | None = None
) -> dict[str, Any]:
    try:
        raw = yaml.load(path.read_text(encoding="utf-8"), Loader=_UniqueKeyLoader)
    except yaml.YAMLError as error:
        raise ConfigurationError(f"invalid YAML in {path}: {error}") from error
    if not isinstance(raw, dict):
        raise ConfigurationError(f"document root must be an object: {path}")
    resolved = resolve_environment(raw, environment)
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    errors = sorted(
        Draft202012Validator(schema).iter_errors(resolved), key=lambda item: list(item.path)
    )
    if errors:
        messages = [
            f"{'/'.join(map(str, error.path)) or '<root>'}: {error.message}" for error in errors
        ]
        raise ConfigurationError("schema validation failed:\n- " + "\n- ".join(messages))
    return cast(dict[str, Any], resolved)


def validate_engine_matrix(config: Mapping[str, Any]) -> None:
    common_conf = config["spark"]["common_conf"]
    forbidden_common = [
        key for key in common_conf if key.startswith("spark.comet.") or key == "spark.plugins"
    ]
    if forbidden_common:
        raise ConfigurationError(
            f"Comet-only keys cannot appear in common_conf: {sorted(forbidden_common)}"
        )

    engines = {entry["name"]: entry["spark_conf"] for entry in config["matrix"]["engines"]}
    expected = {"spark_baseline", "comet_accelerated"}
    if set(engines) != expected:
        raise ConfigurationError(f"engine matrix must contain exactly {sorted(expected)}")
    if engines["spark_baseline"]:
        raise ConfigurationError("spark_baseline spark_conf must be empty")
    unknown = set(engines["comet_accelerated"]) - ALLOWED_COMET_DELTAS
    if unknown:
        raise ConfigurationError(f"non-allowlisted Comet engine deltas: {sorted(unknown)}")

    required = {
        "spark.plugins": "org.apache.spark.CometPlugin",
        "spark.shuffle.manager": "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager",
        "spark.comet.enabled": True,
        "spark.comet.exec.enabled": True,
        "spark.comet.nativeLoadRequired": True,
        "spark.comet.shuffle.enabled": True,
        "spark.comet.exec.strictFloatingPoint": True,
        "spark.comet.parquet.write.enabled": False,
    }
    for key, expected_value in required.items():
        if engines["comet_accelerated"].get(key) != expected_value:
            raise ConfigurationError(f"Comet profile requires {key}={expected_value!r}")


def _read_spark_properties(path: Path) -> dict[str, str]:
    properties: dict[str, str] = {}
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(None, 1) if "=" not in line else line.split("=", 1)
        if len(parts) != 2 or not all(part.strip() for part in parts):
            raise ConfigurationError(f"invalid Spark property at {path}:{line_number}")
        key, value = (part.strip() for part in parts)
        if key in properties:
            raise ConfigurationError(f"duplicate Spark property {key!r} in {path}")
        properties[key] = value
    return properties


def _spark_value_text(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


def runtime_profile_paths(config: Mapping[str, Any], root: Path) -> tuple[Path, Path]:
    """Return the reviewed common/Comet property files for an executable profile."""

    profile = config["spark"]["runtime_profile"]
    if profile in {"smoke-local", "smoke-standalone"}:
        return (
            root / "infrastructure/spark/spark-defaults.conf",
            root / "infrastructure/spark/profiles/comet.properties",
        )
    if profile == "benchmark-laptop":
        return (
            root / "infrastructure/spark/profiles/benchmark-laptop-common.properties",
            root / "infrastructure/spark/profiles/benchmark-laptop-comet.properties",
        )
    raise ConfigurationError(f"runtime profile has no executable property mapping: {profile!r}")


def validate_runtime_profile(config: Mapping[str, Any], root: Path) -> None:
    """Prove that the hashed config is the exact SparkConf the selected profile executes."""

    common_path, comet_path = runtime_profile_paths(config, root)
    actual_common = _read_spark_properties(common_path)
    declared_common = {
        key: _spark_value_text(value) for key, value in config["spark"]["common_conf"].items()
    }
    if actual_common != declared_common:
        raise ConfigurationError(
            f"declared common SparkConf differs from executable profile {common_path.name}"
        )

    engines = {entry["name"]: entry["spark_conf"] for entry in config["matrix"]["engines"]}
    actual_comet = _read_spark_properties(comet_path)
    declared_comet = {
        key: _spark_value_text(value) for key, value in engines["comet_accelerated"].items()
    }
    if actual_comet != declared_comet:
        raise ConfigurationError(
            f"declared Comet SparkConf differs from executable profile {comet_path.name}"
        )


def validate_smoke_runtime_profile(config: Mapping[str, Any], root: Path) -> None:
    """Backward-compatible name for the now profile-wide executable config gate."""

    validate_runtime_profile(config, root)


def load_experiment(
    path: Path, schema_dir: Path, environment: Mapping[str, str] | None = None
) -> dict[str, Any]:
    config = load_document(path, schema_dir / "experiment-config.schema.json", environment)
    validate_engine_matrix(config)
    return config


def _dataset_validation_binding(
    verified: VerifiedDataset,
    *,
    repository_root: Path,
    dataset_manifest_path: Path,
) -> dict[str, str]:
    root = repository_root.expanduser().resolve()
    attestation_path = verified.attestation_path.expanduser().resolve()
    try:
        attestation_relative = attestation_path.relative_to(root).as_posix()
    except ValueError as error:
        raise ConfigurationError("dataset validation attestation leaves repository root") from error
    if (
        verified.manifest_path.expanduser().resolve()
        != dataset_manifest_path.expanduser().resolve()
    ):
        raise ConfigurationError("verified dataset does not match the experiment dataset manifest")
    if verified.manifest_sha256 != sha256_file(dataset_manifest_path):
        raise ConfigurationError("verified dataset manifest SHA-256 is stale")
    current_attestation_file_sha256 = sha256_file(attestation_path)
    if verified.attestation_file_sha256 != current_attestation_file_sha256:
        raise ConfigurationError("verified dataset attestation file SHA-256 is stale")
    return {
        "mode": "content-bound-attestation-v1",
        "attestation_path": attestation_relative,
        "attestation_file_sha256": current_attestation_file_sha256,
        "attestation_payload_sha256": verified.attestation_sha256,
        "content_identity_sha256": verified.content_identity_sha256,
        "validator_git_commit": verified.git_commit,
    }


def build_experiment_manifest(
    config: Mapping[str, Any],
    *,
    config_path: Path,
    runtime_lock_path: Path,
    workload_sql_path: Path,
    workload_manifest_path: Path,
    dataset_manifest_path: Path,
    uv_lock_path: Path,
    spark_defaults_path: Path,
    comet_profile_path: Path,
    dataset_validation: VerifiedDataset | None = None,
) -> dict[str, Any]:
    """Resolve immutable inputs and produce the campaign control manifest."""

    engine_names = tuple(entry["name"] for entry in config["matrix"]["engines"])
    if len(engine_names) != 2:
        raise ConfigurationError("paired schedule requires exactly two engines")
    schedule = paired_randomized_schedule(
        config["experiment"]["measurement_runs"],
        config["experiment"]["seed"],
        (engine_names[0], engine_names[1]),
    )
    manifest = {
        "schema_version": 1,
        "experiment_id": config["experiment"]["id"],
        "protocol": "paired-randomized-ab-ba-v1",
        "resolved_config": redact(deepcopy(config)),
        "schedule": schedule,
        "input_hashes": {
            "experiment_config_sha256": sha256_file(config_path),
            "runtime_lock_sha256": sha256_file(runtime_lock_path),
            "workload_sql_sha256": sha256_file(workload_sql_path),
            "workload_manifest_sha256": sha256_file(workload_manifest_path),
            "dataset_manifest_sha256": sha256_file(dataset_manifest_path),
            "uv_lock_sha256": sha256_file(uv_lock_path),
            "spark_defaults_sha256": sha256_file(spark_defaults_path),
            "comet_profile_sha256": sha256_file(comet_profile_path),
        },
        "readiness": {
            "docker_services": "not_checked",
            "native_library": "not_checked",
            "runtime_versions": "not_checked",
            "dataset_content": "validated",
        },
    }
    if dataset_validation is not None:
        manifest["dataset_validation"] = _dataset_validation_binding(
            dataset_validation,
            repository_root=runtime_lock_path.parent,
            dataset_manifest_path=dataset_manifest_path,
        )
    manifest["manifest_sha256"] = sha256_value(manifest)
    return manifest
