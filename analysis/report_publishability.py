"""Fail-closed publication policy for the reviewed core benchmark suite."""

from __future__ import annotations

import json
import math
import re
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime
from itertools import pairwise
from pathlib import Path
from statistics import median
from typing import Any, Literal, TypeGuard, cast

from jsonschema import Draft202012Validator, FormatChecker

from benchmark.collectors.resources import (
    DEFAULT_OVERHEAD_LIMIT_PERCENT,
    DEFAULT_SAMPLE_INTERVAL_SECONDS,
    CollectorStatus,
    ResourceSample,
    aggregate_samples,
)
from benchmark.parsers.eventlog import parse_event_log
from benchmark.parsers.plan import analyze_plan
from benchmark.runner.campaign import (
    CampaignError,
    CampaignRun,
    plan_campaign,
    validate_run_record,
)
from benchmark.runner.canonical import sha256_file, sha256_value
from benchmark.runner.capacity import (
    CapacitySnapshot,
    CgroupSnapshot,
    FilesystemSnapshot,
    evaluate_capacity_gate,
)
from benchmark.runner.config import (
    ConfigurationError,
    build_experiment_manifest,
    load_document,
    load_experiment,
    runtime_profile_paths,
)
from benchmark.runner.dataset_attestation import verify_attestation
from benchmark.runner.evidence import (
    ARTIFACT_FIELDS,
    ArtifactEvidenceError,
    RepositoryEvidenceError,
    artifact_evidence,
    clean_git_commit,
    control_artifact_evidence,
    raw_records_sha256,
)
from benchmark.runner.record import RawRecordContext, build_raw_record
from benchmark.runner.runtime import validate_runtime_lock
from benchmark.runner.sql import schema_hash
from benchmark.runner.summary import summarize_records
from pipeline.benchmark.run_query import _table_identifier
from scripts.run_research_suite import CORE_CONFIGS

ROOT = Path(__file__).resolve().parents[1]
CAMPAIGN_CONTROL_ROOT = ROOT / ".artifacts/campaigns"
DATASET_VALIDATION_ROOT = ROOT / ".artifacts/dataset-validations"
RESEARCH_SHARED_ROOT = ROOT / ".artifacts/research-shared"
EXPECTED_CORE_CAMPAIGNS = 10
EXPECTED_MEASUREMENT_PAIRS = 10
EXPECTED_MEASUREMENT_RECORDS = EXPECTED_MEASUREMENT_PAIRS * 2
EXPECTED_CAMPAIGN_RUNS = EXPECTED_MEASUREMENT_RECORDS + 4
_ENGINES = frozenset({"spark_baseline", "comet_accelerated"})
_RESOURCE_METRICS = (
    "cpu_core_seconds",
    "cgroup_memory_peak_mib",
    "jvm_gc_time_ms",
    "shuffle_read_mb",
    "shuffle_write_mb",
    "disk_spill_mb",
)
_ATTEMPT_PATTERN = re.compile(r"^campaign-verification-attempt-(\d+)\.json$")
_RUN_ATTEMPT_PATTERN = re.compile(r"^attempt-(\d{4})$")
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_GIT_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_RESOURCE_SAMPLE_FIELDS = frozenset(
    {
        "timestamp_ns",
        "source",
        "status",
        "cpu_usage_ns",
        "memory_current_bytes",
        "memory_peak_bytes",
        "swap_current_bytes",
        "io_read_bytes",
        "io_write_bytes",
        "cpu_limit_cores",
        "process_count",
        "missing_metrics",
        "errors",
    }
)
_WORKER_RESOURCE_FIELDS = frozenset(
    {
        "schema_version",
        "scope",
        "timed_out",
        "window_started",
        "window_completed",
        "aborted",
        "samples",
        "summary",
    }
)
_APPLICATION_RESULT_FIELDS = frozenset(
    {
        "schema_version",
        "artifact_class",
        "experiment_id",
        "run_id",
        "phase",
        "pair_index",
        "engine",
        "timestamp",
        "status",
        "failure",
        "application_id",
        "runtime",
        "workload",
        "query_id",
        "storage_profile",
        "sql_sha256",
        "workload_manifest_sha256",
        "dataset_manifest_sha256",
        "iceberg_snapshot_ids",
        "query_wall_time_ms",
        "driver_resources",
        "schema_json",
        "schema_sha256",
        "row_count",
        "canonical_result_sha256",
        "plan_analysis",
        "artifacts",
    }
)


@dataclass(frozen=True, slots=True)
class CoreExperiment:
    """Identity fixed by one config in ``run_research_suite.CORE_CONFIGS``."""

    config: str
    experiment_id: str
    workload: str
    query_id: str
    storage_profile: str


def core_experiments(repository_root: Path = ROOT) -> tuple[CoreExperiment, ...]:
    """Load the exact reviewed suite rather than duplicating experiment IDs here."""

    if len(CORE_CONFIGS) != EXPECTED_CORE_CAMPAIGNS:
        raise ValueError(f"CORE_CONFIGS must contain exactly {EXPECTED_CORE_CAMPAIGNS} configs")
    if len(CORE_CONFIGS) != len(set(CORE_CONFIGS)):
        raise ValueError("CORE_CONFIGS contains duplicate config paths")
    experiments: list[CoreExperiment] = []
    for relative in CORE_CONFIGS:
        config = load_experiment(repository_root / relative, repository_root / "benchmark/schemas")
        measurement_runs = config["experiment"].get("measurement_runs")
        if not _is_integer(measurement_runs) or measurement_runs != EXPECTED_MEASUREMENT_PAIRS:
            raise ValueError(
                f"{relative} must declare exactly {EXPECTED_MEASUREMENT_PAIRS} measurement runs"
            )
        workload = config["workload"]
        experiments.append(
            CoreExperiment(
                config=relative,
                experiment_id=str(config["experiment"]["id"]),
                workload=str(workload["suite"]),
                query_id=str(workload["query_id"]),
                storage_profile=str(workload["storage_profile"]),
            )
        )
    experiment_ids = [item.experiment_id for item in experiments]
    if len(experiment_ids) != len(set(experiment_ids)):
        raise ValueError("CORE_CONFIGS contains duplicate experiment IDs")
    return tuple(experiments)


def _is_integer(value: object) -> TypeGuard[int]:
    return isinstance(value, int) and not isinstance(value, bool)


def _parse_utc_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.endswith("Z"):
        return None
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError:
        return None
    return parsed if parsed.utcoffset() is not None else None


def _latest_verification(campaign_dir: Path) -> tuple[int, Path] | None:
    candidates: dict[int, Path] = {}
    base = campaign_dir / "campaign-verification.json"
    if base.is_file():
        candidates[1] = base
    if campaign_dir.is_dir():
        for path in campaign_dir.iterdir():
            match = _ATTEMPT_PATTERN.fullmatch(path.name)
            if path.name.startswith("campaign-verification-attempt-") and (
                match is None or not path.is_file()
            ):
                raise ValueError(f"malformed campaign-verification attempt: {path}")
            if match is not None and path.is_file():
                attempt = int(match.group(1))
                if attempt < 2 or attempt in candidates:
                    raise ValueError(f"invalid duplicate campaign-verification attempt: {path}")
                candidates[attempt] = path
    if not candidates:
        return None
    if sorted(candidates) != list(range(1, max(candidates) + 1)):
        raise ValueError("campaign-verification attempts must be contiguous from the base file")
    latest = max(candidates)
    return latest, candidates[latest]


def _repo_path(repository_root: Path, declared: object) -> Path:
    if not isinstance(declared, str) or not declared:
        raise ConfigurationError(f"invalid repository input path: {declared!r}")
    root = repository_root.resolve()
    path = (root / declared).resolve()
    try:
        path.relative_to(root)
    except ValueError as error:
        raise ConfigurationError(f"repository input path leaves root: {declared!r}") from error
    return path


def _repo_relative_path(repository_root: Path, declared: object, *, label: str) -> Path:
    """Resolve a canonical repository-relative path without accepting aliases or escapes."""

    if not isinstance(declared, str) or not declared or "\\" in declared:
        raise ConfigurationError(f"{label} must be a non-empty POSIX repository-relative path")
    relative = Path(declared)
    if relative.is_absolute() or ".." in relative.parts or relative.as_posix() != declared:
        raise ConfigurationError(f"{label} must be a canonical repository-relative path")
    root = repository_root.resolve()
    path = (root / relative).resolve()
    try:
        path.relative_to(root)
    except ValueError as error:
        raise ConfigurationError(f"{label} leaves the repository") from error
    return path


def _load_json_object(path: Path, *, label: str, issues: list[str]) -> dict[str, Any] | None:
    try:
        value: object = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        issues.append(f"{label} is unreadable: {error}")
        return None
    if not isinstance(value, dict):
        issues.append(f"{label} root must be an object")
        return None
    return value


def _latest_passed_control_attempt(
    targets: Mapping[str, Path],
    *,
    prefix: str,
    passed_field: str,
    passed_value: object,
    label: str,
    issues: list[str],
) -> None:
    attempts: list[tuple[int, Path]] = []
    for target_label, path in targets.items():
        if not target_label.startswith(prefix):
            continue
        suffix = target_label.removeprefix(prefix)
        if re.fullmatch(r"[0-9]{4}", suffix) is None or int(suffix) < 1:
            issues.append(f"{label} control target has an invalid attempt label: {target_label}")
            continue
        attempts.append((int(suffix), path))
    attempts.sort()
    if not attempts:
        return
    if [attempt for attempt, _ in attempts] != list(range(1, attempts[-1][0] + 1)):
        issues.append(f"{label} control attempts must be contiguous from 0001")
        return
    latest = _load_json_object(attempts[-1][1], label=f"latest {label}", issues=issues)
    if latest is not None and latest.get(passed_field) != passed_value:
        issues.append(f"latest {label} did not pass")


def _control_attempt_paths(
    targets: Mapping[str, Path], *, prefix: str, label: str, issues: list[str]
) -> tuple[tuple[int, Path], ...]:
    attempts: list[tuple[int, Path]] = []
    for target_label, path in targets.items():
        if not target_label.startswith(prefix):
            continue
        suffix = target_label.removeprefix(prefix)
        if re.fullmatch(r"[0-9]{4}", suffix) is None or int(suffix) < 1:
            issues.append(f"{label} control target has an invalid attempt label: {target_label}")
            continue
        attempts.append((int(suffix), path))
    attempts.sort()
    if attempts and [attempt for attempt, _ in attempts] != list(range(1, attempts[-1][0] + 1)):
        issues.append(f"{label} control attempts must be contiguous from 0001")
        return ()
    return tuple(attempts)


def _attempt_files_on_disk(
    base: Path, *, label: str, issues: list[str]
) -> tuple[tuple[int, Path], ...]:
    attempts: dict[int, Path] = {}
    if base.is_file():
        attempts[1] = base.resolve()
    pattern = re.compile(rf"^{re.escape(base.stem)}-attempt-([0-9]{{4}}){re.escape(base.suffix)}$")
    if base.parent.is_dir():
        for path in base.parent.iterdir():
            match = pattern.fullmatch(path.name)
            if path.name.startswith(f"{base.stem}-attempt-") and (
                match is None or not path.is_file()
            ):
                issues.append(f"{label} has a malformed attempt artifact: {path.name}")
                continue
            if match is None or not path.is_file():
                continue
            attempt = int(match.group(1))
            if attempt < 2 or attempt in attempts:
                issues.append(f"{label} has an invalid duplicate attempt file: {path.name}")
                continue
            attempts[attempt] = path.resolve()
    if attempts and sorted(attempts) != list(range(1, max(attempts) + 1)):
        issues.append(f"{label} on-disk attempts must be contiguous from the base file")
        return ()
    return tuple(sorted(attempts.items()))


def _calibration_artifact_check(
    path: Path,
    *,
    label: str,
    expected_environment: Mapping[str, str] | None,
    repository_root: Path,
    issues: list[str],
) -> None:
    value = _load_json_object(path, label=label, issues=issues)
    if value is None:
        return
    required_top = {
        "schema_version",
        "artifact_class",
        "status",
        "workload",
        "collector",
        "runtime",
        "calibration",
        "environment",
        "created_at",
        "artifact_sha256",
    }
    if set(value) != required_top:
        issues.append(f"{label} has an unexpected top-level shape")
        return
    workload = value.get("workload")
    collector = value.get("collector")
    runtime = value.get("runtime")
    calibration = value.get("calibration")
    if (
        value.get("schema_version") != 1
        or value.get("artifact_class") != "resource-collector-calibration-v1"
    ):
        issues.append(f"{label} identity is invalid")
    environment = value.get("environment")
    created_at = value.get("created_at")
    payload_for_hash = dict(value)
    artifact_sha256 = payload_for_hash.pop("artifact_sha256", None)
    if (
        expected_environment is None
        or environment != dict(expected_environment)
        or _parse_utc_timestamp(created_at) is None
        or not isinstance(artifact_sha256, str)
        or artifact_sha256 != sha256_value(payload_for_hash)
    ):
        issues.append(f"{label} environment binding or self-hash is invalid")
    if not isinstance(workload, Mapping) or set(workload) != {"id", "work_units"}:
        issues.append(f"{label}.workload is invalid")
    elif (
        workload.get("id") != "sha256-chain-v1"
        or not _is_integer(workload.get("work_units"))
        or int(workload["work_units"]) < 1
    ):
        issues.append(f"{label}.workload is not the reviewed deterministic workload")
    expected_collector_fields = {
        "sample_interval_seconds",
        "module_sha256",
        "script_sha256",
        "observed_sources",
        "observed_statuses",
        "minimum_samples_per_run",
        "source_gate_passed",
        "status_gate_passed",
        "sampling_gate_passed",
    }
    if not isinstance(collector, Mapping) or set(collector) != expected_collector_fields:
        issues.append(f"{label}.collector is invalid")
        collector = None
    if (
        not isinstance(runtime, Mapping)
        or set(runtime) != {"python_version", "platform"}
        or not all(
            isinstance(runtime.get(field), str) and runtime.get(field)
            for field in ("python_version", "platform")
        )
    ):
        issues.append(f"{label}.runtime is invalid")
    else:
        try:
            runtime_lock = validate_runtime_lock(
                repository_root / "runtime-versions.lock",
                repository_root / "benchmark/schemas/runtime-lock.schema.json",
            )
            expected_python = next(
                item["version"] for item in runtime_lock["components"] if item["name"] == "python"
            )
        except (
            ConfigurationError,
            KeyError,
            OSError,
            StopIteration,
            TypeError,
            ValueError,
        ) as error:
            issues.append(f"{label}.runtime lock is invalid: {error}")
        else:
            if runtime.get("python_version") != expected_python:
                issues.append(f"{label}.runtime Python version differs from the current lock")
    expected_calibration_fields = {
        "pair_count",
        "threshold_percent",
        "median_baseline_ns",
        "median_instrumented_ns",
        "median_paired_overhead_percent",
        "maximum_paired_overhead_percent",
        "accepted",
        "paired_overhead_percent",
    }
    if not isinstance(calibration, Mapping) or set(calibration) != expected_calibration_fields:
        issues.append(f"{label}.calibration is invalid")
        calibration = None
    accepted = False
    if calibration is not None:
        pairs = calibration.get("paired_overhead_percent")
        pair_count = calibration.get("pair_count")
        numeric_fields = (
            "threshold_percent",
            "median_baseline_ns",
            "median_instrumented_ns",
            "median_paired_overhead_percent",
            "maximum_paired_overhead_percent",
        )
        numbers_valid = all(
            isinstance(calibration.get(field), int | float)
            and not isinstance(calibration.get(field), bool)
            and math.isfinite(float(calibration[field]))
            for field in numeric_fields
        )
        pair_values = (
            [float(item) for item in pairs]
            if isinstance(pairs, list)
            and all(
                isinstance(item, int | float)
                and not isinstance(item, bool)
                and math.isfinite(float(item))
                for item in pairs
            )
            else None
        )
        if (
            not _is_integer(pair_count)
            or pair_count < 3
            or pair_values is None
            or len(pair_values) != pair_count
            or not numbers_valid
            or float(calibration["threshold_percent"]) != DEFAULT_OVERHEAD_LIMIT_PERCENT
            or float(calibration["median_baseline_ns"]) <= 0
            or float(calibration["median_instrumented_ns"]) <= 0
        ):
            issues.append(f"{label}.calibration values are invalid")
        else:
            paired_median = float(median(pair_values))
            expected_accepted = paired_median < DEFAULT_OVERHEAD_LIMIT_PERCENT
            accepted = calibration.get("accepted") is expected_accepted
            if (
                not math.isclose(
                    float(calibration["median_paired_overhead_percent"]), paired_median
                )
                or not math.isclose(
                    float(calibration["maximum_paired_overhead_percent"]), max(pair_values)
                )
                or not accepted
            ):
                issues.append(f"{label}.calibration decision is internally inconsistent")
            accepted = expected_accepted and calibration.get("accepted") is True
    gates_passed = False
    if collector is not None:
        sources = collector.get("observed_sources")
        statuses = collector.get("observed_statuses")
        minimum_samples = collector.get("minimum_samples_per_run")
        source_gate = sources == ["cgroup_v2"]
        status_gate = (
            isinstance(statuses, list)
            and bool(statuses)
            and all(status in {"complete", "partial"} for status in statuses)
        )
        sampling_gate = _is_integer(minimum_samples) and minimum_samples >= 2
        gates_passed = source_gate and status_gate and sampling_gate
        expected = {
            "sample_interval_seconds": DEFAULT_SAMPLE_INTERVAL_SECONDS,
            "module_sha256": sha256_file(repository_root / "benchmark/collectors/resources.py"),
            "script_sha256": sha256_file(
                repository_root / "scripts/calibrate_resource_collector.py"
            ),
            "source_gate_passed": source_gate,
            "status_gate_passed": status_gate,
            "sampling_gate_passed": sampling_gate,
        }
        if any(
            collector.get(field) != expected_value for field, expected_value in expected.items()
        ):
            issues.append(f"{label}.collector decision or source identity is invalid")
    expected_status = "passed" if accepted and gates_passed else "failed"
    if value.get("status") != expected_status:
        issues.append(f"{label}.status is inconsistent with its measured gates")


def _capacity_artifact_check(
    path: Path,
    *,
    label: str,
    config: Mapping[str, Any],
    config_path: Path,
    dataset_manifest: Mapping[str, Any],
    dataset_manifest_path: Path,
    issues: list[str],
) -> None:
    value = _load_json_object(path, label=label, issues=issues)
    if value is None:
        return
    observation = value.get("observation")
    if not isinstance(observation, Mapping):
        issues.append(f"{label}.observation is missing")
        return
    filesystem = observation.get("filesystem")
    cgroup = observation.get("cgroup")
    if not isinstance(filesystem, Mapping) or set(filesystem) != {
        "path",
        "free_bytes",
        "total_bytes",
    }:
        issues.append(f"{label}.observation.filesystem is invalid")
        return
    if not isinstance(cgroup, Mapping) or set(cgroup) != {
        "memory_limit_bytes",
        "cpu_limit_cores",
        "swap_current_bytes",
        "swap_delta_bytes",
        "swap_peak_bytes",
    }:
        issues.append(f"{label}.observation.cgroup is invalid")
        return
    snapshot = CapacitySnapshot(
        filesystem=FilesystemSnapshot(
            path=filesystem.get("path"),
            free_bytes=filesystem.get("free_bytes"),
            total_bytes=filesystem.get("total_bytes"),
        ),
        cgroup=CgroupSnapshot(
            memory_limit_bytes=cgroup.get("memory_limit_bytes"),
            cpu_limit_cores=cgroup.get("cpu_limit_cores"),
            swap_current_bytes=cgroup.get("swap_current_bytes"),
            swap_delta_bytes=cgroup.get("swap_delta_bytes"),
            swap_peak_bytes=cgroup.get("swap_peak_bytes"),
        ),
    )
    expected_result = evaluate_capacity_gate(config, dataset_manifest, snapshot).as_dict()
    expected_top = {
        "schema_version",
        "artifact_class",
        "experiment_config_sha256",
        "dataset_manifest_sha256",
        "environment",
        "created_at",
        "artifact_sha256",
        "observation",
        *expected_result,
    }
    if set(value) != expected_top:
        issues.append(f"{label} has an unexpected top-level shape")
    if (
        value.get("schema_version") != 1
        or value.get("artifact_class") != "research-capacity-gate-v1"
        or value.get("experiment_config_sha256") != sha256_file(config_path)
        or value.get("dataset_manifest_sha256") != sha256_file(dataset_manifest_path)
    ):
        issues.append(f"{label} does not bind the reviewed config and dataset")
    environment = value.get("environment")
    if (
        not isinstance(environment, Mapping)
        or set(environment)
        != {
            "git_commit",
            "container_image_digest",
            "storage_identity_sha256",
            "cpu_model",
        }
        or _GIT_COMMIT_PATTERN.fullmatch(str(environment.get("git_commit"))) is None
        or re.fullmatch(r"sha256:[0-9a-f]{64}", str(environment.get("container_image_digest")))
        is None
        or _SHA256_PATTERN.fullmatch(str(environment.get("storage_identity_sha256"))) is None
        or not isinstance(environment.get("cpu_model"), str)
        or not environment.get("cpu_model")
    ):
        issues.append(f"{label} environment identity is invalid")
    payload_for_hash = dict(value)
    artifact_sha256 = payload_for_hash.pop("artifact_sha256", None)
    if (
        _parse_utc_timestamp(value.get("created_at")) is None
        or not isinstance(artifact_sha256, str)
        or artifact_sha256 != sha256_value(payload_for_hash)
    ):
        issues.append(f"{label} creation timestamp or self-hash is invalid")
    declared_result = {field: value.get(field) for field in expected_result}
    if declared_result != expected_result:
        issues.append(f"{label} decision does not recompute from its recorded observation")


def _medallion_artifact_check(
    path: Path,
    *,
    experiment: CoreExperiment,
    records: list[Mapping[str, Any]],
    config: Mapping[str, Any],
    dataset_manifest: Mapping[str, Any],
    dataset_manifest_path: Path,
    repository_root: Path,
    issues: list[str],
) -> None:
    value = _load_json_object(path, label="Medallion audit", issues=issues)
    if value is None:
        return
    common_fields = {
        "schema_version",
        "status",
        "pipeline",
        "runtime",
        "dataset_id",
        "dataset_manifest_sha256",
        "dataset_validation_attestation_sha256",
        "benchmark_eligible",
        "quality",
        "snapshots",
        "created_at",
        "artifact_sha256",
    }
    tpch = experiment.workload == "tpch"
    expected_fields = common_fields | (
        {"scale_factor", "table_counts"} if tpch else {"bronze_counts", "derived_counts"}
    )
    if set(value) != expected_fields:
        issues.append("Medallion audit has an unexpected top-level shape")
    expected_pipeline = "tpch-derived-iceberg-import-v1" if tpch else "ecommerce-medallion-v1"
    if (
        value.get("schema_version") != 1
        or value.get("status") != "passed"
        or value.get("pipeline") != expected_pipeline
        or value.get("dataset_id") != dataset_manifest.get("dataset_id")
        or value.get("dataset_manifest_sha256") != sha256_file(dataset_manifest_path)
        or value.get("benchmark_eligible") is not True
    ):
        issues.append("Medallion audit identity does not match the reviewed dataset")
    medallion_for_hash = dict(value)
    medallion_hash = medallion_for_hash.pop("artifact_sha256", None)
    if (
        _parse_utc_timestamp(value.get("created_at")) is None
        or not isinstance(medallion_hash, str)
        or medallion_hash != sha256_value(medallion_for_hash)
    ):
        issues.append("Medallion audit creation timestamp or self-hash is invalid")
    raw_dataset_hashes = {
        record.get("provenance", {}).get("dataset_manifest_sha256")
        for record in records
        if isinstance(record.get("provenance"), Mapping)
    }
    if raw_dataset_hashes != {value.get("dataset_manifest_sha256")}:
        issues.append("Medallion dataset manifest differs from the raw campaign provenance")

    raw_runtime = next(
        (record.get("runtime") for record in records if record.get("engine") == "spark_baseline"),
        None,
    )
    medallion_runtime = value.get("runtime")
    runtime_fields = (
        "spark_version",
        "scala_version",
        "java_version",
        "comet_version",
        "iceberg_version",
    )
    if (
        not isinstance(raw_runtime, Mapping)
        or not isinstance(medallion_runtime, Mapping)
        or any(medallion_runtime.get(field) != raw_runtime.get(field) for field in runtime_fields)
    ):
        issues.append("Medallion runtime differs from the baseline raw-record runtime")
    try:
        runtime_lock = validate_runtime_lock(
            repository_root / "runtime-versions.lock",
            repository_root / "benchmark/schemas/runtime-lock.schema.json",
        )
        components = {item["name"]: item for item in runtime_lock["components"]}
    except (ConfigurationError, KeyError, OSError, TypeError, ValueError) as error:
        issues.append(f"Medallion runtime lock is invalid: {error}")
    else:
        expected_runtime_fields = {
            "spark_version",
            "scala_version",
            "java_version",
            "java_runtime_version",
            "python_version",
            "comet_version",
            "iceberg_version",
            "iceberg_full_version",
            "machine",
            "runtime_lock_sha256",
            "artifact_sha256",
            "class_resources",
        }
        expected_artifacts = {
            name: str(components[name]["sha256_or_digest"]).removeprefix("sha256:")
            for name in (
                "scala",
                "datafusion-comet",
                "apache-iceberg-runtime",
                "apache-iceberg-aws-bundle",
            )
        }
        if (
            not isinstance(medallion_runtime, Mapping)
            or set(medallion_runtime) != expected_runtime_fields
            or medallion_runtime.get("runtime_lock_sha256")
            != sha256_file(repository_root / "runtime-versions.lock")
            or medallion_runtime.get("artifact_sha256") != expected_artifacts
            or medallion_runtime.get("python_version") != components["python"]["version"]
            or medallion_runtime.get("java_runtime_version") != components["java"]["version"]
            or medallion_runtime.get("comet_version") is not None
            or not isinstance(medallion_runtime.get("machine"), str)
            or not medallion_runtime.get("machine")
            or not isinstance(medallion_runtime.get("iceberg_full_version"), str)
            or components["apache-iceberg-runtime"]["version"]
            not in medallion_runtime.get("iceberg_full_version", "")
        ):
            issues.append("Medallion runtime fingerprint does not bind the current lock")
        class_resources = (
            medallion_runtime.get("class_resources")
            if isinstance(medallion_runtime, Mapping)
            else None
        )
        expected_resource_keys = {
            "scala_properties",
            "iceberg_build",
            "iceberg_s3_file_io",
            "aws_s3_client",
            "comet_plugin",
            "hadoop_s3a",
        }
        expected_jar_fragments = {
            "scala_properties": f"scala-library-{components['scala']['version']}.jar",
            "iceberg_build": (
                "iceberg-spark-runtime-4.1_2.13-"
                f"{components['apache-iceberg-runtime']['version']}.jar"
            ),
            "iceberg_s3_file_io": (
                "iceberg-spark-runtime-4.1_2.13-"
                f"{components['apache-iceberg-runtime']['version']}.jar"
            ),
            "aws_s3_client": (
                f"iceberg-aws-bundle-{components['apache-iceberg-aws-bundle']['version']}.jar"
            ),
            "comet_plugin": (
                f"comet-spark-spark4.1_2.13-{components['datafusion-comet']['version']}.jar"
            ),
        }
        if (
            not isinstance(class_resources, Mapping)
            or set(class_resources) != expected_resource_keys
        ):
            issues.append("Medallion class-resource fingerprint is incomplete")
        else:
            for key, fragment in expected_jar_fragments.items():
                origins = class_resources.get(key)
                if not isinstance(origins, list) or len(origins) != 1 or fragment not in origins[0]:
                    issues.append(f"Medallion class-resource origin is invalid: {key}")
            if class_resources.get("hadoop_s3a") != []:
                issues.append("Medallion runtime unexpectedly exposes Hadoop S3A classes")

    tables = dataset_manifest.get("tables")
    if not isinstance(tables, Mapping) or not tables:
        issues.append("reviewed dataset manifest has no table inventory")
        return
    source_counts = {
        str(name): table.get("row_count")
        for name, table in tables.items()
        if isinstance(table, Mapping)
    }
    if len(source_counts) != len(tables) or any(
        not _is_integer(count) or count < 0 for count in source_counts.values()
    ):
        issues.append("reviewed dataset table row counts are invalid")
    if tpch:
        if value.get("scale_factor") != config.get("workload", {}).get("scale_factor"):
            issues.append("Medallion TPC-H scale factor differs from the experiment config")
        if value.get("table_counts") != source_counts:
            issues.append("Medallion TPC-H table counts differ from the source manifest")
        if value.get("quality") != dataset_manifest.get("validation"):
            issues.append("Medallion TPC-H quality evidence differs from source validation")
        expected_snapshot_keys = {f"tpch.{name}" for name in source_counts}
    else:
        if value.get("bronze_counts") != source_counts:
            issues.append("Medallion Bronze counts differ from the source manifest")
        quality = value.get("quality")
        if (
            not isinstance(quality, Mapping)
            or not quality
            or any(not _is_integer(item) or item != 0 for item in quality.values())
        ):
            issues.append("Medallion E-commerce quality checks must all equal zero")
        derived_counts = value.get("derived_counts")
        required_derived = {
            "silver.sales_enriched",
            "silver.events",
            "gold.daily_revenue",
            "gold.customer_ltv",
            "gold.product_ranking",
            "gold.category_growth",
        }
        if (
            not isinstance(derived_counts, Mapping)
            or set(derived_counts) != required_derived
            or any(not _is_integer(item) or item < 0 for item in derived_counts.values())
        ):
            issues.append("Medallion derived table counts are incomplete or invalid")
        expected_snapshot_keys = {
            *(f"bronze.{name}" for name in source_counts),
            "silver.sales_enriched",
            "silver.events",
            "gold.daily_revenue",
            "gold.customer_ltv",
            "gold.product_ranking",
            "gold.category_growth",
        }

    snapshots = value.get("snapshots")
    if not isinstance(snapshots, Mapping) or set(snapshots) != expected_snapshot_keys:
        issues.append("Medallion snapshot inventory is incomplete or unexpected")
        return
    snapshot_ids: dict[str, int] = {}
    for name, snapshot in snapshots.items():
        if (
            not isinstance(snapshot, Mapping)
            or set(snapshot) != {"snapshot_id", "manifest_list"}
            or not _is_integer(snapshot.get("snapshot_id"))
            or int(snapshot["snapshot_id"]) < 1
            or not isinstance(snapshot.get("manifest_list"), str)
            or not snapshot.get("manifest_list")
        ):
            issues.append(f"Medallion snapshot entry is invalid: {name}")
            continue
        snapshot_ids[str(name)] = int(snapshot["snapshot_id"])
    if len(set(snapshot_ids.values())) != len(snapshot_ids):
        issues.append("Medallion snapshot IDs must be unique across physical tables")

    try:
        workload_path = _repo_path(repository_root, config["workload"]["manifest_file"])
        workload = load_document(
            workload_path,
            repository_root / "benchmark/schemas/workload-manifest.schema.json",
        )
        bindings = workload["relation_bindings"]
        selected_snapshot_ids = sorted(
            {
                snapshot_ids[
                    _table_identifier(binding["logical_table"], suite=experiment.workload)[1]
                ]
                for binding in bindings.values()
            }
        )
    except (ConfigurationError, KeyError, OSError, TypeError, ValueError) as error:
        issues.append(f"Medallion workload snapshot binding is invalid: {error}")
        return
    raw_snapshot_sets = {
        tuple(record.get("provenance", {}).get("iceberg_snapshot_ids", ()))
        for record in records
        if isinstance(record.get("provenance"), Mapping)
    }
    if raw_snapshot_sets != {tuple(selected_snapshot_ids)}:
        issues.append("raw Iceberg snapshot IDs do not match the Medallion workload bindings")


def _failed_attempt_records_check(
    root: Path,
    run_attempt_root: Path,
    records: list[Mapping[str, Any]],
    repository_root: Path,
    issues: list[str],
) -> dict[str, int]:
    paths = tuple(sorted(path for path in root.rglob("*.json") if path.is_file()))
    all_files = {path for path in root.rglob("*") if path.is_file()}
    if all_files != set(paths):
        issues.append("failed-attempt root contains a non-JSON or unexpected file")
    experiment_ids = {str(record.get("experiment_id")) for record in records}
    engines = {str(record.get("engine")) for record in records}
    for directory in (path for path in root.rglob("*") if path.is_dir()):
        relative_parts = directory.relative_to(root).parts
        if (
            not relative_parts
            or len(relative_parts) > 2
            or relative_parts[0] not in experiment_ids
            or (len(relative_parts) == 2 and relative_parts[1] not in engines)
        ):
            issues.append(f"failed-attempt root contains an unexpected directory: {directory}")
    if not paths:
        return {}
    try:
        schema_value: object = json.loads(
            (repository_root / "benchmark/schemas/raw-result.schema.json").read_text(
                encoding="utf-8"
            )
        )
    except (OSError, json.JSONDecodeError) as error:
        issues.append(f"raw-result schema is unreadable while checking failed attempts: {error}")
        return {}
    if not isinstance(schema_value, dict):
        issues.append("raw-result schema root is invalid while checking failed attempts")
        return {}
    validator = Draft202012Validator(schema_value, format_checker=FormatChecker())
    succeeded_by_run = {str(record.get("run_id")): record for record in records}
    sequences: dict[str, list[int]] = defaultdict(list)
    failure_timestamps: dict[str, list[tuple[int, datetime]]] = defaultdict(list)
    pattern = re.compile(r"^(?P<run>.+)-attempt-(?P<attempt>[0-9]{4})\.json$")
    failed_records: list[Mapping[str, Any]] = []
    for path in paths:
        match = pattern.fullmatch(path.name)
        if match is None or int(match.group("attempt")) < 1:
            issues.append(f"failed-attempt filename is invalid: {path.name}")
            continue
        run_id = match.group("run")
        attempt = int(match.group("attempt"))
        success = succeeded_by_run.get(run_id)
        if success is None:
            issues.append(
                f"failed-attempt record does not map to a planned successful run: {run_id}"
            )
            continue
        expected_parent = root / str(success.get("experiment_id")) / str(success.get("engine"))
        if path.parent.resolve() != expected_parent.resolve():
            issues.append(f"failed-attempt record has a non-canonical campaign path: {path}")
        value = _load_json_object(path, label=f"failed attempt {path.name}", issues=issues)
        if value is None:
            continue
        phase = success.get("phase")
        engine = success.get("engine")
        if phase not in {"correctness", "plan_capture", "measurement"} or engine not in {
            "spark_baseline",
            "comet_accelerated",
        }:
            issues.append(f"successful run identity is invalid for failed attempt {path.name}")
            continue
        run = CampaignRun(
            experiment_id=str(success.get("experiment_id")),
            run_id=run_id,
            phase=cast(Literal["correctness", "plan_capture", "measurement"], phase),
            engine=cast(Literal["spark_baseline", "comet_accelerated"], engine),
            pair_index=success.get("pair_index"),
            order_index=1,
            warmup_runs=0,
            timeout_seconds=1,
        )
        expected_provenance = {
            **dict(success.get("provenance", {})),
            "resources": dict(success.get("resources", {})),
        }
        try:
            validate_run_record(
                value,
                run,
                validator,
                expected_provenance=expected_provenance,
            )
        except (CampaignError, KeyError, TypeError, ValueError) as error:
            issues.append(f"failed attempt {path.name} is invalid: {error}")
            continue
        if value.get("status") not in {"failed", "timeout"}:
            issues.append(f"failed attempt {path.name} has a non-retryable terminal status")
        if value.get("runtime") != success.get("runtime"):
            issues.append(f"failed attempt {path.name} runtime differs from its successful run")
        failure_timestamp = _parse_utc_timestamp(value.get("timestamp"))
        if failure_timestamp is None:
            issues.append(f"failed attempt {path.name} timestamp is invalid")
        else:
            failure_timestamps[run_id].append((attempt, failure_timestamp))
        _record_artifact_ownership_check(
            value,
            run_attempt_root / run_id / f"attempt-{attempt:04d}",
            repository_root,
            allow_null=True,
            label=f"failed attempt {path.name}",
            issues=issues,
        )
        sequences[run_id].append(attempt)
        failed_records.append(value)
    for run_id, observed in sequences.items():
        ordered = sorted(observed)
        if ordered != list(range(1, ordered[-1] + 1)):
            issues.append(f"failed-attempt sequence is not contiguous for {run_id}")
        ordered_failure_times = [
            timestamp for _, timestamp in sorted(failure_timestamps.get(run_id, ()))
        ]
        success_timestamp = _parse_utc_timestamp(succeeded_by_run[run_id].get("timestamp"))
        if (
            len(ordered_failure_times) != len(ordered)
            or success_timestamp is None
            or any(
                current >= following
                for current, following in zip(
                    ordered_failure_times,
                    [*ordered_failure_times[1:], success_timestamp],
                    strict=True,
                )
            )
        ):
            issues.append(f"failed-attempt chronology is invalid for {run_id}")
    if failed_records:
        try:
            artifact_evidence(failed_records, repository_root, require_succeeded=False)
        except (ArtifactEvidenceError, OSError) as error:
            issues.append(f"failed-attempt artifact evidence is incomplete: {error}")
    return {run_id: len(attempts) for run_id, attempts in sequences.items()}


def _record_artifact_ownership_check(
    record: Mapping[str, Any],
    attempt_dir: Path,
    repository_root: Path,
    *,
    allow_null: bool,
    label: str,
    issues: list[str],
) -> None:
    artifacts = record.get("artifacts")
    if not isinstance(artifacts, Mapping):
        issues.append(f"{label} has no artifact mapping")
        return
    expected_names = {
        "event_log": "event-log",
        "physical_plan": "final-plan.txt",
        "resource_samples": "worker-resource-samples.json",
        "stdout": "stdout.log",
        "stderr": "stderr.log",
    }
    for field in ARTIFACT_FIELDS:
        declared = artifacts.get(field)
        if allow_null and declared is None:
            continue
        try:
            path = _repo_relative_path(
                repository_root,
                declared,
                label=f"{label} artifact {field}",
            )
        except ConfigurationError as error:
            issues.append(str(error))
            continue
        expected = (attempt_dir / expected_names[field]).resolve()
        if path.resolve() != expected:
            issues.append(f"{label} artifact {field} is outside its canonical run attempt")


def _resource_sample_from_json(value: object, *, index: int) -> ResourceSample:
    if not isinstance(value, Mapping) or set(value) != _RESOURCE_SAMPLE_FIELDS:
        raise ValueError(f"sample {index} has an unexpected field set")
    timestamp_ns = value.get("timestamp_ns")
    source = value.get("source")
    status = value.get("status")
    if not _is_integer(timestamp_ns):
        raise ValueError(f"sample {index} timestamp_ns is not an integer")
    if not isinstance(source, str) or not source:
        raise ValueError(f"sample {index} source is invalid")
    if not isinstance(status, str):
        raise ValueError(f"sample {index} status is invalid")
    integer_fields = (
        "cpu_usage_ns",
        "memory_current_bytes",
        "memory_peak_bytes",
        "swap_current_bytes",
        "io_read_bytes",
        "io_write_bytes",
        "process_count",
    )
    for field in integer_fields:
        observed = value.get(field)
        if observed is not None and not _is_integer(observed):
            raise ValueError(f"sample {index} {field} is not an integer or null")
    cpu_limit = value.get("cpu_limit_cores")
    if cpu_limit is not None and (
        isinstance(cpu_limit, bool) or not isinstance(cpu_limit, int | float)
    ):
        raise ValueError(f"sample {index} cpu_limit_cores is not numeric or null")
    missing_metrics = value.get("missing_metrics")
    errors = value.get("errors")
    if not isinstance(missing_metrics, list) or not all(
        isinstance(item, str) for item in missing_metrics
    ):
        raise ValueError(f"sample {index} missing_metrics is invalid")
    if not isinstance(errors, list) or not all(isinstance(item, str) for item in errors):
        raise ValueError(f"sample {index} errors is invalid")
    return ResourceSample(
        timestamp_ns=timestamp_ns,
        source=source,
        status=CollectorStatus(status),
        cpu_usage_ns=cast(int | None, value.get("cpu_usage_ns")),
        memory_current_bytes=cast(int | None, value.get("memory_current_bytes")),
        memory_peak_bytes=cast(int | None, value.get("memory_peak_bytes")),
        swap_current_bytes=cast(int | None, value.get("swap_current_bytes")),
        io_read_bytes=cast(int | None, value.get("io_read_bytes")),
        io_write_bytes=cast(int | None, value.get("io_write_bytes")),
        cpu_limit_cores=float(cpu_limit) if cpu_limit is not None else None,
        process_count=cast(int | None, value.get("process_count")),
        missing_metrics=tuple(missing_metrics),
        errors=tuple(errors),
    )


def _worker_resource_summary(
    path: Path,
    *,
    label: str,
    issues: list[str],
) -> dict[str, object] | None:
    value = _load_json_object(path, label=f"{label} worker resource evidence", issues=issues)
    if value is None:
        return None
    if set(value) != _WORKER_RESOURCE_FIELDS:
        issues.append(f"{label} worker resource evidence has an unexpected field set")
    if not _is_integer(value.get("schema_version")) or value.get("schema_version") != 1:
        issues.append(f"{label} worker resource schema_version must equal 1")
    if value.get("scope") != "spark-worker-executor-container":
        issues.append(f"{label} worker resource scope is invalid")
    expected_flags = {
        "timed_out": False,
        "window_started": True,
        "window_completed": True,
        "aborted": False,
    }
    for field, expected in expected_flags.items():
        if value.get(field) is not expected:
            issues.append(f"{label} worker resource {field} must equal {expected!r}")
    raw_samples = value.get("samples")
    if not isinstance(raw_samples, list) or len(raw_samples) < 2:
        issues.append(f"{label} worker resource evidence requires at least two samples")
        return None
    try:
        samples = [
            _resource_sample_from_json(sample, index=index)
            for index, sample in enumerate(raw_samples)
        ]
        recomputed = aggregate_samples(samples).as_dict()
    except (TypeError, ValueError) as error:
        issues.append(f"{label} worker resource samples are invalid: {error}")
        return None
    if value.get("summary") != recomputed:
        issues.append(f"{label} worker resource summary does not match its samples")
    if recomputed.get("status") != "complete":
        issues.append(f"{label} worker resource sampling window is not complete")
    return recomputed


def _attempt_admission_check(
    path: Path,
    planned_run: CampaignRun,
    attempt: int,
    terminal_record: Mapping[str, Any],
    issues: list[str],
) -> None:
    label = f"run attempt {planned_run.run_id} attempt-{attempt:04d}"
    value = _load_json_object(path, label=f"{label} admission", issues=issues)
    if value is None:
        return
    expected_fields = {
        "schema_version",
        "artifact_class",
        "attempt_index",
        "run",
        "provenance",
        "runtime",
        "created_at",
        "artifact_sha256",
    }
    if set(value) != expected_fields:
        issues.append(f"{label} admission has an unexpected field set")
    payload = dict(value)
    declared_hash = payload.pop("artifact_sha256", None)
    if not isinstance(declared_hash, str) or declared_hash != sha256_value(payload):
        issues.append(f"{label} admission self-hash is invalid")
    if value.get("schema_version") != 1 or isinstance(value.get("schema_version"), bool):
        issues.append(f"{label} admission schema_version must equal 1")
    if value.get("artifact_class") != "research-run-attempt-admission-v1":
        issues.append(f"{label} admission artifact_class is invalid")
    if value.get("attempt_index") != attempt or isinstance(value.get("attempt_index"), bool):
        issues.append(f"{label} admission attempt_index is invalid")
    expected_run = {
        "experiment_id": planned_run.experiment_id,
        "run_id": planned_run.run_id,
        "phase": planned_run.phase,
        "engine": planned_run.engine,
        "pair_index": planned_run.pair_index,
        "order_index": planned_run.order_index,
        "warmup_runs": planned_run.warmup_runs,
        "timeout_seconds": planned_run.timeout_seconds,
    }
    if value.get("run") != expected_run:
        issues.append(f"{label} admission run identity differs from the current campaign plan")
    provenance = terminal_record.get("provenance")
    resources = terminal_record.get("resources")
    expected_provenance = (
        {**dict(provenance), "resources": dict(resources)}
        if isinstance(provenance, Mapping) and isinstance(resources, Mapping)
        else None
    )
    if expected_provenance is None or value.get("provenance") != expected_provenance:
        issues.append(f"{label} admission provenance differs from its terminal record")
    if value.get("runtime") != terminal_record.get("runtime"):
        issues.append(f"{label} admission runtime differs from its terminal record")
    created_at = _parse_utc_timestamp(value.get("created_at"))
    terminal_at = _parse_utc_timestamp(terminal_record.get("timestamp"))
    if created_at is None or terminal_at is None or created_at >= terminal_at:
        issues.append(f"{label} admission timestamp does not precede its terminal record")


def _successful_run_evidence_check(
    record: Mapping[str, Any],
    planned_run: CampaignRun,
    attempt_dir: Path,
    expected_workload_manifest_sha256: str,
    raw_schema: Mapping[str, Any],
    issues: list[str],
) -> None:
    run_id = str(record.get("run_id"))
    label = f"successful run {run_id}"
    application = _load_json_object(
        attempt_dir / "application-result.json",
        label=f"{label} application result",
        issues=issues,
    )
    if application is None:
        return
    if set(application) != _APPLICATION_RESULT_FIELDS:
        issues.append(f"{label} application result has an unexpected field set")
    if not _is_integer(application.get("schema_version")) or application.get("schema_version") != 1:
        issues.append(f"{label} application result schema_version must equal 1")
    if application.get("artifact_class") != "benchmark-application-run-v1":
        issues.append(f"{label} application result artifact_class is invalid")
    if application.get("status") != "succeeded":
        issues.append(f"{label} application result status must equal 'succeeded'")
    if application.get("workload_manifest_sha256") != expected_workload_manifest_sha256:
        issues.append(f"{label} application result does not bind the current workload manifest")
    application_artifacts = application.get("artifacts")
    expected_application_artifacts = {
        "initial_plan": "initial-plan.txt",
        "final_plan": "final-plan.txt",
        "driver_resource_samples": "driver-resource-samples.json",
    }
    if application_artifacts != expected_application_artifacts:
        issues.append(f"{label} application artifact declarations are not canonical")
    else:
        for name in expected_application_artifacts.values():
            artifact_path = attempt_dir / name
            if artifact_path.is_symlink() or not artifact_path.is_file():
                issues.append(f"{label} application artifact is missing or a symlink: {name}")

    schema_json = application.get("schema_json")
    if not isinstance(schema_json, str):
        issues.append(f"{label} application schema_json is missing")
    else:
        try:
            recomputed_schema_hash = schema_hash(schema_json)
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            issues.append(f"{label} application schema_json is invalid: {error}")
        else:
            if application.get("schema_sha256") != recomputed_schema_hash:
                issues.append(f"{label} application schema_sha256 is not derived from schema_json")

    plan_path = attempt_dir / "final-plan.txt"
    try:
        plan_text = plan_path.read_text(encoding="utf-8")
    except OSError as error:
        issues.append(f"{label} final physical plan is unreadable: {error}")
        return
    engine = record.get("engine")
    phase = record.get("phase")
    pair_index = record.get("pair_index")
    if engine not in _ENGINES or phase not in {"correctness", "plan_capture", "measurement"}:
        issues.append(f"{label} has an invalid run identity")
        return
    recomputed_plan = analyze_plan(
        plan_text,
        comet_enabled=engine == "comet_accelerated",
    )
    if application.get("plan_analysis") != recomputed_plan:
        issues.append(f"{label} application plan_analysis does not match final-plan.txt")
    if record.get("plan_analysis") != recomputed_plan:
        issues.append(f"{label} raw plan_analysis does not match final-plan.txt")

    resource_summary = _worker_resource_summary(
        attempt_dir / "worker-resource-samples.json",
        label=label,
        issues=issues,
    )
    if resource_summary is None:
        return
    try:
        event_report = parse_event_log(attempt_dir / "event-log")
    except (OSError, ValueError) as error:
        issues.append(f"{label} Spark event log is unreadable: {error}")
        return
    runtime = record.get("runtime")
    if not isinstance(runtime, Mapping) or event_report.spark_version != runtime.get(
        "spark_version"
    ):
        issues.append(f"{label} event-log Spark version differs from the raw runtime")

    provenance = record.get("provenance")
    resources = record.get("resources")
    artifacts = record.get("artifacts")
    if (
        not isinstance(provenance, Mapping)
        or not isinstance(resources, Mapping)
        or not isinstance(artifacts, Mapping)
    ):
        issues.append(f"{label} cannot be rebuilt because raw identity fields are malformed")
        return
    required_strings = (
        provenance.get("git_commit"),
        provenance.get("container_image_digest"),
        provenance.get("spark_conf_sha256"),
        resources.get("cpu_model"),
        *(artifacts.get(field) for field in ARTIFACT_FIELDS),
    )
    required_integers = (
        resources.get("allocated_cores"),
        resources.get("cgroup_memory_limit_mib"),
        resources.get("executor_heap_mib"),
        resources.get("off_heap_mib"),
    )
    if not all(isinstance(item, str) and item for item in required_strings) or not all(
        _is_integer(item) for item in required_integers
    ):
        issues.append(f"{label} cannot be rebuilt because raw provenance/resources are malformed")
        return
    if pair_index is not None and not _is_integer(pair_index):
        issues.append(f"{label} cannot be rebuilt because pair_index is invalid")
        return
    context = RawRecordContext(
        git_commit=cast(str, provenance.get("git_commit")),
        container_image_digest=cast(str, provenance.get("container_image_digest")),
        spark_conf_sha256=cast(str, provenance.get("spark_conf_sha256")),
        cpu_model=cast(str, resources.get("cpu_model")),
        allocated_cores=cast(int, resources.get("allocated_cores")),
        cgroup_memory_limit_mib=cast(int, resources.get("cgroup_memory_limit_mib")),
        executor_heap_mib=cast(int, resources.get("executor_heap_mib")),
        off_heap_mib=cast(int, resources.get("off_heap_mib")),
        event_log=cast(str, artifacts.get("event_log")),
        physical_plan=cast(str, artifacts.get("physical_plan")),
        resource_samples=cast(str, artifacts.get("resource_samples")),
        stdout=cast(str, artifacts.get("stdout")),
        stderr=cast(str, artifacts.get("stderr")),
        executor_resources=resource_summary,
    )
    try:
        rebuilt = build_raw_record(
            planned_run,
            application,
            event_report,
            context,
            raw_schema=raw_schema,
        )
    except (CampaignError, KeyError, TypeError, ValueError) as error:
        issues.append(f"{label} cannot be rebuilt from source evidence: {error}")
        return
    observed = dict(record)
    if rebuilt != observed:
        fields = sorted(
            key for key in set(rebuilt) | set(observed) if rebuilt.get(key) != observed.get(key)
        )
        issues.append(
            f"{label} raw record differs from independently rebuilt evidence: " + ", ".join(fields)
        )


def _current_planned_runs(
    experiment: CoreExperiment,
    repository_root: Path,
) -> tuple[dict[str, CampaignRun], str]:
    config_path = repository_root / experiment.config
    config = load_experiment(config_path, repository_root / "benchmark/schemas")
    common_profile, comet_profile = runtime_profile_paths(config, repository_root)
    workload = config["workload"]
    manifest = build_experiment_manifest(
        config,
        config_path=config_path,
        runtime_lock_path=repository_root / "runtime-versions.lock",
        workload_sql_path=_repo_path(repository_root, workload["sql_file"]),
        workload_manifest_path=_repo_path(repository_root, workload["manifest_file"]),
        dataset_manifest_path=_repo_path(repository_root, workload["dataset_manifest"]),
        uv_lock_path=repository_root / "uv.lock",
        spark_defaults_path=common_profile,
        comet_profile_path=comet_profile,
    )
    planned = plan_campaign(manifest)
    return (
        {run.run_id: run for run in planned},
        str(manifest["input_hashes"]["workload_manifest_sha256"]),
    )


def _control_artifact_check(
    report: Mapping[str, Any],
    experiment: CoreExperiment,
    records: list[Mapping[str, Any]],
    repository_root: Path,
    cache: dict[str, tuple[dict[str, object] | None, tuple[str, ...]]],
) -> tuple[dict[str, object] | None, list[str]]:
    """Rebuild campaign-control evidence and independently count execution attempts."""

    issues: list[str] = []
    execution_attempt_count = report.get("execution_attempt_count")
    failed_attempt_record_count = report.get("failed_attempt_record_count")
    if not _is_integer(execution_attempt_count) or execution_attempt_count < EXPECTED_CAMPAIGN_RUNS:
        issues.append(
            f"report.execution_attempt_count must be an integer >= {EXPECTED_CAMPAIGN_RUNS}"
        )
    if not _is_integer(failed_attempt_record_count) or failed_attempt_record_count < 0:
        issues.append("report.failed_attempt_record_count must be a non-negative integer")

    declared = report.get("control_artifacts")
    if not isinstance(declared, Mapping):
        issues.append("report.control_artifacts must be an object")
        return None, issues
    cache_key = sha256_value(
        {
            "control_artifacts": dict(declared),
            "experiment_id": experiment.experiment_id,
            "raw_records_sha256": raw_records_sha256(records),
            "execution_attempt_count": execution_attempt_count,
            "failed_attempt_record_count": failed_attempt_record_count,
        }
    )
    cached = cache.get(cache_key)
    if cached is not None:
        evidence, cached_issues = cached
        return evidence, list(cached_issues)

    def finish(
        evidence: dict[str, object] | None,
    ) -> tuple[dict[str, object] | None, list[str]]:
        cache[cache_key] = (evidence, tuple(issues))
        return evidence, issues

    raw_targets = declared.get("targets")
    if not isinstance(raw_targets, list):
        issues.append("report.control_artifacts.targets must be an array")
        return finish(None)

    targets: dict[str, Path] = {}
    malformed = False
    for index, raw_target in enumerate(raw_targets):
        if not isinstance(raw_target, Mapping) or set(raw_target) != {"label", "path", "kind"}:
            issues.append(
                f"report.control_artifacts.targets[{index}] must contain exactly label/path/kind"
            )
            malformed = True
            continue
        label = raw_target.get("label")
        declared_path = raw_target.get("path")
        kind = raw_target.get("kind")
        if not isinstance(label, str) or not label:
            issues.append(f"report.control_artifacts.targets[{index}].label is invalid")
            malformed = True
            continue
        if label in targets:
            issues.append(f"report.control_artifacts target label is duplicated: {label}")
            malformed = True
            continue
        if kind not in {"file", "directory"}:
            issues.append(f"report.control_artifacts.targets[{index}].kind is invalid")
            malformed = True
            continue
        expected_kind = (
            "directory" if label in {"run-attempts", "failed-attempt-records"} else "file"
        )
        if kind != expected_kind:
            issues.append(
                f"report.control_artifacts.targets[{index}].kind must equal {expected_kind!r} "
                f"for {label!r}"
            )
            malformed = True
            continue
        try:
            targets[label] = _repo_relative_path(
                repository_root,
                declared_path,
                label=f"control artifact {label!r}",
            )
        except ConfigurationError as error:
            issues.append(str(error))
            malformed = True

    required_labels = {
        "run-attempts",
        "failed-attempt-records",
        "dataset-validation-attestation",
        "medallion-audit",
    }
    missing_labels = sorted(required_labels - set(targets))
    if not any(label.startswith("capacity-gate-") for label in targets):
        missing_labels.append("capacity-gate-<attempt>")
    if not any(label.startswith("collector-calibration-") for label in targets):
        missing_labels.append("collector-calibration-<attempt>")
    if missing_labels:
        issues.append("report.control_artifacts is missing targets: " + ", ".join(missing_labels))
    unexpected_labels = sorted(
        label
        for label in targets
        if label not in required_labels
        and re.fullmatch(r"(?:capacity-gate|collector-calibration)-[0-9]{4}", label) is None
    )
    if unexpected_labels:
        issues.append(
            "report.control_artifacts has unexpected targets: " + ", ".join(unexpected_labels)
        )
        malformed = True
    if malformed or missing_labels:
        return finish(None)

    try:
        actual = control_artifact_evidence(targets, repository_root)
    except (ArtifactEvidenceError, OSError) as error:
        issues.append(f"campaign control-artifact evidence is incomplete: {error}")
        return finish(None)
    if dict(declared) != actual:
        issues.append("report.control_artifacts does not bind current campaign control artifacts")

    run_attempt_root = targets["run-attempts"]
    actual_execution_attempts = 0
    if run_attempt_root.is_dir():
        actual_execution_attempts = sum(
            1
            for run_dir in run_attempt_root.iterdir()
            if run_dir.is_dir()
            for attempt_dir in run_dir.iterdir()
            if attempt_dir.is_dir() and _RUN_ATTEMPT_PATTERN.fullmatch(attempt_dir.name) is not None
        )
    if execution_attempt_count != actual_execution_attempts:
        issues.append(
            "report.execution_attempt_count does not equal the run-attempt directory count"
        )

    failed_attempt_root = targets["failed-attempt-records"]
    actual_failed_records = (
        sum(1 for path in failed_attempt_root.rglob("*.json") if path.is_file())
        if failed_attempt_root.is_dir()
        else 0
    )
    if failed_attempt_record_count != actual_failed_records:
        issues.append(
            "report.failed_attempt_record_count does not equal the failed-attempt JSON count"
        )
    if (
        _is_integer(execution_attempt_count)
        and _is_integer(failed_attempt_record_count)
        and execution_attempt_count != EXPECTED_CAMPAIGN_RUNS + failed_attempt_record_count
    ):
        issues.append("report.execution_attempt_count must equal planned runs plus failed attempts")

    failed_attempt_counts = _failed_attempt_records_check(
        failed_attempt_root,
        run_attempt_root,
        records,
        repository_root,
        issues,
    )

    campaign_control_root = run_attempt_root.parent
    expected_control_base = (
        CAMPAIGN_CONTROL_ROOT
        if repository_root.resolve() == ROOT.resolve()
        else repository_root / ".artifacts/campaigns"
    )
    expected_campaign_control_root = (expected_control_base / experiment.experiment_id).resolve()
    if (
        campaign_control_root.resolve() != expected_campaign_control_root
        or run_attempt_root.name != "runs"
        or failed_attempt_root != campaign_control_root / "failed-attempts"
    ):
        issues.append(
            "run and failed-attempt controls must use the canonical campaign-local control root"
        )
    expected_run_ids = {str(record.get("run_id")) for record in records}
    actual_run_entries = tuple(run_attempt_root.iterdir()) if run_attempt_root.is_dir() else ()
    actual_run_ids = {path.name for path in actual_run_entries if path.is_dir()}
    if any(not path.is_dir() for path in actual_run_entries) or actual_run_ids != expected_run_ids:
        issues.append("run-attempt directory names must equal the exact planned run IDs")
    for run_id in sorted(expected_run_ids):
        run_dir = run_attempt_root / run_id
        entries = tuple(run_dir.iterdir()) if run_dir.is_dir() else ()
        attempts = sorted(
            int(match.group(1))
            for path in entries
            if path.is_dir() and (match := _RUN_ATTEMPT_PATTERN.fullmatch(path.name)) is not None
        )
        expected_attempts = failed_attempt_counts.get(run_id, 0) + 1
        if (
            any(
                not path.is_dir() or _RUN_ATTEMPT_PATTERN.fullmatch(path.name) is None
                for path in entries
            )
            or attempts != list(range(1, expected_attempts + 1))
            or expected_attempts > 3
        ):
            issues.append(
                f"run-attempt sequence for {run_id} must be contiguous, equal failures + one "
                "success, and contain at most 3 attempts"
            )
    records_by_run = {str(record.get("run_id")): record for record in records}
    raw_schema_value = _load_json_object(
        repository_root / "benchmark/schemas/raw-result.schema.json",
        label="raw-result schema for source-evidence reconstruction",
        issues=issues,
    )
    try:
        planned_by_run, expected_workload_manifest_sha256 = _current_planned_runs(
            experiment,
            repository_root,
        )
    except (CampaignError, ConfigurationError, KeyError, OSError, TypeError, ValueError) as error:
        issues.append(f"current campaign plan is unreadable for attempt evidence: {error}")
        planned_by_run = {}
        expected_workload_manifest_sha256 = None
    for run_id, record in records_by_run.items():
        successful_attempt = failed_attempt_counts.get(run_id, 0) + 1
        attempt_dir = run_attempt_root / run_id / f"attempt-{successful_attempt:04d}"
        _record_artifact_ownership_check(
            record,
            attempt_dir,
            repository_root,
            allow_null=False,
            label=f"successful run {run_id}",
            issues=issues,
        )
        planned_run = planned_by_run.get(run_id)
        if planned_run is not None:
            for attempt in range(1, successful_attempt + 1):
                terminal: Mapping[str, Any] | None
                if attempt == successful_attempt:
                    terminal = record
                else:
                    terminal = _load_json_object(
                        failed_attempt_root
                        / str(record.get("experiment_id"))
                        / str(record.get("engine"))
                        / f"{run_id}-attempt-{attempt:04d}.json",
                        label=f"failed terminal record for {run_id} attempt-{attempt:04d}",
                        issues=issues,
                    )
                if terminal is not None:
                    _attempt_admission_check(
                        run_attempt_root
                        / run_id
                        / f"attempt-{attempt:04d}"
                        / "attempt-admission.json",
                        planned_run,
                        attempt,
                        terminal,
                        issues,
                    )
        if (
            raw_schema_value is not None
            and planned_run is not None
            and expected_workload_manifest_sha256 is not None
        ):
            _successful_run_evidence_check(
                record,
                planned_run,
                attempt_dir,
                expected_workload_manifest_sha256,
                raw_schema_value,
                issues,
            )

    medallion = _load_json_object(
        targets["medallion-audit"], label="Medallion audit", issues=issues
    )
    if medallion is not None and medallion.get("status") != "passed":
        issues.append("Medallion audit status must equal 'passed'")
    _latest_passed_control_attempt(
        targets,
        prefix="capacity-gate-",
        passed_field="passed",
        passed_value=True,
        label="capacity gate",
        issues=issues,
    )
    _latest_passed_control_attempt(
        targets,
        prefix="collector-calibration-",
        passed_field="status",
        passed_value="passed",
        label="collector calibration",
        issues=issues,
    )
    capacity_attempts = _control_attempt_paths(
        targets,
        prefix="capacity-gate-",
        label="capacity gate",
        issues=issues,
    )
    for attempt, path in capacity_attempts:
        expected_name = (
            "capacity-gate.json" if attempt == 1 else f"capacity-gate-attempt-{attempt:04d}.json"
        )
        if path.parent != campaign_control_root or path.name != expected_name:
            issues.append(
                f"capacity gate attempt {attempt:04d} is not in the canonical campaign path"
            )
    capacity_on_disk = _attempt_files_on_disk(
        campaign_control_root / "capacity-gate.json",
        label="capacity gate",
        issues=issues,
    )
    if tuple((attempt, path.resolve()) for attempt, path in capacity_attempts) != capacity_on_disk:
        issues.append("capacity-gate declarations do not include the complete on-disk history")
    try:
        config_path = repository_root / experiment.config
        config = load_experiment(config_path, repository_root / "benchmark/schemas")
        dataset_manifest_path = _repo_path(repository_root, config["workload"]["dataset_manifest"])
        dataset_manifest_value: object = json.loads(
            dataset_manifest_path.read_text(encoding="utf-8")
        )
        if not isinstance(dataset_manifest_value, dict):
            raise ConfigurationError("dataset manifest root must be an object")
    except (ConfigurationError, KeyError, OSError, json.JSONDecodeError, TypeError) as error:
        issues.append(f"capacity-gate inputs are unreadable: {error}")
    else:
        for attempt, path in capacity_attempts:
            _capacity_artifact_check(
                path,
                label=f"capacity gate attempt {attempt:04d}",
                config=config,
                config_path=config_path,
                dataset_manifest=dataset_manifest_value,
                dataset_manifest_path=dataset_manifest_path,
                issues=issues,
            )
        _medallion_artifact_check(
            targets["medallion-audit"],
            experiment=experiment,
            records=records,
            config=config,
            dataset_manifest=dataset_manifest_value,
            dataset_manifest_path=dataset_manifest_path,
            repository_root=repository_root,
            issues=issues,
        )
    calibration_attempts = _control_attempt_paths(
        targets,
        prefix="collector-calibration-",
        label="collector calibration",
        issues=issues,
    )
    calibration_base: Path | None = None
    for attempt, path in calibration_attempts:
        expected_name = (
            "collector-calibration.json"
            if attempt == 1
            else f"collector-calibration-attempt-{attempt:04d}.json"
        )
        if path.name != expected_name:
            issues.append(
                f"collector calibration attempt {attempt:04d} has a non-canonical filename"
            )
        if calibration_base is None:
            calibration_base = path.parent / "collector-calibration.json"
        elif path.parent != calibration_base.parent:
            issues.append("collector calibration attempts must share one producer directory")
    if calibration_base is not None:
        calibration_on_disk = _attempt_files_on_disk(
            calibration_base,
            label="collector calibration",
            issues=issues,
        )
        if (
            tuple((attempt, path.resolve()) for attempt, path in calibration_attempts)
            != calibration_on_disk
        ):
            issues.append(
                "collector-calibration declarations do not include the complete on-disk history"
            )
    raw_git_commits = {
        record.get("provenance", {}).get("git_commit")
        for record in records
        if isinstance(record.get("provenance"), Mapping)
    }
    raw_image_digests = {
        record.get("provenance", {}).get("container_image_digest")
        for record in records
        if isinstance(record.get("provenance"), Mapping)
    }
    raw_cpu_models = {
        record.get("resources", {}).get("cpu_model")
        for record in records
        if isinstance(record.get("resources"), Mapping)
    }
    expected_calibration_environment: dict[str, str] | None = None
    if (
        calibration_base is not None
        and len(raw_git_commits) == len(raw_image_digests) == len(raw_cpu_models) == 1
        and all(
            isinstance(next(iter(values)), str) and bool(next(iter(values)))
            for values in (raw_git_commits, raw_image_digests, raw_cpu_models)
        )
    ):
        git_commit = str(next(iter(raw_git_commits)))
        image_digest = str(next(iter(raw_image_digests)))
        cpu_model = str(next(iter(raw_cpu_models)))
        storage_identity = calibration_base.parent.name
        expected_calibration_environment = {
            "git_commit": git_commit,
            "container_image_digest": image_digest,
            "storage_identity_sha256": storage_identity,
            "cpu_model": cpu_model,
        }
        expected_shared_base = (
            RESEARCH_SHARED_ROOT
            if repository_root.resolve() == ROOT.resolve()
            else repository_root / ".artifacts/research-shared"
        )
        expected_calibration_root = (
            expected_shared_base
            / git_commit
            / image_digest.removeprefix("sha256:")
            / storage_identity
        ).resolve()
        if (
            _SHA256_PATTERN.fullmatch(storage_identity) is None
            or calibration_base.parent.resolve() != expected_calibration_root
        ):
            issues.append("collector calibration is outside its canonical shared producer key")
        for attempt, capacity_path in capacity_attempts:
            capacity_value = _load_json_object(
                capacity_path,
                label=f"capacity gate attempt {attempt:04d}",
                issues=issues,
            )
            if capacity_value is not None and capacity_value.get("environment") != (
                expected_calibration_environment
            ):
                issues.append(
                    f"capacity gate attempt {attempt:04d} environment differs from raw and "
                    "calibration identity"
                )
        if isinstance(medallion, Mapping):
            dataset_hash = medallion.get("dataset_manifest_sha256")
            attestation_hash = medallion.get("dataset_validation_attestation_sha256")
            if isinstance(dataset_hash, str) and isinstance(attestation_hash, str):
                expected_medallion_path = (
                    expected_calibration_root
                    / "datasets"
                    / dataset_hash
                    / attestation_hash
                    / "medallion.json"
                ).resolve()
                if targets["medallion-audit"].resolve() != expected_medallion_path:
                    issues.append("Medallion audit is outside its canonical shared producer key")
            else:
                issues.append("Medallion audit lacks canonical dataset identity hashes")
    else:
        issues.append("collector calibration environment cannot be derived from raw records")
    for attempt, path in calibration_attempts:
        _calibration_artifact_check(
            path,
            label=f"collector calibration attempt {attempt:04d}",
            expected_environment=expected_calibration_environment,
            repository_root=repository_root,
            issues=issues,
        )
    if capacity_attempts and calibration_attempts and isinstance(medallion, Mapping):
        latest_capacity = _load_json_object(
            capacity_attempts[-1][1], label="latest capacity gate", issues=issues
        )
        latest_calibration = _load_json_object(
            calibration_attempts[-1][1], label="latest collector calibration", issues=issues
        )
        control_timestamps = (
            _parse_utc_timestamp(
                latest_capacity.get("created_at") if latest_capacity is not None else None
            ),
            _parse_utc_timestamp(
                latest_calibration.get("created_at") if latest_calibration is not None else None
            ),
            _parse_utc_timestamp(medallion.get("created_at")),
        )
        raw_timestamps = [_parse_utc_timestamp(record.get("timestamp")) for record in records]
        if (
            any(timestamp is None for timestamp in control_timestamps)
            or any(timestamp is None for timestamp in raw_timestamps)
            or max(timestamp for timestamp in control_timestamps if timestamp is not None)
            >= min(timestamp for timestamp in raw_timestamps if timestamp is not None)
        ):
            issues.append(
                "control timestamps do not prove capacity, calibration, and Medallion admission "
                "before every campaign run"
            )
    return finish(actual)


def _dataset_validation_check(
    manifest: Mapping[str, Any],
    repository_root: Path,
    current_git_commit: str | None,
    cache: dict[tuple[Path, Path, str, str], str | None],
) -> list[str]:
    """Validate one manifest's content-bound dataset attestation, memoizing its full rehash."""

    issues: list[str] = []
    raw_validation = manifest.get("dataset_validation")
    required_fields = {
        "mode",
        "attestation_path",
        "attestation_file_sha256",
        "attestation_payload_sha256",
        "content_identity_sha256",
        "validator_git_commit",
    }
    if not isinstance(raw_validation, Mapping):
        return ["experiment manifest dataset_validation must be an object"]
    missing = sorted(required_fields - set(raw_validation))
    unexpected = sorted(set(raw_validation) - required_fields)
    if missing:
        issues.append("experiment manifest dataset_validation is missing: " + ", ".join(missing))
    if unexpected:
        issues.append(
            "experiment manifest dataset_validation has unexpected fields: " + ", ".join(unexpected)
        )
    if raw_validation.get("mode") != "content-bound-attestation-v1":
        issues.append(
            "experiment manifest dataset_validation.mode must equal 'content-bound-attestation-v1'"
        )

    hashes: dict[str, str] = {}
    for field in (
        "attestation_file_sha256",
        "attestation_payload_sha256",
        "content_identity_sha256",
    ):
        value = raw_validation.get(field)
        if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
            issues.append(f"experiment manifest dataset_validation.{field} is invalid")
        else:
            hashes[field] = value
    validator_git_commit = raw_validation.get("validator_git_commit")
    if (
        not isinstance(validator_git_commit, str)
        or _GIT_COMMIT_PATTERN.fullmatch(validator_git_commit) is None
    ):
        issues.append("experiment manifest dataset_validation.validator_git_commit is invalid")
        validator_git_commit = None
    elif current_git_commit != validator_git_commit:
        issues.append("dataset validation validator Git commit differs from current clean HEAD")

    try:
        attestation_path = _repo_relative_path(
            repository_root,
            raw_validation.get("attestation_path"),
            label="dataset validation attestation_path",
        )
    except ConfigurationError as error:
        issues.append(str(error))
        return issues
    if not attestation_path.is_file():
        issues.append("dataset validation attestation file is missing")
        return issues
    if hashes.get("attestation_file_sha256") != sha256_file(attestation_path):
        issues.append("dataset validation attestation_file_sha256 does not bind the file")

    try:
        payload = json.loads(attestation_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        issues.append(f"dataset validation attestation is unreadable: {error}")
        return issues
    if not isinstance(payload, dict):
        issues.append("dataset validation attestation root must be an object")
        return issues
    payload_for_hash = dict(payload)
    payload_hash = payload_for_hash.pop("attestation_sha256", None)
    if (
        hashes.get("attestation_payload_sha256") != payload_hash
        or not isinstance(payload_hash, str)
        or payload_hash != sha256_value(payload_for_hash)
    ):
        issues.append("dataset validation attestation_payload_sha256 is invalid")

    dataset = payload.get("dataset")
    validator = payload.get("validator")
    if not isinstance(dataset, Mapping):
        issues.append("dataset validation attestation dataset payload is missing")
    elif dataset.get("content_identity_sha256") != hashes.get("content_identity_sha256"):
        issues.append("dataset validation content_identity_sha256 differs from the attestation")
    input_hashes = manifest.get("input_hashes")
    if (
        isinstance(dataset, Mapping)
        and isinstance(input_hashes, Mapping)
        and dataset.get("manifest_sha256") != input_hashes.get("dataset_manifest_sha256")
    ):
        issues.append("dataset validation attestation differs from the experiment dataset hash")
    if (
        isinstance(dataset, Mapping)
        and validator_git_commit is not None
        and isinstance(dataset.get("manifest_sha256"), str)
    ):
        expected_validation_base = (
            DATASET_VALIDATION_ROOT
            if repository_root.resolve() == ROOT.resolve()
            else repository_root / ".artifacts/dataset-validations"
        )
        expected_attestation_path = (
            expected_validation_base / validator_git_commit / f"{dataset['manifest_sha256']}.json"
        ).resolve()
        if attestation_path.resolve() != expected_attestation_path:
            issues.append("dataset validation attestation is outside its canonical producer key")
    if not isinstance(validator, Mapping):
        issues.append("dataset validation attestation validator payload is missing")
    elif validator.get("git_commit") != validator_git_commit:
        issues.append("dataset validation validator_git_commit differs from the attestation")

    manifest_declared = dataset.get("manifest_path") if isinstance(dataset, Mapping) else None
    expected_python = (
        validator.get("expected_python_version") if isinstance(validator, Mapping) else None
    )
    try:
        dataset_manifest_path = _repo_relative_path(
            repository_root,
            manifest_declared,
            label="attested dataset manifest_path",
        )
    except ConfigurationError as error:
        issues.append(str(error))
        return issues
    if not isinstance(expected_python, str) or not expected_python:
        issues.append("dataset validation attestation expected Python version is invalid")
    if not dataset_manifest_path.is_file():
        issues.append("attested dataset manifest file is missing")

    if issues or validator_git_commit is None or not isinstance(expected_python, str):
        return issues
    cache_key = (
        dataset_manifest_path,
        attestation_path,
        expected_python,
        validator_git_commit,
    )
    if cache_key not in cache:
        try:
            verify_attestation(
                repository_root,
                dataset_manifest_path,
                attestation_path,
                expected_python_version=expected_python,
                expected_git_commit=validator_git_commit,
            )
        except (OSError, KeyError, TypeError, ValueError) as error:
            cache[cache_key] = str(error)
        else:
            cache[cache_key] = None
    verification_error = cache[cache_key]
    if verification_error is not None:
        issues.append(f"dataset validation attestation verification failed: {verification_error}")
    return issues


def _current_input_hashes(experiment: CoreExperiment, repository_root: Path) -> dict[str, str]:
    """Rebuild the exact campaign input identity from the current reviewed files."""

    root = repository_root.resolve()
    config_path = _repo_path(root, experiment.config)
    config = load_experiment(config_path, root / "benchmark/schemas")
    common_profile, comet_profile = runtime_profile_paths(config, root)
    workload = config["workload"]
    manifest = build_experiment_manifest(
        config,
        config_path=config_path,
        runtime_lock_path=root / "runtime-versions.lock",
        workload_sql_path=_repo_path(root, workload["sql_file"]),
        workload_manifest_path=_repo_path(root, workload["manifest_file"]),
        dataset_manifest_path=_repo_path(root, workload["dataset_manifest"]),
        uv_lock_path=root / "uv.lock",
        spark_defaults_path=common_profile,
        comet_profile_path=comet_profile,
    )
    return dict(manifest["input_hashes"])


def _repository_provenance_check(
    records: list[Mapping[str, Any]], repository_root: Path
) -> dict[str, Any]:
    issues: list[str] = []
    try:
        commit = clean_git_commit(repository_root)
    except RepositoryEvidenceError as error:
        commit = None
        issues.append(str(error))
    raw_commits = sorted(
        {
            str(provenance.get("git_commit"))
            for row in records
            if isinstance((provenance := row.get("provenance")), Mapping)
        }
    )
    if commit is not None and raw_commits != [commit]:
        issues.append("raw campaign git_commit does not equal the current clean Git HEAD")
    return {
        "passed": not issues,
        "current_git_commit": commit,
        "raw_git_commits": raw_commits,
        "issues": issues,
    }


def _campaign_record_check(
    experiment: CoreExperiment,
    records: list[Mapping[str, Any]],
    repository_root: Path,
) -> dict[str, Any]:
    """Validate the four admission gates plus the twenty measurements as one campaign."""

    issues: list[str] = []
    identity = (experiment.workload, experiment.query_id, experiment.storage_profile)
    identities = {
        (str(row.get("workload")), str(row.get("query_id")), str(row.get("storage_profile")))
        for row in records
    }
    if identities != {identity}:
        issues.append(f"raw campaign identity must equal {identity!r}")
    try:
        raw_schema: object = json.loads(
            (repository_root / "benchmark/schemas/raw-result.schema.json").read_text(
                encoding="utf-8"
            )
        )
    except (OSError, json.JSONDecodeError) as error:
        issues.append(f"raw-result schema is unreadable: {error}")
    else:
        if not isinstance(raw_schema, dict):
            issues.append("raw-result schema root must be an object")
        else:
            validator = Draft202012Validator(raw_schema, format_checker=FormatChecker())
            for record in records:
                schema_errors = sorted(
                    validator.iter_errors(record), key=lambda item: list(item.path)
                )
                issues.extend(
                    f"raw record {record.get('run_id')!r} schema violation at "
                    f"{'/'.join(map(str, error.path)) or '<root>'}: {error.message}"
                    for error in schema_errors
                )
    if len(records) != EXPECTED_CAMPAIGN_RUNS:
        issues.append(f"raw campaign record count must equal {EXPECTED_CAMPAIGN_RUNS}")

    run_ids = [str(row.get("run_id", "")) for row in records]
    if any(not run_id for run_id in run_ids) or len(run_ids) != len(set(run_ids)):
        issues.append("raw campaign run IDs must be present and unique")
    timestamps = [_parse_utc_timestamp(row.get("timestamp")) for row in records]
    if any(timestamp is None for timestamp in timestamps) or len(set(timestamps)) != len(
        timestamps
    ):
        issues.append("raw campaign timestamps must be valid UTC values and strictly unique")
    phases = Counter(str(row.get("phase")) for row in records)
    expected_phases = Counter(
        {"correctness": 2, "plan_capture": 2, "measurement": EXPECTED_MEASUREMENT_RECORDS}
    )
    if phases != expected_phases:
        issues.append(f"raw campaign phases must equal {dict(expected_phases)!r}")
    if any(row.get("status") != "succeeded" for row in records):
        issues.append("all raw campaign records must have status='succeeded'")

    by_run = {str(row.get("run_id")): row for row in records}
    expected_gates = {
        "correctness-spark_baseline": ("correctness", "spark_baseline"),
        "correctness-comet_accelerated": ("correctness", "comet_accelerated"),
        "plan-spark_baseline": ("plan_capture", "spark_baseline"),
        "plan-comet_accelerated": ("plan_capture", "comet_accelerated"),
    }
    for run_id, (phase, engine) in expected_gates.items():
        gate_record = by_run.get(run_id)
        if gate_record is None:
            issues.append(f"required gate record is missing: {run_id}")
        elif gate_record.get("phase") != phase or gate_record.get("engine") != engine:
            issues.append(f"gate record identity is invalid: {run_id}")

    baseline_correctness = by_run.get("correctness-spark_baseline")
    comet_correctness = by_run.get("correctness-comet_accelerated")
    if baseline_correctness is not None and comet_correctness is not None:
        baseline_value = baseline_correctness.get("correctness")
        comet_value = comet_correctness.get("correctness")
        fields = ("schema_sha256", "row_count", "canonical_result_sha256")
        if not isinstance(baseline_value, Mapping) or not isinstance(comet_value, Mapping):
            issues.append("correctness gate payloads must be objects")
        elif any(baseline_value.get(field) != comet_value.get(field) for field in fields):
            issues.append("Spark/Comet correctness gate payloads must match")
        baseline_provenance = baseline_correctness.get("provenance")
        comet_provenance = comet_correctness.get("provenance")
        if (
            not isinstance(baseline_provenance, Mapping)
            or not isinstance(comet_provenance, Mapping)
            or baseline_provenance.get("iceberg_snapshot_ids")
            != comet_provenance.get("iceberg_snapshot_ids")
        ):
            issues.append("Spark/Comet correctness gates must use identical Iceberg snapshots")

    baseline_plan = by_run.get("plan-spark_baseline")
    comet_plan = by_run.get("plan-comet_accelerated")
    if baseline_plan is not None and comet_plan is not None:
        baseline_analysis = baseline_plan.get("plan_analysis")
        comet_analysis = comet_plan.get("plan_analysis")
        if not isinstance(baseline_analysis, Mapping) or not isinstance(comet_analysis, Mapping):
            issues.append("plan gate payloads must be objects")
        else:
            if (
                baseline_analysis.get("status") != "complete"
                or comet_analysis.get("status") != "complete"
            ):
                issues.append("both plan gates must have complete analysis")
            if baseline_analysis.get("comet_native_operators") != 0:
                issues.append("baseline plan gate must contain zero Comet native operators")
            comet_native = comet_analysis.get("comet_native_operators")
            if not _is_integer(comet_native) or comet_native < 1:
                issues.append("Comet plan gate must contain at least one native operator")

    provenance_fields = (
        "git_commit",
        "container_image_digest",
        "dataset_manifest_sha256",
        "sql_sha256",
        "iceberg_snapshot_ids",
    )
    provenances = [row.get("provenance") for row in records]
    if not all(isinstance(value, Mapping) for value in provenances):
        issues.append("every raw campaign record must contain provenance")
    else:
        typed_provenances = [value for value in provenances if isinstance(value, Mapping)]
        for field in provenance_fields:
            if any(field not in value for value in typed_provenances):
                issues.append(f"raw campaign provenance is missing {field}")
            elif len({sha256_value(value[field]) for value in typed_provenances}) != 1:
                issues.append(f"raw campaign provenance disagrees on {field}")
        for engine in sorted(_ENGINES):
            engine_values = [
                value.get("spark_conf_sha256")
                for row, value in zip(records, typed_provenances, strict=True)
                if row.get("engine") == engine
            ]
            if not engine_values or any(value is None for value in engine_values):
                issues.append(f"{engine} raw records are missing spark_conf_sha256")
            elif len(set(engine_values)) != 1:
                issues.append(f"{engine} raw records disagree on spark_conf_sha256")

    resources = [row.get("resources") for row in records]
    if not all(isinstance(value, Mapping) for value in resources):
        issues.append("every raw campaign record must contain a resource identity")
    elif len({sha256_value(value) for value in resources}) != 1:
        issues.append("raw campaign records disagree on host/resource identity")

    try:
        runtime_lock = validate_runtime_lock(
            repository_root / "runtime-versions.lock",
            repository_root / "benchmark/schemas/runtime-lock.schema.json",
        )
        components = {item["name"]: item for item in runtime_lock["components"]}
    except (ConfigurationError, KeyError, OSError, TypeError, ValueError) as error:
        issues.append(f"current runtime lock is invalid: {error}")
    else:
        common_runtime = {
            "spark_version": components["apache-spark"]["version"],
            "scala_version": components["scala"]["version"],
            "java_version": str(components["java"]["version"]).partition("+")[0],
            "iceberg_version": components["apache-iceberg-runtime"]["version"],
        }
        for record in records:
            expected_runtime = {
                **common_runtime,
                "comet_version": (
                    components["datafusion-comet"]["version"]
                    if record.get("engine") == "comet_accelerated"
                    else None
                ),
            }
            if record.get("runtime") != expected_runtime:
                issues.append(
                    f"raw runtime differs from the current lock: {record.get('run_id')!r}"
                )

    return {
        "experiment_id": experiment.experiment_id,
        "passed": not issues,
        "observed_records": len(records),
        "raw_records_sha256": raw_records_sha256(records),
        "issues": issues,
    }


def _verification_check(
    experiment: CoreExperiment,
    campaign_root: Path,
    records: list[Mapping[str, Any]],
    repository_root: Path,
    current_git_commit: str | None,
    attestation_cache: dict[tuple[Path, Path, str, str], str | None],
    control_cache: dict[str, tuple[dict[str, object] | None, tuple[str, ...]]],
) -> dict[str, Any]:
    campaign_dir = campaign_root / experiment.experiment_id
    issues: list[str] = []
    result: dict[str, Any] = {
        "experiment_id": experiment.experiment_id,
        "passed": False,
        "selected_attempt": None,
        "selected_file": None,
        "issues": issues,
    }
    try:
        selected = _latest_verification(campaign_dir)
    except ValueError as error:
        issues.append(str(error))
        return result
    if selected is None:
        issues.append("campaign verification is missing")
        return result

    attempt, path = selected
    result["selected_attempt"] = attempt
    result["selected_file"] = f"{experiment.experiment_id}/{path.name}"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        issues.append(f"latest campaign verification is unreadable: {error}")
        return result
    if not isinstance(value, dict):
        issues.append("latest campaign verification root is not an object")
        return result

    schema_version = value.get("schema_version")
    if not _is_integer(schema_version) or schema_version != 1:
        issues.append("schema_version must equal 1")
    if value.get("status") != "passed":
        issues.append("status must equal 'passed'")
    report = value.get("report")
    if not isinstance(report, dict):
        issues.append("report must be an object")
        return result

    if report.get("experiment_id") != experiment.experiment_id:
        issues.append(f"report.experiment_id must equal {experiment.experiment_id!r}")
    counters = {
        "planned": EXPECTED_CAMPAIGN_RUNS,
        "succeeded": EXPECTED_CAMPAIGN_RUNS,
        "failed": 0,
    }
    for field, expected in counters.items():
        observed = report.get(field)
        if not _is_integer(observed) or observed != expected:
            issues.append(f"report.{field} must equal {expected!r}")
    if report.get("complete") is not True:
        issues.append("report.complete must equal True")
    executed = report.get("executed")
    resumed = report.get("resumed")
    if not _is_integer(executed) or executed < 0:
        issues.append("report.executed must be a non-negative integer")
    if not _is_integer(resumed) or resumed < 0:
        issues.append("report.resumed must be a non-negative integer")
    if (
        _is_integer(executed)
        and _is_integer(resumed)
        and executed + resumed != EXPECTED_CAMPAIGN_RUNS
    ):
        issues.append(f"report.executed + report.resumed must equal {EXPECTED_CAMPAIGN_RUNS}")
    raw_record_count = report.get("raw_record_count")
    if not _is_integer(raw_record_count) or raw_record_count != EXPECTED_CAMPAIGN_RUNS:
        issues.append(f"report.raw_record_count must equal {EXPECTED_CAMPAIGN_RUNS}")
    observed_raw_hash = report.get("raw_records_sha256")
    expected_raw_hash = raw_records_sha256(records)
    if observed_raw_hash != expected_raw_hash:
        issues.append("report.raw_records_sha256 does not bind the loaded raw campaign")

    try:
        artifacts = artifact_evidence(records, repository_root)
    except (ArtifactEvidenceError, OSError) as error:
        issues.append(f"campaign artifact evidence is incomplete: {error}")
    else:
        result["artifact_evidence"] = artifacts
        artifact_file_count = report.get("artifact_file_count")
        if not _is_integer(artifact_file_count) or artifact_file_count != artifacts["file_count"]:
            issues.append(f"report.artifact_file_count must equal {artifacts['file_count']!r}")
        if report.get("artifact_files_sha256") != artifacts["sha256"]:
            issues.append("report.artifact_files_sha256 does not bind campaign artifacts")

    control_artifacts, control_issues = _control_artifact_check(
        report,
        experiment,
        records,
        repository_root,
        control_cache,
    )
    issues.extend(control_issues)
    if control_artifacts is not None:
        result["control_artifact_evidence"] = control_artifacts

    manifest_path = campaign_dir / "experiment-manifest.json"
    result["experiment_manifest"] = f"{experiment.experiment_id}/{manifest_path.name}"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        issues.append(f"experiment manifest is unreadable: {error}")
    else:
        if not isinstance(manifest, dict):
            issues.append("experiment manifest root is not an object")
        else:
            try:
                manifest_schema: object = json.loads(
                    (
                        repository_root / "benchmark/schemas/experiment-manifest.schema.json"
                    ).read_text(encoding="utf-8")
                )
            except (OSError, json.JSONDecodeError) as error:
                issues.append(f"experiment manifest schema is unreadable: {error}")
            else:
                if not isinstance(manifest_schema, dict):
                    issues.append("experiment manifest schema root is not an object")
                else:
                    manifest_errors = sorted(
                        Draft202012Validator(
                            manifest_schema,
                            format_checker=FormatChecker(),
                        ).iter_errors(manifest),
                        key=lambda item: list(item.path),
                    )
                    issues.extend(
                        "experiment manifest schema violation at "
                        f"{'/'.join(map(str, error.path)) or '<root>'}: {error.message}"
                        for error in manifest_errors
                    )
            manifest_for_hash = dict(manifest)
            declared_manifest_hash = manifest_for_hash.pop("manifest_sha256", None)
            if not isinstance(
                declared_manifest_hash, str
            ) or declared_manifest_hash != sha256_value(manifest_for_hash):
                issues.append("experiment manifest self-hash is invalid")
            if report.get("experiment_manifest_sha256") != declared_manifest_hash:
                issues.append(
                    "report.experiment_manifest_sha256 does not bind the experiment manifest"
                )
            if manifest.get("experiment_id") != experiment.experiment_id:
                issues.append("experiment manifest ID differs from the reviewed core config")
            try:
                current_config = load_experiment(
                    repository_root / experiment.config,
                    repository_root / "benchmark/schemas",
                )
            except (ConfigurationError, OSError, TypeError, ValueError) as error:
                issues.append(f"current experiment config is unreadable: {error}")
                current_config = None
            if current_config is not None:
                if manifest.get("resolved_config") != current_config:
                    issues.append(
                        "experiment manifest resolved_config differs from the current reviewed "
                        "config"
                    )
                engine_entries = current_config.get("matrix", {}).get("engines", [])
                current_spark_hashes = {
                    entry["name"]: sha256_value(
                        {
                            "common": current_config["spark"]["common_conf"],
                            "engine": entry["spark_conf"],
                        }
                    )
                    for entry in engine_entries
                    if isinstance(entry, Mapping)
                    and entry.get("name") in _ENGINES
                    and isinstance(entry.get("spark_conf"), Mapping)
                }
                for engine in sorted(_ENGINES):
                    observed_hashes = {
                        row["provenance"].get("spark_conf_sha256")
                        for row in records
                        if row.get("engine") == engine
                        and isinstance(row.get("provenance"), Mapping)
                    }
                    if observed_hashes != {current_spark_hashes.get(engine)}:
                        issues.append(
                            f"{engine} raw SparkConf hash differs from the current reviewed config"
                        )
            issues.extend(
                _dataset_validation_check(
                    manifest,
                    repository_root,
                    current_git_commit,
                    attestation_cache,
                )
            )
            dataset_validation = manifest.get("dataset_validation")
            declared_controls = report.get("control_artifacts")
            raw_control_targets = (
                declared_controls.get("targets") if isinstance(declared_controls, Mapping) else None
            )
            if (
                control_artifacts is not None
                and isinstance(dataset_validation, Mapping)
                and isinstance(raw_control_targets, list)
            ):
                control_paths = {
                    target.get("label"): target.get("path")
                    for target in raw_control_targets
                    if isinstance(target, Mapping)
                }
                if control_paths.get("dataset-validation-attestation") != (
                    dataset_validation.get("attestation_path")
                ):
                    issues.append(
                        "control evidence dataset attestation differs from the experiment manifest"
                    )
                try:
                    medallion_path = _repo_relative_path(
                        repository_root,
                        control_paths.get("medallion-audit"),
                        label="Medallion control path",
                    )
                except ConfigurationError as error:
                    issues.append(str(error))
                else:
                    medallion = _load_json_object(
                        medallion_path, label="Medallion audit", issues=issues
                    )
                    if medallion is not None and medallion.get(
                        "dataset_validation_attestation_sha256"
                    ) != dataset_validation.get("attestation_file_sha256"):
                        issues.append(
                            "Medallion audit does not bind the experiment dataset attestation"
                        )
            input_hashes = manifest.get("input_hashes")
            if not isinstance(input_hashes, Mapping):
                issues.append("experiment manifest input_hashes is absent")
            else:
                try:
                    current_input_hashes = _current_input_hashes(experiment, repository_root)
                except (ConfigurationError, KeyError, OSError, TypeError, ValueError) as error:
                    issues.append(f"current campaign inputs are unreadable: {error}")
                else:
                    for field, current_hash in current_input_hashes.items():
                        if input_hashes.get(field) != current_hash:
                            issues.append(f"experiment manifest does not bind current {field}")
                    unexpected_fields = sorted(set(input_hashes) - set(current_input_hashes))
                    if unexpected_fields:
                        issues.append(
                            "experiment manifest contains unexpected input hashes: "
                            + ", ".join(unexpected_fields)
                        )
                raw_dataset_hashes = {
                    row["provenance"].get("dataset_manifest_sha256")
                    for row in records
                    if isinstance(row.get("provenance"), Mapping)
                }
                if raw_dataset_hashes != {input_hashes.get("dataset_manifest_sha256")}:
                    issues.append("experiment manifest dataset hash differs from raw records")
                raw_sql_hashes = {
                    row["provenance"].get("sql_sha256")
                    for row in records
                    if isinstance(row.get("provenance"), Mapping)
                }
                if raw_sql_hashes != {input_hashes.get("workload_sql_sha256")}:
                    issues.append("experiment manifest SQL hash differs from raw records")
            try:
                planned_runs = plan_campaign(manifest)
            except CampaignError as error:
                issues.append(f"experiment manifest cannot reproduce the campaign plan: {error}")
            else:
                planned = {
                    run.run_id: (run.phase, run.engine, run.pair_index) for run in planned_runs
                }
                observed = {
                    str(row.get("run_id")): (
                        row.get("phase"),
                        row.get("engine"),
                        row.get("pair_index"),
                    )
                    for row in records
                }
                if observed != planned:
                    issues.append("raw records do not match the experiment manifest plan")
                timestamps_by_run = {
                    str(row.get("run_id")): _parse_utc_timestamp(row.get("timestamp"))
                    for row in records
                }
                planned_order = [run.run_id for run in planned_runs]
                ordered_timestamps = [timestamps_by_run.get(run_id) for run_id in planned_order]
                if any(timestamp is None for timestamp in ordered_timestamps) or any(
                    current is not None and following is not None and current >= following
                    for current, following in pairwise(ordered_timestamps)
                ):
                    issues.append(
                        "raw timestamps do not prove correctness, plan, and measurement "
                        "execution in manifest order"
                    )
    result["passed"] = not issues
    return result


def _positive_latency(record: Mapping[str, Any]) -> bool:
    metrics = record.get("metrics")
    if not isinstance(metrics, Mapping):
        return False
    value = metrics.get("query_wall_time_ms")
    return (
        isinstance(value, int | float)
        and not isinstance(value, bool)
        and math.isfinite(float(value))
        and float(value) > 0
    )


def _measurement_check(
    experiment: CoreExperiment, records: list[Mapping[str, Any]]
) -> dict[str, Any]:
    issues: list[str] = []
    identity = (experiment.workload, experiment.query_id, experiment.storage_profile)
    identities = {
        (str(row.get("workload")), str(row.get("query_id")), str(row.get("storage_profile")))
        for row in records
    }
    if identities != {identity}:
        issues.append(f"measurement identity must equal {identity!r}")
    if len(records) != EXPECTED_MEASUREMENT_RECORDS:
        issues.append(f"measurement record count must equal {EXPECTED_MEASUREMENT_RECORDS}")

    run_ids = [str(row.get("run_id", "")) for row in records]
    if any(not run_id for run_id in run_ids) or len(run_ids) != len(set(run_ids)):
        issues.append("measurement run IDs must be present and unique")
    failed = [str(row.get("run_id", "")) for row in records if row.get("status") != "succeeded"]
    if failed:
        issues.append("all measurement records must have status='succeeded'")
    invalid_latency = [
        str(row.get("run_id", ""))
        for row in records
        if row.get("status") == "succeeded" and not _positive_latency(row)
    ]
    if invalid_latency:
        issues.append("all succeeded measurements must have positive finite wall time")
    incomplete_collectors = [
        str(row.get("run_id", ""))
        for row in records
        if not isinstance(row.get("metrics"), Mapping)
        or row["metrics"].get("collector_status") != "complete"
    ]
    if incomplete_collectors:
        issues.append("all measurements must have collector_status='complete'")
    incomplete_plans = [
        str(row.get("run_id", ""))
        for row in records
        if not isinstance(row.get("plan_analysis"), Mapping)
        or row["plan_analysis"].get("status") != "complete"
    ]
    if incomplete_plans:
        issues.append("all measurements must have plan_analysis.status='complete'")

    pair_engines: dict[int, list[str]] = defaultdict(list)
    invalid_pair_indexes = False
    for row in records:
        pair_index = row.get("pair_index")
        if not _is_integer(pair_index):
            invalid_pair_indexes = True
            continue
        pair_engines[pair_index].append(str(row.get("engine")))
    expected_pairs = set(range(1, EXPECTED_MEASUREMENT_PAIRS + 1))
    if invalid_pair_indexes or set(pair_engines) != expected_pairs:
        issues.append(f"pair indexes must be exactly 1..{EXPECTED_MEASUREMENT_PAIRS}")
    malformed_pairs = [
        pair_index
        for pair_index, engines in sorted(pair_engines.items())
        if Counter(engines) != Counter(_ENGINES)
    ]
    if malformed_pairs:
        issues.append("each pair must contain exactly one baseline and one Comet record")

    try:
        summary = summarize_records([dict(row) for row in records])
    except (KeyError, TypeError, ValueError) as error:
        issues.append(f"schema-v1 summary could not be built: {error}")
    else:
        summary_counts = {
            "n_total": EXPECTED_MEASUREMENT_RECORDS,
            "n_succeeded": EXPECTED_MEASUREMENT_RECORDS,
            "n_failed": 0,
        }
        for field, expected in summary_counts.items():
            observed = summary.get(field)
            if not _is_integer(observed) or observed != expected:
                issues.append(f"summary.{field} must equal {expected}")
        engines = summary.get("engines")
        if not isinstance(engines, Mapping) or set(engines) != _ENGINES:
            issues.append("summary.engines must contain exactly baseline and Comet")
        else:
            for engine in sorted(_ENGINES):
                engine_summary = engines.get(engine)
                observed = engine_summary.get("n") if isinstance(engine_summary, Mapping) else None
                if not _is_integer(observed) or observed != EXPECTED_MEASUREMENT_PAIRS:
                    issues.append(
                        f"summary.engines.{engine}.n must equal {EXPECTED_MEASUREMENT_PAIRS}"
                    )
        paired_speedup = summary.get("paired_speedup")
        paired_n = paired_speedup.get("n") if isinstance(paired_speedup, Mapping) else None
        if not _is_integer(paired_n) or paired_n != EXPECTED_MEASUREMENT_PAIRS:
            issues.append(f"summary.paired_speedup.n must equal {EXPECTED_MEASUREMENT_PAIRS}")
        if summary.get("paired_failures") != []:
            issues.append("summary.paired_failures must be empty")

        resources = summary.get("paired_resource_savings")
        if not isinstance(resources, Mapping) or set(resources) != set(_RESOURCE_METRICS):
            issues.append("summary.paired_resource_savings has an unexpected metric set")
        else:
            for metric in _RESOURCE_METRICS:
                resource = resources.get(metric)
                absolute = resource.get("absolute_delta") if isinstance(resource, Mapping) else None
                absolute_n = absolute.get("n") if isinstance(absolute, Mapping) else None
                if not _is_integer(absolute_n) or absolute_n != EXPECTED_MEASUREMENT_PAIRS:
                    issues.append(
                        f"summary.paired_resource_savings.{metric}.absolute_delta.n "
                        f"must equal {EXPECTED_MEASUREMENT_PAIRS}"
                    )
                excluded = (
                    resource.get("excluded_pair_ids") if isinstance(resource, Mapping) else None
                )
                if excluded != []:
                    issues.append(
                        f"summary.paired_resource_savings.{metric}.excluded_pair_ids must be empty"
                    )

    return {
        "experiment_id": experiment.experiment_id,
        "passed": not issues,
        "observed_measurement_records": len(records),
        "observed_pair_indexes": sorted(pair_engines),
        "issues": issues,
    }


def assess_report_publishability(
    records: Iterable[Mapping[str, Any]],
    campaign_root: Path,
    *,
    repository_root: Path = ROOT,
) -> dict[str, Any]:
    """Evaluate the complete core-suite policy and return deterministic evidence."""

    all_rows = list(records)
    repository_provenance = _repository_provenance_check(all_rows, repository_root)
    rows = [row for row in all_rows if row.get("phase") == "measurement"]
    experiments = core_experiments(repository_root)
    expected_ids = {item.experiment_id for item in experiments}
    observed_ids = {str(row.get("experiment_id")) for row in all_rows}
    missing = sorted(expected_ids - observed_ids)
    unexpected = sorted(observed_ids - expected_ids)
    exact_core_set = not missing and not unexpected and len(experiments) == EXPECTED_CORE_CAMPAIGNS

    by_experiment: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        by_experiment[str(row.get("experiment_id"))].append(row)
    all_by_experiment: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in all_rows:
        all_by_experiment[str(row.get("experiment_id"))].append(row)
    measurement_checks = [
        _measurement_check(experiment, by_experiment[experiment.experiment_id])
        for experiment in experiments
    ]
    campaign_record_checks = [
        _campaign_record_check(
            experiment,
            all_by_experiment[experiment.experiment_id],
            repository_root,
        )
        for experiment in experiments
    ]
    attestation_cache: dict[tuple[Path, Path, str, str], str | None] = {}
    control_cache: dict[str, tuple[dict[str, object] | None, tuple[str, ...]]] = {}
    verification_checks = [
        _verification_check(
            experiment,
            campaign_root,
            all_by_experiment[experiment.experiment_id],
            repository_root,
            repository_provenance["current_git_commit"],
            attestation_cache,
            control_cache,
        )
        for experiment in experiments
    ]
    publishable = (
        exact_core_set
        and repository_provenance["passed"]
        and all(check["passed"] for check in measurement_checks)
        and all(check["passed"] for check in campaign_record_checks)
        and all(check["passed"] for check in verification_checks)
    )
    issues: list[str] = []
    if missing:
        issues.append("missing core experiments: " + ", ".join(missing))
    if unexpected:
        issues.append("unexpected raw experiments: " + ", ".join(unexpected))
    issues.extend(f"repository provenance: {issue}" for issue in repository_provenance["issues"])
    for category, checks in (
        ("measurement", measurement_checks),
        ("raw campaign", campaign_record_checks),
        ("verification", verification_checks),
    ):
        for check in checks:
            for issue in check["issues"]:
                issues.append(f"{check['experiment_id']} {category}: {issue}")

    return {
        "schema_version": 1,
        "status": "passed" if publishable else "failed",
        "publishable": publishable,
        "policy": {
            "core_configs": list(CORE_CONFIGS),
            "core_experiments": [asdict(item) for item in experiments],
            "expected_campaigns": EXPECTED_CORE_CAMPAIGNS,
            "expected_campaign_runs_per_experiment": EXPECTED_CAMPAIGN_RUNS,
            "expected_measurement_pairs_per_experiment": EXPECTED_MEASUREMENT_PAIRS,
            "expected_measurement_records_per_experiment": EXPECTED_MEASUREMENT_RECORDS,
        },
        "checks": {
            "exact_core_experiment_set": {
                "passed": exact_core_set,
                "expected": sorted(expected_ids),
                "observed": sorted(observed_ids),
                "missing": missing,
                "unexpected": unexpected,
            },
            "repository_provenance": repository_provenance,
            "measurements": measurement_checks,
            "campaign_records": campaign_record_checks,
            "campaign_verifications": verification_checks,
        },
        "issues": issues,
    }
