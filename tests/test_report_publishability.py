from __future__ import annotations

import copy
import hashlib
import json
import os
import platform
import shutil
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast
from uuid import uuid4

import pyarrow as pa  # type: ignore[import-untyped]
import pytest

import analysis.report_publishability as publishability
import benchmark.runner.evidence as evidence_module
from analysis.report_publishability import (
    EXPECTED_CAMPAIGN_RUNS,
    EXPECTED_CORE_CAMPAIGNS,
    assess_report_publishability,
    core_experiments,
)
from benchmark.collectors.resources import (
    DEFAULT_OVERHEAD_LIMIT_PERCENT,
    DEFAULT_SAMPLE_INTERVAL_SECONDS,
    CollectorStatus,
    ResourceSample,
    aggregate_samples,
)
from benchmark.parsers.eventlog import parse_event_log
from benchmark.parsers.plan import analyze_plan
from benchmark.runner.campaign import CampaignRun, plan_campaign
from benchmark.runner.canonical import sha256_file, sha256_value
from benchmark.runner.capacity import (
    CapacitySnapshot,
    CgroupSnapshot,
    FilesystemSnapshot,
    evaluate_capacity_gate,
)
from benchmark.runner.config import (
    build_experiment_manifest,
    load_document,
    load_experiment,
    runtime_profile_paths,
)
from benchmark.runner.dataset_attestation import VerifiedDataset
from benchmark.runner.evidence import (
    RepositoryEvidenceError,
    artifact_evidence,
    control_artifact_evidence,
)
from benchmark.runner.record import RawRecordContext, build_raw_record
from benchmark.runner.runtime import validate_runtime_lock
from benchmark.runner.sql import schema_hash
from pipeline.benchmark.run_query import _table_identifier
from scripts.run_research_suite import CORE_CONFIGS

HASH = "a" * 64
TEST_COMMIT = "b" * 40
ROOT = Path(__file__).resolve().parents[1]
_TEST_EVIDENCE_ROOT: Path | None = None
_TEST_STATE: _FixtureState | None = None

_CONTROL_CREATED_AT = "2026-08-30T00:00:00Z"
_MEDALLION_CREATED_AT = "2026-08-30T00:00:01Z"
_RAW_STARTED_AT_HOUR = 1
_IMAGE_DIGEST = f"sha256:{HASH}"
_CPU_MODEL = "test-cpu"
_ARTIFACT_NAMES = {
    "event_log": "event-log",
    "physical_plan": "final-plan.txt",
    "resource_samples": "worker-resource-samples.json",
    "stdout": "stdout.log",
    "stderr": "stderr.log",
}


@dataclass(slots=True)
class _FixtureState:
    evidence_root: Path
    controls: dict[str, dict[str, Path]]
    manifests: dict[str, dict[str, object]]
    medallions: dict[str, Path]
    attestations: dict[str, Path]
    records: list[dict[str, object]]


@dataclass(frozen=True, slots=True)
class _TreeSnapshot:
    directories: frozenset[Path]
    files: dict[Path, bytes]
    file_mtime_ns: dict[Path, int]
    file_sha256: dict[Path, str]


@dataclass(frozen=True, slots=True)
class _SharedEvidence:
    root: Path
    state: _FixtureState
    snapshot: _TreeSnapshot


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def _snapshot_tree(root: Path) -> _TreeSnapshot:
    if not root.is_dir():
        raise AssertionError(f"evidence root is not a directory: {root}")

    directories: set[Path] = set()
    files: dict[Path, bytes] = {}
    file_mtime_ns: dict[Path, int] = {}
    file_sha256: dict[Path, str] = {}
    for path in root.rglob("*"):
        relative = path.relative_to(root)
        if path.is_symlink():
            raise AssertionError(f"evidence tree contains a symbolic link: {relative}")
        if path.is_dir():
            directories.add(relative)
        elif path.is_file():
            payload = path.read_bytes()
            files[relative] = payload
            file_mtime_ns[relative] = path.stat().st_mtime_ns
            file_sha256[relative] = hashlib.sha256(payload).hexdigest()
        else:
            raise AssertionError(f"evidence tree contains an unsupported path: {relative}")
    return _TreeSnapshot(
        directories=frozenset(directories),
        files=files,
        file_mtime_ns=file_mtime_ns,
        file_sha256=file_sha256,
    )


def _restore_tree(root: Path, snapshot: _TreeSnapshot) -> None:
    if root.is_symlink() or root.is_file():
        root.unlink()
    root.mkdir(parents=True, exist_ok=True)

    current_paths = sorted(
        root.rglob("*"),
        key=lambda path: len(path.relative_to(root).parts),
        reverse=True,
    )
    for path in current_paths:
        relative = path.relative_to(root)
        if path.is_symlink():
            path.unlink()
            continue
        if path.is_file():
            expected = snapshot.files.get(relative)
            if expected is None:
                path.unlink()
            elif path.read_bytes() != expected:
                path.write_bytes(expected)
            continue
        if path.is_dir():
            if relative not in snapshot.directories:
                path.rmdir()
            continue
        raise AssertionError(f"evidence tree contains an unsupported path: {relative}")

    for relative in sorted(snapshot.directories, key=lambda value: len(value.parts)):
        (root / relative).mkdir(parents=True, exist_ok=True)
    for relative, expected in snapshot.files.items():
        path = root / relative
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(expected)
        mtime_ns = snapshot.file_mtime_ns[relative]
        os.utime(path, ns=(mtime_ns, mtime_ns))


def _cached_snapshot_sha256(path: Path, root: Path, snapshot: _TreeSnapshot) -> str:
    """Reuse fixture hashes only while size and restored mtime still match the snapshot."""

    try:
        relative = path.resolve(strict=False).relative_to(root.resolve())
    except ValueError:
        return sha256_file(path)
    expected = snapshot.files.get(relative)
    if expected is not None and path.is_file() and not path.is_symlink():
        stat = path.stat()
        if stat.st_size == len(expected) and stat.st_mtime_ns == snapshot.file_mtime_ns[relative]:
            return snapshot.file_sha256[relative]
    return sha256_file(path)


def _repo_relative(path: Path) -> str:
    return path.resolve().relative_to(ROOT.resolve()).as_posix()


def _self_hashed(value: dict[str, object], field: str = "artifact_sha256") -> dict[str, object]:
    value[field] = sha256_value(value)
    return value


def _runtime_components() -> dict[str, dict[str, object]]:
    runtime_lock = validate_runtime_lock(
        ROOT / "runtime-versions.lock",
        ROOT / "benchmark/schemas/runtime-lock.schema.json",
    )
    return {str(item["name"]): item for item in runtime_lock["components"]}


def _attested_dataset(
    config: dict[str, object],
    attestation_root: Path,
    components: dict[str, dict[str, object]],
) -> VerifiedDataset:
    workload = config["workload"]
    assert isinstance(workload, dict)
    manifest_path = ROOT / str(workload["dataset_manifest"])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert isinstance(manifest, dict)
    tables = manifest["tables"]
    assert isinstance(tables, dict)
    inventory = sorted(
        (
            {
                "table": str(table_name),
                "path": str(file_value["path"]),
                "row_count": int(file_value["row_count"]),
                "size_bytes": int(file_value["size_bytes"]),
                "sha256": str(file_value["sha256"]),
            }
            for table_name, table_value in tables.items()
            if isinstance(table_value, dict)
            for file_value in table_value["files"]
            if isinstance(file_value, dict)
        ),
        key=lambda item: (item["table"], item["path"]),
    )
    suite: Literal["ecommerce", "tpch"] = "tpch" if workload["suite"] == "tpch" else "ecommerce"
    manifest_sha256 = sha256_file(manifest_path)
    manifest_relative = _repo_relative(manifest_path)
    content_identity = sha256_value(
        {
            "suite": suite,
            "manifest_path": manifest_relative,
            "manifest_sha256": manifest_sha256,
            "runtime_parquet_inventory": inventory,
        }
    )
    expected_python = str(components["python"]["version"])
    value: dict[str, object] = {
        "artifact_class": "dataset-validation-attestation-v1",
        "schema_version": 1,
        "status": "passed",
        "dataset": {
            "suite": suite,
            "dataset_id": manifest["dataset_id"],
            "manifest_path": manifest_relative,
            "manifest_sha256": manifest_sha256,
            "content_identity_sha256": content_identity,
        },
        "validator": {
            "git_commit": TEST_COMMIT,
            "expected_python_version": expected_python,
            "python_implementation": platform.python_implementation(),
            "python_version": platform.python_version(),
            "pyarrow_version": pa.__version__,
            "runtime_lock_sha256": sha256_file(ROOT / "runtime-versions.lock"),
            "result_sha256": sha256_value(
                {"dataset_id": manifest["dataset_id"], "test_fixture": True}
            ),
        },
        "runtime_parquet_inventory": inventory,
    }
    value["attestation_sha256"] = sha256_value(value)
    attestation_path = attestation_root / TEST_COMMIT / f"{manifest_sha256}.json"
    if not attestation_path.exists():
        _write_json(attestation_path, value)
    return VerifiedDataset(
        suite=suite,
        dataset_id=str(manifest["dataset_id"]),
        manifest_path=manifest_path,
        manifest_sha256=manifest_sha256,
        content_identity_sha256=content_identity,
        attestation_path=attestation_path,
        attestation_file_sha256=sha256_file(attestation_path),
        attestation_sha256=str(value["attestation_sha256"]),
        git_commit=TEST_COMMIT,
        expected_python_version=expected_python,
        file_count=len(inventory),
        total_bytes=sum(cast(int, item["size_bytes"]) for item in inventory),
    )


def _storage_environment(storage_identity: str) -> dict[str, str]:
    return {
        "git_commit": TEST_COMMIT,
        "container_image_digest": _IMAGE_DIGEST,
        "storage_identity_sha256": storage_identity,
        "cpu_model": _CPU_MODEL,
    }


def _calibration_artifact(path: Path, storage_identity: str) -> None:
    paired_overhead = [0.5, 0.75, 1.0]
    value: dict[str, object] = {
        "schema_version": 1,
        "artifact_class": "resource-collector-calibration-v1",
        "status": "passed",
        "workload": {"id": "sha256-chain-v1", "work_units": 1},
        "collector": {
            "sample_interval_seconds": DEFAULT_SAMPLE_INTERVAL_SECONDS,
            "module_sha256": sha256_file(ROOT / "benchmark/collectors/resources.py"),
            "script_sha256": sha256_file(ROOT / "scripts/calibrate_resource_collector.py"),
            "observed_sources": ["cgroup_v2"],
            "observed_statuses": ["complete"],
            "minimum_samples_per_run": 2,
            "source_gate_passed": True,
            "status_gate_passed": True,
            "sampling_gate_passed": True,
        },
        "runtime": {
            "python_version": platform.python_version(),
            "platform": platform.platform(),
        },
        "calibration": {
            "pair_count": len(paired_overhead),
            "threshold_percent": DEFAULT_OVERHEAD_LIMIT_PERCENT,
            "median_baseline_ns": 100.0,
            "median_instrumented_ns": 101.0,
            "median_paired_overhead_percent": 0.75,
            "maximum_paired_overhead_percent": 1.0,
            "accepted": True,
            "paired_overhead_percent": paired_overhead,
        },
        "environment": _storage_environment(storage_identity),
        "created_at": _CONTROL_CREATED_AT,
    }
    _write_json(path, _self_hashed(value))


def _raw_runtime(engine: str, components: dict[str, dict[str, object]]) -> dict[str, str | None]:
    return {
        "spark_version": str(components["apache-spark"]["version"]),
        "scala_version": str(components["scala"]["version"]),
        "java_version": str(components["java"]["version"]).partition("+")[0],
        "comet_version": (
            str(components["datafusion-comet"]["version"])
            if engine == "comet_accelerated"
            else None
        ),
        "iceberg_version": str(components["apache-iceberg-runtime"]["version"]),
    }


def _medallion_artifact(
    path: Path,
    config: dict[str, object],
    verified: VerifiedDataset,
    components: dict[str, dict[str, object]],
) -> dict[str, object]:
    manifest = json.loads(verified.manifest_path.read_text(encoding="utf-8"))
    assert isinstance(manifest, dict)
    tables = manifest["tables"]
    assert isinstance(tables, dict)
    counts = {
        str(name): int(table["row_count"])
        for name, table in tables.items()
        if isinstance(table, dict)
    }
    tpch = config["workload"]["suite"] == "tpch"  # type: ignore[index]
    snapshot_names = (
        sorted(f"tpch.{name}" for name in counts)
        if tpch
        else sorted(
            {
                *(f"bronze.{name}" for name in counts),
                "silver.sales_enriched",
                "silver.events",
                "gold.daily_revenue",
                "gold.customer_ltv",
                "gold.product_ranking",
                "gold.category_growth",
            }
        )
    )
    snapshots = {
        name: {
            "snapshot_id": index,
            "manifest_list": f"s3://test/{name}/snap-{index}.avro",
        }
        for index, name in enumerate(snapshot_names, 1)
    }
    scala_version = str(components["scala"]["version"])
    iceberg_version = str(components["apache-iceberg-runtime"]["version"])
    comet_version = str(components["datafusion-comet"]["version"])
    aws_version = str(components["apache-iceberg-aws-bundle"]["version"])
    runtime = {
        **_raw_runtime("spark_baseline", components),
        "java_runtime_version": str(components["java"]["version"]),
        "python_version": str(components["python"]["version"]),
        "iceberg_full_version": f"Apache Iceberg {iceberg_version}",
        "machine": "test-machine",
        "runtime_lock_sha256": sha256_file(ROOT / "runtime-versions.lock"),
        "artifact_sha256": {
            name: str(components[name]["sha256_or_digest"]).removeprefix("sha256:")
            for name in (
                "scala",
                "datafusion-comet",
                "apache-iceberg-runtime",
                "apache-iceberg-aws-bundle",
            )
        },
        "class_resources": {
            "scala_properties": [f"file:/jars/scala-library-{scala_version}.jar"],
            "iceberg_build": [f"file:/jars/iceberg-spark-runtime-4.1_2.13-{iceberg_version}.jar"],
            "iceberg_s3_file_io": [
                f"file:/jars/iceberg-spark-runtime-4.1_2.13-{iceberg_version}.jar"
            ],
            "aws_s3_client": [f"file:/jars/iceberg-aws-bundle-{aws_version}.jar"],
            "comet_plugin": [f"file:/jars/comet-spark-spark4.1_2.13-{comet_version}.jar"],
            "hadoop_s3a": [],
        },
    }
    value: dict[str, object] = {
        "schema_version": 1,
        "status": "passed",
        "pipeline": "tpch-derived-iceberg-import-v1" if tpch else "ecommerce-medallion-v1",
        "runtime": runtime,
        "dataset_id": manifest["dataset_id"],
        "dataset_manifest_sha256": verified.manifest_sha256,
        "dataset_validation_attestation_sha256": verified.attestation_file_sha256,
        "benchmark_eligible": True,
        "quality": manifest["validation"] if tpch else {"invalid_rows": 0},
        "snapshots": snapshots,
        "created_at": _MEDALLION_CREATED_AT,
    }
    if tpch:
        workload = config["workload"]
        assert isinstance(workload, dict)
        value.update({"scale_factor": workload["scale_factor"], "table_counts": counts})
    else:
        value.update(
            {
                "bronze_counts": counts,
                "derived_counts": {
                    "silver.sales_enriched": counts.get("order_items", 0),
                    "silver.events": counts.get("events", 0),
                    "gold.daily_revenue": 365,
                    "gold.customer_ltv": counts.get("customers", 0),
                    "gold.product_ranking": counts.get("products", 0),
                    "gold.category_growth": 1,
                },
            }
        )
    _write_json(path, _self_hashed(value))
    return value


def _selected_snapshot_ids(config: dict[str, object], medallion: dict[str, object]) -> list[int]:
    workload = config["workload"]
    assert isinstance(workload, dict)
    workload_manifest = load_document(
        ROOT / str(workload["manifest_file"]),
        ROOT / "benchmark/schemas/workload-manifest.schema.json",
    )
    snapshots = medallion["snapshots"]
    assert isinstance(snapshots, dict)
    return sorted(
        {
            int(
                snapshots[
                    _table_identifier(binding["logical_table"], suite=str(workload["suite"]))[1]
                ]["snapshot_id"]
            )
            for binding in workload_manifest["relation_bindings"].values()
        }
    )


def _capacity_artifact(
    path: Path,
    config_path: Path,
    config: dict[str, object],
    verified: VerifiedDataset,
    storage_identity: str,
) -> None:
    dataset_manifest = json.loads(verified.manifest_path.read_text(encoding="utf-8"))
    assert isinstance(dataset_manifest, dict)
    observation = {
        "filesystem": {
            "path": "/test/lakehouse",
            "free_bytes": 10**15,
            "total_bytes": 2 * 10**15,
        },
        "cgroup": {
            "memory_limit_bytes": 8 * 1024**3,
            "cpu_limit_cores": 2.0,
            "swap_current_bytes": 0,
            "swap_delta_bytes": 0,
            "swap_peak_bytes": 0,
        },
    }
    snapshot = CapacitySnapshot(
        filesystem=FilesystemSnapshot(
            path="/test/lakehouse",
            free_bytes=10**15,
            total_bytes=2 * 10**15,
        ),
        cgroup=CgroupSnapshot(
            memory_limit_bytes=8 * 1024**3,
            cpu_limit_cores=2.0,
            swap_current_bytes=0,
            swap_delta_bytes=0,
            swap_peak_bytes=0,
        ),
    )
    result = evaluate_capacity_gate(config, dataset_manifest, snapshot).as_dict()
    value: dict[str, object] = {
        "schema_version": 1,
        "artifact_class": "research-capacity-gate-v1",
        "experiment_config_sha256": sha256_file(config_path),
        "dataset_manifest_sha256": verified.manifest_sha256,
        "environment": _storage_environment(storage_identity),
        "created_at": _CONTROL_CREATED_AT,
        "observation": observation,
        **result,
    }
    _write_json(path, _self_hashed(value))


def _event_log_artifact(
    path: Path,
    run: CampaignRun,
    execution_id: int,
    spark_version: str,
) -> None:
    stage_id = execution_id + 1_000
    events = [
        {"Event": "SparkListenerLogStart", "Spark Version": spark_version},
        {
            "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
            "executionId": execution_id,
            "rootExecutionId": execution_id,
            "jobGroupId": run.run_id,
            "description": "SELECT",
            "time": 1_000,
        },
        {
            "Event": "SparkListenerJobStart",
            "Job ID": execution_id,
            "Submission Time": 1_001,
            "Stage IDs": [stage_id],
            "Properties": {"spark.sql.execution.id": str(execution_id)},
        },
        {
            "Event": "SparkListenerStageSubmitted",
            "Stage Info": {"Stage ID": stage_id, "Stage Attempt ID": 0},
            "Properties": {"spark.sql.execution.id": str(execution_id)},
        },
        {
            "Event": "SparkListenerTaskEnd",
            "Stage ID": stage_id,
            "Stage Attempt ID": 0,
            "Task Type": "ResultTask",
            "Task End Reason": {"Reason": "Success"},
            "Task Info": {"Task ID": execution_id, "Attempt": 0},
            "Task Metrics": {
                "Executor CPU Time": 1_000_000_000,
                "Executor Run Time": 8,
                "JVM GC Time": 1,
                "Memory Bytes Spilled": 0,
                "Disk Bytes Spilled": 1_000_000,
                "Shuffle Read Metrics": {
                    "Remote Bytes Read": 1_000_000,
                    "Local Bytes Read": 0,
                },
                "Shuffle Write Metrics": {"Shuffle Bytes Written": 1_000_000},
                "Input Metrics": {"Bytes Read": 1_000_000},
                "Output Metrics": {"Bytes Written": 0},
            },
        },
        {
            "Event": "SparkListenerStageCompleted",
            "Stage Info": {
                "Stage ID": stage_id,
                "Stage Attempt ID": 0,
                "Completion Time": 1_008,
            },
        },
        {
            "Event": "SparkListenerJobEnd",
            "Job ID": execution_id,
            "Completion Time": 1_009,
            "Job Result": {"Result": "JobSucceeded"},
        },
        {
            "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd",
            "executionId": execution_id,
            "time": 1_009,
        },
    ]
    path.write_text(
        "".join(json.dumps(event, separators=(",", ":")) + "\n" for event in events),
        encoding="utf-8",
    )


def _resource_artifact(path: Path) -> dict[str, object]:
    samples = [
        ResourceSample(
            timestamp_ns=1_000_000_000,
            source="cgroup_v2",
            status=CollectorStatus.COMPLETE,
            cpu_usage_ns=0,
            memory_current_bytes=80 * 1024 * 1024,
            memory_peak_bytes=100 * 1024 * 1024,
            swap_current_bytes=0,
            io_read_bytes=0,
            io_write_bytes=0,
            cpu_limit_cores=2.0,
            process_count=1,
        ),
        ResourceSample(
            timestamp_ns=2_000_000_000,
            source="cgroup_v2",
            status=CollectorStatus.COMPLETE,
            cpu_usage_ns=1_000_000_000,
            memory_current_bytes=100 * 1024 * 1024,
            memory_peak_bytes=100 * 1024 * 1024,
            swap_current_bytes=0,
            io_read_bytes=1_000_000,
            io_write_bytes=1_000_000,
            cpu_limit_cores=2.0,
            process_count=1,
        ),
    ]
    summary = aggregate_samples(samples).as_dict()
    _write_json(
        path,
        {
            "schema_version": 1,
            "scope": "spark-worker-executor-container",
            "timed_out": False,
            "window_started": True,
            "window_completed": True,
            "aborted": False,
            "samples": [sample.as_dict() for sample in samples],
            "summary": summary,
        },
    )
    return summary


def _attempt_admission_artifact(
    path: Path,
    run: CampaignRun,
    attempt_index: int,
    provenance: dict[str, object],
    runtime: dict[str, str | None],
) -> None:
    value: dict[str, object] = {
        "schema_version": 1,
        "artifact_class": "research-run-attempt-admission-v1",
        "attempt_index": attempt_index,
        "run": {
            "experiment_id": run.experiment_id,
            "run_id": run.run_id,
            "phase": run.phase,
            "engine": run.engine,
            "pair_index": run.pair_index,
            "order_index": run.order_index,
            "warmup_runs": run.warmup_runs,
            "timeout_seconds": run.timeout_seconds,
        },
        "provenance": provenance,
        "runtime": runtime,
        "created_at": _CONTROL_CREATED_AT,
    }
    _write_json(path, _self_hashed(value))


def _record(
    *,
    experiment: publishability.CoreExperiment,
    run: CampaignRun,
    config: dict[str, object],
    manifest: dict[str, object],
    snapshots: list[int],
    attempt_dir: Path,
    sequence: int,
    components: dict[str, dict[str, object]],
) -> dict[str, object]:
    engine = run.engine
    matrix = cast(dict[str, object], config["matrix"])
    engines = cast(list[dict[str, object]], matrix["engines"])
    spark_entry = next(item for item in engines if item["name"] == engine)
    spark = cast(dict[str, object], config["spark"])
    input_hashes = cast(dict[str, str], manifest["input_hashes"])
    native = engine == "comet_accelerated"
    attempt_dir.mkdir(parents=True, exist_ok=True)
    plan_text = "CometIcebergNativeScan test_table\n" if native else "BatchScan test_table\n"
    (attempt_dir / "initial-plan.txt").write_text(plan_text, encoding="utf-8")
    (attempt_dir / "final-plan.txt").write_text(plan_text, encoding="utf-8")
    plan_analysis = analyze_plan(plan_text, comet_enabled=native)
    execution_id = sequence + 1
    _event_log_artifact(
        attempt_dir / "event-log",
        run,
        execution_id,
        str(components["apache-spark"]["version"]),
    )
    resource_summary = _resource_artifact(attempt_dir / "worker-resource-samples.json")
    _write_json(
        attempt_dir / "driver-resource-samples.json",
        {
            "schema_version": 1,
            "scope": "spark-client-driver-container",
            "samples": [],
            "summary": resource_summary,
        },
    )
    (attempt_dir / "stdout.log").write_text("", encoding="utf-8")
    (attempt_dir / "stderr.log").write_text("", encoding="utf-8")
    schema_json = json.dumps(
        {
            "type": "struct",
            "fields": [
                {
                    "name": "value",
                    "type": "long",
                    "nullable": False,
                    "metadata": {},
                }
            ],
        },
        separators=(",", ":"),
    )
    timestamp = f"2026-08-30T{_RAW_STARTED_AT_HOUR:02d}:00:{sequence:02d}Z"
    runtime = _raw_runtime(engine, components)
    spark_conf_sha256 = sha256_value(
        {"common": spark["common_conf"], "engine": spark_entry["spark_conf"]}
    )
    resources: dict[str, object] = {
        "cpu_model": _CPU_MODEL,
        "allocated_cores": 2,
        "cgroup_memory_limit_mib": 8192,
        "executor_heap_mib": 2048,
        "off_heap_mib": 1024,
    }
    provenance: dict[str, object] = {
        "git_commit": TEST_COMMIT,
        "container_image_digest": _IMAGE_DIGEST,
        "dataset_manifest_sha256": input_hashes["dataset_manifest_sha256"],
        "spark_conf_sha256": spark_conf_sha256,
        "sql_sha256": input_hashes["workload_sql_sha256"],
        "iceberg_snapshot_ids": snapshots,
        "resources": resources,
    }
    _attempt_admission_artifact(
        attempt_dir / "attempt-admission.json",
        run,
        int(attempt_dir.name.removeprefix("attempt-")),
        provenance,
        runtime,
    )
    application: dict[str, object] = {
        "schema_version": 1,
        "artifact_class": "benchmark-application-run-v1",
        "experiment_id": experiment.experiment_id,
        "run_id": run.run_id,
        "pair_index": run.pair_index,
        "phase": run.phase,
        "timestamp": timestamp,
        "status": "succeeded",
        "failure": None,
        "engine": engine,
        "application_id": f"application-{execution_id}",
        "runtime": runtime,
        "workload": experiment.workload,
        "query_id": experiment.query_id,
        "storage_profile": experiment.storage_profile,
        "sql_sha256": input_hashes["workload_sql_sha256"],
        "workload_manifest_sha256": input_hashes["workload_manifest_sha256"],
        "dataset_manifest_sha256": input_hashes["dataset_manifest_sha256"],
        "iceberg_snapshot_ids": snapshots,
        "query_wall_time_ms": 10.0,
        "driver_resources": resource_summary,
        "schema_json": schema_json,
        "schema_sha256": schema_hash(schema_json),
        "row_count": 1,
        "canonical_result_sha256": HASH,
        "plan_analysis": plan_analysis,
        "artifacts": {
            "initial_plan": "initial-plan.txt",
            "final_plan": "final-plan.txt",
            "driver_resource_samples": "driver-resource-samples.json",
        },
    }
    _write_json(attempt_dir / "application-result.json", application)
    artifacts = {
        field: _repo_relative(attempt_dir / name) for field, name in _ARTIFACT_NAMES.items()
    }
    context = RawRecordContext(
        git_commit=TEST_COMMIT,
        container_image_digest=_IMAGE_DIGEST,
        spark_conf_sha256=spark_conf_sha256,
        cpu_model=_CPU_MODEL,
        allocated_cores=2,
        cgroup_memory_limit_mib=8192,
        executor_heap_mib=2048,
        off_heap_mib=1024,
        event_log=artifacts["event_log"],
        physical_plan=artifacts["physical_plan"],
        resource_samples=artifacts["resource_samples"],
        stdout=artifacts["stdout"],
        stderr=artifacts["stderr"],
        executor_resources=resource_summary,
    )
    return cast(
        dict[str, object],
        build_raw_record(
            run,
            application,
            parse_event_log(attempt_dir / "event-log"),
            context,
        ),
    )


def _build_fixture_state(evidence_root: Path) -> _FixtureState:
    components = _runtime_components()
    control_root = evidence_root / "campaigns"
    attestation_root = evidence_root / "dataset-validations"
    shared_root = evidence_root / "research-shared"
    controls: dict[str, dict[str, Path]] = {}
    manifests: dict[str, dict[str, object]] = {}
    medallions: dict[str, Path] = {}
    attestations: dict[str, Path] = {}
    records: list[dict[str, object]] = []
    verified_by_manifest: dict[str, VerifiedDataset] = {}
    shared_by_storage: dict[str, tuple[Path, dict[str, object], str]] = {}

    for experiment in core_experiments():
        config_path = ROOT / experiment.config
        config = load_experiment(config_path, ROOT / "benchmark/schemas")
        workload = config["workload"]
        dataset_relative = str(workload["dataset_manifest"])
        verified = verified_by_manifest.get(dataset_relative)
        if verified is None:
            verified = _attested_dataset(config, attestation_root, components)
            verified_by_manifest[dataset_relative] = verified
            attestations[dataset_relative] = verified.attestation_path

        common_profile, comet_profile = runtime_profile_paths(config, ROOT)
        manifest = build_experiment_manifest(
            config,
            config_path=config_path,
            runtime_lock_path=ROOT / "runtime-versions.lock",
            workload_sql_path=ROOT / str(workload["sql_file"]),
            workload_manifest_path=ROOT / str(workload["manifest_file"]),
            dataset_manifest_path=verified.manifest_path,
            uv_lock_path=ROOT / "uv.lock",
            spark_defaults_path=common_profile,
            comet_profile_path=comet_profile,
            dataset_validation=verified,
        )
        manifests[experiment.experiment_id] = manifest

        storage_identity = sha256_value(
            {
                "storage_profile": experiment.storage_profile,
                "dataset_manifest_sha256": verified.manifest_sha256,
            }
        )
        shared = shared_by_storage.get(storage_identity)
        if shared is None:
            producer_root = (
                shared_root / TEST_COMMIT / _IMAGE_DIGEST.removeprefix("sha256:") / storage_identity
            )
            calibration_path = producer_root / "collector-calibration.json"
            _calibration_artifact(calibration_path, storage_identity)
            medallion_path = (
                producer_root
                / "datasets"
                / verified.manifest_sha256
                / verified.attestation_file_sha256
                / "medallion.json"
            )
            medallion = _medallion_artifact(
                medallion_path,
                config,
                verified,
                components,
            )
            shared = (calibration_path, medallion, storage_identity)
            shared_by_storage[storage_identity] = shared
        calibration_path, medallion, storage_identity = shared
        medallion_path = (
            calibration_path.parent
            / "datasets"
            / verified.manifest_sha256
            / verified.attestation_file_sha256
            / "medallion.json"
        )
        medallions[experiment.experiment_id] = medallion_path

        campaign_control = control_root / experiment.experiment_id
        capacity_path = campaign_control / "capacity-gate.json"
        _capacity_artifact(capacity_path, config_path, config, verified, storage_identity)
        run_root = campaign_control / "runs"
        failed_root = campaign_control / "failed-attempts"
        failed_root.mkdir(parents=True, exist_ok=True)
        snapshots = _selected_snapshot_ids(config, medallion)
        for sequence, run in enumerate(plan_campaign(manifest)):
            records.append(
                _record(
                    experiment=experiment,
                    run=run,
                    config=config,
                    manifest=manifest,
                    snapshots=snapshots,
                    attempt_dir=run_root / run.run_id / "attempt-0001",
                    sequence=sequence,
                    components=components,
                )
            )
        controls[experiment.experiment_id] = {
            "capacity-gate-0001": capacity_path,
            "collector-calibration-0001": calibration_path,
            "dataset-validation-attestation": verified.attestation_path,
            "failed-attempt-records": failed_root,
            "medallion-audit": medallion_path,
            "run-attempts": run_root,
        }
    return _FixtureState(
        evidence_root=evidence_root,
        controls=controls,
        manifests=manifests,
        medallions=medallions,
        attestations=attestations,
        records=records,
    )


def _fixture_state() -> _FixtureState:
    global _TEST_STATE
    if _TEST_STATE is None:
        if _TEST_EVIDENCE_ROOT is None:
            raise AssertionError("test evidence fixture is not active")
        _TEST_STATE = _build_fixture_state(_TEST_EVIDENCE_ROOT)
    return _TEST_STATE


def _control_targets(experiment_id: str) -> dict[str, Path]:
    return _fixture_state().controls[experiment_id]


def _current_hashes(experiment: publishability.CoreExperiment) -> dict[str, str]:
    value = _fixture_state().manifests[experiment.experiment_id]["input_hashes"]
    assert isinstance(value, dict)
    return copy.deepcopy(value)


@pytest.fixture(scope="module")
def _shared_evidence_tree() -> Iterator[_SharedEvidence]:
    global _TEST_EVIDENCE_ROOT
    global _TEST_STATE

    evidence_root = ROOT / ".artifacts/report-publishability-tests" / uuid4().hex
    _TEST_EVIDENCE_ROOT = evidence_root
    _TEST_STATE = _build_fixture_state(evidence_root)
    shared = _SharedEvidence(
        root=evidence_root,
        state=_TEST_STATE,
        snapshot=_snapshot_tree(evidence_root),
    )
    try:
        yield shared
    finally:
        shutil.rmtree(evidence_root, ignore_errors=True)
        _TEST_EVIDENCE_ROOT = None
        _TEST_STATE = None


@pytest.fixture(autouse=True)
def _stable_current_repository(
    monkeypatch: pytest.MonkeyPatch,
    _shared_evidence_tree: _SharedEvidence,
) -> Iterator[None]:
    global _TEST_EVIDENCE_ROOT
    global _TEST_STATE

    shared = _shared_evidence_tree
    evidence_root = shared.root
    _TEST_EVIDENCE_ROOT = evidence_root
    _TEST_STATE = shared.state
    monkeypatch.setattr(publishability, "CAMPAIGN_CONTROL_ROOT", evidence_root / "campaigns")
    monkeypatch.setattr(
        publishability,
        "DATASET_VALIDATION_ROOT",
        evidence_root / "dataset-validations",
    )
    monkeypatch.setattr(
        publishability,
        "RESEARCH_SHARED_ROOT",
        evidence_root / "research-shared",
    )
    monkeypatch.setattr(publishability, "clean_git_commit", lambda _root: TEST_COMMIT)

    def cached_sha256(path: Path) -> str:
        return _cached_snapshot_sha256(path, evidence_root, shared.snapshot)

    monkeypatch.setattr(publishability, "sha256_file", cached_sha256)
    monkeypatch.setattr(evidence_module, "sha256_file", cached_sha256)
    # Full content rehashing is covered by test_dataset_attestation.py. These policy tests keep
    # the complete attestation payload/path contract but avoid re-reading hundreds of MB per case.
    monkeypatch.setattr(publishability, "verify_attestation", lambda *_args, **_kwargs: None)
    try:
        yield
    finally:
        _restore_tree(evidence_root, shared.snapshot)


def _measurement_records() -> list[dict[str, object]]:
    return copy.deepcopy(_fixture_state().records)


def _experiment_manifest(experiment: publishability.CoreExperiment) -> dict[str, object]:
    return copy.deepcopy(_fixture_state().manifests[experiment.experiment_id])


def _verification_value(
    experiment_id: str,
    records: list[dict[str, object]] | None = None,
    manifest: dict[str, object] | None = None,
) -> dict[str, object]:
    source = _measurement_records() if records is None else records
    if manifest is None:
        experiment = next(
            item for item in core_experiments() if item.experiment_id == experiment_id
        )
        selected_manifest = _experiment_manifest(experiment)
    else:
        selected_manifest = manifest
    selected = sorted(
        (record for record in source if record["experiment_id"] == experiment_id),
        key=lambda record: str(record["run_id"]),
    )
    artifacts = artifact_evidence(selected, ROOT)
    control_targets = _control_targets(experiment_id)
    controls = control_artifact_evidence(control_targets, ROOT)
    execution_attempt_count = sum(
        1
        for run_dir in control_targets["run-attempts"].iterdir()
        if run_dir.is_dir()
        for attempt_dir in run_dir.iterdir()
        if attempt_dir.is_dir() and attempt_dir.name.startswith("attempt-")
    )
    failed_attempt_record_count = sum(
        1 for path in control_targets["failed-attempt-records"].rglob("*.json") if path.is_file()
    )
    return {
        "schema_version": 1,
        "status": "passed",
        "report": {
            "experiment_id": experiment_id,
            "planned": EXPECTED_CAMPAIGN_RUNS,
            "executed": EXPECTED_CAMPAIGN_RUNS,
            "resumed": 0,
            "succeeded": EXPECTED_CAMPAIGN_RUNS,
            "failed": 0,
            "complete": True,
            "raw_record_count": EXPECTED_CAMPAIGN_RUNS,
            "raw_records_sha256": sha256_value(selected),
            "artifact_file_count": artifacts["file_count"],
            "artifact_files_sha256": artifacts["sha256"],
            "control_artifacts": controls,
            "execution_attempt_count": execution_attempt_count,
            "failed_attempt_record_count": failed_attempt_record_count,
            "experiment_manifest_sha256": selected_manifest["manifest_sha256"],
        },
    }


def _write_verifications(
    campaign_root: Path,
    records: list[dict[str, object]] | None = None,
) -> None:
    source = _measurement_records() if records is None else records
    for experiment in core_experiments():
        directory = campaign_root / experiment.experiment_id
        directory.mkdir(parents=True)
        manifest = _experiment_manifest(experiment)
        (directory / "experiment-manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        value = _verification_value(experiment.experiment_id, source, manifest)
        (directory / "campaign-verification.json").write_text(json.dumps(value), encoding="utf-8")


def test_tree_snapshot_restore_repairs_content_and_path_type_collisions(tmp_path: Path) -> None:
    root = tmp_path / "evidence"
    changed = root / "nested" / "changed.json"
    missing = root / "nested" / "missing.json"
    file_replaced_by_directory = root / "file.json"
    directory_replaced_by_file = root / "empty"
    cached_file = root / "cached.bin"
    changed.parent.mkdir(parents=True)
    directory_replaced_by_file.mkdir()
    changed.write_bytes(b'{"value":"original"}')
    missing.write_bytes(b'{"value":"required"}')
    file_replaced_by_directory.write_bytes(b'{"kind":"file"}')
    cached_file.write_bytes(b"abcdef")
    snapshot = _snapshot_tree(root)

    baseline_hash = _cached_snapshot_sha256(cached_file, root, snapshot)
    cached_file.write_bytes(b"ghijkl")
    cached_mtime = snapshot.file_mtime_ns[cached_file.relative_to(root)] + 1_000_000_000
    os.utime(cached_file, ns=(cached_mtime, cached_mtime))
    assert _cached_snapshot_sha256(cached_file, root, snapshot) != baseline_hash

    changed.write_bytes(b'{"value":"tampered"}')
    missing.unlink()
    file_replaced_by_directory.unlink()
    file_replaced_by_directory.mkdir()
    (file_replaced_by_directory / "unexpected.txt").write_text("unexpected", encoding="utf-8")
    directory_replaced_by_file.rmdir()
    directory_replaced_by_file.write_text("wrong path type", encoding="utf-8")
    extra = root / "extra" / "nested"
    extra.mkdir(parents=True)
    (extra / "unexpected.json").write_text("{}", encoding="utf-8")

    _restore_tree(root, snapshot)

    assert _snapshot_tree(root) == snapshot
    assert _cached_snapshot_sha256(cached_file, root, snapshot) == baseline_hash


def test_exact_core_suite_is_publishable(tmp_path: Path) -> None:
    campaign_root = tmp_path / "campaigns"
    _write_verifications(campaign_root)

    evidence = assess_report_publishability(_measurement_records(), campaign_root)

    assert len(core_experiments()) == EXPECTED_CORE_CAMPAIGNS == 10
    assert evidence["status"] == "passed", evidence["issues"]
    assert evidence["publishable"] is True
    assert evidence["checks"]["exact_core_experiment_set"]["passed"] is True
    assert all(check["passed"] for check in evidence["checks"]["measurements"])
    assert all(check["passed"] for check in evidence["checks"]["campaign_records"])
    assert all(check["passed"] for check in evidence["checks"]["campaign_verifications"])


def test_core_catalog_rejects_any_count_other_than_ten(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(publishability, "CORE_CONFIGS", CORE_CONFIGS[:-1])

    with pytest.raises(ValueError, match="exactly 10 configs"):
        core_experiments()


def test_core_catalog_requires_ten_measurement_runs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config = {
        "experiment": {"id": "EXP", "measurement_runs": 9},
        "workload": {"suite": "micro", "query_id": "M02", "storage_profile": "test"},
    }
    monkeypatch.setattr(publishability, "load_experiment", lambda *_args: config)

    with pytest.raises(ValueError, match="exactly 10 measurement runs"):
        core_experiments(tmp_path)


def test_latest_campaign_verification_fails_closed_without_fallback(tmp_path: Path) -> None:
    campaign_root = tmp_path / "campaigns"
    _write_verifications(campaign_root)
    first = core_experiments()[0]
    latest = campaign_root / first.experiment_id / "campaign-verification-attempt-0002.json"
    latest.write_text('{"schema_version":', encoding="utf-8")

    evidence = assess_report_publishability(_measurement_records(), campaign_root)

    assert evidence["publishable"] is False
    check = next(
        item
        for item in evidence["checks"]["campaign_verifications"]
        if item["experiment_id"] == first.experiment_id
    )
    assert check["selected_attempt"] == 2
    assert check["selected_file"].endswith("campaign-verification-attempt-0002.json")
    assert check["passed"] is False
    assert any("unreadable" in issue for issue in check["issues"])


def test_measurement_policy_requires_all_ten_complete_pairs(tmp_path: Path) -> None:
    campaign_root = tmp_path / "campaigns"
    _write_verifications(campaign_root)
    records = _measurement_records()
    records.pop(
        next(index for index, record in enumerate(records) if record["phase"] == "measurement")
    )

    evidence = assess_report_publishability(records, campaign_root)

    assert evidence["publishable"] is False
    assert any("measurement record count must equal 20" in issue for issue in evidence["issues"])


def test_missing_correctness_gate_blocks_publication(tmp_path: Path) -> None:
    campaign_root = tmp_path / "campaigns"
    records = _measurement_records()
    _write_verifications(campaign_root, records)
    first = core_experiments()[0]
    records = [
        record
        for record in records
        if not (
            record["experiment_id"] == first.experiment_id
            and record["run_id"] == "correctness-spark_baseline"
        )
    ]

    evidence = assess_report_publishability(records, campaign_root)

    assert evidence["publishable"] is False
    assert any("required gate record is missing" in issue for issue in evidence["issues"])
    assert any("does not bind the loaded raw campaign" in issue for issue in evidence["issues"])


@pytest.mark.parametrize(
    ("section", "field", "invalid", "expected_issue"),
    [
        ("metrics", "collector_status", "partial", "collector_status='complete'"),
        ("plan_analysis", "status", "partial", "plan_analysis.status='complete'"),
    ],
)
def test_incomplete_measurement_observability_blocks_publication(
    tmp_path: Path,
    section: str,
    field: str,
    invalid: str,
    expected_issue: str,
) -> None:
    campaign_root = tmp_path / "campaigns"
    records = _measurement_records()
    _write_verifications(campaign_root, records)
    measurement = next(record for record in records if record["phase"] == "measurement")
    payload = measurement[section]
    assert isinstance(payload, dict)
    payload[field] = invalid

    evidence = assess_report_publishability(records, campaign_root)

    assert evidence["publishable"] is False
    assert any(expected_issue in issue for issue in evidence["issues"])


def test_resource_metric_exclusion_blocks_publication(tmp_path: Path) -> None:
    campaign_root = tmp_path / "campaigns"
    _write_verifications(campaign_root)
    records = _measurement_records()
    measurement = next(record for record in records if record["phase"] == "measurement")
    metrics = measurement["metrics"]
    assert isinstance(metrics, dict)
    metrics.pop("cpu_core_seconds")

    evidence = assess_report_publishability(records, campaign_root)

    assert evidence["publishable"] is False
    assert any(
        "cpu_core_seconds.excluded_pair_ids must be empty" in issue for issue in evidence["issues"]
    )


def test_failed_measurement_blocks_summary_admission(tmp_path: Path) -> None:
    campaign_root = tmp_path / "campaigns"
    _write_verifications(campaign_root)
    records = _measurement_records()
    measurement = next(record for record in records if record["phase"] == "measurement")
    measurement["status"] = "failed"

    evidence = assess_report_publishability(records, campaign_root)

    assert evidence["publishable"] is False
    assert any("status='succeeded'" in issue for issue in evidence["issues"])
    assert any("summary.n_succeeded must equal 20" in issue for issue in evidence["issues"])
    assert any("summary.paired_failures must be empty" in issue for issue in evidence["issues"])


def test_unexpected_measurement_experiment_blocks_exact_core_set(tmp_path: Path) -> None:
    campaign_root = tmp_path / "campaigns"
    _write_verifications(campaign_root)
    records = _measurement_records()
    unexpected = dict(records[0])
    unexpected["experiment_id"] = "EXP-UNREVIEWED"
    unexpected["run_id"] = "unreviewed-measurement"
    records.append(unexpected)

    evidence = assess_report_publishability(records, campaign_root)

    assert evidence["publishable"] is False
    exact_set = evidence["checks"]["exact_core_experiment_set"]
    assert exact_set["unexpected"] == ["EXP-UNREVIEWED"]


@pytest.mark.parametrize(
    ("field", "invalid_value", "expected_issue"),
    [
        ("planned", True, "report.planned must equal 24"),
        ("succeeded", True, "report.succeeded must equal 24"),
        ("failed", False, "report.failed must equal 0"),
        ("executed", True, "report.executed must be a non-negative integer"),
        ("resumed", False, "report.resumed must be a non-negative integer"),
        ("complete", 1, "report.complete must equal True"),
    ],
)
def test_verification_rejects_boolean_counters_and_non_boolean_complete(
    tmp_path: Path, field: str, invalid_value: object, expected_issue: str
) -> None:
    campaign_root = tmp_path / "campaigns"
    _write_verifications(campaign_root)
    first = core_experiments()[0]
    path = campaign_root / first.experiment_id / "campaign-verification.json"
    verification = _verification_value(first.experiment_id)
    report = verification["report"]
    assert isinstance(report, dict)
    report[field] = invalid_value
    path.write_text(json.dumps(verification), encoding="utf-8")

    evidence = assess_report_publishability(_measurement_records(), campaign_root)

    assert evidence["publishable"] is False
    check = next(
        item
        for item in evidence["checks"]["campaign_verifications"]
        if item["experiment_id"] == first.experiment_id
    )
    assert expected_issue in check["issues"]


def test_latest_valid_attempt_supersedes_failed_base(tmp_path: Path) -> None:
    campaign_root = tmp_path / "campaigns"
    _write_verifications(campaign_root)
    first = core_experiments()[0]
    directory = campaign_root / first.experiment_id
    base = _verification_value(first.experiment_id)
    base["status"] = "failed"
    (directory / "campaign-verification.json").write_text(json.dumps(base), encoding="utf-8")
    latest = directory / "campaign-verification-attempt-0002.json"
    latest.write_text(json.dumps(_verification_value(first.experiment_id)), encoding="utf-8")

    evidence = assess_report_publishability(_measurement_records(), campaign_root)

    assert evidence["publishable"] is True
    check = next(
        item
        for item in evidence["checks"]["campaign_verifications"]
        if item["experiment_id"] == first.experiment_id
    )
    assert check["selected_attempt"] == 2
    assert check["passed"] is True


def test_verification_must_bind_physical_artifact_hashes(tmp_path: Path) -> None:
    campaign_root = tmp_path / "campaigns"
    records = _measurement_records()
    _write_verifications(campaign_root, records)
    first = core_experiments()[0]
    path = campaign_root / first.experiment_id / "campaign-verification.json"
    verification = json.loads(path.read_text(encoding="utf-8"))
    verification["report"]["artifact_files_sha256"] = "0" * 64
    path.write_text(json.dumps(verification), encoding="utf-8")

    evidence = assess_report_publishability(records, campaign_root)

    assert evidence["publishable"] is False
    assert any("does not bind campaign artifacts" in issue for issue in evidence["issues"])


def test_experiment_manifest_must_bind_current_core_config(tmp_path: Path) -> None:
    campaign_root = tmp_path / "campaigns"
    records = _measurement_records()
    _write_verifications(campaign_root, records)
    first = core_experiments()[0]
    directory = campaign_root / first.experiment_id
    manifest_path = directory / "experiment-manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["input_hashes"]["experiment_config_sha256"] = "0" * 64
    manifest.pop("manifest_sha256")
    manifest["manifest_sha256"] = sha256_value(manifest)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    verification_path = directory / "campaign-verification.json"
    verification = json.loads(verification_path.read_text(encoding="utf-8"))
    verification["report"]["experiment_manifest_sha256"] = manifest["manifest_sha256"]
    verification_path.write_text(json.dumps(verification), encoding="utf-8")

    evidence = assess_report_publishability(records, campaign_root)

    assert evidence["publishable"] is False
    assert any(
        "does not bind current experiment_config_sha256" in issue for issue in evidence["issues"]
    )


@pytest.mark.parametrize(
    "field",
    [
        "runtime_lock_sha256",
        "workload_sql_sha256",
        "workload_manifest_sha256",
        "dataset_manifest_sha256",
        "uv_lock_sha256",
        "spark_defaults_sha256",
        "comet_profile_sha256",
    ],
)
def test_experiment_manifest_must_bind_every_current_input(tmp_path: Path, field: str) -> None:
    campaign_root = tmp_path / "campaigns"
    records = _measurement_records()
    _write_verifications(campaign_root, records)
    first = core_experiments()[0]
    directory = campaign_root / first.experiment_id
    manifest_path = directory / "experiment-manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["input_hashes"][field] = "0" * 64
    manifest.pop("manifest_sha256")
    manifest["manifest_sha256"] = sha256_value(manifest)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    verification_path = directory / "campaign-verification.json"
    verification = json.loads(verification_path.read_text(encoding="utf-8"))
    verification["report"]["experiment_manifest_sha256"] = manifest["manifest_sha256"]
    verification_path.write_text(json.dumps(verification), encoding="utf-8")

    evidence = assess_report_publishability(records, campaign_root)

    assert evidence["publishable"] is False
    assert any(f"does not bind current {field}" in issue for issue in evidence["issues"])


def test_experiment_manifest_rejects_unexpected_input_hash(tmp_path: Path) -> None:
    campaign_root = tmp_path / "campaigns"
    records = _measurement_records()
    _write_verifications(campaign_root, records)
    first = core_experiments()[0]
    directory = campaign_root / first.experiment_id
    manifest_path = directory / "experiment-manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["input_hashes"]["unexpected_sha256"] = "0" * 64
    manifest.pop("manifest_sha256")
    manifest["manifest_sha256"] = sha256_value(manifest)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    verification_path = directory / "campaign-verification.json"
    verification = json.loads(verification_path.read_text(encoding="utf-8"))
    verification["report"]["experiment_manifest_sha256"] = manifest["manifest_sha256"]
    verification_path.write_text(json.dumps(verification), encoding="utf-8")

    evidence = assess_report_publishability(records, campaign_root)

    assert evidence["publishable"] is False
    assert any("unexpected input hashes" in issue for issue in evidence["issues"])


def test_raw_git_commit_must_equal_current_clean_head(tmp_path: Path) -> None:
    campaign_root = tmp_path / "campaigns"
    records = _measurement_records()
    _write_verifications(campaign_root, records)
    provenance = records[0]["provenance"]
    assert isinstance(provenance, dict)
    provenance["git_commit"] = "c" * 40

    evidence = assess_report_publishability(records, campaign_root)

    assert evidence["publishable"] is False
    check = evidence["checks"]["repository_provenance"]
    assert check["passed"] is False
    assert any("current clean Git HEAD" in issue for issue in check["issues"])


def test_dirty_repository_blocks_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    campaign_root = tmp_path / "campaigns"
    records = _measurement_records()
    _write_verifications(campaign_root, records)

    def fail_dirty(_root: Path) -> str:
        raise RepositoryEvidenceError("repository worktree is not clean")

    monkeypatch.setattr(publishability, "clean_git_commit", fail_dirty)

    evidence = assess_report_publishability(records, campaign_root)

    assert evidence["publishable"] is False
    check = evidence["checks"]["repository_provenance"]
    assert check["passed"] is False
    assert check["current_git_commit"] is None
    assert "repository worktree is not clean" in check["issues"]


def test_control_artifact_tampering_blocks_publication(tmp_path: Path) -> None:
    campaign_root = tmp_path / "campaigns"
    records = _measurement_records()
    _write_verifications(campaign_root, records)
    first = core_experiments()[0]
    medallion = _fixture_state().medallions[first.experiment_id]
    medallion.write_text('{"status":"failed"}', encoding="utf-8")

    evidence = assess_report_publishability(records, campaign_root)

    assert evidence["publishable"] is False
    assert any("control artifacts" in issue or "Medallion" in issue for issue in evidence["issues"])


def test_rehashed_physical_plan_tampering_blocks_publication(tmp_path: Path) -> None:
    campaign_root = tmp_path / "campaigns"
    records = _measurement_records()
    record = records[0]
    artifacts = cast(dict[str, str], record["artifacts"])
    (ROOT / artifacts["physical_plan"]).write_text("Filter forged_predicate\n", encoding="utf-8")
    _write_verifications(campaign_root, records)

    evidence = assess_report_publishability(records, campaign_root)

    assert evidence["publishable"] is False
    assert any(
        "plan_analysis does not match final-plan.txt" in issue for issue in evidence["issues"]
    )


def test_rehashed_event_metric_tampering_blocks_publication(tmp_path: Path) -> None:
    campaign_root = tmp_path / "campaigns"
    records = _measurement_records()
    record = records[0]
    artifacts = cast(dict[str, str], record["artifacts"])
    event_path = ROOT / artifacts["event_log"]
    events = [json.loads(line) for line in event_path.read_text(encoding="utf-8").splitlines()]
    task = next(event for event in events if event["Event"] == "SparkListenerTaskEnd")
    task["Task Metrics"]["Executor CPU Time"] = 9_000_000_000
    event_path.write_text(
        "".join(json.dumps(event, separators=(",", ":")) + "\n" for event in events),
        encoding="utf-8",
    )
    _write_verifications(campaign_root, records)

    evidence = assess_report_publishability(records, campaign_root)

    assert evidence["publishable"] is False
    assert any(
        "raw record differs from independently rebuilt evidence: metrics" in issue
        for issue in evidence["issues"]
    )


def test_rehashed_resource_summary_tampering_blocks_publication(tmp_path: Path) -> None:
    campaign_root = tmp_path / "campaigns"
    records = _measurement_records()
    record = records[0]
    artifacts = cast(dict[str, str], record["artifacts"])
    resource_path = ROOT / artifacts["resource_samples"]
    resource = json.loads(resource_path.read_text(encoding="utf-8"))
    resource["summary"]["cpu_peak_percent_of_limit"] = 99.0
    _write_json(resource_path, resource)
    _write_verifications(campaign_root, records)

    evidence = assess_report_publishability(records, campaign_root)

    assert evidence["publishable"] is False
    assert any(
        "resource summary does not match its samples" in issue for issue in evidence["issues"]
    )


def test_rehashed_application_result_tampering_blocks_publication(tmp_path: Path) -> None:
    campaign_root = tmp_path / "campaigns"
    records = _measurement_records()
    record = records[0]
    artifacts = cast(dict[str, str], record["artifacts"])
    application_path = (ROOT / artifacts["physical_plan"]).parent / "application-result.json"
    application = json.loads(application_path.read_text(encoding="utf-8"))
    application["row_count"] = 2
    _write_json(application_path, application)
    _write_verifications(campaign_root, records)

    evidence = assess_report_publishability(records, campaign_root)

    assert evidence["publishable"] is False
    assert any(
        "raw record differs from independently rebuilt evidence: correctness" in issue
        for issue in evidence["issues"]
    )


def test_rehashed_attempt_admission_tampering_blocks_publication(tmp_path: Path) -> None:
    campaign_root = tmp_path / "campaigns"
    records = _measurement_records()
    record = records[0]
    artifacts = cast(dict[str, str], record["artifacts"])
    admission_path = (ROOT / artifacts["physical_plan"]).parent / "attempt-admission.json"
    admission = json.loads(admission_path.read_text(encoding="utf-8"))
    admission["run"]["timeout_seconds"] += 1
    admission.pop("artifact_sha256")
    _write_json(admission_path, _self_hashed(admission))
    _write_verifications(campaign_root, records)

    evidence = assess_report_publishability(records, campaign_root)

    assert evidence["publishable"] is False
    assert any("admission run identity differs" in issue for issue in evidence["issues"])


def test_dataset_attestation_tampering_blocks_publication(tmp_path: Path) -> None:
    campaign_root = tmp_path / "campaigns"
    records = _measurement_records()
    _write_verifications(campaign_root, records)
    first = core_experiments()[0]
    attestation = _control_targets(first.experiment_id)["dataset-validation-attestation"]
    payload = json.loads(attestation.read_text(encoding="utf-8"))
    payload["dataset"]["content_identity_sha256"] = "0" * 64
    attestation.write_text(json.dumps(payload), encoding="utf-8")

    evidence = assess_report_publishability(records, campaign_root)

    assert evidence["publishable"] is False
    assert any("attestation" in issue for issue in evidence["issues"])


def test_visible_transient_retry_evidence_remains_publishable(tmp_path: Path) -> None:
    campaign_root = tmp_path / "campaigns"
    records = _measurement_records()
    first = core_experiments()[0]
    success = next(
        record
        for record in records
        if record["experiment_id"] == first.experiment_id
        and record["run_id"] == "correctness-spark_baseline"
    )
    failed = copy.deepcopy(success)
    failed["timestamp"] = "2026-08-30T00:59:59Z"
    failed["status"] = "timeout"
    failed["failure"] = {"class": "TimeoutExpired", "message": "transient timeout"}

    controls = _control_targets(first.experiment_id)
    run_attempts = controls["run-attempts"]
    retry_dir = run_attempts / str(success["run_id"]) / "attempt-0002"
    shutil.copytree(retry_dir.parent / "attempt-0001", retry_dir)
    admission_path = retry_dir / "attempt-admission.json"
    admission = json.loads(admission_path.read_text(encoding="utf-8"))
    admission["attempt_index"] = 2
    admission.pop("artifact_sha256")
    admission_path.write_text(json.dumps(_self_hashed(admission)), encoding="utf-8")
    success["artifacts"] = {
        field: _repo_relative(retry_dir / name) for field, name in _ARTIFACT_NAMES.items()
    }
    failed_attempt = (
        controls["failed-attempt-records"]
        / first.experiment_id
        / str(success["engine"])
        / f"{success['run_id']}-attempt-0001.json"
    )
    _write_json(failed_attempt, failed)
    _write_verifications(campaign_root, records)

    evidence = assess_report_publishability(records, campaign_root)

    assert evidence["publishable"] is True
