from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parents[1]
HASH = "a" * 64


def valid_raw_record() -> dict[str, object]:
    return {
        "schema_version": 1,
        "experiment_id": "EXP-M02",
        "run_id": "spark-baseline-001",
        "pair_index": 1,
        "phase": "measurement",
        "timestamp": "2026-08-24T12:00:00Z",
        "status": "succeeded",
        "failure": None,
        "engine": "spark_baseline",
        "workload": "micro",
        "query_id": "M02",
        "storage_profile": "ecommerce_iceberg_rest",
        "provenance": {
            "git_commit": "abcdef0",
            "container_image_digest": f"sha256:{HASH}",
            "dataset_manifest_sha256": HASH,
            "spark_conf_sha256": HASH,
            "sql_sha256": HASH,
            "iceberg_snapshot_ids": [1],
        },
        "runtime": {
            "spark_version": "4.1.3",
            "scala_version": "2.13.17",
            "java_version": "17.0.19+10",
            "comet_version": None,
            "iceberg_version": "1.11.0",
        },
        "resources": {
            "cpu_model": "test-cpu",
            "allocated_cores": 2,
            "cgroup_memory_limit_mib": 8192,
            "executor_heap_mib": 2048,
            "off_heap_mib": 1024,
        },
        "metrics": {
            "sql_execution_id": 1,
            "query_wall_time_ms": 10.0,
            "sql_execution_time_ms": 9.0,
            "cpu_core_seconds": 0.1,
            "cpu_peak_percent_of_limit": 50.0,
            "cgroup_memory_peak_mib": 1024.0,
            "jvm_gc_time_ms": 0.0,
            "shuffle_read_mb": 0.0,
            "shuffle_write_mb": 0.0,
            "disk_spill_mb": 0.0,
            "collector_status": "complete",
        },
        "plan_analysis": {
            "status": "complete",
            "total_operators": 3,
            "comet_native_operators": 0,
            "spark_fallback_operators": 0,
            "transition_count": 1,
            "native_subtree_count": 0,
            "native_coverage_ratio": None,
            "fallback_reasons": [],
            "unknown_nodes": [],
            "scan_implementations": ["BatchScan"],
        },
        "correctness": {
            "status": "passed",
            "schema_sha256": HASH,
            "row_count": 1,
            "canonical_result_sha256": HASH,
        },
        "artifacts": {
            "event_log": "eventlog.zstd",
            "physical_plan": "final-plan.txt",
            "resource_samples": "resources.csv.zstd",
            "stdout": "stdout.log.zstd",
            "stderr": "stderr.log.zstd",
        },
    }


def _validator() -> Draft202012Validator:
    schema = json.loads(
        (ROOT / "benchmark/schemas/raw-result.schema.json").read_text(encoding="utf-8")
    )
    return Draft202012Validator(schema, format_checker=FormatChecker())


def test_raw_result_schema_accepts_complete_success() -> None:
    assert list(_validator().iter_errors(valid_raw_record())) == []


def test_raw_result_schema_rejects_null_success_latency_and_invalid_time() -> None:
    record = deepcopy(valid_raw_record())
    record["timestamp"] = "not-a-time"
    record["metrics"]["query_wall_time_ms"] = None  # type: ignore[index]
    messages = [error.message for error in _validator().iter_errors(record)]
    assert any("date-time" in message for message in messages)
    assert any("not of type 'number'" in message for message in messages)


def test_raw_result_schema_requires_failure_details_on_failure() -> None:
    record = deepcopy(valid_raw_record())
    record["status"] = "timeout"
    assert any(
        "not of type 'object'" in error.message for error in _validator().iter_errors(record)
    )
