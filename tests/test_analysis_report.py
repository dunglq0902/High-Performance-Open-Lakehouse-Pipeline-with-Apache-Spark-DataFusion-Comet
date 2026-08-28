from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from analysis.scripts.build_report import build_report

HASH = "a" * 64


def _record(engine: str, pair: int, latency: float) -> dict:
    native = engine == "comet_accelerated"
    return {
        "schema_version": 1,
        "experiment_id": "EXP-REPORT-M02",
        "run_id": f"{engine}-{pair}",
        "pair_index": pair,
        "phase": "measurement",
        "timestamp": "2026-08-27T00:00:00Z",
        "status": "succeeded",
        "failure": None,
        "engine": engine,
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
            "comet_version": "1.0.0" if native else None,
            "iceberg_version": "1.11.0",
        },
        "resources": {
            "cpu_model": "test",
            "allocated_cores": 2,
            "cgroup_memory_limit_mib": 5120,
            "executor_heap_mib": 2048,
            "off_heap_mib": 1024,
        },
        "metrics": {
            "sql_execution_id": pair,
            "query_wall_time_ms": latency,
            "sql_execution_time_ms": latency - 1,
            "cpu_core_seconds": 1.0,
            "cpu_peak_percent_of_limit": 50.0,
            "cgroup_memory_peak_mib": 512.0,
            "jvm_gc_time_ms": 0.0,
            "shuffle_read_mb": 0.0,
            "shuffle_write_mb": 0.0,
            "disk_spill_mb": 0.0,
            "collector_status": "complete",
        },
        "plan_analysis": {
            "status": "complete",
            "total_operators": 2,
            "comet_native_operators": 2 if native else 0,
            "spark_fallback_operators": 0,
            "transition_count": 0,
            "native_subtree_count": 1 if native else 0,
            "native_coverage_ratio": 1.0 if native else None,
            "fallback_reasons": [],
            "unknown_nodes": [],
            "scan_implementations": ["CometScan" if native else "BatchScan"],
        },
        "correctness": {
            "status": "passed",
            "schema_sha256": HASH,
            "row_count": 1,
            "canonical_result_sha256": HASH,
        },
        "artifacts": {
            "event_log": "eventlog",
            "physical_plan": "plan.txt",
            "resource_samples": "resources.json",
            "stdout": "stdout.log",
            "stderr": "stderr.log",
        },
    }


def test_report_is_rebuildable_and_suppresses_small_sample_p95(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    for pair in range(1, 4):
        for record in (
            _record("spark_baseline", pair, 200.0),
            _record("comet_accelerated", pair, 100.0),
        ):
            (raw / f"{record['run_id']}.json").write_text(json.dumps(record), encoding="utf-8")
    output = tmp_path / "report"
    produced = build_report(raw, output)
    assert output / "technical-report.md" in produced
    report = (output / "technical-report.md").read_text(encoding="utf-8")
    assert "2.000" in report
    assert "Ratio of medians" in report
    assert "geometric-mean speedup" in report
    assert "paired CPU saving ratio" in report
    assert "[2.000, 2.000]" in report
    assert "100.00%" in report
    summary = json.loads((output / "EXP-REPORT-M02.summary.json").read_text())
    assert summary["engines"]["spark_baseline"]["p95"] is None
    suite_summary = json.loads((output / "suite-summary.json").read_text())
    assert suite_summary["geometric_mean_speedup"] == 2.0
    assert (output / "EXP-REPORT-M02.latency.svg").read_text().startswith("<svg")


def test_report_rejects_smoke_identity(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    record = deepcopy(_record("spark_baseline", 1, 10.0))
    record["experiment_id"] = "SMOKE-M02"
    (raw / "smoke.json").write_text(json.dumps(record), encoding="utf-8")
    try:
        build_report(raw, tmp_path / "output")
    except ValueError as error:
        assert "cannot be promoted" in str(error)
    else:
        raise AssertionError("smoke record was promoted into a research report")
