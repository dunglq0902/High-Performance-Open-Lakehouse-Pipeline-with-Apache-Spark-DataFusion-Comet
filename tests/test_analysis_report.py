from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

import analysis.scripts.build_report as report_module
from analysis.scripts.build_report import ReportNotPublishableError, build_report
from benchmark.parsers.plan import analyze_plan

HASH = "a" * 64


def _record(engine: str, pair: int, latency: float) -> dict[str, Any]:
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
    produced = build_report(raw, output, campaign_root=tmp_path / "campaigns")
    assert output / "technical-report.md" in produced
    report = (output / "technical-report.md").read_text(encoding="utf-8")
    assert "2.000" in report
    assert "Ratio of medians" in report
    assert "geometric-mean speedup" in report
    assert "paired CPU saving ratio" in report
    assert "[2.000, 2.000]" in report
    assert "100.00%" in report
    assert "DIAGNOSTIC ONLY — NOT PUBLISHABLE" in report
    summary = json.loads((output / "EXP-REPORT-M02.summary.json").read_text())
    assert summary["engines"]["spark_baseline"]["p95"] is None
    suite_summary = json.loads((output / "suite-summary.json").read_text())
    assert suite_summary["geometric_mean_speedup"] == 2.0
    assert (output / "EXP-REPORT-M02.latency.svg").read_text().startswith("<svg")
    publication = json.loads((output / "report-publishability.json").read_text())
    assert publication["publishable"] is False


def test_report_claims_follow_slower_partial_native_fresh_results(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    for pair in range(1, 4):
        spark = _record("spark_baseline", pair, 100.0)
        comet = _record("comet_accelerated", pair, 200.0)
        comet["plan_analysis"]["native_coverage_ratio"] = 0.5
        comet["plan_analysis"]["comet_native_operators"] = 1
        for record in (spark, comet):
            (raw / f"{record['run_id']}.json").write_text(json.dumps(record), encoding="utf-8")

    output = tmp_path / "report"
    build_report(raw, output, campaign_root=tmp_path / "campaigns")

    report = (output / "technical-report.md").read_text(encoding="utf-8")
    assert "No admitted TPC-H experiment evidence is present." in report
    assert (
        "median paired speedup was above 1 in 0 of 1 experiment, equal to 1 in 0, and below 1 in 1"
    ) in report
    assert "H1 classification counts were supported=0, inconclusive=0, and decreased=1" in report
    assert "smallest was 0.500x for M02 (EXP-REPORT-M02)" in report
    assert (
        "Median count-based native coverage was 100% in 0 of 1 experiment, partial in 1" in report
    )
    assert "coverage or operator counts detected fallback in 1 experiment" in report
    assert "fallback observed; no explicit reason annotation" in report
    assert (
        "Median paired CPU core-seconds saving ratios by experiment: positive=0, zero=1" in report
    )
    assert "Median paired peak-memory saving ratios by experiment: positive=0, zero=1" in report
    assert "Admitted paired sample size is n=3 pairs per experiment" in report
    assert "Query complexity and fallback transitions changed together" not in report
    assert "Comet reduced the median paired wall time" not in report


@pytest.mark.parametrize(
    ("scales", "expected"),
    [
        ([1], "covers SF1 only; SF10 is absent"),
        ([10], "covers SF10 only; SF1 is absent"),
        ([1, 10], "covers SF1 and SF10; matched-query comparability is evaluated in RQ3"),
    ],
)
def test_tpch_scope_claim_is_derived_from_admitted_scales(scales: list[int], expected: str) -> None:
    findings = {"RQ3": {"scale_comparison": {"scales_present": scales}}}

    assert expected in report_module._tpch_evidence_sentence(findings)


def test_scale_limit_does_not_equate_missing_admitted_evidence_with_not_run() -> None:
    findings = {
        "RQ3": {
            "scale_comparison": {
                "scales_present": [1],
                "estimability": "not_estimable",
                "reason": "Only SF1 TPC-H evidence is present.",
            }
        }
    }

    line = report_module._scale_limit_line(findings)

    assert "No admitted SF10 evidence is present" in line
    assert "was not run" not in line


def test_scale_limit_requires_matched_queries_before_describing_comparison() -> None:
    findings = {
        "RQ3": {
            "scale_comparison": {
                "scales_present": [1, 10],
                "estimability": "not_estimable",
                "reason": "SF1 and SF10 do not contain the same TPC-H query set.",
            }
        }
    }

    line = report_module._scale_limit_line(findings)

    assert "Multiple admitted scales are present, but scale sensitivity is not estimable" in line
    assert "two-point descriptive evidence" not in line


def test_combined_gate_passes_only_when_report_content_is_complete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    plans: dict[str, tuple[str, str]] = {
        "spark_baseline": (
            "Project [value#1]\n+- BatchScan test.table[value#1]\n",
            "plans/spark/final-plan.txt",
        ),
        "comet_accelerated": (
            "CometProject [value#1]\n+- CometNativeScan [value#1], test.table\n",
            "plans/comet/final-plan.txt",
        ),
    }
    resource_path = tmp_path / "resources.json"
    resource_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "scope": "spark-worker-executor-container",
                "timed_out": False,
                "window_started": True,
                "window_completed": True,
                "aborted": False,
                "samples": [
                    {
                        "timestamp_ns": timestamp,
                        "source": "cgroup_v2",
                        "status": "complete",
                        "cpu_usage_ns": cpu,
                        "memory_current_bytes": memory,
                        "memory_peak_bytes": memory,
                        "swap_current_bytes": 0,
                        "io_read_bytes": 0,
                        "io_write_bytes": 0,
                        "cpu_limit_cores": 2.0,
                        "process_count": 1,
                        "missing_metrics": [],
                        "errors": [],
                    }
                    for timestamp, cpu, memory in (
                        (0, 0, 128 * 1024 * 1024),
                        (1_000_000_000, 1_000_000_000, 256 * 1024 * 1024),
                    )
                ],
                "summary": {
                    "status": "complete",
                    "source": "cgroup_v2",
                    "sample_count": 2,
                    "missing_metrics": [],
                    "issues": [],
                },
            }
        ),
        encoding="utf-8",
    )
    for plan, relative in plans.values():
        final_path = tmp_path / relative
        final_path.parent.mkdir(parents=True)
        final_path.write_text(plan, encoding="utf-8")
        (final_path.parent / "initial-plan.txt").write_text(plan, encoding="utf-8")

    for pair in range(1, 4):
        for record in (
            _record("spark_baseline", pair, 200.0),
            _record("comet_accelerated", pair, 100.0),
        ):
            plan, relative = plans[record["engine"]]
            record["plan_analysis"] = analyze_plan(
                plan, comet_enabled=record["engine"] == "comet_accelerated"
            )
            record["artifacts"]["physical_plan"] = relative
            record["artifacts"]["resource_samples"] = "resources.json"
            (raw / f"{record['run_id']}.json").write_text(json.dumps(record), encoding="utf-8")

    def fake_publication(*_args: object, **_kwargs: object) -> dict[str, object]:
        return {
            "schema_version": 1,
            "status": "passed",
            "publishable": True,
            "policy": {
                "core_experiments": [{"experiment_id": "EXP-REPORT-M02"}],
                "expected_measurement_pairs_per_experiment": 3,
            },
            "checks": {
                "campaign_verifications": [
                    {
                        "experiment_id": "EXP-REPORT-M02",
                        "passed": True,
                        "execution_attempt_count": 6,
                        "failed_attempt_record_count": 0,
                        "attempt_counts_verified": True,
                    }
                ]
            },
            "issues": [],
        }

    monkeypatch.setattr(report_module, "assess_report_publishability", fake_publication)
    output = tmp_path / "report"

    produced = build_report(
        raw,
        output,
        campaign_root=tmp_path / "campaigns",
        require_publishable=True,
        repository_root=tmp_path,
    )

    assert output / "report-contract.json" in produced
    assert json.loads((output / "report-contract.json").read_text())["passed"] is True
    assert json.loads((output / "report-publishability.json").read_text())["publishable"] is True
    report = (output / "technical-report.md").read_text(encoding="utf-8")
    assert "PUBLICATION GATE: PASSED" in report
    assert "### RQ1" in report and "### RQ2" in report and "### RQ3" in report
    assert "M08 partial-native evidence was not observed" in report
    assert "No partial-native M08 result was observed, so none is claimed." in report
    assert "- H3: not estimable;" in report
    assert "median coverage was 40%" not in report


def test_strict_report_fails_only_after_diagnostic_artifacts_are_written(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    for record in (
        _record("spark_baseline", 1, 200.0),
        _record("comet_accelerated", 1, 100.0),
    ):
        (raw / f"{record['run_id']}.json").write_text(json.dumps(record), encoding="utf-8")
    output = tmp_path / "report"

    with pytest.raises(ReportNotPublishableError, match="diagnostic-only"):
        build_report(
            raw,
            output,
            campaign_root=tmp_path / "campaigns",
            require_publishable=True,
        )

    assert (output / "report-publishability.json").is_file()
    assert (output / "technical-report.md").is_file()
    assert (output / "normalized-measurements.csv").is_file()
    assert "DIAGNOSTIC ONLY" in (output / "technical-report.md").read_text(encoding="utf-8")


def test_report_rejects_smoke_identity(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    record = deepcopy(_record("spark_baseline", 1, 10.0))
    record["experiment_id"] = "SMOKE-M02"
    (raw / "smoke.json").write_text(json.dumps(record), encoding="utf-8")
    try:
        build_report(raw, tmp_path / "output", campaign_root=tmp_path / "campaigns")
    except ValueError as error:
        assert "cannot be promoted" in str(error)
    else:
        raise AssertionError("smoke record was promoted into a research report")


def test_rebuild_prunes_stale_per_experiment_artifacts(tmp_path: Path) -> None:
    first_raw = tmp_path / "first-raw"
    first_raw.mkdir()
    for record in (
        _record("spark_baseline", 1, 200.0),
        _record("comet_accelerated", 1, 100.0),
    ):
        (first_raw / f"{record['run_id']}.json").write_text(json.dumps(record), encoding="utf-8")
    output = tmp_path / "report"
    build_report(first_raw, output, campaign_root=tmp_path / "campaigns")
    stale = (
        output / "EXP-REPORT-M02.summary.json",
        output / "EXP-REPORT-M02.latency.svg",
        output / "EXP-REPORT-M02.native-coverage.svg",
    )
    assert all(path.is_file() for path in stale)

    empty_raw = tmp_path / "empty-raw"
    empty_raw.mkdir()
    build_report(empty_raw, output, campaign_root=tmp_path / "campaigns")

    assert all(not path.exists() for path in stale)
    assert (output / "technical-report.md").is_file()
