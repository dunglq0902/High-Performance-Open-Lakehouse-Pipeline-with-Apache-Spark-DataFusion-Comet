from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from benchmark.runner.campaign import (
    CampaignError,
    CampaignRunner,
    plan_campaign,
    run_subprocess,
)
from benchmark.runner.canonical import sha256_value

ROOT = Path(__file__).resolve().parents[1]
HASH = "a" * 64


def manifest(measurements: int = 2) -> dict:
    schedule = [
        {
            "pair_index": index,
            "order": (
                ["spark_baseline", "comet_accelerated"]
                if index % 2
                else ["comet_accelerated", "spark_baseline"]
            ),
        }
        for index in range(1, measurements + 1)
    ]
    return {
        "experiment_id": "EXP-M02",
        "resolved_config": {
            "experiment": {"timeout_seconds": 10, "warmup_runs": 2},
            "matrix": {
                "engines": [
                    {"name": "spark_baseline"},
                    {"name": "comet_accelerated"},
                ]
            },
        },
        "schedule": schedule,
    }


def raw_record(run, *, status: str = "succeeded") -> dict:
    native = 1 if run.engine == "comet_accelerated" else 0
    plan_status = "complete"
    failure = None if status == "succeeded" else {"class": "TestFailure", "message": "failed"}
    metric_execution_id = 1 if status == "succeeded" else None
    wall_time = 10.0 if status == "succeeded" else None
    execution_time = 9.0 if status == "succeeded" else None
    correctness_status = "passed" if status == "succeeded" else "not_checked"
    correctness_hash = HASH if status == "succeeded" else None
    row_count = 1 if status == "succeeded" else None
    return {
        "schema_version": 1,
        "experiment_id": run.experiment_id,
        "run_id": run.run_id,
        "pair_index": run.pair_index,
        "phase": run.phase,
        "timestamp": "2026-08-27T00:00:00Z",
        "status": status,
        "failure": failure,
        "engine": run.engine,
        "workload": "micro",
        "query_id": "M02",
        "storage_profile": "ecommerce_iceberg_rest",
        "provenance": {
            "git_commit": "abcdef0",
            "container_image_digest": f"sha256:{HASH}",
            "dataset_manifest_sha256": HASH,
            "spark_conf_sha256": sha256_value({"engine": run.engine}),
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
            "cgroup_memory_limit_mib": 8192,
            "executor_heap_mib": 2048,
            "off_heap_mib": 1024,
        },
        "metrics": {
            "sql_execution_id": metric_execution_id,
            "query_wall_time_ms": wall_time,
            "sql_execution_time_ms": execution_time,
            "cpu_core_seconds": None,
            "cpu_peak_percent_of_limit": None,
            "cgroup_memory_peak_mib": None,
            "jvm_gc_time_ms": None,
            "shuffle_read_mb": None,
            "shuffle_write_mb": None,
            "disk_spill_mb": None,
            "collector_status": "unavailable",
        },
        "plan_analysis": {
            "status": plan_status,
            "total_operators": 1,
            "comet_native_operators": native,
            "spark_fallback_operators": 0,
            "transition_count": 0,
            "native_subtree_count": native,
            "native_coverage_ratio": float(native) if native else None,
            "fallback_reasons": [],
            "unknown_nodes": [],
            "scan_implementations": ["CometScan" if native else "BatchScan"],
        },
        "correctness": {
            "status": correctness_status,
            "schema_sha256": correctness_hash,
            "row_count": row_count,
            "canonical_result_sha256": correctness_hash,
        },
        "artifacts": {
            "event_log": "event-log",
            "physical_plan": "final-plan.txt",
            "resource_samples": "resources.json",
            "stdout": "stdout.log",
            "stderr": "stderr.log",
        },
    }


def current_provenance(run) -> dict:
    record = raw_record(run)
    return {**record["provenance"], "resources": record["resources"]}


def runner() -> CampaignRunner:
    return CampaignRunner.from_schema_path(ROOT / "benchmark/schemas/raw-result.schema.json")


def test_campaign_plan_is_deterministic_and_carries_warmups() -> None:
    plan = plan_campaign(manifest())
    assert [run.phase for run in plan[:4]] == [
        "correctness",
        "correctness",
        "plan_capture",
        "plan_capture",
    ]
    assert [run.run_id for run in plan[4:]] == [
        "measurement-p0001-o1-spark_baseline",
        "measurement-p0001-o2-comet_accelerated",
        "measurement-p0002-o1-comet_accelerated",
        "measurement-p0002-o2-spark_baseline",
    ]
    assert all(run.warmup_runs == 2 for run in plan[4:])
    assert all(run.warmup_runs == 0 for run in plan[:4])


def test_campaign_writes_immutable_records_and_resumes(tmp_path: Path) -> None:
    calls = []

    def execute(run):
        calls.append(run.run_id)
        return raw_record(run)

    first = runner().run(manifest(), tmp_path, execute, expected_provenance=current_provenance)
    assert first.complete
    assert first.executed == 8
    assert first.resumed == 0

    second = runner().run(manifest(), tmp_path, execute, expected_provenance=current_provenance)
    assert second.complete
    assert second.executed == 0
    assert second.resumed == 8
    assert len(calls) == 8


def test_campaign_refuses_mismatched_resume_and_failed_gate(tmp_path: Path) -> None:
    plan = plan_campaign(manifest())
    first_path = plan[0].raw_path(tmp_path)
    first_path.parent.mkdir(parents=True)
    mismatched = raw_record(plan[0])
    mismatched["run_id"] = "other"
    first_path.write_text(json.dumps(mismatched), encoding="utf-8")
    with pytest.raises(CampaignError, match="disagrees"):
        runner().run(manifest(), tmp_path, raw_record, expected_provenance=current_provenance)

    failed_root = tmp_path / "failed"

    def fail_comet_correctness(run):
        status = "failed" if run.run_id == "correctness-comet_accelerated" else "succeeded"
        return raw_record(run, status=status)

    with pytest.raises(CampaignError, match="stopped"):
        runner().run(manifest(), failed_root, fail_comet_correctness)
    with pytest.raises(CampaignError, match="failed correctness"):
        runner().run(
            manifest(),
            failed_root,
            fail_comet_correctness,
            continue_on_failure=True,
            expected_provenance=current_provenance,
        )


def test_campaign_requires_current_provenance_to_resume(tmp_path: Path) -> None:
    first = plan_campaign(manifest())[0]
    path = first.raw_path(tmp_path)
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(raw_record(first)), encoding="utf-8")
    calls = []

    def execute(run):
        calls.append(run.run_id)
        return raw_record(run)

    with pytest.raises(CampaignError, match="resume requires current campaign provenance"):
        runner().run(manifest(), tmp_path, execute)
    assert calls == []


def test_campaign_rejects_unplanned_raw_artifacts_before_execution(tmp_path: Path) -> None:
    experiment_root = tmp_path / manifest()["experiment_id"] / "spark_baseline"
    experiment_root.mkdir(parents=True)
    unexpected = experiment_root / "stale-run.json"
    unexpected.write_text("{}", encoding="utf-8")
    calls = []

    def execute(run):
        calls.append(run.run_id)
        return raw_record(run)

    with pytest.raises(CampaignError, match="unexpected artifacts"):
        runner().run(manifest(), tmp_path, execute, expected_provenance=current_provenance)
    assert calls == []
    assert unexpected.read_text(encoding="utf-8") == "{}"


def test_campaign_rejects_incomplete_current_provenance_before_execution(tmp_path: Path) -> None:
    calls = []

    def execute(run):
        calls.append(run.run_id)
        return raw_record(run)

    with pytest.raises(CampaignError, match="current campaign provenance is incomplete"):
        runner().run(
            manifest(),
            tmp_path,
            execute,
            expected_provenance=lambda run: {"git_commit": current_provenance(run)["git_commit"]},
        )
    assert calls == []


@pytest.mark.parametrize(
    ("field", "stale_value"),
    [
        ("git_commit", "1234567"),
        ("container_image_digest", f"sha256:{'b' * 64}"),
        ("dataset_manifest_sha256", "b" * 64),
        ("spark_conf_sha256", "b" * 64),
        ("sql_sha256", "b" * 64),
        ("iceberg_snapshot_ids", [2]),
    ],
)
def test_campaign_preflights_all_resume_provenance_before_execution(
    tmp_path: Path, field: str, stale_value: object
) -> None:
    late_run = plan_campaign(manifest())[-1]
    path = late_run.raw_path(tmp_path)
    path.parent.mkdir(parents=True)
    stale = raw_record(late_run)
    stale["provenance"][field] = stale_value
    path.write_text(json.dumps(stale), encoding="utf-8")
    calls = []

    def execute(run):
        calls.append(run.run_id)
        return raw_record(run)

    with pytest.raises(CampaignError, match=field):
        runner().run(manifest(), tmp_path, execute, expected_provenance=current_provenance)
    assert calls == []
    assert list(tmp_path.rglob("*.json")) == [path]


def test_campaign_rejects_new_record_with_wrong_current_provenance(tmp_path: Path) -> None:
    def execute(run):
        record = raw_record(run)
        record["provenance"]["git_commit"] = "1234567"
        return record

    with pytest.raises(CampaignError, match="git_commit"):
        runner().run(manifest(), tmp_path, execute, expected_provenance=current_provenance)
    assert not list(tmp_path.rglob("*.json"))


def test_campaign_preflights_resume_resource_identity_before_execution(tmp_path: Path) -> None:
    late_run = plan_campaign(manifest())[-1]
    path = late_run.raw_path(tmp_path)
    path.parent.mkdir(parents=True)
    stale = raw_record(late_run)
    stale["resources"]["cpu_model"] = "different-host"
    path.write_text(json.dumps(stale), encoding="utf-8")
    calls = []

    def execute(run):
        calls.append(run.run_id)
        return raw_record(run)

    with pytest.raises(CampaignError, match="resource identity"):
        runner().run(manifest(), tmp_path, execute, expected_provenance=current_provenance)
    assert calls == []


def test_campaign_blocks_correctness_mismatch_before_measurement(tmp_path: Path) -> None:
    def mismatch(run):
        record = raw_record(run)
        if run.run_id == "correctness-comet_accelerated":
            record["correctness"]["canonical_result_sha256"] = "b" * 64
        return record

    with pytest.raises(CampaignError, match="correctness mismatch"):
        runner().run(manifest(), tmp_path, mismatch)
    assert len(list(tmp_path.rglob("*.json"))) == 4


@pytest.mark.parametrize("engine", ["spark_baseline", "comet_accelerated"])
def test_campaign_blocks_partial_plan_analysis_before_measurement(
    tmp_path: Path, engine: str
) -> None:
    def partial_plan(run):
        record = raw_record(run)
        if run.phase == "plan_capture" and run.engine == engine:
            record["plan_analysis"]["status"] = "partial"
            record["plan_analysis"]["unknown_nodes"] = ["UnknownExec"]
        return record

    with pytest.raises(CampaignError, match="incomplete plan analysis"):
        runner().run(manifest(), tmp_path, partial_plan)
    assert len(list(tmp_path.rglob("*.json"))) == 4


def test_subprocess_timeout_and_immutable_logs(tmp_path: Path) -> None:
    stdout_path = tmp_path / "stdout.log"
    stderr_path = tmp_path / "stderr.log"
    outcome = run_subprocess(
        [sys.executable, "-c", "import time; time.sleep(2)"],
        timeout_seconds=0.05,
        termination_grace_seconds=0.05,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
    )
    assert outcome.timed_out
    assert outcome.return_code is not None
    with pytest.raises(FileExistsError):
        run_subprocess(
            [sys.executable, "-c", "pass"],
            timeout_seconds=1,
            stdout_path=stdout_path,
            stderr_path=tmp_path / "other-stderr.log",
        )
