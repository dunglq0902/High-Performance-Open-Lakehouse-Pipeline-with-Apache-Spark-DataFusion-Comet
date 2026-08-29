import json
from pathlib import Path

import pytest
import yaml

from benchmark.runner.campaign import CampaignError, CampaignReport, CampaignRun
from benchmark.runner.canonical import sha256_file, sha256_value
from scripts.run_research_campaign import (
    CALIBRATION_SCRIPT_PATH,
    COLLECTOR_PATH,
    ROOT,
    DockerCampaignExecutor,
    _container_path,
    _find_event_log,
    _resource_summary,
    _runtime_from_lock,
    _service_decision,
    _spark_submit_command,
    _validate_collector_calibration,
    _write_attempt_artifact,
    _write_campaign_verification,
)


def test_runtime_failure_fingerprint_comes_from_exact_lock() -> None:
    baseline = _runtime_from_lock("spark_baseline")
    comet = _runtime_from_lock("comet_accelerated")
    assert baseline["spark_version"] == "4.1.3"
    assert baseline["comet_version"] is None
    assert comet["comet_version"] == "1.0.0"


def test_container_path_rejects_paths_outside_repository(tmp_path: Path) -> None:
    assert _container_path(ROOT / "benchmark") == "/opt/lakehouse/benchmark"
    with pytest.raises(ValueError, match="leaves repository"):
        _container_path(tmp_path)


def test_event_log_lookup_is_unique(tmp_path: Path) -> None:
    event = tmp_path / "app-123"
    event.write_text("{}\n", encoding="utf-8")
    assert _find_event_log("app-123", tmp_path) == event.resolve()
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested/app-123").write_text("{}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="expected one event log"):
        _find_event_log("app-123", tmp_path)


def test_submit_command_uses_locked_profile_and_only_comet_deltas(tmp_path: Path) -> None:
    config = yaml.safe_load(
        (ROOT / "benchmark/configs/benchmark-laptop-m02.yaml").read_text(encoding="utf-8")
    )
    config["_config_relative_path"] = "benchmark/configs/benchmark-laptop-m02.yaml"
    run = CampaignRun(
        experiment_id="EXP-ECOM-SMALL-M02",
        run_id="measurement-p0001-o1-comet_accelerated",
        phase="measurement",
        engine="comet_accelerated",
        pair_index=1,
        order_index=1,
        warmup_runs=2,
        timeout_seconds=1800,
    )
    medallion = ROOT / ".artifacts/test/medallion.json"
    output = ROOT / ".artifacts/test/application.json"
    command = _spark_submit_command(config, run, medallion, output)
    assert "--properties-file" in command
    assert any(value == "spark.comet.enabled=true" for value in command)
    assert "--pair-index" in command
    assert "--worker-sampler-start-file" in command
    assert "--worker-sampler-started-file" in command
    assert "--worker-sampler-stop-file" in command
    assert "spark.executor.memory=3g" not in command


def test_campaign_executor_binds_resume_to_current_raw_provenance(tmp_path: Path) -> None:
    config = yaml.safe_load(
        (ROOT / "benchmark/configs/benchmark-laptop-m02.yaml").read_text(encoding="utf-8")
    )
    manifest = {
        "input_hashes": {
            "dataset_manifest_sha256": "a" * 64,
            "workload_sql_sha256": "b" * 64,
        }
    }
    medallion_path = tmp_path / "medallion.json"
    medallion_path.write_text(
        json.dumps(
            {
                "snapshots": {
                    "bronze.orders": {"snapshot_id": 7},
                    "bronze.customers": {"snapshot_id": 3},
                    "gold.unrelated": {"snapshot_id": 11},
                }
            }
        ),
        encoding="utf-8",
    )
    executor = DockerCampaignExecutor(
        config=config,
        manifest=manifest,
        medallion_path=medallion_path,
        artifact_root=tmp_path,
        git_commit="c" * 40,
        spark_image_id=f"sha256:{'d' * 64}",
        secrets=(),
    )
    run = CampaignRun(
        experiment_id="EXP-ECOM-SMALL-M02",
        run_id="correctness-comet_accelerated",
        phase="correctness",
        engine="comet_accelerated",
        pair_index=None,
        order_index=2,
        warmup_runs=0,
        timeout_seconds=1800,
    )
    comet_conf = config["matrix"]["engines"][1]["spark_conf"]

    assert executor.expected_provenance(run) == {
        "git_commit": "c" * 40,
        "container_image_digest": f"sha256:{'d' * 64}",
        "dataset_manifest_sha256": "a" * 64,
        "spark_conf_sha256": sha256_value(
            {"common": config["spark"]["common_conf"], "engine": comet_conf}
        ),
        "sql_sha256": "b" * 64,
        "iceberg_snapshot_ids": [7],
        "resources": {
            "cpu_model": executor.cpu_model,
            "allocated_cores": 2,
            "cgroup_memory_limit_mib": 5120,
            "executor_heap_mib": 2048,
            "off_heap_mib": 1024,
        },
    }

    del executor.medallion["snapshots"]["bronze.orders"]
    with pytest.raises(CampaignError, match="no pinned snapshot for bronze.orders"):
        executor.expected_provenance(run)


def test_worker_resource_summary_requires_a_completed_measurement_window(tmp_path: Path) -> None:
    artifact = tmp_path / "resources.json"
    payload = {
        "timed_out": False,
        "window_started": True,
        "window_completed": True,
        "aborted": False,
        "summary": {"status": "complete", "memory_peak_bytes": 123},
    }
    artifact.write_text(json.dumps(payload), encoding="utf-8")
    assert _resource_summary(artifact) == payload["summary"]

    payload["aborted"] = True
    artifact.write_text(json.dumps(payload), encoding="utf-8")
    assert _resource_summary(artifact) is None


def test_collector_calibration_gate_is_hash_bound_and_fail_closed() -> None:
    artifact = {
        "artifact_class": "resource-collector-calibration-v1",
        "status": "passed",
        "collector": {
            "sample_interval_seconds": 0.2,
            "module_sha256": sha256_file(COLLECTOR_PATH),
            "script_sha256": sha256_file(CALIBRATION_SCRIPT_PATH),
            "observed_sources": ["cgroup_v2"],
            "source_gate_passed": True,
            "status_gate_passed": True,
            "sampling_gate_passed": True,
        },
        "calibration": {"threshold_percent": 2.0, "accepted": True},
    }
    assert _validate_collector_calibration(artifact) == artifact

    artifact["collector"]["module_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="source hash"):
        _validate_collector_calibration(artifact)


@pytest.mark.parametrize(
    ("state", "exit_code", "expected", "decision"),
    [
        ("healthy", 0, "healthy", "ready"),
        ("starting", 0, "healthy", "waiting"),
        ("exited", 0, "completed", "ready"),
        ("running", 0, "completed", "waiting"),
        ("unhealthy", 0, "healthy", "failed"),
        ("exited", 1, "completed", "failed"),
        ("exited", 0, "healthy", "failed"),
    ],
)
def test_compose_service_decision(state: str, exit_code: int, expected: str, decision: str) -> None:
    assert _service_decision(state, exit_code, expected) == decision


def test_admission_artifacts_are_immutable_and_retryable(tmp_path: Path) -> None:
    output = tmp_path / "capacity-gate.json"
    assert _write_attempt_artifact(output, {"passed": False}) == output
    retry = _write_attempt_artifact(output, {"passed": True})
    assert retry.name == "capacity-gate-attempt-0002.json"
    assert output.read_text(encoding="utf-8") == '{"passed":false}\n'
    assert retry.read_text(encoding="utf-8") == '{"passed":true}\n'

    assert _write_attempt_artifact(output, {"passed": True}) == retry


def test_campaign_verification_is_idempotent_across_resume_attempts(tmp_path: Path) -> None:
    output = tmp_path / "campaign-verification.json"
    first = CampaignReport(
        experiment_id="EXP-ECOM-SMALL-M02",
        planned=24,
        executed=24,
        resumed=0,
        succeeded=24,
        failed=0,
    )
    resumed = CampaignReport(
        experiment_id=first.experiment_id,
        planned=24,
        executed=0,
        resumed=24,
        succeeded=24,
        failed=0,
    )
    evidence_file = tmp_path / "evidence.txt"
    evidence_file.write_text("immutable evidence", encoding="utf-8")
    raw_records = tuple(
        {
            "experiment_id": first.experiment_id,
            "run_id": f"run-{index:02d}",
            "status": "succeeded",
            "artifacts": {
                field: evidence_file.name
                for field in (
                    "event_log",
                    "physical_plan",
                    "resource_samples",
                    "stdout",
                    "stderr",
                )
            },
        }
        for index in range(24)
    )
    manifest = {"schema_version": 1, "experiment_id": first.experiment_id}
    manifest["manifest_sha256"] = sha256_value(manifest)

    assert (
        _write_campaign_verification(
            output,
            first,
            raw_records,
            manifest,
            repository_root=tmp_path,
        )
        == output
    )
    original = output.read_bytes()
    retry = _write_campaign_verification(
        output,
        resumed,
        raw_records,
        manifest,
        repository_root=tmp_path,
    )
    assert retry.name == "campaign-verification-attempt-0002.json"
    assert output.read_bytes() == original
    assert (
        _write_campaign_verification(
            output,
            resumed,
            raw_records,
            manifest,
            repository_root=tmp_path,
        )
        == retry
    )
    assert sorted(path.name for path in tmp_path.iterdir()) == [
        "campaign-verification-attempt-0002.json",
        "campaign-verification.json",
        "evidence.txt",
    ]
