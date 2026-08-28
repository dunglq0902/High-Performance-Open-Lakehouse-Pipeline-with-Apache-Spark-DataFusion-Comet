import json
from pathlib import Path

import pytest
import yaml

from benchmark.runner.campaign import CampaignRun
from benchmark.runner.canonical import sha256_file
from scripts.run_research_campaign import (
    CALIBRATION_SCRIPT_PATH,
    COLLECTOR_PATH,
    ROOT,
    _container_path,
    _find_event_log,
    _resource_summary,
    _runtime_from_lock,
    _service_decision,
    _spark_submit_command,
    _validate_collector_calibration,
    _write_admission_artifact,
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
    assert _write_admission_artifact(output, {"passed": False}) == output
    retry = _write_admission_artifact(output, {"passed": True})
    assert retry.name == "capacity-gate-attempt-0002.json"
    assert output.read_text(encoding="utf-8") == '{"passed":false}\n'
    assert retry.read_text(encoding="utf-8") == '{"passed":true}\n'

    assert _write_admission_artifact(output, {"passed": True}) == retry
