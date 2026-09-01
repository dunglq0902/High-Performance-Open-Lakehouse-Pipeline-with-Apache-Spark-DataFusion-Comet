import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from benchmark.runner.campaign import CampaignError, CampaignReport, CampaignRun
from benchmark.runner.canonical import sha256_file, sha256_value
from scripts.run_research_campaign import (
    ATTEMPT_ADMISSION_FILENAME,
    CALIBRATION_SCRIPT_PATH,
    COLLECTOR_PATH,
    ROOT,
    DockerCampaignExecutor,
    _capacity_gate,
    _collector_calibration,
    _container_path,
    _find_event_log,
    _make_event_logs_host_readable,
    _next_run_attempt_dir,
    _online_admission_failure,
    _prepare_medallion,
    _resource_summary,
    _runtime_from_lock,
    _service_decision,
    _spark_submit_command,
    _start_worker_sampler,
    _storage_identity,
    _validate_collector_calibration,
    _write_attempt_artifact,
    _write_campaign_verification,
)
from scripts.run_research_campaign import (
    main as campaign_main,
)


def test_runtime_failure_fingerprint_comes_from_exact_lock() -> None:
    baseline = _runtime_from_lock("spark_baseline")
    comet = _runtime_from_lock("comet_accelerated")
    assert baseline["spark_version"] == "4.1.3"
    assert baseline["comet_version"] is None
    assert comet["comet_version"] == "1.0.0"


def test_research_campaign_cli_requires_dataset_attestation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_research_campaign.py", "--config", "benchmark/configs/benchmark-laptop-m02.yaml"],
    )

    with pytest.raises(SystemExit) as error:
        campaign_main()

    assert error.value.code == 2


def test_container_path_rejects_paths_outside_repository(tmp_path: Path) -> None:
    assert _container_path(ROOT / "benchmark") == "/opt/lakehouse/benchmark"
    with pytest.raises(ValueError, match="leaves repository"):
        _container_path(tmp_path)


def test_event_log_lookup_is_unique(tmp_path: Path) -> None:
    event = tmp_path / "app-123"
    event.write_text("{}\n", encoding="utf-8")
    assert _find_event_log("app-123", tmp_path) == event.resolve()
    (tmp_path / "eventlog_v2_app-123").mkdir()
    with pytest.raises(ValueError, match="expected one event log"):
        _find_event_log("app-123", tmp_path)


def test_event_log_lookup_accepts_spark_v2_directory(tmp_path: Path) -> None:
    event = tmp_path / "eventlog_v2_app-123"
    event.mkdir()
    (event / "events_1_app-123").write_text("{}\n", encoding="utf-8")

    assert _find_event_log("app-123", tmp_path) == event.resolve()


def test_event_log_permissions_are_fixed_inside_running_container(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def fake_run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr("scripts.run_research_campaign._run", fake_run)
    _make_event_logs_host_readable()

    assert calls == [
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "--user",
            "0",
            "spark-master",
            "chmod",
            "-R",
            "a+rX",
            _container_path(ROOT / ".artifacts/spark-events"),
        ]
    ]


def test_event_log_permission_failure_is_a_hard_collection_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr("scripts.run_research_campaign._run", fail_run)
    with pytest.raises(CampaignError, match="host-readable"):
        _make_event_logs_host_readable()


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
        cpu_model="test-cpu",
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


def test_sampler_readiness_failure_escalates_terminate_to_kill_and_closes_logs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class StubbornSampler:
        returncode = None

        def __init__(self, *, stdout: object, stderr: object) -> None:
            self.stdout = stdout
            self.stderr = stderr
            self.terminated = False
            self.killed = False
            self.waits = 0

        def poll(self) -> None:
            return None

        def terminate(self) -> None:
            self.terminated = True

        def kill(self) -> None:
            self.killed = True

        def wait(self, timeout: float) -> int:
            self.waits += 1
            if not self.killed:
                raise subprocess.TimeoutExpired("sampler", timeout)
            self.returncode = -9
            return self.returncode

    observed: list[StubbornSampler] = []

    def fake_popen(*_args: object, **kwargs: object) -> StubbornSampler:
        process = StubbornSampler(stdout=kwargs["stdout"], stderr=kwargs["stderr"])
        observed.append(process)
        return process

    def fail_readiness(*_args: object, **_kwargs: object) -> None:
        raise CampaignError("synthetic readiness failure")

    monkeypatch.setattr("scripts.run_research_campaign._container_path", lambda path: str(path))
    monkeypatch.setattr("scripts.run_research_campaign.subprocess.Popen", fake_popen)
    monkeypatch.setattr("scripts.run_research_campaign._wait_for_file", fail_readiness)

    with pytest.raises(CampaignError, match="synthetic readiness failure"):
        _start_worker_sampler(tmp_path, 30)

    process = observed[0]
    assert process.terminated is True
    assert process.killed is True
    assert process.waits == 2
    assert process.stdout.closed is True
    assert process.stderr.closed is True


def test_online_measurement_admission_requires_complete_plan_and_collectors() -> None:
    run = CampaignRun(
        experiment_id="EXP-ECOM-SMALL-M02",
        run_id="measurement-p0001-o1-spark_baseline",
        phase="measurement",
        engine="spark_baseline",
        pair_index=1,
        order_index=1,
        warmup_runs=2,
        timeout_seconds=1800,
    )
    record = {
        "plan_analysis": {"status": "complete"},
        "metrics": {"collector_status": "complete"},
    }
    assert _online_admission_failure(run, record) is None

    record["metrics"] = {"collector_status": "partial"}
    assert _online_admission_failure(run, record)[:2] == (
        "invalid_environment",
        "IncompleteResourceEvidence",
    )
    record["plan_analysis"] = {"status": "partial"}
    assert _online_admission_failure(run, record)[:2] == (
        "invalid_result",
        "IncompletePlanEvidence",
    )


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


def test_collector_calibration_reuses_latest_successful_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    base = tmp_path / "collector-calibration.json"
    base.write_text('{"status":"failed"}\n', encoding="utf-8")
    latest = tmp_path / "collector-calibration-attempt-0002.json"
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
    latest.write_text(json.dumps(artifact), encoding="utf-8")

    def unexpected_run(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("successful calibration retry must be reused")

    monkeypatch.setattr("scripts.run_research_campaign._run", unexpected_run)
    value, selected = _collector_calibration(base)

    assert value == artifact
    assert selected == latest


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


def test_storage_identity_binds_named_volume_creation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    containers = {"iceberg-rest": "iceberg-container", "minio": "minio-container"}
    volumes = {
        "iceberg-container": ("iceberg-volume", "/var/lib/iceberg"),
        "minio-container": ("minio-volume", "/data"),
    }

    def fake_run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        if command[:5] == ["docker", "compose", "ps", "--all", "-q"]:
            stdout = containers[command[5]] + "\n"
        elif command[:2] == ["docker", "inspect"]:
            name, destination = volumes[command[2]]
            stdout = json.dumps(
                [{"Mounts": [{"Type": "volume", "Name": name, "Destination": destination}]}]
            )
        elif command[:3] == ["docker", "volume", "inspect"]:
            stdout = json.dumps(
                [
                    {
                        "Name": command[3],
                        "CreatedAt": "2026-08-31T00:00:00Z",
                        "Driver": "local",
                        "Scope": "local",
                    }
                ]
            )
        else:  # pragma: no cover - protects the command contract
            raise AssertionError(command)
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    monkeypatch.setattr("scripts.run_research_campaign._run", fake_run)
    identity = _storage_identity()

    assert len(identity) == 64
    assert identity == _storage_identity()


def test_storage_identity_rejects_missing_named_mount(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        stdout = "container\n" if command[1:3] == ["compose", "ps"] else '[{"Mounts": []}]'
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    monkeypatch.setattr("scripts.run_research_campaign._run", fake_run)
    with pytest.raises(CampaignError, match="expected one named volume"):
        _storage_identity()


def test_admission_artifacts_are_immutable_and_retryable(tmp_path: Path) -> None:
    output = tmp_path / "capacity-gate.json"
    assert _write_attempt_artifact(output, {"passed": False}) == output
    retry = _write_attempt_artifact(output, {"passed": True})
    assert retry.name == "capacity-gate-attempt-0002.json"
    assert output.read_text(encoding="utf-8") == '{"passed":false}\n'
    assert retry.read_text(encoding="utf-8") == '{"passed":true}\n'

    assert _write_attempt_artifact(output, {"passed": True}) == retry


def test_capacity_gate_reuses_current_passed_evidence_on_resume(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mebibyte = 1024**2
    gibibyte = 1024**3
    config = {
        "_config_relative_path": "config.json",
        "experiment": {
            "id": "EXP-CAPACITY",
            "measurement_runs": 10,
            "labels": ["primary", "laptop"],
        },
        "workload": {"suite": "micro", "dataset_manifest": "manifest.json"},
        "spark": {
            "runtime_profile": "benchmark-laptop",
            "common_conf": {
                "spark.master": "spark://spark-master:7077",
                "spark.executor.instances": 1,
                "spark.executor.cores": 2,
                "spark.executor.memory": "2g",
                "spark.executor.memoryOverhead": "1g",
                "spark.driver.memory": "1g",
                "spark.memory.offHeap.enabled": True,
                "spark.memory.offHeap.size": "1g",
                "spark.sql.shuffle.partitions": 16,
            },
        },
        "capacity": {
            "estimated_largest_shuffle_bytes": 100 * mebibyte,
            "safety_margin_ratio": 0.2,
        },
    }

    def table(name: str, size: int) -> dict[str, object]:
        return {
            "file_count": 1,
            "total_bytes": size,
            "files": [{"path": f"{name}/part-00000.parquet", "size_bytes": size}],
        }

    manifest = {
        "benchmark_eligible": True,
        "scale_profile": "small",
        "generator": {
            "name": "data.generator",
            "version": "1.0.0",
            "git_commit": "a" * 40,
            "worktree_dirty": False,
        },
        "profile": {"profile_id": "small", "benchmark_eligible": True},
        "tables": {
            "customers": table("customers", mebibyte),
            "orders": table("orders", 9 * mebibyte),
            "order_items": table("order_items", 9 * mebibyte),
            "events": table("events", 9 * mebibyte),
        },
    }
    config_path = tmp_path / "config.json"
    manifest_path = tmp_path / "manifest.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    (tmp_path / ".runtime").mkdir()
    environment = {
        "git_commit": "b" * 40,
        "container_image_digest": f"sha256:{'c' * 64}",
        "storage_identity_sha256": "d" * 64,
        "cpu_model": "test-cpu",
    }
    monkeypatch.setattr("scripts.run_research_campaign.ROOT", tmp_path)
    monkeypatch.setattr(
        "scripts.run_research_campaign._worker_limits", lambda: (5 * gibibyte, 2.0, 0)
    )
    monkeypatch.setattr(
        "scripts.run_research_campaign.shutil.disk_usage",
        lambda _path: SimpleNamespace(total=20 * gibibyte, used=10 * gibibyte, free=10 * gibibyte),
    )
    output = tmp_path / "capacity-gate.json"

    first = _capacity_gate(config, manifest, output, environment=environment)
    first_bytes = first.read_bytes()

    def unexpected_observation() -> object:
        raise AssertionError("a current passed capacity decision must be reused")

    monkeypatch.setattr("scripts.run_research_campaign._worker_limits", unexpected_observation)
    selected = _capacity_gate(config, manifest, output, environment=environment)

    assert selected == first
    assert selected.read_bytes() == first_bytes
    assert sorted(output.parent.glob("capacity-gate*.json")) == [output]

    config_path.write_text("{}\n", encoding="utf-8")
    with pytest.raises(CampaignError, match="current capacity inputs and semantics"):
        _capacity_gate(config, manifest, output, environment=environment)
    assert sorted(output.parent.glob("capacity-gate*.json")) == [output]


def test_run_attempt_directories_are_immutable_and_monotonic(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("scripts.run_research_campaign._container_path", lambda path: str(path))
    artifact_root = tmp_path / "artifacts"
    run = CampaignRun(
        experiment_id="EXP-TEST",
        run_id="correctness-spark_baseline",
        phase="correctness",
        engine="spark_baseline",
        pair_index=None,
        order_index=1,
        warmup_runs=0,
        timeout_seconds=30,
    )
    provenance = {"git_commit": "a" * 40}
    runtime = {"spark_version": "test"}
    first = _next_run_attempt_dir(artifact_root, run, provenance=provenance, runtime=runtime)
    second = _next_run_attempt_dir(artifact_root, run, provenance=provenance, runtime=runtime)

    assert first.name == "attempt-0001"
    assert second.name == "attempt-0002"
    admission = json.loads((first / ATTEMPT_ADMISSION_FILENAME).read_text(encoding="utf-8"))
    declared_hash = admission.pop("artifact_sha256")
    assert declared_hash == sha256_value(admission)
    with pytest.raises(CampaignError, match="unsafe run ID"):
        _next_run_attempt_dir(
            artifact_root,
            CampaignRun(
                experiment_id="EXP-TEST",
                run_id="../escape",
                phase="correctness",
                engine="spark_baseline",
                pair_index=None,
                order_index=1,
                warmup_runs=0,
                timeout_seconds=30,
            ),
            provenance=provenance,
            runtime=runtime,
        )


def test_docker_executor_recovers_one_admitted_orphan_without_deleting_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("scripts.run_research_campaign._container_path", lambda path: str(path))
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
        json.dumps({"snapshots": {"bronze.orders": {"snapshot_id": 7}}}),
        encoding="utf-8",
    )
    executor = DockerCampaignExecutor(
        config=config,
        manifest=manifest,
        medallion_path=medallion_path,
        artifact_root=tmp_path / "campaign",
        git_commit="c" * 40,
        spark_image_id=f"sha256:{'d' * 64}",
        cpu_model="test-cpu",
        secrets=(),
    )
    run = CampaignRun(
        experiment_id=config["experiment"]["id"],
        run_id="correctness-spark_baseline",
        phase="correctness",
        engine="spark_baseline",
        pair_index=None,
        order_index=1,
        warmup_runs=0,
        timeout_seconds=30,
    )
    first = _next_run_attempt_dir(
        executor.artifact_root,
        run,
        provenance=executor.expected_provenance(run),
        runtime=_runtime_from_lock(run.engine),
    )
    admission_bytes = (first / ATTEMPT_ADMISSION_FILENAME).read_bytes()
    failure_root = tmp_path / "failed-attempts"
    raw_path = tmp_path / "raw" / "result.json"

    executor.recover_interrupted_attempt(
        run,
        raw_path=raw_path,
        failure_root=failure_root,
    )
    failure_path = next(failure_root.rglob("*.json"))
    failure_bytes = failure_path.read_bytes()
    failure = json.loads(failure_bytes)

    assert failure["status"] == "failed"
    assert failure["failure"]["class"] == "InterruptedAttempt"
    assert (first / ATTEMPT_ADMISSION_FILENAME).read_bytes() == admission_bytes

    executor.recover_interrupted_attempt(
        run,
        raw_path=raw_path,
        failure_root=failure_root,
    )
    assert failure_path.read_bytes() == failure_bytes

    _next_run_attempt_dir(
        executor.artifact_root,
        run,
        provenance=executor.expected_provenance(run),
        runtime=_runtime_from_lock(run.engine),
    )
    raw_path.parent.mkdir(parents=True)
    raw_path.write_text("{}\n", encoding="utf-8")
    executor.recover_interrupted_attempt(
        run,
        raw_path=raw_path,
        failure_root=failure_root,
    )
    assert list(failure_root.rglob("*.json")) == [failure_path]


def test_existing_medallion_must_bind_current_dataset_and_attestation(
    tmp_path: Path,
) -> None:
    config = yaml.safe_load(
        (ROOT / "benchmark/configs/benchmark-laptop-m02.yaml").read_text(encoding="utf-8")
    )
    dataset_manifest = tmp_path / "manifest.json"
    dataset_manifest.write_text('{"dataset_id":"fixture"}\n', encoding="utf-8")
    config["workload"]["dataset_manifest"] = str(dataset_manifest)
    attestation = tmp_path / "attestation.json"
    attestation.write_text("{}\n", encoding="utf-8")
    output = tmp_path / "medallion.json"
    output.write_text(
        json.dumps(
            {
                "status": "passed",
                "dataset_manifest_sha256": sha256_file(dataset_manifest),
                "dataset_validation_attestation_sha256": sha256_file(attestation),
            }
        ),
        encoding="utf-8",
    )

    _prepare_medallion(
        config,
        output,
        dataset_attestation=attestation,
        expected_git_commit="a" * 40,
    )
    stale = json.loads(output.read_text(encoding="utf-8"))
    stale["dataset_manifest_sha256"] = "0" * 64
    output.write_text(json.dumps(stale), encoding="utf-8")
    with pytest.raises(CampaignError, match="identity differs"):
        _prepare_medallion(
            config,
            output,
            dataset_attestation=attestation,
            expected_git_commit="a" * 40,
        )


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
    run_attempts = tmp_path / "runs"
    failed_attempts = tmp_path / "failed-attempts"
    failed_attempts.mkdir()
    for index in range(24):
        (run_attempts / f"run-{index:02d}" / "attempt-0001").mkdir(parents=True)
    control_targets = {
        "run-attempts": run_attempts,
        "failed-attempt-records": failed_attempts,
    }

    assert (
        _write_campaign_verification(
            output,
            first,
            raw_records,
            manifest,
            control_targets=control_targets,
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
        control_targets=control_targets,
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
            control_targets=control_targets,
            repository_root=tmp_path,
        )
        == retry
    )
    assert sorted(path.name for path in tmp_path.iterdir()) == [
        "campaign-verification-attempt-0002.json",
        "campaign-verification.json",
        "evidence.txt",
        "failed-attempts",
        "runs",
    ]
