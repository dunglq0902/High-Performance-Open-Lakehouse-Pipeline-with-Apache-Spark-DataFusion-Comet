from __future__ import annotations

import json
from pathlib import Path

import pytest

import analysis.report_publishability as publishability
from analysis.report_publishability import (
    EXPECTED_CAMPAIGN_RUNS,
    EXPECTED_CORE_CAMPAIGNS,
    assess_report_publishability,
    core_experiments,
)
from benchmark.runner.canonical import sha256_file, sha256_value
from benchmark.runner.evidence import artifact_evidence

HASH = "a" * 64
TEST_COMMIT = "b" * 40
ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_PATH = "tests/test_report_publishability.py"


def _current_hashes(experiment: publishability.CoreExperiment) -> dict[str, str]:
    return {
        "experiment_config_sha256": sha256_file(ROOT / experiment.config),
        "runtime_lock_sha256": "1" * 64,
        "workload_sql_sha256": "d" * 64,
        "workload_manifest_sha256": "2" * 64,
        "dataset_manifest_sha256": HASH,
        "uv_lock_sha256": "3" * 64,
        "spark_defaults_sha256": "4" * 64,
        "comet_profile_sha256": "5" * 64,
    }


@pytest.fixture(autouse=True)
def _stable_current_repository(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(publishability, "clean_git_commit", lambda _root: TEST_COMMIT)
    monkeypatch.setattr(
        publishability,
        "_current_input_hashes",
        lambda experiment, _root: _current_hashes(experiment),
    )


def _measurement_records() -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for experiment in core_experiments():
        for pair_index in range(1, 11):
            for order_index, engine in enumerate(("spark_baseline", "comet_accelerated"), 1):
                native = engine == "comet_accelerated"
                records.append(
                    {
                        "experiment_id": experiment.experiment_id,
                        "run_id": (f"measurement-p{pair_index:04d}-o{order_index}-{engine}"),
                        "phase": "measurement",
                        "pair_index": pair_index,
                        "engine": engine,
                        "status": "succeeded",
                        "workload": experiment.workload,
                        "query_id": experiment.query_id,
                        "storage_profile": experiment.storage_profile,
                        "provenance": {
                            "git_commit": TEST_COMMIT,
                            "container_image_digest": f"sha256:{HASH}",
                            "dataset_manifest_sha256": HASH,
                            "spark_conf_sha256": ("b" if native else "c") * 64,
                            "sql_sha256": "d" * 64,
                            "iceberg_snapshot_ids": [1],
                        },
                        "resources": {
                            "cpu_model": "test-cpu",
                            "allocated_cores": 2,
                            "cgroup_memory_limit_mib": 5120,
                            "executor_heap_mib": 2048,
                            "off_heap_mib": 1024,
                        },
                        "metrics": {
                            "query_wall_time_ms": 10.0,
                            "cpu_core_seconds": 1.0,
                            "cgroup_memory_peak_mib": 100.0,
                            "jvm_gc_time_ms": 1.0,
                            "shuffle_read_mb": 1.0,
                            "shuffle_write_mb": 1.0,
                            "disk_spill_mb": 1.0,
                            "collector_status": "complete",
                        },
                        "plan_analysis": {
                            "status": "complete",
                            "comet_native_operators": 1 if native else 0,
                        },
                        "correctness": {
                            "schema_sha256": HASH,
                            "row_count": 1,
                            "canonical_result_sha256": HASH,
                        },
                        "artifacts": {
                            field: ARTIFACT_PATH
                            for field in (
                                "event_log",
                                "physical_plan",
                                "resource_samples",
                                "stdout",
                                "stderr",
                            )
                        },
                    }
                )
        for phase, prefix in (("correctness", "correctness"), ("plan_capture", "plan")):
            for engine in ("spark_baseline", "comet_accelerated"):
                native = engine == "comet_accelerated"
                records.append(
                    {
                        "experiment_id": experiment.experiment_id,
                        "run_id": f"{prefix}-{engine}",
                        "phase": phase,
                        "pair_index": None,
                        "engine": engine,
                        "status": "succeeded",
                        "workload": experiment.workload,
                        "query_id": experiment.query_id,
                        "storage_profile": experiment.storage_profile,
                        "provenance": {
                            "git_commit": TEST_COMMIT,
                            "container_image_digest": f"sha256:{HASH}",
                            "dataset_manifest_sha256": HASH,
                            "spark_conf_sha256": ("b" if native else "c") * 64,
                            "sql_sha256": "d" * 64,
                            "iceberg_snapshot_ids": [1],
                        },
                        "resources": {
                            "cpu_model": "test-cpu",
                            "allocated_cores": 2,
                            "cgroup_memory_limit_mib": 5120,
                            "executor_heap_mib": 2048,
                            "off_heap_mib": 1024,
                        },
                        "metrics": {"collector_status": "complete"},
                        "plan_analysis": {
                            "status": "complete",
                            "comet_native_operators": 1 if native else 0,
                        },
                        "correctness": {
                            "schema_sha256": HASH,
                            "row_count": 1,
                            "canonical_result_sha256": HASH,
                        },
                        "artifacts": {
                            field: ARTIFACT_PATH
                            for field in (
                                "event_log",
                                "physical_plan",
                                "resource_samples",
                                "stdout",
                                "stderr",
                            )
                        },
                    }
                )
    return records


def _experiment_manifest(experiment: publishability.CoreExperiment) -> dict[str, object]:
    manifest: dict[str, object] = {
        "schema_version": 1,
        "experiment_id": experiment.experiment_id,
        "resolved_config": {
            "experiment": {"timeout_seconds": 300, "warmup_runs": 2},
            "matrix": {
                "engines": [
                    {"name": "spark_baseline"},
                    {"name": "comet_accelerated"},
                ]
            },
        },
        "schedule": [
            {
                "pair_index": pair_index,
                "order": ["spark_baseline", "comet_accelerated"],
            }
            for pair_index in range(1, 11)
        ],
        "input_hashes": _current_hashes(experiment),
    }
    manifest["manifest_sha256"] = sha256_value(manifest)
    return manifest


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


def test_exact_core_suite_is_publishable(tmp_path: Path) -> None:
    campaign_root = tmp_path / "campaigns"
    _write_verifications(campaign_root)

    evidence = assess_report_publishability(_measurement_records(), campaign_root)

    assert len(core_experiments()) == EXPECTED_CORE_CAMPAIGNS == 10
    assert evidence["status"] == "passed"
    assert evidence["publishable"] is True
    assert evidence["checks"]["exact_core_experiment_set"]["passed"] is True
    assert all(check["passed"] for check in evidence["checks"]["measurements"])
    assert all(check["passed"] for check in evidence["checks"]["campaign_records"])
    assert all(check["passed"] for check in evidence["checks"]["campaign_verifications"])


def test_core_catalog_rejects_any_count_other_than_ten(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(publishability, "CORE_CONFIGS", publishability.CORE_CONFIGS[:-1])

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
    payload = records[0][section]
    assert isinstance(payload, dict)
    payload[field] = invalid

    evidence = assess_report_publishability(records, campaign_root)

    assert evidence["publishable"] is False
    assert any(expected_issue in issue for issue in evidence["issues"])


def test_resource_metric_exclusion_blocks_publication(tmp_path: Path) -> None:
    campaign_root = tmp_path / "campaigns"
    _write_verifications(campaign_root)
    records = _measurement_records()
    metrics = records[0]["metrics"]
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
    records[0]["status"] = "failed"

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
        raise publishability.RepositoryEvidenceError("repository worktree is not clean")

    monkeypatch.setattr(publishability, "clean_git_commit", fail_dirty)

    evidence = assess_report_publishability(records, campaign_root)

    assert evidence["publishable"] is False
    check = evidence["checks"]["repository_provenance"]
    assert check["passed"] is False
    assert check["current_git_commit"] is None
    assert "repository worktree is not clean" in check["issues"]
