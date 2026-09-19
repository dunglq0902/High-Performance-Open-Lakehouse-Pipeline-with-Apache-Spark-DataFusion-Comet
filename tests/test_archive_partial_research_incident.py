from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from benchmark.runner.canonical import sha256_value, write_json
from scripts import archive_partial_research_incident as incident_module
from scripts.archive_partial_research_incident import (
    EvidenceArchiveError,
    IncidentPlan,
    execute_incident_archive,
    plan_incident_archive,
    rollback_incident_staging,
    verify_incident_archive,
)

SOURCE_COMMIT = "a" * 40
EXPERIMENT_IDS = ("EXP-ONE", "EXP-TWO")


def test_make_archive_inputs_are_not_evaluated_as_make_functions(tmp_path: Path) -> None:
    sentinel = tmp_path / "make-expansion-sentinel"
    injected_label = f"$(shell printf unsafe > {sentinel})safe"

    completed = subprocess.run(
        [
            "make",
            "--no-print-directory",
            "archive-partial-research-incident-dry-run",
            "UV=printf",
            f"EVIDENCE_SOURCE_COMMIT={SOURCE_COMMIT}",
            f"EVIDENCE_ARCHIVE_LABEL={injected_label}",
        ],
        cwd=incident_module.ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert not sentinel.exists()


def _write_manifest(campaign_directory: Path, experiment_id: str) -> str:
    campaign_directory.mkdir(parents=True)
    manifest = {
        "schema_version": 1,
        "experiment_id": experiment_id,
        "dataset_validation": {"validator_git_commit": SOURCE_COMMIT},
    }
    manifest["manifest_sha256"] = sha256_value(manifest)
    write_json(campaign_directory / "experiment-manifest.json", manifest)
    return manifest["manifest_sha256"]


def _write_verification(
    path: Path,
    *,
    experiment_id: str,
    manifest_sha256: str,
    raw_record_count: int,
    status: str = "passed",
    executed: int | None = None,
    resumed: int = 0,
) -> None:
    executed_count = raw_record_count if executed is None else executed
    write_json(
        path,
        {
            "schema_version": 1,
            "status": status,
            "report": {
                "experiment_id": experiment_id,
                "planned": raw_record_count,
                "executed": executed_count,
                "resumed": resumed,
                "succeeded": raw_record_count,
                "failed": 0,
                "complete": status == "passed",
                "raw_record_count": raw_record_count,
                "experiment_manifest_sha256": manifest_sha256,
            },
        },
    )


def _write_raw(raw_root: Path, experiment_id: str, run_id: str) -> Path:
    path = raw_root / experiment_id / "spark_baseline" / f"{run_id}.json"
    write_json(
        path,
        {
            "schema_version": 1,
            "experiment_id": experiment_id,
            "run_id": run_id,
            "provenance": {"git_commit": SOURCE_COMMIT},
        },
    )
    return path


def _fixture(repository: Path) -> tuple[Path, Path, Path]:
    raw_root = repository / "results/raw"
    campaign_root = repository / ".artifacts/campaigns"
    archive_root = repository / ".artifacts/research-incident-archives"
    raw_root.mkdir(parents=True)
    campaign_root.mkdir(parents=True)
    (raw_root / ".gitkeep").write_text("", encoding="utf-8")
    reports = repository / "results/reports"
    reports.mkdir()
    (reports / "must-remain.txt").write_text("unrelated", encoding="utf-8")

    _write_raw(raw_root, "EXP-ONE", "run-1")
    (raw_root / "EXP-TWO").mkdir()
    first_campaign = campaign_root / "EXP-ONE"
    first_manifest_sha256 = _write_manifest(first_campaign, "EXP-ONE")
    (first_campaign / "runs/run-1/attempt-0001").mkdir(parents=True)
    (first_campaign / "runs/run-1/attempt-0001/trace.bin").write_bytes(b"trace")
    (first_campaign / "failed-attempts").mkdir()
    _write_verification(
        first_campaign / "campaign-verification.json",
        experiment_id="EXP-ONE",
        manifest_sha256=first_manifest_sha256,
        raw_record_count=1,
    )
    _write_manifest(campaign_root / "EXP-TWO", "EXP-TWO")
    return raw_root, campaign_root, archive_root


def _plan(repository: Path) -> tuple[IncidentPlan, Path, Path, Path]:
    raw_root, campaign_root, archive_root = _fixture(repository)
    plan = plan_incident_archive(
        repository,
        raw_root=raw_root,
        campaign_root=campaign_root,
        archive_root=archive_root,
        source_commit=SOURCE_COMMIT,
        label="docker-pause-q06",
        experiment_ids=EXPERIMENT_IDS,
        now=datetime(2026, 9, 12, 13, 21, 37, tzinfo=UTC),
    )
    return plan, raw_root, campaign_root, archive_root


def _transaction_journal(plan: IncidentPlan) -> dict[str, object]:
    return incident_module._journal_value(
        plan, incident_module._capture_scaffold_identity(plan.staging)
    )


def test_dry_run_binds_verified_and_plan_only_states_without_writing(tmp_path: Path) -> None:
    plan, raw_root, campaign_root, archive_root = _plan(tmp_path)

    assert not archive_root.exists()
    assert [state.campaign_root_state for state in plan.states] == ["verified", "plan-only"]
    assert [state.raw_root_state for state in plan.states] == ["records", "empty"]
    assert plan.states[0].run_attempt_count == 1
    assert len(plan.suite_state_sha256) == 64
    assert (raw_root / ".gitkeep").is_file()
    assert all((campaign_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)


def test_execute_moves_only_present_roots_and_verifies_diagnostic_contract(
    tmp_path: Path,
) -> None:
    plan, raw_root, campaign_root, _archive_root = _plan(tmp_path)

    destination = execute_incident_archive(plan)
    manifest = verify_incident_archive(destination)

    assert manifest["artifact_class"] == incident_module.ARCHIVE_CLASS
    assert manifest["archive_purpose"] == "diagnostic-only"
    assert manifest["publication_eligible"] is False
    assert manifest["inventory_sha256"] == plan.inventory_sha256
    assert manifest["suite_state_sha256"] == plan.suite_state_sha256
    assert (raw_root / ".gitkeep").is_file()
    assert not any((raw_root / experiment_id).exists() for experiment_id in EXPERIMENT_IDS)
    assert not any((campaign_root / experiment_id).exists() for experiment_id in EXPERIMENT_IDS)
    assert (tmp_path / "results/reports/must-remain.txt").read_text() == "unrelated"

    with pytest.raises(EvidenceArchiveError, match="overwrite existing incident archive"):
        execute_incident_archive(plan)


def test_missing_roots_are_hashed_as_state_and_not_materialized(tmp_path: Path) -> None:
    raw_root, campaign_root, archive_root = _fixture(tmp_path)
    raw_two = raw_root / "EXP-TWO"
    raw_two.rmdir()
    campaign_two = campaign_root / "EXP-TWO"
    (campaign_two / "experiment-manifest.json").unlink()
    campaign_two.rmdir()

    plan = plan_incident_archive(
        tmp_path,
        raw_root=raw_root,
        campaign_root=campaign_root,
        archive_root=archive_root,
        source_commit=SOURCE_COMMIT,
        label="missing-second-experiment",
        experiment_ids=EXPERIMENT_IDS,
    )
    destination = execute_incident_archive(plan)
    manifest = verify_incident_archive(destination)

    second = manifest["suite_state"][1]
    assert second["raw_root_state"] == "missing"
    assert second["campaign_root_state"] == "missing"
    assert not (destination / "raw/EXP-TWO").exists()
    assert not (destination / "campaigns/EXP-TWO").exists()


def test_rejects_a_complete_suite_to_preserve_full_archive_semantics(tmp_path: Path) -> None:
    raw_root, campaign_root, archive_root = _fixture(tmp_path)
    _write_raw(raw_root, "EXP-TWO", "run-2")
    manifest = json.loads(
        (campaign_root / "EXP-TWO/experiment-manifest.json").read_text(encoding="utf-8")
    )
    _write_verification(
        campaign_root / "EXP-TWO/campaign-verification.json",
        experiment_id="EXP-TWO",
        manifest_sha256=manifest["manifest_sha256"],
        raw_record_count=1,
    )

    with pytest.raises(EvidenceArchiveError, match="suite is complete"):
        plan_incident_archive(
            tmp_path,
            raw_root=raw_root,
            campaign_root=campaign_root,
            archive_root=archive_root,
            source_commit=SOURCE_COMMIT,
            label="reject-complete",
            experiment_ids=EXPERIMENT_IDS,
        )


def test_latest_resumed_verification_attempt_controls_completeness(tmp_path: Path) -> None:
    raw_root, campaign_root, archive_root = _fixture(tmp_path)
    _write_raw(raw_root, "EXP-TWO", "run-2")
    write_json(campaign_root / "EXP-TWO/campaign-verification.json", {"status": "failed"})
    manifest = json.loads(
        (campaign_root / "EXP-TWO/experiment-manifest.json").read_text(encoding="utf-8")
    )
    _write_verification(
        campaign_root / "EXP-TWO/campaign-verification-attempt-0002.json",
        experiment_id="EXP-TWO",
        manifest_sha256=manifest["manifest_sha256"],
        raw_record_count=1,
        executed=0,
        resumed=1,
    )

    with pytest.raises(EvidenceArchiveError, match="suite is complete"):
        plan_incident_archive(
            tmp_path,
            raw_root=raw_root,
            campaign_root=campaign_root,
            archive_root=archive_root,
            source_commit=SOURCE_COMMIT,
            label="reject-resumed-complete",
            experiment_ids=EXPERIMENT_IDS,
        )


def test_unbound_pass_status_is_not_classified_as_verified(tmp_path: Path) -> None:
    raw_root, campaign_root, archive_root = _fixture(tmp_path)
    write_json(
        campaign_root / "EXP-ONE/campaign-verification.json",
        {"schema_version": 1, "status": "passed"},
        immutable=False,
    )

    plan = plan_incident_archive(
        tmp_path,
        raw_root=raw_root,
        campaign_root=campaign_root,
        archive_root=archive_root,
        source_commit=SOURCE_COMMIT,
        label="unbound-pass",
        experiment_ids=EXPERIMENT_IDS,
    )

    assert plan.states[0].verification_status == "passed"
    assert plan.states[0].campaign_root_state == "started"


@pytest.mark.parametrize(
    ("field_path", "lookalike"),
    (
        (("schema_version",), True),
        (("schema_version",), 1.0),
        (("report", "raw_record_count"), True),
        (("report", "raw_record_count"), 1.0),
    ),
)
def test_numeric_lookalikes_are_not_classified_as_verified(
    tmp_path: Path, field_path: tuple[str, ...], lookalike: object
) -> None:
    raw_root, campaign_root, archive_root = _fixture(tmp_path)
    verification_path = campaign_root / "EXP-ONE/campaign-verification.json"
    verification = json.loads(verification_path.read_text(encoding="utf-8"))
    if len(field_path) == 1:
        verification[field_path[0]] = lookalike
    else:
        verification[field_path[0]][field_path[1]] = lookalike
    write_json(verification_path, verification, immutable=False)

    plan = plan_incident_archive(
        tmp_path,
        raw_root=raw_root,
        campaign_root=campaign_root,
        archive_root=archive_root,
        source_commit=SOURCE_COMMIT,
        label="numeric-lookalike",
        experiment_ids=EXPERIMENT_IDS,
    )

    assert plan.states[0].verification_status == "passed"
    assert plan.states[0].campaign_root_state == "started"


def test_rejects_raw_record_from_another_commit(tmp_path: Path) -> None:
    raw_root, campaign_root, archive_root = _fixture(tmp_path)
    record_path = raw_root / "EXP-ONE/spark_baseline/run-1.json"
    record = json.loads(record_path.read_text(encoding="utf-8"))
    record["provenance"]["git_commit"] = "b" * 40
    write_json(record_path, record, immutable=False)

    with pytest.raises(EvidenceArchiveError, match="raw record is not bound"):
        plan_incident_archive(
            tmp_path,
            raw_root=raw_root,
            campaign_root=campaign_root,
            archive_root=archive_root,
            source_commit=SOURCE_COMMIT,
            label="reject-provenance",
            experiment_ids=EXPERIMENT_IDS,
        )


def test_mutation_after_plan_is_rejected_before_any_move(tmp_path: Path) -> None:
    plan, raw_root, campaign_root, _archive_root = _plan(tmp_path)
    trace = campaign_root / "EXP-ONE/runs/run-1/attempt-0001/trace.bin"
    trace.write_bytes(b"changed")

    with pytest.raises(EvidenceArchiveError, match="partial evidence changed"):
        execute_incident_archive(plan)

    assert all((raw_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert all((campaign_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert not plan.staging.exists()
    assert not plan.destination.exists()


def test_source_root_symlink_swap_is_rejected_before_any_move(tmp_path: Path) -> None:
    plan, raw_root, campaign_root, _archive_root = _plan(tmp_path)
    relocated_raw = tmp_path / "relocated-raw"
    raw_root.rename(relocated_raw)
    raw_root.symlink_to(relocated_raw, target_is_directory=True)

    with pytest.raises(EvidenceArchiveError, match="raw root contains a symlink component"):
        execute_incident_archive(plan)

    assert raw_root.is_symlink()
    assert all((relocated_raw / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert all((campaign_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert not plan.staging.exists()


def test_archive_parent_symlink_swap_is_rejected_before_any_move(tmp_path: Path) -> None:
    plan, raw_root, campaign_root, _archive_root = _plan(tmp_path)
    external = tmp_path / "external-archive"
    external.mkdir()
    plan.staging.parent.parent.mkdir(parents=True)
    plan.staging.parent.symlink_to(external, target_is_directory=True)

    with pytest.raises(EvidenceArchiveError, match="contains a symlink component"):
        execute_incident_archive(plan)

    assert not any(external.iterdir())
    assert all((raw_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert all((campaign_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)


def test_move_failure_rolls_back_all_completed_moves(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, raw_root, campaign_root, _archive_root = _plan(tmp_path)
    original = incident_module._rename_directory
    calls = 0

    def fail_third_move(source: Path, destination: Path, **kwargs: object) -> None:
        nonlocal calls
        calls += 1
        if calls == 3:
            raise OSError("synthetic move failure")
        original(source, destination, **kwargs)

    monkeypatch.setattr(incident_module, "_rename_directory", fail_third_move)
    with pytest.raises(OSError, match="synthetic move failure"):
        execute_incident_archive(plan)

    assert all((raw_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert all((campaign_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert not plan.staging.exists()
    assert not plan.destination.exists()


def test_journal_write_failure_removes_unjournaled_scaffold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, raw_root, campaign_root, _archive_root = _plan(tmp_path)

    def fail_write(_path: Path, _value: object) -> None:
        raise OSError("synthetic journal write failure")

    monkeypatch.setattr(incident_module, "write_json", fail_write)
    with pytest.raises(OSError, match="synthetic journal write failure"):
        execute_incident_archive(plan)

    assert all((raw_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert all((campaign_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert not plan.staging.exists()
    assert not plan.destination.exists()


@pytest.mark.parametrize("cleanup_target", ("raw", "campaigns", "container"))
def test_transient_full_scaffold_cleanup_failure_retries_without_orphan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_target: str
) -> None:
    plan, raw_root, campaign_root, _archive_root = _plan(tmp_path)
    original_write = incident_module.write_json
    original_rmdir = Path.rmdir
    failed = False

    def fail_journal_write(path: Path, value: object, **kwargs: object) -> None:
        if path.name == incident_module.JOURNAL_NAME:
            raise OSError("synthetic journal write failure")
        original_write(path, value, **kwargs)

    def fail_cleanup_once(path: Path) -> None:
        nonlocal failed
        target = plan.staging if cleanup_target == "container" else plan.staging / cleanup_target
        if path == target and not failed:
            failed = True
            raise OSError("synthetic transient scaffold cleanup failure")
        original_rmdir(path)

    monkeypatch.setattr(incident_module, "write_json", fail_journal_write)
    monkeypatch.setattr(Path, "rmdir", fail_cleanup_once)
    with pytest.raises(OSError, match="synthetic journal write failure"):
        execute_incident_archive(plan)

    assert failed
    assert not plan.staging.exists()
    assert all((raw_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert all((campaign_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)


def test_full_scaffold_cleanup_replacement_is_preserved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, raw_root, campaign_root, _archive_root = _plan(tmp_path)
    original_write = incident_module.write_json
    original_rmdir = Path.rmdir
    relocated = tmp_path / "relocated-owned-campaign-scaffold"
    swapped = False

    def fail_journal_write(path: Path, value: object, **kwargs: object) -> None:
        if path.name == incident_module.JOURNAL_NAME:
            raise OSError("synthetic journal write failure")
        original_write(path, value, **kwargs)

    def replace_campaign_root(path: Path) -> None:
        nonlocal swapped
        if path == plan.staging / "campaigns" and not swapped:
            swapped = True
            path.rename(relocated)
            path.mkdir()
            raise OSError("synthetic cleanup replacement")
        original_rmdir(path)

    monkeypatch.setattr(incident_module, "write_json", fail_journal_write)
    monkeypatch.setattr(Path, "rmdir", replace_campaign_root)
    with pytest.raises(EvidenceArchiveError, match="compensation was incomplete"):
        execute_incident_archive(plan)

    assert relocated.is_dir()
    assert (plan.staging / "campaigns").is_dir()
    assert all((raw_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert all((campaign_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)


@pytest.mark.parametrize("failed_child", ("raw", "campaigns"))
def test_scaffold_creation_failure_removes_pinned_unjournaled_scaffold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failed_child: str
) -> None:
    plan, raw_root, campaign_root, _archive_root = _plan(tmp_path)
    original_mkdir = Path.mkdir

    def fail_campaign_scaffold(path: Path, *args: object, **kwargs: object) -> None:
        if path == plan.staging / failed_child:
            raise OSError("synthetic scaffold creation failure")
        original_mkdir(path, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", fail_campaign_scaffold)
    with pytest.raises(OSError, match="synthetic scaffold creation failure"):
        execute_incident_archive(plan)

    assert all((raw_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert all((campaign_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert not plan.staging.exists()
    assert not plan.destination.exists()


@pytest.mark.parametrize("cleanup_target", ("raw", "container"))
def test_transient_partial_scaffold_cleanup_failure_retries_without_orphan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_target: str
) -> None:
    plan, raw_root, campaign_root, _archive_root = _plan(tmp_path)
    original_mkdir = Path.mkdir
    original_rmdir = Path.rmdir
    cleanup_failed = False

    def fail_campaign_scaffold(path: Path, *args: object, **kwargs: object) -> None:
        if path == plan.staging / "campaigns":
            raise OSError("synthetic campaign scaffold failure")
        original_mkdir(path, *args, **kwargs)

    def fail_cleanup_once(path: Path) -> None:
        nonlocal cleanup_failed
        target = plan.staging if cleanup_target == "container" else plan.staging / cleanup_target
        if path == target and not cleanup_failed:
            cleanup_failed = True
            raise OSError("synthetic transient partial cleanup failure")
        original_rmdir(path)

    monkeypatch.setattr(Path, "mkdir", fail_campaign_scaffold)
    monkeypatch.setattr(Path, "rmdir", fail_cleanup_once)
    with pytest.raises(OSError, match="synthetic campaign scaffold failure"):
        execute_incident_archive(plan)

    assert cleanup_failed
    assert not plan.staging.exists()
    assert all((raw_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert all((campaign_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)


def test_partial_scaffold_cleanup_replacement_is_preserved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, raw_root, campaign_root, _archive_root = _plan(tmp_path)
    original_mkdir = Path.mkdir
    original_rmdir = Path.rmdir
    relocated = tmp_path / "relocated-owned-partial-raw-scaffold"
    swapped = False

    def fail_campaign_scaffold(path: Path, *args: object, **kwargs: object) -> None:
        if path == plan.staging / "campaigns":
            raise OSError("synthetic campaign scaffold failure")
        original_mkdir(path, *args, **kwargs)

    def replace_raw_during_cleanup(path: Path) -> None:
        nonlocal swapped
        if path == plan.staging / "raw" and not swapped:
            swapped = True
            path.rename(relocated)
            original_mkdir(path)
            raise OSError("synthetic partial cleanup replacement")
        original_rmdir(path)

    monkeypatch.setattr(Path, "mkdir", fail_campaign_scaffold)
    monkeypatch.setattr(Path, "rmdir", replace_raw_during_cleanup)
    with pytest.raises(EvidenceArchiveError, match="compensation was incomplete"):
        execute_incident_archive(plan)

    assert relocated.is_dir()
    assert (plan.staging / "raw").is_dir()
    assert all((raw_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert all((campaign_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)


def test_unjournaled_scaffold_replacement_is_preserved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, raw_root, campaign_root, _archive_root = _plan(tmp_path)
    original_mkdir = Path.mkdir
    relocated = tmp_path / "relocated-owned-unjournaled-scaffold"

    def replace_before_campaign_scaffold(path: Path, *args: object, **kwargs: object) -> None:
        if path == plan.staging / "campaigns":
            plan.staging.rename(relocated)
            original_mkdir(plan.staging)
            (plan.staging / "competitor.txt").write_text("keep", encoding="utf-8")
            raise OSError("synthetic scaffold replacement")
        original_mkdir(path, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", replace_before_campaign_scaffold)
    with pytest.raises(EvidenceArchiveError, match="safe unjournaled-scaffold cleanup"):
        execute_incident_archive(plan)

    assert (relocated / "raw").is_dir()
    assert (plan.staging / "competitor.txt").read_text(encoding="utf-8") == "keep"
    assert all((raw_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert all((campaign_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)


def test_unjournaled_scaffold_with_unexpected_entry_is_preserved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, raw_root, campaign_root, _archive_root = _plan(tmp_path)
    original_mkdir = Path.mkdir

    def add_entry_before_campaign_scaffold(path: Path, *args: object, **kwargs: object) -> None:
        if path == plan.staging / "campaigns":
            (plan.staging / "unexpected.txt").write_text("keep", encoding="utf-8")
            raise OSError("synthetic unexpected scaffold entry")
        original_mkdir(path, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", add_entry_before_campaign_scaffold)
    with pytest.raises(EvidenceArchiveError, match="entries changed"):
        execute_incident_archive(plan)

    assert (plan.staging / "raw").is_dir()
    assert (plan.staging / "unexpected.txt").read_text(encoding="utf-8") == "keep"
    assert all((raw_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert all((campaign_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)


def test_failure_before_staging_identity_capture_retains_unverified_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, raw_root, campaign_root, _archive_root = _plan(tmp_path)
    original_identity = incident_module._directory_identity

    def fail_staging_identity(path: Path, *, label: str) -> incident_module.DirectoryIdentity:
        if path == plan.staging:
            raise OSError("synthetic staging identity failure")
        return original_identity(path, label=label)

    monkeypatch.setattr(incident_module, "_directory_identity", fail_staging_identity)
    with pytest.raises(EvidenceArchiveError, match="before its staging identity was pinned"):
        execute_incident_archive(plan)

    assert plan.staging.is_dir()
    assert not any(plan.staging.iterdir())
    assert all((raw_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert all((campaign_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)


def test_lost_staging_creation_race_does_not_delete_competing_transaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, raw_root, campaign_root, _archive_root = _plan(tmp_path)
    original_mkdir = Path.mkdir

    def lose_staging_race(path: Path, *args: object, **kwargs: object) -> None:
        if path == plan.staging:
            original_mkdir(path)
            original_mkdir(path / "raw")
            original_mkdir(path / "campaigns")
            (path / "competing-owner.txt").write_text("keep", encoding="utf-8")
            raise FileExistsError("synthetic staging race")
        original_mkdir(path, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", lose_staging_race)
    with pytest.raises(FileExistsError, match="synthetic staging race"):
        execute_incident_archive(plan)

    assert (plan.staging / "competing-owner.txt").read_text(encoding="utf-8") == "keep"
    assert all((raw_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert all((campaign_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)


def test_archive_parent_swap_during_staging_creation_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, raw_root, campaign_root, _archive_root = _plan(tmp_path)
    plan.staging.parent.mkdir(parents=True)
    external = tmp_path / "external-race-target"
    external.mkdir()
    displaced_parent = tmp_path / "displaced-commit-parent"
    original_mkdir = Path.mkdir

    def swap_parent_before_staging_mkdir(path: Path, *args: object, **kwargs: object) -> None:
        if path == plan.staging:
            plan.staging.parent.rename(displaced_parent)
            plan.staging.parent.symlink_to(external, target_is_directory=True)
        original_mkdir(path, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", swap_parent_before_staging_mkdir)
    with pytest.raises(EvidenceArchiveError, match="contains a symlink component"):
        execute_incident_archive(plan)

    assert (external / plan.staging.name).is_dir()
    assert all((raw_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert all((campaign_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)


def test_staging_identity_replacement_after_creation_is_preserved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, raw_root, campaign_root, _archive_root = _plan(tmp_path)
    original_revalidate = incident_module._revalidate_plan_paths
    relocated = tmp_path / "relocated-owned-staging"
    calls = 0

    def replace_staging_before_revalidation(current_plan: IncidentPlan) -> None:
        nonlocal calls
        calls += 1
        if calls == 3:
            current_plan.staging.rename(relocated)
            current_plan.staging.mkdir()
        original_revalidate(current_plan)

    monkeypatch.setattr(
        incident_module, "_revalidate_plan_paths", replace_staging_before_revalidation
    )
    with pytest.raises(EvidenceArchiveError, match="identity changed"):
        execute_incident_archive(plan)

    assert relocated.is_dir()
    assert plan.staging.is_dir()
    assert all((raw_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert all((campaign_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)


def test_source_root_swap_at_move_boundary_moves_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, raw_root, campaign_root, _archive_root = _plan(tmp_path)
    relocated_raw = tmp_path / "relocated-raw-at-move"
    original_rename = incident_module._rename_directory
    swapped = False

    def swap_before_move(source: Path, destination: Path, **kwargs: object) -> None:
        nonlocal swapped
        if not swapped:
            swapped = True
            raw_root.rename(relocated_raw)
            raw_root.symlink_to(relocated_raw, target_is_directory=True)
        original_rename(source, destination, **kwargs)

    monkeypatch.setattr(incident_module, "_rename_directory", swap_before_move)
    with pytest.raises(EvidenceArchiveError, match="automatic rollback was refused"):
        execute_incident_archive(plan)

    assert (relocated_raw / "EXP-ONE").is_dir()
    assert (relocated_raw / "EXP-TWO").is_dir()
    assert (campaign_root / "EXP-ONE").is_dir()
    assert (plan.staging / incident_module.JOURNAL_NAME).is_file()


def test_source_directory_replacement_at_move_boundary_is_not_moved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, raw_root, _campaign_root, _archive_root = _plan(tmp_path)
    original_source = raw_root / "EXP-ONE"
    displaced_source = tmp_path / "displaced-exp-one"
    original_rename = incident_module._rename_directory
    swapped = False

    def replace_before_move(source: Path, destination: Path, **kwargs: object) -> None:
        nonlocal swapped
        if not swapped:
            swapped = True
            original_source.rename(displaced_source)
            original_source.mkdir()
        original_rename(source, destination, **kwargs)

    monkeypatch.setattr(incident_module, "_rename_directory", replace_before_move)
    with pytest.raises(EvidenceArchiveError, match="automatic rollback was refused"):
        execute_incident_archive(plan)

    assert displaced_source.is_dir()
    assert original_source.is_dir()
    assert (plan.staging / incident_module.JOURNAL_NAME).is_file()


def test_payload_root_swap_before_journal_moves_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, raw_root, campaign_root, _archive_root = _plan(tmp_path)
    original_write = incident_module.write_json
    relocated_payload = tmp_path / "relocated-transaction-raw"

    def swap_before_journal(path: Path, value: object, **kwargs: object) -> None:
        if path.name == incident_module.JOURNAL_NAME:
            (plan.staging / "raw").rename(relocated_payload)
            (plan.staging / "raw").mkdir()
        original_write(path, value, **kwargs)

    monkeypatch.setattr(incident_module, "write_json", swap_before_journal)
    with pytest.raises(EvidenceArchiveError, match="automatic rollback was incomplete"):
        execute_incident_archive(plan)

    assert relocated_payload.is_dir()
    assert (plan.staging / "raw").is_dir()
    assert all((raw_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert all((campaign_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)


def test_atomic_publish_preserves_a_competing_destination(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, raw_root, campaign_root, _archive_root = _plan(tmp_path)
    original_publish = incident_module._publish_directory_no_replace

    def create_competitor_then_publish(source: Path, destination: Path) -> None:
        destination.mkdir()
        (destination / "competitor.txt").write_text("keep", encoding="utf-8")
        original_publish(source, destination)

    monkeypatch.setattr(
        incident_module, "_publish_directory_no_replace", create_competitor_then_publish
    )
    with pytest.raises(OSError):
        execute_incident_archive(plan)

    assert (plan.destination / "competitor.txt").read_text(encoding="utf-8") == "keep"
    assert not plan.staging.exists()
    assert all((raw_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert all((campaign_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)


def test_automatic_rollback_refuses_a_corrupted_staged_payload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, raw_root, _campaign_root, _archive_root = _plan(tmp_path)
    original_scan = incident_module._scan_archived_inventory
    corrupted = False

    def corrupt_before_scan(
        archive: Path, states: tuple[incident_module.ExperimentState, ...]
    ) -> tuple[incident_module.ArchiveEntry, ...]:
        nonlocal corrupted
        if not corrupted:
            corrupted = True
            trace = archive / "campaigns/EXP-ONE/runs/run-1/attempt-0001/trace.bin"
            trace.write_bytes(b"CORRUPT")
        return original_scan(archive, states)

    monkeypatch.setattr(incident_module, "_scan_archived_inventory", corrupt_before_scan)
    with pytest.raises(EvidenceArchiveError, match="automatic rollback was refused"):
        execute_incident_archive(plan)

    staged_trace = plan.staging / "campaigns/EXP-ONE/runs/run-1/attempt-0001/trace.bin"
    assert staged_trace.read_bytes() == b"CORRUPT"
    assert (plan.staging / incident_module.JOURNAL_NAME).is_file()
    assert not (raw_root / "EXP-ONE").exists()


def test_staged_gate_rejects_a_corrupted_journal_and_preserves_transaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _raw_root, _campaign_root, _archive_root = _plan(tmp_path)
    original_scan = incident_module._scan_archived_inventory
    corrupted = False

    def corrupt_journal_after_scan(
        archive: Path, states: tuple[incident_module.ExperimentState, ...]
    ) -> tuple[incident_module.ArchiveEntry, ...]:
        nonlocal corrupted
        entries = original_scan(archive, states)
        if not corrupted:
            corrupted = True
            (archive / incident_module.JOURNAL_NAME).write_text("not json", encoding="utf-8")
        return entries

    monkeypatch.setattr(incident_module, "_scan_archived_inventory", corrupt_journal_after_scan)
    with pytest.raises(EvidenceArchiveError, match="automatic rollback was refused"):
        execute_incident_archive(plan)

    assert (plan.staging / incident_module.JOURNAL_NAME).read_text(encoding="utf-8") == "not json"
    assert not plan.destination.exists()


def test_scaffold_cleanup_failure_is_compensated_and_retryable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, raw_root, campaign_root, archive_root = _plan(tmp_path)
    plan.staging.parent.mkdir(parents=True)
    plan.staging.mkdir()
    (plan.staging / "raw").mkdir()
    (plan.staging / "campaigns").mkdir()
    journal_path = plan.staging / incident_module.JOURNAL_NAME
    write_json(journal_path, _transaction_journal(plan))
    original_rmdir = Path.rmdir
    failed = False

    def fail_campaign_cleanup_once(path: Path) -> None:
        nonlocal failed
        if path == plan.staging / "campaigns" and not failed:
            failed = True
            raise OSError("synthetic scaffold cleanup failure")
        original_rmdir(path)

    monkeypatch.setattr(Path, "rmdir", fail_campaign_cleanup_once)
    with pytest.raises(EvidenceArchiveError, match="cleanup was incomplete"):
        rollback_incident_staging(
            tmp_path,
            raw_root=raw_root,
            campaign_root=campaign_root,
            archive_root=archive_root,
            source_commit=SOURCE_COMMIT,
            label=plan.label,
            experiment_ids=EXPERIMENT_IDS,
        )

    assert (plan.staging / "raw").is_dir()
    assert (plan.staging / "campaigns").is_dir()
    assert journal_path.is_file()

    rollback_incident_staging(
        tmp_path,
        raw_root=raw_root,
        campaign_root=campaign_root,
        archive_root=archive_root,
        source_commit=SOURCE_COMMIT,
        label=plan.label,
        experiment_ids=EXPERIMENT_IDS,
    )
    assert not plan.staging.exists()


@pytest.mark.parametrize("published_staging", [False, True])
def test_explicit_rollback_recovers_a_journaled_transaction(
    tmp_path: Path, published_staging: bool
) -> None:
    plan, raw_root, campaign_root, archive_root = _plan(tmp_path)
    plan.staging.parent.mkdir(parents=True)
    plan.staging.mkdir()
    (plan.staging / "raw").mkdir()
    (plan.staging / "campaigns").mkdir()
    write_json(plan.staging / incident_module.JOURNAL_NAME, _transaction_journal(plan))
    first_source, first_staged = incident_module._move_pairs(plan, plan.staging)[0]
    first_source.rename(first_staged)
    if published_staging:
        plan.staging.rename(plan.destination)

    rollback_incident_staging(
        tmp_path,
        raw_root=raw_root,
        campaign_root=campaign_root,
        archive_root=archive_root,
        source_commit=SOURCE_COMMIT,
        label=plan.label,
        experiment_ids=EXPERIMENT_IDS,
    )

    assert all((raw_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert all((campaign_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert not plan.staging.exists()
    assert not plan.destination.exists()


def test_explicit_retry_selects_the_only_journaled_container(
    tmp_path: Path,
) -> None:
    plan, raw_root, campaign_root, archive_root = _plan(tmp_path)
    plan.staging.parent.mkdir(parents=True)
    plan.staging.mkdir()
    (plan.staging / "raw").mkdir()
    (plan.staging / "campaigns").mkdir()
    write_json(plan.staging / incident_module.JOURNAL_NAME, _transaction_journal(plan))
    plan.destination.mkdir()
    competitor = plan.destination / "competitor.txt"
    competitor.write_text("keep", encoding="utf-8")

    rollback_incident_staging(
        tmp_path,
        raw_root=raw_root,
        campaign_root=campaign_root,
        archive_root=archive_root,
        source_commit=SOURCE_COMMIT,
        label=plan.label,
        experiment_ids=EXPERIMENT_IDS,
    )

    assert not plan.staging.exists()
    assert competitor.read_text(encoding="utf-8") == "keep"
    assert all((raw_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert all((campaign_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)


def test_rollback_rejects_symlinked_payload_root_before_mutation(tmp_path: Path) -> None:
    plan, raw_root, campaign_root, archive_root = _plan(tmp_path)
    plan.staging.parent.mkdir(parents=True)
    plan.staging.mkdir()
    (plan.staging / "raw").mkdir()
    (plan.staging / "campaigns").mkdir()
    journal = _transaction_journal(plan)
    (plan.staging / "raw").rmdir()
    external_raw = tmp_path / "external-raw"
    external_raw.mkdir()
    (plan.staging / "raw").symlink_to(external_raw, target_is_directory=True)
    write_json(plan.staging / incident_module.JOURNAL_NAME, journal)

    with pytest.raises(EvidenceArchiveError, match="payload root is missing or unsafe"):
        rollback_incident_staging(
            tmp_path,
            raw_root=raw_root,
            campaign_root=campaign_root,
            archive_root=archive_root,
            source_commit=SOURCE_COMMIT,
            label=plan.label,
            experiment_ids=EXPERIMENT_IDS,
        )

    assert all((raw_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert all((campaign_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert (plan.staging / incident_module.JOURNAL_NAME).is_file()


def test_rollback_rejects_symlinked_journal_before_mutation(tmp_path: Path) -> None:
    plan, raw_root, campaign_root, archive_root = _plan(tmp_path)
    plan.staging.parent.mkdir(parents=True)
    plan.staging.mkdir()
    (plan.staging / "raw").mkdir()
    (plan.staging / "campaigns").mkdir()
    external_journal = tmp_path / "external-journal.json"
    write_json(external_journal, _transaction_journal(plan))
    (plan.staging / incident_module.JOURNAL_NAME).symlink_to(external_journal)

    with pytest.raises(EvidenceArchiveError, match="journal is missing or unsafe"):
        rollback_incident_staging(
            tmp_path,
            raw_root=raw_root,
            campaign_root=campaign_root,
            archive_root=archive_root,
            source_commit=SOURCE_COMMIT,
            label=plan.label,
            experiment_ids=EXPERIMENT_IDS,
        )

    assert all((raw_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert all((campaign_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)


def test_rollback_rejects_extra_staged_root_without_deleting_journal(tmp_path: Path) -> None:
    plan, raw_root, campaign_root, archive_root = _plan(tmp_path)
    plan.staging.parent.mkdir(parents=True)
    plan.staging.mkdir()
    (plan.staging / "raw").mkdir()
    (plan.staging / "campaigns").mkdir()
    journal_path = plan.staging / incident_module.JOURNAL_NAME
    write_json(journal_path, _transaction_journal(plan))
    (plan.staging / "raw/UNJOURNALED").mkdir()

    with pytest.raises(EvidenceArchiveError, match="unexpected incident transaction payload"):
        rollback_incident_staging(
            tmp_path,
            raw_root=raw_root,
            campaign_root=campaign_root,
            archive_root=archive_root,
            source_commit=SOURCE_COMMIT,
            label=plan.label,
            experiment_ids=EXPERIMENT_IDS,
        )

    assert journal_path.is_file()


def test_rollback_checks_journaled_state_before_moving_any_root(tmp_path: Path) -> None:
    plan, raw_root, campaign_root, archive_root = _plan(tmp_path)
    plan.staging.parent.mkdir(parents=True)
    plan.staging.mkdir()
    (plan.staging / "raw").mkdir()
    (plan.staging / "campaigns").mkdir()
    journal = _transaction_journal(plan)
    journal["suite_state"][0]["raw_record_count"] = 999
    journal["suite_state_sha256"] = sha256_value(journal["suite_state"])
    journal.pop("journal_sha256")
    journal["journal_sha256"] = sha256_value(journal)
    journal_path = plan.staging / incident_module.JOURNAL_NAME
    write_json(journal_path, journal)
    first_source, first_staged = incident_module._move_pairs(plan, plan.staging)[0]
    first_source.rename(first_staged)

    with pytest.raises(EvidenceArchiveError, match="payload state does not match"):
        rollback_incident_staging(
            tmp_path,
            raw_root=raw_root,
            campaign_root=campaign_root,
            archive_root=archive_root,
            source_commit=SOURCE_COMMIT,
            label=plan.label,
            experiment_ids=EXPERIMENT_IDS,
        )

    assert not first_source.exists()
    assert first_staged.is_dir()
    assert journal_path.is_file()


def test_explicit_rollback_rejects_a_replaced_staged_directory(
    tmp_path: Path,
) -> None:
    plan, raw_root, campaign_root, archive_root = _plan(tmp_path)
    plan.staging.parent.mkdir(parents=True)
    plan.staging.mkdir()
    (plan.staging / "raw").mkdir()
    (plan.staging / "campaigns").mkdir()
    journal_path = plan.staging / incident_module.JOURNAL_NAME
    write_json(journal_path, _transaction_journal(plan))
    first_source, first_staged = incident_module._move_pairs(plan, plan.staging)[0]
    first_source.rename(first_staged)
    displaced = tmp_path / "displaced-staged-source"
    first_staged.rename(displaced)
    first_staged.mkdir()

    with pytest.raises(EvidenceArchiveError, match="identity changed"):
        rollback_incident_staging(
            tmp_path,
            raw_root=raw_root,
            campaign_root=campaign_root,
            archive_root=archive_root,
            source_commit=SOURCE_COMMIT,
            label=plan.label,
            experiment_ids=EXPERIMENT_IDS,
        )

    assert displaced.is_dir()
    assert first_staged.is_dir()
    assert journal_path.is_file()
    assert not first_source.exists()


def test_verifier_detects_payload_tampering(tmp_path: Path) -> None:
    plan, _raw_root, _campaign_root, _archive_root = _plan(tmp_path)
    destination = execute_incident_archive(plan)
    payload = destination / "campaigns/EXP-ONE/runs/run-1/attempt-0001/trace.bin"
    payload.write_bytes(b"tampered")

    with pytest.raises(EvidenceArchiveError, match="manifest inventory"):
        verify_incident_archive(destination)


def test_verifier_rejects_an_uninventoried_payload_root_file(tmp_path: Path) -> None:
    plan, _raw_root, _campaign_root, _archive_root = _plan(tmp_path)
    destination = execute_incident_archive(plan)
    (destination / "raw/.gitkeep").write_text("unexpected", encoding="utf-8")

    with pytest.raises(EvidenceArchiveError, match="unexpected incident payload entry"):
        verify_incident_archive(destination)


def test_verifier_rejects_symlinked_manifest(tmp_path: Path) -> None:
    plan, _raw_root, _campaign_root, _archive_root = _plan(tmp_path)
    destination = execute_incident_archive(plan)
    manifest_path = destination / incident_module.MANIFEST_NAME
    external_manifest = tmp_path / "external-incident-manifest.json"
    manifest_path.rename(external_manifest)
    manifest_path.symlink_to(external_manifest)

    with pytest.raises(EvidenceArchiveError, match="manifest is missing or unsafe"):
        verify_incident_archive(destination)


def test_explicit_rollback_rejects_tampered_payload_before_moving_it(tmp_path: Path) -> None:
    plan, raw_root, campaign_root, archive_root = _plan(tmp_path)
    plan.staging.parent.mkdir(parents=True)
    plan.staging.mkdir()
    (plan.staging / "raw").mkdir()
    (plan.staging / "campaigns").mkdir()
    write_json(plan.staging / incident_module.JOURNAL_NAME, _transaction_journal(plan))
    first_source, first_staged = incident_module._move_pairs(plan, plan.staging)[0]
    first_source.rename(first_staged)
    raw_record = next(first_staged.rglob("*.json"))
    raw_record.write_bytes(raw_record.read_bytes() + b" ")

    with pytest.raises(EvidenceArchiveError, match="transaction inventory hash"):
        rollback_incident_staging(
            tmp_path,
            raw_root=raw_root,
            campaign_root=campaign_root,
            archive_root=archive_root,
            source_commit=SOURCE_COMMIT,
            label=plan.label,
            experiment_ids=EXPERIMENT_IDS,
        )

    assert not first_source.exists()
    assert first_staged.is_dir()


def test_cli_defaults_to_a_read_only_dry_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    raw_root, campaign_root, archive_root = _fixture(tmp_path)
    monkeypatch.setattr(
        incident_module.complete_archive,
        "core_experiment_ids",
        lambda _root: EXPERIMENT_IDS,
    )

    exit_code = incident_module.main(
        [
            "--root",
            str(tmp_path),
            "--raw-root",
            str(raw_root),
            "--campaign-root",
            str(campaign_root),
            "--archive-root",
            str(archive_root),
            "--expected-source-commit",
            SOURCE_COMMIT,
            "--label",
            "cli-dry-run",
        ]
    )

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert output["mode"] == "dry-run"
    assert output["archive_purpose"] == "diagnostic-only"
    assert output["publication_eligible"] is False
    assert not archive_root.exists()
