from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from benchmark.runner.canonical import sha256_value, write_json
from scripts import archive_research_evidence as archive_module
from scripts.archive_research_evidence import (
    ArchivePlan,
    EvidenceArchiveError,
    execute_archive,
    plan_archive,
    rollback_staging,
    verify_archive,
)

SOURCE_COMMIT = "a" * 40
EXPERIMENT_IDS = ("EXP-ONE", "EXP-TWO")


def _fixture(repository: Path) -> tuple[Path, Path, Path]:
    raw_root = repository / "results/raw"
    campaign_root = repository / ".artifacts/campaigns"
    archive_root = repository / ".artifacts/campaign-archives"
    raw_root.mkdir(parents=True)
    campaign_root.mkdir(parents=True)
    (raw_root / ".gitkeep").write_text("", encoding="utf-8")
    reports = repository / "results/reports"
    reports.mkdir()
    (reports / "must-remain.txt").write_text("unrelated", encoding="utf-8")

    for index, experiment_id in enumerate(EXPERIMENT_IDS):
        raw_directory = raw_root / experiment_id / "spark_baseline"
        raw_directory.mkdir(parents=True)
        write_json(
            raw_directory / f"run-{index}.json",
            {
                "schema_version": 1,
                "experiment_id": experiment_id,
                "run_id": f"run-{index}",
                "provenance": {"git_commit": SOURCE_COMMIT},
            },
        )
        campaign_directory = campaign_root / experiment_id
        (campaign_directory / "failed-attempts").mkdir(parents=True)
        (campaign_directory / "runs/run-1").mkdir(parents=True)
        (campaign_directory / "runs/run-1/trace.bin").write_bytes(f"trace-{experiment_id}".encode())
        manifest = {
            "schema_version": 1,
            "experiment_id": experiment_id,
            "dataset_validation": {"validator_git_commit": SOURCE_COMMIT},
        }
        manifest["manifest_sha256"] = sha256_value(manifest)
        write_json(campaign_directory / "experiment-manifest.json", manifest)
    return raw_root, campaign_root, archive_root


def _plan(repository: Path) -> tuple[ArchivePlan, Path, Path, Path]:
    raw_root, campaign_root, archive_root = _fixture(repository)
    plan = plan_archive(
        repository,
        raw_root=raw_root,
        campaign_root=campaign_root,
        archive_root=archive_root,
        source_commit=SOURCE_COMMIT,
        label="before-final-rerun",
        experiment_ids=EXPERIMENT_IDS,
        now=datetime(2026, 9, 9, 4, 5, 6, tzinfo=UTC),
    )
    return plan, raw_root, campaign_root, archive_root


def test_dry_run_snapshots_exact_sources_without_writing(tmp_path: Path) -> None:
    plan, raw_root, campaign_root, archive_root = _plan(tmp_path)

    assert not archive_root.exists()
    assert (raw_root / ".gitkeep").is_file()
    assert all((raw_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert all((campaign_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert "campaigns/EXP-ONE/failed-attempts" in {
        entry.path for entry in plan.entries if entry.kind == "directory"
    }
    assert plan.created_at == "2026-09-09T04:05:06Z"
    assert len(plan.inventory_sha256) == 64


def test_execute_moves_only_experiment_directories_and_verifies(tmp_path: Path) -> None:
    plan, raw_root, campaign_root, _archive_root = _plan(tmp_path)

    destination = execute_archive(plan)
    manifest = verify_archive(destination)

    assert manifest["source_commit"] == SOURCE_COMMIT
    assert manifest["inventory_sha256"] == plan.inventory_sha256
    assert (raw_root / ".gitkeep").is_file()
    assert raw_root.is_dir()
    assert campaign_root.is_dir()
    assert not any((raw_root / experiment_id).exists() for experiment_id in EXPERIMENT_IDS)
    assert not any((campaign_root / experiment_id).exists() for experiment_id in EXPERIMENT_IDS)
    assert (tmp_path / "results/reports/must-remain.txt").read_text() == "unrelated"
    assert (destination / "campaigns/EXP-ONE/failed-attempts").is_dir()
    assert not any((destination / "campaigns/EXP-ONE/failed-attempts").iterdir())

    with pytest.raises(EvidenceArchiveError, match="overwrite existing archive"):
        execute_archive(plan)


def test_rejects_unexpected_source_root_entry(tmp_path: Path) -> None:
    raw_root, campaign_root, archive_root = _fixture(tmp_path)
    (raw_root / "EXP-UNREVIEWED").mkdir()

    with pytest.raises(EvidenceArchiveError, match="unexpected entry"):
        plan_archive(
            tmp_path,
            raw_root=raw_root,
            campaign_root=campaign_root,
            archive_root=archive_root,
            source_commit=SOURCE_COMMIT,
            label="reject-extra",
            experiment_ids=EXPERIMENT_IDS,
        )


def test_rejects_raw_record_from_another_commit(tmp_path: Path) -> None:
    raw_root, campaign_root, archive_root = _fixture(tmp_path)
    record_path = raw_root / "EXP-ONE/spark_baseline/run-0.json"
    record = json.loads(record_path.read_text(encoding="utf-8"))
    record["provenance"]["git_commit"] = "b" * 40
    write_json(record_path, record, immutable=False)

    with pytest.raises(EvidenceArchiveError, match="raw record is not bound"):
        plan_archive(
            tmp_path,
            raw_root=raw_root,
            campaign_root=campaign_root,
            archive_root=archive_root,
            source_commit=SOURCE_COMMIT,
            label="reject-provenance",
            experiment_ids=EXPERIMENT_IDS,
        )


def test_rejects_mutation_after_plan_before_any_move(tmp_path: Path) -> None:
    plan, raw_root, campaign_root, _archive_root = _plan(tmp_path)
    changed = campaign_root / "EXP-ONE/runs/run-1/trace.bin"
    changed.write_bytes(b"changed-after-plan")

    with pytest.raises(EvidenceArchiveError, match="evidence changed"):
        execute_archive(plan)

    assert all((raw_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert all((campaign_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert not plan.staging.exists()
    assert not plan.destination.exists()


def test_move_failure_rolls_back_all_completed_moves(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, raw_root, campaign_root, _archive_root = _plan(tmp_path)
    original = archive_module._rename_directory
    calls = 0

    def fail_third_move(source: Path, destination: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 3:
            raise OSError("synthetic move failure")
        original(source, destination)

    monkeypatch.setattr(archive_module, "_rename_directory", fail_third_move)
    with pytest.raises(OSError, match="synthetic move failure"):
        execute_archive(plan)

    assert all((raw_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert all((campaign_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
    assert (raw_root / ".gitkeep").is_file()
    assert not plan.staging.exists()
    assert not plan.destination.exists()


@pytest.mark.parametrize("published_staging", [False, True])
def test_explicit_rollback_recovers_interrupted_transaction(
    tmp_path: Path, published_staging: bool
) -> None:
    plan, raw_root, campaign_root, archive_root = _plan(tmp_path)
    plan.staging.parent.mkdir(parents=True)
    plan.staging.mkdir()
    (plan.staging / "raw").mkdir()
    (plan.staging / "campaigns").mkdir()
    write_json(plan.staging / archive_module.JOURNAL_NAME, archive_module._journal_value(plan))
    first_source, first_staged = archive_module._move_pairs(plan, plan.staging)[0]
    first_source.rename(first_staged)
    if published_staging:
        plan.staging.rename(plan.destination)

    rollback_staging(
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


def test_verifier_detects_payload_tampering(tmp_path: Path) -> None:
    plan, _raw_root, _campaign_root, _archive_root = _plan(tmp_path)
    destination = execute_archive(plan)
    payload = destination / "campaigns/EXP-TWO/runs/run-1/trace.bin"
    payload.write_bytes(b"tampered")

    with pytest.raises(EvidenceArchiveError, match="manifest inventory"):
        verify_archive(destination)


def test_cli_defaults_to_read_only_dry_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    raw_root, campaign_root, archive_root = _fixture(tmp_path)
    monkeypatch.setattr(archive_module, "core_experiment_ids", lambda _root: EXPERIMENT_IDS)

    exit_code = archive_module.main(
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
    assert not archive_root.exists()
    assert all((raw_root / experiment_id).is_dir() for experiment_id in EXPERIMENT_IDS)
