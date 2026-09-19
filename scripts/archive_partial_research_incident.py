"""Transactionally archive an interrupted core research suite for diagnostics only.

This workflow is deliberately separate from ``archive_research_evidence.py``.  It preserves an
incomplete suite, including missing experiment roots, without making that evidence publishable or
weakening the complete-suite archive contract.  Dry-run is the default; ``--execute`` performs the
verified move and ``--rollback-staging`` restores an interrupted transaction.
"""

from __future__ import annotations

import argparse
import ctypes
import errno
import json
import os
import re
import stat
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from benchmark.runner.canonical import canonical_json_bytes, sha256_value, write_json
from scripts import archive_research_evidence as complete_archive

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RAW_ROOT = ROOT / "results/raw"
DEFAULT_CAMPAIGN_ROOT = ROOT / ".artifacts/campaigns"
DEFAULT_ARCHIVE_ROOT = ROOT / ".artifacts/research-incident-archives"
MANIFEST_NAME = "incident-manifest.json"
JOURNAL_NAME = "transaction-journal.json"
ARCHIVE_CLASS = "research-partial-incident-archive-v1"
JOURNAL_CLASS = "research-partial-incident-transaction-v2"
ARCHIVE_PURPOSE = "diagnostic-only"
_ATTEMPT_PATTERN = re.compile(r"^attempt-[0-9]{4}$")
_VERIFICATION_ATTEMPT_PATTERN = re.compile(
    r"^campaign-verification-attempt-(?P<attempt>[0-9]{4})\.json$"
)

ArchiveEntry = complete_archive.ArchiveEntry
EvidenceArchiveError = complete_archive.EvidenceArchiveError
DirectoryIdentity = tuple[int, int]


def _directory_identity(path: Path, *, label: str) -> DirectoryIdentity:
    if complete_archive._is_linklike(path) or not path.is_dir():
        raise EvidenceArchiveError(f"{label} is not a real directory: {path}")
    try:
        status = path.stat(follow_symlinks=False)
    except OSError as error:
        raise EvidenceArchiveError(f"cannot stat {label}: {path}") from error
    if status.st_dev < 0 or status.st_ino <= 0:
        raise EvidenceArchiveError(f"{label} has no usable filesystem identity: {path}")
    return status.st_dev, status.st_ino


def _assert_directory_identity(path: Path, expected: DirectoryIdentity, *, label: str) -> None:
    if _directory_identity(path, label=label) != expected:
        raise EvidenceArchiveError(f"{label} identity changed: {path}")


@dataclass(frozen=True, slots=True)
class ExperimentState:
    """Observed state for one experiment at the read-only planning boundary."""

    experiment_id: str
    raw_root_state: str
    raw_record_count: int
    campaign_root_state: str
    run_attempt_count: int
    failed_attempt_count: int
    verification_status: str | None

    def manifest_value(self) -> dict[str, Any]:
        return {
            "experiment_id": self.experiment_id,
            "raw_root_state": self.raw_root_state,
            "raw_record_count": self.raw_record_count,
            "campaign_root_state": self.campaign_root_state,
            "run_attempt_count": self.run_attempt_count,
            "failed_attempt_count": self.failed_attempt_count,
            "verification_status": self.verification_status,
        }

    @property
    def raw_present(self) -> bool:
        return self.raw_root_state != "missing"

    @property
    def campaign_present(self) -> bool:
        return self.campaign_root_state != "missing"


@dataclass(frozen=True, slots=True)
class IncidentPlan:
    """Immutable snapshot of an incomplete suite and its transactional destination."""

    repository_root: Path
    repository_identity: DirectoryIdentity
    raw_root: Path
    raw_root_identity: DirectoryIdentity
    campaign_root: Path
    campaign_root_identity: DirectoryIdentity
    archive_root: Path
    destination: Path
    staging: Path
    source_commit: str
    label: str
    created_at: str
    experiment_ids: tuple[str, ...]
    states: tuple[ExperimentState, ...]
    entries: tuple[ArchiveEntry, ...]
    source_identities: tuple[DirectoryIdentity, ...]

    @property
    def inventory_sha256(self) -> str:
        return sha256_value([entry.manifest_value() for entry in self.entries])

    @property
    def suite_state_sha256(self) -> str:
        return sha256_value([state.manifest_value() for state in self.states])

    @property
    def file_count(self) -> int:
        return sum(entry.kind == "file" for entry in self.entries)

    @property
    def directory_count(self) -> int:
        return sum(entry.kind == "directory" for entry in self.entries)

    @property
    def total_bytes(self) -> int:
        return sum(entry.size_bytes or 0 for entry in self.entries)


@dataclass(frozen=True, slots=True)
class ScaffoldIdentity:
    """Pinned filesystem identities for one transaction-owned scaffold."""

    container: DirectoryIdentity
    raw: DirectoryIdentity
    campaigns: DirectoryIdentity


def _validate_partial_root(root: Path, experiment_ids: Sequence[str]) -> None:
    expected = set(experiment_ids)
    for child in sorted(root.iterdir(), key=lambda item: item.name):
        if complete_archive._is_linklike(child):
            raise EvidenceArchiveError(f"source root contains a symlink: {child}")
        if child.name == ".gitkeep" and child.is_file():
            continue
        if child.name not in expected or not child.is_dir():
            raise EvidenceArchiveError(f"unexpected entry in source root: {child}")


def _regular_files(root: Path) -> tuple[Path, ...]:
    files: list[Path] = []
    for path in sorted(root.rglob("*"), key=lambda item: item.relative_to(root).as_posix()):
        if complete_archive._is_linklike(path):
            raise EvidenceArchiveError(f"evidence tree contains a symlink: {path}")
        if path.is_dir():
            continue
        if not path.is_file():
            raise EvidenceArchiveError(f"evidence tree contains a non-regular entry: {path}")
        files.append(path)
    return tuple(files)


def _validate_raw_records(directory: Path, experiment_id: str, source_commit: str) -> int:
    files = _regular_files(directory)
    for path in files:
        if path.suffix != ".json":
            raise EvidenceArchiveError(f"raw experiment contains a non-JSON file: {path}")
        record = complete_archive._json_object(path, label="raw record")
        if record.get("experiment_id") != experiment_id:
            raise EvidenceArchiveError(f"raw record experiment ID does not match its root: {path}")
        provenance = record.get("provenance")
        commit = provenance.get("git_commit") if isinstance(provenance, Mapping) else None
        if commit != source_commit:
            raise EvidenceArchiveError(f"raw record is not bound to {source_commit}: {path}")
    return len(files)


def _validate_campaign_manifest(directory: Path, experiment_id: str, source_commit: str) -> str:
    manifest_path = directory / "experiment-manifest.json"
    manifest = complete_archive._json_object(manifest_path, label="experiment manifest")
    if manifest.get("experiment_id") != experiment_id:
        raise EvidenceArchiveError(
            f"experiment manifest ID does not match its root: {manifest_path}"
        )
    declared_hash = manifest.get("manifest_sha256")
    unsigned = dict(manifest)
    unsigned.pop("manifest_sha256", None)
    if not isinstance(declared_hash, str) or declared_hash != sha256_value(unsigned):
        raise EvidenceArchiveError(f"experiment manifest hash is invalid: {manifest_path}")
    validation = manifest.get("dataset_validation")
    validator_commit = (
        validation.get("validator_git_commit") if isinstance(validation, Mapping) else None
    )
    if validator_commit != source_commit:
        raise EvidenceArchiveError(
            f"experiment manifest is not bound to {source_commit}: {manifest_path}"
        )
    return declared_hash


def _count_run_attempts(directory: Path) -> int:
    runs = directory / "runs"
    if not runs.is_dir() or complete_archive._is_linklike(runs):
        return 0
    return sum(
        path.is_dir()
        and not complete_archive._is_linklike(path)
        and _ATTEMPT_PATTERN.fullmatch(path.name) is not None
        for path in runs.glob("*/attempt-*")
    )


def _count_failed_attempts(directory: Path) -> int:
    failures = directory / "failed-attempts"
    if not failures.is_dir() or complete_archive._is_linklike(failures):
        return 0
    return sum(path.is_file() and path.suffix == ".json" for path in failures.rglob("*"))


def _nonnegative_integer(value: object) -> int | None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        return None
    return value


def _verification_status(
    directory: Path,
    *,
    experiment_id: str,
    experiment_manifest_sha256: str,
    raw_record_count: int,
) -> tuple[str | None, bool]:
    base = directory / "campaign-verification.json"
    candidates: dict[int, Path] = {}
    if base.exists() or complete_archive._is_linklike(base):
        if complete_archive._is_linklike(base) or not base.is_file():
            raise EvidenceArchiveError(f"campaign verification is not a regular file: {base}")
        candidates[1] = base
    for path in directory.iterdir():
        match = _VERIFICATION_ATTEMPT_PATTERN.fullmatch(path.name)
        if match is None:
            continue
        if complete_archive._is_linklike(path) or not path.is_file():
            raise EvidenceArchiveError(f"campaign verification is not a regular file: {path}")
        attempt = int(match.group("attempt"))
        if attempt < 2 or attempt in candidates:
            raise EvidenceArchiveError(f"invalid duplicate campaign verification: {path}")
        candidates[attempt] = path
    attempts = tuple(sorted(candidates.items()))
    if attempts and [attempt for attempt, _path in attempts] != list(range(1, attempts[-1][0] + 1)):
        raise EvidenceArchiveError(
            f"campaign verification attempt sequence is not contiguous: {base}"
        )
    if not attempts:
        return None, False
    path = attempts[-1][1]
    value = complete_archive._json_object(path, label="campaign verification")
    status = value.get("status")
    if not isinstance(status, str) or not status:
        raise EvidenceArchiveError(f"campaign verification has no valid status: {path}")
    report = value.get("report")
    schema_version = _nonnegative_integer(value.get("schema_version"))
    planned = _nonnegative_integer(report.get("planned")) if isinstance(report, Mapping) else None
    executed = _nonnegative_integer(report.get("executed")) if isinstance(report, Mapping) else None
    resumed = _nonnegative_integer(report.get("resumed")) if isinstance(report, Mapping) else None
    succeeded = (
        _nonnegative_integer(report.get("succeeded")) if isinstance(report, Mapping) else None
    )
    failed = _nonnegative_integer(report.get("failed")) if isinstance(report, Mapping) else None
    reported_raw_record_count = (
        _nonnegative_integer(report.get("raw_record_count"))
        if isinstance(report, Mapping)
        else None
    )
    passed_completion = (
        schema_version == 1
        and status == "passed"
        and isinstance(report, Mapping)
        and planned is not None
        and executed is not None
        and resumed is not None
        and succeeded is not None
        and failed is not None
        and reported_raw_record_count is not None
        and report.get("complete") is True
        and report.get("experiment_id") == experiment_id
        and report.get("experiment_manifest_sha256") == experiment_manifest_sha256
        and reported_raw_record_count == raw_record_count
        and planned == raw_record_count
        and executed + resumed == planned
        and succeeded == planned
        and failed == 0
    )
    return status, passed_completion


def _inspect_experiment(
    raw_directory: Path,
    campaign_directory: Path,
    experiment_id: str,
    source_commit: str,
) -> ExperimentState:
    if raw_directory.exists() or complete_archive._is_linklike(raw_directory):
        if complete_archive._is_linklike(raw_directory) or not raw_directory.is_dir():
            raise EvidenceArchiveError(
                f"raw experiment root is not a real directory: {raw_directory}"
            )
        raw_count = _validate_raw_records(raw_directory, experiment_id, source_commit)
        raw_state = "records" if raw_count else "empty"
    else:
        raw_count = 0
        raw_state = "missing"

    if campaign_directory.exists() or complete_archive._is_linklike(campaign_directory):
        if complete_archive._is_linklike(campaign_directory) or not campaign_directory.is_dir():
            raise EvidenceArchiveError(
                f"campaign experiment root is not a real directory: {campaign_directory}"
            )
        _regular_files(campaign_directory)
        manifest_sha256 = _validate_campaign_manifest(
            campaign_directory, experiment_id, source_commit
        )
        run_attempt_count = _count_run_attempts(campaign_directory)
        failed_attempt_count = _count_failed_attempts(campaign_directory)
        verification_status, passed_completion = _verification_status(
            campaign_directory,
            experiment_id=experiment_id,
            experiment_manifest_sha256=manifest_sha256,
            raw_record_count=raw_count,
        )
        if passed_completion:
            campaign_state = "verified"
        elif run_attempt_count or failed_attempt_count or raw_count:
            campaign_state = "started"
        else:
            campaign_state = "plan-only"
    else:
        run_attempt_count = 0
        failed_attempt_count = 0
        verification_status = None
        campaign_state = "missing"

    return ExperimentState(
        experiment_id=experiment_id,
        raw_root_state=raw_state,
        raw_record_count=raw_count,
        campaign_root_state=campaign_state,
        run_attempt_count=run_attempt_count,
        failed_attempt_count=failed_attempt_count,
        verification_status=verification_status,
    )


def _inspect_suite(
    raw_root: Path,
    campaign_root: Path,
    experiment_ids: Sequence[str],
    source_commit: str,
) -> tuple[ExperimentState, ...]:
    _validate_partial_root(raw_root, experiment_ids)
    _validate_partial_root(campaign_root, experiment_ids)
    return tuple(
        _inspect_experiment(
            raw_root / experiment_id,
            campaign_root / experiment_id,
            experiment_id,
            source_commit,
        )
        for experiment_id in experiment_ids
    )


def _suite_is_complete(states: Sequence[ExperimentState]) -> bool:
    return all(
        state.raw_record_count > 0
        and state.campaign_root_state == "verified"
        and state.verification_status == "passed"
        for state in states
    )


def _scan_inventory(
    raw_root: Path, campaign_root: Path, states: Sequence[ExperimentState]
) -> tuple[ArchiveEntry, ...]:
    entries: list[ArchiveEntry] = []
    for state in states:
        if state.raw_present:
            entries.extend(
                complete_archive._scan_directory(
                    raw_root / state.experiment_id, f"raw/{state.experiment_id}"
                )
            )
        if state.campaign_present:
            entries.extend(
                complete_archive._scan_directory(
                    campaign_root / state.experiment_id, f"campaigns/{state.experiment_id}"
                )
            )
    return _canonical_entries(entries)


def _canonical_entries(entries: Sequence[ArchiveEntry]) -> tuple[ArchiveEntry, ...]:
    ordered = sorted(entries, key=lambda entry: (entry.path.casefold(), entry.path, entry.kind))
    folded: dict[str, str] = {}
    for entry in ordered:
        collision = folded.get(entry.path.casefold())
        if collision is not None and collision != entry.path:
            raise EvidenceArchiveError(
                f"case-colliding evidence paths are forbidden: {collision}, {entry.path}"
            )
        folded[entry.path.casefold()] = entry.path
    return tuple(ordered)


def _present_source_directories(
    raw_root: Path,
    campaign_root: Path,
    states: Sequence[ExperimentState],
) -> tuple[Path, ...]:
    return tuple(
        [raw_root / state.experiment_id for state in states if state.raw_present]
        + [campaign_root / state.experiment_id for state in states if state.campaign_present]
    )


def plan_incident_archive(
    repository_root: Path,
    *,
    raw_root: Path,
    campaign_root: Path,
    archive_root: Path,
    source_commit: str,
    label: str,
    experiment_ids: Sequence[str],
    now: datetime | None = None,
) -> IncidentPlan:
    """Validate and snapshot one incomplete suite without writing any archive data."""

    complete_archive._validate_identity(source_commit, label)
    identifiers = complete_archive._validate_experiment_ids(experiment_ids)
    repository = repository_root.resolve(strict=True)
    raw = complete_archive._existing_repository_directory(repository, raw_root, label="raw root")
    campaigns = complete_archive._existing_repository_directory(
        repository, campaign_root, label="campaign root"
    )
    archives = complete_archive._archive_root_path(repository, archive_root)
    complete_archive._validate_source_roots(raw, campaigns, archives)
    repository_identity = _directory_identity(repository, label="repository root")
    raw_identity = _directory_identity(raw, label="raw root")
    campaign_identity = _directory_identity(campaigns, label="campaign root")
    destination = archives / source_commit / label
    staging = destination.parent / f".{label}.staging"
    complete_archive._validate_archive_destination(repository, destination, staging)
    if destination.exists() or complete_archive._is_linklike(destination):
        raise EvidenceArchiveError(
            f"refusing to overwrite existing incident archive: {destination}"
        )
    if staging.exists() or complete_archive._is_linklike(staging):
        raise EvidenceArchiveError(
            f"stale incident transaction exists: {staging}; run --rollback-staging first"
        )
    states = _inspect_suite(raw, campaigns, identifiers, source_commit)
    if _suite_is_complete(states):
        raise EvidenceArchiveError(
            "suite is complete; use archive_research_evidence.py so publication semantics "
            "remain strict"
        )
    if not any(state.raw_present or state.campaign_present for state in states):
        raise EvidenceArchiveError("no partial research evidence exists to archive")
    entries = _scan_inventory(raw, campaigns, states)
    source_directories = _present_source_directories(raw, campaigns, states)
    source_identities = tuple(
        _directory_identity(path, label="planned experiment source") for path in source_directories
    )
    _assert_directory_identity(repository, repository_identity, label="repository root")
    _assert_directory_identity(raw, raw_identity, label="raw root")
    _assert_directory_identity(campaigns, campaign_identity, label="campaign root")
    for path, identity in zip(source_directories, source_identities, strict=True):
        _assert_directory_identity(path, identity, label="planned experiment source")
    return IncidentPlan(
        repository_root=repository,
        repository_identity=repository_identity,
        raw_root=raw,
        raw_root_identity=raw_identity,
        campaign_root=campaigns,
        campaign_root_identity=campaign_identity,
        archive_root=archives,
        destination=destination,
        staging=staging,
        source_commit=source_commit,
        label=label,
        created_at=complete_archive._iso_utc(now or datetime.now(UTC)),
        experiment_ids=identifiers,
        states=states,
        entries=entries,
        source_identities=source_identities,
    )


def _relative_to_repository(plan: IncidentPlan, path: Path) -> str:
    return path.relative_to(plan.repository_root).as_posix()


def _identity_value(identity: DirectoryIdentity) -> dict[str, int]:
    return {"st_dev": identity[0], "st_ino": identity[1]}


def _identity_from_value(value: object, *, label: str) -> DirectoryIdentity:
    if not isinstance(value, Mapping) or set(value) != {"st_dev", "st_ino"}:
        raise EvidenceArchiveError(f"{label} has invalid filesystem identity fields")
    device = value.get("st_dev")
    inode = value.get("st_ino")
    if type(device) is not int or type(inode) is not int or device < 0 or inode <= 0:
        raise EvidenceArchiveError(f"{label} has an invalid filesystem identity")
    return device, inode


def _move_pairs(plan: IncidentPlan, container: Path) -> tuple[tuple[Path, Path], ...]:
    pairs: list[tuple[Path, Path]] = []
    for state in plan.states:
        if state.raw_present:
            pairs.append(
                (
                    plan.raw_root / state.experiment_id,
                    container / "raw" / state.experiment_id,
                )
            )
    for state in plan.states:
        if state.campaign_present:
            pairs.append(
                (
                    plan.campaign_root / state.experiment_id,
                    container / "campaigns" / state.experiment_id,
                )
            )
    return tuple(pairs)


def _manifest_value(plan: IncidentPlan) -> dict[str, Any]:
    states = [state.manifest_value() for state in plan.states]
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "artifact_class": ARCHIVE_CLASS,
        "archive_purpose": ARCHIVE_PURPOSE,
        "publication_eligible": False,
        "source_commit": plan.source_commit,
        "archive_label": plan.label,
        "created_at": plan.created_at,
        "source_roots": {
            "raw": _relative_to_repository(plan, plan.raw_root),
            "campaigns": _relative_to_repository(plan, plan.campaign_root),
        },
        "experiment_ids": list(plan.experiment_ids),
        "suite_state": states,
        "suite_state_sha256": sha256_value(states),
        "inventory_scope": (
            "all present experiment directories and regular files; missing roots are bound by "
            "suite_state"
        ),
        "entries": [entry.manifest_value() for entry in plan.entries],
        "summary": {
            "present_source_directories": len(_move_pairs(plan, plan.staging)),
            "file_count": plan.file_count,
            "directory_count": plan.directory_count,
            "total_bytes": plan.total_bytes,
        },
        "inventory_sha256": plan.inventory_sha256,
        "restore_notice": (
            "Diagnostic evidence only; never use this archive as publishable benchmark evidence. "
            "Verify the manifest, then restore only into empty canonical experiment roots."
        ),
    }
    manifest["manifest_sha256"] = sha256_value(manifest)
    return manifest


def _journal_value(plan: IncidentPlan, scaffold_identity: ScaffoldIdentity) -> dict[str, Any]:
    states = [state.manifest_value() for state in plan.states]
    move_pairs = _move_pairs(plan, plan.staging)
    journal: dict[str, Any] = {
        "schema_version": 2,
        "artifact_class": JOURNAL_CLASS,
        "archive_purpose": ARCHIVE_PURPOSE,
        "source_commit": plan.source_commit,
        "archive_label": plan.label,
        "inventory_sha256": plan.inventory_sha256,
        "suite_state": states,
        "suite_state_sha256": sha256_value(states),
        "source_root_identities": {
            "raw": _identity_value(plan.raw_root_identity),
            "campaigns": _identity_value(plan.campaign_root_identity),
        },
        "scaffold_identities": {
            "container": _identity_value(scaffold_identity.container),
            "raw": _identity_value(scaffold_identity.raw),
            "campaigns": _identity_value(scaffold_identity.campaigns),
        },
        "moves": [
            {
                "source": _relative_to_repository(plan, source),
                "staged": destination.relative_to(plan.staging).as_posix(),
                "source_identity": _identity_value(source_identity),
            }
            for (source, destination), source_identity in zip(
                move_pairs, plan.source_identities, strict=True
            )
        ],
    }
    journal["journal_sha256"] = sha256_value(journal)
    return journal


def _assert_unchanged(plan: IncidentPlan) -> None:
    source_directories = _present_source_directories(plan.raw_root, plan.campaign_root, plan.states)
    for path, identity in zip(source_directories, plan.source_identities, strict=True):
        _assert_directory_identity(path, identity, label="planned experiment source")
    states = _inspect_suite(
        plan.raw_root, plan.campaign_root, plan.experiment_ids, plan.source_commit
    )
    if states != plan.states:
        raise EvidenceArchiveError("partial evidence state changed after planning")
    if _scan_inventory(plan.raw_root, plan.campaign_root, states) != plan.entries:
        raise EvidenceArchiveError("partial evidence changed after planning")
    for path, identity in zip(source_directories, plan.source_identities, strict=True):
        _assert_directory_identity(path, identity, label="planned experiment source")


def _revalidate_plan_paths(plan: IncidentPlan) -> None:
    try:
        repository = plan.repository_root.resolve(strict=True)
    except OSError as error:
        raise EvidenceArchiveError("planned repository root is no longer available") from error
    if repository != plan.repository_root:
        raise EvidenceArchiveError("planned repository root identity changed")
    _assert_directory_identity(
        repository, plan.repository_identity, label="planned repository root"
    )
    raw = complete_archive._existing_repository_directory(
        repository, plan.raw_root, label="raw root"
    )
    campaigns = complete_archive._existing_repository_directory(
        repository, plan.campaign_root, label="campaign root"
    )
    archives = complete_archive._archive_root_path(repository, plan.archive_root)
    if raw != plan.raw_root or campaigns != plan.campaign_root or archives != plan.archive_root:
        raise EvidenceArchiveError("planned evidence-root identity changed")
    _assert_directory_identity(raw, plan.raw_root_identity, label="planned raw root")
    _assert_directory_identity(
        campaigns, plan.campaign_root_identity, label="planned campaign root"
    )
    complete_archive._validate_source_roots(raw, campaigns, archives)
    expected_destination = archives / plan.source_commit / plan.label
    expected_staging = expected_destination.parent / f".{plan.label}.staging"
    if plan.destination != expected_destination or plan.staging != expected_staging:
        raise EvidenceArchiveError("planned archive destination identity changed")
    complete_archive._validate_archive_destination(repository, plan.destination, plan.staging)


def _scan_archived_inventory(
    archive: Path, states: Sequence[ExperimentState]
) -> tuple[ArchiveEntry, ...]:
    raw = archive / "raw"
    campaigns = archive / "campaigns"
    for root, present_ids in (
        (raw, {state.experiment_id for state in states if state.raw_present}),
        (campaigns, {state.experiment_id for state in states if state.campaign_present}),
    ):
        observed_ids: set[str] = set()
        for child in root.iterdir():
            if complete_archive._is_linklike(child) or not child.is_dir():
                raise EvidenceArchiveError(f"unexpected incident payload entry: {child}")
            observed_ids.add(child.name)
        if observed_ids != present_ids:
            raise EvidenceArchiveError(
                f"incident payload roots differ from suite state under {root}"
            )
    for state in states:
        for present, path in (
            (state.raw_present, raw / state.experiment_id),
            (state.campaign_present, campaigns / state.experiment_id),
        ):
            if present and (complete_archive._is_linklike(path) or not path.is_dir()):
                raise EvidenceArchiveError(f"incident archive is missing a present root: {path}")
            if not present and (path.exists() or complete_archive._is_linklike(path)):
                raise EvidenceArchiveError(f"incident archive materialized a missing root: {path}")
    return _scan_inventory(raw, campaigns, states)


def _states_from_manifest(
    value: object, experiment_ids: Sequence[str]
) -> tuple[ExperimentState, ...]:
    if not isinstance(value, list) or len(value) != len(experiment_ids):
        raise EvidenceArchiveError("incident suite state does not match the experiment set")
    states: list[ExperimentState] = []
    required = {
        "experiment_id",
        "raw_root_state",
        "raw_record_count",
        "campaign_root_state",
        "run_attempt_count",
        "failed_attempt_count",
        "verification_status",
    }
    for expected_id, item in zip(experiment_ids, value, strict=True):
        if not isinstance(item, Mapping) or set(item) != required:
            raise EvidenceArchiveError("incident suite state has invalid fields")
        if item.get("experiment_id") != expected_id:
            raise EvidenceArchiveError("incident suite state order or identity is invalid")
        raw_state = item.get("raw_root_state")
        campaign_state = item.get("campaign_root_state")
        verification_status = item.get("verification_status")
        counts = (
            item.get("raw_record_count"),
            item.get("run_attempt_count"),
            item.get("failed_attempt_count"),
        )
        if raw_state not in {"missing", "empty", "records"}:
            raise EvidenceArchiveError("incident raw-root state is invalid")
        if campaign_state not in {"missing", "plan-only", "started", "verified"}:
            raise EvidenceArchiveError("incident campaign-root state is invalid")
        if verification_status is not None and not isinstance(verification_status, str):
            raise EvidenceArchiveError("incident verification status is invalid")
        if any(type(count) is not int or count < 0 for count in counts):
            raise EvidenceArchiveError("incident suite counts are invalid")
        states.append(
            ExperimentState(
                experiment_id=expected_id,
                raw_root_state=str(raw_state),
                raw_record_count=cast(int, counts[0]),
                campaign_root_state=str(campaign_state),
                run_attempt_count=cast(int, counts[1]),
                failed_attempt_count=cast(int, counts[2]),
                verification_status=verification_status,
            )
        )
    return tuple(states)


def verify_incident_archive(
    archive_dir: Path, *, _allow_transaction_journal: bool = False
) -> dict[str, Any]:
    """Re-hash a completed diagnostic archive and validate its provenance and absence states."""

    if complete_archive._is_linklike(archive_dir) or not archive_dir.is_dir():
        raise EvidenceArchiveError(f"incident archive is not a real directory: {archive_dir}")
    archive = archive_dir.resolve(strict=True)
    allowed = {"raw", "campaigns", MANIFEST_NAME}
    if _allow_transaction_journal:
        allowed.add(JOURNAL_NAME)
    observed = {path.name for path in archive.iterdir()}
    if observed != allowed:
        raise EvidenceArchiveError(
            f"incident archive root entries do not match the contract: {sorted(observed)}"
        )
    for name in ("raw", "campaigns"):
        directory = archive / name
        if complete_archive._is_linklike(directory) or not directory.is_dir():
            raise EvidenceArchiveError(f"incident archive payload root is invalid: {directory}")
    if _allow_transaction_journal:
        journal_path = archive / JOURNAL_NAME
        if complete_archive._is_linklike(journal_path) or not journal_path.is_file():
            raise EvidenceArchiveError("incident transaction journal is missing or unsafe")
    manifest_path = archive / MANIFEST_NAME
    if complete_archive._is_linklike(manifest_path) or not manifest_path.is_file():
        raise EvidenceArchiveError("incident manifest is missing or unsafe")
    manifest = complete_archive._json_object(manifest_path, label="incident manifest")
    if manifest_path.read_bytes() != canonical_json_bytes(manifest) + b"\n":
        raise EvidenceArchiveError("incident manifest is not canonical JSON")
    required = {
        "schema_version",
        "artifact_class",
        "archive_purpose",
        "publication_eligible",
        "source_commit",
        "archive_label",
        "created_at",
        "source_roots",
        "experiment_ids",
        "suite_state",
        "suite_state_sha256",
        "inventory_scope",
        "entries",
        "summary",
        "inventory_sha256",
        "restore_notice",
        "manifest_sha256",
    }
    if set(manifest) != required or manifest.get("schema_version") != 1:
        raise EvidenceArchiveError("incident manifest fields do not match the v1 contract")
    if (
        manifest.get("artifact_class") != ARCHIVE_CLASS
        or manifest.get("archive_purpose") != ARCHIVE_PURPOSE
        or manifest.get("publication_eligible") is not False
    ):
        raise EvidenceArchiveError("incident archive is not marked diagnostic-only")
    declared_manifest_hash = manifest.get("manifest_sha256")
    unsigned = dict(manifest)
    unsigned.pop("manifest_sha256", None)
    if declared_manifest_hash != sha256_value(unsigned):
        raise EvidenceArchiveError("incident manifest self-hash is invalid")
    source_commit = manifest.get("source_commit")
    label = manifest.get("archive_label")
    if not isinstance(source_commit, str) or not isinstance(label, str):
        raise EvidenceArchiveError("incident archive identity is invalid")
    complete_archive._validate_identity(source_commit, label)
    created_at = manifest.get("created_at")
    if (
        not isinstance(created_at, str)
        or complete_archive._UTC_SECONDS_PATTERN.fullmatch(created_at) is None
    ):
        raise EvidenceArchiveError("incident archive timestamp is invalid")
    try:
        datetime.fromisoformat(created_at.removesuffix("Z") + "+00:00")
    except ValueError as error:
        raise EvidenceArchiveError("incident archive timestamp is invalid") from error
    complete_archive._validate_manifest_source_roots(manifest.get("source_roots"))
    experiment_ids_value = manifest.get("experiment_ids")
    if not isinstance(experiment_ids_value, list) or not all(
        isinstance(item, str) for item in experiment_ids_value
    ):
        raise EvidenceArchiveError("incident manifest experiment IDs are invalid")
    experiment_ids = complete_archive._validate_experiment_ids(experiment_ids_value)
    states = _states_from_manifest(manifest.get("suite_state"), experiment_ids)
    state_values = [state.manifest_value() for state in states]
    if manifest.get("suite_state_sha256") != sha256_value(state_values):
        raise EvidenceArchiveError("incident suite-state hash is invalid")
    if _suite_is_complete(states):
        raise EvidenceArchiveError("complete evidence cannot use the diagnostic archive contract")
    entries = _scan_archived_inventory(archive, states)
    expected_entries = [entry.manifest_value() for entry in entries]
    if manifest.get("entries") != expected_entries:
        raise EvidenceArchiveError("incident payload does not match the manifest inventory")
    if manifest.get("inventory_sha256") != sha256_value(expected_entries):
        raise EvidenceArchiveError("incident inventory hash is invalid")
    expected_summary = {
        "present_source_directories": sum(
            state.raw_present + state.campaign_present for state in states
        ),
        "file_count": sum(entry.kind == "file" for entry in entries),
        "directory_count": sum(entry.kind == "directory" for entry in entries),
        "total_bytes": sum(entry.size_bytes or 0 for entry in entries),
    }
    if manifest.get("summary") != expected_summary:
        raise EvidenceArchiveError("incident archive summary does not match its payload")
    observed_states = _inspect_suite(
        archive / "raw", archive / "campaigns", experiment_ids, source_commit
    )
    if observed_states != states:
        raise EvidenceArchiveError("incident suite state does not match its payload")
    return manifest


def _capture_scaffold_identity(container: Path) -> ScaffoldIdentity:
    identity = ScaffoldIdentity(
        container=_directory_identity(container, label="transaction scaffold"),
        raw=_directory_identity(container / "raw", label="transaction raw root"),
        campaigns=_directory_identity(container / "campaigns", label="transaction campaign root"),
    )
    _assert_scaffold_identity(container, identity)
    return identity


def _assert_scaffold_identity(container: Path, expected: ScaffoldIdentity) -> None:
    _assert_directory_identity(container, expected.container, label="owned transaction scaffold")
    _assert_directory_identity(container / "raw", expected.raw, label="owned transaction raw root")
    _assert_directory_identity(
        container / "campaigns",
        expected.campaigns,
        label="owned transaction campaign root",
    )


def _linux_rename_no_replace(
    source: bytes,
    destination: bytes,
    *,
    source_directory_fd: int = -100,
    destination_directory_fd: int = -100,
) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    try:
        renameat2 = libc.renameat2
    except AttributeError as error:
        raise EvidenceArchiveError(
            "the Linux runtime does not provide atomic no-replace directory publication"
        ) from error
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    rename_noreplace = 1
    if (
        renameat2(
            source_directory_fd,
            source,
            destination_directory_fd,
            destination,
            rename_noreplace,
        )
        != 0
    ):
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number), str(destination))


def _publish_directory_no_replace(source: Path, destination: Path) -> None:
    """Atomically publish a directory while refusing any existing destination."""

    if os.name == "nt":
        source.rename(destination)
        return
    if not sys.platform.startswith("linux"):
        raise EvidenceArchiveError(
            "atomic no-replace directory publication is unsupported on this platform"
        )
    try:
        _linux_rename_no_replace(os.fsencode(source), os.fsencode(destination))
    except OSError as error:
        if error.errno not in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP}:
            raise
        _windows_host_directory_move_no_replace(source, destination)


def _windows_host_path(path: Path) -> str:
    match = re.fullmatch(r"/mnt/(?P<drive>[A-Za-z])(?:/(?P<tail>.*))?", path.absolute().as_posix())
    if match is None:
        raise EvidenceArchiveError(
            "atomic no-replace fallback is available only for Windows-mounted WSL paths"
        )
    tail = (match.group("tail") or "").replace("/", "\\")
    return f"\\\\?\\{match.group('drive').upper()}:\\{tail}"


def _windows_host_directory_move_no_replace(source: Path, destination: Path) -> None:
    """Use Directory.Move on DrvFS, where Linux RENAME_NOREPLACE is unsupported."""

    environment = os.environ.copy()
    environment["CODEX_INCIDENT_MOVE_SOURCE"] = _windows_host_path(source)
    environment["CODEX_INCIDENT_MOVE_DESTINATION"] = _windows_host_path(destination)
    forwarded = [
        item
        for item in environment.get("WSLENV", "").split(":")
        if item
        and item.split("/", maxsplit=1)[0]
        not in {"CODEX_INCIDENT_MOVE_SOURCE", "CODEX_INCIDENT_MOVE_DESTINATION"}
    ]
    forwarded.extend(["CODEX_INCIDENT_MOVE_SOURCE", "CODEX_INCIDENT_MOVE_DESTINATION"])
    environment["WSLENV"] = ":".join(forwarded)
    command = (
        "$ErrorActionPreference='Stop'; "
        "[IO.Directory]::Move($env:CODEX_INCIDENT_MOVE_SOURCE, "
        "$env:CODEX_INCIDENT_MOVE_DESTINATION)"
    )
    completed = subprocess.run(
        ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", command],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise EvidenceArchiveError(
            f"Windows host refused atomic no-replace directory move: {detail}"
        )


def _rebind_journal_scaffold_identity(
    journal_path: Path, scaffold_identity: ScaffoldIdentity
) -> None:
    journal = complete_archive._json_object(
        journal_path, label="compensated incident transaction journal"
    )
    if journal.get("schema_version") != 2 or journal.get("artifact_class") != JOURNAL_CLASS:
        raise EvidenceArchiveError("cannot rebind an unknown transaction journal")
    declared_hash = journal.get("journal_sha256")
    unsigned = dict(journal)
    unsigned.pop("journal_sha256", None)
    if declared_hash != sha256_value(unsigned):
        raise EvidenceArchiveError("cannot rebind a transaction journal with an invalid hash")
    journal["scaffold_identities"] = {
        "container": _identity_value(scaffold_identity.container),
        "raw": _identity_value(scaffold_identity.raw),
        "campaigns": _identity_value(scaffold_identity.campaigns),
    }
    journal.pop("journal_sha256", None)
    journal["journal_sha256"] = sha256_value(journal)
    write_json(journal_path, journal, immutable=False)


def _remove_transaction_scaffold(
    container: Path,
    expected_identity: ScaffoldIdentity,
    *,
    _retry_after_compensation: bool = True,
) -> None:
    _assert_scaffold_identity(container, expected_identity)
    allowed = {"raw", "campaigns", MANIFEST_NAME, JOURNAL_NAME}
    unexpected = {path.name for path in container.iterdir()} - allowed
    if unexpected:
        raise EvidenceArchiveError(
            f"transaction scaffold contains unexpected entries: {sorted(unexpected)}"
        )

    directories: list[Path] = []
    for name in ("raw", "campaigns"):
        directory = container / name
        if directory.exists() or complete_archive._is_linklike(directory):
            if complete_archive._is_linklike(directory) or not directory.is_dir():
                raise EvidenceArchiveError(f"transaction payload root is unsafe: {directory}")
            directories.append(directory)

    file_snapshots: list[tuple[Path, bytes]] = []
    for name in (MANIFEST_NAME, JOURNAL_NAME):
        path = container / name
        if path.exists() or complete_archive._is_linklike(path):
            if complete_archive._is_linklike(path) or not path.is_file():
                raise EvidenceArchiveError(f"transaction metadata is unsafe: {path}")
            file_snapshots.append((path, path.read_bytes()))

    removed_directories: set[Path] = set()
    removed_files: set[Path] = set()
    try:
        for directory in directories:
            _assert_directory_identity(
                container, expected_identity.container, label="owned transaction scaffold"
            )
            expected_directory = (
                expected_identity.raw if directory.name == "raw" else expected_identity.campaigns
            )
            _assert_directory_identity(
                directory, expected_directory, label="owned transaction payload root"
            )
            directory.rmdir()
            removed_directories.add(directory)
        for path, _payload in file_snapshots:
            _assert_directory_identity(
                container, expected_identity.container, label="owned transaction scaffold"
            )
            path.unlink()
            removed_files.add(path)
        _assert_directory_identity(
            container, expected_identity.container, label="owned transaction scaffold"
        )
        container.rmdir()
    except (OSError, EvidenceArchiveError) as error:
        restore_errors: list[str] = []
        try:
            if container.exists() or complete_archive._is_linklike(container):
                _assert_directory_identity(
                    container,
                    expected_identity.container,
                    label="owned transaction scaffold",
                )
            else:
                raise EvidenceArchiveError(
                    f"owned transaction scaffold disappeared during cleanup: {container}"
                )
            for directory in directories:
                if directory.exists() or complete_archive._is_linklike(directory):
                    if directory in removed_directories:
                        raise EvidenceArchiveError(
                            f"removed transaction payload root unexpectedly reappeared: {directory}"
                        )
                    if complete_archive._is_linklike(directory) or not directory.is_dir():
                        raise EvidenceArchiveError(
                            f"cannot restore transaction payload root: {directory}"
                        )
                    expected_directory = (
                        expected_identity.raw
                        if directory.name == "raw"
                        else expected_identity.campaigns
                    )
                    _assert_directory_identity(
                        directory,
                        expected_directory,
                        label="retained transaction payload root",
                    )
                else:
                    if directory not in removed_directories:
                        raise EvidenceArchiveError(
                            f"transaction payload root disappeared during cleanup: {directory}"
                        )
                    directory.mkdir()
            for path, payload in file_snapshots:
                if path.exists() or complete_archive._is_linklike(path):
                    if path in removed_files:
                        raise EvidenceArchiveError(
                            f"removed transaction metadata unexpectedly reappeared: {path}"
                        )
                    if (
                        complete_archive._is_linklike(path)
                        or not path.is_file()
                        or path.read_bytes() != payload
                    ):
                        raise EvidenceArchiveError(f"cannot restore transaction metadata: {path}")
                else:
                    if path not in removed_files:
                        raise EvidenceArchiveError(
                            f"transaction metadata disappeared during cleanup: {path}"
                        )
                    path.write_bytes(payload)
            journal_path = container / JOURNAL_NAME
            if journal_path.is_file() and not complete_archive._is_linklike(journal_path):
                _rebind_journal_scaffold_identity(
                    journal_path, _capture_scaffold_identity(container)
                )
        except (OSError, EvidenceArchiveError) as restore_error:
            restore_errors.append(str(restore_error))
        if restore_errors:
            raise EvidenceArchiveError(
                f"transaction scaffold cleanup failed ({error}) and compensation was incomplete: "
                f"{restore_errors}"
            ) from error
        if _retry_after_compensation and not file_snapshots:
            _assert_directory_identity(
                container,
                expected_identity.container,
                label="owned transaction scaffold",
            )
            _remove_transaction_scaffold(
                container,
                _capture_scaffold_identity(container),
                _retry_after_compensation=False,
            )
            return
        raise


def _remove_unjournaled_scaffold(
    container: Path,
    *,
    container_identity: DirectoryIdentity,
    raw_identity: DirectoryIdentity | None,
    campaigns_identity: DirectoryIdentity | None,
    _retry_after_compensation: bool = True,
) -> None:
    """Remove only the exact empty scaffold pinned before journal creation."""

    _assert_directory_identity(
        container, container_identity, label="owned unjournaled transaction scaffold"
    )
    child_identities = (("raw", raw_identity), ("campaigns", campaigns_identity))
    expected_entries = {name for name, identity in child_identities if identity is not None}
    observed_entries = {path.name for path in container.iterdir()}
    if observed_entries != expected_entries:
        raise EvidenceArchiveError(
            "unjournaled transaction scaffold entries changed: "
            f"expected {sorted(expected_entries)}, observed {sorted(observed_entries)}"
        )

    removed_children: set[str] = set()
    try:
        for name, identity in reversed(child_identities):
            if identity is None:
                continue
            child = container / name
            _assert_directory_identity(
                container,
                container_identity,
                label="owned unjournaled transaction scaffold",
            )
            _assert_directory_identity(
                child, identity, label=f"owned unjournaled transaction {name} root"
            )
            child.rmdir()
            removed_children.add(name)
        _assert_directory_identity(
            container, container_identity, label="owned unjournaled transaction scaffold"
        )
        container.rmdir()
    except (OSError, EvidenceArchiveError) as error:
        restore_errors: list[str] = []
        try:
            _assert_directory_identity(
                container,
                container_identity,
                label="owned unjournaled transaction scaffold",
            )
            restored_entries = {path.name for path in container.iterdir()}
            unexpected = restored_entries - expected_entries
            if unexpected:
                raise EvidenceArchiveError(
                    f"unjournaled transaction scaffold changed during cleanup: {sorted(unexpected)}"
                )
            for name, identity in child_identities:
                if identity is None:
                    continue
                child = container / name
                if child.exists() or complete_archive._is_linklike(child):
                    if name in removed_children:
                        raise EvidenceArchiveError(
                            f"removed unjournaled transaction root unexpectedly reappeared: {child}"
                        )
                    _assert_directory_identity(
                        child,
                        identity,
                        label=f"retained unjournaled transaction {name} root",
                    )
                else:
                    if name not in removed_children:
                        raise EvidenceArchiveError(
                            f"unjournaled transaction root disappeared during cleanup: {child}"
                        )
                    child.mkdir()
            _assert_directory_identity(
                container,
                container_identity,
                label="owned unjournaled transaction scaffold",
            )
            restored_entries = {path.name for path in container.iterdir()}
            if restored_entries != expected_entries:
                raise EvidenceArchiveError(
                    "unjournaled transaction scaffold could not be restored exactly: "
                    f"expected {sorted(expected_entries)}, observed {sorted(restored_entries)}"
                )
        except (OSError, EvidenceArchiveError) as restore_error:
            restore_errors.append(str(restore_error))
        if restore_errors:
            raise EvidenceArchiveError(
                f"unjournaled transaction scaffold cleanup failed ({error}) and compensation "
                f"was incomplete: {restore_errors}"
            ) from error
        if _retry_after_compensation:
            _remove_unjournaled_scaffold(
                container,
                container_identity=container_identity,
                raw_identity=(
                    _directory_identity(
                        container / "raw", label="restored unjournaled transaction raw root"
                    )
                    if raw_identity is not None
                    else None
                ),
                campaigns_identity=(
                    _directory_identity(
                        container / "campaigns",
                        label="restored unjournaled transaction campaign root",
                    )
                    if campaigns_identity is not None
                    else None
                ),
                _retry_after_compensation=False,
            )
            return
        raise


def _rename_directory(
    source: Path,
    destination: Path,
    *,
    source_identity: DirectoryIdentity,
    source_parent_identity: DirectoryIdentity,
    destination_parent_identity: DirectoryIdentity,
) -> None:
    """Move one pinned directory between pinned parents without replacing a destination."""

    _assert_directory_identity(source.parent, source_parent_identity, label="move source parent")
    _assert_directory_identity(source, source_identity, label="move source")
    _assert_directory_identity(
        destination.parent,
        destination_parent_identity,
        label="move destination parent",
    )
    if destination.exists() or complete_archive._is_linklike(destination):
        raise EvidenceArchiveError(f"refusing to overwrite move destination: {destination}")

    if sys.platform.startswith("linux"):
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
        source_fd = os.open(source.parent, flags)
        destination_fd = os.open(destination.parent, flags)
        try:
            source_parent_status = os.fstat(source_fd)
            destination_parent_status = os.fstat(destination_fd)
            if (source_parent_status.st_dev, source_parent_status.st_ino) != (
                source_parent_identity
            ):
                raise EvidenceArchiveError("move source parent identity changed before rename")
            if (destination_parent_status.st_dev, destination_parent_status.st_ino) != (
                destination_parent_identity
            ):
                raise EvidenceArchiveError("move destination parent identity changed before rename")
            source_status = os.stat(source.name, dir_fd=source_fd, follow_symlinks=False)
            if (
                not stat.S_ISDIR(source_status.st_mode)
                or (
                    source_status.st_dev,
                    source_status.st_ino,
                )
                != source_identity
            ):
                raise EvidenceArchiveError("move source identity changed before rename")
            try:
                os.stat(destination.name, dir_fd=destination_fd, follow_symlinks=False)
            except FileNotFoundError:
                pass
            else:
                raise EvidenceArchiveError(f"refusing to overwrite move destination: {destination}")
            try:
                _linux_rename_no_replace(
                    os.fsencode(source.name),
                    os.fsencode(destination.name),
                    source_directory_fd=source_fd,
                    destination_directory_fd=destination_fd,
                )
            except OSError as error:
                if error.errno not in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP}:
                    raise
                _assert_directory_identity(
                    source.parent,
                    source_parent_identity,
                    label="DrvFS move source parent",
                )
                _assert_directory_identity(
                    destination.parent,
                    destination_parent_identity,
                    label="DrvFS move destination parent",
                )
                _windows_host_directory_move_no_replace(source, destination)
            destination_status = os.stat(
                destination.name, dir_fd=destination_fd, follow_symlinks=False
            )
            if (
                destination_status.st_dev,
                destination_status.st_ino,
            ) != source_identity:
                raise EvidenceArchiveError("moved directory identity did not reach its destination")
        finally:
            os.close(destination_fd)
            os.close(source_fd)
    elif os.name == "nt":
        source.rename(destination)
    else:
        raise EvidenceArchiveError("atomic pinned directory moves are unsupported on this platform")

    _assert_directory_identity(source.parent, source_parent_identity, label="move source parent")
    _assert_directory_identity(
        destination.parent,
        destination_parent_identity,
        label="move destination parent",
    )
    _assert_directory_identity(destination, source_identity, label="moved directory")


def _rollback_pairs(
    moved: Sequence[tuple[Path, Path, DirectoryIdentity]],
    container: Path,
    scaffold_identity: ScaffoldIdentity,
    source_root_identities: Sequence[tuple[Path, DirectoryIdentity]],
) -> list[str]:
    errors: list[str] = []
    source_root_identity_by_path = dict(source_root_identities)
    for source, destination, moved_identity in reversed(moved):
        try:
            _assert_scaffold_identity(container, scaffold_identity)
            for root, identity in source_root_identities:
                _assert_directory_identity(root, identity, label="rollback source root")
            if source.exists() or complete_archive._is_linklike(source):
                raise EvidenceArchiveError(f"rollback source already exists: {source}")
            if not destination.is_dir() or complete_archive._is_linklike(destination):
                raise EvidenceArchiveError(f"rollback destination is unavailable: {destination}")
            destination_parent_identity = (
                scaffold_identity.raw
                if destination.parent.name == "raw"
                else scaffold_identity.campaigns
            )
            try:
                source_parent_identity = source_root_identity_by_path[source.parent]
            except KeyError as error:
                raise EvidenceArchiveError(
                    f"rollback source parent is not journaled: {source.parent}"
                ) from error
            _rename_directory(
                destination,
                source,
                source_identity=moved_identity,
                source_parent_identity=destination_parent_identity,
                destination_parent_identity=source_parent_identity,
            )
        except (OSError, EvidenceArchiveError) as error:
            errors.append(str(error))
    return errors


def _scan_transaction_inventory(
    raw_root: Path,
    campaign_root: Path,
    container: Path,
    states: Sequence[ExperimentState],
) -> tuple[ArchiveEntry, ...]:
    entries: list[ArchiveEntry] = []
    for state in states:
        for present, source, staged, prefix in (
            (
                state.raw_present,
                raw_root / state.experiment_id,
                container / "raw" / state.experiment_id,
                "raw",
            ),
            (
                state.campaign_present,
                campaign_root / state.experiment_id,
                container / "campaigns" / state.experiment_id,
                "campaigns",
            ),
        ):
            selected = _select_transaction_directory(source, staged, expected_present=present)
            if selected is None:
                continue
            entries.extend(
                complete_archive._scan_directory(selected, f"{prefix}/{state.experiment_id}")
            )
    return _canonical_entries(entries)


def _select_transaction_directory(
    source: Path, staged: Path, *, expected_present: bool
) -> Path | None:
    source_exists = source.exists() or complete_archive._is_linklike(source)
    staged_exists = staged.exists() or complete_archive._is_linklike(staged)
    if not expected_present:
        if source_exists or staged_exists:
            raise EvidenceArchiveError(
                f"journaled missing root unexpectedly exists: {source}, {staged}"
            )
        return None
    source_valid = source_exists and source.is_dir() and not complete_archive._is_linklike(source)
    staged_valid = staged_exists and staged.is_dir() and not complete_archive._is_linklike(staged)
    if (
        source_valid == staged_valid
        or source_exists != source_valid
        or staged_exists != staged_valid
    ):
        raise EvidenceArchiveError(
            f"incident transaction requires exactly one real root copy: {source}, {staged}"
        )
    return source if source_valid else staged


def _inspect_transaction_state(
    raw_root: Path,
    campaign_root: Path,
    container: Path,
    states: Sequence[ExperimentState],
    source_commit: str,
) -> tuple[ExperimentState, ...]:
    observed: list[ExperimentState] = []
    for state in states:
        raw_source = raw_root / state.experiment_id
        raw_staged = container / "raw" / state.experiment_id
        campaign_source = campaign_root / state.experiment_id
        campaign_staged = container / "campaigns" / state.experiment_id
        raw_directory = _select_transaction_directory(
            raw_source, raw_staged, expected_present=state.raw_present
        )
        campaign_directory = _select_transaction_directory(
            campaign_source,
            campaign_staged,
            expected_present=state.campaign_present,
        )
        observed.append(
            _inspect_experiment(
                raw_directory or raw_source,
                campaign_directory or campaign_source,
                state.experiment_id,
                source_commit,
            )
        )
    return tuple(observed)


def _validate_plan_journal(
    plan: IncidentPlan, container: Path, scaffold_identity: ScaffoldIdentity
) -> None:
    journal_path = container / JOURNAL_NAME
    if complete_archive._is_linklike(journal_path) or not journal_path.is_file():
        raise EvidenceArchiveError("incident transaction journal is missing or unsafe")
    expected = canonical_json_bytes(_journal_value(plan, scaffold_identity)) + b"\n"
    if journal_path.read_bytes() != expected:
        raise EvidenceArchiveError("incident transaction journal does not match its plan")


def _validate_plan_transaction(
    plan: IncidentPlan, container: Path, scaffold_identity: ScaffoldIdentity
) -> None:
    _revalidate_plan_paths(plan)
    _assert_scaffold_identity(container, scaffold_identity)
    _validate_transaction_scaffold(container, plan.states)
    _validate_plan_journal(plan, container, scaffold_identity)
    for (source, staged), identity in zip(
        _move_pairs(plan, container), plan.source_identities, strict=True
    ):
        selected = _select_transaction_directory(source, staged, expected_present=True)
        if selected is None:
            raise EvidenceArchiveError("planned transaction source is unavailable")
        _assert_directory_identity(selected, identity, label="planned transaction source")
    observed = _inspect_transaction_state(
        plan.raw_root,
        plan.campaign_root,
        container,
        plan.states,
        plan.source_commit,
    )
    if observed != plan.states:
        raise EvidenceArchiveError("incident transaction payload state changed after planning")
    if (
        _scan_transaction_inventory(
            plan.raw_root,
            plan.campaign_root,
            container,
            plan.states,
        )
        != plan.entries
    ):
        raise EvidenceArchiveError("incident transaction inventory changed after planning")
    _assert_scaffold_identity(container, scaffold_identity)
    _revalidate_plan_paths(plan)


def execute_incident_archive(plan: IncidentPlan) -> Path:
    """Move the exact planned incomplete evidence into a verified diagnostic archive."""

    _revalidate_plan_paths(plan)
    if plan.destination.exists() or complete_archive._is_linklike(plan.destination):
        raise EvidenceArchiveError(
            f"refusing to overwrite existing incident archive: {plan.destination}"
        )
    if plan.staging.exists() or complete_archive._is_linklike(plan.staging):
        raise EvidenceArchiveError(f"stale incident transaction exists: {plan.staging}")
    _assert_unchanged(plan)
    plan.staging.parent.mkdir(parents=True, exist_ok=True)
    _revalidate_plan_paths(plan)
    _assert_unchanged(plan)
    if plan.raw_root.stat().st_dev != plan.staging.parent.stat().st_dev:
        raise EvidenceArchiveError("raw evidence and incident staging must share a filesystem")
    if plan.campaign_root.stat().st_dev != plan.staging.parent.stat().st_dev:
        raise EvidenceArchiveError("campaign evidence and incident staging must share a filesystem")
    moved: list[tuple[Path, Path, DirectoryIdentity]] = []
    container = plan.staging
    owns_staging = False
    container_identity: DirectoryIdentity | None = None
    raw_scaffold_identity: DirectoryIdentity | None = None
    campaigns_scaffold_identity: DirectoryIdentity | None = None
    scaffold_identity: ScaffoldIdentity | None = None
    journal_established = False
    source_root_identities = (
        (plan.raw_root, plan.raw_root_identity),
        (plan.campaign_root, plan.campaign_root_identity),
    )
    try:
        plan.staging.mkdir()
        owns_staging = True
        container_identity = _directory_identity(plan.staging, label="new transaction scaffold")
        _revalidate_plan_paths(plan)
        _assert_directory_identity(
            plan.staging, container_identity, label="new transaction scaffold"
        )
        (plan.staging / "raw").mkdir()
        _assert_directory_identity(
            plan.staging, container_identity, label="new transaction scaffold"
        )
        raw_scaffold_identity = _directory_identity(
            plan.staging / "raw", label="new transaction raw root"
        )
        (plan.staging / "campaigns").mkdir()
        _assert_directory_identity(
            plan.staging, container_identity, label="new transaction scaffold"
        )
        _assert_directory_identity(
            plan.staging / "raw", raw_scaffold_identity, label="new transaction raw root"
        )
        campaigns_scaffold_identity = _directory_identity(
            plan.staging / "campaigns", label="new transaction campaign root"
        )
        scaffold_identity = ScaffoldIdentity(
            container=container_identity,
            raw=raw_scaffold_identity,
            campaigns=campaigns_scaffold_identity,
        )
        _assert_scaffold_identity(plan.staging, scaffold_identity)
        _revalidate_plan_paths(plan)
        write_json(
            plan.staging / JOURNAL_NAME,
            _journal_value(plan, scaffold_identity),
        )
        _assert_scaffold_identity(plan.staging, scaffold_identity)
        _validate_plan_journal(plan, plan.staging, scaffold_identity)
        journal_established = True
        for (source, destination), source_identity in zip(
            _move_pairs(plan, plan.staging), plan.source_identities, strict=True
        ):
            _revalidate_plan_paths(plan)
            _assert_scaffold_identity(plan.staging, scaffold_identity)
            if destination.exists() or complete_archive._is_linklike(destination):
                raise EvidenceArchiveError(
                    f"refusing to overwrite incident staging path: {destination}"
                )
            source_parent_identity = (
                plan.raw_root_identity
                if source.parent == plan.raw_root
                else plan.campaign_root_identity
            )
            destination_parent_identity = (
                scaffold_identity.raw
                if destination.parent.name == "raw"
                else scaffold_identity.campaigns
            )
            _rename_directory(
                source,
                destination,
                source_identity=source_identity,
                source_parent_identity=source_parent_identity,
                destination_parent_identity=destination_parent_identity,
            )
            moved.append((source, destination, source_identity))
            _assert_scaffold_identity(plan.staging, scaffold_identity)
            _revalidate_plan_paths(plan)
        _assert_scaffold_identity(plan.staging, scaffold_identity)
        if _scan_archived_inventory(plan.staging, plan.states) != plan.entries:
            raise EvidenceArchiveError("staged incident failed byte-for-byte verification")
        _assert_scaffold_identity(plan.staging, scaffold_identity)
        write_json(plan.staging / MANIFEST_NAME, _manifest_value(plan))
        _assert_scaffold_identity(plan.staging, scaffold_identity)
        _validate_plan_transaction(plan, plan.staging, scaffold_identity)
        verify_incident_archive(plan.staging, _allow_transaction_journal=True)
        _assert_scaffold_identity(plan.staging, scaffold_identity)
        _revalidate_plan_paths(plan)
        if plan.destination.exists() or complete_archive._is_linklike(plan.destination):
            raise EvidenceArchiveError(
                f"refusing to overwrite existing incident archive: {plan.destination}"
            )
        _publish_directory_no_replace(plan.staging, plan.destination)
        container = plan.destination
        _assert_scaffold_identity(container, scaffold_identity)
        moved = [
            (
                source,
                plan.destination / destination.relative_to(plan.staging),
                source_identity,
            )
            for source, destination, source_identity in moved
        ]
        _validate_plan_transaction(plan, plan.destination, scaffold_identity)
        _assert_scaffold_identity(plan.destination, scaffold_identity)
        (plan.destination / JOURNAL_NAME).unlink()
        journal_established = False
        _assert_scaffold_identity(plan.destination, scaffold_identity)
        verify_incident_archive(plan.destination)
    except BaseException as error:
        if not owns_staging:
            raise
        if scaffold_identity is None:
            if moved or journal_established:
                raise EvidenceArchiveError(
                    "pre-journal incident transaction invariant was violated; retained staging "
                    "for explicit integrity review"
                ) from error
            if container_identity is None:
                raise EvidenceArchiveError(
                    f"incident archive failed before its staging identity was pinned ({error}); "
                    "no evidence was moved and the unverified staging path was retained"
                ) from error
            try:
                _revalidate_plan_paths(plan)
                _assert_unchanged(plan)
                _remove_unjournaled_scaffold(
                    plan.staging,
                    container_identity=container_identity,
                    raw_identity=raw_scaffold_identity,
                    campaigns_identity=campaigns_scaffold_identity,
                )
            except (OSError, EvidenceArchiveError) as cleanup_error:
                raise EvidenceArchiveError(
                    f"incident archive failed before its journal was established ({error}); "
                    "safe unjournaled-scaffold cleanup was refused or incomplete: "
                    f"{cleanup_error}; no evidence was moved"
                ) from error
            raise
        if moved and not journal_established:
            raise EvidenceArchiveError(
                f"incident archive failed after its journal was removed ({error}); retained "
                f"published payload for explicit integrity review: {container}"
            ) from error
        if journal_established:
            try:
                _validate_plan_transaction(plan, container, scaffold_identity)
            except (OSError, EvidenceArchiveError) as validation_error:
                raise EvidenceArchiveError(
                    f"incident archive failed ({error}); automatic rollback was refused because "
                    f"the retained transaction no longer matches its plan: {validation_error}"
                ) from error
        rollback_errors = _rollback_pairs(
            moved, container, scaffold_identity, source_root_identities
        )
        if not rollback_errors:
            try:
                if journal_established:
                    _validate_plan_transaction(plan, container, scaffold_identity)
                else:
                    _revalidate_plan_paths(plan)
                    _assert_unchanged(plan)
                _remove_transaction_scaffold(container, scaffold_identity)
            except (OSError, EvidenceArchiveError) as rollback_error:
                rollback_errors.append(str(rollback_error))
        if rollback_errors:
            raise EvidenceArchiveError(
                f"incident archive failed ({error}); automatic rollback was incomplete: "
                f"{rollback_errors}"
            ) from error
        raise
    return plan.destination


def _paths_for_identity(
    repository_root: Path,
    raw_root: Path,
    campaign_root: Path,
    archive_root: Path,
    source_commit: str,
    label: str,
) -> tuple[Path, Path, Path, Path, Path]:
    complete_archive._validate_identity(source_commit, label)
    repository = repository_root.resolve(strict=True)
    raw = complete_archive._existing_repository_directory(repository, raw_root, label="raw root")
    campaigns = complete_archive._existing_repository_directory(
        repository, campaign_root, label="campaign root"
    )
    archives = complete_archive._archive_root_path(repository, archive_root)
    complete_archive._validate_source_roots(raw, campaigns, archives)
    destination = archives / source_commit / label
    staging = destination.parent / f".{label}.staging"
    complete_archive._validate_archive_destination(repository, destination, staging)
    return repository, raw, campaigns, destination, staging


def _validate_transaction_scaffold(
    container: Path, states: Sequence[ExperimentState] | None
) -> tuple[Path, Path, Path]:
    if complete_archive._is_linklike(container) or not container.is_dir():
        raise EvidenceArchiveError(
            f"incident transaction root is not a real directory: {container}"
        )
    resolved_container = container.resolve(strict=True)
    allowed = {"raw", "campaigns", JOURNAL_NAME, MANIFEST_NAME}
    unexpected = {path.name for path in container.iterdir()} - allowed
    if unexpected:
        raise EvidenceArchiveError(
            f"incident transaction contains unexpected entries: {sorted(unexpected)}"
        )
    payload_roots: list[Path] = []
    for name in ("raw", "campaigns"):
        path = container / name
        if complete_archive._is_linklike(path) or not path.is_dir():
            raise EvidenceArchiveError(
                f"incident transaction payload root is missing or unsafe: {path}"
            )
        try:
            resolved = path.resolve(strict=True)
            resolved.relative_to(resolved_container)
        except (OSError, ValueError) as error:
            raise EvidenceArchiveError(
                f"incident transaction payload root leaves its container: {path}"
            ) from error
        payload_roots.append(path)

    if states is not None:
        raw_expected = {state.experiment_id for state in states if state.raw_present}
        campaign_expected = {state.experiment_id for state in states if state.campaign_present}
        for root, expected in zip(payload_roots, (raw_expected, campaign_expected), strict=True):
            for child in root.iterdir():
                if (
                    child.name not in expected
                    or complete_archive._is_linklike(child)
                    or not child.is_dir()
                ):
                    raise EvidenceArchiveError(
                        f"unexpected incident transaction payload entry: {child}"
                    )

    journal_path = container / JOURNAL_NAME
    if complete_archive._is_linklike(journal_path) or not journal_path.is_file():
        raise EvidenceArchiveError("incident transaction journal is missing or unsafe")
    manifest_path = container / MANIFEST_NAME
    if (manifest_path.exists() or complete_archive._is_linklike(manifest_path)) and (
        complete_archive._is_linklike(manifest_path) or not manifest_path.is_file()
    ):
        raise EvidenceArchiveError("incident transaction manifest is unsafe")
    return payload_roots[0], payload_roots[1], journal_path


def rollback_incident_staging(
    repository_root: Path,
    *,
    raw_root: Path,
    campaign_root: Path,
    archive_root: Path,
    source_commit: str,
    label: str,
    experiment_ids: Sequence[str],
) -> None:
    """Restore exact roots left by an interrupted, journaled incident transaction."""

    identifiers = complete_archive._validate_experiment_ids(experiment_ids)
    repository, raw, campaigns, destination, staging = _paths_for_identity(
        repository_root, raw_root, campaign_root, archive_root, source_commit, label
    )
    _validate_partial_root(raw, identifiers)
    _validate_partial_root(campaigns, identifiers)
    candidates = [
        path
        for path in (staging, destination)
        if path.is_dir()
        and not complete_archive._is_linklike(path)
        and ((path / JOURNAL_NAME).exists() or complete_archive._is_linklike(path / JOURNAL_NAME))
    ]
    if len(candidates) != 1:
        if destination.is_dir() and not (destination / JOURNAL_NAME).exists():
            raise EvidenceArchiveError(
                "completed incident archive exists; transaction rollback is not applicable"
            )
        raise EvidenceArchiveError("exactly one journaled incident transaction is required")
    container = candidates[0]
    raw_payload, campaign_payload, journal_path = _validate_transaction_scaffold(
        container,
        None,
    )
    observed_scaffold_identity = ScaffoldIdentity(
        container=_directory_identity(container, label="incident transaction root"),
        raw=_directory_identity(raw_payload, label="incident transaction raw root"),
        campaigns=_directory_identity(campaign_payload, label="incident transaction campaign root"),
    )
    _assert_scaffold_identity(container, observed_scaffold_identity)
    journal = complete_archive._json_object(journal_path, label="incident transaction journal")
    if journal_path.read_bytes() != canonical_json_bytes(journal) + b"\n":
        raise EvidenceArchiveError("incident transaction journal is not canonical JSON")
    required = {
        "schema_version",
        "artifact_class",
        "archive_purpose",
        "source_commit",
        "archive_label",
        "inventory_sha256",
        "suite_state",
        "suite_state_sha256",
        "source_root_identities",
        "scaffold_identities",
        "moves",
        "journal_sha256",
    }
    if set(journal) != required:
        raise EvidenceArchiveError("incident transaction journal fields are invalid")
    declared_hash = journal.get("journal_sha256")
    unsigned = dict(journal)
    unsigned.pop("journal_sha256", None)
    if declared_hash != sha256_value(unsigned):
        raise EvidenceArchiveError("incident transaction journal self-hash is invalid")
    if (
        journal.get("schema_version") != 2
        or journal.get("artifact_class") != JOURNAL_CLASS
        or journal.get("archive_purpose") != ARCHIVE_PURPOSE
        or journal.get("source_commit") != source_commit
        or journal.get("archive_label") != label
    ):
        raise EvidenceArchiveError("incident transaction identity is invalid")
    states = _states_from_manifest(journal.get("suite_state"), identifiers)
    state_values = [state.manifest_value() for state in states]
    if journal.get("suite_state_sha256") != sha256_value(state_values):
        raise EvidenceArchiveError("incident transaction suite-state hash is invalid")

    source_root_values = journal.get("source_root_identities")
    if not isinstance(source_root_values, Mapping) or set(source_root_values) != {
        "raw",
        "campaigns",
    }:
        raise EvidenceArchiveError("incident transaction source-root identities are invalid")
    raw_root_identity = _identity_from_value(
        source_root_values.get("raw"), label="journaled raw root"
    )
    campaign_root_identity = _identity_from_value(
        source_root_values.get("campaigns"), label="journaled campaign root"
    )
    _assert_directory_identity(raw, raw_root_identity, label="journaled raw root")
    _assert_directory_identity(campaigns, campaign_root_identity, label="journaled campaign root")

    scaffold_values = journal.get("scaffold_identities")
    if not isinstance(scaffold_values, Mapping) or set(scaffold_values) != {
        "container",
        "raw",
        "campaigns",
    }:
        raise EvidenceArchiveError("incident transaction scaffold identities are invalid")
    journaled_scaffold_identity = ScaffoldIdentity(
        container=_identity_from_value(
            scaffold_values.get("container"), label="journaled transaction root"
        ),
        raw=_identity_from_value(
            scaffold_values.get("raw"), label="journaled transaction raw root"
        ),
        campaigns=_identity_from_value(
            scaffold_values.get("campaigns"),
            label="journaled transaction campaign root",
        ),
    )
    if journaled_scaffold_identity != observed_scaffold_identity:
        raise EvidenceArchiveError("incident transaction scaffold identity changed")
    _assert_scaffold_identity(container, journaled_scaffold_identity)

    _validate_transaction_scaffold(container, states)

    expected_move_paths: list[tuple[Path, Path]] = []
    expected_pairs: list[tuple[Path, Path]] = []
    for state in states:
        if state.raw_present:
            source = raw / state.experiment_id
            staged = container / "raw" / state.experiment_id
            expected_pairs.append((source, staged))
            expected_move_paths.append((source, staged))
    for state in states:
        if state.campaign_present:
            source = campaigns / state.experiment_id
            staged = container / "campaigns" / state.experiment_id
            expected_pairs.append((source, staged))
            expected_move_paths.append((source, staged))
    move_values = journal.get("moves")
    if not isinstance(move_values, list) or len(move_values) != len(expected_move_paths):
        raise EvidenceArchiveError("incident transaction moves do not match its suite state")
    move_identities: list[DirectoryIdentity] = []
    for item, (source, staged) in zip(move_values, expected_move_paths, strict=True):
        if not isinstance(item, Mapping) or set(item) != {
            "source",
            "staged",
            "source_identity",
        }:
            raise EvidenceArchiveError("incident transaction move fields are invalid")
        if (
            item.get("source") != source.relative_to(repository).as_posix()
            or item.get("staged") != staged.relative_to(container).as_posix()
        ):
            raise EvidenceArchiveError("incident transaction moves do not match its suite state")
        move_identity = _identity_from_value(
            item.get("source_identity"), label="journaled move source"
        )
        selected = _select_transaction_directory(source, staged, expected_present=True)
        if selected is None:
            raise EvidenceArchiveError("journaled move source is unavailable")
        _assert_directory_identity(selected, move_identity, label="journaled move source")
        move_identities.append(move_identity)
    observed_transaction_state = _inspect_transaction_state(
        raw, campaigns, container, states, source_commit
    )
    if observed_transaction_state != states:
        raise EvidenceArchiveError("incident transaction payload state does not match its journal")
    transaction_entries = _scan_transaction_inventory(raw, campaigns, container, states)
    transaction_hash = sha256_value([entry.manifest_value() for entry in transaction_entries])
    if journal.get("inventory_sha256") != transaction_hash:
        raise EvidenceArchiveError("incident transaction inventory hash is invalid")
    moved = [
        (source, staged, identity)
        for (source, staged), identity in zip(expected_pairs, move_identities, strict=True)
        if staged.exists()
    ]
    errors = _rollback_pairs(
        moved,
        container,
        journaled_scaffold_identity,
        ((raw, raw_root_identity), (campaigns, campaign_root_identity)),
    )
    if errors:
        raise EvidenceArchiveError(f"incident transaction rollback was incomplete: {errors}")
    restored = _inspect_suite(raw, campaigns, identifiers, source_commit)
    if restored != states:
        raise EvidenceArchiveError("restored incident evidence does not match its journaled state")
    restored_entries = _scan_inventory(raw, campaigns, restored)
    restored_hash = sha256_value([entry.manifest_value() for entry in restored_entries])
    if journal.get("inventory_sha256") != restored_hash:
        raise EvidenceArchiveError("restored incident inventory does not match its journal")
    for (source, _staged), identity in zip(expected_pairs, move_identities, strict=True):
        _assert_directory_identity(source, identity, label="restored experiment source")
    _assert_scaffold_identity(container, journaled_scaffold_identity)
    try:
        _remove_transaction_scaffold(container, journaled_scaffold_identity)
    except (OSError, EvidenceArchiveError) as error:
        raise EvidenceArchiveError(
            "incident transaction rollback restored the payload but cleanup was incomplete: "
            f"{error}"
        ) from error


def _plan_summary(plan: IncidentPlan, *, mode: str) -> dict[str, Any]:
    return {
        "mode": mode,
        "archive_purpose": ARCHIVE_PURPOSE,
        "publication_eligible": False,
        "source_commit": plan.source_commit,
        "archive_label": plan.label,
        "destination": _relative_to_repository(plan, plan.destination),
        "experiment_ids": list(plan.experiment_ids),
        "suite_state": [state.manifest_value() for state in plan.states],
        "suite_state_sha256": plan.suite_state_sha256,
        "file_count": plan.file_count,
        "directory_count": plan.directory_count,
        "total_bytes": plan.total_bytes,
        "inventory_sha256": plan.inventory_sha256,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-source-commit", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--raw-root", type=Path)
    parser.add_argument("--campaign-root", type=Path)
    parser.add_argument("--archive-root", type=Path)
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--execute", action="store_true", help="perform the verified move")
    action.add_argument(
        "--rollback-staging",
        action="store_true",
        help="restore a journaled interrupted-incident transaction",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    repository = args.root.resolve()
    raw_root = args.raw_root or repository / "results/raw"
    campaign_root = args.campaign_root or repository / ".artifacts/campaigns"
    archive_root = args.archive_root or repository / ".artifacts/research-incident-archives"
    try:
        identifiers = complete_archive.core_experiment_ids(repository)
        if args.rollback_staging:
            rollback_incident_staging(
                repository,
                raw_root=raw_root,
                campaign_root=campaign_root,
                archive_root=archive_root,
                source_commit=args.expected_source_commit,
                label=args.label,
                experiment_ids=identifiers,
            )
            print(json.dumps({"mode": "rollback", "status": "completed"}, indent=2))
            return 0
        plan = plan_incident_archive(
            repository,
            raw_root=raw_root,
            campaign_root=campaign_root,
            archive_root=archive_root,
            source_commit=args.expected_source_commit,
            label=args.label,
            experiment_ids=identifiers,
        )
        if args.execute:
            execute_incident_archive(plan)
        print(
            json.dumps(_plan_summary(plan, mode="execute" if args.execute else "dry-run"), indent=2)
        )
    except (EvidenceArchiveError, OSError) as error:
        print(f"Partial research incident archive rejected: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
