"""Safely rotate one complete core research suite into a byte-verified archive.

The default action is a read-only dry run.  ``--execute`` moves only the exact core experiment
directories after snapshotting every file and directory.  A staging journal permits an explicit
rollback after an abrupt interruption; ordinary exceptions are rolled back automatically.
"""

from __future__ import annotations

import argparse
import json
import re
import stat
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from benchmark.runner.canonical import canonical_json_bytes, sha256_file, sha256_value, write_json
from benchmark.runner.config import load_experiment
from scripts.run_research_suite import CORE_CONFIGS

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_ROOT = ROOT / "benchmark/schemas"
DEFAULT_RAW_ROOT = ROOT / "results/raw"
DEFAULT_CAMPAIGN_ROOT = ROOT / ".artifacts/campaigns"
DEFAULT_ARCHIVE_ROOT = ROOT / ".artifacts/campaign-archives"
MANIFEST_NAME = "archive-manifest.json"
JOURNAL_NAME = "transaction-journal.json"
EXPECTED_CORE_EXPERIMENTS = 10
_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_LABEL_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{0,79}$")
_EXPERIMENT_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_UTC_SECONDS_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


class EvidenceArchiveError(ValueError):
    """Evidence cannot be archived without weakening preservation or provenance."""


def _is_linklike(path: Path) -> bool:
    """Reject symbolic links and Windows directory junctions without dereferencing them."""

    return path.is_symlink() or path.is_junction()


@dataclass(frozen=True, slots=True)
class ArchiveEntry:
    """One directory or regular file in the archive payload."""

    path: str
    kind: str
    size_bytes: int | None = None
    sha256: str | None = None

    def manifest_value(self) -> dict[str, int | str]:
        value: dict[str, int | str] = {"path": self.path, "kind": self.kind}
        if self.kind == "file":
            if self.size_bytes is None or self.sha256 is None:
                raise EvidenceArchiveError(f"file entry has no fingerprint: {self.path}")
            value.update({"size_bytes": self.size_bytes, "sha256": self.sha256})
        return value


@dataclass(frozen=True, slots=True)
class ArchivePlan:
    """Validated, read-only snapshot that can be executed as one staged transaction."""

    repository_root: Path
    raw_root: Path
    campaign_root: Path
    archive_root: Path
    destination: Path
    staging: Path
    source_commit: str
    label: str
    created_at: str
    experiment_ids: tuple[str, ...]
    entries: tuple[ArchiveEntry, ...]

    @property
    def inventory_sha256(self) -> str:
        return sha256_value([entry.manifest_value() for entry in self.entries])

    @property
    def file_count(self) -> int:
        return sum(entry.kind == "file" for entry in self.entries)

    @property
    def directory_count(self) -> int:
        return sum(entry.kind == "directory" for entry in self.entries)

    @property
    def total_bytes(self) -> int:
        return sum(entry.size_bytes or 0 for entry in self.entries)


def core_experiment_ids(repository_root: Path = ROOT) -> tuple[str, ...]:
    """Resolve the exact reviewed experiment set from the canonical suite configuration."""

    schema_root = repository_root / "benchmark/schemas"
    if len(CORE_CONFIGS) != EXPECTED_CORE_EXPERIMENTS:
        raise EvidenceArchiveError(
            f"core suite must contain exactly {EXPECTED_CORE_EXPERIMENTS} configurations"
        )
    identifiers: list[str] = []
    for config in CORE_CONFIGS:
        loaded = load_experiment(repository_root / config, schema_root)
        experiment = loaded.get("experiment")
        identifier = experiment.get("id") if isinstance(experiment, Mapping) else None
        if not isinstance(identifier, str) or not identifier:
            raise EvidenceArchiveError(f"core config has no experiment ID: {config}")
        identifiers.append(identifier)
    return _validate_experiment_ids(identifiers)


def _validate_experiment_ids(experiment_ids: Sequence[str]) -> tuple[str, ...]:
    identifiers = tuple(experiment_ids)
    if not identifiers:
        raise EvidenceArchiveError("at least one experiment ID is required")
    folded: set[str] = set()
    for identifier in identifiers:
        if _EXPERIMENT_PATTERN.fullmatch(identifier) is None:
            raise EvidenceArchiveError(f"unsafe experiment ID: {identifier!r}")
        if identifier.casefold() in folded:
            raise EvidenceArchiveError(f"duplicate or case-colliding experiment ID: {identifier}")
        folded.add(identifier.casefold())
    return identifiers


def _validate_identity(source_commit: str, label: str) -> None:
    if _COMMIT_PATTERN.fullmatch(source_commit) is None:
        raise EvidenceArchiveError("expected source commit must be a lowercase 40-character SHA-1")
    if _LABEL_PATTERN.fullmatch(label) is None:
        raise EvidenceArchiveError(
            "archive label must be 1-80 lowercase alphanumeric, dot, underscore, "
            "or hyphen characters"
        )


def _existing_repository_directory(repository_root: Path, supplied: Path, *, label: str) -> Path:
    root = repository_root.resolve(strict=True)
    candidate = supplied if supplied.is_absolute() else root / supplied
    absolute = candidate.absolute()
    _reject_symlink_components(root, absolute, label=label)
    try:
        resolved = absolute.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError) as error:
        raise EvidenceArchiveError(
            f"{label} must be an existing directory inside the repository"
        ) from error
    if not resolved.is_dir():
        raise EvidenceArchiveError(f"{label} is not a directory: {resolved}")
    return resolved


def _reject_symlink_components(repository_root: Path, path: Path, *, label: str) -> None:
    try:
        relative = path.relative_to(repository_root)
    except ValueError as error:
        raise EvidenceArchiveError(f"{label} must stay inside the repository") from error
    current = repository_root
    for component in relative.parts:
        current /= component
        if _is_linklike(current):
            raise EvidenceArchiveError(f"{label} contains a symlink component: {current}")


def _archive_root_path(repository_root: Path, supplied: Path) -> Path:
    root = repository_root.resolve(strict=True)
    candidate = supplied if supplied.is_absolute() else root / supplied
    try:
        absolute = candidate.absolute()
        absolute.relative_to(root)
    except ValueError as error:
        raise EvidenceArchiveError("archive root must stay inside the repository") from error
    _reject_symlink_components(root, absolute, label="archive root")
    current = absolute
    while current != root and not current.exists():
        current = current.parent
    try:
        current.resolve(strict=True).relative_to(root)
    except (OSError, ValueError) as error:
        raise EvidenceArchiveError("archive root resolves outside the repository") from error
    return absolute


def _validate_archive_destination(repository_root: Path, destination: Path, staging: Path) -> None:
    for label, path in (("archive destination", destination), ("archive staging", staging)):
        _reject_symlink_components(repository_root, path.absolute(), label=label)
        parent = path.parent
        if parent.exists():
            if _is_linklike(parent) or not parent.is_dir():
                raise EvidenceArchiveError(f"{label} parent is not a real directory: {parent}")
            try:
                parent.resolve(strict=True).relative_to(repository_root)
            except (OSError, ValueError) as error:
                raise EvidenceArchiveError(f"{label} parent leaves the repository") from error


def _validate_source_roots(raw_root: Path, campaign_root: Path, archive_root: Path) -> None:
    if raw_root == campaign_root:
        raise EvidenceArchiveError("raw and campaign roots must be different")
    for first, second in (
        (raw_root, campaign_root),
        (raw_root, archive_root),
        (campaign_root, archive_root),
    ):
        nested = False
        for parent, child in ((first, second), (second, first)):
            try:
                child.relative_to(parent)
            except ValueError:
                continue
            nested = True
        if nested:
            raise EvidenceArchiveError(
                f"archive/source roots must not be nested: {first} and {second}"
            )


def _validate_exact_root(root: Path, experiment_ids: Sequence[str]) -> None:
    expected = set(experiment_ids)
    observed: set[str] = set()
    for child in sorted(root.iterdir(), key=lambda item: item.name):
        if _is_linklike(child):
            raise EvidenceArchiveError(f"source root contains a symlink: {child}")
        if child.name == ".gitkeep" and child.is_file():
            continue
        if child.name not in expected or not child.is_dir():
            raise EvidenceArchiveError(f"unexpected entry in source root: {child}")
        observed.add(child.name)
    missing = sorted(expected - observed)
    if missing:
        raise EvidenceArchiveError(f"source root is missing core experiment directories: {missing}")


def _scan_directory(directory: Path, archive_prefix: str) -> list[ArchiveEntry]:
    entries: list[ArchiveEntry] = [ArchiveEntry(archive_prefix, "directory")]
    descendants = sorted(
        directory.rglob("*"), key=lambda path: path.relative_to(directory).as_posix()
    )
    for path in descendants:
        relative = path.relative_to(directory).as_posix()
        archive_path = f"{archive_prefix}/{relative}"
        if _is_linklike(path):
            raise EvidenceArchiveError(f"evidence tree contains a symlink: {path}")
        metadata = path.lstat()
        if stat.S_ISDIR(metadata.st_mode):
            entries.append(ArchiveEntry(archive_path, "directory"))
        elif stat.S_ISREG(metadata.st_mode):
            digest = sha256_file(path)
            final_size = path.stat().st_size
            if final_size != metadata.st_size:
                raise EvidenceArchiveError(f"evidence file changed while hashing: {path}")
            entries.append(ArchiveEntry(archive_path, "file", final_size, digest))
        else:
            raise EvidenceArchiveError(f"evidence tree contains a non-regular entry: {path}")
    return entries


def _scan_inventory(
    raw_root: Path, campaign_root: Path, experiment_ids: Sequence[str]
) -> tuple[ArchiveEntry, ...]:
    entries: list[ArchiveEntry] = []
    for prefix, root in (("raw", raw_root), ("campaigns", campaign_root)):
        for experiment_id in experiment_ids:
            entries.extend(_scan_directory(root / experiment_id, f"{prefix}/{experiment_id}"))
    entries.sort(key=lambda entry: (entry.path.casefold(), entry.path, entry.kind))
    folded: dict[str, str] = {}
    for entry in entries:
        collision = folded.get(entry.path.casefold())
        if collision is not None and collision != entry.path:
            raise EvidenceArchiveError(
                f"case-colliding evidence paths are forbidden: {collision}, {entry.path}"
            )
        folded[entry.path.casefold()] = entry.path
    return tuple(entries)


def _json_object(path: Path, *, label: str) -> dict[str, Any]:
    try:
        value: object = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise EvidenceArchiveError(f"{label} is unreadable: {path}: {error}") from error
    if not isinstance(value, dict):
        raise EvidenceArchiveError(f"{label} must contain a JSON object: {path}")
    return value


def _validate_manifest_source_roots(value: object) -> None:
    if not isinstance(value, Mapping) or set(value) != {"raw", "campaigns"}:
        raise EvidenceArchiveError("archive manifest source roots are invalid")
    paths: list[str] = []
    for label in ("raw", "campaigns"):
        declared = value.get(label)
        if not isinstance(declared, str) or not declared or "\\" in declared:
            raise EvidenceArchiveError("archive manifest source roots are invalid")
        relative = Path(declared)
        if relative.is_absolute() or ".." in relative.parts or relative.as_posix() != declared:
            raise EvidenceArchiveError("archive manifest source roots are not canonical")
        paths.append(declared)
    if paths[0].casefold() == paths[1].casefold():
        raise EvidenceArchiveError("archive manifest source roots collide")


def _validate_campaign_provenance(
    raw_root: Path,
    campaign_root: Path,
    experiment_ids: Sequence[str],
    source_commit: str,
) -> None:
    for experiment_id in experiment_ids:
        raw_files = tuple(sorted((raw_root / experiment_id).rglob("*")))
        records = [path for path in raw_files if path.is_file()]
        if not records:
            raise EvidenceArchiveError(f"raw experiment has no records: {experiment_id}")
        for path in records:
            if path.suffix != ".json":
                raise EvidenceArchiveError(f"raw experiment contains a non-JSON file: {path}")
            record = _json_object(path, label="raw record")
            if record.get("experiment_id") != experiment_id:
                raise EvidenceArchiveError(
                    f"raw record experiment ID does not match its root: {path}"
                )
            provenance = record.get("provenance")
            commit = provenance.get("git_commit") if isinstance(provenance, Mapping) else None
            if commit != source_commit:
                raise EvidenceArchiveError(f"raw record is not bound to {source_commit}: {path}")

        manifest_path = campaign_root / experiment_id / "experiment-manifest.json"
        manifest = _json_object(manifest_path, label="experiment manifest")
        if manifest.get("experiment_id") != experiment_id:
            raise EvidenceArchiveError(
                f"experiment manifest ID does not match its root: {manifest_path}"
            )
        declared_hash = manifest.get("manifest_sha256")
        unsigned = dict(manifest)
        unsigned.pop("manifest_sha256", None)
        if declared_hash != sha256_value(unsigned):
            raise EvidenceArchiveError(f"experiment manifest hash is invalid: {manifest_path}")
        validation = manifest.get("dataset_validation")
        validator_commit = (
            validation.get("validator_git_commit") if isinstance(validation, Mapping) else None
        )
        if validator_commit != source_commit:
            raise EvidenceArchiveError(
                f"experiment manifest is not bound to {source_commit}: {manifest_path}"
            )


def _iso_utc(now: datetime | None = None) -> str:
    current = now or datetime.now(UTC)
    if current.tzinfo is None:
        raise EvidenceArchiveError("archive timestamp must be timezone-aware")
    return current.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def plan_archive(
    repository_root: Path,
    *,
    raw_root: Path,
    campaign_root: Path,
    archive_root: Path,
    source_commit: str,
    label: str,
    experiment_ids: Sequence[str],
    now: datetime | None = None,
) -> ArchivePlan:
    """Validate the exact sources and return a read-only archive plan."""

    _validate_identity(source_commit, label)
    identifiers = _validate_experiment_ids(experiment_ids)
    repository = repository_root.resolve(strict=True)
    raw = _existing_repository_directory(repository, raw_root, label="raw root")
    campaigns = _existing_repository_directory(repository, campaign_root, label="campaign root")
    archives = _archive_root_path(repository, archive_root)
    _validate_source_roots(raw, campaigns, archives)
    _validate_exact_root(raw, identifiers)
    _validate_exact_root(campaigns, identifiers)
    destination = archives / source_commit / label
    staging = destination.parent / f".{label}.staging"
    _validate_archive_destination(repository, destination, staging)
    if destination.exists() or _is_linklike(destination):
        raise EvidenceArchiveError(f"refusing to overwrite existing archive: {destination}")
    if staging.exists() or _is_linklike(staging):
        raise EvidenceArchiveError(
            f"stale archive transaction exists: {staging}; run --rollback-staging first"
        )
    entries = _scan_inventory(raw, campaigns, identifiers)
    _validate_campaign_provenance(raw, campaigns, identifiers, source_commit)
    return ArchivePlan(
        repository_root=repository,
        raw_root=raw,
        campaign_root=campaigns,
        archive_root=archives,
        destination=destination,
        staging=staging,
        source_commit=source_commit,
        label=label,
        created_at=_iso_utc(now),
        experiment_ids=identifiers,
        entries=entries,
    )


def _relative_to_repository(plan: ArchivePlan, path: Path) -> str:
    return path.relative_to(plan.repository_root).as_posix()


def _manifest_value(plan: ArchivePlan) -> dict[str, Any]:
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "artifact_class": "research-evidence-archive-v2",
        "source_commit": plan.source_commit,
        "archive_label": plan.label,
        "created_at": plan.created_at,
        "source_roots": {
            "raw": _relative_to_repository(plan, plan.raw_root),
            "campaigns": _relative_to_repository(plan, plan.campaign_root),
        },
        "experiment_ids": list(plan.experiment_ids),
        "inventory_scope": "all payload directories and regular files under raw/ and campaigns/",
        "entries": [entry.manifest_value() for entry in plan.entries],
        "summary": {
            "file_count": plan.file_count,
            "directory_count": plan.directory_count,
            "total_bytes": plan.total_bytes,
        },
        "inventory_sha256": plan.inventory_sha256,
        "restore_notice": (
            "Verify this manifest before restoring. Restore only the listed experiment directories "
            "into empty original roots; never merge or overwrite evidence."
        ),
    }
    manifest["manifest_sha256"] = sha256_value(manifest)
    return manifest


def _move_pairs(plan: ArchivePlan, container: Path) -> tuple[tuple[Path, Path], ...]:
    pairs: list[tuple[Path, Path]] = []
    for prefix, source_root in (("raw", plan.raw_root), ("campaigns", plan.campaign_root)):
        for experiment_id in plan.experiment_ids:
            pairs.append((source_root / experiment_id, container / prefix / experiment_id))
    return tuple(pairs)


def _journal_value(plan: ArchivePlan) -> dict[str, Any]:
    journal: dict[str, Any] = {
        "schema_version": 1,
        "artifact_class": "research-evidence-archive-transaction-v1",
        "source_commit": plan.source_commit,
        "archive_label": plan.label,
        "inventory_sha256": plan.inventory_sha256,
        "moves": [
            {
                "source": _relative_to_repository(plan, source),
                "staged": destination.relative_to(plan.staging).as_posix(),
            }
            for source, destination in _move_pairs(plan, plan.staging)
        ],
    }
    journal["journal_sha256"] = sha256_value(journal)
    return journal


def _rename_directory(source: Path, destination: Path) -> None:
    source.rename(destination)


def _remove_transaction_scaffold(container: Path) -> None:
    for name in (MANIFEST_NAME, JOURNAL_NAME):
        path = container / name
        if path.exists() and path.is_file() and not _is_linklike(path):
            path.unlink()
    for name in ("raw", "campaigns"):
        directory = container / name
        if directory.is_dir() and not _is_linklike(directory):
            directory.rmdir()
    if container.is_dir() and not _is_linklike(container):
        container.rmdir()


def _rollback_pairs(moved: Sequence[tuple[Path, Path]], container: Path) -> list[str]:
    errors: list[str] = []
    for source, destination in reversed(moved):
        try:
            if source.exists() or _is_linklike(source):
                raise EvidenceArchiveError(f"rollback source already exists: {source}")
            if not destination.is_dir() or _is_linklike(destination):
                raise EvidenceArchiveError(f"rollback destination is unavailable: {destination}")
            destination.rename(source)
        except (OSError, EvidenceArchiveError) as error:
            errors.append(str(error))
    if not errors:
        try:
            _remove_transaction_scaffold(container)
        except OSError as error:
            errors.append(str(error))
    return errors


def _assert_unchanged(plan: ArchivePlan) -> None:
    _validate_exact_root(plan.raw_root, plan.experiment_ids)
    _validate_exact_root(plan.campaign_root, plan.experiment_ids)
    current = _scan_inventory(plan.raw_root, plan.campaign_root, plan.experiment_ids)
    if current != plan.entries:
        raise EvidenceArchiveError("evidence changed after the dry-run snapshot; create a new plan")


def _inventory_from_archive(
    archive_dir: Path, experiment_ids: Sequence[str]
) -> tuple[ArchiveEntry, ...]:
    raw = archive_dir / "raw"
    campaigns = archive_dir / "campaigns"
    _validate_exact_root(raw, experiment_ids)
    _validate_exact_root(campaigns, experiment_ids)
    return _scan_inventory(raw, campaigns, experiment_ids)


def verify_archive(archive_dir: Path) -> dict[str, Any]:
    """Re-hash a completed archive and validate its canonical self-bound manifest."""

    if _is_linklike(archive_dir) or not archive_dir.is_dir():
        raise EvidenceArchiveError(f"archive is not a real directory: {archive_dir}")
    allowed = {"raw", "campaigns", MANIFEST_NAME}
    observed = {path.name for path in archive_dir.iterdir()}
    if observed != allowed:
        raise EvidenceArchiveError(
            f"archive root entries do not match the contract: {sorted(observed)}"
        )
    manifest_path = archive_dir / MANIFEST_NAME
    if _is_linklike(manifest_path) or not manifest_path.is_file():
        raise EvidenceArchiveError("archive manifest is missing or unsafe")
    manifest = _json_object(manifest_path, label="archive manifest")
    if manifest_path.read_bytes() != canonical_json_bytes(manifest) + b"\n":
        raise EvidenceArchiveError("archive manifest is not canonical JSON")
    required_fields = {
        "schema_version",
        "artifact_class",
        "source_commit",
        "archive_label",
        "created_at",
        "source_roots",
        "experiment_ids",
        "inventory_scope",
        "entries",
        "summary",
        "inventory_sha256",
        "restore_notice",
        "manifest_sha256",
    }
    if set(manifest) != required_fields or manifest.get("schema_version") != 1:
        raise EvidenceArchiveError("archive manifest fields do not match the v2 contract")
    declared_manifest_hash = manifest.get("manifest_sha256")
    unsigned = dict(manifest)
    unsigned.pop("manifest_sha256", None)
    if declared_manifest_hash != sha256_value(unsigned):
        raise EvidenceArchiveError("archive manifest self-hash is invalid")
    if manifest.get("artifact_class") != "research-evidence-archive-v2":
        raise EvidenceArchiveError("archive manifest has an unsupported artifact class")
    source_commit = manifest.get("source_commit")
    label = manifest.get("archive_label")
    if not isinstance(source_commit, str) or not isinstance(label, str):
        raise EvidenceArchiveError("archive manifest identity is invalid")
    _validate_identity(source_commit, label)
    created_at = manifest.get("created_at")
    if not isinstance(created_at, str) or _UTC_SECONDS_PATTERN.fullmatch(created_at) is None:
        raise EvidenceArchiveError("archive manifest timestamp is invalid")
    try:
        datetime.fromisoformat(created_at.removesuffix("Z") + "+00:00")
    except ValueError as error:
        raise EvidenceArchiveError("archive manifest timestamp is invalid") from error
    _validate_manifest_source_roots(manifest.get("source_roots"))
    experiment_ids_value = manifest.get("experiment_ids")
    if not isinstance(experiment_ids_value, list) or not all(
        isinstance(item, str) for item in experiment_ids_value
    ):
        raise EvidenceArchiveError("archive manifest experiment IDs are invalid")
    experiment_ids = _validate_experiment_ids(experiment_ids_value)
    entries = _inventory_from_archive(archive_dir, experiment_ids)
    manifest_entries = manifest.get("entries")
    expected_entries = [entry.manifest_value() for entry in entries]
    if manifest_entries != expected_entries:
        raise EvidenceArchiveError("archive payload does not match the manifest inventory")
    if manifest.get("inventory_sha256") != sha256_value(expected_entries):
        raise EvidenceArchiveError("archive inventory hash is invalid")
    summary = manifest.get("summary")
    expected_summary = {
        "file_count": sum(entry.kind == "file" for entry in entries),
        "directory_count": sum(entry.kind == "directory" for entry in entries),
        "total_bytes": sum(entry.size_bytes or 0 for entry in entries),
    }
    if summary != expected_summary:
        raise EvidenceArchiveError("archive summary does not match its payload")
    _validate_campaign_provenance(
        archive_dir / "raw", archive_dir / "campaigns", experiment_ids, source_commit
    )
    return manifest


def execute_archive(plan: ArchivePlan) -> Path:
    """Move the planned directories transactionally and verify the completed archive."""

    _validate_archive_destination(plan.repository_root, plan.destination, plan.staging)
    if plan.destination.exists() or _is_linklike(plan.destination):
        raise EvidenceArchiveError(f"refusing to overwrite existing archive: {plan.destination}")
    if plan.staging.exists() or _is_linklike(plan.staging):
        raise EvidenceArchiveError(f"stale archive transaction exists: {plan.staging}")
    _assert_unchanged(plan)
    plan.staging.parent.mkdir(parents=True, exist_ok=True)
    if plan.raw_root.stat().st_dev != plan.staging.parent.stat().st_dev:
        raise EvidenceArchiveError(
            "raw evidence and archive staging must be on the same filesystem"
        )
    if plan.campaign_root.stat().st_dev != plan.staging.parent.stat().st_dev:
        raise EvidenceArchiveError(
            "campaign evidence and archive staging must be on the same filesystem"
        )
    plan.staging.mkdir()
    (plan.staging / "raw").mkdir()
    (plan.staging / "campaigns").mkdir()
    write_json(plan.staging / JOURNAL_NAME, _journal_value(plan))
    moved: list[tuple[Path, Path]] = []
    container = plan.staging
    try:
        for source, destination in _move_pairs(plan, plan.staging):
            if destination.exists() or _is_linklike(destination):
                raise EvidenceArchiveError(f"refusing to overwrite staging path: {destination}")
            _rename_directory(source, destination)
            moved.append((source, destination))
        staged_inventory = _inventory_from_archive(plan.staging, plan.experiment_ids)
        if staged_inventory != plan.entries:
            raise EvidenceArchiveError("staged archive failed byte-for-byte inventory verification")
        write_json(plan.staging / MANIFEST_NAME, _manifest_value(plan))
        plan.staging.rename(plan.destination)
        container = plan.destination
        moved = [
            (source, plan.destination / destination.relative_to(plan.staging))
            for source, destination in moved
        ]
        (plan.destination / JOURNAL_NAME).unlink()
        verify_archive(plan.destination)
    except BaseException as error:
        rollback_errors = _rollback_pairs(moved, container)
        if rollback_errors:
            raise EvidenceArchiveError(
                f"archive failed ({error}); automatic rollback was incomplete: {rollback_errors}"
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
    _validate_identity(source_commit, label)
    repository = repository_root.resolve(strict=True)
    raw = _existing_repository_directory(repository, raw_root, label="raw root")
    campaigns = _existing_repository_directory(repository, campaign_root, label="campaign root")
    archives = _archive_root_path(repository, archive_root)
    _validate_source_roots(raw, campaigns, archives)
    destination = archives / source_commit / label
    staging = destination.parent / f".{label}.staging"
    _validate_archive_destination(repository, destination, staging)
    return repository, raw, campaigns, destination, staging


def rollback_staging(
    repository_root: Path,
    *,
    raw_root: Path,
    campaign_root: Path,
    archive_root: Path,
    source_commit: str,
    label: str,
    experiment_ids: Sequence[str],
) -> None:
    """Recover exact directories left in a journaled staging area after process interruption."""

    identifiers = _validate_experiment_ids(experiment_ids)
    repository, raw, campaigns, destination, staging = _paths_for_identity(
        repository_root, raw_root, campaign_root, archive_root, source_commit, label
    )
    present = [path for path in (staging, destination) if path.exists() or _is_linklike(path)]
    if len(present) > 1:
        raise EvidenceArchiveError(
            "archive destination and staging both exist; rollback is ambiguous"
        )
    candidates = [
        candidate
        for candidate in (staging, destination)
        if candidate.is_dir()
        and not _is_linklike(candidate)
        and (candidate / JOURNAL_NAME).is_file()
    ]
    if len(candidates) != 1:
        if destination.is_dir() and not (destination / JOURNAL_NAME).exists():
            raise EvidenceArchiveError(
                "completed archive exists; interrupted-transaction rollback is not applicable"
            )
        raise EvidenceArchiveError("exactly one journaled archive transaction is required")
    container = candidates[0]
    journal_path = container / JOURNAL_NAME
    if _is_linklike(journal_path) or not journal_path.is_file():
        raise EvidenceArchiveError("transaction journal is missing or unsafe")
    journal = _json_object(journal_path, label="transaction journal")
    if journal_path.read_bytes() != canonical_json_bytes(journal) + b"\n":
        raise EvidenceArchiveError("transaction journal is not canonical JSON")
    declared_hash = journal.get("journal_sha256")
    unsigned = dict(journal)
    unsigned.pop("journal_sha256", None)
    if declared_hash != sha256_value(unsigned):
        raise EvidenceArchiveError("transaction journal self-hash is invalid")
    expected_moves: list[dict[str, str]] = []
    expected_pairs: list[tuple[Path, Path]] = []
    for prefix, source_root in (("raw", raw), ("campaigns", campaigns)):
        for experiment_id in identifiers:
            source = source_root / experiment_id
            staged = container / prefix / experiment_id
            expected_pairs.append((source, staged))
            expected_moves.append(
                {
                    "source": source.relative_to(repository).as_posix(),
                    "staged": staged.relative_to(container).as_posix(),
                }
            )
    if (
        journal.get("artifact_class") != "research-evidence-archive-transaction-v1"
        or journal.get("source_commit") != source_commit
        or journal.get("archive_label") != label
        or journal.get("moves") != expected_moves
    ):
        raise EvidenceArchiveError("transaction journal does not match the requested core suite")
    allowed = {"raw", "campaigns", JOURNAL_NAME, MANIFEST_NAME}
    unexpected = {path.name for path in container.iterdir()} - allowed
    if unexpected:
        raise EvidenceArchiveError(f"staging transaction contains unexpected entries: {unexpected}")
    moved = [(source, staged) for source, staged in expected_pairs if staged.exists()]
    for source, staged in expected_pairs:
        if source.exists() and staged.exists():
            raise EvidenceArchiveError(f"both rollback copies exist: {source}, {staged}")
        if not source.exists() and not staged.exists():
            raise EvidenceArchiveError(f"both rollback copies are missing: {source}, {staged}")
    errors = _rollback_pairs(moved, container)
    if errors:
        raise EvidenceArchiveError(f"transaction rollback was incomplete: {errors}")
    _validate_exact_root(raw, identifiers)
    _validate_exact_root(campaigns, identifiers)


def _plan_summary(plan: ArchivePlan, *, mode: str) -> dict[str, Any]:
    return {
        "mode": mode,
        "source_commit": plan.source_commit,
        "archive_label": plan.label,
        "destination": _relative_to_repository(plan, plan.destination),
        "experiment_ids": list(plan.experiment_ids),
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
        help="restore a journaled, interrupted staging transaction",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    repository = args.root.resolve()
    raw_root = args.raw_root or repository / "results/raw"
    campaign_root = args.campaign_root or repository / ".artifacts/campaigns"
    archive_root = args.archive_root or repository / ".artifacts/campaign-archives"
    try:
        identifiers = core_experiment_ids(repository)
        if args.rollback_staging:
            rollback_staging(
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
        plan = plan_archive(
            repository,
            raw_root=raw_root,
            campaign_root=campaign_root,
            archive_root=archive_root,
            source_commit=args.expected_source_commit,
            label=args.label,
            experiment_ids=identifiers,
        )
        if args.execute:
            execute_archive(plan)
        print(
            json.dumps(_plan_summary(plan, mode="execute" if args.execute else "dry-run"), indent=2)
        )
    except (EvidenceArchiveError, OSError) as error:
        print(f"Evidence archive rejected: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
