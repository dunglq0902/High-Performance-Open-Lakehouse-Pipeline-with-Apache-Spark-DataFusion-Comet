"""Build and verify a self-contained, integrity-bound research evidence bundle."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
import zipfile
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

from benchmark.runner.canonical import sha256_value
from benchmark.runner.evidence import (
    ArtifactEvidenceError,
    RepositoryEvidenceError,
    clean_git_commit,
    control_artifact_evidence,
    isolated_git_environment,
    raw_records_sha256,
)

ROOT = Path(__file__).resolve().parents[1]
MANIFEST_NAME = "RELEASE-MANIFEST.json"
CHECKSUMS_NAME = "SHA256SUMS"
RESTORE_NAME = "RESTORE.md"

_MANIFEST_SCHEMA = ROOT / "benchmark/schemas/evidence-bundle-manifest.schema.json"
_STORED_SUFFIXES = frozenset(
    {".parquet", ".pptx", ".mp4", ".zip", ".gz", ".tgz", ".jar", ".png", ".jpg", ".jpeg"}
)
_TEXT_SUFFIXES = frozenset(
    {
        ".conf",
        ".csv",
        ".json",
        ".log",
        ".md",
        ".properties",
        ".sql",
        ".txt",
        ".yaml",
        ".yml",
    }
)
_RUN_ATTEMPT_PATTERN = re.compile(r"^attempt-([0-9]{4})$")
_CONTROL_LABEL_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_.-]*$")
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_HISTORY_SERVER_URL = "http://127.0.0.1:18080"
_REPORT_INVENTORY_CANONICALIZATION = "json-sort-keys-compact-utf8-v1"
_DEMO_ENGINES = ("spark_baseline", "comet_accelerated")
_PRESENTATION_VISUAL_REVIEW_SCOPE = (
    "All 12 rendered slides were inspected for clipping, overlap, legibility, chart/table "
    "rendering, and the correct publication or diagnostic label."
)


class EvidenceBundleError(ValueError):
    """Evidence inputs or a produced bundle violate the release contract."""


@dataclass(frozen=True, slots=True)
class BundleSource:
    source_path: Path
    archive_path: str
    roles: tuple[str, ...]
    kind: str = "file"
    size_bytes: int | None = None
    sha256: str | None = None


@dataclass(slots=True)
class _MutableSource:
    source_path: Path
    roles: set[str]
    size_bytes: int
    sha256: str


@dataclass(slots=True)
class _MutableDirectory:
    source_path: Path
    roles: set[str]


@dataclass(frozen=True, slots=True)
class _DemoApplicationBinding:
    source_event_log: str
    staged_event_log: str
    event_log_inventory: tuple[dict[str, object], ...]
    video_value: dict[str, object]


@dataclass(frozen=True, slots=True)
class _DemoManifestBinding:
    applications: tuple[_DemoApplicationBinding, ...]
    event_log_binding: tuple[dict[str, object], ...]
    source_file_count: int
    staged_file_count: int


class SourceCollector:
    """Collect regular files and empty directories into case-safe archive paths."""

    def __init__(self, repository_root: Path) -> None:
        self.repository_root = repository_root.resolve()
        self._sources: dict[str, _MutableSource] = {}
        self._empty_directories: dict[str, _MutableDirectory] = {}
        self._folded_paths: dict[str, str] = {}

    def _add(self, source: Path, archive_path: str, role: str) -> None:
        archive_path = safe_archive_path(archive_path)
        source = source.resolve()
        if not source.is_file() or source.is_symlink():
            raise EvidenceBundleError(f"bundle source must be a regular file: {source}")
        if archive_path in self._empty_directories or any(
            archive_path.startswith(f"{directory}/") for directory in self._empty_directories
        ):
            raise EvidenceBundleError(
                f"bundle file conflicts with a declared empty directory: {archive_path}"
            )
        folded = archive_path.casefold()
        collision = self._folded_paths.get(folded)
        if collision is not None and collision != archive_path:
            raise EvidenceBundleError(
                f"case-colliding archive paths are forbidden: {collision}, {archive_path}"
            )
        self._folded_paths[folded] = archive_path
        existing = self._sources.get(archive_path)
        if existing is not None:
            if existing.source_path != source:
                raise EvidenceBundleError(f"archive path has multiple sources: {archive_path}")
            existing.roles.add(role)
            return
        size_bytes = source.stat().st_size
        sha256 = _sha256_file(source)
        if source.stat().st_size != size_bytes:
            raise EvidenceBundleError(f"bundle source changed while fingerprinting: {source}")
        self._sources[archive_path] = _MutableSource(
            source_path=source,
            roles={role},
            size_bytes=size_bytes,
            sha256=sha256,
        )

    def _add_empty_directory(self, path: Path, archive_path: str, role: str) -> None:
        archive_path = safe_archive_path(archive_path)
        if not path.is_dir() or path.is_symlink():
            raise EvidenceBundleError(f"bundle empty directory must be a real directory: {path}")
        if any(
            source == archive_path or source.startswith(f"{archive_path}/")
            for source in self._sources
        ):
            raise EvidenceBundleError(
                f"declared empty directory contains a collected file: {archive_path}"
            )
        folded = archive_path.casefold()
        collision = self._folded_paths.get(folded)
        if collision is not None and collision != archive_path:
            raise EvidenceBundleError(
                f"case-colliding archive paths are forbidden: {collision}, {archive_path}"
            )
        self._folded_paths[folded] = archive_path
        existing = self._empty_directories.get(archive_path)
        if existing is not None:
            existing.roles.add(role)
            return
        if any(path.iterdir()):
            raise EvidenceBundleError(f"bundle directory is not empty: {path}")
        self._empty_directories[archive_path] = _MutableDirectory(
            source_path=path.resolve(), roles={role}
        )

    def add_repository_path(self, path: Path, role: str) -> None:
        resolved = path.resolve()
        try:
            relative = resolved.relative_to(self.repository_root)
        except ValueError as error:
            raise EvidenceBundleError(f"repository evidence path escapes root: {path}") from error
        if path.is_symlink():
            raise EvidenceBundleError(f"repository evidence path is a symbolic link: {path}")
        if path.is_file():
            self._add(path, (PurePosixPath("repository") / relative.as_posix()).as_posix(), role)
            return
        if not path.is_dir():
            raise EvidenceBundleError(f"repository evidence path is unavailable: {path}")
        descendants = tuple(sorted(path.rglob("*")))
        for child in descendants:
            if child.is_symlink():
                raise EvidenceBundleError(f"repository evidence contains a symbolic link: {child}")
            if child.is_dir():
                continue
            if not child.is_file():
                raise EvidenceBundleError(f"repository evidence contains unsupported path: {child}")
            child_relative = child.resolve().relative_to(self.repository_root)
            self._add(
                child,
                (PurePosixPath("repository") / child_relative.as_posix()).as_posix(),
                role,
            )
        for directory in (path, *(child for child in descendants if child.is_dir())):
            if any(directory.iterdir()):
                continue
            directory_relative = directory.resolve().relative_to(self.repository_root)
            self._add_empty_directory(
                directory,
                (PurePosixPath("repository") / directory_relative.as_posix()).as_posix(),
                role,
            )

    def add_external_file(self, path: Path, archive_path: str, role: str) -> None:
        self._add(path, archive_path, role)

    def sources(self) -> tuple[BundleSource, ...]:
        files = [
            BundleSource(
                source_path=value.source_path,
                archive_path=archive_path,
                roles=tuple(sorted(value.roles)),
                size_bytes=value.size_bytes,
                sha256=value.sha256,
            )
            for archive_path, value in sorted(self._sources.items())
        ]
        directories = [
            BundleSource(
                source_path=value.source_path,
                archive_path=archive_path,
                roles=tuple(sorted(value.roles)),
                kind="empty_directory",
            )
            for archive_path, value in sorted(self._empty_directories.items())
        ]
        return tuple(sorted((*files, *directories), key=lambda source: source.archive_path))

    def empty_directories(self) -> tuple[dict[str, object], ...]:
        """Return the exact empty-directory inventory for manifest binding."""

        return tuple(
            {"path": archive_path, "roles": sorted(value.roles)}
            for archive_path, value in sorted(self._empty_directories.items())
        )


def safe_archive_path(value: str) -> str:
    if not isinstance(value, str) or not value or value.startswith("/") or "\\" in value:
        raise EvidenceBundleError(f"unsafe archive path: {value!r}")
    parts = value.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise EvidenceBundleError(f"unsafe archive path: {value!r}")
    pure = PurePosixPath(value)
    if pure.is_absolute() or ":" in parts[0]:
        raise EvidenceBundleError(f"unsafe archive path: {value!r}")
    return pure.as_posix()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _load_object(path: Path, *, label: str) -> dict[str, Any]:
    if not path.is_file() or path.is_symlink():
        raise EvidenceBundleError(f"{label} must be a regular file: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise EvidenceBundleError(f"cannot read {label} {path}: {error}") from error
    if not isinstance(value, dict):
        raise EvidenceBundleError(f"{label} root must be an object: {path}")
    return value


def _load_archive_object(archive: zipfile.ZipFile, name: str, *, label: str) -> dict[str, Any]:
    try:
        payload = archive.read(name)
    except KeyError as error:
        raise EvidenceBundleError(f"bundle is missing {label}: {name}") from error
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as error:
        raise EvidenceBundleError(f"bundle {label} is invalid: {error}") from error
    if not isinstance(value, dict):
        raise EvidenceBundleError(f"bundle {label} root must be an object")
    return value


def _resolve_repository_path(repository_root: Path, value: object, *, label: str) -> Path:
    if not isinstance(value, str) or not value:
        raise EvidenceBundleError(f"{label} must be a non-empty repository-relative path")
    candidate = Path(value)
    if candidate.is_absolute():
        raise EvidenceBundleError(f"{label} must be repository-relative: {value}")
    resolved = (repository_root / candidate).resolve()
    try:
        resolved.relative_to(repository_root.resolve())
    except ValueError as error:
        raise EvidenceBundleError(f"{label} escapes repository root: {value}") from error
    return resolved


def _archive_repository_path(repository_root: Path, path: Path) -> str:
    try:
        relative = path.resolve().relative_to(repository_root.resolve())
    except ValueError as error:
        raise EvidenceBundleError(f"path escapes repository root: {path}") from error
    return (PurePosixPath("repository") / relative.as_posix()).as_posix()


def _bound_file(
    repository_root: Path,
    declaration: Mapping[str, Any],
    *,
    field: str,
    label: str,
) -> Path:
    path = _resolve_repository_path(repository_root, declaration.get("path"), label=label)
    if not path.is_file() or path.is_symlink():
        raise EvidenceBundleError(f"{label} is unavailable: {path}")
    size = declaration.get("size_bytes")
    if isinstance(size, bool) or not isinstance(size, int) or size != path.stat().st_size:
        raise EvidenceBundleError(f"{label} size binding is invalid")
    digest = declaration.get("sha256")
    if digest != _sha256_file(path):
        raise EvidenceBundleError(f"{label} SHA-256 binding is invalid")
    value = declaration.get(field)
    if value is not None and value != path.suffix.lower().removeprefix("."):
        raise EvidenceBundleError(f"{label} format binding is invalid")
    return path


def _verify_report_inventory(
    report_dir: Path,
    inventory_path: Path,
) -> dict[str, Any]:
    inventory = _load_object(inventory_path, label="report artifact inventory")
    values, _, _ = _report_inventory_summary(inventory)
    declared: set[str] = set()
    for value in values:
        relative = str(value["path"])
        declared.add(relative)
        path = report_dir / relative
        if not path.is_file() or path.is_symlink():
            raise EvidenceBundleError(f"report inventory artifact is unavailable: {path}")
        if value.get("size_bytes") != path.stat().st_size or value.get("sha256") != _sha256_file(
            path
        ):
            raise EvidenceBundleError(f"report inventory binding is invalid: {relative}")
    actual = {
        path.name
        for path in report_dir.iterdir()
        if path.is_file() and not path.is_symlink() and path.name != ".gitkeep"
    }
    expected = declared | {inventory_path.name}
    if actual != expected:
        raise EvidenceBundleError(
            "report directory differs from its exact inventory: "
            f"missing={sorted(expected - actual)}, unexpected={sorted(actual - expected)}"
        )
    return inventory


def _report_inventory_summary(
    inventory: Mapping[str, Any],
) -> tuple[list[Mapping[str, Any]], int, str]:
    expected_keys = {"schema_version", "scope", "artifact_count", "artifacts"}
    values = inventory.get("artifacts")
    artifact_count = inventory.get("artifact_count")
    if (
        set(inventory) != expected_keys
        or inventory.get("schema_version") != 1
        or inventory.get("scope") != "generated-report-artifacts-excluding-this-inventory"
        or not isinstance(values, list)
        or isinstance(artifact_count, bool)
        or not isinstance(artifact_count, int)
        or artifact_count != len(values)
    ):
        raise EvidenceBundleError("report artifact inventory header is invalid")

    normalized: list[Mapping[str, Any]] = []
    paths: list[str] = []
    for index, value in enumerate(values):
        if not isinstance(value, Mapping) or set(value) != {"path", "size_bytes", "sha256"}:
            raise EvidenceBundleError(f"report inventory entry {index} fields are invalid")
        relative = safe_archive_path(str(value.get("path", "")))
        if "/" in relative or value.get("path") != relative:
            raise EvidenceBundleError(f"report inventory entry must be flat: {relative}")
        size = value.get("size_bytes")
        digest = value.get("sha256")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise EvidenceBundleError(f"report inventory entry {index} size is invalid")
        if not isinstance(digest, str) or _SHA256_PATTERN.fullmatch(digest) is None:
            raise EvidenceBundleError(f"report inventory entry {index} SHA-256 is invalid")
        paths.append(relative)
        normalized.append(value)
    if paths != sorted(paths):
        raise EvidenceBundleError("report inventory artifact paths must be sorted")
    if len({path.casefold() for path in paths}) != len(paths):
        raise EvidenceBundleError("report inventory paths must be unique under case folding")
    total_bytes = sum(int(value["size_bytes"]) for value in normalized)
    return normalized, total_bytes, sha256_value(normalized)


def _integer(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise EvidenceBundleError(f"{label} must be an integer")
    return value


def _raw_campaigns(raw_root: Path) -> dict[str, list[dict[str, Any]]]:
    campaigns: dict[str, list[dict[str, Any]]] = {}
    identities: set[tuple[str, str]] = set()
    for path in sorted(raw_root.rglob("*.json")):
        record = _load_object(path, label="accepted raw record")
        experiment_id = record.get("experiment_id")
        run_id = record.get("run_id")
        if not isinstance(experiment_id, str) or not experiment_id:
            raise EvidenceBundleError(f"raw record has invalid experiment ID: {path}")
        if not isinstance(run_id, str) or not run_id:
            raise EvidenceBundleError(f"raw record has invalid run ID: {path}")
        identity = (experiment_id, run_id)
        if identity in identities:
            raise EvidenceBundleError(f"duplicate raw record identity: {experiment_id}/{run_id}")
        identities.add(identity)
        campaigns.setdefault(experiment_id, []).append(record)
    if not campaigns:
        raise EvidenceBundleError(f"accepted raw record root is empty: {raw_root}")
    return campaigns


def _verify_campaign_attempt_controls(
    repository_root: Path,
    experiment_id: str,
    records: list[dict[str, Any]],
    verification: Mapping[str, Any],
    targets: Mapping[str, Path],
) -> None:
    run_root = targets.get("run-attempts")
    failed_root = targets.get("failed-attempt-records")
    canonical_root = (repository_root / ".artifacts/campaigns" / experiment_id).resolve()
    if (
        run_root is None
        or failed_root is None
        or run_root.resolve() != canonical_root / "runs"
        or failed_root.resolve() != canonical_root / "failed-attempts"
    ):
        raise EvidenceBundleError(f"campaign attempt controls are not canonical: {experiment_id}")

    expected_run_ids = {str(record.get("run_id")) for record in records}
    run_entries = tuple(run_root.iterdir())
    actual_run_ids = {path.name for path in run_entries if path.is_dir()}
    if any(not path.is_dir() or path.is_symlink() for path in run_entries) or (
        actual_run_ids != expected_run_ids
    ):
        raise EvidenceBundleError(
            f"campaign attempt run IDs differ from accepted records: {experiment_id}"
        )

    execution_attempts = 0
    for run_id in sorted(expected_run_ids):
        entries = tuple((run_root / run_id).iterdir())
        attempts: list[int] = []
        for path in entries:
            match = _RUN_ATTEMPT_PATTERN.fullmatch(path.name)
            if not path.is_dir() or path.is_symlink() or match is None:
                raise EvidenceBundleError(
                    f"campaign attempt directory is invalid: {experiment_id}/{run_id}/{path.name}"
                )
            attempts.append(int(match.group(1)))
        attempts.sort()
        if not attempts or attempts != list(range(1, len(attempts) + 1)) or len(attempts) > 3:
            raise EvidenceBundleError(
                f"campaign attempt sequence is invalid: {experiment_id}/{run_id}"
            )
        execution_attempts += len(attempts)

    failed_files = tuple(path for path in failed_root.rglob("*") if path.is_file())
    if any(path.is_symlink() or path.suffix.lower() != ".json" for path in failed_files):
        raise EvidenceBundleError(f"campaign failed-attempt tree is invalid: {experiment_id}")
    declared_execution_attempts = _integer(
        verification.get("execution_attempt_count"),
        label=f"{experiment_id} execution attempt count",
    )
    declared_failed_attempts = _integer(
        verification.get("failed_attempt_record_count"),
        label=f"{experiment_id} failed attempt count",
    )
    if (
        execution_attempts != declared_execution_attempts
        or len(failed_files) != declared_failed_attempts
        or declared_execution_attempts != len(records) + declared_failed_attempts
    ):
        raise EvidenceBundleError(
            f"campaign attempt counts differ from the control tree: {experiment_id}"
        )


def _git_tree(repository_root: Path) -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--verify", "HEAD^{tree}"],
            cwd=repository_root,
            env=isolated_git_environment(),
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        raise EvidenceBundleError(f"Git tree identity is unavailable: {error}") from error
    tree = result.stdout.strip()
    if re.fullmatch(r"[0-9a-f]{40}", tree) is None:
        raise EvidenceBundleError(f"unexpected Git tree object ID: {tree!r}")
    return tree


def _create_repository_bundle(repository_root: Path, output: Path) -> None:
    environment = isolated_git_environment()
    try:
        subprocess.run(
            ["git", "bundle", "create", str(output), "--all"],
            cwd=repository_root,
            env=environment,
            check=True,
            capture_output=True,
            text=True,
        )
        subprocess.run(
            ["git", "bundle", "verify", str(output)],
            cwd=repository_root,
            env=environment,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        raise EvidenceBundleError(
            f"cannot create verified Git repository bundle: {error}"
        ) from error


def _secret_values(repository_root: Path) -> tuple[bytes, ...]:
    env_path = repository_root / ".env"
    if not env_path.is_file() or env_path.is_symlink():
        return ()
    values: set[bytes] = set()
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip().upper()
        value = value.strip().strip("\"'")
        if ("SECRET" in key or "PASSWORD" in key or key == "AWS_ACCESS_KEY_ID") and len(value) >= 8:
            values.add(value.encode("utf-8"))
    return tuple(sorted(values))


def _contains_value(path: Path, value: bytes) -> bool:
    overlap = max(0, len(value) - 1)
    previous = b""
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            payload = previous + chunk
            if value in payload:
                return True
            previous = payload[-overlap:] if overlap else b""
    return False


def _scan_for_local_secrets(sources: Iterable[BundleSource], repository_root: Path) -> None:
    values = _secret_values(repository_root)
    if not values:
        return
    leaks: list[str] = []
    for source in sources:
        if source.kind != "file":
            continue
        path = source.source_path
        if path.suffix.lower() not in _TEXT_SUFFIXES and not path.name.startswith(
            ("events_", "appstatus_")
        ):
            continue
        if any(_contains_value(path, value) for value in values):
            leaks.append(source.archive_path)
    if leaks:
        raise EvidenceBundleError(
            "local credential value detected in candidate evidence files: " + ", ".join(leaks)
        )


def _verify_attested_inventory(
    attestation: Mapping[str, Any],
    dataset_root: Path,
    identity_cache: dict[Path, tuple[int, str]],
) -> None:
    for field, required in (
        ("runtime_parquet_inventory", True),
        ("runtime_source_inventory", False),
    ):
        raw_values = attestation.get(field)
        if raw_values is None and not required:
            continue
        if not isinstance(raw_values, list) or (required and not raw_values):
            raise EvidenceBundleError(f"dataset attestation {field} is invalid")
        for index, value in enumerate(raw_values):
            if not isinstance(value, Mapping):
                raise EvidenceBundleError(f"dataset attestation {field}[{index}] is invalid")
            relative = safe_archive_path(str(value.get("path", "")))
            path = (dataset_root / Path(relative)).resolve()
            try:
                path.relative_to(dataset_root.resolve())
            except ValueError as error:
                raise EvidenceBundleError(
                    f"dataset attestation path escapes its dataset root: {relative}"
                ) from error
            if not path.is_file() or path.is_symlink():
                raise EvidenceBundleError(f"attested dataset file is unavailable: {path}")
            observed = identity_cache.get(path)
            if observed is None:
                observed = (path.stat().st_size, _sha256_file(path))
                identity_cache[path] = observed
            if value.get("size_bytes") != observed[0] or value.get("sha256") != observed[1]:
                raise EvidenceBundleError(f"attested dataset file binding is invalid: {path}")


def _dataset_closure(
    repository_root: Path,
    collector: SourceCollector,
    attestation_paths: Iterable[Path],
) -> list[dict[str, object]]:
    pending = list(attestation_paths)
    visited: set[Path] = set()
    datasets: dict[str, dict[str, object]] = {}
    identity_cache: dict[Path, tuple[int, str]] = {}
    while pending:
        path = pending.pop().resolve()
        if path in visited:
            continue
        visited.add(path)
        collector.add_repository_path(path, "dataset_attestation")
        attestation = _load_object(path, label="dataset validation attestation")
        dataset = attestation.get("dataset")
        if not isinstance(dataset, Mapping):
            raise EvidenceBundleError(f"attestation has no dataset identity: {path}")
        manifest_path = _resolve_repository_path(
            repository_root,
            dataset.get("manifest_path"),
            label="dataset manifest",
        )
        if not manifest_path.is_file() or manifest_path.is_symlink():
            raise EvidenceBundleError(f"dataset manifest is unavailable: {manifest_path}")
        declared_manifest_hash = dataset.get("manifest_sha256")
        if declared_manifest_hash != _sha256_file(manifest_path):
            raise EvidenceBundleError(f"dataset manifest SHA-256 is invalid: {manifest_path}")
        _verify_attested_inventory(attestation, manifest_path.parent, identity_cache)
        collector.add_repository_path(manifest_path.parent, "primary_dataset")
        dataset_id = dataset.get("dataset_id")
        if not isinstance(dataset_id, str) or not dataset_id:
            raise EvidenceBundleError(f"attestation dataset ID is invalid: {path}")
        base_entry: dict[str, object] = {
            "dataset_id": dataset_id,
            "root": _archive_repository_path(repository_root, manifest_path.parent),
            "manifest_path": _archive_repository_path(repository_root, manifest_path),
            "manifest_sha256": str(declared_manifest_hash),
        }
        existing = datasets.get(dataset_id)
        if (
            existing is not None
            and {key: value for key, value in existing.items() if key != "attestation_paths"}
            != base_entry
        ):
            raise EvidenceBundleError(f"dataset identity is inconsistent: {dataset_id}")
        if existing is None:
            existing = {**base_entry, "attestation_paths": []}
            datasets[dataset_id] = existing
        declared_attestations = existing["attestation_paths"]
        if not isinstance(declared_attestations, list):
            raise EvidenceBundleError("internal dataset attestation list is invalid")
        declared_attestations.append(_archive_repository_path(repository_root, path))
        validation = attestation.get("validation")
        origin = validation.get("origin") if isinstance(validation, Mapping) else None
        if isinstance(origin, Mapping) and origin.get("attestation_path") is not None:
            pending.append(
                _resolve_repository_path(
                    repository_root,
                    origin.get("attestation_path"),
                    label="origin dataset attestation",
                )
            )
    if len(datasets) != 2:
        raise EvidenceBundleError(
            f"final evidence bundle requires exactly two datasets, found {len(datasets)}"
        )
    result = [datasets[key] for key in sorted(datasets)]
    for entry in result:
        values = entry["attestation_paths"]
        if not isinstance(values, list):
            raise EvidenceBundleError("internal dataset attestation list is invalid")
        entry["attestation_paths"] = sorted(set(values))
    return result


def _canonical_relative_path(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise EvidenceBundleError(f"{label} must be a non-empty relative path")
    canonical = safe_archive_path(value)
    if canonical != value or any(":" in part for part in PurePosixPath(canonical).parts):
        raise EvidenceBundleError(f"{label} must be a canonical relative POSIX path")
    return canonical


def _required_string(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise EvidenceBundleError(f"{label} must be a non-empty string")
    return value


def _required_sha256(value: object, *, label: str) -> str:
    digest = _required_string(value, label=label)
    if _SHA256_PATTERN.fullmatch(digest) is None:
        raise EvidenceBundleError(f"{label} must be a lowercase SHA-256")
    return digest


def _nonnegative_integer(value: object, *, label: str) -> int:
    result = _integer(value, label=label)
    if result < 0:
        raise EvidenceBundleError(f"{label} must be non-negative")
    return result


def _positive_number(value: object, *, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float) or value <= 0:
        raise EvidenceBundleError(f"{label} must be a positive number")
    return float(value)


def _file_inventory(value: object, *, label: str) -> tuple[dict[str, object], ...]:
    if not isinstance(value, list) or not value:
        raise EvidenceBundleError(f"{label} must be a non-empty file inventory")
    result: list[dict[str, object]] = []
    for index, item in enumerate(value):
        if not isinstance(item, Mapping) or set(item) != {"path", "size_bytes", "sha256"}:
            raise EvidenceBundleError(f"{label} entry {index} fields are invalid")
        relative = _canonical_relative_path(item.get("path"), label=f"{label} path")
        size = _nonnegative_integer(item.get("size_bytes"), label=f"{label} size")
        digest = _required_sha256(item.get("sha256"), label=f"{label} SHA-256")
        result.append({"path": relative, "size_bytes": size, "sha256": digest})
    paths = [str(item["path"]) for item in result]
    if paths != sorted(paths):
        raise EvidenceBundleError(f"{label} paths must be sorted")
    if len({path.casefold() for path in paths}) != len(paths):
        raise EvidenceBundleError(f"{label} paths must be unique under case folding")
    return tuple(result)


def _verify_repository_directory_inventory(
    root: Path,
    declared: tuple[dict[str, object], ...],
    *,
    label: str,
) -> None:
    if not root.is_dir() or root.is_symlink():
        raise EvidenceBundleError(f"{label} must be a regular directory: {root}")
    observed: list[str] = []
    for child in root.rglob("*"):
        if child.is_symlink():
            raise EvidenceBundleError(f"{label} contains a symbolic link: {child}")
        if child.is_file():
            observed.append(child.relative_to(root).as_posix())
        elif child.is_dir():
            if not any(child.iterdir()):
                raise EvidenceBundleError(
                    f"{label} contains an undeclared empty directory: {child}"
                )
        else:
            raise EvidenceBundleError(f"{label} contains a non-regular entry: {child}")
    expected = [str(item["path"]) for item in declared]
    if sorted(observed) != expected:
        raise EvidenceBundleError(f"{label} file set differs from its exact inventory")
    for item in declared:
        file_path = root.joinpath(*PurePosixPath(str(item["path"])).parts)
        if (
            not file_path.is_file()
            or file_path.is_symlink()
            or file_path.stat().st_size != item["size_bytes"]
            or _sha256_file(file_path) != item["sha256"]
        ):
            raise EvidenceBundleError(
                f"{label} file differs from its size/SHA-256 binding: {item['path']}"
            )


def _demo_manifest_binding(
    manifest: Mapping[str, Any],
    *,
    demo_repository_path: str,
    commit: str,
    report_publishability_path: str,
    report_publishability_sha256: str,
) -> _DemoManifestBinding:
    expected_root_fields = {
        "schema_version",
        "status",
        "experiment_id",
        "pair_index",
        "workload",
        "query_id",
        "storage_profile",
        "git_commit",
        "dataset_manifest_sha256",
        "sql_sha256",
        "iceberg_snapshot_ids",
        "correctness",
        "report_publishability",
        "applications",
        "history_server",
        "demo_disclosure",
    }
    if set(manifest) != expected_root_fields or manifest.get("schema_version") != 2:
        raise EvidenceBundleError("source demo manifest fields do not match schema version 2")
    if (
        manifest.get("status") != "publishable"
        or manifest.get("git_commit") != commit
        or manifest.get("demo_disclosure") != "Publication evidence"
    ):
        raise EvidenceBundleError("source demo manifest is not publishable for the release commit")
    if re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        raise EvidenceBundleError("source demo manifest Git commit is invalid")
    experiment_id = _required_string(manifest.get("experiment_id"), label="demo experiment ID")
    pair_index = _integer(manifest.get("pair_index"), label="demo pair index")
    if pair_index < 1:
        raise EvidenceBundleError("demo pair index must be positive")
    _required_string(manifest.get("workload"), label="demo workload")
    _required_string(manifest.get("query_id"), label="demo query ID")
    _required_string(manifest.get("storage_profile"), label="demo storage profile")
    _required_sha256(manifest.get("dataset_manifest_sha256"), label="demo dataset manifest hash")
    _required_sha256(manifest.get("sql_sha256"), label="demo SQL hash")
    correctness = manifest.get("correctness")
    if not isinstance(correctness, Mapping) or set(correctness) != {
        "status",
        "schema_sha256",
        "row_count",
        "canonical_result_sha256",
    }:
        raise EvidenceBundleError("source demo correctness binding is invalid")

    report = manifest.get("report_publishability")
    expected_report = {
        "path": report_publishability_path,
        "sha256": report_publishability_sha256,
        "publishable": True,
        "report_contract_passed": True,
    }
    if report != expected_report:
        raise EvidenceBundleError("source demo manifest does not bind the exact release report")

    values = manifest.get("applications")
    if not isinstance(values, list) or len(values) != 2:
        raise EvidenceBundleError("source demo manifest must contain exactly two applications")
    application_fields = {
        "engine",
        "run_id",
        "raw_record",
        "raw_record_sha256",
        "source_event_log",
        "source_event_log_inventory",
        "staged_event_log",
        "staged_event_log_inventory",
        "application_id",
        "application_name",
        "event_count",
        "sql_execution_count",
        "application_start_time_ms",
        "application_end_time_ms",
        "measured_sql_execution_id",
        "measured_sql_execution_description",
        "measured_sql_execution_start_time_ms",
        "measured_sql_execution_end_time_ms",
        "measured_sql_execution_duration_ms",
        "measured_sql_execution_url",
        "query_wall_time_ms",
        "sql_execution_time_ms",
        "spark_conf_sha256",
        "native_coverage_ratio",
        "native_operator_count",
        "fallback_operator_count",
        "transition_count",
    }
    applications: list[_DemoApplicationBinding] = []
    app_ids: set[str] = set()
    run_ids: set[str] = set()
    for index, value in enumerate(values):
        if not isinstance(value, Mapping) or set(value) != application_fields:
            raise EvidenceBundleError(f"source demo application {index + 1} fields are invalid")
        engine = _required_string(value.get("engine"), label="demo application engine")
        if engine != _DEMO_ENGINES[index]:
            raise EvidenceBundleError("source demo applications must order baseline then Comet")
        run_id = _required_string(value.get("run_id"), label="demo application run ID")
        application_id = _required_string(value.get("application_id"), label="demo application ID")
        if "/" in run_id or "/" in application_id:
            raise EvidenceBundleError("demo application identifiers must be path-safe")
        if run_id in run_ids or application_id in app_ids:
            raise EvidenceBundleError("demo application and run IDs must be distinct")
        run_ids.add(run_id)
        app_ids.add(application_id)
        application_name = _required_string(
            value.get("application_name"), label="demo application name"
        )
        raw_record = _canonical_relative_path(value.get("raw_record"), label="demo raw record")
        raw_record_sha256 = _required_sha256(
            value.get("raw_record_sha256"), label="demo raw record SHA-256"
        )
        source_event_log = _canonical_relative_path(
            value.get("source_event_log"), label="demo source event log"
        )
        source_parts = PurePosixPath(source_event_log).parts
        if (
            len(source_parts) != 7
            or source_parts[:4] != (".artifacts", "campaigns", experiment_id, "runs")
            or source_parts[4] != run_id
            or _RUN_ATTEMPT_PATTERN.fullmatch(source_parts[5]) is None
            or source_parts[6] != "event-log"
        ):
            raise EvidenceBundleError("demo source event-log path is not canonical")
        staged_event_log = _canonical_relative_path(
            value.get("staged_event_log"), label="demo staged event log"
        )
        if staged_event_log != f"event-logs/eventlog_v2_{application_id}":
            raise EvidenceBundleError("demo staged event-log path is not canonical")
        source_inventory = _file_inventory(
            value.get("source_event_log_inventory"),
            label=f"{engine} source event-log inventory",
        )
        staged_inventory = _file_inventory(
            value.get("staged_event_log_inventory"),
            label=f"{engine} staged event-log inventory",
        )
        if source_inventory != staged_inventory:
            raise EvidenceBundleError("demo source and staged event-log inventories differ")

        event_count = _nonnegative_integer(value.get("event_count"), label="event count")
        sql_count = _nonnegative_integer(
            value.get("sql_execution_count"), label="SQL execution count"
        )
        if event_count == 0 or sql_count == 0:
            raise EvidenceBundleError("demo event and SQL execution counts must be positive")
        app_start = _nonnegative_integer(
            value.get("application_start_time_ms"), label="application start time"
        )
        app_end = _nonnegative_integer(
            value.get("application_end_time_ms"), label="application end time"
        )
        execution_id = _nonnegative_integer(
            value.get("measured_sql_execution_id"), label="measured SQL execution ID"
        )
        measured_start = _nonnegative_integer(
            value.get("measured_sql_execution_start_time_ms"),
            label="measured SQL execution start time",
        )
        measured_end = _nonnegative_integer(
            value.get("measured_sql_execution_end_time_ms"),
            label="measured SQL execution end time",
        )
        measured_duration = _nonnegative_integer(
            value.get("measured_sql_execution_duration_ms"),
            label="measured SQL execution duration",
        )
        if (
            measured_duration == 0
            or app_end < app_start
            or measured_start < app_start
            or measured_end > app_end
            or measured_end - measured_start != measured_duration
            or value.get("sql_execution_time_ms") != value.get("measured_sql_execution_duration_ms")
        ):
            raise EvidenceBundleError("demo measured SQL execution timing is inconsistent")
        if value.get("measured_sql_execution_description") != (
            f"measured terminal action for {run_id}"
        ):
            raise EvidenceBundleError("demo measured SQL execution description is not canonical")
        measured_url = (
            f"{_HISTORY_SERVER_URL}/history/{application_id}/SQL/execution/?id={execution_id}"
        )
        if value.get("measured_sql_execution_url") != measured_url:
            raise EvidenceBundleError("demo measured SQL execution URL is not canonical")
        _positive_number(value.get("query_wall_time_ms"), label="query wall time")
        _required_sha256(value.get("spark_conf_sha256"), label="Spark configuration hash")
        native_coverage = value.get("native_coverage_ratio")
        if (
            isinstance(native_coverage, bool)
            or not isinstance(native_coverage, int | float)
            or not 0 <= native_coverage <= 1
        ):
            raise EvidenceBundleError("demo native coverage ratio is invalid")
        for field in ("native_operator_count", "fallback_operator_count", "transition_count"):
            _nonnegative_integer(value.get(field), label=f"demo {field}")

        canonical_inventory = [dict(item) for item in source_inventory]
        video_value: dict[str, object] = {
            "engine": engine,
            "run_id": run_id,
            "application_id": application_id,
            "application_name": application_name,
            "raw_record": raw_record,
            "raw_record_sha256": raw_record_sha256,
            "measured_sql_execution_id": execution_id,
            "measured_sql_execution_description": value["measured_sql_execution_description"],
            "measured_sql_execution_duration_ms": value["measured_sql_execution_duration_ms"],
            "measured_sql_execution_url": measured_url,
            "source_event_log": source_event_log,
            "source_event_log_inventory": canonical_inventory,
            "source_event_log_inventory_sha256": sha256_value(canonical_inventory),
            "staged_event_log": staged_event_log,
            "staged_event_log_inventory": canonical_inventory,
            "staged_event_log_inventory_sha256": sha256_value(canonical_inventory),
        }
        applications.append(
            _DemoApplicationBinding(
                source_event_log=source_event_log,
                staged_event_log=staged_event_log,
                event_log_inventory=source_inventory,
                video_value=video_value,
            )
        )

    history = manifest.get("history_server")
    demo_parent = PurePosixPath(demo_repository_path).parent.as_posix()
    expected_history = {
        "url": _HISTORY_SERVER_URL,
        "event_log_uri": f"file:///opt/lakehouse/{demo_parent}/event-logs",
        "application_urls": [
            f"{_HISTORY_SERVER_URL}/history/{application.video_value['application_id']}/SQL/"
            for application in applications
        ],
        "measured_execution_urls": [
            application.video_value["measured_sql_execution_url"] for application in applications
        ],
    }
    if history != expected_history:
        raise EvidenceBundleError("source demo History Server binding is not canonical")

    event_log_binding = tuple(
        {
            "engine": application.video_value["engine"],
            "run_id": application.video_value["run_id"],
            "application_id": application.video_value["application_id"],
            "raw_record": application.video_value["raw_record"],
            "raw_record_sha256": application.video_value["raw_record_sha256"],
            "measured_sql_execution_id": application.video_value["measured_sql_execution_id"],
            "measured_sql_execution_description": application.video_value[
                "measured_sql_execution_description"
            ],
            "measured_sql_execution_duration_ms": application.video_value[
                "measured_sql_execution_duration_ms"
            ],
            "source_event_log": application.source_event_log,
            "source_event_log_inventory_sha256": application.video_value[
                "source_event_log_inventory_sha256"
            ],
            "staged_event_log": application.staged_event_log,
            "staged_event_log_inventory_sha256": application.video_value[
                "staged_event_log_inventory_sha256"
            ],
        }
        for application in applications
    )
    file_count = sum(len(application.event_log_inventory) for application in applications)
    return _DemoManifestBinding(
        applications=tuple(applications),
        event_log_binding=event_log_binding,
        source_file_count=file_count,
        staged_file_count=file_count,
    )


def _validate_repository_demo_files(
    repository_root: Path,
    demo_path: Path,
    binding: _DemoManifestBinding,
) -> tuple[Path, ...]:
    source_roots: list[Path] = []
    staged_roots: list[Path] = []
    for application in binding.applications:
        raw_path = _resolve_repository_path(
            repository_root,
            application.video_value["raw_record"],
            label="demo raw record",
        )
        if (
            not raw_path.is_file()
            or raw_path.is_symlink()
            or _sha256_file(raw_path) != application.video_value["raw_record_sha256"]
        ):
            raise EvidenceBundleError("demo raw record differs from its SHA-256 binding")
        raw_record = _load_object(raw_path, label="demo raw record")
        _validate_demo_raw_measurement(raw_record, application.video_value)
        source = _resolve_repository_path(
            repository_root,
            application.source_event_log,
            label="demo source event log",
        )
        staged = (demo_path.parent / Path(application.staged_event_log)).resolve()
        try:
            staged.relative_to(demo_path.parent.resolve())
        except ValueError as error:
            raise EvidenceBundleError("demo staged event log escapes its bundle") from error
        _verify_repository_directory_inventory(
            source, application.event_log_inventory, label="demo source event log"
        )
        _verify_repository_directory_inventory(
            staged, application.event_log_inventory, label="demo staged event log"
        )
        source_roots.append(source)
        staged_roots.append(staged)

    for roots, label in ((source_roots, "source"), (staged_roots, "staged")):
        for index, root in enumerate(roots):
            for other in roots[index + 1 :]:
                if root == other or root in other.parents or other in root.parents:
                    raise EvidenceBundleError(
                        f"demo {label} event-log roots must be distinct and non-overlapping"
                    )
    event_log_root = demo_path.parent / "event-logs"
    if not event_log_root.is_dir() or event_log_root.is_symlink():
        raise EvidenceBundleError("demo staged event-log root is unavailable")
    entries = tuple(event_log_root.iterdir())
    if any(not value.is_dir() or value.is_symlink() for value in entries) or {
        value.resolve() for value in entries
    } != set(staged_roots):
        raise EvidenceBundleError("demo staged event-log root contains unexpected entries")
    return tuple(source_roots)


def _validate_demo_raw_measurement(
    raw_record: Mapping[str, Any], application: Mapping[str, object]
) -> None:
    if (
        raw_record.get("run_id") != application.get("run_id")
        or raw_record.get("engine") != application.get("engine")
        or raw_record.get("status") != "succeeded"
    ):
        raise EvidenceBundleError("demo application identity differs from its accepted raw record")
    metrics = raw_record.get("metrics")
    if (
        not isinstance(metrics, Mapping)
        or metrics.get("sql_execution_id") != application.get("measured_sql_execution_id")
        or metrics.get("sql_execution_time_ms")
        != application.get("measured_sql_execution_duration_ms")
    ):
        raise EvidenceBundleError(
            "demo measured SQL execution differs from its accepted raw record"
        )


def _validate_presentation_manifest(
    repository_root: Path,
    path: Path,
    *,
    commit: str,
    report_publishability_path: Path,
    report_publishability_sha256: str,
    report_inventory_path: Path,
    report_inventory: Mapping[str, Any],
) -> tuple[dict[str, Any], Path]:
    manifest = _load_object(path, label="presentation manifest")
    if (
        set(manifest)
        != {
            "schema_version",
            "status",
            "presentation",
            "report_publishability",
            "report_inventory",
            "git_commit",
            "visual_review",
            "integrity_notice",
        }
        or manifest.get("schema_version") != 2
    ):
        raise EvidenceBundleError("presentation manifest fields do not match schema version 2")
    if manifest.get("status") != "publishable" or manifest.get("git_commit") != commit:
        raise EvidenceBundleError("presentation manifest is not publishable for the release commit")
    report = manifest.get("report_publishability")
    expected_report = {
        "path": report_publishability_path.resolve().relative_to(repository_root).as_posix(),
        "size_bytes": report_publishability_path.stat().st_size,
        "sha256": report_publishability_sha256,
        "publishable": True,
    }
    if report != expected_report:
        raise EvidenceBundleError("presentation manifest does not bind the current report")
    _, artifact_total_bytes, artifact_set_sha256 = _report_inventory_summary(report_inventory)
    expected_inventory = {
        "path": report_inventory_path.resolve().relative_to(repository_root).as_posix(),
        "size_bytes": report_inventory_path.stat().st_size,
        "sha256": _sha256_file(report_inventory_path),
        "artifact_count": report_inventory["artifact_count"],
        "artifact_total_bytes": artifact_total_bytes,
        "artifact_set_sha256": artifact_set_sha256,
        "artifact_set_canonicalization": _REPORT_INVENTORY_CANONICALIZATION,
    }
    if manifest.get("report_inventory") != expected_inventory:
        raise EvidenceBundleError(
            "presentation manifest does not bind the exact report artifact inventory"
        )
    presentation = manifest.get("presentation")
    if not isinstance(presentation, Mapping) or set(presentation) != {
        "path",
        "size_bytes",
        "sha256",
        "format",
        "slide_count",
        "slide_width_emu",
        "slide_height_emu",
        "native_chart_count",
        "native_table_slide_count",
        "notes_slide_count",
        "diagnostic_marker_slide_count",
    }:
        raise EvidenceBundleError("presentation manifest has no presentation binding")
    asset = _bound_file(
        repository_root,
        presentation,
        field="format",
        label="presentation",
    )
    slide_count = _nonnegative_integer(
        presentation.get("slide_count"), label="presentation slide count"
    )
    if slide_count == 0:
        raise EvidenceBundleError("presentation slide count must be positive")
    for field in (
        "slide_width_emu",
        "slide_height_emu",
        "native_chart_count",
        "native_table_slide_count",
        "notes_slide_count",
    ):
        value = _nonnegative_integer(presentation.get(field), label=f"presentation {field}")
        if field in {"slide_width_emu", "slide_height_emu"} and value == 0:
            raise EvidenceBundleError(f"presentation {field} must be positive")
    if presentation.get("notes_slide_count") != slide_count:
        raise EvidenceBundleError("presentation notes must cover every slide")
    if presentation.get("diagnostic_marker_slide_count") != 0:
        raise EvidenceBundleError("publishable presentation contains a diagnostic marker")
    if manifest.get("visual_review") != {
        "confirmed": True,
        "scope": _PRESENTATION_VISUAL_REVIEW_SCOPE,
    }:
        raise EvidenceBundleError("presentation visual review is not publishable")
    _required_string(manifest.get("integrity_notice"), label="presentation integrity notice")
    return manifest, asset


def _validate_video_manifest(
    repository_root: Path,
    path: Path,
    *,
    commit: str,
    report_publishability_path: Path,
    report_publishability_sha256: str,
) -> tuple[dict[str, Any], Path, Path, tuple[Path, ...]]:
    manifest = _load_object(path, label="video manifest")
    if (
        set(manifest)
        != {
            "schema_version",
            "status",
            "video",
            "playback_validation",
            "source_demo_manifest",
            "report_publishability",
            "experiment_id",
            "pair_index",
            "query_id",
            "git_commit",
            "applications",
            "event_log_binding",
            "visual_review",
            "integrity_notice",
        }
        or manifest.get("schema_version") != 2
    ):
        raise EvidenceBundleError("video manifest fields do not match schema version 2")
    if manifest.get("status") != "publishable" or manifest.get("git_commit") != commit:
        raise EvidenceBundleError("video manifest is not publishable for the release commit")
    video = manifest.get("video")
    if not isinstance(video, Mapping) or set(video) != {
        "path",
        "size_bytes",
        "sha256",
        "container",
        "duration_seconds",
        "width",
        "height",
        "codec_name",
        "codec_tag",
        "profile",
        "pixel_format",
        "average_frame_rate",
        "decoded_frame_count",
        "top_level_boxes",
        "container_inspection",
    }:
        raise EvidenceBundleError("video manifest has no video binding")
    asset = _bound_file(repository_root, video, field="container", label="demo video")
    _positive_number(video.get("duration_seconds"), label="demo video duration")
    for field in ("width", "height"):
        if _nonnegative_integer(video.get(field), label=f"demo video {field}") == 0:
            raise EvidenceBundleError(f"demo video {field} must be positive")
    boxes = video.get("top_level_boxes")
    if (
        not isinstance(boxes, list)
        or any(not isinstance(value, str) or not value for value in boxes)
        or not {"ftyp", "moov", "mdat"}.issubset(set(boxes))
    ):
        raise EvidenceBundleError("demo video top-level box inventory is invalid")
    if video.get("container_inspection") != {
        "status": "passed",
        "method": "iso_bmff_box_structure",
        "decodability_established": False,
    }:
        raise EvidenceBundleError("demo video container inspection binding is invalid")

    playback = manifest.get("playback_validation")
    if not isinstance(playback, Mapping):
        raise EvidenceBundleError("video manifest has no playback validation")
    method = playback.get("method")
    if method == "ffprobe_complete_frame_scan":
        if (
            set(playback)
            != {
                "status",
                "method",
                "automated_decoder_validation",
                "full_playback_attested",
                "ffprobe_version",
                "ffprobe_sha256",
            }
            or playback.get("status") != "passed"
            or playback.get("automated_decoder_validation") is not True
        ):
            raise EvidenceBundleError("ffprobe playback validation is not publishable")
        _required_string(playback.get("ffprobe_version"), label="ffprobe version")
        _required_sha256(playback.get("ffprobe_sha256"), label="ffprobe SHA-256")
        if not isinstance(playback.get("full_playback_attested"), bool):
            raise EvidenceBundleError("ffprobe playback attestation flag is invalid")
        for field in ("codec_name", "codec_tag", "pixel_format"):
            _required_string(video.get(field), label=f"decoded video {field}")
        _positive_number(video.get("average_frame_rate"), label="decoded frame rate")
        if _nonnegative_integer(video.get("decoded_frame_count"), label="decoded frame count") == 0:
            raise EvidenceBundleError("decoded frame count must be positive")
    elif method == "explicit_full_playback_attestation":
        if (
            set(playback)
            != {
                "status",
                "method",
                "automated_decoder_validation",
                "full_playback_attested",
                "scope",
            }
            or playback.get("status") != "attested"
            or playback.get("automated_decoder_validation") is not False
            or playback.get("full_playback_attested") is not True
        ):
            raise EvidenceBundleError("explicit playback attestation is not publishable")
        _required_string(playback.get("scope"), label="full playback attestation scope")
    else:
        raise EvidenceBundleError("video playback validation is not publishable")

    source = manifest.get("source_demo_manifest")
    if (
        not isinstance(source, Mapping)
        or set(source)
        != {
            "path",
            "size_bytes",
            "sha256",
            "status",
        }
        or source.get("status") != "publishable"
    ):
        raise EvidenceBundleError("video manifest has no source demo binding")
    demo_path = _bound_file(
        repository_root,
        source,
        field="unused",
        label="source demo manifest",
    )
    demo = _load_object(demo_path, label="source demo manifest")
    demo_relative = demo_path.relative_to(repository_root).as_posix()
    demo_binding = _demo_manifest_binding(
        demo,
        demo_repository_path=demo_relative,
        commit=commit,
        report_publishability_path=report_publishability_path.relative_to(
            repository_root
        ).as_posix(),
        report_publishability_sha256=report_publishability_sha256,
    )
    expected_video_report = {
        "path": report_publishability_path.relative_to(repository_root).as_posix(),
        "size_bytes": report_publishability_path.stat().st_size,
        "sha256": report_publishability_sha256,
        "status": "passed",
        "publishable": True,
        "report_contract_passed": True,
    }
    if manifest.get("report_publishability") != expected_video_report:
        raise EvidenceBundleError("video manifest does not bind the exact release report")
    source_roots = _validate_repository_demo_files(repository_root, demo_path, demo_binding)
    expected_applications = [application.video_value for application in demo_binding.applications]
    if manifest.get("applications") != expected_applications:
        raise EvidenceBundleError("video manifest applications differ from the source demo")
    expected_event_binding = {
        "application_count": 2,
        "binding_sha256": sha256_value(list(demo_binding.event_log_binding)),
        "source_file_count": demo_binding.source_file_count,
        "staged_file_count": demo_binding.staged_file_count,
    }
    if manifest.get("event_log_binding") != expected_event_binding:
        raise EvidenceBundleError("video event-log aggregate binding differs from the source demo")
    if (
        manifest.get("experiment_id") != demo.get("experiment_id")
        or manifest.get("pair_index") != demo.get("pair_index")
        or manifest.get("query_id") != demo.get("query_id")
    ):
        raise EvidenceBundleError("video manifest identity differs from the source demo")
    visual_review = manifest.get("visual_review")
    if (
        not isinstance(visual_review, Mapping)
        or set(visual_review) != {"confirmed", "scope"}
        or visual_review.get("confirmed") is not True
    ):
        raise EvidenceBundleError("video visual review is not publishable")
    _required_string(visual_review.get("scope"), label="video visual review scope")
    _required_string(manifest.get("integrity_notice"), label="video integrity notice")
    return manifest, asset, demo_path, source_roots


def _restore_text(bundle_filename: str, commit: str) -> str:
    return f"""# Restore and verify this evidence release

1. Verify the outer archive hash with `{bundle_filename}.sha256`.
2. Run `python scripts/verify_evidence_bundle.py {bundle_filename}` from a trusted checkout of this
   project. The verifier rejects unsafe paths, duplicates, unexpected files, and any size/hash or
   cross-document binding mismatch.
3. Extract with the verifier's `--extract-to` option. Do not merge into an existing directory.
4. Clone `repository.bundle`, then check out commit `{commit}`. Copy the extracted `repository/`
   tree over that checkout; it contains ignored raw evidence, reports, datasets, and media.
5. Install the locked environment and rerun `make report`. The restored ignored artifacts should
   not dirty the checkout, and strict publication should resolve to the same commit and evidence.

The SHA-256 records prove byte integrity and internal binding, not third-party authenticity. Retain
the outer hash through an independently trusted channel when authenticity matters.
"""


def collect_release_sources(
    *,
    repository_root: Path,
    report_publishability_path: Path,
    report_inventory_path: Path,
    presentation_manifest_path: Path,
    video_manifest_path: Path,
    temporary_dir: Path,
    bundle_filename: str,
) -> tuple[tuple[BundleSource, ...], dict[str, object]]:
    """Resolve the complete final evidence closure and release metadata."""

    repository_root = repository_root.resolve()
    try:
        commit = clean_git_commit(repository_root)
    except RepositoryEvidenceError as error:
        raise EvidenceBundleError(f"release repository is not admissible: {error}") from error
    tree = _git_tree(repository_root)
    publication = _load_object(report_publishability_path, label="report publishability artifact")
    if (
        publication.get("schema_version") != 1
        or isinstance(publication.get("schema_version"), bool)
        or publication.get("publishable") is not True
        or publication.get("status") != "passed"
    ):
        raise EvidenceBundleError("strict report publishability must pass before release packaging")
    contract = publication.get("report_contract")
    if (
        not isinstance(contract, Mapping)
        or contract.get("path") != "report-contract.json"
        or contract.get("status") != "passed"
        or contract.get("passed") is not True
    ):
        raise EvidenceBundleError("report content contract must pass before release packaging")
    checks = publication.get("checks")
    if not isinstance(checks, Mapping):
        raise EvidenceBundleError("report publishability artifact has no checks")
    repository_check = checks.get("repository_provenance")
    if not isinstance(repository_check, Mapping):
        raise EvidenceBundleError("report repository provenance check is missing")
    if (
        repository_check.get("passed") is not True
        or repository_check.get("current_git_commit") != commit
        or repository_check.get("raw_git_commits") != [commit]
    ):
        raise EvidenceBundleError("report provenance does not equal the current clean Git commit")

    report_dir = report_publishability_path.resolve().parent
    if report_inventory_path.resolve().parent != report_dir:
        raise EvidenceBundleError("report publishability and inventory must share one directory")
    report_inventory = _verify_report_inventory(report_dir, report_inventory_path.resolve())
    report_inventory_values, _, _ = _report_inventory_summary(report_inventory)
    if "report-contract.json" not in {str(value["path"]) for value in report_inventory_values}:
        raise EvidenceBundleError("report artifact inventory does not contain report-contract.json")
    contract_artifact = _load_object(
        report_dir / "report-contract.json", label="report content contract artifact"
    )
    if (
        contract_artifact.get("schema_version") != 1
        or contract_artifact.get("status") != "passed"
        or contract_artifact.get("passed") is not True
    ):
        raise EvidenceBundleError("report content contract artifact is not passed")
    publication_hash = _sha256_file(report_publishability_path.resolve())

    collector = SourceCollector(repository_root)
    report_values = report_inventory["artifacts"]
    if not isinstance(report_values, list):
        raise EvidenceBundleError("report artifact inventory is invalid")
    for value in report_values:
        if not isinstance(value, Mapping):
            raise EvidenceBundleError("report artifact inventory entry is invalid")
        collector.add_repository_path(report_dir / str(value["path"]), "generated_report")
    collector.add_repository_path(report_inventory_path, "generated_report_inventory")
    raw_root = repository_root / "results/raw"
    raw_campaigns = _raw_campaigns(raw_root)
    collector.add_repository_path(raw_root, "accepted_raw_records")

    policy = publication.get("policy")
    experiments = policy.get("core_experiments") if isinstance(policy, Mapping) else None
    if not isinstance(experiments, list):
        raise EvidenceBundleError("report core experiment policy is missing")
    experiment_ids = sorted(
        str(value.get("experiment_id")) for value in experiments if isinstance(value, Mapping)
    )
    if len(experiment_ids) != 10 or len(set(experiment_ids)) != 10:
        raise EvidenceBundleError("release requires exactly ten unique core experiments")

    campaign_records = checks.get("campaign_records")
    verification_rows = checks.get("campaign_verifications")
    if not isinstance(campaign_records, list) or not isinstance(verification_rows, list):
        raise EvidenceBundleError("report campaign checks are incomplete")
    record_by_id = {
        str(value.get("experiment_id")): value
        for value in campaign_records
        if isinstance(value, Mapping)
    }
    verification_by_id = {
        str(value.get("experiment_id")): value
        for value in verification_rows
        if isinstance(value, Mapping)
    }
    if set(record_by_id) != set(experiment_ids) or set(verification_by_id) != set(experiment_ids):
        raise EvidenceBundleError(
            "report campaign checks differ from the exact core experiment set"
        )
    if set(raw_campaigns) != set(experiment_ids):
        raise EvidenceBundleError(
            "current raw campaign set differs from the report core experiment set"
        )

    accepted_records = 0
    execution_attempts = 0
    failed_attempts = 0
    attestation_paths: set[Path] = set()
    for experiment_id in experiment_ids:
        record_check = record_by_id[experiment_id]
        verification = verification_by_id[experiment_id]
        if record_check.get("passed") is not True:
            raise EvidenceBundleError(f"campaign raw-record check failed: {experiment_id}")
        current_records = raw_campaigns[experiment_id]
        if record_check.get("observed_records") != len(current_records) or record_check.get(
            "raw_records_sha256"
        ) != raw_records_sha256(current_records):
            raise EvidenceBundleError(
                f"current raw campaign no longer matches the report: {experiment_id}"
            )
        if (
            verification.get("passed") is not True
            or verification.get("attempt_counts_verified") is not True
        ):
            raise EvidenceBundleError(f"campaign verification failed: {experiment_id}")
        accepted_records += _integer(
            record_check.get("observed_records"), label=f"{experiment_id} accepted record count"
        )
        execution_attempts += _integer(
            verification.get("execution_attempt_count"),
            label=f"{experiment_id} execution attempt count",
        )
        failed_attempts += _integer(
            verification.get("failed_attempt_record_count"),
            label=f"{experiment_id} failed attempt count",
        )
        collector.add_repository_path(
            repository_root / ".artifacts/campaigns" / experiment_id,
            "campaign_evidence",
        )
        control = verification.get("control_artifact_evidence")
        if not isinstance(control, Mapping):
            raise EvidenceBundleError(f"campaign control evidence is missing: {experiment_id}")
        targets = control.get("targets")
        if not isinstance(targets, list):
            raise EvidenceBundleError(f"campaign control targets are missing: {experiment_id}")
        resolved_targets: dict[str, Path] = {}
        for target in targets:
            if not isinstance(target, Mapping):
                raise EvidenceBundleError(f"campaign control target is invalid: {experiment_id}")
            label = target.get("label")
            kind = target.get("kind")
            if not isinstance(label, str) or not label or label in resolved_targets:
                raise EvidenceBundleError(
                    f"campaign control target label is invalid: {experiment_id}"
                )
            target_path = _resolve_repository_path(
                repository_root,
                target.get("path"),
                label=f"{experiment_id} control target",
            )
            if (kind == "file" and not target_path.is_file()) or (
                kind == "directory" and not target_path.is_dir()
            ):
                raise EvidenceBundleError(
                    f"campaign control target kind is invalid: {experiment_id}/{label}"
                )
            if kind not in {"file", "directory"} or target_path.is_symlink():
                raise EvidenceBundleError(
                    f"campaign control target type is invalid: {experiment_id}/{label}"
                )
            resolved_targets[label] = target_path
            collector.add_repository_path(target_path, "campaign_control")
            if label == "dataset-validation-attestation":
                attestation_paths.add(target_path)
        try:
            observed_control = control_artifact_evidence(resolved_targets, repository_root)
        except ArtifactEvidenceError as error:
            raise EvidenceBundleError(
                f"cannot reconstruct campaign control evidence: {experiment_id}: {error}"
            ) from error
        if any(
            control.get(field) != observed_control[field]
            for field in ("targets", "file_count", "sha256")
        ):
            raise EvidenceBundleError(
                f"campaign control evidence changed after report publication: {experiment_id}"
            )
        _verify_campaign_attempt_controls(
            repository_root,
            experiment_id,
            current_records,
            verification,
            resolved_targets,
        )

    datasets = _dataset_closure(repository_root, collector, attestation_paths)
    presentation_manifest, presentation_path = _validate_presentation_manifest(
        repository_root,
        presentation_manifest_path.resolve(),
        commit=commit,
        report_publishability_path=report_publishability_path.resolve(),
        report_publishability_sha256=publication_hash,
        report_inventory_path=report_inventory_path.resolve(),
        report_inventory=report_inventory,
    )
    collector.add_repository_path(presentation_manifest_path, "presentation_manifest")
    collector.add_repository_path(presentation_path, "presentation")
    video_manifest, video_path, demo_path, demo_source_roots = _validate_video_manifest(
        repository_root,
        video_manifest_path.resolve(),
        commit=commit,
        report_publishability_path=report_publishability_path.resolve(),
        report_publishability_sha256=publication_hash,
    )
    collector.add_repository_path(video_manifest_path, "video_manifest")
    collector.add_repository_path(video_path, "demo_video")
    collector.add_repository_path(demo_path.parent, "spark_ui_demo_evidence")
    for source_root in demo_source_roots:
        collector.add_repository_path(source_root, "spark_ui_demo_source_event_log")

    repository_bundle = temporary_dir / "repository.bundle"
    _create_repository_bundle(repository_root, repository_bundle)
    collector.add_external_file(repository_bundle, "repository.bundle", "source_repository")
    restore_path = temporary_dir / RESTORE_NAME
    restore_path.write_text(_restore_text(bundle_filename, commit), encoding="utf-8", newline="\n")
    collector.add_external_file(restore_path, RESTORE_NAME, "restore_instructions")

    sources = collector.sources()
    empty_directories = list(collector.empty_directories())
    _scan_for_local_secrets(sources, repository_root)
    metadata: dict[str, object] = {
        "schema_version": 1,
        "artifact_class": "lakehouse-comet-evidence-bundle-v1",
        "status": "publishable",
        "bundle_filename": bundle_filename,
        "git": {"commit": commit, "tree": tree, "repository_bundle": "repository.bundle"},
        "report": {
            "publishability_path": _archive_repository_path(
                repository_root, report_publishability_path
            ),
            "publishability_sha256": publication_hash,
            "inventory_path": _archive_repository_path(repository_root, report_inventory_path),
            "inventory_sha256": _sha256_file(report_inventory_path),
        },
        "campaigns": {
            "count": len(experiment_ids),
            "experiment_ids": experiment_ids,
            "accepted_raw_records": accepted_records,
            "execution_attempts": execution_attempts,
            "failed_attempt_records": failed_attempts,
        },
        "datasets": datasets,
        "presentation_manifest": {
            "path": _archive_repository_path(repository_root, presentation_manifest_path),
            "sha256": _sha256_file(presentation_manifest_path),
        },
        "video_manifest": {
            "path": _archive_repository_path(repository_root, video_manifest_path),
            "sha256": _sha256_file(video_manifest_path),
        },
        "inventory_scope": "all archive files excluding RELEASE-MANIFEST.json and SHA256SUMS",
        "empty_directories": empty_directories,
        "total_empty_directories": len(empty_directories),
        "integrity_notice": (
            "SHA-256 proves byte integrity and internal binding, not independent authenticity."
        ),
    }
    if (
        presentation_manifest.get("status") != "publishable"
        or video_manifest.get("status") != "publishable"
    ):
        raise EvidenceBundleError("release media sidecars must both be publishable")
    return sources, metadata


def _zip_info(name: str, *, stored: bool | None = None) -> zipfile.ZipInfo:
    info = zipfile.ZipInfo(safe_archive_path(name), date_time=(1980, 1, 1, 0, 0, 0))
    info.create_system = 3
    info.external_attr = 0o100644 << 16
    info.flag_bits |= 0x800
    if stored is None:
        stored = Path(name).suffix.lower() in _STORED_SUFFIXES
    info.compress_type = zipfile.ZIP_STORED if stored else zipfile.ZIP_DEFLATED
    return info


def _write_source(
    archive: zipfile.ZipFile,
    source: BundleSource,
) -> dict[str, object]:
    if source.kind != "file":
        raise EvidenceBundleError(
            f"cannot write non-file source as a ZIP member: {source.archive_path}"
        )
    if (
        source.size_bytes is None
        or source.sha256 is None
        or source.source_path.stat().st_size != source.size_bytes
    ):
        raise EvidenceBundleError(f"bundle source changed after collection: {source.source_path}")
    digest = hashlib.sha256()
    size = 0
    info = _zip_info(source.archive_path)
    with (
        source.source_path.open("rb") as input_handle,
        archive.open(info, "w", force_zip64=True) as output_handle,
    ):
        while chunk := input_handle.read(1024 * 1024):
            output_handle.write(chunk)
            digest.update(chunk)
            size += len(chunk)
    if size != source.size_bytes or digest.hexdigest() != source.sha256:
        raise EvidenceBundleError(f"bundle source changed after collection: {source.source_path}")
    return {
        "path": source.archive_path,
        "roles": list(source.roles),
        "size_bytes": size,
        "sha256": source.sha256,
    }


def _validate_manifest(value: Mapping[str, Any]) -> None:
    schema = _load_object(_MANIFEST_SCHEMA, label="evidence bundle manifest schema")
    errors = sorted(
        Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(value),
        key=lambda error: list(error.path),
    )
    if errors:
        details = "; ".join(
            f"{'/'.join(map(str, error.path)) or '<root>'}: {error.message}" for error in errors
        )
        raise EvidenceBundleError(f"evidence bundle manifest schema violation: {details}")


def write_evidence_bundle(
    output_path: Path,
    sources: Iterable[BundleSource],
    metadata: Mapping[str, object],
) -> Path:
    """Write a deterministic ZIP64 bundle and its outer SHA-256 sidecar."""

    output_path = output_path.resolve()
    checksum_path = output_path.with_suffix(output_path.suffix + ".sha256")
    if output_path.suffix.lower() != ".zip":
        raise EvidenceBundleError("evidence bundle output must use the .zip extension")
    if output_path.exists() or checksum_path.exists():
        raise EvidenceBundleError(f"refusing to overwrite evidence bundle output: {output_path}")
    ordered_sources = tuple(sorted(sources, key=lambda source: source.archive_path))
    archive_paths = [source.archive_path for source in ordered_sources]
    if len(archive_paths) != len(set(archive_paths)):
        raise EvidenceBundleError("bundle source archive paths must be unique")
    if len({path.casefold() for path in archive_paths}) != len(archive_paths):
        raise EvidenceBundleError("bundle source archive paths must not collide by case")
    if any(name in {MANIFEST_NAME, CHECKSUMS_NAME} for name in archive_paths):
        raise EvidenceBundleError("bundle sources cannot replace release metadata files")
    file_sources = tuple(source for source in ordered_sources if source.kind == "file")
    directory_sources = tuple(
        source for source in ordered_sources if source.kind == "empty_directory"
    )
    if len(file_sources) + len(directory_sources) != len(ordered_sources):
        raise EvidenceBundleError("bundle source has an unsupported kind")
    expected_empty_directories = [
        {"path": source.archive_path, "roles": list(source.roles)} for source in directory_sources
    ]
    if metadata.get("empty_directories") != expected_empty_directories or metadata.get(
        "total_empty_directories"
    ) != len(directory_sources):
        raise EvidenceBundleError(
            "bundle empty-directory metadata differs from the collected source snapshot"
        )

    def validate_empty_sources() -> None:
        for source in directory_sources:
            if (
                source.source_path.is_symlink()
                or not source.source_path.is_dir()
                or any(source.source_path.iterdir())
            ):
                raise EvidenceBundleError(
                    f"bundle empty directory changed after collection: {source.source_path}"
                )

    validate_empty_sources()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{output_path.name}.", suffix=".tmp", dir=output_path.parent
    )
    os.close(descriptor)
    temporary_path = Path(temporary_name)
    try:
        inventory: list[dict[str, object]] = []
        with zipfile.ZipFile(
            temporary_path,
            "w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=6,
            allowZip64=True,
        ) as archive:
            for source in file_sources:
                inventory.append(_write_source(archive, source))
            validate_empty_sources()
            manifest = dict(metadata)
            manifest["inventory"] = inventory
            manifest["total_files"] = len(inventory)
            manifest["total_bytes"] = sum(
                _integer(item["size_bytes"], label="inventory size") for item in inventory
            )
            manifest["manifest_sha256"] = sha256_value(manifest)
            _validate_manifest(manifest)
            manifest_bytes = (
                json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                + "\n"
            ).encode("utf-8")
            checksums_bytes = "".join(
                f"{item['sha256']}  {item['path']}\n" for item in inventory
            ).encode("utf-8")
            archive.writestr(_zip_info(MANIFEST_NAME), manifest_bytes)
            archive.writestr(_zip_info(CHECKSUMS_NAME), checksums_bytes)
        verify_evidence_bundle(temporary_path, require_outer_checksum=False)
        validate_empty_sources()
        os.replace(temporary_path, output_path)
        temporary_checksum = checksum_path.with_name(f".{checksum_path.name}.tmp")
        temporary_checksum.write_text(
            f"{_sha256_file(output_path)}  {output_path.name}\n",
            encoding="utf-8",
            newline="\n",
        )
        os.replace(temporary_checksum, checksum_path)
        return output_path
    finally:
        if temporary_path.is_file():
            temporary_path.unlink()


def _zip_member_names(archive: zipfile.ZipFile) -> tuple[str, ...]:
    names: list[str] = []
    folded: set[str] = set()
    for info in archive.infolist():
        name = safe_archive_path(info.filename)
        if info.is_dir():
            raise EvidenceBundleError(f"explicit directory entry is forbidden: {name}")
        mode = (info.external_attr >> 16) & 0o170000
        if mode == 0o120000:
            raise EvidenceBundleError(f"symbolic link entry is forbidden: {name}")
        if info.flag_bits & 0x1:
            raise EvidenceBundleError(f"encrypted bundle entry is forbidden: {name}")
        lowered = name.casefold()
        if lowered in folded:
            raise EvidenceBundleError(f"duplicate/case-colliding bundle entry: {name}")
        folded.add(lowered)
        names.append(name)
    return tuple(names)


def _inventory_by_path(manifest: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    values = manifest.get("inventory")
    if not isinstance(values, list):
        raise EvidenceBundleError("bundle manifest inventory must be an array")
    result: dict[str, Mapping[str, Any]] = {}
    folded: set[str] = set()
    for value in values:
        if not isinstance(value, Mapping):
            raise EvidenceBundleError("bundle manifest inventory entry must be an object")
        path = safe_archive_path(str(value.get("path", "")))
        if path in result or path.casefold() in folded:
            raise EvidenceBundleError(f"duplicate/case-colliding inventory path: {path}")
        roles = value.get("roles")
        if not isinstance(roles, list) or roles != sorted(set(roles)):
            raise EvidenceBundleError(f"inventory roles must be unique and sorted: {path}")
        folded.add(path.casefold())
        result[path] = value
    return result


def _empty_directory_paths(
    manifest: Mapping[str, Any],
    inventory: Mapping[str, Mapping[str, Any]],
) -> tuple[str, ...]:
    values = manifest.get("empty_directories")
    if not isinstance(values, list):
        raise EvidenceBundleError("bundle empty-directory inventory must be an array")
    result: list[str] = []
    folded: set[str] = set()
    for value in values:
        if not isinstance(value, Mapping):
            raise EvidenceBundleError("bundle empty-directory entry must be an object")
        path = safe_archive_path(str(value.get("path", "")))
        if not path.startswith("repository/"):
            raise EvidenceBundleError(f"empty directory must be repository evidence: {path}")
        roles = value.get("roles")
        if not isinstance(roles, list) or roles != sorted(set(roles)):
            raise EvidenceBundleError(f"empty-directory roles must be unique and sorted: {path}")
        lowered = path.casefold()
        if lowered in folded:
            raise EvidenceBundleError(f"duplicate/case-colliding empty directory: {path}")
        folded.add(lowered)
        result.append(path)
    if result != sorted(result):
        raise EvidenceBundleError("bundle empty-directory inventory must be path-sorted")
    if manifest.get("total_empty_directories") != len(result):
        raise EvidenceBundleError("bundle empty-directory total is invalid")

    lowered_files = {path.casefold() for path in inventory}
    for index, path in enumerate(result):
        lowered = path.casefold()
        if lowered in lowered_files:
            raise EvidenceBundleError(f"empty directory collides with a bundle file: {path}")
        for file_path in lowered_files:
            if file_path.startswith(f"{lowered}/") or lowered.startswith(f"{file_path}/"):
                raise EvidenceBundleError(
                    f"empty directory conflicts with the file inventory: {path}"
                )
        for other in result[index + 1 :]:
            other_lowered = other.casefold()
            if other_lowered.startswith(f"{lowered}/") or lowered.startswith(f"{other_lowered}/"):
                raise EvidenceBundleError(
                    f"nested paths cannot both be declared empty: {path}, {other}"
                )
    return tuple(result)


def _directory_inventory_from_bundle(
    archive_root: str,
    inventory: Mapping[str, Mapping[str, Any]],
    empty_directories: set[str],
) -> tuple[list[str], list[str]]:
    prefix = f"{archive_root}/"
    files = sorted(path for path in inventory if path.startswith(prefix))
    declared_empty = sorted(
        path for path in empty_directories if path == archive_root or path.startswith(prefix)
    )
    if archive_root not in empty_directories and not files and not declared_empty:
        raise EvidenceBundleError(f"bundled control directory is missing: {archive_root}")

    root = PurePosixPath(archive_root)
    directories: set[str] = set()
    for file_path in files:
        parent = PurePosixPath(file_path).parent
        while parent != root:
            directories.add(parent.as_posix())
            parent = parent.parent
    for directory_path in declared_empty:
        parent = PurePosixPath(directory_path)
        if parent == root:
            continue
        while parent != root:
            directories.add(parent.as_posix())
            parent = parent.parent
    return sorted(directories), files


def _bundled_raw_campaigns(
    archive: zipfile.ZipFile,
    inventory: Mapping[str, Mapping[str, Any]],
    expected_ids: set[str],
) -> dict[str, list[dict[str, Any]]]:
    """Reconstruct accepted raw campaigns from the archive itself.

    The outer inventory proves only that the bytes agree with the release manifest.  This
    reconstruction additionally binds those bytes back to the report's campaign fingerprints, so
    an archive cannot be made to pass merely by editing a raw record and recomputing outer hashes.
    """

    prefix = "repository/results/raw/"
    paths = sorted(path for path in inventory if path.startswith(prefix))
    unexpected = [
        path for path in paths if not path.endswith(".json") and path != prefix + ".gitkeep"
    ]
    if unexpected:
        raise EvidenceBundleError(
            f"bundled raw evidence contains an unexpected file: {unexpected[0]}"
        )
    json_paths = [path for path in paths if path.endswith(".json")]
    if not json_paths:
        raise EvidenceBundleError("bundled accepted raw record root is empty")

    campaigns: dict[str, list[dict[str, Any]]] = {}
    identities: set[tuple[str, str]] = set()
    for archive_path in json_paths:
        relative = PurePosixPath(archive_path.removeprefix(prefix))
        record = _load_archive_object(archive, archive_path, label="accepted raw record")
        experiment_id = record.get("experiment_id")
        run_id = record.get("run_id")
        engine = record.get("engine")
        if (
            not isinstance(experiment_id, str)
            or not experiment_id
            or not isinstance(run_id, str)
            or not run_id
            or not isinstance(engine, str)
            or not engine
        ):
            raise EvidenceBundleError(f"bundled raw record identity is invalid: {archive_path}")
        if experiment_id not in expected_ids:
            raise EvidenceBundleError(
                f"bundled raw record is outside the release experiment set: {archive_path}"
            )
        if relative.parts != (experiment_id, engine, f"{run_id}.json"):
            raise EvidenceBundleError(f"bundled raw record path is not canonical: {archive_path}")
        identity = (experiment_id, run_id)
        if identity in identities:
            raise EvidenceBundleError(
                f"duplicate bundled raw record identity: {experiment_id}/{run_id}"
            )
        identities.add(identity)
        campaigns.setdefault(experiment_id, []).append(record)
    if set(campaigns) != expected_ids:
        raise EvidenceBundleError(
            "bundled raw campaign set differs from the release experiment set"
        )
    return campaigns


def _verify_bundled_attempt_tree(
    archive_root: str,
    directories: list[str],
    files: list[str],
    expected_run_ids: set[str],
    *,
    experiment_id: str,
) -> int:
    root = PurePosixPath(archive_root)
    run_ids: set[str] = set()
    attempts: dict[str, set[int]] = {run_id: set() for run_id in expected_run_ids}

    for directory in directories:
        parts = PurePosixPath(directory).parts[len(root.parts) :]
        if not parts or parts[0] not in expected_run_ids:
            raise EvidenceBundleError(
                f"bundled campaign attempt directory is invalid: {experiment_id}/{directory}"
            )
        run_ids.add(parts[0])
        if len(parts) == 1:
            continue
        match = _RUN_ATTEMPT_PATTERN.fullmatch(parts[1])
        if match is None:
            raise EvidenceBundleError(
                f"bundled campaign attempt directory is invalid: {experiment_id}/{directory}"
            )
        attempts[parts[0]].add(int(match.group(1)))

    for path in files:
        parts = PurePosixPath(path).parts[len(root.parts) :]
        if (
            len(parts) < 3
            or parts[0] not in expected_run_ids
            or _RUN_ATTEMPT_PATTERN.fullmatch(parts[1]) is None
        ):
            raise EvidenceBundleError(
                f"bundled campaign attempt file is invalid: {experiment_id}/{path}"
            )
        run_ids.add(parts[0])

    if run_ids != expected_run_ids:
        raise EvidenceBundleError(
            f"bundled campaign attempt run IDs differ from accepted records: {experiment_id}"
        )
    for run_id, values in attempts.items():
        ordered = sorted(values)
        if not ordered or ordered != list(range(1, len(ordered) + 1)) or len(ordered) > 3:
            raise EvidenceBundleError(
                f"bundled campaign attempt sequence is invalid: {experiment_id}/{run_id}"
            )
    return sum(len(values) for values in attempts.values())


def _verify_bundled_campaign_controls(
    archive: zipfile.ZipFile,
    publication: Mapping[str, Any],
    manifest: Mapping[str, Any],
    inventory: Mapping[str, Mapping[str, Any]],
    empty_directories: tuple[str, ...],
) -> None:
    checks = publication.get("checks")
    campaign_manifest = manifest.get("campaigns")
    if not isinstance(checks, Mapping) or not isinstance(campaign_manifest, Mapping):
        raise EvidenceBundleError("bundle has no campaign control binding")
    raw_verifications = checks.get("campaign_verifications")
    raw_record_checks = checks.get("campaign_records")
    expected_ids = campaign_manifest.get("experiment_ids")
    if (
        not isinstance(raw_verifications, list)
        or not isinstance(raw_record_checks, list)
        or not isinstance(expected_ids, list)
    ):
        raise EvidenceBundleError("bundle campaign verification list is invalid")
    expected_id_set = {value for value in expected_ids if isinstance(value, str) and value}
    if len(expected_id_set) != len(expected_ids):
        raise EvidenceBundleError("bundle campaign experiment IDs are invalid")
    raw_campaigns = _bundled_raw_campaigns(archive, inventory, expected_id_set)

    verifications: dict[str, Mapping[str, Any]] = {}
    for value in raw_verifications:
        if not isinstance(value, Mapping):
            raise EvidenceBundleError("bundled campaign verification is invalid")
        experiment_id = value.get("experiment_id")
        if (
            not isinstance(experiment_id, str)
            or not experiment_id
            or experiment_id in verifications
            or value.get("passed") is not True
            or value.get("attempt_counts_verified") is not True
        ):
            raise EvidenceBundleError("bundled campaign verification identity is invalid")
        verifications[experiment_id] = value
    if set(verifications) != set(expected_ids):
        raise EvidenceBundleError(
            "bundled campaign verifications differ from the release experiment set"
        )
    record_counts: dict[str, int] = {}
    for value in raw_record_checks:
        if not isinstance(value, Mapping):
            raise EvidenceBundleError("bundled campaign record check is invalid")
        experiment_id = value.get("experiment_id")
        if not isinstance(experiment_id, str) or experiment_id in record_counts:
            raise EvidenceBundleError("bundled campaign record-check identity is invalid")
        records = raw_campaigns.get(experiment_id)
        if records is None or value.get("passed") is not True:
            raise EvidenceBundleError(
                f"bundled campaign record check is not passed: {experiment_id}"
            )
        observed_count = _integer(
            value.get("observed_records"), label=f"{experiment_id} accepted record count"
        )
        if observed_count != len(records) or value.get("raw_records_sha256") != raw_records_sha256(
            records
        ):
            raise EvidenceBundleError(
                f"bundled raw campaign differs from its report fingerprint: {experiment_id}"
            )
        record_counts[experiment_id] = observed_count
    if set(record_counts) != set(expected_ids):
        raise EvidenceBundleError(
            "bundled campaign record checks differ from the release experiment set"
        )

    empty_set = set(empty_directories)
    total_execution_attempts = 0
    total_failed_attempts = 0
    for experiment_id in expected_ids:
        verification = verifications[str(experiment_id)]
        expected_run_ids = {str(record["run_id"]) for record in raw_campaigns[str(experiment_id)]}
        control = verification.get("control_artifact_evidence")
        targets = control.get("targets") if isinstance(control, Mapping) else None
        if not isinstance(control, Mapping) or not isinstance(targets, list):
            raise EvidenceBundleError(
                f"bundled campaign control evidence is invalid: {experiment_id}"
            )
        declarations: list[dict[str, str]] = []
        entries: list[dict[str, int | str]] = []
        attempt_directory_count: int | None = None
        failed_record_count: int | None = None
        labels: set[str] = set()
        for target in targets:
            if not isinstance(target, Mapping) or set(target) != {"label", "path", "kind"}:
                raise EvidenceBundleError(
                    f"bundled campaign control target is invalid: {experiment_id}"
                )
            label = target.get("label")
            relative = _canonical_relative_path(
                target.get("path"), label=f"{experiment_id} control target path"
            )
            kind = target.get("kind")
            if (
                not isinstance(label, str)
                or _CONTROL_LABEL_PATTERN.fullmatch(label) is None
                or label in labels
                or kind not in {"file", "directory"}
            ):
                raise EvidenceBundleError(
                    f"bundled campaign control target identity is invalid: {experiment_id}"
                )
            labels.add(label)
            declaration = {"label": label, "path": relative, "kind": str(kind)}
            declarations.append(declaration)
            archive_path = f"repository/{relative}"
            if kind == "file":
                outer = inventory.get(archive_path)
                if outer is None:
                    raise EvidenceBundleError(
                        f"bundled campaign control file is missing: {archive_path}"
                    )
                entries.append(
                    {
                        "label": label,
                        "declared_path": relative,
                        "path": relative,
                        "kind": "file",
                        "size_bytes": _integer(
                            outer.get("size_bytes"), label=f"{archive_path} size"
                        ),
                        "sha256": str(outer.get("sha256")),
                    }
                )
                continue

            directories, files = _directory_inventory_from_bundle(
                archive_path, inventory, empty_set
            )
            for path in directories:
                entries.append(
                    {
                        "label": label,
                        "declared_path": relative,
                        "path": path.removeprefix("repository/"),
                        "kind": "directory",
                    }
                )
            for path in files:
                outer = inventory[path]
                entries.append(
                    {
                        "label": label,
                        "declared_path": relative,
                        "path": path.removeprefix("repository/"),
                        "kind": "file",
                        "size_bytes": _integer(outer.get("size_bytes"), label=f"{path} size"),
                        "sha256": str(outer.get("sha256")),
                    }
                )
            if label == "run-attempts":
                expected_root = f"repository/.artifacts/campaigns/{experiment_id}/runs"
                if archive_path != expected_root:
                    raise EvidenceBundleError(
                        f"bundled campaign attempt control is not canonical: {experiment_id}"
                    )
                attempt_directory_count = _verify_bundled_attempt_tree(
                    archive_path,
                    directories,
                    files,
                    expected_run_ids,
                    experiment_id=str(experiment_id),
                )
            elif label == "failed-attempt-records":
                expected_root = f"repository/.artifacts/campaigns/{experiment_id}/failed-attempts"
                if archive_path != expected_root:
                    raise EvidenceBundleError(
                        f"bundled failed-attempt control is not canonical: {experiment_id}"
                    )
                if any(not path.lower().endswith(".json") for path in files):
                    raise EvidenceBundleError(
                        f"bundled failed-attempt tree is invalid: {experiment_id}"
                    )
                failed_record_count = sum(path.lower().endswith(".json") for path in files)

        if "run-attempts" not in labels or "failed-attempt-records" not in labels:
            raise EvidenceBundleError(
                f"bundled campaign attempt controls are incomplete: {experiment_id}"
            )

        observed = {
            "targets": declarations,
            "file_count": sum(entry["kind"] == "file" for entry in entries),
            "sha256": sha256_value({"targets": declarations, "entries": entries}),
        }
        if any(control.get(field) != observed[field] for field in observed):
            raise EvidenceBundleError(
                f"bundled campaign controls differ from their fingerprint: {experiment_id}"
            )
        execution_attempts = _integer(
            verification.get("execution_attempt_count"),
            label=f"{experiment_id} execution attempt count",
        )
        failed_attempts = _integer(
            verification.get("failed_attempt_record_count"),
            label=f"{experiment_id} failed attempt count",
        )
        if (
            attempt_directory_count != execution_attempts
            or failed_record_count != failed_attempts
            or execution_attempts != record_counts[str(experiment_id)] + failed_attempts
        ):
            raise EvidenceBundleError(
                f"bundled campaign attempt counts differ from controls: {experiment_id}"
            )
        total_execution_attempts += execution_attempts
        total_failed_attempts += failed_attempts

    if (
        campaign_manifest.get("count") != len(verifications)
        or campaign_manifest.get("accepted_raw_records") != sum(record_counts.values())
        or campaign_manifest.get("execution_attempts") != total_execution_attempts
        or campaign_manifest.get("failed_attempt_records") != total_failed_attempts
    ):
        raise EvidenceBundleError("release campaign totals differ from bundled controls")


def _verify_archive_directory_inventory(
    archive_root: str,
    declared: tuple[dict[str, object], ...],
    inventory: Mapping[str, Mapping[str, Any]],
    empty_directories: tuple[str, ...],
    *,
    label: str,
) -> None:
    archive_root = safe_archive_path(archive_root)
    prefix = f"{archive_root}/"
    expected = {prefix + str(item["path"]): item for item in declared}
    observed = {path: value for path, value in inventory.items() if path.startswith(prefix)}
    if set(observed) != set(expected):
        raise EvidenceBundleError(f"{label} file set differs from its exact inventory")
    unexpected_empty = [
        path for path in empty_directories if path == archive_root or path.startswith(prefix)
    ]
    if unexpected_empty:
        raise EvidenceBundleError(f"{label} contains undeclared empty directories")
    for path, nested in expected.items():
        outer = observed[path]
        if (
            outer.get("size_bytes") != nested["size_bytes"]
            or outer.get("sha256") != nested["sha256"]
        ):
            raise EvidenceBundleError(
                f"{label} file differs from its nested size/SHA-256 binding: {path}"
            )


def _validate_archive_demo_manifest(
    archive: zipfile.ZipFile,
    demo_archive_path: str,
    *,
    commit: str,
    report_publishability_path: str,
    report_publishability_sha256: str,
    inventory: Mapping[str, Mapping[str, Any]],
    empty_directories: tuple[str, ...],
) -> tuple[Mapping[str, Any], _DemoManifestBinding]:
    if not demo_archive_path.startswith("repository/"):
        raise EvidenceBundleError("bundled demo manifest must be repository evidence")
    demo = _load_archive_object(archive, demo_archive_path, label="source demo manifest")
    repository_path = demo_archive_path.removeprefix("repository/")
    binding = _demo_manifest_binding(
        demo,
        demo_repository_path=repository_path,
        commit=commit,
        report_publishability_path=report_publishability_path.removeprefix("repository/"),
        report_publishability_sha256=report_publishability_sha256,
    )
    demo_parent = PurePosixPath(demo_archive_path).parent.as_posix()
    expected_staged_roots: set[str] = set()
    source_roots: list[str] = []
    for application in binding.applications:
        raw_path = "repository/" + str(application.video_value["raw_record"])
        raw_entry = inventory.get(raw_path)
        if (
            raw_entry is None
            or raw_entry.get("sha256") != application.video_value["raw_record_sha256"]
        ):
            raise EvidenceBundleError("bundled demo raw record differs from its SHA-256 binding")
        raw_record = _load_archive_object(archive, raw_path, label="demo raw record")
        _validate_demo_raw_measurement(raw_record, application.video_value)
        source_root = "repository/" + application.source_event_log
        staged_root = safe_archive_path(f"{demo_parent}/{application.staged_event_log}")
        _verify_archive_directory_inventory(
            source_root,
            application.event_log_inventory,
            inventory,
            empty_directories,
            label="bundled demo source event log",
        )
        _verify_archive_directory_inventory(
            staged_root,
            application.event_log_inventory,
            inventory,
            empty_directories,
            label="bundled demo staged event log",
        )
        source_roots.append(source_root)
        expected_staged_roots.add(staged_root)
    for index, root in enumerate(source_roots):
        for other in source_roots[index + 1 :]:
            if root == other or root.startswith(f"{other}/") or other.startswith(f"{root}/"):
                raise EvidenceBundleError(
                    "bundled demo source event-log roots must be distinct and non-overlapping"
                )

    event_root = f"{demo_parent}/event-logs"
    event_prefix = f"{event_root}/"
    observed_files = {path for path in inventory if path.startswith(event_prefix)}
    expected_files = {
        f"{root}/{item['path']}"
        for root, application in zip(
            sorted(expected_staged_roots),
            sorted(binding.applications, key=lambda item: item.staged_event_log),
            strict=True,
        )
        for item in application.event_log_inventory
    }
    if observed_files != expected_files or any(
        path == event_root or path.startswith(event_prefix) for path in empty_directories
    ):
        raise EvidenceBundleError("bundled demo event-log root contains unexpected entries")
    return demo, binding


def _verify_archive_presentation_manifest(
    sidecar: Mapping[str, Any],
    *,
    commit: str,
    report: Mapping[str, Any],
    report_inventory: Mapping[str, Any],
    inventory: Mapping[str, Mapping[str, Any]],
) -> None:
    if (
        set(sidecar)
        != {
            "schema_version",
            "status",
            "presentation",
            "report_publishability",
            "report_inventory",
            "git_commit",
            "visual_review",
            "integrity_notice",
        }
        or sidecar.get("schema_version") != 2
    ):
        raise EvidenceBundleError("bundled presentation fields do not match schema version 2")
    if sidecar.get("status") != "publishable" or sidecar.get("git_commit") != commit:
        raise EvidenceBundleError("bundled presentation is not publishable for the release commit")
    presentation = sidecar.get("presentation")
    expected_presentation_fields = {
        "path",
        "size_bytes",
        "sha256",
        "format",
        "slide_count",
        "slide_width_emu",
        "slide_height_emu",
        "native_chart_count",
        "native_table_slide_count",
        "notes_slide_count",
        "diagnostic_marker_slide_count",
    }
    if not isinstance(presentation, Mapping) or set(presentation) != expected_presentation_fields:
        raise EvidenceBundleError("bundled presentation asset binding is invalid")
    asset_path = "repository/" + _canonical_relative_path(
        presentation.get("path"), label="bundled presentation path"
    )
    outer = inventory.get(asset_path)
    if (
        presentation.get("format") != "pptx"
        or outer is None
        or outer.get("size_bytes") != presentation.get("size_bytes")
        or outer.get("sha256") != presentation.get("sha256")
    ):
        raise EvidenceBundleError("bundled presentation asset hash binding is invalid")
    slide_count = _nonnegative_integer(
        presentation.get("slide_count"), label="bundled presentation slide count"
    )
    if (
        slide_count == 0
        or presentation.get("notes_slide_count") != slide_count
        or presentation.get("diagnostic_marker_slide_count") != 0
    ):
        raise EvidenceBundleError("bundled presentation slide evidence is not publishable")
    for field in ("slide_width_emu", "slide_height_emu"):
        if _nonnegative_integer(presentation.get(field), label=field) == 0:
            raise EvidenceBundleError("bundled presentation dimensions are invalid")
    for field in ("native_chart_count", "native_table_slide_count"):
        _nonnegative_integer(presentation.get(field), label=field)

    publication_path = str(report["publishability_path"])
    publication_outer = inventory[publication_path]
    expected_report = {
        "path": publication_path.removeprefix("repository/"),
        "size_bytes": publication_outer["size_bytes"],
        "sha256": report["publishability_sha256"],
        "publishable": True,
    }
    if sidecar.get("report_publishability") != expected_report:
        raise EvidenceBundleError("bundled presentation does not bind the exact report")

    _, artifact_total_bytes, artifact_set_sha256 = _report_inventory_summary(report_inventory)
    report_inventory_path = str(report["inventory_path"])
    report_inventory_outer = inventory[report_inventory_path]
    expected_report_inventory = {
        "path": report_inventory_path.removeprefix("repository/"),
        "size_bytes": report_inventory_outer["size_bytes"],
        "sha256": report["inventory_sha256"],
        "artifact_count": report_inventory["artifact_count"],
        "artifact_total_bytes": artifact_total_bytes,
        "artifact_set_sha256": artifact_set_sha256,
        "artifact_set_canonicalization": _REPORT_INVENTORY_CANONICALIZATION,
    }
    if sidecar.get("report_inventory") != expected_report_inventory:
        raise EvidenceBundleError(
            "bundled presentation does not bind the nested exact report inventory"
        )
    if sidecar.get("visual_review") != {
        "confirmed": True,
        "scope": _PRESENTATION_VISUAL_REVIEW_SCOPE,
    }:
        raise EvidenceBundleError("bundled presentation visual review is not publishable")
    _required_string(sidecar.get("integrity_notice"), label="presentation integrity notice")


def _verify_archive_video_manifest(
    archive: zipfile.ZipFile,
    sidecar: Mapping[str, Any],
    *,
    commit: str,
    report: Mapping[str, Any],
    inventory: Mapping[str, Mapping[str, Any]],
    empty_directories: tuple[str, ...],
) -> None:
    expected_fields = {
        "schema_version",
        "status",
        "video",
        "playback_validation",
        "source_demo_manifest",
        "report_publishability",
        "experiment_id",
        "pair_index",
        "query_id",
        "git_commit",
        "applications",
        "event_log_binding",
        "visual_review",
        "integrity_notice",
    }
    if set(sidecar) != expected_fields or sidecar.get("schema_version") != 2:
        raise EvidenceBundleError("bundled video fields do not match schema version 2")
    if sidecar.get("status") != "publishable" or sidecar.get("git_commit") != commit:
        raise EvidenceBundleError("bundled video is not publishable for the release commit")
    video = sidecar.get("video")
    video_fields = {
        "path",
        "size_bytes",
        "sha256",
        "container",
        "duration_seconds",
        "width",
        "height",
        "codec_name",
        "codec_tag",
        "profile",
        "pixel_format",
        "average_frame_rate",
        "decoded_frame_count",
        "top_level_boxes",
        "container_inspection",
    }
    if not isinstance(video, Mapping) or set(video) != video_fields:
        raise EvidenceBundleError("bundled video asset binding is invalid")
    asset_path = "repository/" + _canonical_relative_path(
        video.get("path"), label="bundled video path"
    )
    outer = inventory.get(asset_path)
    if (
        video.get("container") != "mp4"
        or outer is None
        or outer.get("size_bytes") != video.get("size_bytes")
        or outer.get("sha256") != video.get("sha256")
    ):
        raise EvidenceBundleError("bundled video asset hash binding is invalid")
    _positive_number(video.get("duration_seconds"), label="bundled video duration")
    for field in ("width", "height"):
        if _nonnegative_integer(video.get(field), label=f"bundled video {field}") == 0:
            raise EvidenceBundleError(f"bundled video {field} must be positive")
    if video.get("container_inspection") != {
        "status": "passed",
        "method": "iso_bmff_box_structure",
        "decodability_established": False,
    }:
        raise EvidenceBundleError("bundled video container inspection is invalid")
    boxes = video.get("top_level_boxes")
    if not isinstance(boxes, list) or not {"ftyp", "moov", "mdat"}.issubset(set(boxes)):
        raise EvidenceBundleError("bundled video top-level box inventory is invalid")

    playback = sidecar.get("playback_validation")
    if not isinstance(playback, Mapping):
        raise EvidenceBundleError("bundled video playback validation is missing")
    if playback.get("method") == "ffprobe_complete_frame_scan":
        if (
            set(playback)
            != {
                "status",
                "method",
                "automated_decoder_validation",
                "full_playback_attested",
                "ffprobe_version",
                "ffprobe_sha256",
            }
            or playback.get("status") != "passed"
            or playback.get("automated_decoder_validation") is not True
        ):
            raise EvidenceBundleError("bundled ffprobe validation is not publishable")
        _required_string(playback.get("ffprobe_version"), label="bundled ffprobe version")
        _required_sha256(playback.get("ffprobe_sha256"), label="bundled ffprobe SHA-256")
        if not isinstance(playback.get("full_playback_attested"), bool):
            raise EvidenceBundleError("bundled full-playback flag is invalid")
        for field in ("codec_name", "codec_tag", "pixel_format"):
            _required_string(video.get(field), label=f"bundled decoded video {field}")
        _positive_number(video.get("average_frame_rate"), label="bundled frame rate")
        if _nonnegative_integer(video.get("decoded_frame_count"), label="decoded frames") == 0:
            raise EvidenceBundleError("bundled decoded frame count must be positive")
    elif playback.get("method") == "explicit_full_playback_attestation":
        if (
            set(playback)
            != {
                "status",
                "method",
                "automated_decoder_validation",
                "full_playback_attested",
                "scope",
            }
            or playback.get("status") != "attested"
            or playback.get("automated_decoder_validation") is not False
            or playback.get("full_playback_attested") is not True
        ):
            raise EvidenceBundleError("bundled playback attestation is not publishable")
        _required_string(playback.get("scope"), label="bundled playback scope")
    else:
        raise EvidenceBundleError("bundled video playback validation is not publishable")

    source = sidecar.get("source_demo_manifest")
    if (
        not isinstance(source, Mapping)
        or set(source)
        != {
            "path",
            "size_bytes",
            "sha256",
            "status",
        }
        or source.get("status") != "publishable"
    ):
        raise EvidenceBundleError("bundled video source demo binding is invalid")
    demo_path = "repository/" + _canonical_relative_path(
        source.get("path"), label="bundled source demo path"
    )
    demo_outer = inventory.get(demo_path)
    if (
        demo_outer is None
        or demo_outer.get("size_bytes") != source.get("size_bytes")
        or demo_outer.get("sha256") != source.get("sha256")
    ):
        raise EvidenceBundleError("bundled source demo manifest hash binding is invalid")
    demo, demo_binding = _validate_archive_demo_manifest(
        archive,
        demo_path,
        commit=commit,
        report_publishability_path=str(report["publishability_path"]),
        report_publishability_sha256=str(report["publishability_sha256"]),
        inventory=inventory,
        empty_directories=empty_directories,
    )
    report_path = str(report["publishability_path"])
    report_outer = inventory[report_path]
    expected_video_report = {
        "path": report_path.removeprefix("repository/"),
        "size_bytes": report_outer["size_bytes"],
        "sha256": report["publishability_sha256"],
        "status": "passed",
        "publishable": True,
        "report_contract_passed": True,
    }
    if sidecar.get("report_publishability") != expected_video_report:
        raise EvidenceBundleError("bundled video does not bind the exact release report")
    if sidecar.get("applications") != [
        application.video_value for application in demo_binding.applications
    ]:
        raise EvidenceBundleError("bundled video applications differ from the source demo")
    expected_event_binding = {
        "application_count": 2,
        "binding_sha256": sha256_value(list(demo_binding.event_log_binding)),
        "source_file_count": demo_binding.source_file_count,
        "staged_file_count": demo_binding.staged_file_count,
    }
    if sidecar.get("event_log_binding") != expected_event_binding:
        raise EvidenceBundleError("bundled video event-log aggregate binding is invalid")
    if (
        sidecar.get("experiment_id") != demo.get("experiment_id")
        or sidecar.get("pair_index") != demo.get("pair_index")
        or sidecar.get("query_id") != demo.get("query_id")
    ):
        raise EvidenceBundleError("bundled video identity differs from the source demo")
    visual_review = sidecar.get("visual_review")
    if (
        not isinstance(visual_review, Mapping)
        or set(visual_review) != {"confirmed", "scope"}
        or visual_review.get("confirmed") is not True
    ):
        raise EvidenceBundleError("bundled video visual review is not publishable")
    _required_string(visual_review.get("scope"), label="bundled visual review scope")
    _required_string(sidecar.get("integrity_notice"), label="bundled video integrity notice")


def _verify_cross_bindings(
    archive: zipfile.ZipFile,
    manifest: Mapping[str, Any],
    inventory: Mapping[str, Mapping[str, Any]],
    empty_directories: tuple[str, ...],
) -> None:
    if manifest.get("status") != "publishable":
        raise EvidenceBundleError("release bundle status must be publishable")
    git = manifest.get("git")
    if not isinstance(git, Mapping) or git.get("repository_bundle") not in inventory:
        raise EvidenceBundleError("release bundle has no source repository bundle")
    commit = _required_string(git.get("commit"), label="release Git commit")
    report = manifest.get("report")
    if not isinstance(report, Mapping):
        raise EvidenceBundleError("release bundle has no report binding")
    publication_path = str(report.get("publishability_path", ""))
    publication = _load_archive_object(
        archive, publication_path, label="report publishability artifact"
    )
    if (
        publication.get("schema_version") != 1
        or isinstance(publication.get("schema_version"), bool)
        or publication.get("publishable") is not True
        or publication.get("status") != "passed"
    ):
        raise EvidenceBundleError("bundled report is not publishable")
    contract = publication.get("report_contract")
    if (
        not isinstance(contract, Mapping)
        or contract.get("path") != "report-contract.json"
        or contract.get("status") != "passed"
        or contract.get("passed") is not True
    ):
        raise EvidenceBundleError("bundled report content contract has not passed")
    checks = publication.get("checks")
    provenance = checks.get("repository_provenance") if isinstance(checks, Mapping) else None
    if (
        not isinstance(provenance, Mapping)
        or provenance.get("passed") is not True
        or provenance.get("current_git_commit") != commit
        or provenance.get("raw_git_commits") != [commit]
    ):
        raise EvidenceBundleError("bundled report provenance differs from the release commit")
    campaigns = manifest.get("campaigns")
    policy = publication.get("policy")
    core_experiments = policy.get("core_experiments") if isinstance(policy, Mapping) else None
    expected_ids = campaigns.get("experiment_ids") if isinstance(campaigns, Mapping) else None
    policy_ids = (
        [value.get("experiment_id") for value in core_experiments if isinstance(value, Mapping)]
        if isinstance(core_experiments, list)
        else None
    )
    if (
        not isinstance(expected_ids, list)
        or not isinstance(policy_ids, list)
        or any(not isinstance(value, str) or not value for value in policy_ids)
        or len(policy_ids) != len(expected_ids)
        or len(set(policy_ids)) != len(policy_ids)
        or set(policy_ids) != set(expected_ids)
    ):
        raise EvidenceBundleError("bundled report policy differs from the release experiment set")
    _verify_bundled_campaign_controls(archive, publication, manifest, inventory, empty_directories)
    if inventory.get(publication_path, {}).get("sha256") != report.get("publishability_sha256"):
        raise EvidenceBundleError("bundled report publishability hash binding is invalid")
    inventory_path = str(report.get("inventory_path", ""))
    if inventory.get(inventory_path, {}).get("sha256") != report.get("inventory_sha256"):
        raise EvidenceBundleError("bundled report inventory hash binding is invalid")
    report_inventory = _load_archive_object(
        archive, inventory_path, label="report artifact inventory"
    )
    report_values, _, _ = _report_inventory_summary(report_inventory)
    report_prefix = str(PurePosixPath(inventory_path).parent).rstrip("/") + "/"
    declared_report_paths: set[str] = set()
    for value in report_values:
        if not isinstance(value, Mapping):
            raise EvidenceBundleError("bundled report inventory entry is invalid")
        report_artifact_path = report_prefix + safe_archive_path(str(value.get("path", "")))
        declared_report_paths.add(report_artifact_path)
        outer = inventory.get(report_artifact_path)
        if (
            outer is None
            or outer.get("size_bytes") != value.get("size_bytes")
            or outer.get("sha256") != value.get("sha256")
        ):
            raise EvidenceBundleError(
                f"bundled report artifact differs from its nested inventory: {report_artifact_path}"
            )
    actual_report_paths = {
        name
        for name in inventory
        if name.startswith(report_prefix) and "/" not in name[len(report_prefix) :]
    }
    if actual_report_paths != declared_report_paths | {inventory_path}:
        raise EvidenceBundleError(
            "bundled report directory differs from its nested exact inventory"
        )
    contract_path = report_prefix + "report-contract.json"
    if contract_path not in declared_report_paths:
        raise EvidenceBundleError("bundled report inventory has no report content contract")
    contract_artifact = _load_archive_object(
        archive, contract_path, label="report content contract artifact"
    )
    if (
        contract_artifact.get("schema_version") != 1
        or contract_artifact.get("status") != "passed"
        or contract_artifact.get("passed") is not True
    ):
        raise EvidenceBundleError("bundled report content contract artifact is not passed")

    presentation_declaration = manifest.get("presentation_manifest")
    if not isinstance(presentation_declaration, Mapping):
        raise EvidenceBundleError("release bundle has no presentation manifest binding")
    presentation_manifest_path = str(presentation_declaration.get("path", ""))
    if inventory.get(presentation_manifest_path, {}).get("sha256") != presentation_declaration.get(
        "sha256"
    ):
        raise EvidenceBundleError("bundled presentation manifest hash binding is invalid")
    presentation_sidecar = _load_archive_object(
        archive, presentation_manifest_path, label="presentation manifest"
    )
    _verify_archive_presentation_manifest(
        presentation_sidecar,
        commit=commit,
        report=report,
        report_inventory=report_inventory,
        inventory=inventory,
    )

    video_declaration = manifest.get("video_manifest")
    if not isinstance(video_declaration, Mapping):
        raise EvidenceBundleError("release bundle has no video manifest binding")
    video_manifest_path = str(video_declaration.get("path", ""))
    if inventory.get(video_manifest_path, {}).get("sha256") != video_declaration.get("sha256"):
        raise EvidenceBundleError("bundled video manifest hash binding is invalid")
    video_sidecar = _load_archive_object(archive, video_manifest_path, label="video manifest")
    _verify_archive_video_manifest(
        archive,
        video_sidecar,
        commit=commit,
        report=report,
        inventory=inventory,
        empty_directories=empty_directories,
    )

    datasets = manifest.get("datasets")
    if not isinstance(datasets, list):
        raise EvidenceBundleError("bundle datasets must be an array")
    for dataset in datasets:
        if not isinstance(dataset, Mapping):
            raise EvidenceBundleError("bundle dataset entry must be an object")
        root = str(dataset.get("root", "")).rstrip("/") + "/"
        manifest_path = str(dataset.get("manifest_path", ""))
        if manifest_path not in inventory or not any(path.startswith(root) for path in inventory):
            raise EvidenceBundleError(
                f"bundled dataset closure is incomplete: {dataset.get('dataset_id')}"
            )
        if inventory[manifest_path].get("sha256") != dataset.get("manifest_sha256"):
            raise EvidenceBundleError(f"bundled dataset manifest hash is invalid: {manifest_path}")
        attestation_paths = dataset.get("attestation_paths")
        if not isinstance(attestation_paths, list) or not attestation_paths:
            raise EvidenceBundleError("bundled dataset has no validation attestation")
        attested_parquet: set[str] = set()
        attested_sources: set[str] = set()
        for attestation_path in attestation_paths:
            if not isinstance(attestation_path, str) or attestation_path not in inventory:
                raise EvidenceBundleError("bundled dataset attestation path is invalid")
            attestation = _load_archive_object(
                archive, attestation_path, label="dataset validation attestation"
            )
            identity = attestation.get("dataset")
            if (
                not isinstance(identity, Mapping)
                or identity.get("dataset_id") != dataset.get("dataset_id")
                or "repository/" + str(identity.get("manifest_path", "")) != manifest_path
                or identity.get("manifest_sha256") != dataset.get("manifest_sha256")
            ):
                raise EvidenceBundleError(
                    f"bundled dataset attestation identity is invalid: {attestation_path}"
                )
            for field, required in (
                ("runtime_parquet_inventory", True),
                ("runtime_source_inventory", False),
            ):
                values = attestation.get(field)
                if values is None and not required:
                    continue
                if not isinstance(values, list) or (required and not values):
                    raise EvidenceBundleError(
                        f"bundled dataset attestation {field} is invalid: {attestation_path}"
                    )
                for value in values:
                    if not isinstance(value, Mapping):
                        raise EvidenceBundleError(
                            f"bundled dataset attestation {field} entry is invalid"
                        )
                    relative = safe_archive_path(str(value.get("path", "")))
                    data_path = root + relative
                    outer = inventory.get(data_path)
                    if (
                        outer is None
                        or outer.get("size_bytes") != value.get("size_bytes")
                        or outer.get("sha256") != value.get("sha256")
                    ):
                        raise EvidenceBundleError(
                            f"bundled dataset file differs from attestation: {data_path}"
                        )
                    if field == "runtime_parquet_inventory":
                        attested_parquet.add(data_path)
                    else:
                        attested_sources.add(data_path)
        bundled_parquet = {
            path
            for path in inventory
            if path.startswith(root) and path.lower().endswith(".parquet")
        }
        if bundled_parquet != attested_parquet:
            raise EvidenceBundleError(
                "bundled Parquet set differs from dataset attestations: "
                f"{dataset.get('dataset_id')}"
            )
        if attested_sources:
            bundled_sources = {
                path
                for path in inventory
                if path.startswith(root) and path.lower().endswith(".tbl")
            }
            if bundled_sources != attested_sources:
                raise EvidenceBundleError(
                    "bundled source-table set differs from dataset attestations: "
                    f"{dataset.get('dataset_id')}"
                )


def verify_evidence_bundle(
    archive_path: Path,
    *,
    require_outer_checksum: bool = True,
) -> dict[str, Any]:
    """Verify archive safety, exact inventory, hashes, and release cross-bindings."""

    archive_path = archive_path.resolve()
    if not archive_path.is_file() or archive_path.is_symlink():
        raise EvidenceBundleError(f"evidence bundle must be a regular file: {archive_path}")
    if require_outer_checksum:
        checksum_path = archive_path.with_suffix(archive_path.suffix + ".sha256")
        if not checksum_path.is_file() or checksum_path.is_symlink():
            raise EvidenceBundleError(f"outer checksum sidecar is missing: {checksum_path}")
        line = checksum_path.read_text(encoding="utf-8").strip()
        expected_line = f"{_sha256_file(archive_path)}  {archive_path.name}"
        if line != expected_line:
            raise EvidenceBundleError("outer evidence bundle SHA-256 is invalid")
    try:
        archive = zipfile.ZipFile(archive_path)
    except (OSError, zipfile.BadZipFile) as error:
        raise EvidenceBundleError(f"cannot open evidence bundle: {error}") from error
    with archive:
        names = _zip_member_names(archive)
        manifest = _load_archive_object(archive, MANIFEST_NAME, label="release manifest")
        _validate_manifest(manifest)
        declared_manifest_hash = manifest.get("manifest_sha256")
        manifest_for_hash = dict(manifest)
        manifest_for_hash.pop("manifest_sha256", None)
        if declared_manifest_hash != sha256_value(manifest_for_hash):
            raise EvidenceBundleError("release manifest self-hash is invalid")
        inventory = _inventory_by_path(manifest)
        empty_directories = _empty_directory_paths(manifest, inventory)
        expected_names = set(inventory) | {MANIFEST_NAME, CHECKSUMS_NAME}
        if set(names) != expected_names:
            raise EvidenceBundleError(
                "archive entries differ from exact inventory: "
                f"missing={sorted(expected_names - set(names))}, "
                f"unexpected={sorted(set(names) - expected_names)}"
            )
        info_by_name = {info.filename: info for info in archive.infolist()}
        for name, declaration in inventory.items():
            info = info_by_name[name]
            if declaration.get("size_bytes") != info.file_size:
                raise EvidenceBundleError(f"bundle entry size is invalid: {name}")
            digest = hashlib.sha256()
            size = 0
            with archive.open(info) as handle:
                while chunk := handle.read(1024 * 1024):
                    digest.update(chunk)
                    size += len(chunk)
            if size != info.file_size or declaration.get("sha256") != digest.hexdigest():
                raise EvidenceBundleError(f"bundle entry SHA-256 is invalid: {name}")
        if manifest.get("total_files") != len(inventory) or manifest.get("total_bytes") != sum(
            int(value["size_bytes"]) for value in inventory.values()
        ):
            raise EvidenceBundleError("release manifest inventory totals are invalid")
        expected_sums = "".join(
            f"{value['sha256']}  {name}\n" for name, value in inventory.items()
        ).encode("utf-8")
        if archive.read(CHECKSUMS_NAME) != expected_sums:
            raise EvidenceBundleError("bundled SHA256SUMS differs from the release inventory")
        _verify_cross_bindings(archive, manifest, inventory, empty_directories)
        return manifest


def extract_evidence_bundle(archive_path: Path, destination: Path) -> Path:
    """Verify and extract to a new directory without path traversal or overwrite."""

    manifest = verify_evidence_bundle(archive_path)
    destination = destination.resolve()
    if destination.exists():
        raise EvidenceBundleError(f"refusing to merge into existing extraction path: {destination}")
    destination.mkdir(parents=True)
    try:
        with zipfile.ZipFile(archive_path) as archive:
            for info in archive.infolist():
                relative = PurePosixPath(safe_archive_path(info.filename))
                target = destination.joinpath(*relative.parts)
                target.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(info) as source, target.open("wb") as output:
                    shutil.copyfileobj(source, output, length=1024 * 1024)
        inventory = _inventory_by_path(manifest)
        for name in _empty_directory_paths(manifest, inventory):
            relative = PurePosixPath(name)
            destination.joinpath(*relative.parts).mkdir(parents=True, exist_ok=False)
        return destination
    except Exception:
        shutil.rmtree(destination)
        raise
