"""Deterministic integrity fingerprints for raw campaigns and their external artifacts."""

from __future__ import annotations

import os
import re
import subprocess
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from benchmark.runner.canonical import sha256_file, sha256_value

ARTIFACT_FIELDS = (
    "event_log",
    "physical_plan",
    "resource_samples",
    "stdout",
    "stderr",
)
_CONTROL_LABEL = re.compile(r"^[a-z0-9][a-z0-9_.-]*$")


class ArtifactEvidenceError(ValueError):
    """A raw record does not resolve to a complete, repository-contained artifact set."""


class RepositoryEvidenceError(ValueError):
    """The current repository cannot provide clean, immutable Git provenance."""


def isolated_git_environment() -> dict[str, str]:
    """Return an environment that cannot redirect or rewrite repository object lookups."""

    environment = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    environment["GIT_NO_REPLACE_OBJECTS"] = "1"
    environment["GIT_TERMINAL_PROMPT"] = "0"
    return environment


def validate_git_object_graph(repository_root: Path) -> None:
    """Reject local Git mechanisms that can silently replace commits or ancestry."""

    root = repository_root.resolve()
    environment = isolated_git_environment()
    try:
        top_level_result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=root,
            env=environment,
            check=True,
            capture_output=True,
            text=True,
        )
        replace_result = subprocess.run(
            ["git", "for-each-ref", "--format=%(refname)", "refs/replace"],
            cwd=root,
            env=environment,
            check=True,
            capture_output=True,
            text=True,
        )
        graft_result = subprocess.run(
            ["git", "rev-parse", "--git-path", "info/grafts"],
            cwd=root,
            env=environment,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        raise RepositoryEvidenceError(
            f"Git object-graph provenance is unavailable: {error}"
        ) from error

    try:
        top_level = Path(top_level_result.stdout.strip()).resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise RepositoryEvidenceError("Git returned an invalid repository top level") from error
    if top_level != root:
        raise RepositoryEvidenceError(
            f"Git top level differs from the requested repository: {top_level}"
        )
    if replace_result.stdout.strip():
        raise RepositoryEvidenceError("Git replace refs are not allowed for evidence provenance")

    graft_value = graft_result.stdout.strip()
    if not graft_value:
        raise RepositoryEvidenceError("Git returned no graft-file location")
    graft_path = Path(graft_value)
    if not graft_path.is_absolute():
        graft_path = root / graft_path
    if graft_path.is_symlink() or (graft_path.is_file() and graft_path.stat().st_size > 0):
        raise RepositoryEvidenceError("Git grafts are not allowed for evidence provenance")


def clean_git_commit(repository_root: Path) -> str:
    """Return the full HEAD object ID, failing closed for any worktree change."""

    root = repository_root.resolve()
    validate_git_object_graph(root)
    environment = isolated_git_environment()
    try:
        commit_result = subprocess.run(
            ["git", "rev-parse", "--verify", "HEAD^{commit}"],
            cwd=root,
            env=environment,
            check=True,
            capture_output=True,
            text=True,
        )
        status_result = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=normal"],
            cwd=root,
            env=environment,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        raise RepositoryEvidenceError(f"Git provenance is unavailable: {error}") from error
    validate_git_object_graph(root)
    commit = commit_result.stdout.strip()
    if re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        raise RepositoryEvidenceError(f"unexpected Git HEAD object ID: {commit!r}")
    if status_result.stdout.strip():
        raise RepositoryEvidenceError("repository worktree is not clean")
    return commit


def ordered_raw_records(records: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Return shallow record copies in stable run-ID order."""

    return sorted((dict(record) for record in records), key=lambda row: str(row.get("run_id")))


def raw_records_sha256(records: Iterable[Mapping[str, Any]]) -> str:
    """Hash a campaign independently of raw JSON file layout."""

    return sha256_value(ordered_raw_records(records))


def artifact_evidence(
    records: Iterable[Mapping[str, Any]],
    repository_root: Path,
    *,
    require_succeeded: bool = True,
) -> dict[str, int | str]:
    """Hash every physical file referenced by run records.

    Successful campaign evidence requires all five artifacts.  Failed-attempt evidence may omit
    artifacts that were never created, but every non-null declaration is still resolved and
    hashed with the same repository-containment rules.
    """

    root = repository_root.resolve()
    inventory: list[dict[str, int | str]] = []
    file_fingerprints: dict[Path, tuple[int, str]] = {}
    for record in ordered_raw_records(records):
        run_id = record.get("run_id")
        if not isinstance(run_id, str) or not run_id:
            raise ArtifactEvidenceError("artifact evidence requires a non-empty run_id")
        if require_succeeded and record.get("status") != "succeeded":
            raise ArtifactEvidenceError(f"artifact evidence requires succeeded run: {run_id}")
        artifacts = record.get("artifacts")
        if not isinstance(artifacts, Mapping):
            raise ArtifactEvidenceError(f"raw record has no artifact object: {run_id}")
        for field in ARTIFACT_FIELDS:
            declared = artifacts.get(field)
            if not require_succeeded and declared is None:
                continue
            if not isinstance(declared, str) or not declared:
                raise ArtifactEvidenceError(f"{run_id} artifact {field} is absent")
            relative = Path(declared)
            if relative.is_absolute() or ".." in relative.parts:
                raise ArtifactEvidenceError(f"{run_id} artifact {field} leaves repository")
            supplied = root / relative
            if supplied.is_symlink():
                raise ArtifactEvidenceError(f"{run_id} artifact {field} is a symlink")
            candidate = supplied.resolve()
            try:
                candidate.relative_to(root)
            except ValueError as error:
                raise ArtifactEvidenceError(
                    f"{run_id} artifact {field} leaves repository"
                ) from error
            files: tuple[Path, ...]
            if candidate.is_file():
                files = (candidate,)
            elif candidate.is_dir():
                descendants = tuple(sorted(candidate.rglob("*")))
                symlinks = [path for path in descendants if path.is_symlink()]
                if symlinks:
                    raise ArtifactEvidenceError(
                        f"{run_id} artifact {field} tree contains a symlink: {symlinks[0]}"
                    )
                files = tuple(path for path in descendants if path.is_file())
                if not files:
                    raise ArtifactEvidenceError(f"{run_id} artifact {field} directory is empty")
            else:
                raise ArtifactEvidenceError(f"{run_id} artifact {field} does not exist")
            for path in files:
                try:
                    resolved_file = path.resolve()
                    resolved_file.relative_to(root)
                except ValueError as error:
                    raise ArtifactEvidenceError(
                        f"{run_id} artifact {field} file leaves repository: {path}"
                    ) from error
                path = resolved_file
                fingerprint = file_fingerprints.get(path)
                if fingerprint is None:
                    fingerprint = (path.stat().st_size, sha256_file(path))
                    file_fingerprints[path] = fingerprint
                inventory.append(
                    {
                        "run_id": run_id,
                        "artifact": field,
                        "declared_path": relative.as_posix(),
                        "file": path.relative_to(root).as_posix(),
                        "size_bytes": fingerprint[0],
                        "sha256": fingerprint[1],
                    }
                )
    return {
        "file_count": len(inventory),
        "sha256": sha256_value(inventory),
    }


def control_artifact_evidence(
    targets: Mapping[str, Path], repository_root: Path
) -> dict[str, object]:
    """Fingerprint exact control files/directories, including empty directories.

    The returned target declarations are intentionally part of the digest.  A verifier can
    reconstruct the same mapping from the declarations and detect a deleted target, a changed
    file, or a changed directory inventory.  Symlinks are rejected so evidence cannot silently
    retarget after publication.
    """

    root = repository_root.resolve()
    declarations: list[dict[str, str]] = []
    inventory: list[dict[str, int | str]] = []
    for label, supplied in sorted(targets.items()):
        if _CONTROL_LABEL.fullmatch(label) is None:
            raise ArtifactEvidenceError(f"invalid control-artifact label: {label!r}")
        candidate = supplied if supplied.is_absolute() else root / supplied
        if candidate.is_symlink():
            raise ArtifactEvidenceError(f"control artifact is a symlink: {candidate}")
        resolved = candidate.resolve()
        try:
            relative = resolved.relative_to(root)
        except ValueError as error:
            raise ArtifactEvidenceError(
                f"control artifact leaves repository: {candidate}"
            ) from error
        files: tuple[Path, ...]
        descendants: tuple[Path, ...] = ()
        if resolved.is_file():
            kind = "file"
            files = (resolved,)
        elif resolved.is_dir():
            kind = "directory"
            descendants = tuple(sorted(resolved.rglob("*")))
            symlinks = [path for path in descendants if path.is_symlink()]
            if symlinks:
                raise ArtifactEvidenceError(
                    f"control artifact tree contains a symlink: {symlinks[0]}"
                )
            files = tuple(path for path in descendants if path.is_file())
        else:
            raise ArtifactEvidenceError(f"control artifact does not exist: {candidate}")
        declared_path = relative.as_posix()
        declarations.append({"label": label, "path": declared_path, "kind": kind})
        if kind == "directory":
            for path in descendants:
                if path.is_dir():
                    inventory.append(
                        {
                            "label": label,
                            "declared_path": declared_path,
                            "path": path.relative_to(root).as_posix(),
                            "kind": "directory",
                        }
                    )
        for path in files:
            inventory.append(
                {
                    "label": label,
                    "declared_path": declared_path,
                    "path": path.relative_to(root).as_posix(),
                    "kind": "file",
                    "size_bytes": path.stat().st_size,
                    "sha256": sha256_file(path),
                }
            )
    payload = {"targets": declarations, "entries": inventory}
    return {
        "targets": declarations,
        "file_count": sum(entry["kind"] == "file" for entry in inventory),
        "sha256": sha256_value(payload),
    }
