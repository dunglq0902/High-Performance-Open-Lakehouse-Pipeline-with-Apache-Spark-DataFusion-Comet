"""Deterministic integrity fingerprints for raw campaigns and their external artifacts."""

from __future__ import annotations

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


class ArtifactEvidenceError(ValueError):
    """A raw record does not resolve to a complete, repository-contained artifact set."""


class RepositoryEvidenceError(ValueError):
    """The current repository cannot provide clean, immutable Git provenance."""


def clean_git_commit(repository_root: Path) -> str:
    """Return the full HEAD object ID, failing closed for any worktree change."""

    root = repository_root.resolve()
    try:
        commit_result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
        status_result = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=normal"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        raise RepositoryEvidenceError(f"Git provenance is unavailable: {error}") from error
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
    records: Iterable[Mapping[str, Any]], repository_root: Path
) -> dict[str, int | str]:
    """Hash every physical file referenced by a succeeded raw campaign."""

    root = repository_root.resolve()
    inventory: list[dict[str, int | str]] = []
    file_fingerprints: dict[Path, tuple[int, str]] = {}
    for record in ordered_raw_records(records):
        run_id = record.get("run_id")
        if not isinstance(run_id, str) or not run_id:
            raise ArtifactEvidenceError("artifact evidence requires a non-empty run_id")
        if record.get("status") != "succeeded":
            raise ArtifactEvidenceError(f"artifact evidence requires succeeded run: {run_id}")
        artifacts = record.get("artifacts")
        if not isinstance(artifacts, Mapping):
            raise ArtifactEvidenceError(f"raw record has no artifact object: {run_id}")
        for field in ARTIFACT_FIELDS:
            declared = artifacts.get(field)
            if not isinstance(declared, str) or not declared:
                raise ArtifactEvidenceError(f"{run_id} artifact {field} is absent")
            relative = Path(declared)
            if relative.is_absolute() or ".." in relative.parts:
                raise ArtifactEvidenceError(f"{run_id} artifact {field} leaves repository")
            candidate = (root / relative).resolve()
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
                files = tuple(sorted(path for path in candidate.rglob("*") if path.is_file()))
                if not files:
                    raise ArtifactEvidenceError(f"{run_id} artifact {field} directory is empty")
            else:
                raise ArtifactEvidenceError(f"{run_id} artifact {field} does not exist")
            for path in files:
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
