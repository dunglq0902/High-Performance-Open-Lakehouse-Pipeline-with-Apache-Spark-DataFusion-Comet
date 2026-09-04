from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from benchmark.runner.evidence import (
    RepositoryEvidenceError,
    clean_git_commit,
    isolated_git_environment,
)


def _git(repository: Path, *arguments: str) -> str:
    result = subprocess.run(
        ["git", *arguments],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _repository(tmp_path: Path) -> Path:
    repository = tmp_path / "repository"
    repository.mkdir()
    _git(repository, "init")
    _git(repository, "config", "user.email", "tests@example.invalid")
    _git(repository, "config", "user.name", "Evidence Tests")
    tracked = repository / "tracked.txt"
    tracked.write_text("first\n", encoding="utf-8")
    _git(repository, "add", "tracked.txt")
    _git(repository, "commit", "-m", "first")
    tracked.write_text("second\n", encoding="utf-8")
    _git(repository, "add", "tracked.txt")
    _git(repository, "commit", "-m", "second")
    return repository


def test_isolated_git_environment_scrubs_repository_redirects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GIT_DIR", "redirected")
    monkeypatch.setenv("GIT_WORK_TREE", "redirected")
    monkeypatch.setenv("GIT_OBJECT_DIRECTORY", "redirected")

    environment = isolated_git_environment()

    assert "GIT_DIR" not in environment
    assert "GIT_WORK_TREE" not in environment
    assert "GIT_OBJECT_DIRECTORY" not in environment
    assert environment["GIT_NO_REPLACE_OBJECTS"] == "1"


def test_clean_git_commit_rejects_replace_refs(tmp_path: Path) -> None:
    repository = _repository(tmp_path)
    commits = _git(repository, "rev-list", "--max-count=2", "HEAD").splitlines()
    _git(repository, "replace", commits[0], commits[1])

    with pytest.raises(RepositoryEvidenceError, match="replace refs"):
        clean_git_commit(repository)


def test_clean_git_commit_rejects_grafts(tmp_path: Path) -> None:
    repository = _repository(tmp_path)
    head = _git(repository, "rev-parse", "HEAD")
    parent = _git(repository, "rev-parse", "HEAD^")
    graft_value = _git(repository, "rev-parse", "--git-path", "info/grafts")
    graft = Path(graft_value)
    if not graft.is_absolute():
        graft = repository / graft
    graft.parent.mkdir(parents=True, exist_ok=True)
    graft.write_text(f"{head} {parent}\n", encoding="utf-8")

    with pytest.raises(RepositoryEvidenceError, match="grafts"):
        clean_git_commit(repository)
