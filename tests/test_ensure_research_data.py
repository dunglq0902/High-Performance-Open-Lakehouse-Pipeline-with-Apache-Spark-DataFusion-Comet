from pathlib import Path

import pytest

from scripts import ensure_research_data


def test_missing_research_data_requires_explicit_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ensure_research_data, "DATASET_PATH", tmp_path / "missing")
    with pytest.raises(RuntimeError, match="rerun with --generate"):
        ensure_research_data.ensure_research_data(generate=False)


def test_primary_generation_rejects_dirty_worktree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ensure_research_data, "DATASET_PATH", tmp_path / "missing")
    monkeypatch.setattr(ensure_research_data, "_worktree_is_dirty", lambda _root: True)
    with pytest.raises(RuntimeError, match="dirty worktree"):
        ensure_research_data.ensure_research_data(generate=True)


def test_primary_generation_requires_the_locked_python_patch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ensure_research_data, "DATASET_PATH", tmp_path / "missing")
    monkeypatch.setattr(ensure_research_data, "_worktree_is_dirty", lambda _root: False)
    monkeypatch.setattr(ensure_research_data, "_locked_python_version", lambda: "3.12.13")
    monkeypatch.setattr(ensure_research_data, "_active_python_version", lambda: "3.12.11")

    with pytest.raises(RuntimeError, match="unlocked Python"):
        ensure_research_data.ensure_research_data(generate=True)


def test_runtime_lock_and_python_version_file_agree() -> None:
    assert ensure_research_data._locked_python_version() == "3.12.13"
    assert (ensure_research_data.ROOT / ".python-version").read_text(encoding="utf-8").strip() == (
        "3.12.13"
    )
