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
