from __future__ import annotations

import os
from pathlib import Path

import pytest

from scripts.bootstrap_env import ensure_environment


def test_environment_bootstrap_is_idempotent_and_never_emits_placeholders(
    tmp_path: Path,
) -> None:
    target = tmp_path / ".env"
    assert ensure_environment(target) == "created"
    original = target.read_text(encoding="utf-8")
    assert ensure_environment(target) == "kept"
    assert target.read_text(encoding="utf-8") == original
    assert "replace-with-" not in original
    assert "ICEBERG_JDBC_PASSWORD=" in original
    if os.name != "nt":
        assert target.stat().st_mode & 0o777 == 0o600


def test_environment_bootstrap_rejects_placeholders_and_duplicates(tmp_path: Path) -> None:
    placeholder = tmp_path / "placeholder.env"
    placeholder.write_text("MINIO_ROOT_USER=replace-with-generated-access-key\n", encoding="utf-8")
    with pytest.raises(ValueError, match="placeholder"):
        ensure_environment(placeholder)

    duplicate = tmp_path / "duplicate.env"
    duplicate.write_text("AWS_REGION=one\nAWS_REGION=two\n", encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate"):
        ensure_environment(duplicate)
