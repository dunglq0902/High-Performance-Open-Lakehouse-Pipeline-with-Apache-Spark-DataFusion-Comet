import errno
import os
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from benchmark.runner.canonical import canonical_json_bytes, sha256_value, write_json


def test_canonical_json_sorts_keys_and_normalizes_domain_values() -> None:
    value = {
        "z": Decimal("1.2300"),
        "a": datetime(2026, 8, 24, 1, 2, 3, tzinfo=UTC),
    }
    assert canonical_json_bytes(value) == b'{"a":"2026-08-24T01:02:03.000000Z","z":"1.2300"}'
    assert sha256_value(value) == sha256_value({"a": value["a"], "z": value["z"]})


def test_non_finite_float_requires_explicit_policy() -> None:
    with pytest.raises(ValueError, match="explicit correctness policy"):
        canonical_json_bytes({"value": float("nan")})


def test_immutable_json_is_idempotent_but_not_overwritable(tmp_path) -> None:
    target = tmp_path / "artifact.json"
    write_json(target, {"version": 1})
    write_json(target, {"version": 1})
    with pytest.raises(FileExistsError):
        write_json(target, {"version": 2})


def test_immutable_json_falls_back_when_filesystem_rejects_hard_links(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def reject_hard_link(*_args: object, **_kwargs: object) -> None:
        raise OSError(errno.EXDEV, "cross-device link")

    monkeypatch.setattr(os, "link", reject_hard_link)
    target = tmp_path / "artifact.json"
    write_json(target, {"version": 1})
    write_json(target, {"version": 1})
    assert target.read_bytes() == b'{"version":1}\n'
    with pytest.raises(FileExistsError, match="refusing to overwrite"):
        write_json(target, {"version": 2})
