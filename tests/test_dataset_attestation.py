from __future__ import annotations

import hashlib
import json
import platform
import shutil
from dataclasses import FrozenInstanceError
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from benchmark.runner import dataset_attestation as attestation_module
from benchmark.runner.dataset_attestation import (
    DatasetAttestationError,
    create_attestation,
    verify_attestation,
)
from benchmark.runner.evidence import RepositoryEvidenceError
from data.generator.generate import generate_dataset
from data.generator.profiles import load_profile
from data.generator.schemas import ORDERS_SCHEMA

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_PROFILE = PROJECT_ROOT / "data/generator/configs/fixture.yaml"
GIT_COMMIT = "a" * 40
PYTHON_VERSION = platform.python_version()


@pytest.fixture(autouse=True)
def _clean_repository_provenance(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(attestation_module, "clean_git_commit", lambda _root: GIT_COMMIT)


def _repository(tmp_path: Path) -> tuple[Path, Path]:
    root = tmp_path / "repository"
    root.mkdir()
    shutil.copy2(PROJECT_ROOT / "runtime-versions.lock", root / "runtime-versions.lock")
    profile = load_profile(FIXTURE_PROFILE)
    result = generate_dataset(
        profile,
        root / "data/generated/fixture",
        generator_git_commit=GIT_COMMIT,
        generator_worktree_dirty=False,
    )
    return root, result.manifest_path


def _create(root: Path, manifest: Path) -> tuple[Path, object]:
    output = root / ".artifacts/dataset-validation/fixture.json"
    verified = create_attestation(root, manifest, output, PYTHON_VERSION, GIT_COMMIT)
    return output, verified


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_create_and_verify_attestation_is_content_bound_and_frozen(tmp_path: Path) -> None:
    root, manifest = _repository(tmp_path)
    output, created = _create(root, manifest)
    verified = verify_attestation(root, manifest, output, PYTHON_VERSION, GIT_COMMIT)

    assert verified == created
    assert verified.suite == "ecommerce"
    assert verified.manifest_path == manifest.resolve()
    dataset = json.loads(manifest.read_text(encoding="utf-8"))
    assert verified.file_count == sum(len(table["files"]) for table in dataset["tables"].values())
    assert verified.total_bytes > 0
    assert len(verified.content_identity_sha256) == 64
    assert verified.attestation_file_sha256 == _sha256_file(output)
    with pytest.raises(FrozenInstanceError):
        verified.dataset_id = "changed"  # type: ignore[misc]


def test_verify_rejects_same_size_parquet_tampering(tmp_path: Path) -> None:
    root, manifest = _repository(tmp_path)
    output, _ = _create(root, manifest)
    dataset = json.loads(manifest.read_text(encoding="utf-8"))
    parquet = manifest.parent / dataset["tables"]["customers"]["files"][0]["path"]
    payload = bytearray(parquet.read_bytes())
    payload[len(payload) // 2] ^= 1
    parquet.write_bytes(payload)

    with pytest.raises(DatasetAttestationError, match="SHA-256 mismatch"):
        verify_attestation(root, manifest, output, PYTHON_VERSION, GIT_COMMIT)


def test_verify_rejects_unlisted_runtime_parquet(tmp_path: Path) -> None:
    root, manifest = _repository(tmp_path)
    output, _ = _create(root, manifest)
    source = next((manifest.parent / "customers").glob("*.parquet"))
    shutil.copy2(source, manifest.parent / "customers/part-99999.parquet")

    with pytest.raises(DatasetAttestationError, match="inventory mismatch"):
        verify_attestation(root, manifest, output, PYTHON_VERSION, GIT_COMMIT)


def test_verify_rejects_manifest_and_attestation_provenance_drift(tmp_path: Path) -> None:
    root, manifest = _repository(tmp_path)
    output, _ = _create(root, manifest)

    with pytest.raises(DatasetAttestationError, match="Git commit is stale"):
        verify_attestation(root, manifest, output, PYTHON_VERSION, "b" * 40)

    manifest.write_bytes(manifest.read_bytes() + b"\n")
    with pytest.raises(DatasetAttestationError, match=r"dataset\.manifest_sha256"):
        verify_attestation(root, manifest, output, PYTHON_VERSION, GIT_COMMIT)


def test_verify_rejects_tampered_attestation_self_hash(tmp_path: Path) -> None:
    root, manifest = _repository(tmp_path)
    output, _ = _create(root, manifest)
    value = json.loads(output.read_text(encoding="utf-8"))
    value["validator"]["result_sha256"] = "0" * 64
    output.write_text(json.dumps(value), encoding="utf-8")

    with pytest.raises(DatasetAttestationError, match="self-hash"):
        verify_attestation(root, manifest, output, PYTHON_VERSION, GIT_COMMIT)


def test_create_runs_full_semantic_validation_before_attesting(tmp_path: Path) -> None:
    root, manifest = _repository(tmp_path)
    value = json.loads(manifest.read_text(encoding="utf-8"))
    record = value["tables"]["orders"]["files"][0]
    parquet = manifest.parent / record["path"]
    rows = pq.read_table(parquet).to_pylist()
    rows[0]["customer_id"] = 9_999_999
    pq.write_table(
        pa.Table.from_pylist(rows, schema=ORDERS_SCHEMA),
        parquet,
        compression="snappy",
    )
    record["sha256"] = _sha256_file(parquet)
    record["size_bytes"] = parquet.stat().st_size
    value["tables"]["orders"]["total_bytes"] = sum(
        item["size_bytes"] for item in value["tables"]["orders"]["files"]
    )
    manifest.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8")

    with pytest.raises(DatasetAttestationError, match="orphan customer_id=9999999"):
        create_attestation(
            root,
            manifest,
            root / ".artifacts/dataset-validation/rejected.json",
            PYTHON_VERSION,
            GIT_COMMIT,
        )


def test_validation_result_hash_is_independent_of_repository_location(tmp_path: Path) -> None:
    first_root, first_manifest = _repository(tmp_path)
    second_root = tmp_path / "relocated-repository"
    shutil.copytree(first_root, second_root)
    second_manifest = second_root / first_manifest.relative_to(first_root)
    first_output = first_root / ".artifacts/dataset-validation/fixture.json"
    second_output = second_root / ".artifacts/dataset-validation/fixture.json"

    create_attestation(first_root, first_manifest, first_output, PYTHON_VERSION, GIT_COMMIT)
    create_attestation(second_root, second_manifest, second_output, PYTHON_VERSION, GIT_COMMIT)

    first = json.loads(first_output.read_text(encoding="utf-8"))
    second = json.loads(second_output.read_text(encoding="utf-8"))
    assert first["validator"]["result_sha256"] == second["validator"]["result_sha256"]


def test_create_rejects_wrong_clean_head_before_full_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, manifest = _repository(tmp_path)
    output = root / ".artifacts/dataset-validation/rejected.json"
    monkeypatch.setattr(attestation_module, "clean_git_commit", lambda _root: "b" * 40)

    with pytest.raises(DatasetAttestationError, match="before full dataset validation"):
        create_attestation(root, manifest, output, PYTHON_VERSION, GIT_COMMIT)

    assert not output.exists()


def test_create_rechecks_clean_head_immediately_before_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, manifest = _repository(tmp_path)
    output = root / ".artifacts/dataset-validation/rejected.json"
    checks = 0

    def provenance(_root: Path) -> str:
        nonlocal checks
        checks += 1
        if checks == 1:
            return GIT_COMMIT
        raise RepositoryEvidenceError("repository worktree is not clean")

    monkeypatch.setattr(attestation_module, "clean_git_commit", provenance)

    with pytest.raises(DatasetAttestationError, match="immediately before publication"):
        create_attestation(root, manifest, output, PYTHON_VERSION, GIT_COMMIT)

    assert checks == 2
    assert not output.exists()
