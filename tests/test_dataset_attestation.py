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
from benchmark.runner.canonical import sha256_value
from benchmark.runner.dataset_attestation import (
    DatasetAttestationError,
    DatasetAttestationNotReusable,
    create_attestation,
    discover_full_attestation_origins,
    rebind_attestation,
    verify_attestation,
)
from benchmark.runner.evidence import RepositoryEvidenceError
from data.generator.generate import generate_dataset
from data.generator.profiles import load_profile
from data.generator.schemas import ORDERS_SCHEMA
from data.tpch.contract import TABLE_ORDER as TPCH_TABLE_ORDER

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_PROFILE = PROJECT_ROOT / "data/generator/configs/fixture.yaml"
GIT_COMMIT = "a" * 40
CURRENT_COMMIT = "b" * 40
SEMANTIC_TREE = "c" * 40
PYTHON_VERSION = platform.python_version()


@pytest.fixture(autouse=True)
def _clean_repository_provenance(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(attestation_module, "clean_git_commit", lambda _root: GIT_COMMIT)
    monkeypatch.setattr(
        attestation_module,
        "_git_tree_oid",
        lambda _root, _commit, _path: SEMANTIC_TREE,
    )
    monkeypatch.setattr(attestation_module, "_is_ancestor", lambda *_args: True)


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


def _canonical_attestation(root: Path, manifest: Path, commit: str) -> Path:
    return root / ".artifacts/dataset-validations" / commit / f"{_sha256_file(manifest)}.json"


def _write_v1_origin(root: Path, manifest: Path) -> Path:
    generated = root / ".artifacts/full-v2.json"
    create_attestation(root, manifest, generated, PYTHON_VERSION, GIT_COMMIT)
    value = json.loads(generated.read_text(encoding="utf-8"))
    value["artifact_class"] = "dataset-validation-attestation-v1"
    value["schema_version"] = 1
    value.pop("validation")
    value.pop("runtime_source_inventory")
    value.pop("attestation_sha256")
    value["attestation_sha256"] = sha256_value(value)
    origin = _canonical_attestation(root, manifest, GIT_COMMIT)
    origin.parent.mkdir(parents=True)
    origin.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return origin


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
    value = json.loads(output.read_text(encoding="utf-8"))
    assert value["artifact_class"] == "dataset-validation-attestation-v2"
    assert value["validation"] == {
        "mode": "full",
        "semantic_tree": {"path": "data/generator", "object_id": SEMANTIC_TREE},
    }
    assert value["runtime_source_inventory"] == []
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


def test_create_rejects_manifest_swap_after_full_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, manifest = _repository(tmp_path)
    output = root / ".artifacts/dataset-validation/rejected.json"
    observe = attestation_module._observe_dataset

    def observe_after_swap(
        repository_root: Path,
        manifest_file: Path,
        *,
        include_source_inventory: bool = True,
    ) -> attestation_module._DatasetObservation:
        value = json.loads(manifest.read_text(encoding="utf-8"))
        value["generation_config_sha256"] = "0" * 64
        manifest.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")
        return observe(
            repository_root,
            manifest_file,
            include_source_inventory=include_source_inventory,
        )

    monkeypatch.setattr(attestation_module, "_observe_dataset", observe_after_swap)

    with pytest.raises(DatasetAttestationError, match="changed after full validation"):
        create_attestation(root, manifest, output, PYTHON_VERSION, GIT_COMMIT)

    assert not output.exists()


def test_create_preflights_before_immutable_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, manifest = _repository(tmp_path)
    output = root / ".artifacts/dataset-validation/rejected.json"

    def verification_failure(*_args: object, **_kwargs: object) -> None:
        raise DatasetAttestationError("synthetic staged verification failure")

    monkeypatch.setattr(attestation_module, "verify_attestation", verification_failure)

    with pytest.raises(DatasetAttestationError, match="staged verification failure"):
        create_attestation(root, manifest, output, PYTHON_VERSION, GIT_COMMIT)

    assert not output.exists()


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


def test_v1_full_origin_rebinds_without_running_semantic_validator(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, manifest = _repository(tmp_path)
    origin = _write_v1_origin(root, manifest)
    monkeypatch.setattr(attestation_module, "clean_git_commit", lambda _root: CURRENT_COMMIT)

    def forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("semantic validator must not run during exact rebinding")

    monkeypatch.setattr(attestation_module, "validate_dataset", forbidden)
    monkeypatch.setattr(attestation_module, "validate_tpch_dataset", forbidden)
    output = _canonical_attestation(root, manifest, CURRENT_COMMIT)
    rebound = rebind_attestation(
        root,
        manifest,
        origin,
        output,
        PYTHON_VERSION,
        CURRENT_COMMIT,
    )

    value = json.loads(output.read_text(encoding="utf-8"))
    assert rebound.git_commit == CURRENT_COMMIT
    assert value["validation"]["mode"] == "rebound"
    assert value["validation"]["origin"]["validator_git_commit"] == GIT_COMMIT
    assert value["validation"]["origin"]["artifact_class"] == ("dataset-validation-attestation-v1")
    assert value["validation"]["origin"]["schema_version"] == 1
    assert value["validation"]["origin"]["attestation_file_sha256"] == _sha256_file(origin)
    verify_attestation(root, manifest, output, PYTHON_VERSION, CURRENT_COMMIT)


def test_direct_v1_verification_preserves_parquet_only_source_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, manifest = _repository(tmp_path)
    origin = _write_v1_origin(root, manifest)
    observe = attestation_module._observe_dataset
    source_inventory_flags: list[bool] = []

    def track_observation(
        repository_root: Path,
        manifest_file: Path,
        *,
        include_source_inventory: bool = True,
    ) -> attestation_module._DatasetObservation:
        source_inventory_flags.append(include_source_inventory)
        return observe(
            repository_root,
            manifest_file,
            include_source_inventory=include_source_inventory,
        )

    monkeypatch.setattr(attestation_module, "_observe_dataset", track_observation)
    verify_attestation(root, manifest, origin, PYTHON_VERSION, GIT_COMMIT)

    assert source_inventory_flags == [False]


def test_rebound_offline_verification_uses_bound_lineage_without_git(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, manifest = _repository(tmp_path)
    origin = _write_v1_origin(root, manifest)
    monkeypatch.setattr(attestation_module, "clean_git_commit", lambda _root: CURRENT_COMMIT)
    rebound = _canonical_attestation(root, manifest, CURRENT_COMMIT)
    rebind_attestation(root, manifest, origin, rebound, PYTHON_VERSION, CURRENT_COMMIT)

    calls = 0

    def git_forbidden(*_args: object, **_kwargs: object) -> str:
        nonlocal calls
        calls += 1
        raise DatasetAttestationError("live Git is unavailable")

    monkeypatch.setattr(attestation_module, "_git_tree_oid", git_forbidden)
    monkeypatch.setattr(attestation_module, "_is_ancestor", git_forbidden)

    verified = verify_attestation(
        root,
        manifest,
        rebound,
        PYTHON_VERSION,
        CURRENT_COMMIT,
        require_git_lineage=False,
    )
    assert verified.git_commit == CURRENT_COMMIT
    assert calls == 0

    with pytest.raises(DatasetAttestationError, match="live Git is unavailable"):
        verify_attestation(root, manifest, rebound, PYTHON_VERSION, CURRENT_COMMIT)
    assert calls == 1


def test_rebind_rejects_semantic_tree_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, manifest = _repository(tmp_path)
    origin = _write_v1_origin(root, manifest)
    monkeypatch.setattr(attestation_module, "clean_git_commit", lambda _root: CURRENT_COMMIT)
    monkeypatch.setattr(
        attestation_module,
        "_git_tree_oid",
        lambda _root, commit, _path: SEMANTIC_TREE if commit == GIT_COMMIT else "d" * 40,
    )

    with pytest.raises(DatasetAttestationNotReusable, match="Git tree differs"):
        rebind_attestation(
            root,
            manifest,
            origin,
            _canonical_attestation(root, manifest, CURRENT_COMMIT),
            PYTHON_VERSION,
            CURRENT_COMMIT,
        )


def test_rebind_rejects_origin_outside_current_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, manifest = _repository(tmp_path)
    origin = _write_v1_origin(root, manifest)
    monkeypatch.setattr(attestation_module, "clean_git_commit", lambda _root: CURRENT_COMMIT)
    monkeypatch.setattr(attestation_module, "_is_ancestor", lambda *_args: False)

    with pytest.raises(DatasetAttestationNotReusable, match="not an ancestor"):
        rebind_attestation(
            root,
            manifest,
            origin,
            _canonical_attestation(root, manifest, CURRENT_COMMIT),
            PYTHON_VERSION,
            CURRENT_COMMIT,
        )


def test_rebind_rejects_tampered_origin_self_hash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, manifest = _repository(tmp_path)
    origin = _write_v1_origin(root, manifest)
    value = json.loads(origin.read_text(encoding="utf-8"))
    value["validator"]["result_sha256"] = "0" * 64
    origin.write_text(json.dumps(value), encoding="utf-8")
    monkeypatch.setattr(attestation_module, "clean_git_commit", lambda _root: CURRENT_COMMIT)

    with pytest.raises(DatasetAttestationNotReusable, match="self-hash"):
        rebind_attestation(
            root,
            manifest,
            origin,
            _canonical_attestation(root, manifest, CURRENT_COMMIT),
            PYTHON_VERSION,
            CURRENT_COMMIT,
        )


def test_rebind_rejects_origin_semantic_result_drift_with_valid_self_hash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, manifest = _repository(tmp_path)
    origin = _write_v1_origin(root, manifest)
    value = json.loads(origin.read_text(encoding="utf-8"))
    value["validator"]["result_sha256"] = "0" * 64
    value.pop("attestation_sha256")
    value["attestation_sha256"] = sha256_value(value)
    origin.write_text(json.dumps(value), encoding="utf-8")
    monkeypatch.setattr(attestation_module, "clean_git_commit", lambda _root: CURRENT_COMMIT)

    with pytest.raises(DatasetAttestationNotReusable, match="semantic result hash is stale"):
        rebind_attestation(
            root,
            manifest,
            origin,
            _canonical_attestation(root, manifest, CURRENT_COMMIT),
            PYTHON_VERSION,
            CURRENT_COMMIT,
        )


def test_rebound_attestation_cannot_become_an_origin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, manifest = _repository(tmp_path)
    origin = _write_v1_origin(root, manifest)
    monkeypatch.setattr(attestation_module, "clean_git_commit", lambda _root: CURRENT_COMMIT)
    rebound = _canonical_attestation(root, manifest, CURRENT_COMMIT)
    rebind_attestation(root, manifest, origin, rebound, PYTHON_VERSION, CURRENT_COMMIT)
    final_commit = "e" * 40
    monkeypatch.setattr(attestation_module, "clean_git_commit", lambda _root: final_commit)

    with pytest.raises(DatasetAttestationNotReusable, match="directly to a full"):
        rebind_attestation(
            root,
            manifest,
            rebound,
            _canonical_attestation(root, manifest, final_commit),
            PYTHON_VERSION,
            final_commit,
        )


def test_verify_rejects_rebound_lineage_drift_with_valid_outer_self_hash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, manifest = _repository(tmp_path)
    origin = _write_v1_origin(root, manifest)
    monkeypatch.setattr(attestation_module, "clean_git_commit", lambda _root: CURRENT_COMMIT)
    rebound = _canonical_attestation(root, manifest, CURRENT_COMMIT)
    rebind_attestation(root, manifest, origin, rebound, PYTHON_VERSION, CURRENT_COMMIT)

    value = json.loads(rebound.read_text(encoding="utf-8"))
    value["validation"]["origin"]["attestation_file_sha256"] = "0" * 64
    value.pop("attestation_sha256")
    value["attestation_sha256"] = sha256_value(value)
    rebound.write_text(json.dumps(value), encoding="utf-8")

    with pytest.raises(DatasetAttestationError, match="lineage binding is stale"):
        verify_attestation(root, manifest, rebound, PYTHON_VERSION, CURRENT_COMMIT)


def test_rebind_rechecks_clean_head_immediately_before_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, manifest = _repository(tmp_path)
    origin = _write_v1_origin(root, manifest)
    output = _canonical_attestation(root, manifest, CURRENT_COMMIT)
    checks = 0

    def provenance(_root: Path) -> str:
        nonlocal checks
        checks += 1
        if checks == 1:
            return CURRENT_COMMIT
        raise RepositoryEvidenceError("repository worktree is not clean")

    monkeypatch.setattr(attestation_module, "clean_git_commit", provenance)

    with pytest.raises(DatasetAttestationError, match="immediately before publication"):
        rebind_attestation(
            root,
            manifest,
            origin,
            output,
            PYTHON_VERSION,
            CURRENT_COMMIT,
        )

    assert checks == 2
    assert not output.exists()


def test_discovery_returns_only_canonical_full_ancestor_receipts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, manifest = _repository(tmp_path)
    origin = _write_v1_origin(root, manifest)
    monkeypatch.setattr(
        attestation_module,
        "_ancestor_commits",
        lambda _root, _commit: (CURRENT_COMMIT, GIT_COMMIT),
    )

    assert discover_full_attestation_origins(root, manifest, CURRENT_COMMIT) == (origin,)


def test_discovery_skips_rebound_receipts_and_returns_direct_full_origin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, manifest = _repository(tmp_path)
    origin = _write_v1_origin(root, manifest)
    monkeypatch.setattr(attestation_module, "clean_git_commit", lambda _root: CURRENT_COMMIT)
    rebound = _canonical_attestation(root, manifest, CURRENT_COMMIT)
    rebind_attestation(root, manifest, origin, rebound, PYTHON_VERSION, CURRENT_COMMIT)
    final_commit = "e" * 40
    monkeypatch.setattr(
        attestation_module,
        "_ancestor_commits",
        lambda _root, _commit: (final_commit, CURRENT_COMMIT, GIT_COMMIT),
    )

    assert discover_full_attestation_origins(root, manifest, final_commit) == (origin,)


def test_repository_evidence_path_rejects_symlinked_canonical_origin(tmp_path: Path) -> None:
    root = tmp_path / "repository"
    canonical = root / ".artifacts/dataset-validations" / GIT_COMMIT / f"{'0' * 64}.json"
    target = root / ".artifacts/alternate.json"
    canonical.parent.mkdir(parents=True)
    target.write_text("{}\n", encoding="utf-8")
    try:
        canonical.symlink_to(target)
    except OSError as error:
        pytest.skip(f"symlink creation is unavailable: {error}")

    with pytest.raises(DatasetAttestationError, match="contains a symlink"):
        attestation_module._repo_path(root, canonical, label="origin attestation")


def test_tpch_source_inventory_is_exact_and_tamper_evident(tmp_path: Path) -> None:
    dataset = tmp_path / "tpch"
    raw = dataset / "raw"
    raw.mkdir(parents=True)
    tables: dict[str, object] = {}
    for table in TPCH_TABLE_ORDER:
        path = raw / f"{table}.tbl"
        path.write_text(f"{table}|source\n", encoding="utf-8")
        tables[table] = {
            "source_tbl": {
                "path": path.relative_to(dataset).as_posix(),
                "size_bytes": path.stat().st_size,
                "sha256": _sha256_file(path),
            }
        }
    manifest = {"tables": tables}

    inventory = attestation_module._runtime_source_inventory(dataset, manifest, "tpch")
    assert len(inventory) == len(TPCH_TABLE_ORDER)
    (raw / "orders.tbl").write_bytes(b"tampered-source")
    with pytest.raises(DatasetAttestationError, match="size mismatch|SHA-256 mismatch"):
        attestation_module._runtime_source_inventory(dataset, manifest, "tpch")


def test_tpch_source_inventory_rejects_unlisted_tbl(tmp_path: Path) -> None:
    dataset = tmp_path / "tpch"
    raw = dataset / "raw"
    raw.mkdir(parents=True)
    tables: dict[str, object] = {}
    for table in TPCH_TABLE_ORDER:
        path = raw / f"{table}.tbl"
        path.write_text(f"{table}|source\n", encoding="utf-8")
        tables[table] = {
            "source_tbl": {
                "path": path.relative_to(dataset).as_posix(),
                "size_bytes": path.stat().st_size,
                "sha256": _sha256_file(path),
            }
        }
    (raw / "unlisted.tbl").write_text("unexpected\n", encoding="utf-8")

    with pytest.raises(DatasetAttestationError, match="source inventory mismatch"):
        attestation_module._runtime_source_inventory(dataset, {"tables": tables}, "tpch")


def test_tpch_source_inventory_rejects_table_path_permutation(tmp_path: Path) -> None:
    dataset = tmp_path / "tpch"
    raw = dataset / "raw"
    raw.mkdir(parents=True)
    tables: dict[str, object] = {}
    for table in TPCH_TABLE_ORDER:
        path = raw / f"{table}.tbl"
        path.write_text(f"{table}|source\n", encoding="utf-8")
        tables[table] = {
            "source_tbl": {
                "path": path.relative_to(dataset).as_posix(),
                "size_bytes": path.stat().st_size,
                "sha256": _sha256_file(path),
            }
        }
    customer = tables["customer"]
    orders = tables["orders"]
    assert isinstance(customer, dict) and isinstance(orders, dict)
    customer["source_tbl"], orders["source_tbl"] = (
        orders["source_tbl"],
        customer["source_tbl"],
    )

    with pytest.raises(DatasetAttestationError, match="source file path is invalid"):
        attestation_module._runtime_source_inventory(dataset, {"tables": tables}, "tpch")
