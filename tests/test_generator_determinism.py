from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

from data.generator.constants import GENERATOR_VERSION, TABLE_ORDER
from data.generator.generate import generate_dataset
from data.generator.prf import field_digest
from data.generator.profiles import load_profile
from data.generator.schemas import PRIMARY_KEYS, TABLE_SCHEMAS, ecommerce_contract
from data.generator.validation import validate_dataset

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_PROFILE = ROOT / "data" / "generator" / "configs" / "fixture.yaml"
TINY_PROFILE = ROOT / "data" / "generator" / "configs" / "tiny.yaml"
SMALL_PROFILE = ROOT / "data" / "generator" / "configs" / "small.yaml"

EXPECTED_FIXTURE_CONTENT_HASHES = {
    "customers": "f53981065725d615eb55c8a37816ea6ae2a563551fed31914530cd73bfc9edb2",
    "products": "25d3d0e3647b4ef02123da8f6c12fb03b6c03cd22ad97ca88f91fbf6c7b5d71b",
    "orders": "9b96ad260f4fa4dbbfbb2ed524caa0a351b3cb679c7f010654ae079c9c2db417",
    "order_items": "8ac9dbb8e102e2921a0ef370275bbf3b00557efb4861b38e3aaca38b9e236ce7",
    "events": "d4a4df576035d7e0047865748069ffa306ab3d1b09aec73332c3235eb4eaf653",
}


def test_profile_contracts_are_normative_and_non_research() -> None:
    fixture = load_profile(FIXTURE_PROFILE)
    tiny = load_profile(TINY_PROFILE)

    assert fixture.seed == 42
    assert fixture.dataset_revision == 1
    assert fixture.dataset_id == "ecommerce-fixture-uniform-seed-42-v1"
    assert fixture.benchmark_eligible is False
    assert fixture.counts == {
        "customers": 24,
        "products": 20,
        "orders": 96,
        "order_items": 240,
        "events": 512,
    }
    assert tiny.seed == 20260824
    assert tiny.benchmark_eligible is False
    assert tiny.counts == {
        "customers": 10_000,
        "products": 1_000,
        "orders": 100_000,
        "order_items": 400_000,
        "events": 1_000_000,
    }

    small = load_profile(SMALL_PROFILE)
    assert small.benchmark_eligible is True
    assert small.profile_id == "small"
    assert small.dataset_revision == 3
    assert small.dataset_id == "ecommerce-small-uniform-seed-20260827-v3"
    assert small.as_canonical_mapping()["dataset_revision"] == 3
    assert small.counts["orders"] == 1_000_000
    assert small.rows_per_file["order_items"] == 1_000_000


def test_benchmark_profile_rejects_fragmented_order_item_targets(tmp_path: Path) -> None:
    decoded = yaml.safe_load(SMALL_PROFILE.read_text(encoding="utf-8"))
    decoded["rows_per_file"]["order_items"] = 500_000
    invalid_profile = tmp_path / "fragmented-benchmark.yaml"
    invalid_profile.write_text(yaml.safe_dump(decoded), encoding="utf-8")

    with pytest.raises(ValueError, match="undersized fact-file row targets"):
        load_profile(invalid_profile)


def test_field_prf_is_the_declared_sha256_material() -> None:
    expected = hashlib.sha256(f"{GENERATOR_VERSION}|42|customers|1|region".encode()).digest()
    assert field_digest(42, "customers", 1, "region") == expected


def test_checked_in_arrow_contract_matches_code() -> None:
    checked_in = json.loads(
        (ROOT / "data" / "schemas" / "ecommerce-v1.json").read_text(encoding="utf-8")
    )
    generated = ecommerce_contract()
    assert checked_in["schema_version"] == generated["schema_version"]
    for table_name in TABLE_ORDER:
        assert checked_in["tables"][table_name]["primary_key"] == list(PRIMARY_KEYS[table_name])
        assert (
            checked_in["tables"][table_name]["fields"] == generated["tables"][table_name]["fields"]
        )
        assert (
            checked_in["tables"][table_name]["schema_sha256"]
            == generated["tables"][table_name]["schema_sha256"]
        )


def test_profiles_validate_against_json_schema() -> None:
    schema = json.loads(
        (ROOT / "data" / "schemas" / "generator-profile.schema.json").read_text(encoding="utf-8")
    )
    validator = Draft202012Validator(schema)
    for path in (FIXTURE_PROFILE, TINY_PROFILE, SMALL_PROFILE):
        decoded = yaml.safe_load(path.read_text(encoding="utf-8"))
        validator.validate(decoded)


def test_benchmark_eligibility_cannot_be_added_to_a_tiny_profile(tmp_path: Path) -> None:
    decoded = yaml.safe_load(FIXTURE_PROFILE.read_text(encoding="utf-8"))
    decoded["benchmark_eligible"] = True
    decoded["profile_id"] = "small"
    invalid_profile = tmp_path / "invalid-benchmark.yaml"
    invalid_profile.write_text(yaml.safe_dump(decoded), encoding="utf-8")

    with pytest.raises(ValueError, match="undersized"):
        load_profile(invalid_profile)


def test_profile_loader_rejects_duplicate_keys(tmp_path: Path) -> None:
    invalid_profile = tmp_path / "duplicate.yaml"
    invalid_profile.write_text(
        FIXTURE_PROFILE.read_text(encoding="utf-8") + "seed: 999\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="duplicate YAML mapping key: 'seed'"):
        load_profile(invalid_profile)


def test_same_profile_produces_identical_manifests_and_parquet(
    tmp_path: Path,
) -> None:
    profile = load_profile(FIXTURE_PROFILE)
    first = generate_dataset(
        profile,
        tmp_path / "first",
        generator_git_commit="test-revision",
        generator_worktree_dirty=False,
    )
    second = generate_dataset(
        profile,
        tmp_path / "second",
        generator_git_commit="test-revision",
        generator_worktree_dirty=False,
    )

    assert first.manifest == second.manifest
    assert first.manifest_path.read_bytes() == second.manifest_path.read_bytes()
    assert {
        table_name: first.manifest["tables"][table_name]["content_sha256"]
        for table_name in TABLE_ORDER
    } == EXPECTED_FIXTURE_CONTENT_HASHES
    for table_name in TABLE_ORDER:
        first_files = first.manifest["tables"][table_name]["files"]
        second_files = second.manifest["tables"][table_name]["files"]
        assert first_files == second_files
        for file_record in first_files:
            relative_path = Path(file_record["path"])
            assert (first.dataset_dir / relative_path).read_bytes() == (
                second.dataset_dir / relative_path
            ).read_bytes()

    assert validate_dataset(first.dataset_dir, expected_profile=profile).dataset_id == (
        profile.dataset_id
    )
    assert validate_dataset(second.dataset_dir, expected_profile=profile).dataset_id == (
        profile.dataset_id
    )


def test_content_hash_is_independent_of_parquet_file_boundaries(tmp_path: Path) -> None:
    profile = load_profile(FIXTURE_PROFILE)
    smaller_files = replace(
        profile,
        rows_per_file={
            table_name: max(1, count // 3) for table_name, count in profile.counts.items()
        },
    )
    normal = generate_dataset(
        profile,
        tmp_path / "normal",
        generator_git_commit="test-revision",
        generator_worktree_dirty=False,
    )
    repartitioned = generate_dataset(
        smaller_files,
        tmp_path / "repartitioned",
        generator_git_commit="test-revision",
        generator_worktree_dirty=False,
    )

    for table_name in TABLE_ORDER:
        normal_table = normal.manifest["tables"][table_name]
        repartitioned_table = repartitioned.manifest["tables"][table_name]
        assert normal_table["content_sha256"] == repartitioned_table["content_sha256"]
        assert normal_table["range_hashes"] == repartitioned_table["range_hashes"]
        assert normal_table["file_count"] != repartitioned_table["file_count"]


def test_different_seed_changes_content_identity(tmp_path: Path) -> None:
    profile = load_profile(FIXTURE_PROFILE)
    other_seed = replace(profile, seed=43)
    first = generate_dataset(
        profile,
        tmp_path / "seed-42",
        generator_git_commit="test-revision",
        generator_worktree_dirty=False,
    )
    second = generate_dataset(
        other_seed,
        tmp_path / "seed-43",
        generator_git_commit="test-revision",
        generator_worktree_dirty=False,
    )

    assert first.manifest["dataset_id"] != second.manifest["dataset_id"]
    assert any(
        first.manifest["tables"][table]["content_sha256"]
        != second.manifest["tables"][table]["content_sha256"]
        for table in TABLE_ORDER
    )


def test_manifest_validates_against_json_schema(tmp_path: Path) -> None:
    profile = load_profile(FIXTURE_PROFILE)
    generated = generate_dataset(
        profile,
        tmp_path / "dataset",
        generator_git_commit="test-revision",
        generator_worktree_dirty=False,
    )
    profile_schema = json.loads(
        (ROOT / "data" / "schemas" / "generator-profile.schema.json").read_text(encoding="utf-8")
    )
    manifest_schema = json.loads(
        (ROOT / "data" / "schemas" / "dataset-manifest.schema.json").read_text(encoding="utf-8")
    )
    registry = Registry().with_resource(
        profile_schema["$id"], Resource.from_contents(profile_schema)
    )
    Draft202012Validator(manifest_schema, registry=registry).validate(generated.manifest)


def test_benchmark_manifest_schema_requires_python_provenance(tmp_path: Path) -> None:
    profile = load_profile(FIXTURE_PROFILE)
    generated = generate_dataset(
        profile,
        tmp_path / "dataset",
        generator_git_commit="test-revision",
        generator_worktree_dirty=False,
    )
    manifest = generated.manifest
    manifest["benchmark_eligible"] = True
    del manifest["generator"]["python_version"]
    del manifest["generator"]["python_implementation"]

    profile_schema = json.loads(
        (ROOT / "data" / "schemas" / "generator-profile.schema.json").read_text(encoding="utf-8")
    )
    manifest_schema = json.loads(
        (ROOT / "data" / "schemas" / "dataset-manifest.schema.json").read_text(encoding="utf-8")
    )
    registry = Registry().with_resource(
        profile_schema["$id"], Resource.from_contents(profile_schema)
    )
    errors = list(Draft202012Validator(manifest_schema, registry=registry).iter_errors(manifest))

    assert {"python_version", "python_implementation"}.issubset(
        {
            field
            for error in errors
            for field in ("python_version", "python_implementation")
            if field in error.message
        }
    )


def test_benchmark_manifest_schema_rejects_dirty_generator_provenance(tmp_path: Path) -> None:
    profile = load_profile(FIXTURE_PROFILE)
    generated = generate_dataset(
        profile,
        tmp_path / "dataset",
        generator_git_commit="test-revision",
        generator_worktree_dirty=False,
    )
    manifest = generated.manifest
    manifest["benchmark_eligible"] = True
    manifest["generator"]["worktree_dirty"] = True

    profile_schema = json.loads(
        (ROOT / "data" / "schemas" / "generator-profile.schema.json").read_text(encoding="utf-8")
    )
    manifest_schema = json.loads(
        (ROOT / "data" / "schemas" / "dataset-manifest.schema.json").read_text(encoding="utf-8")
    )
    registry = Registry().with_resource(
        profile_schema["$id"], Resource.from_contents(profile_schema)
    )
    errors = list(Draft202012Validator(manifest_schema, registry=registry).iter_errors(manifest))

    assert any("False was expected" in error.message for error in errors)


def test_generator_rejects_dirty_benchmark_provenance_before_writing(tmp_path: Path) -> None:
    profile = replace(load_profile(FIXTURE_PROFILE), benchmark_eligible=True)
    output = tmp_path / "missing-parent" / "dataset"

    with pytest.raises(ValueError, match="clean generator worktree"):
        generate_dataset(
            profile,
            output,
            generator_git_commit="test-revision",
            generator_worktree_dirty=True,
        )

    assert not output.parent.exists()


@pytest.mark.parametrize(
    ("python_version", "python_implementation", "message"),
    [
        ("", "CPython", "exact MAJOR.MINOR.PATCH"),
        ("3.12", "CPython", "exact MAJOR.MINOR.PATCH"),
        ("3.12.13", "PyPy", "must be 'CPython'"),
    ],
)
def test_generator_rejects_invalid_python_provenance_before_writing(
    tmp_path: Path,
    python_version: str,
    python_implementation: str,
    message: str,
) -> None:
    profile = load_profile(FIXTURE_PROFILE)
    output = tmp_path / "missing-parent" / "dataset"

    with pytest.raises(ValueError, match=message):
        generate_dataset(
            profile,
            output,
            generator_git_commit="test-revision",
            generator_worktree_dirty=False,
            generator_python_version=python_version,
            generator_python_implementation=python_implementation,
        )

    assert not output.parent.exists()


def test_output_is_immutable(tmp_path: Path) -> None:
    profile = load_profile(FIXTURE_PROFILE)
    output = tmp_path / "dataset"
    first = generate_dataset(
        profile,
        output,
        generator_git_commit="test-revision",
        generator_worktree_dirty=False,
    )
    original_manifest = first.manifest_path.read_bytes()
    with pytest.raises(FileExistsError, match="immutable dataset output already exists"):
        generate_dataset(
            profile,
            output,
            generator_git_commit="test-revision",
            generator_worktree_dirty=False,
        )
    assert first.manifest_path.read_bytes() == original_manifest


def test_physical_schemas_counts_and_sort_order(tmp_path: Path) -> None:
    profile = load_profile(FIXTURE_PROFILE)
    result = generate_dataset(
        profile,
        tmp_path / "dataset",
        generator_git_commit="test-revision",
        generator_worktree_dirty=False,
    )

    for table_name in TABLE_ORDER:
        paths = [
            result.dataset_dir / record["path"]
            for record in result.manifest["tables"][table_name]["files"]
        ]
        table = pq.read_table(paths)
        assert table.schema.equals(TABLE_SCHEMAS[table_name])
        assert table.num_rows == profile.counts[table_name]
        keys = [tuple(row[name] for name in PRIMARY_KEYS[table_name]) for row in table.to_pylist()]
        assert keys == sorted(keys)
        assert len(keys) == len(set(keys))
