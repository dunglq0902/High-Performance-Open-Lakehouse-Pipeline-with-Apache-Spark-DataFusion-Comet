from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from data.generator.constants import TABLE_ORDER
from data.generator.generate import generate_dataset
from data.generator.profiles import load_profile
from data.generator.schemas import ORDERS_SCHEMA
from data.generator.validation import DatasetValidationError, validate_dataset

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_PROFILE = ROOT / "data" / "generator" / "configs" / "fixture.yaml"


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _generated_fixture(tmp_path: Path) -> tuple[Path, dict[str, object]]:
    profile = load_profile(FIXTURE_PROFILE)
    result = generate_dataset(
        profile,
        tmp_path / "dataset",
        generator_git_commit="test-revision",
        generator_worktree_dirty=False,
    )
    return result.dataset_dir, result.manifest


def test_integrity_validator_accepts_all_five_tables(tmp_path: Path) -> None:
    dataset_dir, manifest = _generated_fixture(tmp_path)
    profile = load_profile(FIXTURE_PROFILE)
    report = validate_dataset(dataset_dir, expected_profile=profile)

    assert report.dataset_id == profile.dataset_id
    assert report.table_row_counts == dict(profile.counts)
    assert set(report.table_content_sha256) == set(TABLE_ORDER)
    for table_name in TABLE_ORDER:
        table_manifest = manifest["tables"][table_name]  # type: ignore[index]
        assert table_manifest["row_count"] == profile.counts[table_name]
        assert table_manifest["file_count"] == len(table_manifest["files"])
        assert table_manifest["total_bytes"] > 0
        assert table_manifest["range_hashes"]
        assert len(table_manifest["content_sha256"]) == 64


def test_validator_detects_manifest_file_digest_tampering(tmp_path: Path) -> None:
    dataset_dir, manifest = _generated_fixture(tmp_path)
    first_file = manifest["tables"]["customers"]["files"][0]  # type: ignore[index]
    first_file["sha256"] = "0" * 64
    (dataset_dir / "manifest.json").write_text(
        json.dumps(manifest, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
        newline="\n",
    )

    with pytest.raises(DatasetValidationError, match="file SHA-256 mismatch"):
        validate_dataset(dataset_dir)


def test_validator_detects_foreign_key_orphan_even_with_updated_file_digest(
    tmp_path: Path,
) -> None:
    dataset_dir, manifest = _generated_fixture(tmp_path)
    order_file_record = manifest["tables"]["orders"]["files"][0]  # type: ignore[index]
    order_path = dataset_dir / order_file_record["path"]
    rows = pq.read_table(order_path).to_pylist()
    rows[0]["customer_id"] = 9_999_999
    pq.write_table(
        pa.Table.from_pylist(rows, schema=ORDERS_SCHEMA),
        order_path,
        compression="snappy",
        use_dictionary=True,
        write_statistics=True,
        version="2.6",
        data_page_version="1.0",
        use_compliant_nested_type=True,
    )
    order_file_record["sha256"] = _sha256_file(order_path)
    order_file_record["size_bytes"] = order_path.stat().st_size
    manifest["tables"]["orders"]["total_bytes"] = sum(  # type: ignore[index]
        record["size_bytes"]
        for record in manifest["tables"]["orders"]["files"]  # type: ignore[index]
    )
    (dataset_dir / "manifest.json").write_text(
        json.dumps(manifest, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
        newline="\n",
    )

    with pytest.raises(DatasetValidationError, match="orphan customer_id=9999999"):
        validate_dataset(dataset_dir)


def test_validator_detects_unlisted_parquet_file(tmp_path: Path) -> None:
    dataset_dir, _ = _generated_fixture(tmp_path)
    source = next((dataset_dir / "customers").glob("*.parquet"))
    unlisted = dataset_dir / "customers" / "part-99999.parquet"
    unlisted.write_bytes(source.read_bytes())

    with pytest.raises(DatasetValidationError, match="file inventory mismatch"):
        validate_dataset(dataset_dir)
