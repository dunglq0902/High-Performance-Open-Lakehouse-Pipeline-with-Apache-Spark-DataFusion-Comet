from pathlib import Path

import pytest

from data.generator.profiles import load_profile
from data.tpch.contract import TABLE_ORDER as TPCH_TABLE_ORDER
from pipeline.medallion.build import (
    BRONZE_TABLE_DDLS,
    GOLD_TABLE_SQL,
    SILVER_EVENTS_SQL,
    SILVER_SALES_SQL,
    _is_tpch_manifest,
    _safe_table_files,
    _tpch_table_ddl,
    _validate_source_dataset,
)

ROOT = Path(__file__).resolve().parents[1]


def test_medallion_contract_covers_all_source_and_required_derived_tables() -> None:
    assert set(BRONZE_TABLE_DDLS) == {
        "customers",
        "products",
        "orders",
        "order_items",
        "events",
    }
    assert set(GOLD_TABLE_SQL) == {
        "daily_revenue",
        "customer_ltv",
        "product_ranking",
        "category_growth",
    }
    assert "CREATE OR REPLACE TABLE lakehouse.silver.sales_enriched" in SILVER_SALES_SQL
    assert "CREATE OR REPLACE TABLE lakehouse.silver.events" in SILVER_EVENTS_SQL
    assert all("format-version" in sql for sql in BRONZE_TABLE_DDLS.values())
    assert all("CREATE OR REPLACE TABLE lakehouse.gold." in sql for sql in GOLD_TABLE_SQL.values())


def test_primary_profile_is_large_and_benchmark_eligible() -> None:
    profile = load_profile(ROOT / "data/generator/configs/small.yaml")
    assert profile.benchmark_eligible
    assert profile.counts["orders"] >= 1_000_000
    assert profile.counts["order_items"] >= 4_000_000
    assert profile.rows_per_file["order_items"] >= 1_000_000


def test_tpch_import_contract_covers_all_eight_explicit_iceberg_tables() -> None:
    manifest = {
        "scale_factor": 1,
        "storage": {"profile": "tpch_parquet"},
        "tables": {table_name: {} for table_name in TPCH_TABLE_ORDER},
    }
    assert _is_tpch_manifest(manifest)
    for table_name in TPCH_TABLE_ORDER:
        ddl = _tpch_table_ddl(table_name)
        assert f"CREATE OR REPLACE TABLE lakehouse.tpch.{table_name}" in ddl
        assert "USING iceberg" in ddl
        assert "NOT NULL" in ddl


def test_manifest_file_resolution_rejects_cross_table_paths(tmp_path: Path) -> None:
    dataset = tmp_path / "dataset"
    (dataset / "orders").mkdir(parents=True)
    (dataset / "customers").mkdir()
    manifest = dataset / "manifest.json"
    manifest.write_text("{}", encoding="utf-8")
    wrong = dataset / "customers" / "part-00000.parquet"
    wrong.write_bytes(b"not parquet")

    with pytest.raises(RuntimeError, match="another table"):
        _safe_table_files(
            manifest,
            "orders",
            {"files": [{"path": "customers/part-00000.parquet"}]},
        )


def test_attested_medallion_import_uses_quick_verifier_not_full_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = ROOT / "data/generated/ecommerce-small-uniform-seed-20260827-v3/manifest.json"
    attestation = tmp_path / "attestation.json"
    attestation.write_text("content-bound evidence\n", encoding="utf-8")
    observed: dict[str, object] = {}

    def verify_stub(
        root: Path,
        manifest_path: Path,
        attestation_path: Path,
        *,
        expected_python_version: str,
        expected_git_commit: str,
    ) -> None:
        observed.update(
            {
                "root": root,
                "manifest": manifest_path,
                "attestation": attestation_path,
                "python": expected_python_version,
                "commit": expected_git_commit,
            }
        )

    def full_scan_forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("full validator must not run on an attested import")

    monkeypatch.setattr("pipeline.medallion.build.verify_attestation", verify_stub)
    monkeypatch.setattr("pipeline.medallion.build.validate_dataset", full_scan_forbidden)

    result = _validate_source_dataset(
        manifest,
        tpch_dataset=False,
        attestation_path=attestation,
        expected_git_commit="a" * 40,
    )

    assert isinstance(result, str) and len(result) == 64
    assert observed == {
        "root": ROOT,
        "manifest": manifest,
        "attestation": attestation,
        "python": "3.12.13",
        "commit": "a" * 40,
    }


def test_attested_medallion_import_requires_commit_pair(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="must be supplied together"):
        _validate_source_dataset(
            ROOT / "data/generated/ecommerce-small-uniform-seed-20260827-v3/manifest.json",
            tpch_dataset=False,
            attestation_path=tmp_path / "attestation.json",
            expected_git_commit=None,
        )
