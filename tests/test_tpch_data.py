from __future__ import annotations

from pathlib import Path

import pytest

from data.tpch.contract import SF1_ROW_COUNTS, TABLE_ORDER, TPCH_SCHEMAS
from data.tpch.dataset import (
    NOTICE,
    TpchContractError,
    build_dataset_from_tbl,
    parse_tbl_row,
    validate_tpch_dataset,
)
from data.tpch.source import load_source_lock
from scripts.ensure_tpch_data import MINIMUM_FREE_BYTES, PrimaryTpchGateError, _require_primary_gate

ROOT = Path(__file__).resolve().parents[1]
COMMIT = "1" * 40
SOURCE = {
    "name": "tpch-dbgen",
    "version": f"commit-{COMMIT}",
    "commit": COMMIT,
    "archive_url": f"https://codeload.github.com/example/tpch/tar.gz/{COMMIT}",
    "archive_sha256": "2" * 64,
    "source_url": f"https://github.com/example/tpch/commit/{COMMIT}",
    "license": "TPC EULA",
}


def _tiny_tbl_source(root: Path) -> dict[str, int]:
    root.mkdir()
    rows = {
        "region": "0|AFRICA|comment|\n",
        "nation": "0|ALGERIA|0|comment|\n",
        "supplier": "1|Supplier#1|address|0|00-000-000-0000|0.00|comment|\n",
        "customer": "1|Customer#1|address|0|00-000-000-0000|0.00|BUILDING|comment|\n",
        "part": "1|part|mfgr|brand|type|1|SM BOX|1.00|comment|\n",
        "partsupp": "1|1|1|1.00|comment|\n",
        "orders": "1|1|O|1.00|1994-01-01|1-URGENT|Clerk#1|0|comment|\n",
        "lineitem": (
            "1|1|1|1|1.00|1.00|0.05|0.01|N|O|1994-01-02|1994-01-03|"
            "1994-01-04|DELIVER IN PERSON|MAIL|comment|\n"
        ),
    }
    for table_name, row in rows.items():
        (root / f"{table_name}.tbl").write_text(row, encoding="utf-8", newline="")
    return {table_name: 1 for table_name in TABLE_ORDER}


def test_locked_tpch_source_and_sf1_contract_are_explicit() -> None:
    source = load_source_lock(ROOT / "runtime-versions.lock")
    assert source.name == "tpch-dbgen"
    assert len(source.commit) == 40
    assert (
        source.archive_sha256 == "d0d92c4191c776bcc7bce84e0d2156c3a744c115fb9a9ccbcaac908313708c96"
    )
    assert tuple(TPCH_SCHEMAS) == TABLE_ORDER
    assert SF1_ROW_COUNTS["lineitem"] == 6_001_215


def test_tiny_tpch_conversion_is_atomic_hash_bound_and_revalidates(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    expected_counts = _tiny_tbl_source(source_dir)
    output = tmp_path / "dataset"

    result = build_dataset_from_tbl(
        source_dir,
        output,
        source_provenance=SOURCE,
        generator_git_commit=COMMIT,
        expected_counts=expected_counts,
        target_file_size_bytes=1_024,
        row_group_rows=1,
    )
    report = validate_tpch_dataset(
        output,
        expected_counts=expected_counts,
        expected_source=SOURCE,
    )

    assert result.manifest["notice"] == NOTICE
    assert result.manifest["benchmark_eligible"] is True
    assert report.row_counts == expected_counts
    assert report.foreign_key_checks == 10
    assert report.date_bounds["lineitem"]["l_receiptdate"] == ["1994-01-04", "1994-01-04"]

    parquet = output / result.manifest["tables"]["region"]["files"][0]["path"]
    with parquet.open("ab") as stream:
        stream.write(b"tamper")
    with pytest.raises(TpchContractError, match="Parquet hash"):
        validate_tpch_dataset(output, expected_counts=expected_counts, expected_source=SOURCE)


def test_tbl_parser_rejects_noncanonical_rows() -> None:
    with pytest.raises(TpchContractError, match="trailing delimiter"):
        parse_tbl_row("region", "0|AFRICA|comment", 1)
    with pytest.raises(TpchContractError, match="expected 3"):
        parse_tbl_row("region", "0|AFRICA|comment|extra|", 1)


def test_primary_tpch_gate_requires_clean_commit_and_capacity(tmp_path: Path) -> None:
    assert (
        _require_primary_gate(
            tmp_path,
            tmp_path / "output",
            minimum_free_bytes=MINIMUM_FREE_BYTES,
            git_probe=lambda _: (COMMIT, False),
            disk_probe=lambda _: MINIMUM_FREE_BYTES,
        )
        == COMMIT
    )
    with pytest.raises(PrimaryTpchGateError, match="dirty worktree"):
        _require_primary_gate(
            tmp_path,
            tmp_path / "output",
            minimum_free_bytes=MINIMUM_FREE_BYTES,
            git_probe=lambda _: (COMMIT, True),
            disk_probe=lambda _: MINIMUM_FREE_BYTES,
        )
    with pytest.raises(PrimaryTpchGateError, match="insufficient free disk"):
        _require_primary_gate(
            tmp_path,
            tmp_path / "output",
            minimum_free_bytes=MINIMUM_FREE_BYTES,
            git_probe=lambda _: (COMMIT, False),
            disk_probe=lambda _: MINIMUM_FREE_BYTES - 1,
        )
