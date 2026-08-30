from __future__ import annotations

import json
import shutil
import subprocess
import tarfile
from collections.abc import Mapping, Sequence
from pathlib import Path

import pyarrow.parquet as pq  # type: ignore[import-untyped]
import pytest

from data.tpch.contract import SF1_ROW_COUNTS, TABLE_ORDER, TPCH_SCHEMAS
from data.tpch.dataset import (
    CONVERTER_VERSION,
    NOTICE,
    SOURCE_ROW_NORMALIZATION,
    SOURCE_TBL_FORMAT,
    TpchContractError,
    _convert_table,
    _manifest_hash,
    build_dataset_from_tbl,
    parse_tbl_row,
    validate_tpch_dataset,
)
from data.tpch.source import (
    DBGEN_BUILD_COMMAND,
    DBGEN_GENERATE_COMMAND,
    DBGEN_SF1_TIMEOUT_SECONDS,
    SourceLock,
    SourceProvenanceError,
    _run_command,
    load_source_lock,
    materialize_dbgen_tables,
    sha256_file,
)
from scripts.ensure_tpch_data import (
    MINIMUM_FREE_BYTES,
    PrimaryTpchGateError,
    _locked_python_version,
    _require_primary_gate,
)

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
    root.mkdir(parents=True, exist_ok=True)
    rows = {
        "region": "0|AFRICA|comment\n",
        "nation": "0|ALGERIA|0|comment\n",
        "supplier": "1|Supplier#1|address|0|00-000-000-0000|0.00|comment\n",
        "customer": "1|Customer#1|address|0|00-000-000-0000|0.00|BUILDING|comment\n",
        "part": "1|part|mfgr|brand|type|1|SM BOX|1.00|comment\n",
        "partsupp": "1|1|1|1.00|comment\n",
        "orders": "1|1|O|1.00|1994-01-01|1-URGENT|Clerk#1|0|comment\n",
        "lineitem": (
            "1|1|1|1|1.00|1.00|0.05|0.01|N|O|1994-01-02|1994-01-03|"
            "1994-01-04|DELIVER IN PERSON|MAIL|comment\n"
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
    assert result.manifest["generator"]["version"] == CONVERTER_VERSION
    assert result.manifest["generator"]["python_implementation"] == "CPython"
    assert result.manifest["generator"]["python_version"] == "3.12.13"
    assert result.manifest["generation"]["source_tbl_format"] == SOURCE_TBL_FORMAT
    assert result.manifest["generation"]["source_row_normalization"] == SOURCE_ROW_NORMALIZATION
    assert report.row_counts == expected_counts
    assert report.foreign_key_checks == 10
    assert report.date_bounds["lineitem"]["l_receiptdate"] == ["1994-01-04", "1994-01-04"]
    with pytest.raises(TpchContractError, match="Python differs from runtime lock"):
        validate_tpch_dataset(
            output,
            expected_counts=expected_counts,
            expected_source=SOURCE,
            expected_python_version="0.0.0",
        )

    manifest_path = output / "manifest.json"
    original_manifest = manifest_path.read_text(encoding="utf-8")
    tampered_manifest = json.loads(original_manifest)
    tampered_manifest["generation"]["source_tbl_format"]["trailing_delimiter"] = True
    tampered_manifest["manifest_sha256"] = _manifest_hash(tampered_manifest)
    manifest_path.write_text(
        json.dumps(tampered_manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    with pytest.raises(TpchContractError, match="generation metadata"):
        validate_tpch_dataset(output, expected_counts=expected_counts, expected_source=SOURCE)
    manifest_path.write_text(original_manifest, encoding="utf-8", newline="")

    tampered_manifest = json.loads(original_manifest)
    tampered_manifest["generation"]["source_row_normalization"]["partsupp"]["algorithm"] = (
        "unrecorded-sort"
    )
    tampered_manifest["manifest_sha256"] = _manifest_hash(tampered_manifest)
    manifest_path.write_text(
        json.dumps(tampered_manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    with pytest.raises(TpchContractError, match="generation metadata"):
        validate_tpch_dataset(output, expected_counts=expected_counts, expected_source=SOURCE)
    manifest_path.write_text(original_manifest, encoding="utf-8", newline="")

    tampered_manifest = json.loads(original_manifest)
    tampered_manifest["generator"]["git_commit"] = "unknown"
    tampered_manifest["manifest_sha256"] = _manifest_hash(tampered_manifest)
    manifest_path.write_text(
        json.dumps(tampered_manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    with pytest.raises(TpchContractError, match="generator provenance"):
        validate_tpch_dataset(output, expected_counts=expected_counts, expected_source=SOURCE)
    manifest_path.write_text(original_manifest, encoding="utf-8", newline="")

    parquet = output / result.manifest["tables"]["region"]["files"][0]["path"]
    with parquet.open("ab") as stream:
        stream.write(b"tamper")
    with pytest.raises(TpchContractError, match="Parquet hash"):
        validate_tpch_dataset(output, expected_counts=expected_counts, expected_source=SOURCE)


def test_tbl_parser_accepts_locked_and_official_rows_with_exact_column_count() -> None:
    locked = parse_tbl_row("region", "0|AFRICA|comment", 1)
    official = parse_tbl_row("region", "0|AFRICA|comment|", 2)

    assert locked == official == {"r_regionkey": 0, "r_name": "AFRICA", "r_comment": "comment"}
    with pytest.raises(TpchContractError, match="has 2 columns; expected 3"):
        parse_tbl_row("region", "0|AFRICA", 3)
    with pytest.raises(TpchContractError, match="expected 3"):
        parse_tbl_row("region", "0|AFRICA|comment|extra", 4)
    with pytest.raises(TpchContractError, match="has 4 columns; expected 3"):
        parse_tbl_row("region", "0|AFRICA|comment||", 5)


def _write_partsupp_rows(path: Path, keys: Sequence[tuple[int, int]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(
            f"{partkey}|{suppkey}|100|12.34|official dbgen order|\n" for partkey, suppkey in keys
        ),
        encoding="utf-8",
        newline="",
    )


def test_partsupp_official_group_order_is_normalized_in_parquet(tmp_path: Path) -> None:
    source_keys = [
        (2500, 7501),
        (2500, 1),
        (2500, 5001),
        (2500, 2501),
        (2501, 7502),
        (2501, 2),
        (2501, 5002),
        (2501, 2502),
    ]
    raw_path = tmp_path / "raw" / "partsupp.tbl"
    _write_partsupp_rows(raw_path, source_keys)

    record = _convert_table(
        tmp_path,
        raw_path,
        "partsupp",
        len(source_keys),
        target_file_size_bytes=1_024 * 1_024,
        row_group_rows=2,
    )
    parquet_keys: list[tuple[int, int]] = []
    for file_record in record["files"]:
        table = pq.read_table(
            tmp_path / str(file_record["path"]),
            columns=["ps_partkey", "ps_suppkey"],
        )
        parquet_keys.extend(
            zip(
                table.column("ps_partkey").to_pylist(),
                table.column("ps_suppkey").to_pylist(),
                strict=True,
            )
        )

    assert parquet_keys == sorted(source_keys)


def test_partsupp_normalization_still_rejects_duplicate_primary_keys(tmp_path: Path) -> None:
    raw_path = tmp_path / "raw" / "partsupp.tbl"
    _write_partsupp_rows(raw_path, [(2500, 7501), (2500, 1), (2500, 7501)])

    with pytest.raises(TpchContractError, match="primary key is duplicate or not ordered"):
        _convert_table(
            tmp_path,
            raw_path,
            "partsupp",
            3,
            target_file_size_bytes=1_024 * 1_024,
            row_group_rows=2,
        )


def _synthetic_source_archive(tmp_path: Path) -> tuple[Path, SourceLock]:
    source_root = tmp_path / "archive-source" / f"tpch-kit-{COMMIT}"
    dbgen_dir = source_root / "dbgen"
    dbgen_dir.mkdir(parents=True)
    (dbgen_dir / "Makefile").write_text("dbgen:\n\t@true\n", encoding="utf-8")
    archive = tmp_path / "synthetic-tpch.tar.gz"
    with tarfile.open(archive, mode="w:gz") as destination:
        destination.add(source_root, arcname=source_root.name)
    source_lock = SourceLock(
        name="tpch-dbgen",
        version=f"commit-{COMMIT}",
        archive_url=f"https://codeload.github.com/example/tpch/tar.gz/{COMMIT}",
        archive_sha256=sha256_file(archive),
        source_url=f"https://github.com/example/tpch/commit/{COMMIT}",
        license="TPC EULA",
    )
    return archive, source_lock


def test_synthetic_archive_materializes_with_locked_commands(tmp_path: Path) -> None:
    archive, source_lock = _synthetic_source_archive(tmp_path)
    commands: list[tuple[str, ...]] = []

    def downloader(_url: str, destination: Path) -> None:
        shutil.copyfile(archive, destination)

    def command_runner(arguments: Sequence[str], cwd: Path, environment: Mapping[str, str]) -> None:
        command = tuple(arguments)
        commands.append(command)
        assert (cwd / "Makefile").is_file()
        if command == DBGEN_BUILD_COMMAND:
            (cwd / "dbgen").write_text("synthetic binary", encoding="utf-8")
            return
        assert command == DBGEN_GENERATE_COMMAND
        assert environment["LC_ALL"] == environment["LANG"] == "C"
        assert environment["TZ"] == "UTC"
        _tiny_tbl_source(Path(environment["DSS_PATH"]))

    output = tmp_path / "raw"
    materialize_dbgen_tables(
        source_lock,
        tmp_path / "cache",
        output,
        downloader=downloader,
        command_runner=command_runner,
    )

    assert DBGEN_BUILD_COMMAND == (
        "make",
        "-f",
        "Makefile",
        "CC=gcc -std=gnu89",
        "DATABASE=ORACLE",
        "MACHINE=LINUX",
        "WORKLOAD=TPCH",
        "dbgen",
    )
    assert commands == [DBGEN_BUILD_COMMAND, DBGEN_GENERATE_COMMAND]
    assert parse_tbl_row("region", (output / "region.tbl").read_text().rstrip("\n"), 1)


def test_dbgen_command_uses_bounded_sf1_timeout(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    observed: dict[str, object] = {}

    def run_stub(*_args: object, **kwargs: object) -> None:
        observed.update(kwargs)

    monkeypatch.setattr(subprocess, "run", run_stub)

    _run_command(DBGEN_GENERATE_COMMAND, tmp_path, {"LC_ALL": "C"})

    assert DBGEN_SF1_TIMEOUT_SECONDS == 14_400
    assert observed["timeout"] == DBGEN_SF1_TIMEOUT_SECONDS


def test_materialization_wraps_command_output_and_cleans_partial_raw(tmp_path: Path) -> None:
    archive, source_lock = _synthetic_source_archive(tmp_path)

    def downloader(_url: str, destination: Path) -> None:
        shutil.copyfile(archive, destination)

    def command_runner(
        arguments: Sequence[str], cwd: Path, _environment: Mapping[str, str]
    ) -> None:
        command = tuple(arguments)
        if command == DBGEN_BUILD_COMMAND:
            (cwd / "dbgen").write_text("synthetic binary", encoding="utf-8")
            return
        raise subprocess.CalledProcessError(
            2,
            list(command),
            output="generated stdout",
            stderr="generated stderr",
        )

    output = tmp_path / "raw"
    with pytest.raises(SourceProvenanceError) as failure:
        materialize_dbgen_tables(
            source_lock,
            tmp_path / "cache",
            output,
            downloader=downloader,
            command_runner=command_runner,
        )

    message = str(failure.value)
    assert "./dbgen" in message
    assert "generated stdout" in message
    assert "generated stderr" in message
    assert not output.exists()


def test_materialization_wraps_timeout_and_cleans_partial_raw(tmp_path: Path) -> None:
    archive, source_lock = _synthetic_source_archive(tmp_path)

    def downloader(_url: str, destination: Path) -> None:
        shutil.copyfile(archive, destination)

    def command_runner(arguments: Sequence[str], cwd: Path, environment: Mapping[str, str]) -> None:
        command = tuple(arguments)
        if command == DBGEN_BUILD_COMMAND:
            (cwd / "dbgen").write_text("synthetic binary", encoding="utf-8")
            return
        raw = Path(environment["DSS_PATH"])
        (raw / "region.tbl").write_text("partial output", encoding="utf-8")
        raise subprocess.TimeoutExpired(
            list(command),
            DBGEN_SF1_TIMEOUT_SECONDS,
            output=b"partial stdout",
            stderr=b"generation deadline reached",
        )

    output = tmp_path / "raw"
    with pytest.raises(SourceProvenanceError) as failure:
        materialize_dbgen_tables(
            source_lock,
            tmp_path / "cache",
            output,
            downloader=downloader,
            command_runner=command_runner,
        )

    message = str(failure.value)
    assert "./dbgen" in message
    assert "partial stdout" in message
    assert "generation deadline reached" in message
    assert not output.exists()


def test_materialization_cleans_successful_but_incomplete_raw_output(tmp_path: Path) -> None:
    archive, source_lock = _synthetic_source_archive(tmp_path)

    def downloader(_url: str, destination: Path) -> None:
        shutil.copyfile(archive, destination)

    def command_runner(arguments: Sequence[str], cwd: Path, environment: Mapping[str, str]) -> None:
        if tuple(arguments) == DBGEN_BUILD_COMMAND:
            (cwd / "dbgen").write_text("synthetic binary", encoding="utf-8")
            return
        raw = Path(environment["DSS_PATH"])
        raw.mkdir(parents=True, exist_ok=True)
        (raw / "region.tbl").write_text("0|AFRICA|comment\n", encoding="utf-8")

    output = tmp_path / "raw"
    with pytest.raises(SourceProvenanceError, match="non-empty tables"):
        materialize_dbgen_tables(
            source_lock,
            tmp_path / "cache",
            output,
            downloader=downloader,
            command_runner=command_runner,
        )

    assert not output.exists()


def test_tpch_runtime_lock_requires_exact_python_version(tmp_path: Path) -> None:
    lock = tmp_path / "runtime-versions.lock"
    lock.write_text(
        json.dumps({"components": [{"name": "python", "version": "3.12"}]}),
        encoding="utf-8",
    )

    with pytest.raises(PrimaryTpchGateError, match="locked Python"):
        _locked_python_version(lock)


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
