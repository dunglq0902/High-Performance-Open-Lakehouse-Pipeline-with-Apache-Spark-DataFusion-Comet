"""Deterministic DBGEN ``.tbl`` to validated Snappy Parquet conversion."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import shutil
import struct
import tempfile
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from data.tpch.contract import (
    DATE_RANGES,
    FOREIGN_KEYS,
    PRIMARY_KEYS,
    SF1_ROW_COUNTS,
    TABLE_ORDER,
    TPCH_SCHEMAS,
    schema_contract,
    schema_sha256,
)
from data.tpch.source import DBGEN_BUILD_COMMAND, DBGEN_GENERATE_COMMAND, sha256_file

CONVERTER_NAME = "data.tpch"
CONVERTER_VERSION = "1.0.1"
TARGET_FILE_SIZE_BYTES = 128 * 1024**2
ROW_GROUP_ROWS = 64 * 1024
NOTICE = "Derived from TPC-H DBGEN; this is not an audited TPC-H result."
SOURCE_TBL_FORMAT = {
    "encoding": "utf-8",
    "delimiter": "|",
    "trailing_delimiter": False,
    "record_terminator": "LF",
}
_GIT_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_PYTHON_VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")


class TpchContractError(RuntimeError):
    """A source row or converted dataset violates the reviewed TPC-H contract."""


@dataclass(frozen=True, slots=True)
class ValidationReport:
    row_counts: dict[str, int]
    date_bounds: dict[str, dict[str, list[str]]]
    foreign_key_checks: int


@dataclass(frozen=True, slots=True)
class DatasetBuildResult:
    dataset_dir: Path
    manifest_path: Path
    manifest: dict[str, Any]


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _manifest_hash(value: Mapping[str, object]) -> str:
    payload = dict(value)
    payload.pop("manifest_sha256", None)
    return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


def _parse_value(field: pa.Field, text: str, *, table: str, line_number: int) -> object:
    location = f"{table}.tbl:{line_number}:{field.name}"
    if "\x00" in text:
        raise TpchContractError(f"{location} contains a NUL byte")
    try:
        if pa.types.is_int64(field.type) or pa.types.is_int32(field.type):
            integer = int(text)
            if str(integer) != text and not (text.startswith("+") and str(integer) == text[1:]):
                raise ValueError("non-canonical integer")
            if pa.types.is_int32(field.type) and not -(2**31) <= integer < 2**31:
                raise ValueError("integer exceeds INT32")
            return integer
        if pa.types.is_decimal(field.type):
            decimal_value = Decimal(text)
            if not decimal_value.is_finite():
                raise ValueError("decimal is not finite")
            quantum = Decimal(1).scaleb(-field.type.scale)
            if decimal_value.quantize(quantum) != decimal_value:
                raise ValueError(f"decimal has more than {field.type.scale} fractional digits")
            return decimal_value.quantize(quantum)
        if pa.types.is_date32(field.type):
            return date.fromisoformat(text)
        if pa.types.is_string(field.type):
            return text
    except (InvalidOperation, OverflowError, ValueError) as error:
        raise TpchContractError(f"{location} has invalid {field.type}: {text!r}") from error
    raise TpchContractError(f"{location} uses unsupported type {field.type}")


def parse_tbl_row(table_name: str, raw_line: str, line_number: int) -> dict[str, object]:
    if table_name not in TPCH_SCHEMAS:
        raise TpchContractError(f"unknown TPC-H table: {table_name}")
    line = raw_line.rstrip("\r\n")
    schema = TPCH_SCHEMAS[table_name]
    values = line.split("|")
    if len(values) != len(schema) and line.endswith("|"):
        values = line[:-1].split("|")
    if len(values) != len(schema):
        raise TpchContractError(
            f"{table_name}.tbl:{line_number} has {len(values)} columns; expected {len(schema)}"
        )
    return {
        field.name: _parse_value(field, value, table=table_name, line_number=line_number)
        for field, value in zip(schema, values, strict=True)
    }


def iter_tbl_rows(path: Path, table_name: str) -> Iterator[dict[str, object]]:
    with path.open("r", encoding="utf-8", newline="") as source:
        for line_number, raw_line in enumerate(source, 1):
            if not raw_line:
                continue
            yield parse_tbl_row(table_name, raw_line, line_number)


def _encode_scalar(field: pa.Field, value: object) -> bytes:
    if pa.types.is_integer(field.type):
        if isinstance(value, bool) or not isinstance(value, int):
            raise TpchContractError(f"{field.name} is not an integer")
        return b"I" + str(value).encode("ascii")
    if pa.types.is_decimal(field.type):
        if not isinstance(value, Decimal):
            raise TpchContractError(f"{field.name} is not a decimal")
        return b"D" + f"{value:.{field.type.scale}f}".encode("ascii")
    if pa.types.is_date32(field.type):
        if not isinstance(value, date):
            raise TpchContractError(f"{field.name} is not a date")
        return b"A" + value.isoformat().encode("ascii")
    if pa.types.is_string(field.type):
        if not isinstance(value, str):
            raise TpchContractError(f"{field.name} is not a string")
        return b"S" + value.encode("utf-8")
    raise TpchContractError(f"unsupported canonical field type: {field.type}")


class _TableAudit:
    def __init__(self, table_name: str) -> None:
        self.table_name = table_name
        self.schema = TPCH_SCHEMAS[table_name]
        self.primary_key = PRIMARY_KEYS[table_name]
        self.row_count = 0
        self.previous_key: tuple[int, ...] | None = None
        self.digest = hashlib.sha256(b"tpch-canonical-table-v1\0" + table_name.encode("ascii"))
        self.date_minimum: dict[str, date] = {}
        self.date_maximum: dict[str, date] = {}

    def add(self, row: Mapping[str, object]) -> None:
        raw_key = tuple(row[column] for column in self.primary_key)
        key_values: list[int] = []
        for value in raw_key:
            if not isinstance(value, int) or isinstance(value, bool):
                raise TpchContractError(f"{self.table_name} primary key is not integral")
            key_values.append(value)
        key = tuple(key_values)
        if any(value < 0 for value in key):
            raise TpchContractError(f"{self.table_name} primary key contains a negative value")
        if self.previous_key is not None and key <= self.previous_key:
            raise TpchContractError(
                f"{self.table_name} primary key is duplicate or not ordered: {key}"
            )
        self.previous_key = key

        encoded = bytearray()
        for field in self.schema:
            scalar = _encode_scalar(field, row[field.name])
            encoded.extend(struct.pack(">I", len(scalar)))
            encoded.extend(scalar)
            if pa.types.is_date32(field.type):
                value = row[field.name]
                if not isinstance(value, date):
                    raise TpchContractError(f"{field.name} is not a date")
                self.date_minimum[field.name] = min(self.date_minimum.get(field.name, value), value)
                self.date_maximum[field.name] = max(self.date_maximum.get(field.name, value), value)
        self.digest.update(struct.pack(">Q", len(encoded)))
        self.digest.update(encoded)
        self.row_count += 1

    def finish(self) -> dict[str, object]:
        if self.row_count == 0:
            raise TpchContractError(f"{self.table_name}.tbl is empty")
        date_bounds = {
            field: [self.date_minimum[field].isoformat(), self.date_maximum[field].isoformat()]
            for field in sorted(self.date_minimum)
        }
        return {
            "row_count": self.row_count,
            "content_sha256": self.digest.hexdigest(),
            "content_hash_algorithm": "sha256-length-prefixed-schema-order-v1",
            "date_bounds": date_bounds,
        }


def _parquet_file_record(dataset_root: Path, path: Path) -> dict[str, int | str]:
    metadata = pq.ParquetFile(path).metadata
    return {
        "path": path.relative_to(dataset_root).as_posix(),
        "row_count": metadata.num_rows,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def _convert_table(
    dataset_root: Path,
    raw_path: Path,
    table_name: str,
    expected_rows: int,
    *,
    target_file_size_bytes: int,
    row_group_rows: int,
) -> dict[str, Any]:
    if target_file_size_bytes < 1 or row_group_rows < 1:
        raise ValueError("Parquet file and row-group targets must be positive")
    schema = TPCH_SCHEMAS[table_name]
    table_dir = dataset_root / table_name
    table_dir.mkdir(parents=True, exist_ok=False)
    audit = _TableAudit(table_name)
    files: list[dict[str, int | str]] = []
    pending: list[dict[str, object]] = []
    writer: pq.ParquetWriter | None = None
    current_path: Path | None = None
    part_index = 0

    def open_writer() -> None:
        nonlocal writer, current_path, part_index
        current_path = table_dir / f"part-{part_index:05d}.parquet"
        part_index += 1
        writer = pq.ParquetWriter(
            current_path,
            schema,
            compression="snappy",
            use_dictionary=True,
            write_statistics=True,
            version="2.6",
            data_page_version="1.0",
            use_compliant_nested_type=True,
        )

    def close_writer() -> None:
        nonlocal writer, current_path
        if writer is None or current_path is None:
            return
        writer.close()
        files.append(_parquet_file_record(dataset_root, current_path))
        writer = None
        current_path = None

    def flush() -> None:
        if not pending:
            return
        if writer is None:
            open_writer()
        if writer is None or current_path is None:
            raise AssertionError("Parquet writer was not opened")
        batch = pa.Table.from_pylist(pending, schema=schema)
        writer.write_table(batch, row_group_size=len(pending))
        pending.clear()
        if current_path.stat().st_size >= target_file_size_bytes:
            close_writer()

    try:
        for row in iter_tbl_rows(raw_path, table_name):
            audit.add(row)
            pending.append(row)
            if len(pending) == row_group_rows:
                flush()
        flush()
        close_writer()
    except BaseException:
        if writer is not None:
            writer.close()
        raise

    audit_result = audit.finish()
    if audit.row_count != expected_rows:
        raise TpchContractError(
            f"{table_name} row count {audit.row_count} does not match expected {expected_rows}"
        )
    return {
        "primary_key": list(PRIMARY_KEYS[table_name]),
        "foreign_keys": [
            {
                "columns": list(foreign_key.columns),
                "parent_table": foreign_key.parent_table,
                "parent_columns": list(foreign_key.parent_columns),
            }
            for foreign_key in FOREIGN_KEYS[table_name]
        ],
        "schema": schema_contract(schema),
        "schema_sha256": schema_sha256(schema),
        **audit_result,
        "source_tbl": {
            "path": raw_path.relative_to(dataset_root).as_posix(),
            "size_bytes": raw_path.stat().st_size,
            "sha256": sha256_file(raw_path),
        },
        "format": "parquet",
        "compression": "snappy",
        "file_count": len(files),
        "total_bytes": sum(int(record["size_bytes"]) for record in files),
        "files": files,
    }


def _batch_rows(batch: pa.RecordBatch, columns: tuple[str, ...]) -> Iterator[tuple[object, ...]]:
    values = [batch.column(batch.schema.get_field_index(column)).to_pylist() for column in columns]
    yield from zip(*values, strict=True)


def _validate_parquet_tables(
    dataset_root: Path,
    expected_counts: Mapping[str, int],
) -> ValidationReport:
    if set(expected_counts) != set(TABLE_ORDER):
        raise TpchContractError("expected row counts must cover all eight TPC-H tables")
    parent_keys: dict[str, set[tuple[object, ...]]] = {}
    row_counts: dict[str, int] = {}
    all_date_bounds: dict[str, dict[str, list[str]]] = {}
    foreign_key_checks = 0
    referenced_parents = {
        foreign_key.parent_table
        for foreign_keys in FOREIGN_KEYS.values()
        for foreign_key in foreign_keys
    }

    for table_name in TABLE_ORDER:
        paths = sorted((dataset_root / table_name).glob("part-*.parquet"))
        if not paths:
            raise TpchContractError(f"{table_name} has no Parquet files")
        schema = TPCH_SCHEMAS[table_name]
        previous_key: tuple[object, ...] | None = None
        keys: set[tuple[object, ...]] = set()
        count = 0
        date_minimum: dict[str, date] = {}
        date_maximum: dict[str, date] = {}
        required_columns = tuple(
            dict.fromkeys(
                (
                    *PRIMARY_KEYS[table_name],
                    *(column for key in FOREIGN_KEYS[table_name] for column in key.columns),
                    *(field.name for field in schema if pa.types.is_date32(field.type)),
                )
            )
        )

        for path in paths:
            parquet = pq.ParquetFile(path)
            if not parquet.schema_arrow.equals(schema, check_metadata=False):
                raise TpchContractError(f"{path} does not match the explicit TPC-H schema")
            for batch in parquet.iter_batches(
                columns=list(required_columns), batch_size=ROW_GROUP_ROWS
            ):
                batch_columns = {
                    name: batch.column(batch.schema.get_field_index(name)).to_pylist()
                    for name in required_columns
                }
                for row_index in range(batch.num_rows):
                    primary_key = tuple(
                        batch_columns[column][row_index] for column in PRIMARY_KEYS[table_name]
                    )
                    if previous_key is not None and primary_key <= previous_key:
                        raise TpchContractError(
                            f"{table_name} primary key is duplicate or not ordered: {primary_key}"
                        )
                    previous_key = primary_key
                    if table_name in referenced_parents:
                        keys.add(primary_key)
                    for foreign_key in FOREIGN_KEYS[table_name]:
                        local_key = tuple(
                            batch_columns[column][row_index] for column in foreign_key.columns
                        )
                        if local_key not in parent_keys[foreign_key.parent_table]:
                            raise TpchContractError(
                                f"{table_name} foreign key {foreign_key.columns} is orphan: "
                                f"{local_key}"
                            )
                        foreign_key_checks += 1
                    for field in schema:
                        if not pa.types.is_date32(field.type):
                            continue
                        value = batch_columns[field.name][row_index]
                        if not isinstance(value, date):
                            raise TpchContractError(f"{table_name}.{field.name} is not a date")
                        lower, upper = DATE_RANGES[(table_name, field.name)]
                        if not lower <= value <= upper:
                            raise TpchContractError(
                                f"{table_name}.{field.name} is outside [{lower}, {upper}]: {value}"
                            )
                        date_minimum[field.name] = min(date_minimum.get(field.name, value), value)
                        date_maximum[field.name] = max(date_maximum.get(field.name, value), value)
                    if table_name == "lineitem":
                        ship = batch_columns["l_shipdate"][row_index]
                        receipt = batch_columns["l_receiptdate"][row_index]
                        if (
                            not isinstance(ship, date)
                            or not isinstance(receipt, date)
                            or ship > receipt
                        ):
                            raise TpchContractError("lineitem ship date is after receipt date")
                count += batch.num_rows
        if count != expected_counts[table_name]:
            raise TpchContractError(
                f"{table_name} Parquet row count {count} does not match "
                f"{expected_counts[table_name]}"
            )
        row_counts[table_name] = count
        if table_name in referenced_parents:
            parent_keys[table_name] = keys
        all_date_bounds[table_name] = {
            field: [date_minimum[field].isoformat(), date_maximum[field].isoformat()]
            for field in sorted(date_minimum)
        }
    return ValidationReport(row_counts, all_date_bounds, foreign_key_checks)


def build_dataset_from_tbl(
    source_tbl_dir: Path,
    output_dir: Path,
    *,
    source_provenance: Mapping[str, str],
    generator_git_commit: str,
    generator_python_version: str | None = None,
    generator_python_implementation: str | None = None,
    expected_counts: Mapping[str, int] = SF1_ROW_COUNTS,
    benchmark_eligible: bool = True,
    target_file_size_bytes: int = TARGET_FILE_SIZE_BYTES,
    row_group_rows: int = ROW_GROUP_ROWS,
) -> DatasetBuildResult:
    """Atomically convert one complete DBGEN directory into immutable Parquet."""

    if output_dir.exists():
        raise FileExistsError(f"immutable TPC-H output already exists: {output_dir}")
    if _GIT_COMMIT.fullmatch(generator_git_commit) is None:
        raise ValueError("generator_git_commit must be a full Git object ID")
    effective_python_version = (
        platform.python_version() if generator_python_version is None else generator_python_version
    )
    effective_python_implementation = (
        platform.python_implementation()
        if generator_python_implementation is None
        else generator_python_implementation
    )
    if _PYTHON_VERSION.fullmatch(effective_python_version) is None:
        raise ValueError("generator_python_version must be an exact MAJOR.MINOR.PATCH version")
    if effective_python_implementation != "CPython":
        raise ValueError("generator_python_implementation must be 'CPython'")
    if set(source_provenance) < {
        "name",
        "version",
        "commit",
        "archive_url",
        "archive_sha256",
        "source_url",
        "license",
    }:
        raise ValueError("source provenance is incomplete")
    missing = [
        table_name
        for table_name in TABLE_ORDER
        if not (source_tbl_dir / f"{table_name}.tbl").is_file()
    ]
    if missing:
        raise TpchContractError(f"source DBGEN directory misses tables: {missing}")

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.tmp-", dir=output_dir.parent))
    try:
        raw_dir = temporary / "raw"
        raw_dir.mkdir()
        for table_name in TABLE_ORDER:
            shutil.copyfile(
                source_tbl_dir / f"{table_name}.tbl",
                raw_dir / f"{table_name}.tbl",
            )
        tables = {
            table_name: _convert_table(
                temporary,
                raw_dir / f"{table_name}.tbl",
                table_name,
                expected_counts[table_name],
                target_file_size_bytes=target_file_size_bytes,
                row_group_rows=row_group_rows,
            )
            for table_name in TABLE_ORDER
        }
        validation = _validate_parquet_tables(temporary, expected_counts)
        manifest: dict[str, Any] = {
            "schema_version": 1,
            "dataset_id": f"tpch-derived-sf1-{source_provenance['commit'][:12]}-v1",
            "notice": NOTICE,
            "scale_profile": "sf1",
            "scale_factor": 1,
            "benchmark_eligible": benchmark_eligible,
            "generator": {
                "name": f"tpch-dbgen+{CONVERTER_NAME}",
                "version": CONVERTER_VERSION,
                "git_commit": generator_git_commit,
                "worktree_dirty": False,
                "python_version": effective_python_version,
                "python_implementation": effective_python_implementation,
            },
            "source": dict(source_provenance),
            "generation": {
                "build_command": list(DBGEN_BUILD_COMMAND),
                "dbgen_command": list(DBGEN_GENERATE_COMMAND),
                "locale": "C",
                "timezone": "UTC",
                "source_tbl_format": dict(SOURCE_TBL_FORMAT),
            },
            "storage": {
                "profile": "tpch_parquet",
                "format": "parquet",
                "compression": "snappy",
                "target_file_size_bytes": target_file_size_bytes,
                "row_group_rows": row_group_rows,
                "parquet_version": "2.6",
                "data_page_version": "1.0",
                "pyarrow_version": pa.__version__,
                "partitioning": "unpartitioned",
            },
            "validation": {
                "status": "passed",
                "row_counts": validation.row_counts,
                "date_bounds": validation.date_bounds,
                "foreign_key_checks": validation.foreign_key_checks,
                "primary_keys": "strictly-increasing",
            },
            "tables": tables,
        }
        manifest["manifest_sha256"] = _manifest_hash(manifest)
        (temporary / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
            newline="\n",
        )
        os.replace(temporary, output_dir)
    except BaseException:
        if temporary.exists():
            shutil.rmtree(temporary)
        raise
    return DatasetBuildResult(output_dir, output_dir / "manifest.json", manifest)


def _safe_manifest_file(dataset_root: Path, relative_path: object) -> Path:
    if not isinstance(relative_path, str) or not relative_path:
        raise TpchContractError("manifest file path is absent or invalid")
    path = (dataset_root / relative_path).resolve()
    try:
        path.relative_to(dataset_root.resolve())
    except ValueError as error:
        raise TpchContractError(f"manifest file leaves dataset root: {relative_path}") from error
    return path


def validate_tpch_dataset(
    dataset_root: Path,
    *,
    expected_counts: Mapping[str, int] = SF1_ROW_COUNTS,
    expected_source: Mapping[str, str] | None = None,
    expected_python_version: str | None = None,
    require_benchmark_eligible: bool = True,
) -> ValidationReport:
    manifest_path = dataset_root / "manifest.json"
    try:
        manifest: object = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise TpchContractError(f"cannot read TPC-H manifest: {manifest_path}") from error
    if not isinstance(manifest, dict):
        raise TpchContractError("TPC-H manifest root is not an object")
    declared_hash = manifest.get("manifest_sha256")
    if not isinstance(declared_hash, str) or declared_hash != _manifest_hash(manifest):
        raise TpchContractError("TPC-H manifest self-hash is invalid")
    if manifest.get("schema_version") != 1 or manifest.get("scale_factor") != 1:
        raise TpchContractError("TPC-H manifest is not the reviewed SF1 contract")
    if manifest.get("notice") != NOTICE:
        raise TpchContractError("TPC-H derived/non-audited notice is missing")
    if require_benchmark_eligible and manifest.get("benchmark_eligible") is not True:
        raise TpchContractError("TPC-H dataset is not benchmark eligible")
    generator = manifest.get("generator")
    if (
        not isinstance(generator, dict)
        or generator.get("name") != f"tpch-dbgen+{CONVERTER_NAME}"
        or generator.get("version") != CONVERTER_VERSION
        or not isinstance(generator.get("git_commit"), str)
        or _GIT_COMMIT.fullmatch(generator["git_commit"]) is None
        or generator.get("worktree_dirty") is not False
        or (
            require_benchmark_eligible
            and (
                not isinstance(generator.get("python_version"), str)
                or _PYTHON_VERSION.fullmatch(generator["python_version"]) is None
                or generator.get("python_implementation") != "CPython"
            )
        )
    ):
        raise TpchContractError("TPC-H generator provenance is missing or dirty")
    if (
        expected_python_version is not None
        and generator.get("python_version") != expected_python_version
    ):
        raise TpchContractError("TPC-H generator Python differs from runtime lock")
    expected_generation = {
        "build_command": list(DBGEN_BUILD_COMMAND),
        "dbgen_command": list(DBGEN_GENERATE_COMMAND),
        "locale": "C",
        "timezone": "UTC",
        "source_tbl_format": dict(SOURCE_TBL_FORMAT),
    }
    if manifest.get("generation") != expected_generation:
        raise TpchContractError("TPC-H generation metadata is not the reviewed contract")
    source = manifest.get("source")
    if not isinstance(source, dict):
        raise TpchContractError("TPC-H source provenance is missing")
    if expected_source is not None and source != dict(expected_source):
        raise TpchContractError("TPC-H source provenance differs from runtime lock")
    tables = manifest.get("tables")
    if not isinstance(tables, dict) or set(tables) != set(TABLE_ORDER):
        raise TpchContractError("TPC-H manifest must describe exactly eight tables")

    for table_name in TABLE_ORDER:
        record = tables[table_name]
        if not isinstance(record, dict):
            raise TpchContractError(f"TPC-H table manifest is invalid: {table_name}")
        if record.get("schema") != schema_contract(TPCH_SCHEMAS[table_name]):
            raise TpchContractError(f"{table_name} manifest schema is invalid")
        if record.get("schema_sha256") != schema_sha256(TPCH_SCHEMAS[table_name]):
            raise TpchContractError(f"{table_name} manifest schema hash is invalid")
        source_record = record.get("source_tbl")
        if not isinstance(source_record, dict):
            raise TpchContractError(f"{table_name} source .tbl record is absent")
        raw_path = _safe_manifest_file(dataset_root, source_record.get("path"))
        if not raw_path.is_file() or sha256_file(raw_path) != source_record.get("sha256"):
            raise TpchContractError(f"{table_name} source .tbl hash is invalid")
        file_records = record.get("files")
        if not isinstance(file_records, list) or not file_records:
            raise TpchContractError(f"{table_name} has no manifest Parquet files")
        declared_paths: list[Path] = []
        for file_record in file_records:
            if not isinstance(file_record, dict):
                raise TpchContractError(f"{table_name} Parquet record is invalid")
            file_path = _safe_manifest_file(dataset_root, file_record.get("path"))
            declared_paths.append(file_path)
            if not file_path.is_file() or sha256_file(file_path) != file_record.get("sha256"):
                raise TpchContractError(f"{table_name} Parquet hash is invalid: {file_path}")
            if file_path.stat().st_size != file_record.get("size_bytes"):
                raise TpchContractError(f"{table_name} Parquet size is invalid: {file_path}")
        actual_paths = sorted((dataset_root / table_name).glob("part-*.parquet"))
        if sorted(declared_paths) != actual_paths:
            raise TpchContractError(f"{table_name} manifest and physical Parquet files differ")

    report = _validate_parquet_tables(dataset_root, expected_counts)
    validation = manifest.get("validation")
    if not isinstance(validation, dict) or validation.get("status") != "passed":
        raise TpchContractError("TPC-H manifest validation status is not passed")
    if validation.get("row_counts") != report.row_counts:
        raise TpchContractError("TPC-H manifest row counts differ from Parquet")
    if validation.get("date_bounds") != report.date_bounds:
        raise TpchContractError("TPC-H manifest date bounds differ from Parquet")
    return report
