"""Streaming, immutable Parquet dataset generation."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from data.generator.canonical import canonical_json_sha256
from data.generator.constants import (
    CANONICAL_ROW_ENCODING,
    CONTENT_HASH_ALGORITHM,
    DATASET_SCHEMA_VERSION,
    GENERATOR_NAME,
    GENERATOR_VERSION,
    PRIMARY_KEY_RANGE_SIZE,
    TABLE_ORDER,
)
from data.generator.manifest import TableAudit
from data.generator.profiles import GeneratorProfile
from data.generator.rows import iter_table_rows
from data.generator.schemas import PRIMARY_KEYS, TABLE_SCHEMAS, schema_sha256

_WRITE_BATCH_ROWS = 16_384
_PYTHON_VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")


@dataclass(frozen=True, slots=True)
class GenerationResult:
    dataset_dir: Path
    manifest_path: Path
    manifest: dict[str, Any]


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _discover_git_provenance() -> tuple[str, bool]:
    repository_root = Path(__file__).resolve().parents[2]
    try:
        commit_process = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repository_root,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        status_process = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=repository_root,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown", True
    return commit_process.stdout.strip(), bool(status_process.stdout.strip())


def _record_completed_file(
    dataset_root: Path,
    file_path: Path,
    row_count: int,
) -> dict[str, int | str]:
    return {
        "path": file_path.relative_to(dataset_root).as_posix(),
        "row_count": row_count,
        "size_bytes": file_path.stat().st_size,
        "sha256": _sha256_file(file_path),
    }


def _write_table(
    dataset_root: Path,
    table_name: str,
    profile: GeneratorProfile,
) -> dict[str, Any]:
    schema = TABLE_SCHEMAS[table_name]
    primary_key = PRIMARY_KEYS[table_name]
    rows_per_file = profile.rows_per_file[table_name]
    expected_count = profile.counts[table_name]
    table_dir = dataset_root / table_name
    table_dir.mkdir(parents=True, exist_ok=False)

    audit = TableAudit(schema, primary_key)
    files: list[dict[str, int | str]] = []
    pending_rows: list[dict[str, Any]] = []
    writer: pq.ParquetWriter | None = None
    current_file_path: Path | None = None
    current_file_rows = 0
    part_index = 0

    def open_writer() -> None:
        nonlocal writer, current_file_path, current_file_rows, part_index
        current_file_path = table_dir / f"part-{part_index:05d}.parquet"
        part_index += 1
        current_file_rows = 0
        writer = pq.ParquetWriter(
            current_file_path,
            schema,
            compression="snappy",
            use_dictionary=True,
            write_statistics=True,
            version="2.6",
            data_page_version="1.0",
            use_compliant_nested_type=True,
        )

    def flush_rows() -> None:
        if not pending_rows:
            return
        if writer is None:
            raise AssertionError("Parquet writer is not open")
        arrow_table = pa.Table.from_pylist(pending_rows, schema=schema)
        writer.write_table(arrow_table, row_group_size=len(pending_rows))
        pending_rows.clear()

    def close_writer() -> None:
        nonlocal writer, current_file_path, current_file_rows
        if writer is None or current_file_path is None:
            return
        flush_rows()
        writer.close()
        files.append(_record_completed_file(dataset_root, current_file_path, current_file_rows))
        writer = None
        current_file_path = None
        current_file_rows = 0

    try:
        for row in iter_table_rows(table_name, profile):
            if writer is None:
                open_writer()
            audit.add(row)
            pending_rows.append(row)
            current_file_rows += 1
            if len(pending_rows) >= _WRITE_BATCH_ROWS:
                flush_rows()
            if current_file_rows == rows_per_file:
                close_writer()
        close_writer()
    except BaseException:
        if writer is not None:
            writer.close()
        raise

    if audit.row_count != expected_count:
        raise AssertionError(
            f"{table_name} produced {audit.row_count} rows; expected {expected_count}"
        )
    audit_result = audit.finish()
    total_bytes = sum(int(file_record["size_bytes"]) for file_record in files)
    return {
        "primary_key": list(primary_key),
        "schema_sha256": schema_sha256(schema),
        **audit_result,
        "format": "parquet",
        "compression": "snappy",
        "file_count": len(files),
        "total_bytes": total_bytes,
        "files": files,
    }


def _build_manifest(
    profile: GeneratorProfile,
    tables: dict[str, dict[str, Any]],
    *,
    generator_git_commit: str,
    generator_worktree_dirty: bool,
    generator_python_version: str,
    generator_python_implementation: str,
) -> dict[str, Any]:
    normalized_profile = profile.as_canonical_mapping()
    return {
        "schema_version": DATASET_SCHEMA_VERSION,
        "dataset_id": profile.dataset_id,
        "generator": {
            "name": GENERATOR_NAME,
            "version": GENERATOR_VERSION,
            "git_commit": generator_git_commit,
            "worktree_dirty": generator_worktree_dirty,
            "python_version": generator_python_version,
            "python_implementation": generator_python_implementation,
        },
        "seed": profile.seed,
        "scale_profile": profile.profile_id,
        "skew_profile": profile.skew_profile,
        "benchmark_eligible": profile.benchmark_eligible,
        "timezone": profile.timezone,
        "currency": profile.currency,
        "rounding_mode": profile.rounding_mode,
        "generation_config_sha256": canonical_json_sha256(normalized_profile),
        "profile": normalized_profile,
        "content_hash_contract": {
            "algorithm": CONTENT_HASH_ALGORITHM,
            "canonical_row_encoding": CANONICAL_ROW_ENCODING,
            "primary_key_range_size": PRIMARY_KEY_RANGE_SIZE,
        },
        "storage": {
            "format": "parquet",
            "compression": "snappy",
            "pyarrow_version": pa.__version__,
        },
        "tables": tables,
    }


def generate_dataset(
    profile: GeneratorProfile,
    output_dir: str | Path,
    *,
    generator_git_commit: str | None = None,
    generator_worktree_dirty: bool | None = None,
    generator_python_version: str | None = None,
    generator_python_implementation: str | None = None,
) -> GenerationResult:
    """Generate a new immutable dataset directory.

    Existing output paths are rejected, including empty directories. Data is
    first written into a same-filesystem temporary directory and atomically
    renamed only after every table and the manifest are complete.
    """

    output_path = Path(output_dir).expanduser().resolve(strict=False)
    if output_path.exists():
        raise FileExistsError(f"immutable dataset output already exists: {output_path}")
    discovered_commit, discovered_dirty = _discover_git_provenance()
    effective_commit = generator_git_commit or discovered_commit
    effective_dirty = (
        discovered_dirty if generator_worktree_dirty is None else generator_worktree_dirty
    )
    if not effective_commit.strip():
        raise ValueError("generator_git_commit cannot be empty")
    if profile.benchmark_eligible and effective_dirty:
        raise ValueError("benchmark-eligible data requires a clean generator worktree")
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

    output_path.parent.mkdir(parents=True, exist_ok=True)

    temporary_path = Path(
        tempfile.mkdtemp(prefix=f".{output_path.name}.tmp-", dir=output_path.parent)
    )
    try:
        tables = {
            table_name: _write_table(temporary_path, table_name, profile)
            for table_name in TABLE_ORDER
        }
        manifest = _build_manifest(
            profile,
            tables,
            generator_git_commit=effective_commit,
            generator_worktree_dirty=effective_dirty,
            generator_python_version=effective_python_version,
            generator_python_implementation=effective_python_implementation,
        )
        temporary_manifest = temporary_path / "manifest.json"
        temporary_manifest.write_text(
            json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
            newline="\n",
        )
        os.replace(temporary_path, output_path)
    except BaseException:
        if temporary_path.exists():
            shutil.rmtree(temporary_path)
        raise

    return GenerationResult(
        dataset_dir=output_path,
        manifest_path=output_path / "manifest.json",
        manifest=manifest,
    )
