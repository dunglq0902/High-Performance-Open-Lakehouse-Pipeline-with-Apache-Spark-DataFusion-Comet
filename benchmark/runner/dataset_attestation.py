"""Content-bound attestations for reusing an expensive dataset validation.

An attestation is a trusted local cache of a successful full semantic validation, not a
cryptographic signature or a claim made by an external authority. Reuse still hashes the exact
runtime Parquet inventory and, for TPC-H, every source ``.tbl`` file, so changing physical data
never turns into a cache hit merely because file names, sizes, or timestamps are unchanged.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import pyarrow as pa  # type: ignore[import-untyped]
from jsonschema import Draft202012Validator

from benchmark.runner.canonical import sha256_file, sha256_value, write_json
from benchmark.runner.evidence import (
    RepositoryEvidenceError,
    clean_git_commit,
    isolated_git_environment,
    validate_git_object_graph,
)
from data.generator.constants import TABLE_ORDER as ECOMMERCE_TABLE_ORDER
from data.generator.validation import (
    DatasetValidationError,
    validate_dataset,
)
from data.generator.validation import (
    ValidationReport as EcommerceValidationReport,
)
from data.tpch.contract import TABLE_ORDER as TPCH_TABLE_ORDER
from data.tpch.dataset import (
    TpchContractError,
    validate_tpch_dataset,
)
from data.tpch.dataset import (
    ValidationReport as TpchValidationReport,
)
from data.tpch.source import SourceProvenanceError, load_source_lock

DatasetSuite = Literal["ecommerce", "tpch"]

_SCHEMA_ROOT = Path(__file__).resolve().parents[1] / "schemas"
_ATTESTATION_SCHEMA_PATHS = {
    ("dataset-validation-attestation-v1", 1): (
        _SCHEMA_ROOT / "dataset-validation-attestation.schema.json"
    ),
    ("dataset-validation-attestation-v2", 2): (
        _SCHEMA_ROOT / "dataset-validation-attestation-v2.schema.json"
    ),
}
_SEMANTIC_TREE_PATHS: dict[DatasetSuite, str] = {
    "ecommerce": "data/generator",
    "tpch": "data/tpch",
}
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_GIT_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_PYTHON_VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")


class DatasetAttestationError(ValueError):
    """An attestation or its current physical dataset identity is invalid."""


class DatasetAttestationNotReusable(DatasetAttestationError):
    """A prior full attestation cannot safely be rebound to the current commit."""


@dataclass(frozen=True, slots=True)
class VerifiedDataset:
    """Identity returned only after full validation or content-bound verification."""

    suite: DatasetSuite
    dataset_id: str
    manifest_path: Path
    manifest_sha256: str
    content_identity_sha256: str
    attestation_path: Path
    attestation_file_sha256: str
    attestation_sha256: str
    git_commit: str
    expected_python_version: str
    file_count: int
    total_bytes: int


@dataclass(frozen=True, slots=True)
class _DatasetObservation:
    manifest: dict[str, Any]
    suite: DatasetSuite
    dataset_id: str
    manifest_relative: str
    manifest_sha256: str
    parquet_inventory: list[dict[str, int | str]]
    source_inventory: list[dict[str, int | str]]
    content_identity_sha256: str


def _validate_expected_identity(expected_python_version: str, git_commit: str) -> None:
    if _PYTHON_VERSION.fullmatch(expected_python_version) is None:
        raise DatasetAttestationError(
            f"expected Python version is not exact X.Y.Z: {expected_python_version!r}"
        )
    if _GIT_COMMIT.fullmatch(git_commit) is None:
        raise DatasetAttestationError(
            f"Git commit is not a full lowercase object ID: {git_commit!r}"
        )
    if platform.python_implementation() != "CPython":
        raise DatasetAttestationError("dataset attestation requires CPython")
    if platform.python_version() != expected_python_version:
        raise DatasetAttestationError(
            "attestation Python differs from the locked runtime: "
            f"current={platform.python_version()!r}, expected={expected_python_version!r}"
        )


def _require_clean_commit(repository_root: Path, expected_git_commit: str, *, phase: str) -> None:
    try:
        current_git_commit = clean_git_commit(repository_root)
    except RepositoryEvidenceError as error:
        raise DatasetAttestationError(f"repository provenance failed {phase}: {error}") from error
    if current_git_commit != expected_git_commit:
        raise DatasetAttestationError(
            f"Git HEAD changed {phase}: current={current_git_commit}, "
            f"expected={expected_git_commit}"
        )


def _validation_result_sha256(
    report: EcommerceValidationReport | TpchValidationReport,
    suite: DatasetSuite,
) -> str:
    """Hash only canonical semantic results, never validator-local filesystem paths."""

    if suite == "ecommerce":
        if not isinstance(report, EcommerceValidationReport):
            raise DatasetAttestationError("E-commerce validator returned an unexpected result")
        result: dict[str, Any] = {
            "suite": suite,
            "dataset_id": report.dataset_id,
            "table_row_counts": report.table_row_counts,
            "table_content_sha256": report.table_content_sha256,
        }
    else:
        if not isinstance(report, TpchValidationReport):
            raise DatasetAttestationError("TPC-H validator returned an unexpected result")
        result = {
            "suite": suite,
            "row_counts": report.row_counts,
            "date_bounds": report.date_bounds,
            "foreign_key_checks": report.foreign_key_checks,
        }
    return sha256_value(result)


def _repo_path(root: Path, path: Path, *, label: str) -> Path:
    repository_root = root.expanduser().resolve()
    candidate = path.expanduser()
    if not candidate.is_absolute():
        candidate = repository_root / candidate
    candidate = Path(os.path.abspath(candidate))
    try:
        relative = candidate.relative_to(repository_root)
    except ValueError as error:
        raise DatasetAttestationError(f"{label} leaves repository root: {path}") from error
    cursor = repository_root
    for part in relative.parts:
        cursor /= part
        if cursor.is_symlink():
            raise DatasetAttestationError(f"{label} contains a symlink: {cursor}")
    return candidate


def _canonical_repo_path(root: Path, declared: object, *, label: str) -> Path:
    if not isinstance(declared, str) or not declared or "\\" in declared:
        raise DatasetAttestationError(f"{label} must be a canonical repository-relative path")
    relative = Path(declared)
    if relative.is_absolute() or ".." in relative.parts or relative.as_posix() != declared:
        raise DatasetAttestationError(f"{label} must be a canonical repository-relative path")
    return _repo_path(root, relative, label=label)


def _git_output(repository_root: Path, arguments: list[str], *, label: str) -> str:
    try:
        validate_git_object_graph(repository_root)
        process = subprocess.run(
            ["git", *arguments],
            cwd=repository_root,
            env=isolated_git_environment(),
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        validate_git_object_graph(repository_root)
    except (OSError, subprocess.SubprocessError, RepositoryEvidenceError) as error:
        raise DatasetAttestationError(f"cannot resolve {label}: {error}") from error
    output = process.stdout.strip()
    if not output:
        raise DatasetAttestationError(f"cannot resolve {label}: Git returned no value")
    return output


def _git_tree_oid(repository_root: Path, commit: str, tree_path: str) -> str:
    if _GIT_COMMIT.fullmatch(commit) is None or tree_path not in _SEMANTIC_TREE_PATHS.values():
        raise DatasetAttestationError("semantic validator Git tree identity is invalid")
    resolved_commit = _git_output(
        repository_root,
        ["rev-parse", "--verify", f"{commit}^{{commit}}"],
        label="semantic validator commit",
    )
    if resolved_commit != commit:
        raise DatasetAttestationError("semantic validator commit does not resolve exactly")
    object_id = _git_output(
        repository_root,
        ["rev-parse", "--verify", f"{commit}:{tree_path}"],
        label=f"semantic validator tree {tree_path}",
    )
    if _GIT_COMMIT.fullmatch(object_id) is None:
        raise DatasetAttestationError("semantic validator tree object ID is invalid")
    object_type = _git_output(
        repository_root,
        ["cat-file", "-t", object_id],
        label=f"semantic validator tree type {tree_path}",
    )
    if object_type != "tree":
        raise DatasetAttestationError(f"semantic validator object is not a tree: {tree_path}")
    return object_id


def _is_ancestor(repository_root: Path, ancestor: str, descendant: str) -> bool:
    if _GIT_COMMIT.fullmatch(ancestor) is None or _GIT_COMMIT.fullmatch(descendant) is None:
        raise DatasetAttestationError("Git ancestry identity is invalid")
    try:
        validate_git_object_graph(repository_root)
        process = subprocess.run(
            ["git", "merge-base", "--is-ancestor", ancestor, descendant],
            cwd=repository_root,
            env=isolated_git_environment(),
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        validate_git_object_graph(repository_root)
    except (OSError, subprocess.SubprocessError, RepositoryEvidenceError) as error:
        raise DatasetAttestationError(f"cannot verify attestation Git ancestry: {error}") from error
    if process.returncode not in (0, 1):
        message = process.stderr.strip() or f"exit {process.returncode}"
        raise DatasetAttestationError(f"cannot verify attestation Git ancestry: {message}")
    return process.returncode == 0


def _ancestor_commits(repository_root: Path, commit: str) -> tuple[str, ...]:
    if _GIT_COMMIT.fullmatch(commit) is None:
        raise DatasetAttestationError("current Git commit is invalid")
    output = _git_output(
        repository_root,
        ["rev-list", "--topo-order", commit],
        label="attestation ancestor commits",
    )
    commits = tuple(line for line in output.splitlines() if line)
    if not commits or any(_GIT_COMMIT.fullmatch(item) is None for item in commits):
        raise DatasetAttestationError("Git returned an invalid attestation ancestry")
    return commits


def _load_object(path: Path, *, label: str) -> dict[str, Any]:
    try:
        value: object = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise DatasetAttestationError(f"cannot read {label} {path}: {error}") from error
    if not isinstance(value, dict):
        raise DatasetAttestationError(f"{label} root must be an object: {path}")
    return value


def _suite_from_manifest(manifest: dict[str, Any]) -> DatasetSuite:
    tables = manifest.get("tables")
    if not isinstance(tables, dict):
        raise DatasetAttestationError("dataset manifest tables must be an object")
    names = set(tables)
    if names == set(ECOMMERCE_TABLE_ORDER):
        return "ecommerce"
    if names == set(TPCH_TABLE_ORDER):
        return "tpch"
    raise DatasetAttestationError("dataset manifest does not describe a reviewed workload suite")


def _integer(value: object, *, label: str, minimum: int = 0) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise DatasetAttestationError(f"{label} must be an integer >= {minimum}")
    return value


def _stable_sha256(path: Path) -> str:
    before = path.stat()
    digest = sha256_file(path)
    after = path.stat()
    before_identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    after_identity = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if before_identity != after_identity:
        raise DatasetAttestationError(f"dataset file changed while it was hashed: {path}")
    return digest


def _dataset_file(dataset_root: Path, relative_value: object, *, table: str) -> Path:
    if not isinstance(relative_value, str) or not relative_value or "\\" in relative_value:
        raise DatasetAttestationError(f"{table} manifest file path is invalid")
    relative = Path(relative_value)
    if relative.is_absolute() or ".." in relative.parts:
        raise DatasetAttestationError(f"dataset file path escapes root: {relative_value!r}")
    if not relative.parts or relative.parts[0] != table or relative.suffix != ".parquet":
        raise DatasetAttestationError(
            f"{table} runtime file must be a Parquet path inside its table directory: "
            f"{relative_value!r}"
        )
    candidate = Path(os.path.abspath(dataset_root / relative))
    try:
        candidate.relative_to(dataset_root)
    except ValueError as error:
        raise DatasetAttestationError(
            f"dataset file resolves outside root: {relative_value!r}"
        ) from error
    cursor = dataset_root
    for part in relative.parts:
        cursor /= part
        if cursor.is_symlink():
            raise DatasetAttestationError(f"dataset file path contains a symlink: {cursor}")
    return candidate


def _source_file(dataset_root: Path, relative_value: object, *, table: str) -> Path:
    if not isinstance(relative_value, str) or not relative_value or "\\" in relative_value:
        raise DatasetAttestationError(f"{table} source file path is invalid")
    relative = Path(relative_value)
    expected_relative = f"raw/{table}.tbl"
    if relative.is_absolute() or ".." in relative.parts or relative.as_posix() != expected_relative:
        raise DatasetAttestationError(f"TPC-H source file path is invalid: {relative_value!r}")
    candidate = Path(os.path.abspath(dataset_root / relative))
    try:
        candidate.relative_to(dataset_root)
    except ValueError as error:
        raise DatasetAttestationError(
            f"TPC-H source file resolves outside dataset: {relative_value!r}"
        ) from error
    cursor = dataset_root
    for part in relative.parts:
        cursor /= part
        if cursor.is_symlink():
            raise DatasetAttestationError(f"TPC-H source file path contains a symlink: {cursor}")
    return candidate


def _runtime_inventory(
    dataset_root: Path, manifest: dict[str, Any], suite: DatasetSuite
) -> list[dict[str, int | str]]:
    table_order = ECOMMERCE_TABLE_ORDER if suite == "ecommerce" else TPCH_TABLE_ORDER
    raw_tables = manifest.get("tables")
    if not isinstance(raw_tables, dict) or set(raw_tables) != set(table_order):
        raise DatasetAttestationError(f"{suite} manifest table inventory is invalid")

    inventory: list[dict[str, int | str]] = []
    listed_relative_paths: set[str] = set()
    for table in table_order:
        raw_table = raw_tables.get(table)
        if not isinstance(raw_table, dict):
            raise DatasetAttestationError(f"tables.{table} must be an object")
        raw_files = raw_table.get("files")
        if not isinstance(raw_files, list) or not raw_files:
            raise DatasetAttestationError(f"tables.{table}.files must be a non-empty array")
        table_rows = 0
        table_bytes = 0
        for index, raw_file in enumerate(raw_files):
            if not isinstance(raw_file, dict):
                raise DatasetAttestationError(f"tables.{table}.files[{index}] must be an object")
            relative_value = raw_file.get("path")
            path = _dataset_file(dataset_root, relative_value, table=table)
            relative = path.relative_to(dataset_root).as_posix()
            if relative in listed_relative_paths:
                raise DatasetAttestationError(f"dataset manifest lists duplicate file: {relative}")
            listed_relative_paths.add(relative)
            if not path.is_file():
                raise DatasetAttestationError(f"declared runtime Parquet file is missing: {path}")
            size_bytes = _integer(
                raw_file.get("size_bytes"),
                label=f"tables.{table}.files[{index}].size_bytes",
                minimum=1,
            )
            row_count = _integer(
                raw_file.get("row_count"),
                label=f"tables.{table}.files[{index}].row_count",
                minimum=1,
            )
            declared_sha256 = raw_file.get("sha256")
            if not isinstance(declared_sha256, str) or _SHA256.fullmatch(declared_sha256) is None:
                raise DatasetAttestationError(f"tables.{table}.files[{index}].sha256 is invalid")
            if path.stat().st_size != size_bytes:
                raise DatasetAttestationError(f"runtime Parquet size mismatch: {path}")
            actual_sha256 = _stable_sha256(path)
            if actual_sha256 != declared_sha256:
                raise DatasetAttestationError(f"runtime Parquet SHA-256 mismatch: {path}")
            table_rows += row_count
            table_bytes += size_bytes
            inventory.append(
                {
                    "table": table,
                    "path": relative,
                    "row_count": row_count,
                    "size_bytes": size_bytes,
                    "sha256": actual_sha256,
                }
            )
        if raw_table.get("file_count") != len(raw_files):
            raise DatasetAttestationError(f"tables.{table}.file_count is inconsistent")
        if raw_table.get("row_count") != table_rows:
            raise DatasetAttestationError(f"tables.{table}.row_count is inconsistent")
        if raw_table.get("total_bytes") != table_bytes:
            raise DatasetAttestationError(f"tables.{table}.total_bytes is inconsistent")

    actual_relative_paths: set[str] = set()
    for path in dataset_root.rglob("*.parquet"):
        if path.is_symlink():
            raise DatasetAttestationError(f"runtime Parquet inventory contains a symlink: {path}")
        if not path.is_file():
            raise DatasetAttestationError(f"runtime Parquet inventory contains a non-file: {path}")
        resolved = Path(os.path.abspath(path))
        try:
            resolved.relative_to(dataset_root)
        except ValueError as error:
            raise DatasetAttestationError(
                f"runtime Parquet resolves outside dataset: {path}"
            ) from error
        actual_relative_paths.add(path.relative_to(dataset_root).as_posix())
    if actual_relative_paths != listed_relative_paths:
        missing = sorted(listed_relative_paths - actual_relative_paths)
        unlisted = sorted(actual_relative_paths - listed_relative_paths)
        raise DatasetAttestationError(
            f"runtime Parquet inventory mismatch; missing={missing}, unlisted={unlisted}"
        )
    return sorted(inventory, key=lambda item: (str(item["table"]), str(item["path"])))


def _runtime_source_inventory(
    dataset_root: Path, manifest: dict[str, Any], suite: DatasetSuite
) -> list[dict[str, int | str]]:
    if suite == "ecommerce":
        return []
    raw_tables = manifest.get("tables")
    if not isinstance(raw_tables, dict) or set(raw_tables) != set(TPCH_TABLE_ORDER):
        raise DatasetAttestationError("TPC-H manifest table inventory is invalid")
    inventory: list[dict[str, int | str]] = []
    listed_paths: set[Path] = set()
    for table in TPCH_TABLE_ORDER:
        raw_table = raw_tables.get(table)
        source = raw_table.get("source_tbl") if isinstance(raw_table, dict) else None
        if not isinstance(source, dict):
            raise DatasetAttestationError(f"tables.{table}.source_tbl must be an object")
        path = _source_file(dataset_root, source.get("path"), table=table)
        if path in listed_paths:
            raise DatasetAttestationError(f"TPC-H source inventory duplicates {path}")
        listed_paths.add(path)
        if not path.is_file():
            raise DatasetAttestationError(f"declared TPC-H source file is missing: {path}")
        size_bytes = _integer(
            source.get("size_bytes"), label=f"tables.{table}.source_tbl.size_bytes", minimum=1
        )
        declared_sha256 = source.get("sha256")
        if not isinstance(declared_sha256, str) or _SHA256.fullmatch(declared_sha256) is None:
            raise DatasetAttestationError(f"tables.{table}.source_tbl.sha256 is invalid")
        if path.stat().st_size != size_bytes:
            raise DatasetAttestationError(f"TPC-H source file size mismatch: {path}")
        actual_sha256 = _stable_sha256(path)
        if actual_sha256 != declared_sha256:
            raise DatasetAttestationError(f"TPC-H source file SHA-256 mismatch: {path}")
        inventory.append(
            {
                "table": table,
                "path": path.relative_to(dataset_root).as_posix(),
                "size_bytes": size_bytes,
                "sha256": actual_sha256,
            }
        )
    actual_paths: set[Path] = set()
    for path in dataset_root.rglob("*.tbl"):
        if path.is_symlink():
            raise DatasetAttestationError(f"TPC-H source inventory contains a symlink: {path}")
        if path.is_file():
            actual_paths.add(Path(os.path.abspath(path)))
    if actual_paths != listed_paths:
        missing = sorted(str(path) for path in listed_paths - actual_paths)
        unlisted = sorted(str(path) for path in actual_paths - listed_paths)
        raise DatasetAttestationError(
            f"TPC-H source inventory mismatch; missing={missing}, unlisted={unlisted}"
        )
    return sorted(inventory, key=lambda item: (str(item["table"]), str(item["path"])))


def _content_identity(
    *,
    suite: DatasetSuite,
    manifest_relative: str,
    manifest_sha256: str,
    inventory: list[dict[str, int | str]],
) -> str:
    return sha256_value(
        {
            "suite": suite,
            "manifest_path": manifest_relative,
            "manifest_sha256": manifest_sha256,
            "runtime_parquet_inventory": inventory,
        }
    )


def _manifest_validation_result_sha256(manifest: dict[str, Any], suite: DatasetSuite) -> str:
    if suite == "ecommerce":
        tables = manifest.get("tables")
        if not isinstance(tables, dict) or set(tables) != set(ECOMMERCE_TABLE_ORDER):
            raise DatasetAttestationError("E-commerce semantic result manifest is invalid")
        row_counts: dict[str, int] = {}
        content_hashes: dict[str, str] = {}
        for table in ECOMMERCE_TABLE_ORDER:
            record = tables.get(table)
            if not isinstance(record, dict):
                raise DatasetAttestationError(f"tables.{table} semantic result is invalid")
            row_counts[table] = _integer(
                record.get("row_count"), label=f"tables.{table}.row_count", minimum=1
            )
            content_hash = record.get("content_sha256")
            if not isinstance(content_hash, str) or _SHA256.fullmatch(content_hash) is None:
                raise DatasetAttestationError(f"tables.{table}.content_sha256 is invalid")
            content_hashes[table] = content_hash
        result: dict[str, Any] = {
            "suite": suite,
            "dataset_id": manifest.get("dataset_id"),
            "table_row_counts": row_counts,
            "table_content_sha256": content_hashes,
        }
    else:
        validation = manifest.get("validation")
        if not isinstance(validation, dict) or validation.get("status") != "passed":
            raise DatasetAttestationError("TPC-H semantic validation manifest is invalid")
        tpch_row_counts = validation.get("row_counts")
        date_bounds = validation.get("date_bounds")
        foreign_key_checks = validation.get("foreign_key_checks")
        if (
            not isinstance(tpch_row_counts, dict)
            or set(tpch_row_counts) != set(TPCH_TABLE_ORDER)
            or not isinstance(date_bounds, dict)
            or set(date_bounds) != set(TPCH_TABLE_ORDER)
        ):
            raise DatasetAttestationError("TPC-H semantic validation result shape is invalid")
        result = {
            "suite": suite,
            "row_counts": tpch_row_counts,
            "date_bounds": date_bounds,
            "foreign_key_checks": foreign_key_checks,
        }
    return sha256_value(result)


def _observe_dataset(
    repository_root: Path,
    manifest_file: Path,
    *,
    include_source_inventory: bool = True,
) -> _DatasetObservation:
    try:
        manifest_bytes = manifest_file.read_bytes()
        manifest_value: object = json.loads(manifest_bytes)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise DatasetAttestationError(
            f"cannot read dataset manifest {manifest_file}: {error}"
        ) from error
    if not isinstance(manifest_value, dict):
        raise DatasetAttestationError(f"dataset manifest root must be an object: {manifest_file}")
    manifest = manifest_value
    suite = _suite_from_manifest(manifest)
    dataset_id = manifest.get("dataset_id")
    if not isinstance(dataset_id, str) or not dataset_id:
        raise DatasetAttestationError("dataset manifest has no dataset_id")
    manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
    manifest_relative = manifest_file.relative_to(repository_root).as_posix()
    parquet_inventory = _runtime_inventory(manifest_file.parent, manifest, suite)
    source_inventory = (
        _runtime_source_inventory(manifest_file.parent, manifest, suite)
        if include_source_inventory
        else []
    )
    if manifest_file.read_bytes() != manifest_bytes:
        raise DatasetAttestationError("dataset manifest changed while it was observed")
    return _DatasetObservation(
        manifest=manifest,
        suite=suite,
        dataset_id=dataset_id,
        manifest_relative=manifest_relative,
        manifest_sha256=manifest_sha256,
        parquet_inventory=parquet_inventory,
        source_inventory=source_inventory,
        content_identity_sha256=_content_identity(
            suite=suite,
            manifest_relative=manifest_relative,
            manifest_sha256=manifest_sha256,
            inventory=parquet_inventory,
        ),
    )


def _validate_attestation_schema(value: dict[str, Any]) -> None:
    artifact_class = value.get("artifact_class")
    schema_version = value.get("schema_version")
    if not isinstance(artifact_class, str) or not isinstance(schema_version, int):
        raise DatasetAttestationError("dataset attestation identity is invalid")
    identity = (artifact_class, schema_version)
    schema_path = _ATTESTATION_SCHEMA_PATHS.get(identity)
    if schema_path is None:
        raise DatasetAttestationError(f"unsupported dataset attestation identity: {identity!r}")
    schema = _load_object(schema_path, label="attestation schema")
    errors = sorted(
        Draft202012Validator(schema).iter_errors(value), key=lambda error: list(error.path)
    )
    if errors:
        messages = [
            f"{'/'.join(map(str, error.path)) or '<root>'}: {error.message}" for error in errors
        ]
        raise DatasetAttestationError(
            "dataset attestation schema validation failed:\n- " + "\n- ".join(messages)
        )


def _validate_self_hash(value: dict[str, Any]) -> None:
    value_for_hash = dict(value)
    declared = value_for_hash.pop("attestation_sha256", None)
    if not isinstance(declared, str) or declared != sha256_value(value_for_hash):
        raise DatasetAttestationError("dataset attestation self-hash is invalid")


def _verified_dataset(
    value: dict[str, Any], *, manifest_path: Path, attestation_path: Path
) -> VerifiedDataset:
    dataset = value["dataset"]
    validator = value["validator"]
    inventory = value["runtime_parquet_inventory"]
    return VerifiedDataset(
        suite=dataset["suite"],
        dataset_id=dataset["dataset_id"],
        manifest_path=manifest_path,
        manifest_sha256=dataset["manifest_sha256"],
        content_identity_sha256=dataset["content_identity_sha256"],
        attestation_path=attestation_path,
        attestation_file_sha256=sha256_file(attestation_path),
        attestation_sha256=value["attestation_sha256"],
        git_commit=validator["git_commit"],
        expected_python_version=validator["expected_python_version"],
        file_count=len(inventory),
        total_bytes=sum(int(item["size_bytes"]) for item in inventory),
    )


def _attestation_mode(value: dict[str, Any]) -> str:
    if value.get("artifact_class") == "dataset-validation-attestation-v1":
        return "full"
    validation = value.get("validation")
    if not isinstance(validation, dict) or not isinstance(validation.get("mode"), str):
        raise DatasetAttestationError("dataset attestation validation mode is invalid")
    return str(validation["mode"])


def _validate_payload_core(
    repository_root: Path,
    value: dict[str, Any],
    observation: _DatasetObservation,
    *,
    expected_python_version: str,
    expected_git_commit: str,
) -> None:
    validator = value["validator"]
    if validator["git_commit"] != expected_git_commit:
        raise DatasetAttestationError("dataset attestation Git commit is stale")
    if validator["expected_python_version"] != expected_python_version:
        raise DatasetAttestationError("dataset attestation expected Python version is stale")
    current_runtime = {
        "python_implementation": platform.python_implementation(),
        "python_version": platform.python_version(),
        "pyarrow_version": pa.__version__,
    }
    for field, current in current_runtime.items():
        if validator[field] != current:
            raise DatasetAttestationError(f"dataset attestation runtime field is stale: {field}")
    runtime_lock_path = repository_root / "runtime-versions.lock"
    if not runtime_lock_path.is_file() or validator["runtime_lock_sha256"] != sha256_file(
        runtime_lock_path
    ):
        raise DatasetAttestationError("dataset attestation runtime lock is stale")

    dataset = value["dataset"]
    expected_dataset_fields: dict[str, object] = {
        "suite": observation.suite,
        "dataset_id": observation.dataset_id,
        "manifest_path": observation.manifest_relative,
        "manifest_sha256": observation.manifest_sha256,
        "content_identity_sha256": observation.content_identity_sha256,
    }
    for field, current in expected_dataset_fields.items():
        if dataset[field] != current:
            raise DatasetAttestationError(f"dataset attestation field is stale: dataset.{field}")
    if value["runtime_parquet_inventory"] != observation.parquet_inventory:
        raise DatasetAttestationError("dataset attestation runtime Parquet inventory is stale")
    if value.get("artifact_class") == "dataset-validation-attestation-v2" and (
        value["runtime_source_inventory"] != observation.source_inventory
    ):
        raise DatasetAttestationError("dataset attestation runtime source inventory is stale")
    expected_result_sha256 = _manifest_validation_result_sha256(
        observation.manifest, observation.suite
    )
    if validator["result_sha256"] != expected_result_sha256:
        raise DatasetAttestationError("dataset attestation semantic result hash is stale")


def _validate_semantic_tree(
    repository_root: Path,
    value: dict[str, Any],
    suite: DatasetSuite,
    commit: str,
    *,
    require_git_lineage: bool = True,
) -> str:
    tree_path = _SEMANTIC_TREE_PATHS[suite]
    if value.get("artifact_class") != "dataset-validation-attestation-v2":
        if not require_git_lineage:
            raise DatasetAttestationError(
                "offline semantic-tree verification requires a v2 attestation"
            )
        return _git_tree_oid(repository_root, commit, tree_path)

    validation = value["validation"]
    declared = validation["semantic_tree"]
    if declared["path"] != tree_path:
        raise DatasetAttestationError("dataset attestation semantic validator tree path is stale")
    declared_object_id = declared["object_id"]
    if not require_git_lineage:
        return str(declared_object_id)
    object_id = _git_tree_oid(repository_root, commit, tree_path)
    if declared_object_id != object_id:
        raise DatasetAttestationError("dataset attestation semantic validator tree is stale")
    return object_id


def _canonical_origin_path(repository_root: Path, origin_commit: str, manifest_sha256: str) -> Path:
    return (
        repository_root
        / ".artifacts/dataset-validations"
        / origin_commit
        / f"{manifest_sha256}.json"
    )


def _validate_full_origin(
    repository_root: Path,
    origin_path: Path,
    observation: _DatasetObservation,
    *,
    expected_python_version: str,
    current_git_commit: str,
    current_tree_object_id: str,
    current_attestation_path: Path,
    require_git_lineage: bool = True,
    claimed_origin_tree_object_id: str | None = None,
) -> dict[str, Any]:
    source_file = _repo_path(repository_root, origin_path, label="origin attestation")
    if source_file == current_attestation_path:
        raise DatasetAttestationError("rebound attestation cannot reference itself")
    if not source_file.is_file():
        raise DatasetAttestationError(f"origin attestation is missing: {source_file}")
    source_file_sha256 = _stable_sha256(source_file)
    source = _load_object(source_file, label="origin attestation")
    _validate_attestation_schema(source)
    _validate_self_hash(source)
    if _attestation_mode(source) != "full":
        raise DatasetAttestationError("rebound lineage must point directly to a full attestation")
    origin_validator = source["validator"]
    origin_commit = origin_validator["git_commit"]
    if not isinstance(origin_commit, str) or _GIT_COMMIT.fullmatch(origin_commit) is None:
        raise DatasetAttestationError("origin attestation Git commit is invalid")
    if origin_commit == current_git_commit:
        raise DatasetAttestationError("origin attestation must come from a prior commit")
    expected_source_path = _canonical_origin_path(
        repository_root, origin_commit, observation.manifest_sha256
    )
    if source_file != expected_source_path:
        raise DatasetAttestationError("origin attestation is outside its canonical producer key")
    _validate_payload_core(
        repository_root,
        source,
        observation,
        expected_python_version=expected_python_version,
        expected_git_commit=origin_commit,
    )
    if require_git_lineage:
        if not _is_ancestor(repository_root, origin_commit, current_git_commit):
            raise DatasetAttestationError(
                "origin attestation commit is not an ancestor of current HEAD"
            )
        origin_tree_object_id = _validate_semantic_tree(
            repository_root, source, observation.suite, origin_commit
        )
    else:
        if (
            not isinstance(claimed_origin_tree_object_id, str)
            or _GIT_COMMIT.fullmatch(claimed_origin_tree_object_id) is None
        ):
            raise DatasetAttestationError("offline origin semantic-tree binding is invalid")
        origin_tree_object_id = claimed_origin_tree_object_id
        if source.get("artifact_class") == "dataset-validation-attestation-v2":
            declared_origin_tree = _validate_semantic_tree(
                repository_root,
                source,
                observation.suite,
                origin_commit,
                require_git_lineage=False,
            )
            if declared_origin_tree != origin_tree_object_id:
                raise DatasetAttestationError(
                    "origin semantic-tree declaration differs from rebound lineage"
                )
    if origin_tree_object_id != current_tree_object_id:
        raise DatasetAttestationError("semantic validator Git tree differs from the full origin")
    return {
        "artifact_class": source["artifact_class"],
        "schema_version": source["schema_version"],
        "attestation_path": source_file.relative_to(repository_root).as_posix(),
        "attestation_file_sha256": source_file_sha256,
        "attestation_payload_sha256": source["attestation_sha256"],
        "validator_git_commit": origin_commit,
        "semantic_tree_object_id": origin_tree_object_id,
        "manifest_sha256": observation.manifest_sha256,
        "content_identity_sha256": observation.content_identity_sha256,
        "result_sha256": origin_validator["result_sha256"],
    }


def _base_v2_value(
    observation: _DatasetObservation,
    *,
    git_commit: str,
    expected_python_version: str,
    runtime_lock_sha256: str,
    result_sha256: str,
    validation: dict[str, Any],
) -> dict[str, Any]:
    return {
        "artifact_class": "dataset-validation-attestation-v2",
        "schema_version": 2,
        "status": "passed",
        "dataset": {
            "suite": observation.suite,
            "dataset_id": observation.dataset_id,
            "manifest_path": observation.manifest_relative,
            "manifest_sha256": observation.manifest_sha256,
            "content_identity_sha256": observation.content_identity_sha256,
        },
        "validator": {
            "git_commit": git_commit,
            "expected_python_version": expected_python_version,
            "python_implementation": platform.python_implementation(),
            "python_version": platform.python_version(),
            "pyarrow_version": pa.__version__,
            "runtime_lock_sha256": runtime_lock_sha256,
            "result_sha256": result_sha256,
        },
        "validation": validation,
        "runtime_parquet_inventory": observation.parquet_inventory,
        "runtime_source_inventory": observation.source_inventory,
    }


def _publish_verified_attestation(
    repository_root: Path,
    manifest_file: Path,
    output_file: Path,
    value: dict[str, Any],
    *,
    expected_python_version: str,
    git_commit: str,
    expected_manifest_bytes: bytes,
) -> VerifiedDataset:
    """Strictly verify a staged receipt before its immutable canonical publication."""

    output_file.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".attestation-preflight-", dir=output_file.parent
    ) as temporary_directory:
        staged = Path(temporary_directory) / output_file.name
        write_json(staged, value)
        staged_verified = verify_attestation(
            repository_root,
            manifest_file,
            staged,
            expected_python_version,
            git_commit,
        )
        staged_file_sha256 = staged_verified.attestation_file_sha256

    _require_clean_commit(repository_root, git_commit, phase="immediately before publication")
    if manifest_file.read_bytes() != expected_manifest_bytes:
        raise DatasetAttestationError("dataset manifest changed immediately before publication")
    try:
        write_json(output_file, value)
    except FileExistsError as error:
        raise DatasetAttestationError(str(error)) from error
    if _stable_sha256(output_file) != staged_file_sha256:
        raise DatasetAttestationError("published dataset attestation differs from its preflight")
    return _verified_dataset(
        value,
        manifest_path=manifest_file,
        attestation_path=output_file,
    )


def create_attestation(
    root: Path,
    manifest_path: Path,
    output: Path,
    expected_python_version: str,
    git_commit: str,
) -> VerifiedDataset:
    """Run the suite's full validator and immutably attest its physical data identity."""

    repository_root = root.expanduser().resolve()
    manifest_file = _repo_path(repository_root, manifest_path, label="dataset manifest")
    output_file = _repo_path(repository_root, output, label="attestation output")
    if manifest_file.name != "manifest.json" or not manifest_file.is_file():
        raise DatasetAttestationError(f"dataset manifest is missing or misnamed: {manifest_file}")
    _validate_expected_identity(expected_python_version, git_commit)
    _require_clean_commit(repository_root, git_commit, phase="before full dataset validation")
    runtime_lock_path = repository_root / "runtime-versions.lock"
    if not runtime_lock_path.is_file():
        raise DatasetAttestationError(f"runtime lock is missing: {runtime_lock_path}")

    manifest_before = manifest_file.read_bytes()
    try:
        manifest_value: object = json.loads(manifest_before)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise DatasetAttestationError(
            f"cannot read dataset manifest {manifest_file}: {error}"
        ) from error
    if not isinstance(manifest_value, dict):
        raise DatasetAttestationError(f"dataset manifest root must be an object: {manifest_file}")
    manifest = manifest_value
    suite = _suite_from_manifest(manifest)
    try:
        if suite == "ecommerce":
            report: EcommerceValidationReport | TpchValidationReport = validate_dataset(
                manifest_file.parent,
                expected_python_version=expected_python_version,
            )
        else:
            source = load_source_lock(runtime_lock_path).as_manifest()
            report = validate_tpch_dataset(
                manifest_file.parent,
                expected_source=source,
                expected_python_version=expected_python_version,
            )
    except (DatasetValidationError, TpchContractError, SourceProvenanceError) as error:
        raise DatasetAttestationError(f"full dataset validation failed: {error}") from error
    if manifest_file.read_bytes() != manifest_before:
        raise DatasetAttestationError("dataset manifest changed during full validation")

    observation = _observe_dataset(repository_root, manifest_file)
    if (
        observation.manifest != manifest
        or observation.manifest_sha256 != hashlib.sha256(manifest_before).hexdigest()
        or manifest_file.read_bytes() != manifest_before
    ):
        raise DatasetAttestationError("dataset manifest changed after full validation")
    result_sha256 = _validation_result_sha256(report, suite)
    if result_sha256 != _manifest_validation_result_sha256(observation.manifest, suite):
        raise DatasetAttestationError("full validation result differs from the dataset manifest")
    tree_path = _SEMANTIC_TREE_PATHS[suite]
    tree_object_id = _git_tree_oid(repository_root, git_commit, tree_path)
    value = _base_v2_value(
        observation,
        git_commit=git_commit,
        expected_python_version=expected_python_version,
        runtime_lock_sha256=sha256_file(runtime_lock_path),
        result_sha256=result_sha256,
        validation={
            "mode": "full",
            "semantic_tree": {"path": tree_path, "object_id": tree_object_id},
        },
    )
    value["attestation_sha256"] = sha256_value(value)
    _validate_attestation_schema(value)
    return _publish_verified_attestation(
        repository_root,
        manifest_file,
        output_file,
        value,
        expected_python_version=expected_python_version,
        git_commit=git_commit,
        expected_manifest_bytes=manifest_before,
    )


def rebind_attestation(
    root: Path,
    manifest_path: Path,
    origin_attestation_path: Path,
    output: Path,
    expected_python_version: str,
    git_commit: str,
) -> VerifiedDataset:
    """Rebind one exact full semantic receipt to a new clean commit without decoding rows."""

    repository_root = root.expanduser().resolve()
    manifest_file = _repo_path(repository_root, manifest_path, label="dataset manifest")
    output_file = _repo_path(repository_root, output, label="attestation output")
    if manifest_file.name != "manifest.json" or not manifest_file.is_file():
        raise DatasetAttestationError(f"dataset manifest is missing or misnamed: {manifest_file}")
    _validate_expected_identity(expected_python_version, git_commit)
    _require_clean_commit(repository_root, git_commit, phase="before attestation rebinding")
    runtime_lock_path = repository_root / "runtime-versions.lock"
    if not runtime_lock_path.is_file():
        raise DatasetAttestationError(f"runtime lock is missing: {runtime_lock_path}")
    manifest_before = manifest_file.read_bytes()
    observation = _observe_dataset(repository_root, manifest_file)
    if observation.manifest_sha256 != hashlib.sha256(manifest_before).hexdigest():
        raise DatasetAttestationError("dataset manifest changed during attestation rebinding")
    tree_path = _SEMANTIC_TREE_PATHS[observation.suite]
    tree_object_id = _git_tree_oid(repository_root, git_commit, tree_path)
    try:
        origin = _validate_full_origin(
            repository_root,
            origin_attestation_path,
            observation,
            expected_python_version=expected_python_version,
            current_git_commit=git_commit,
            current_tree_object_id=tree_object_id,
            current_attestation_path=output_file,
        )
    except DatasetAttestationError as error:
        raise DatasetAttestationNotReusable(str(error)) from error
    if manifest_file.read_bytes() != manifest_before:
        raise DatasetAttestationError("dataset manifest changed during attestation rebinding")
    value = _base_v2_value(
        observation,
        git_commit=git_commit,
        expected_python_version=expected_python_version,
        runtime_lock_sha256=sha256_file(runtime_lock_path),
        result_sha256=origin["result_sha256"],
        validation={
            "mode": "rebound",
            "semantic_tree": {"path": tree_path, "object_id": tree_object_id},
            "origin": origin,
        },
    )
    value["attestation_sha256"] = sha256_value(value)
    _validate_attestation_schema(value)
    return _publish_verified_attestation(
        repository_root,
        manifest_file,
        output_file,
        value,
        expected_python_version=expected_python_version,
        git_commit=git_commit,
        expected_manifest_bytes=manifest_before,
    )


def verify_attestation(
    root: Path,
    manifest_path: Path,
    attestation_path: Path,
    expected_python_version: str,
    expected_git_commit: str,
    *,
    require_git_lineage: bool = True,
) -> VerifiedDataset:
    """Verify content and provenance, resolving Git lineage unless explicitly delegated."""

    repository_root = root.expanduser().resolve()
    manifest_file = _repo_path(repository_root, manifest_path, label="dataset manifest")
    attestation_file = _repo_path(repository_root, attestation_path, label="dataset attestation")
    _validate_expected_identity(expected_python_version, expected_git_commit)
    if manifest_file.name != "manifest.json" or not manifest_file.is_file():
        raise DatasetAttestationError(f"dataset manifest is missing or misnamed: {manifest_file}")
    if not attestation_file.is_file():
        raise DatasetAttestationError(f"dataset attestation is missing: {attestation_file}")

    value = _load_object(attestation_file, label="dataset attestation")
    _validate_attestation_schema(value)
    _validate_self_hash(value)
    is_v2 = value.get("artifact_class") == "dataset-validation-attestation-v2"
    if not require_git_lineage and not is_v2:
        raise DatasetAttestationError("offline verification requires a v2 attestation")
    observation = _observe_dataset(
        repository_root,
        manifest_file,
        include_source_inventory=is_v2,
    )
    _validate_payload_core(
        repository_root,
        value,
        observation,
        expected_python_version=expected_python_version,
        expected_git_commit=expected_git_commit,
    )
    if is_v2:
        current_tree_object_id = _validate_semantic_tree(
            repository_root,
            value,
            observation.suite,
            expected_git_commit,
            require_git_lineage=require_git_lineage,
        )
        if _attestation_mode(value) == "rebound":
            validation = value["validation"]
            origin = _validate_full_origin(
                repository_root,
                _canonical_repo_path(
                    repository_root,
                    validation["origin"]["attestation_path"],
                    label="origin attestation path",
                ),
                observation,
                expected_python_version=expected_python_version,
                current_git_commit=expected_git_commit,
                current_tree_object_id=current_tree_object_id,
                current_attestation_path=attestation_file,
                require_git_lineage=require_git_lineage,
                claimed_origin_tree_object_id=validation["origin"]["semantic_tree_object_id"],
            )
            if validation["origin"] != origin:
                raise DatasetAttestationError("dataset attestation lineage binding is stale")
    return _verified_dataset(
        value,
        manifest_path=manifest_file,
        attestation_path=attestation_file,
    )


def discover_full_attestation_origins(
    root: Path, manifest_path: Path, current_git_commit: str
) -> tuple[Path, ...]:
    """Return canonical full receipts in nearest-ancestor order without scanning dataset rows."""

    repository_root = root.expanduser().resolve()
    manifest_file = _repo_path(repository_root, manifest_path, label="dataset manifest")
    manifest_sha256 = sha256_file(manifest_file)
    validation_root = repository_root / ".artifacts/dataset-validations"
    if not validation_root.is_dir():
        return ()
    origins: list[Path] = []
    for commit in _ancestor_commits(repository_root, current_git_commit):
        if commit == current_git_commit:
            continue
        candidate = _canonical_origin_path(repository_root, commit, manifest_sha256)
        if not candidate.is_file():
            continue
        try:
            value = _load_object(candidate, label="origin attestation")
            _validate_attestation_schema(value)
            _validate_self_hash(value)
            if (
                _attestation_mode(value) != "full"
                or value["validator"]["git_commit"] != commit
                or value["dataset"]["manifest_sha256"] != manifest_sha256
            ):
                continue
        except DatasetAttestationError:
            continue
        origins.append(candidate)
    return tuple(origins)
