"""Content-bound attestations for reusing an expensive dataset validation.

An attestation is a trusted local cache of a successful full semantic validation, not a
cryptographic signature or a claim made by an external authority. Reuse still hashes the exact
runtime Parquet inventory, so changing physical data never turns into a cache hit merely because
file names, sizes, or timestamps are unchanged.
"""

from __future__ import annotations

import json
import platform
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import pyarrow as pa  # type: ignore[import-untyped]
from jsonschema import Draft202012Validator

from benchmark.runner.canonical import sha256_file, sha256_value, write_json
from benchmark.runner.evidence import RepositoryEvidenceError, clean_git_commit
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

_ATTESTATION_SCHEMA_PATH = (
    Path(__file__).resolve().parents[1] / "schemas" / "dataset-validation-attestation.schema.json"
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_GIT_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_PYTHON_VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")


class DatasetAttestationError(ValueError):
    """An attestation or its current physical dataset identity is invalid."""


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
    candidate = candidate.resolve(strict=False)
    try:
        candidate.relative_to(repository_root)
    except ValueError as error:
        raise DatasetAttestationError(f"{label} leaves repository root: {path}") from error
    return candidate


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
    candidate = (dataset_root / relative).resolve(strict=False)
    try:
        candidate.relative_to(dataset_root)
    except ValueError as error:
        raise DatasetAttestationError(
            f"dataset file resolves outside root: {relative_value!r}"
        ) from error
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
        if not path.is_file():
            raise DatasetAttestationError(f"runtime Parquet inventory contains a non-file: {path}")
        resolved = path.resolve()
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


def _validate_attestation_schema(value: dict[str, Any]) -> None:
    schema = _load_object(_ATTESTATION_SCHEMA_PATH, label="attestation schema")
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


def create_attestation(
    root: Path,
    manifest_path: Path,
    output: Path,
    expected_python_version: str,
    git_commit: str,
) -> VerifiedDataset:
    """Run the suite's full validator and immutably attest its runtime Parquet identity."""

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
    manifest = _load_object(manifest_file, label="dataset manifest")
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

    manifest_sha256 = sha256_file(manifest_file)
    manifest_relative = manifest_file.relative_to(repository_root).as_posix()
    inventory = _runtime_inventory(manifest_file.parent, manifest, suite)
    content_identity_sha256 = _content_identity(
        suite=suite,
        manifest_relative=manifest_relative,
        manifest_sha256=manifest_sha256,
        inventory=inventory,
    )
    dataset_id = manifest.get("dataset_id")
    if not isinstance(dataset_id, str) or not dataset_id:
        raise DatasetAttestationError("dataset manifest has no dataset_id")
    value: dict[str, Any] = {
        "artifact_class": "dataset-validation-attestation-v1",
        "schema_version": 1,
        "status": "passed",
        "dataset": {
            "suite": suite,
            "dataset_id": dataset_id,
            "manifest_path": manifest_relative,
            "manifest_sha256": manifest_sha256,
            "content_identity_sha256": content_identity_sha256,
        },
        "validator": {
            "git_commit": git_commit,
            "expected_python_version": expected_python_version,
            "python_implementation": platform.python_implementation(),
            "python_version": platform.python_version(),
            "pyarrow_version": pa.__version__,
            "runtime_lock_sha256": sha256_file(runtime_lock_path),
            "result_sha256": _validation_result_sha256(report, suite),
        },
        "runtime_parquet_inventory": inventory,
    }
    value["attestation_sha256"] = sha256_value(value)
    _validate_attestation_schema(value)
    _require_clean_commit(repository_root, git_commit, phase="immediately before publication")
    try:
        write_json(output_file, value)
    except FileExistsError as error:
        raise DatasetAttestationError(str(error)) from error
    return verify_attestation(
        repository_root,
        manifest_file,
        output_file,
        expected_python_version,
        git_commit,
    )


def verify_attestation(
    root: Path,
    manifest_path: Path,
    attestation_path: Path,
    expected_python_version: str,
    expected_git_commit: str,
) -> VerifiedDataset:
    """Verify provenance, manifest bytes, and every current runtime Parquet file."""

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
    value_for_hash = dict(value)
    declared_attestation_sha256 = value_for_hash.pop("attestation_sha256")
    if declared_attestation_sha256 != sha256_value(value_for_hash):
        raise DatasetAttestationError("dataset attestation self-hash is invalid")

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

    manifest = _load_object(manifest_file, label="dataset manifest")
    suite = _suite_from_manifest(manifest)
    dataset = value["dataset"]
    manifest_relative = manifest_file.relative_to(repository_root).as_posix()
    manifest_sha256 = sha256_file(manifest_file)
    dataset_id = manifest.get("dataset_id")
    expected_dataset_fields: dict[str, object] = {
        "suite": suite,
        "dataset_id": dataset_id,
        "manifest_path": manifest_relative,
        "manifest_sha256": manifest_sha256,
    }
    for field, current in expected_dataset_fields.items():
        if dataset[field] != current:
            raise DatasetAttestationError(f"dataset attestation field is stale: dataset.{field}")

    inventory = _runtime_inventory(manifest_file.parent, manifest, suite)
    if value["runtime_parquet_inventory"] != inventory:
        raise DatasetAttestationError("dataset attestation runtime Parquet inventory is stale")
    content_identity_sha256 = _content_identity(
        suite=suite,
        manifest_relative=manifest_relative,
        manifest_sha256=manifest_sha256,
        inventory=inventory,
    )
    if dataset["content_identity_sha256"] != content_identity_sha256:
        raise DatasetAttestationError("dataset attestation content identity is invalid")
    return _verified_dataset(
        value,
        manifest_path=manifest_file,
        attestation_path=attestation_file,
    )
