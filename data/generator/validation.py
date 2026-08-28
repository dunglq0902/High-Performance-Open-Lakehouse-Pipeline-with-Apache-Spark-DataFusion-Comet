"""Dataset-manifest, Parquet, and E-commerce integrity validation."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq  # type: ignore[import-untyped]

from data.generator.canonical import canonical_json_sha256
from data.generator.constants import (
    CANONICAL_ROW_ENCODING,
    CATEGORIES,
    CONTENT_HASH_ALGORITHM,
    DATASET_SCHEMA_VERSION,
    DEVICE_TYPES,
    EVENT_TYPES,
    GENERATOR_NAME,
    GENERATOR_VERSION,
    ORDER_STATUSES,
    PAYMENT_METHODS,
    PRIMARY_KEY_RANGE_SIZE,
    REGIONS,
    SEGMENTS,
    TABLE_ORDER,
)
from data.generator.manifest import TableAudit
from data.generator.profiles import GeneratorProfile, profile_from_mapping
from data.generator.schemas import PRIMARY_KEYS, TABLE_SCHEMAS, schema_sha256


class DatasetValidationError(ValueError):
    """Raised with every detected contract violation."""

    def __init__(self, issues: list[str]) -> None:
        self.issues = tuple(issues)
        super().__init__("dataset validation failed:\n- " + "\n- ".join(issues))


@dataclass(frozen=True, slots=True)
class ValidationReport:
    dataset_id: str
    manifest_path: Path
    table_row_counts: dict[str, int]
    table_content_sha256: dict[str, str]


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_data_file(dataset_dir: Path, relative_path: object) -> Path:
    if not isinstance(relative_path, str):
        raise ValueError("manifest file path must be a string")
    candidate = Path(relative_path)
    if candidate.is_absolute() or ".." in candidate.parts:
        raise ValueError(f"manifest file path escapes the dataset: {relative_path!r}")
    resolved = (dataset_dir / candidate).resolve(strict=False)
    if not resolved.is_relative_to(dataset_dir):
        raise ValueError(f"manifest file path escapes the dataset: {relative_path!r}")
    return resolved


def _load_manifest(dataset_dir: Path) -> tuple[Path, dict[str, Any]]:
    manifest_path = dataset_dir / "manifest.json"
    try:
        decoded = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise DatasetValidationError([f"cannot read manifest.json: {error}"]) from error
    if not isinstance(decoded, dict):
        raise DatasetValidationError(["manifest root must be an object"])
    return manifest_path, decoded


def _profile_from_manifest(
    manifest: dict[str, Any], expected_profile: GeneratorProfile | None, issues: list[str]
) -> GeneratorProfile | None:
    raw_profile = manifest.get("profile")
    if not isinstance(raw_profile, dict):
        issues.append("manifest.profile must be an object")
        return None
    try:
        profile = profile_from_mapping(raw_profile)
    except (TypeError, ValueError) as error:
        issues.append(f"manifest.profile is invalid: {error}")
        return None
    if expected_profile is not None and (
        profile.as_canonical_mapping() != expected_profile.as_canonical_mapping()
    ):
        issues.append("manifest profile does not match the expected generator profile")
    if manifest.get("generation_config_sha256") != canonical_json_sha256(
        profile.as_canonical_mapping()
    ):
        issues.append("generation_config_sha256 does not match manifest.profile")
    return profile


def _validate_manifest_header(
    manifest: dict[str, Any], profile: GeneratorProfile | None, issues: list[str]
) -> None:
    if manifest.get("schema_version") != DATASET_SCHEMA_VERSION:
        issues.append(f"manifest.schema_version must be {DATASET_SCHEMA_VERSION}")
    generator = manifest.get("generator")
    if not isinstance(generator, dict):
        issues.append("manifest.generator must be an object")
    else:
        if generator.get("name") != GENERATOR_NAME:
            issues.append(f"manifest.generator.name must be {GENERATOR_NAME!r}")
        if generator.get("version") != GENERATOR_VERSION:
            issues.append(f"manifest.generator.version must be {GENERATOR_VERSION!r}")
        if not isinstance(generator.get("git_commit"), str) or not generator.get("git_commit"):
            issues.append("manifest.generator.git_commit must be a non-empty string")
        if not isinstance(generator.get("worktree_dirty"), bool):
            issues.append("manifest.generator.worktree_dirty must be a boolean")

    expected_hash_contract = {
        "algorithm": CONTENT_HASH_ALGORITHM,
        "canonical_row_encoding": CANONICAL_ROW_ENCODING,
        "primary_key_range_size": PRIMARY_KEY_RANGE_SIZE,
    }
    if manifest.get("content_hash_contract") != expected_hash_contract:
        issues.append("manifest content_hash_contract does not match generator version")

    storage = manifest.get("storage")
    if not isinstance(storage, dict):
        issues.append("manifest.storage must be an object")
    elif storage.get("format") != "parquet" or storage.get("compression") != "snappy":
        issues.append("manifest storage must be Parquet with Snappy compression")

    if profile is None:
        return
    scalar_expectations: dict[str, object] = {
        "dataset_id": profile.dataset_id,
        "seed": profile.seed,
        "scale_profile": profile.profile_id,
        "skew_profile": profile.skew_profile,
        "benchmark_eligible": profile.benchmark_eligible,
        "timezone": profile.timezone,
        "currency": profile.currency,
        "rounding_mode": profile.rounding_mode,
    }
    for field_name, expected in scalar_expectations.items():
        if manifest.get(field_name) != expected:
            issues.append(f"manifest.{field_name} does not match profile ({expected!r})")


def _manifest_files(
    dataset_dir: Path,
    table_name: str,
    table_manifest: dict[str, Any],
    issues: list[str],
) -> list[tuple[Path, dict[str, Any]]]:
    raw_files = table_manifest.get("files")
    if not isinstance(raw_files, list) or not raw_files:
        issues.append(f"tables.{table_name}.files must be a non-empty array")
        return []
    files: list[tuple[Path, dict[str, Any]]] = []
    listed_paths: set[Path] = set()
    for index, raw_file in enumerate(raw_files):
        if not isinstance(raw_file, dict):
            issues.append(f"tables.{table_name}.files[{index}] must be an object")
            continue
        try:
            path = _safe_data_file(dataset_dir, raw_file.get("path"))
        except ValueError as error:
            issues.append(str(error))
            continue
        if path in listed_paths:
            issues.append(f"tables.{table_name} lists duplicate file {path.name}")
            continue
        listed_paths.add(path)
        if not path.is_file():
            issues.append(f"listed Parquet file does not exist: {path}")
            continue
        if path.suffix != ".parquet":
            issues.append(f"listed data file is not Parquet: {path}")
        if raw_file.get("size_bytes") != path.stat().st_size:
            issues.append(f"file size mismatch: {path}")
        if raw_file.get("sha256") != _sha256_file(path):
            issues.append(f"file SHA-256 mismatch: {path}")
        files.append((path, raw_file))

    table_dir = dataset_dir / table_name
    actual_paths = set(table_dir.glob("*.parquet")) if table_dir.is_dir() else set()
    if actual_paths != listed_paths:
        missing = sorted(str(path) for path in listed_paths - actual_paths)
        unlisted = sorted(str(path) for path in actual_paths - listed_paths)
        issues.append(
            f"tables.{table_name} file inventory mismatch; missing={missing}, unlisted={unlisted}"
        )
    return files


def _validate_table_metadata(
    table_name: str,
    table_manifest: dict[str, Any],
    audit_result: dict[str, Any],
    files: list[tuple[Path, dict[str, Any]]],
    issues: list[str],
) -> None:
    schema = TABLE_SCHEMAS[table_name]
    expected_values: dict[str, object] = {
        "primary_key": list(PRIMARY_KEYS[table_name]),
        "schema_sha256": schema_sha256(schema),
        "format": "parquet",
        "compression": "snappy",
        "file_count": len(files),
        "total_bytes": sum(path.stat().st_size for path, _ in files),
    }
    for field_name in (
        "row_count",
        "content_sha256",
        "content_hash_algorithm",
        "canonical_row_encoding",
        "primary_key_range_size",
        "range_key",
        "range_hashes",
        "min_max",
        "null_counts",
    ):
        expected_values[field_name] = audit_result[field_name]
    for field_name, expected in expected_values.items():
        if table_manifest.get(field_name) != expected:
            issues.append(f"tables.{table_name}.{field_name} does not match actual data")


def _validate_customer(row: dict[str, Any], emails: set[str], issues: list[str]) -> None:
    email = row["email"]
    if row["region"] not in REGIONS:
        issues.append(f"customers[{row['customer_id']}].region is outside the enum")
    if row["segment"] not in SEGMENTS:
        issues.append(f"customers[{row['customer_id']}].segment is outside the enum")
    if not isinstance(row["customer_name"], str) or not 1 <= len(row["customer_name"]) <= 120:
        issues.append(f"customers[{row['customer_id']}].customer_name length is invalid")
    if not isinstance(email, str) or not email.endswith("@example.test"):
        issues.append(f"customers[{row['customer_id']}].email does not use example.test")
    elif email in emails:
        issues.append(f"customers email is duplicate: {email}")
    else:
        emails.add(email)


def _validate_product(row: dict[str, Any], issues: list[str]) -> None:
    if row["category"] not in CATEGORIES:
        issues.append(f"products[{row['product_id']}].category is outside the enum")
    if not isinstance(row["product_name"], str) or not 1 <= len(row["product_name"]) <= 200:
        issues.append(f"products[{row['product_id']}].product_name length is invalid")
    if not isinstance(row["base_price"], Decimal) or row["base_price"] < 0:
        issues.append(f"products[{row['product_id']}].base_price is negative or invalid")


def validate_dataset(
    dataset_dir: str | Path,
    *,
    expected_profile: GeneratorProfile | None = None,
) -> ValidationReport:
    """Validate manifest identity, physical files, schemas, and relational gates."""

    root = Path(dataset_dir).expanduser().resolve(strict=False)
    manifest_path, manifest = _load_manifest(root)
    issues: list[str] = []
    profile = _profile_from_manifest(manifest, expected_profile, issues)
    _validate_manifest_header(manifest, profile, issues)
    raw_tables = manifest.get("tables")
    if not isinstance(raw_tables, dict) or set(raw_tables) != set(TABLE_ORDER):
        issues.append("manifest.tables must contain exactly the five E-commerce tables")
        raise DatasetValidationError(issues)

    customer_ids: set[int] = set()
    customer_signup: dict[int, datetime] = {}
    customer_emails: set[str] = set()
    product_ids: set[int] = set()
    orders: dict[int, tuple[int, datetime]] = {}
    session_state: dict[str, tuple[datetime, set[str], int | None]] = {}
    table_row_counts: dict[str, int] = {}
    table_content_hashes: dict[str, str] = {}

    for table_name in TABLE_ORDER:
        raw_table_manifest = raw_tables[table_name]
        if not isinstance(raw_table_manifest, dict):
            issues.append(f"tables.{table_name} must be an object")
            continue
        files = _manifest_files(root, table_name, raw_table_manifest, issues)
        audit = TableAudit(TABLE_SCHEMAS[table_name], PRIMARY_KEYS[table_name])
        table_rows = 0
        for file_path, raw_file in files:
            file_rows = 0
            try:
                parquet_file = pq.ParquetFile(file_path)
                if not parquet_file.schema_arrow.equals(TABLE_SCHEMAS[table_name]):
                    issues.append(f"Arrow schema mismatch: {file_path}")
                    continue
                for batch in parquet_file.iter_batches(batch_size=16_384):
                    for row in batch.to_pylist():
                        try:
                            audit.add(row)
                        except (TypeError, ValueError) as error:
                            issues.append(f"{table_name} content contract error: {error}")
                            continue
                        table_rows += 1
                        file_rows += 1
                        if table_name == "customers":
                            _validate_customer(row, customer_emails, issues)
                            customer_id = int(row["customer_id"])
                            customer_ids.add(customer_id)
                            customer_signup[customer_id] = row["signup_time"]
                        elif table_name == "products":
                            _validate_product(row, issues)
                            product_ids.add(int(row["product_id"]))
                        elif table_name == "orders":
                            order_id = int(row["order_id"])
                            customer_id = int(row["customer_id"])
                            if customer_id not in customer_ids:
                                issues.append(
                                    f"orders[{order_id}] has orphan customer_id={customer_id}"
                                )
                            if row["status"] not in ORDER_STATUSES:
                                issues.append(f"orders[{order_id}].status is outside the enum")
                            if row["payment_method"] not in PAYMENT_METHODS:
                                issues.append(
                                    f"orders[{order_id}].payment_method is outside the enum"
                                )
                            signup = customer_signup.get(customer_id)
                            if signup is not None and row["order_time"] < signup:
                                issues.append(f"orders[{order_id}].order_time precedes signup_time")
                            orders[order_id] = (customer_id, row["order_time"])
                        elif table_name == "order_items":
                            order_id = int(row["order_id"])
                            product_id = int(row["product_id"])
                            if order_id not in orders:
                                issues.append(f"order_items has orphan order_id={order_id}")
                            if product_id not in product_ids:
                                issues.append(f"order_items has orphan product_id={product_id}")
                            if not 1 <= row["line_number"] <= 20:
                                issues.append(
                                    f"order_items[{order_id}] line_number is outside 1..20"
                                )
                            if not 1 <= row["quantity"] <= 20:
                                issues.append(f"order_items[{order_id}] quantity is outside 1..20")
                            if row["unit_price"] < 0:
                                issues.append(f"order_items[{order_id}] unit_price is negative")
                            if not Decimal("0.0000") <= row["discount"] <= Decimal("1.0000"):
                                issues.append(f"order_items[{order_id}] discount is outside [0,1]")
                        else:
                            event_id = int(row["event_id"])
                            event_type = row["event_type"]
                            customer_id = row["customer_id"]
                            product_id = row["product_id"]
                            order_id = row["order_id"]
                            if event_type not in EVENT_TYPES:
                                issues.append(f"events[{event_id}].event_type is outside the enum")
                            if row["device_type"] not in DEVICE_TYPES:
                                issues.append(f"events[{event_id}].device_type is outside the enum")
                            if customer_id is not None and customer_id not in customer_ids:
                                issues.append(
                                    f"events[{event_id}] has orphan customer_id={customer_id}"
                                )
                            if product_id is not None and product_id not in product_ids:
                                issues.append(
                                    f"events[{event_id}] has orphan product_id={product_id}"
                                )
                            if event_type in EVENT_TYPES[:2] and product_id is None:
                                issues.append(f"events[{event_id}] requires product_id")
                            if order_id is not None and order_id not in orders:
                                issues.append(f"events[{event_id}] has orphan order_id={order_id}")

                            session_id = row["session_id"]
                            previous = session_state.get(session_id)
                            seen_types: set[str] = set()
                            session_customer: int | None = customer_id
                            if previous is not None:
                                previous_time, previous_types, previous_customer = previous
                                if row["event_time"] < previous_time:
                                    issues.append(f"events session {session_id!r} is out of order")
                                seen_types = set(previous_types)
                                session_customer = previous_customer
                                if (
                                    customer_id is not None
                                    and previous_customer is not None
                                    and customer_id != previous_customer
                                ):
                                    issues.append(
                                        f"events session {session_id!r} changes customer_id"
                                    )
                            if event_type == "purchase":
                                if order_id is None:
                                    issues.append(f"events[{event_id}] purchase requires order_id")
                                elif orders.get(order_id, (None, None))[0] != customer_id:
                                    issues.append(
                                        f"events[{event_id}] purchase customer does not own order"
                                    )
                                required = {"view_product", "add_to_cart", "checkout"}
                                if not required.issubset(seen_types):
                                    issues.append(
                                        f"events[{event_id}] purchase lacks ordered funnel steps"
                                    )
                            seen_types.add(event_type)
                            session_state[session_id] = (
                                row["event_time"],
                                seen_types,
                                session_customer,
                            )
            except (OSError, ValueError) as error:
                issues.append(f"cannot read Parquet file {file_path}: {error}")
                continue
            if raw_file.get("row_count") != file_rows:
                issues.append(f"manifest row count mismatch for file {file_path}")

        if table_rows == 0:
            issues.append(f"table {table_name} has no readable rows")
            continue
        try:
            audit_result = audit.finish()
        except ValueError as error:
            issues.append(f"cannot finalize table {table_name}: {error}")
            continue
        _validate_table_metadata(table_name, raw_table_manifest, audit_result, files, issues)
        if profile is not None and table_rows != profile.counts[table_name]:
            issues.append(
                f"table {table_name} has {table_rows} rows; profile requires "
                f"{profile.counts[table_name]}"
            )
        table_row_counts[table_name] = table_rows
        table_content_hashes[table_name] = str(audit_result["content_sha256"])

    if issues:
        raise DatasetValidationError(issues)
    return ValidationReport(
        dataset_id=str(manifest["dataset_id"]),
        manifest_path=manifest_path,
        table_row_counts=table_row_counts,
        table_content_sha256=table_content_hashes,
    )
