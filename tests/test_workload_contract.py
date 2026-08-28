import hashlib
import json
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator

from pipeline.smoke.prepare_iceberg import ORDERS_TABLE_DDL
from pipeline.smoke.run_workload import _load_iceberg_snapshot

ROOT = Path(__file__).resolve().parents[1]


def test_m02_manifest_and_schema_hash_are_self_consistent() -> None:
    manifest_path = ROOT / "workloads/manifests/micro/M02_filter.yaml"
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    schema = json.loads(
        (ROOT / "benchmark/schemas/workload-manifest.schema.json").read_text(encoding="utf-8")
    )
    Draft202012Validator(schema).validate(manifest)

    declared_sql = (manifest_path.parent / manifest["sql_file"]).resolve()
    assert declared_sql == (ROOT / "workloads/micro/M02_filter.sql").resolve()
    payload = manifest["expected_schema"]["canonical_json"].encode("utf-8")
    assert hashlib.sha256(payload).hexdigest() == manifest["expected_schema_hash"]


def test_iceberg_snapshot_uses_spark_41_version_as_of() -> None:
    class Reader:
        def __init__(self) -> None:
            self.calls: list[tuple[str, ...]] = []

        def format(self, value: str) -> "Reader":
            self.calls.append(("format", value))
            return self

        def option(self, name: str, value: str) -> "Reader":
            self.calls.append(("option", name, value))
            return self

        def load(self, table: str) -> object:
            self.calls.append(("load", table))
            return object()

    class Spark:
        def __init__(self, reader: Reader) -> None:
            self.read = reader

    reader = Reader()
    _load_iceberg_snapshot(Spark(reader), "lakehouse.bronze.orders", 12345)
    assert reader.calls == [
        ("format", "iceberg"),
        ("option", "versionAsOf", "12345"),
        ("load", "lakehouse.bronze.orders"),
    ]


def test_smoke_iceberg_table_preserves_required_columns() -> None:
    compact = " ".join(ORDERS_TABLE_DDL.upper().split())
    for name, data_type in (
        ("ORDER_ID", "BIGINT"),
        ("CUSTOMER_ID", "BIGINT"),
        ("ORDER_TIME", "TIMESTAMP"),
        ("STATUS", "STRING"),
        ("PAYMENT_METHOD", "STRING"),
    ):
        assert f"{name} {data_type} NOT NULL" in compact
