from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
import pytest

from benchmark.runner.config import (
    load_document,
    load_experiment,
    validate_runtime_profile,
)
from benchmark.runner.sql import canonical_schema_json, render_sql, schema_hash
from data.generator.schemas import TABLE_SCHEMAS
from data.tpch.contract import TPCH_SCHEMAS

ROOT = Path(__file__).resolve().parents[1]
MANIFEST_ROOT = ROOT / "workloads" / "manifests"
WORKLOAD_SCHEMA = ROOT / "benchmark" / "schemas" / "workload-manifest.schema.json"
EXPERIMENT_SCHEMA_ROOT = ROOT / "benchmark" / "schemas"
CONFIG_ROOT = ROOT / "benchmark" / "configs"
CORE_IDS = {"M02", "M04", "M05", "M08", "M10", "B01"}
TPCH_CORE_IDS = {"Q01", "Q03", "Q06", "Q12"}
CORE_OPERATOR_BY_ID = {
    "M02": "filter",
    "M04": "join",
    "M05": "aggregate",
    "M08": "window",
    "M10": "shuffle",
    "B01": "join",
}


def _manifest_paths() -> list[Path]:
    return sorted(MANIFEST_ROOT.rglob("*.yaml"))


def _representative_parameters(manifest: dict[str, Any]) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for name, definition in manifest["parameters"].items():
        allowed = definition.get("allowed_values")
        if allowed:
            values[name] = allowed[0]
            continue
        value_by_type: dict[str, Any] = {
            "timestamp": "2025-01-01 00:00:00"
            if not name.endswith("end")
            else "2026-01-01 00:00:00",
            "date": "2025-01-01" if not name.endswith("end") else "2026-01-01",
            "integer": 1,
            "number": 1.0,
            "string": "representative",
            "boolean": True,
        }
        values[name] = value_by_type[definition["type"]]
    return values


def _spark_type(data_type: pa.DataType) -> str:
    if pa.types.is_int64(data_type):
        return "BIGINT"
    if pa.types.is_int32(data_type):
        return "INT"
    if pa.types.is_string(data_type):
        return "STRING"
    if pa.types.is_timestamp(data_type):
        return "TIMESTAMP"
    if pa.types.is_date32(data_type):
        return "DATE"
    if pa.types.is_decimal(data_type):
        return f"DECIMAL({data_type.precision},{data_type.scale})"
    raise AssertionError(f"unmapped generator type: {data_type}")


def test_every_workload_manifest_loads_renders_and_has_a_canonical_schema() -> None:
    manifests: list[dict[str, Any]] = []
    for manifest_path in _manifest_paths():
        manifest = load_document(manifest_path, WORKLOAD_SCHEMA)
        manifests.append(manifest)

        sql_path = (manifest_path.parent / manifest["sql_file"]).resolve()
        sql_path.relative_to(ROOT / "workloads")
        sql = sql_path.read_text(encoding="utf-8")
        rendered = render_sql(sql, manifest["parameters"], _representative_parameters(manifest))
        assert not re.search(r"{{\s*[A-Za-z_]", rendered)

        canonical_json = manifest["expected_schema"]["canonical_json"]
        assert canonical_schema_json(canonical_json) == canonical_json
        assert schema_hash(canonical_json) == manifest["expected_schema_hash"]
        schema_fields = [
            {
                "name": field["name"],
                "type": field["type"],
                "nullable": field["nullable"],
            }
            for field in json.loads(canonical_json)["fields"]
        ]
        assert schema_fields == manifest["expected_schema"]["fields"]

        for relation_name, binding in manifest["relation_bindings"].items():
            assert re.search(rf"\b{re.escape(relation_name)}\b", sql)
            schema_catalog = TPCH_SCHEMAS if manifest["suite"] == "tpch" else TABLE_SCHEMAS
            source_schema = schema_catalog[binding["logical_table"]]
            source_fields = {field.name: field for field in source_schema}
            for required in binding["required_columns"]:
                source_field = source_fields[required["name"]]
                assert required["type"] == _spark_type(source_field.type)
                assert required["nullable"] is source_field.nullable

    workload_ids = [manifest["id"] for manifest in manifests]
    assert len(workload_ids) == len(set(workload_ids))
    assert set(workload_ids) >= CORE_IDS


def test_core_catalog_covers_reviewed_operators_and_bounded_ordered_results() -> None:
    manifests = {
        manifest["id"]: (manifest_path, manifest)
        for manifest_path in _manifest_paths()
        for manifest in [load_document(manifest_path, WORKLOAD_SCHEMA)]
        if manifest["id"] in CORE_IDS
    }
    assert set(manifests) == CORE_IDS

    all_tags: set[str] = set()
    logical_tables: set[str] = set()
    for workload_id, (manifest_path, manifest) in manifests.items():
        tags = set(manifest["operator_tags"])
        all_tags.update(tags)
        logical_tables.update(
            binding["logical_table"] for binding in manifest["relation_bindings"].values()
        )
        assert CORE_OPERATOR_BY_ID[workload_id] in tags
        assert manifest["result_mode"] == "collect"

        sql = (manifest_path.parent / manifest["sql_file"]).read_text(encoding="utf-8")
        upper_sql = sql.upper()
        if workload_id != "M02":
            assert "ORDER BY" in upper_sql
            assert "LIMIT {{RESULT_LIMIT}}" in upper_sql
            assert manifest["correctness"]["ordering"] == "ordered"
        assert "/*+" not in sql
        assert "COMET" not in upper_sql
        assert "BROADCAST" not in upper_sql
        assert "SPARK." not in upper_sql

    assert {"scan", "filter", "join", "aggregate", "window", "shuffle", "sort", "limit"} <= all_tags
    assert logical_tables == set(TABLE_SCHEMAS)


def test_core_ordering_contains_stable_tie_breakers() -> None:
    expected_ordering_tokens = {
        "M04": ("oi.order_id", "oi.line_number", "oi.product_id"),
        "M05": ("status", "payment_method"),
        "M08": ("session_id", "event_rank", "event_time", "event_id"),
        "M10": ("event_count", "session_id", "customer_id_bucket", "device_type"),
        "B01": ("revenue_date", "region"),
    }
    for manifest_path in _manifest_paths():
        manifest = load_document(manifest_path, WORKLOAD_SCHEMA)
        if manifest["id"] not in expected_ordering_tokens:
            continue
        sql = (manifest_path.parent / manifest["sql_file"]).read_text(encoding="utf-8")
        order_by = sql.rsplit("ORDER BY", maxsplit=1)[1].split("LIMIT", maxsplit=1)[0]
        positions = [order_by.index(token) for token in expected_ordering_tokens[manifest["id"]]]
        assert positions == sorted(positions)


def test_every_core_workload_has_an_executable_laptop_config() -> None:
    configs: dict[str, dict[str, Any]] = {}
    for config_path in sorted(CONFIG_ROOT.glob("benchmark-laptop-*.yaml")):
        config = load_experiment(config_path, EXPERIMENT_SCHEMA_ROOT)
        validate_runtime_profile(config, ROOT)
        if config["workload"].get("scale_factor") == 10:
            continue
        query_id = config["workload"]["query_id"]
        assert query_id not in configs
        configs[query_id] = config

        manifest_path = ROOT / config["workload"]["manifest_file"]
        manifest = load_document(manifest_path, WORKLOAD_SCHEMA)
        assert manifest["id"] == query_id
        assert manifest["suite"] == config["workload"]["suite"]
        assert manifest["storage_profile"] == config["workload"]["storage_profile"]
        assert manifest["result_mode"] == config["workload"]["result_mode"]
        sql_path = ROOT / config["workload"]["sql_file"]
        assert (manifest_path.parent / manifest["sql_file"]).resolve() == sql_path.resolve()
        render_sql(
            sql_path.read_text(encoding="utf-8"),
            manifest["parameters"],
            config["workload"]["parameters"],
        )

    assert set(configs) == CORE_IDS | TPCH_CORE_IDS


def test_smoke_config_matches_its_executable_spark_profile() -> None:
    config = load_experiment(CONFIG_ROOT / "smoke-m02.yaml", EXPERIMENT_SCHEMA_ROOT)
    validate_runtime_profile(config, ROOT)


@pytest.mark.parametrize(
    "suffix,identity,pairs,seed",
    [
        ("", "SF10", 5, 20260827),
        ("r2-", "SF10-R2", 10, 20260923),
    ],
)
def test_sf10_configs_preserve_sf1_queries_and_runtime_with_separate_identity(
    suffix: str, identity: str, pairs: int, seed: int
) -> None:
    configs = sorted(CONFIG_ROOT.glob(f"benchmark-laptop-tpch-sf10-{suffix}q*.yaml"))
    assert len(configs) == 4
    queries = set()
    for path in configs:
        config = load_experiment(path, EXPERIMENT_SCHEMA_ROOT)
        validate_runtime_profile(config, ROOT)
        queries.add(config["workload"]["query_id"])
        baseline = load_experiment(
            path.with_name(path.name.replace(f"sf10-{suffix}", "")), EXPERIMENT_SCHEMA_ROOT
        )
        assert config["spark"] == baseline["spark"]
        assert config["matrix"] == baseline["matrix"]
        assert config["workload"]["parameters"] == baseline["workload"]["parameters"]
        assert config["workload"]["scale_factor"] == 10
        assert "sf10-v1" in config["workload"]["dataset_manifest"]
        assert config["experiment"]["id"] == baseline["experiment"]["id"].replace("SF1", identity)
        assert config["experiment"]["measurement_runs"] == pairs
        assert config["experiment"]["seed"] == seed
        assert config["experiment"]["warmup_runs"] == baseline["experiment"]["warmup_runs"]
        assert "exploratory" in config["experiment"]["labels"]
        assert "primary" not in config["experiment"]["labels"]
    assert queries == TPCH_CORE_IDS
