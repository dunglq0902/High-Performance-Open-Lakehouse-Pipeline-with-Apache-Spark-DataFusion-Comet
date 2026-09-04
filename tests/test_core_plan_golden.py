"""Offline regression checks for real plans, never primary benchmark evidence."""

from __future__ import annotations

import copy
import hashlib
import json
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

from benchmark.parsers.plan import analyze_plan, operator_sequence, semantic_plan_sha256
from benchmark.runner.canonical import sha256_file
from benchmark.runner.sql import render_sql, schema_hash

ROOT = Path(__file__).resolve().parents[1]
BUNDLE = ROOT / "tests/golden_plans/spark-4.1.3_comet-1.0.0_iceberg-1.11.0/core-preflight"
QUERY_IDS = ("M02", "M04", "M05", "M08", "M10", "B01", "Q01", "Q03", "Q06", "Q12")
ENGINES = ("spark_baseline", "comet_accelerated")
CONTRACT = json.loads((BUNDLE / "contract.json").read_text(encoding="utf-8"))


def _verify_identity(root: Path, identity: dict[str, Any]) -> Path:
    relative = Path(identity["path"])
    assert not relative.is_absolute() and ".." not in relative.parts
    path = root / relative
    assert sha256_file(path) == identity["sha256"], f"input hash drift: {relative}"
    return path


def _verify_inputs(root: Path, query_id: str, entry: dict[str, Any]) -> dict[str, Any]:
    paths = {name: _verify_identity(root, identity) for name, identity in entry["inputs"].items()}
    assert set(paths) == {"experiment_config", "workload_manifest", "workload_sql"}
    config = yaml.safe_load(paths["experiment_config"].read_text(encoding="utf-8"))
    workload = yaml.safe_load(paths["workload_manifest"].read_text(encoding="utf-8"))
    assert config["workload"]["query_id"] == workload["id"] == query_id
    assert config["workload"]["suite"] == workload["suite"] == entry["suite"]
    assert root / config["workload"]["manifest_file"] == paths["workload_manifest"]
    assert root / config["workload"]["sql_file"] == paths["workload_sql"]
    assert (paths["workload_manifest"].parent / workload["sql_file"]).resolve() == paths[
        "workload_sql"
    ].resolve()
    assert {engine["name"] for engine in config["matrix"]["engines"]} == set(ENGINES)
    # Source dataset identity is descriptive: no generated data is needed to parse a plan.
    dataset = entry["dataset_source_identity"]
    assert config["workload"]["dataset_manifest"] == dataset["manifest_path"]
    assert re.fullmatch(r"[0-9a-f]{64}", dataset["manifest_sha256"])
    assert dataset["dataset_id"] and dataset["verification_scope"]
    rendered = render_sql(
        paths["workload_sql"].read_text(encoding="utf-8"),
        workload["parameters"],
        config["workload"]["parameters"],
    )
    assert hashlib.sha256(rendered.encode("utf-8")).hexdigest() == entry["rendered_sql_sha256"]
    return workload


def _verify_plan(bundle: Path, query_id: str, engine: str, record: dict[str, Any]) -> None:
    assert record["plan_file"] == f"{engine}/{query_id}/final-plan.txt"
    path = bundle / record["plan_file"]
    assert sha256_file(path) == record["plan_sha256"], "plan byte hash drift"
    plan = path.read_text(encoding="utf-8")
    assert record["comet_enabled"] is (engine == "comet_accelerated")
    analysis = analyze_plan(plan, comet_enabled=record["comet_enabled"])
    assert semantic_plan_sha256(plan) == record["semantic_sha256"], "semantic hash drift"
    assert operator_sequence(plan) == record["operator_sequence"], "operator sequence drift"
    assert analysis == record["expected_analysis"], "plan analysis drift"
    assert analysis["status"] == "complete"
    assert analysis["unknown_nodes"] == []
    assert analysis["total_operators"] > 0
    if engine == "spark_baseline":
        assert analysis["comet_native_operators"] == 0
        assert analysis["spark_fallback_operators"] == 0
        assert analysis["native_coverage_ratio"] is None
    else:
        assert analysis["comet_native_operators"] > 0


def _verify_observations(entry: dict[str, Any], workload: dict[str, Any]) -> None:
    observed_schema = schema_hash(entry["observed_schema_json"])
    assert observed_schema == workload["expected_schema_hash"]
    paired = []
    for engine in ENGINES:
        observation = entry["engines"][engine]["diagnostic_observation"]
        assert observation["publishable"] is False
        assert observation["captured_status"] == "passed"
        assert observation["schema_matches"] is True
        assert observation["schema_sha256"] == observed_schema
        assert type(observation["row_count"]) is int and observation["row_count"] >= 0
        for key in ("schema_sha256", "canonical_result_sha256", "source_result_sha256"):
            assert re.fullmatch(r"[0-9a-f]{64}", observation[key])
        paired.append(
            tuple(
                observation[key]
                for key in ("schema_sha256", "canonical_result_sha256", "row_count")
            )
        )
    assert paired[0] == paired[1], "paired diagnostic output drift"


def test_core_corpus_is_exactly_ten_queries_by_two_engines_and_non_publishable() -> None:
    assert CONTRACT["schema_version"] == 1
    assert CONTRACT["artifact_class"] == "non-publishable-core-plan-regression-v1"
    for field in ("publishable", "performance_evidence", "primary_correctness_evidence"):
        assert CONTRACT[field] is False
    assert set(CONTRACT["queries"]) == set(QUERY_IDS)
    expected_files = {
        f"{engine}/{query_id}/final-plan.txt" for query_id in QUERY_IDS for engine in ENGINES
    }
    declared_files = set()
    for query_id in QUERY_IDS:
        engines = CONTRACT["queries"][query_id]["engines"]
        assert set(engines) == set(ENGINES)
        declared_files.update(record["plan_file"] for record in engines.values())
    assert declared_files == expected_files
    assert {path.relative_to(BUNDLE).as_posix() for path in BUNDLE.rglob("*.txt")} == (
        expected_files
    )


def test_core_diagnostic_runtime_and_provenance_match_locked_runtime() -> None:
    assert set(CONTRACT["inputs"]) == {"runtime_lock"}
    lock_path = _verify_identity(ROOT, CONTRACT["inputs"]["runtime_lock"])
    components = {item["name"]: item for item in json.loads(lock_path.read_text())["components"]}
    source = CONTRACT["source_diagnostic"]
    assert source["path"] == ".artifacts/diagnostics/plan-preflight-20260904-01"
    assert re.fullmatch(r"sha256:[0-9a-f]{64}", source["image_id"])
    assert re.fullmatch(r"[0-9a-f]{64}", source["notice_sha256"])
    assert re.fullmatch(r"[0-9a-f]{64}", source["launch_notice_parser_source_sha256"])
    assert re.fullmatch(r"[0-9a-f]{40}", source["observed_git_head_not_primary_provenance"])
    assert set(source["sessions"]) == set(ENGINES)
    parser_hashes = set()
    for engine, session in source["sessions"].items():
        assert session["uid"] == 185
        assert re.fullmatch(r"[0-9a-f]{64}", session["session_sha256"])
        assert re.fullmatch(r"[0-9a-f]{64}", session["captured_parser_source_sha256"])
        parser_hashes.add(session["captured_parser_source_sha256"])
        runtime = session["runtime"]
        assert runtime["runtime_lock_sha256"] == CONTRACT["inputs"]["runtime_lock"]["sha256"]
        for field, component in (
            ("spark_version", "apache-spark"),
            ("scala_version", "scala"),
            ("java_runtime_version", "java"),
            ("python_version", "python"),
            ("iceberg_version", "apache-iceberg-runtime"),
        ):
            assert runtime[field] == components[component]["version"]
        if engine == "comet_accelerated":
            assert runtime["comet_version"] == components["datafusion-comet"]["version"]
        else:
            assert runtime["comet_version"] is None
        for component, digest in runtime["artifact_sha256"].items():
            assert digest == components[component]["sha256_or_digest"]
    assert len(parser_hashes) == 1
    assert CONTRACT["review"]["parser_path"] == "benchmark/parsers/plan.py"
    assert re.fullmatch(r"[0-9a-f]{64}", CONTRACT["review"]["parser_source_sha256"])


@pytest.mark.parametrize("query_id", QUERY_IDS)
def test_core_query_inputs_and_observed_diagnostic_pair(query_id: str) -> None:
    entry = CONTRACT["queries"][query_id]
    workload = _verify_inputs(ROOT, query_id, entry)
    _verify_observations(entry, workload)


@pytest.mark.parametrize("query_id", QUERY_IDS)
@pytest.mark.parametrize("engine", ENGINES)
def test_core_final_plan_bytes_semantics_and_complete_analysis(query_id: str, engine: str) -> None:
    _verify_plan(BUNDLE, query_id, engine, CONTRACT["queries"][query_id]["engines"][engine])


def test_m08_real_plan_keeps_jvm_columnar_shuffle_in_fallback_denominator() -> None:
    record = CONTRACT["queries"]["M08"]["engines"]["comet_accelerated"]
    assert "CometColumnarExchange" in record["operator_sequence"]
    analysis = record["expected_analysis"]
    assert analysis["comet_native_operators"] == 4
    assert analysis["spark_fallback_operators"] == 6
    assert analysis["total_operators"] == 10
    assert analysis["native_coverage_ratio"] == 0.4


@pytest.mark.parametrize(
    "input_name", ["runtime_lock", "experiment_config", "workload_manifest", "workload_sql"]
)
def test_core_input_identity_rejects_one_byte_drift(tmp_path: Path, input_name: str) -> None:
    inputs = CONTRACT["inputs"] | CONTRACT["queries"]["M04"]["inputs"]
    identity = inputs[input_name]
    target = tmp_path / identity["path"]
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes((ROOT / identity["path"]).read_bytes() + b" ")
    with pytest.raises(AssertionError, match="input hash drift"):
        _verify_identity(tmp_path, identity)


def test_core_plan_identity_rejects_one_byte_drift(tmp_path: Path) -> None:
    record = CONTRACT["queries"]["M04"]["engines"]["comet_accelerated"]
    target = tmp_path / record["plan_file"]
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes((BUNDLE / record["plan_file"]).read_bytes() + b" ")
    with pytest.raises(AssertionError, match="plan byte hash drift"):
        _verify_plan(tmp_path, "M04", "comet_accelerated", record)


@pytest.mark.parametrize("field", ["semantic_sha256", "operator_sequence", "expected_analysis"])
def test_core_plan_contract_rejects_parser_expectation_drift(field: str) -> None:
    record = copy.deepcopy(CONTRACT["queries"]["M08"]["engines"]["comet_accelerated"])
    if field == "semantic_sha256":
        record[field] = "0" * 64
    elif field == "operator_sequence":
        record[field] = record[field][1:]
    else:
        record[field]["spark_fallback_operators"] = 0
    with pytest.raises(AssertionError, match="drift"):
        _verify_plan(BUNDLE, "M08", "comet_accelerated", record)


@pytest.mark.parametrize("field", ["schema_sha256", "canonical_result_sha256", "row_count"])
def test_core_observed_pair_rejects_identity_drift(field: str) -> None:
    entry = copy.deepcopy(CONTRACT["queries"]["M04"])
    observation = entry["engines"]["comet_accelerated"]["diagnostic_observation"]
    observation[field] = observation[field] + 1 if field == "row_count" else "0" * 64
    workload = yaml.safe_load(
        (ROOT / entry["inputs"]["workload_manifest"]["path"]).read_text(encoding="utf-8")
    )
    with pytest.raises(AssertionError):
        _verify_observations(entry, workload)
