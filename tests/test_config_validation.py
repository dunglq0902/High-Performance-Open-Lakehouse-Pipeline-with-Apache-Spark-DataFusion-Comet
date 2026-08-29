from copy import deepcopy
from pathlib import Path

import pytest
import yaml

from benchmark.runner.config import (
    ConfigurationError,
    load_document,
    redact,
    resolve_environment,
    validate_engine_matrix,
    validate_runtime_profile,
)
from scripts.run_research_suite import ECOMMERCE_CORE_CONFIGS

ROOT = Path(__file__).resolve().parents[1]
EXPERIMENT_SCHEMA = ROOT / "benchmark/schemas/experiment-config.schema.json"
SMOKE_CONFIG = ROOT / "benchmark/configs/smoke-m02.yaml"


def valid_config() -> dict:
    return {
        "spark": {
            "common_conf": {
                "spark.sql.session.timeZone": "UTC",
                "spark.memory.offHeap.enabled": True,
            }
        },
        "matrix": {
            "engines": [
                {"name": "spark_baseline", "spark_conf": {}},
                {
                    "name": "comet_accelerated",
                    "spark_conf": {
                        "spark.plugins": "org.apache.spark.CometPlugin",
                        "spark.shuffle.manager": (
                            "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager"
                        ),
                        "spark.comet.enabled": True,
                        "spark.comet.exec.enabled": True,
                        "spark.comet.nativeLoadRequired": True,
                        "spark.comet.shuffle.enabled": True,
                        "spark.comet.exec.strictFloatingPoint": True,
                        "spark.comet.parquet.write.enabled": False,
                    },
                },
            ]
        },
    }


def test_engine_matrix_accepts_only_reviewed_deltas() -> None:
    validate_engine_matrix(valid_config())
    config = deepcopy(valid_config())
    config["matrix"]["engines"][1]["spark_conf"]["spark.executor.memory"] = "99g"
    with pytest.raises(ConfigurationError, match="non-allowlisted"):
        validate_engine_matrix(config)


def test_baseline_cannot_hide_comet_keys_in_common_configuration() -> None:
    config = valid_config()
    config["spark"]["common_conf"]["spark.comet.enabled"] = True
    with pytest.raises(ConfigurationError, match="common_conf"):
        validate_engine_matrix(config)


def test_environment_resolution_and_redaction() -> None:
    value = {"endpoint": "${HOST}:9000", "fallback": "${MISSING:-safe}", "api_token": "secret"}
    assert resolve_environment(value, {"HOST": "minio"})["endpoint"] == "minio:9000"
    assert resolve_environment(value, {"HOST": "minio"})["fallback"] == "safe"
    assert redact(value)["api_token"] == "<redacted>"
    with pytest.raises(ConfigurationError, match="unresolved"):
        resolve_environment("${NO_VALUE}", {})


def test_document_loader_rejects_duplicate_yaml_keys(tmp_path: Path) -> None:
    document = tmp_path / "duplicate.yaml"
    schema = tmp_path / "schema.json"
    document.write_text("name: first\nname: second\n", encoding="utf-8")
    schema.write_text('{"type":"object"}', encoding="utf-8")
    with pytest.raises(ConfigurationError, match="duplicate"):
        load_document(document, schema)


@pytest.mark.parametrize("scale_factor", [1, 10])
def test_experiment_schema_accepts_active_scale_factors(tmp_path: Path, scale_factor: int) -> None:
    config = yaml.safe_load(SMOKE_CONFIG.read_text(encoding="utf-8"))
    config["workload"]["scale_factor"] = scale_factor
    document = tmp_path / "experiment.yaml"
    document.write_text(yaml.safe_dump(config), encoding="utf-8")

    assert load_document(document, EXPERIMENT_SCHEMA)["workload"]["scale_factor"] == scale_factor


@pytest.mark.parametrize("scale_factor", [50, 100])
def test_experiment_schema_rejects_out_of_scope_scale_factors(
    tmp_path: Path, scale_factor: int
) -> None:
    config = yaml.safe_load(SMOKE_CONFIG.read_text(encoding="utf-8"))
    config["workload"]["scale_factor"] = scale_factor
    document = tmp_path / "experiment.yaml"
    document.write_text(yaml.safe_dump(config), encoding="utf-8")

    with pytest.raises(ConfigurationError, match="schema validation failed"):
        load_document(document, EXPERIMENT_SCHEMA)


@pytest.mark.parametrize("runtime_profile", ["benchmark-single-node", "benchmark-scale-out"])
def test_experiment_schema_rejects_retired_runtime_profiles(
    tmp_path: Path, runtime_profile: str
) -> None:
    config = yaml.safe_load(SMOKE_CONFIG.read_text(encoding="utf-8"))
    config["spark"]["runtime_profile"] = runtime_profile
    document = tmp_path / "experiment.yaml"
    document.write_text(yaml.safe_dump(config), encoding="utf-8")

    with pytest.raises(ConfigurationError, match="schema validation failed"):
        load_document(document, EXPERIMENT_SCHEMA)


def test_experiment_schema_accepts_laptop_runtime_profile(tmp_path: Path) -> None:
    config = yaml.safe_load(SMOKE_CONFIG.read_text(encoding="utf-8"))
    config["spark"]["runtime_profile"] = "benchmark-laptop"
    document = tmp_path / "experiment.yaml"
    document.write_text(yaml.safe_dump(config), encoding="utf-8")

    assert load_document(document, EXPERIMENT_SCHEMA)["spark"]["runtime_profile"] == (
        "benchmark-laptop"
    )


def test_laptop_runtime_profile_matches_reviewed_property_files() -> None:
    config = yaml.safe_load(SMOKE_CONFIG.read_text(encoding="utf-8"))
    config["spark"]["runtime_profile"] = "benchmark-laptop"
    config["spark"]["common_conf"]["spark.sql.shuffle.partitions"] = 16

    validate_runtime_profile(config, ROOT)

    config["spark"]["common_conf"]["spark.executor.memory"] = "3g"
    with pytest.raises(ConfigurationError, match="differs from executable profile"):
        validate_runtime_profile(config, ROOT)


@pytest.mark.parametrize("relative_config", ECOMMERCE_CORE_CONFIGS)
def test_checked_in_laptop_config_is_schema_valid_and_runtime_exact(
    relative_config: str,
) -> None:
    config = load_document(ROOT / relative_config, EXPERIMENT_SCHEMA)
    validate_engine_matrix(config)
    validate_runtime_profile(config, ROOT)
    assert config["experiment"]["measurement_runs"] == 10
    assert config["experiment"]["warmup_runs"] == 2
    assert config["workload"]["dataset_manifest"].endswith("seed-20260827-v2/manifest.json")
