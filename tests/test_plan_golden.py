import json
import shutil
from pathlib import Path

import pytest

from benchmark.parsers.golden import (
    GoldenPlanError,
    load_golden_contract,
    verify_dataset_identity,
    verify_golden_bundle,
)
from benchmark.parsers.plan import operator_sequence, semantic_plan_sha256

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = (
    ROOT / "tests/golden_plans/spark-4.1.3_comet-1.0.0_iceberg-1.11.0/M02_filter/contract.json"
)


def _shadow_bundle(destination: Path) -> Path:
    contract = load_golden_contract(ROOT, CONTRACT)
    schema = ROOT / "benchmark/schemas/golden-plan-contract.schema.json"
    schema_target = destination / schema.relative_to(ROOT)
    schema_target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(schema, schema_target)

    for identity in contract["inputs"].values():
        source = ROOT / identity["path"]
        target = destination / identity["path"]
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)

    target_contract = destination / CONTRACT.relative_to(ROOT)
    target_contract.parent.mkdir(parents=True, exist_ok=True)
    for name in ("contract.json", "baseline-final-plan.txt", "comet-final-plan.txt"):
        shutil.copy2(CONTRACT.parent / name, target_contract.parent / name)
    return target_contract


def test_checked_in_golden_bundle_matches_locked_inputs_and_parser() -> None:
    contract = verify_golden_bundle(ROOT, CONTRACT)
    assert contract["workload_id"] == "M02"
    assert (
        contract["engines"]["comet_accelerated"]["expected_analysis"]["native_coverage_ratio"]
        == 1.0
    )


@pytest.mark.parametrize("target_name", ["runtime_lock", "workload_sql", "baseline_plan"])
def test_golden_bundle_rejects_one_byte_drift(tmp_path: Path, target_name: str) -> None:
    target_contract = _shadow_bundle(tmp_path)
    contract = verify_golden_bundle(tmp_path, target_contract)
    if target_name == "baseline_plan":
        target = target_contract.parent / contract["engines"]["spark_baseline"]["plan_file"]
    else:
        target = tmp_path / contract["inputs"][target_name]["path"]
    target.write_bytes(target.read_bytes() + b" ")
    with pytest.raises(GoldenPlanError, match="hash"):
        verify_golden_bundle(tmp_path, target_contract)


def test_dataset_identity_excludes_vcs_provenance_but_rejects_content_drift(
    tmp_path: Path,
) -> None:
    contract = load_golden_contract(ROOT, CONTRACT)
    expected = contract["dataset"]
    manifest = {
        "dataset_id": expected["dataset_id"],
        "generation_config_sha256": expected["generation_config_sha256"],
        "generator": {"git_commit": "volatile", "worktree_dirty": True},
        "tables": {"orders": expected["orders"].copy()},
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    verify_dataset_identity(contract, manifest_path)

    manifest["tables"]["orders"]["row_count"] += 1
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(GoldenPlanError, match="dataset identity drifted"):
        verify_dataset_identity(contract, manifest_path)


def test_semantic_plan_fingerprint_normalizes_ids_but_locks_filter_logic() -> None:
    path = CONTRACT.parent / "comet-final-plan.txt"
    plan = path.read_text(encoding="utf-8")
    volatile_variant = (
        plan.replace("snapshotId=5521914208012683423", "snapshotId=7")
        .replace("#14L", "#999L")
        .replace(
            "00005-d7ddeae6-6f4e-4a5a-9ab2-cb312159cd73.metadata.json",
            "00009-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee.metadata.json",
        )
    )
    assert semantic_plan_sha256(volatile_variant) == semantic_plan_sha256(plan)
    assert operator_sequence(volatile_variant) == operator_sequence(plan)
    assert semantic_plan_sha256(plan.replace("< 10", "< 50")) != semantic_plan_sha256(plan)
