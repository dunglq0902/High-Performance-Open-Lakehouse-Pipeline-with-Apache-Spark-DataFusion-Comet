"""Validation for runtime-locked physical-plan golden bundles."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

from jsonschema import Draft202012Validator

from benchmark.parsers.plan import analyze_plan, operator_sequence, semantic_plan_sha256
from benchmark.runner.canonical import sha256_file


class GoldenPlanError(ValueError):
    """A golden bundle no longer matches its reviewed inputs or parser result."""


def _safe_path(base: Path, relative: str, *, label: str) -> Path:
    candidate_path = Path(relative)
    candidate = (base / candidate_path).resolve()
    if candidate_path.is_absolute() or not candidate.is_relative_to(base.resolve()):
        raise GoldenPlanError(f"{label} escapes its allowed root: {relative}")
    if not candidate.is_file():
        raise GoldenPlanError(f"{label} does not exist: {candidate}")
    return candidate


def load_golden_contract(root: Path, contract_path: Path) -> dict[str, Any]:
    root = root.resolve()
    resolved_contract = contract_path.resolve()
    if not resolved_contract.is_relative_to(root):
        raise GoldenPlanError(f"golden contract is outside the repository: {resolved_contract}")
    contract = cast(dict[str, Any], json.loads(resolved_contract.read_text(encoding="utf-8")))
    schema = json.loads(
        (root / "benchmark/schemas/golden-plan-contract.schema.json").read_text(encoding="utf-8")
    )
    errors = sorted(
        Draft202012Validator(schema).iter_errors(contract), key=lambda item: list(item.path)
    )
    if errors:
        messages = [
            f"{'.'.join(map(str, error.path)) or '<root>'}: {error.message}" for error in errors
        ]
        raise GoldenPlanError(
            "golden contract schema validation failed:\n- " + "\n- ".join(messages)
        )
    return contract


def verify_golden_bundle(root: Path, contract_path: Path) -> dict[str, Any]:
    """Verify immutable inputs, captured plans, and their exact semantic analyses."""

    root = root.resolve()
    resolved_contract = contract_path.resolve()
    contract = load_golden_contract(root, resolved_contract)

    for name, identity in cast(dict[str, dict[str, str]], contract["inputs"]).items():
        path = _safe_path(root, identity["path"], label=f"golden input {name}")
        actual_hash = sha256_file(path)
        if actual_hash != identity["sha256"]:
            raise GoldenPlanError(f"golden input {name} hash {actual_hash} != {identity['sha256']}")

    engines = cast(dict[str, dict[str, Any]], contract["engines"])
    expected_flags = {"spark_baseline": False, "comet_accelerated": True}
    for engine_name, expected_flag in expected_flags.items():
        engine = engines[engine_name]
        if engine["comet_enabled"] is not expected_flag:
            raise GoldenPlanError(f"golden engine {engine_name} has an invalid Comet flag")
        plan_path = _safe_path(
            resolved_contract.parent,
            cast(str, engine["plan_file"]),
            label=f"golden plan {engine_name}",
        )
        actual_hash = sha256_file(plan_path)
        if actual_hash != engine["plan_sha256"]:
            raise GoldenPlanError(
                f"golden plan {engine_name} hash {actual_hash} != {engine['plan_sha256']}"
            )
        analysis = analyze_plan(plan_path.read_text(encoding="utf-8"), comet_enabled=expected_flag)
        if analysis != engine["expected_analysis"]:
            raise GoldenPlanError(
                f"golden plan {engine_name} analysis drifted: "
                f"actual={analysis!r}, expected={engine['expected_analysis']!r}"
            )
        plan_text = plan_path.read_text(encoding="utf-8")
        semantic_hash = semantic_plan_sha256(plan_text)
        if semantic_hash != engine["semantic_sha256"]:
            raise GoldenPlanError(
                f"golden plan {engine_name} semantic hash {semantic_hash} "
                f"!= {engine['semantic_sha256']}"
            )
        sequence = operator_sequence(plan_text)
        if sequence != engine["operator_sequence"]:
            raise GoldenPlanError(
                f"golden plan {engine_name} operator sequence {sequence!r} "
                f"!= {engine['operator_sequence']!r}"
            )
    return contract


def verify_dataset_identity(contract: dict[str, Any], manifest_path: Path) -> None:
    """Compare only deterministic dataset identity fields, excluding VCS provenance."""

    manifest = cast(dict[str, Any], json.loads(manifest_path.read_text(encoding="utf-8")))
    expected = cast(dict[str, Any], contract["dataset"])
    orders = cast(dict[str, Any], manifest.get("tables", {}).get("orders", {}))
    actual = {
        "manifest_path": expected["manifest_path"],
        "dataset_id": manifest.get("dataset_id"),
        "generation_config_sha256": manifest.get("generation_config_sha256"),
        "orders": {
            "schema_sha256": orders.get("schema_sha256"),
            "content_sha256": orders.get("content_sha256"),
            "row_count": orders.get("row_count"),
        },
    }
    if actual != expected:
        raise GoldenPlanError(
            f"golden dataset identity drifted: actual={actual!r}, expected={expected!r}"
        )
