"""Compare baseline and Comet readiness artifacts exactly."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, cast

from benchmark.parsers.golden import verify_dataset_identity, verify_golden_bundle
from benchmark.parsers.plan import analyze_plan, operator_sequence, semantic_plan_sha256
from benchmark.runner.canonical import write_json


def _load_result(path: Path) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(path.read_text(encoding="utf-8")))


def _final_plan_path(result_path: Path, result: dict[str, Any]) -> Path:
    relative = Path(cast(str, result["artifacts"]["final_plan"]))
    base = result_path.parent.resolve()
    candidate = (base / relative).resolve()
    if relative.is_absolute() or not candidate.is_relative_to(base) or not candidate.is_file():
        raise ValueError(f"invalid final-plan artifact path: {relative}")
    return candidate


def _runtime_matches(
    result: dict[str, Any],
    expected: dict[str, str],
    *,
    expected_comet_version: str | None,
    runtime_lock_sha256: str,
) -> bool:
    runtime = cast(dict[str, Any], result["runtime"])
    fields = (
        "spark_version",
        "scala_version",
        "java_runtime_version",
        "python_version",
        "iceberg_version",
    )
    return (
        all(runtime.get(field) == expected[field] for field in fields)
        and runtime.get("comet_version") == expected_comet_version
        and runtime.get("runtime_lock_sha256") == runtime_lock_sha256
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--comet", type=Path, required=True)
    parser.add_argument("--golden-contract", type=Path, required=True)
    parser.add_argument("--dataset-manifest", type=Path, required=True)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    root = args.repo_root.resolve()
    contract = verify_golden_bundle(root, args.golden_contract)
    expected_manifest = (root / cast(str, contract["dataset"]["manifest_path"])).resolve()
    if args.dataset_manifest.resolve() != expected_manifest:
        raise ValueError(
            f"dataset manifest {args.dataset_manifest.resolve()} != golden path {expected_manifest}"
        )
    verify_dataset_identity(contract, args.dataset_manifest)

    baseline = _load_result(args.baseline)
    comet = _load_result(args.comet)
    baseline_plan = _final_plan_path(args.baseline, baseline).read_text(encoding="utf-8")
    comet_plan = _final_plan_path(args.comet, comet).read_text(encoding="utf-8")
    baseline_fresh = analyze_plan(baseline_plan, comet_enabled=False)
    comet_fresh = analyze_plan(comet_plan, comet_enabled=True)
    engines = cast(dict[str, dict[str, Any]], contract["engines"])
    correctness = cast(dict[str, Any], contract["correctness"])
    runtime = cast(dict[str, str], contract["runtime"])
    input_identities = cast(dict[str, dict[str, str]], contract["inputs"])
    runtime_lock_sha256 = input_identities["runtime_lock"]["sha256"]
    checks = {
        "both_passed": baseline["status"] == comet["status"] == "passed",
        "schema_equal": baseline["schema_sha256"] == comet["schema_sha256"],
        "row_count_equal": baseline["row_count"] == comet["row_count"],
        "result_equal": baseline["canonical_result_sha256"] == comet["canonical_result_sha256"],
        "snapshot_equal": baseline["iceberg_snapshot_id"] == comet["iceberg_snapshot_id"],
        "comet_native_operator_present": comet["plan_analysis"]["comet_native_operators"] > 0,
        "baseline_has_no_native_operator": baseline["plan_analysis"]["comet_native_operators"] == 0,
        "baseline_plan_artifact_consistent": baseline["plan_analysis"] == baseline_fresh,
        "comet_plan_artifact_consistent": comet["plan_analysis"] == comet_fresh,
        "baseline_golden_analysis": baseline_fresh
        == engines["spark_baseline"]["expected_analysis"],
        "comet_golden_analysis": comet_fresh == engines["comet_accelerated"]["expected_analysis"],
        "baseline_golden_semantics": semantic_plan_sha256(baseline_plan)
        == engines["spark_baseline"]["semantic_sha256"],
        "comet_golden_semantics": semantic_plan_sha256(comet_plan)
        == engines["comet_accelerated"]["semantic_sha256"],
        "baseline_golden_operator_sequence": operator_sequence(baseline_plan)
        == engines["spark_baseline"]["operator_sequence"],
        "comet_golden_operator_sequence": operator_sequence(comet_plan)
        == engines["comet_accelerated"]["operator_sequence"],
        "baseline_runtime_locked": _runtime_matches(
            baseline,
            runtime,
            expected_comet_version=None,
            runtime_lock_sha256=runtime_lock_sha256,
        ),
        "comet_runtime_locked": _runtime_matches(
            comet,
            runtime,
            expected_comet_version=runtime["comet_version"],
            runtime_lock_sha256=runtime_lock_sha256,
        ),
        "workload_identity_locked": (
            baseline["workload_id"] == comet["workload_id"] == contract["workload_id"]
            and baseline["sql_sha256"]
            == comet["sql_sha256"]
            == input_identities["workload_sql"]["sha256"]
            and baseline["workload_manifest_sha256"]
            == comet["workload_manifest_sha256"]
            == input_identities["workload_manifest"]["sha256"]
        ),
        "expected_schema": baseline["schema_sha256"]
        == comet["schema_sha256"]
        == correctness["schema_sha256"],
        "expected_result": baseline["canonical_result_sha256"]
        == comet["canonical_result_sha256"]
        == correctness["canonical_result_sha256"],
        "expected_result_row_count": baseline["row_count"]
        == comet["row_count"]
        == correctness["row_count"],
    }
    status = "passed" if all(checks.values()) else "failed"
    write_json(args.output, {"schema_version": 1, "status": status, "checks": checks})
    if status != "passed":
        raise SystemExit(1)
    print("Spark and Comet smoke results match; native Comet execution is present.")


if __name__ == "__main__":
    main()
