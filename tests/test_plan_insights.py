from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import pytest

from analysis.plan_insights import PlanInsightsError, build_plan_insights
from benchmark.parsers.plan import analyze_plan, semantic_plan_sha256

BASELINE_PLAN = """\
TakeOrderedAndProject(limit=5000)
+- Exchange hashpartitioning(key#1L, 16), ENSURE_REQUIREMENTS, [plan_id=77]
   +- BroadcastHashJoin [key#1L], [key#2L], Inner, BuildRight, false
      :- BatchScan table.left[key#1L], 12
      +- BatchScan table.right[key#2L], 8
"""

BASELINE_FINAL_VARIANT = """\
TakeOrderedAndProject(limit=5000)
+- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=91]
   +- SortMergeJoin [key#3L], [key#4L], Inner
      :- BatchScan table.left[key#3L], 99
      +- BatchScan table.right[key#4L], 100
"""

COMET_PLAN_ANNOTATED = """\
CometProject [key#1L]
+- CometHashJoin [key#1L], [key#2L], Inner
   :- CometIcebergNativeScan [key#1L], table.left
   +- Project [key#2L] [COMET: unsupported decimal expression]
      +- BatchScan table.right[key#2L]
"""

COMET_PLAN_UNANNOTATED = """\
CometProject [key#3L]
+- CometExchange hashpartitioning(key#3L, 32), ENSURE_REQUIREMENTS, CometNativeShuffle
   +- SortMergeJoin [key#3L], [key#4L], Inner
      :- CometIcebergNativeScan [key#3L], table.left
      +- BatchScan table.right[key#4L]
"""


def _write_plans(root: Path, name: str, initial: str, final: str) -> str:
    directory = root / "plans" / name
    directory.mkdir(parents=True)
    (directory / "initial-plan.txt").write_text(initial, encoding="utf-8")
    (directory / "final-plan.txt").write_text(final, encoding="utf-8")
    return (directory / "final-plan.txt").relative_to(root).as_posix()


def _record(
    root: Path,
    *,
    engine: str,
    pair_index: int,
    initial: str,
    final: str,
    latency: float,
) -> dict[str, Any]:
    path = _write_plans(root, f"{engine}-{pair_index}", initial, final)
    comet_enabled = engine == "comet_accelerated"
    return {
        "schema_version": 1,
        "experiment_id": "EXP-Q03",
        "run_id": f"measurement-{engine}-{pair_index}",
        "pair_index": pair_index,
        "phase": "measurement",
        "status": "succeeded",
        "failure": None,
        "engine": engine,
        "workload": "tpch",
        "query_id": "Q03",
        "storage_profile": "tpch_iceberg",
        "provenance": {
            "git_commit": "a" * 40,
            "container_image_digest": "sha256:" + "b" * 64,
            "dataset_manifest_sha256": "c" * 64,
            "spark_conf_sha256": ("d" if comet_enabled else "e") * 64,
            "sql_sha256": "f" * 64,
            "iceberg_snapshot_ids": [1, 2],
        },
        "runtime": {
            "spark_version": "4.1.3",
            "scala_version": "2.13.17",
            "java_version": "17.0.19",
            "comet_version": "1.0.0" if comet_enabled else None,
            "iceberg_version": "1.11.0",
        },
        "resources": {
            "cpu_model": "test CPU",
            "allocated_cores": 2,
            "cgroup_memory_limit_mib": 5120,
            "executor_heap_mib": 2048,
            "off_heap_mib": 1024,
        },
        "metrics": {"query_wall_time_ms": latency},
        "plan_analysis": analyze_plan(final, comet_enabled=comet_enabled),
        "artifacts": {"physical_plan": path},
    }


def _records(root: Path) -> list[dict[str, Any]]:
    return [
        _record(
            root,
            engine="spark_baseline",
            pair_index=1,
            initial=BASELINE_PLAN,
            final=BASELINE_PLAN,
            latency=20.0,
        ),
        _record(
            root,
            engine="comet_accelerated",
            pair_index=1,
            initial=COMET_PLAN_ANNOTATED,
            final=COMET_PLAN_ANNOTATED,
            latency=10.0,
        ),
        _record(
            root,
            engine="spark_baseline",
            pair_index=2,
            initial=BASELINE_PLAN,
            final=BASELINE_FINAL_VARIANT,
            latency=30.0,
        ),
        _record(
            root,
            engine="comet_accelerated",
            pair_index=2,
            initial=COMET_PLAN_ANNOTATED,
            final=COMET_PLAN_UNANNOTATED,
            latency=15.0,
        ),
    ]


def test_builds_plan_stability_structure_metrics_and_paired_hash_strata(
    tmp_path: Path,
) -> None:
    records = _records(tmp_path)

    result = build_plan_insights(records, tmp_path)

    assert result["schema_version"] == 1
    assert result["bootstrap"]["seed"] == 20260824
    assert result["run_counts"] == {
        "measurement_records": 4,
        "experiments": 1,
        "complete_pairs": 2,
        "by_engine": {"spark_baseline": 2, "comet_accelerated": 2},
    }
    experiment = result["experiments"]["EXP-Q03"]
    assert experiment["pair_count"] == 2
    assert experiment["paired_final_plan_stable"] is False
    assert experiment["paired_final_plan_hash_combination_count"] == 2
    paired = experiment["paired_final_plan_hash_strata"]
    assert [stratum["n"] for stratum in paired] == [1, 1]
    assert {stratum["paired_speedup"]["median"] for stratum in paired} == {2.0}
    assert all(stratum["paired_speedup"]["median_ci_95"] is not None for stratum in paired)

    baseline = experiment["engines"]["spark_baseline"]
    assert baseline["plans"]["initial"]["stable"] is True
    assert baseline["plans"]["final"]["stable"] is False
    assert baseline["plans"]["final"]["distinct_hash_count"] == 2
    initial_stratum = baseline["plans"]["initial"]["hash_strata"][0]
    assert initial_stratum["semantic_sha256"] == semantic_plan_sha256(BASELINE_PLAN)
    assert initial_stratum["latency_ms"]["n"] == 2
    structure = initial_stratum["structure"]
    assert structure["operator_sequence"] == [
        "TakeOrderedAndProject",
        "Exchange",
        "BroadcastHashJoin",
        "BatchScan",
        "BatchScan",
    ]
    assert structure["join_strategies"] == {
        "values": ["BroadcastHashJoin"],
        "counts": {"BroadcastHashJoin": 1},
    }
    assert structure["partition_counts"] == {"values": [16], "counts": {"16": 1}}
    assert structure["scan_implementations"] == {
        "values": ["BatchScan"],
        "counts": {"BatchScan": 2},
    }
    assert baseline["final_plan_metrics"]["transition_count"]["n"] == 2
    assert baseline["final_plan_metrics"]["transition_count"]["median_ci_95"] is not None


def test_infers_locked_tpch_scale_from_experiment_identity(tmp_path: Path) -> None:
    records = _records(tmp_path)
    for record in records:
        record["experiment_id"] = "EXP-TPCH-SF1-Q03"

    result = build_plan_insights(records, tmp_path)

    assert result["experiments"]["EXP-TPCH-SF1-Q03"]["identity"]["scale_factor"] == 1


def test_preserves_fallback_annotations_and_marks_unannotated_fallback(tmp_path: Path) -> None:
    result = build_plan_insights(_records(tmp_path), tmp_path)

    comet = result["experiments"]["EXP-Q03"]["engines"]["comet_accelerated"]
    annotations = comet["fallback_annotations"]
    assert annotations["reasons"] == [
        {"reason": "unsupported decimal expression", "record_count": 1}
    ]
    assert annotations["records"] == [
        {
            "pair_index": 1,
            "spark_fallback_operators": 2,
            "reasons": ["unsupported decimal expression"],
            "unannotated_fallback": False,
        },
        {
            "pair_index": 2,
            "spark_fallback_operators": 2,
            "reasons": [],
            "unannotated_fallback": True,
        },
    ]
    assert annotations["unannotated_fallback"] is True
    final_strata = comet["plans"]["final"]["hash_strata"]
    unannotated = next(
        stratum for stratum in final_strata if stratum["structure"]["unannotated_fallback"]
    )
    assert unannotated["structure"]["partition_counts"] == {
        "values": [32],
        "counts": {"32": 1},
    }


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (lambda rows: rows[0].update(status="failed"), "status must equal"),
        (
            lambda rows: rows[0]["plan_analysis"].update(status="partial"),
            "plan_analysis.status",
        ),
        (lambda rows: rows[0].update(query_id="Q12"), "mixed experiment identities"),
        (lambda rows: rows.pop(), "incomplete Spark/Comet pairs"),
    ],
)
def test_fails_closed_on_non_success_partial_mixed_or_incomplete_measurements(
    tmp_path: Path, mutation: Any, match: str
) -> None:
    records = _records(tmp_path)
    mutation(records)

    with pytest.raises(PlanInsightsError, match=match):
        build_plan_insights(records, tmp_path)


def test_fails_closed_on_escaping_missing_or_tampered_plan_artifacts(tmp_path: Path) -> None:
    records = _records(tmp_path)
    escaped = copy.deepcopy(records)
    escaped[0]["artifacts"]["physical_plan"] = "../final-plan.txt"
    with pytest.raises(PlanInsightsError, match="canonical relative path"):
        build_plan_insights(escaped, tmp_path)

    missing = copy.deepcopy(records)
    missing[0]["artifacts"]["physical_plan"] = "plans/missing/final-plan.txt"
    with pytest.raises(PlanInsightsError, match="missing or cannot be resolved"):
        build_plan_insights(missing, tmp_path)

    physical = tmp_path / records[0]["artifacts"]["physical_plan"]
    physical.write_text("MysteryOperator [key#1L]\n", encoding="utf-8")
    with pytest.raises(PlanInsightsError, match="plan analysis is not complete"):
        build_plan_insights(records, tmp_path)
