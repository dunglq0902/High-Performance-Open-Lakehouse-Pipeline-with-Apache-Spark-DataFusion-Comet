from pathlib import Path

import pytest

from benchmark.parsers.plan import analyze_plan

M04_BASELINE_PLAN = (
    Path(__file__).parent
    / "fixtures/plans/spark-4.1.3_comet-1.0.0_iceberg-1.11.0/M04_baseline-final-plan.txt"
)


def test_fully_native_plan_counts_operators_and_transition() -> None:
    plan = """
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=true
+- CometColumnarToRow
   +- CometHashAggregate(keys=[region#1], functions=[sum(amount#2)])
      +- CometFilter (isnotnull(region#1))
         +- CometNativeScan parquet [region#1, amount#2]
"""
    result = analyze_plan(plan)
    assert result["status"] == "complete"
    assert result["comet_native_operators"] == 3
    assert result["spark_fallback_operators"] == 0
    assert result["transition_count"] == 1
    assert result["native_subtree_count"] == 1
    assert result["native_coverage_ratio"] == 1.0
    assert result["scan_implementations"] == ["CometNativeScan"]


def test_fallback_reason_is_preserved_and_unknown_node_is_partial() -> None:
    plan = """
CometColumnarToRow
+- Project [COMET: Native support for expression PythonUDF is disabled.]
   +- MysteryOperator value
      +- CometNativeScan parquet [a#1]
"""
    result = analyze_plan(plan)
    assert result["status"] == "partial"
    assert result["spark_fallback_operators"] == 1
    assert result["unknown_nodes"] == ["MysteryOperator"]
    assert result["fallback_reasons"] == ["Native support for expression PythonUDF is disabled."]


def test_baseline_plan_does_not_call_spark_operators_fallbacks() -> None:
    result = analyze_plan("FileScan parquet [id#1]", comet_enabled=False)
    assert result["total_operators"] == 1
    assert result["spark_fallback_operators"] == 0
    assert result["native_coverage_ratio"] is None


def test_aqe_parser_counts_only_the_final_plan() -> None:
    plan = """
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   ResultQueryStage 1
   +- CometHashAggregate [count#1L], [Final], [count(1)]
      +- CometIcebergNativeScan [id#1L]
+- == Initial Plan ==
   CometHashAggregate [count#1L], [Final], [count(1)]
   +- CometIcebergNativeScan [id#1L]
"""
    result = analyze_plan(plan)
    assert result["total_operators"] == 2
    assert result["comet_native_operators"] == 2
    assert result["native_subtree_count"] == 1


def test_observed_m04_broadcast_join_plan_is_complete() -> None:
    # Captured from the successful Spark 4.1.3 correctness application; the initial
    # AQE section must not be counted a second time.
    plan = M04_BASELINE_PLAN.read_text(encoding="utf-8")
    assert analyze_plan(plan, comet_enabled=False) == {
        "status": "complete",
        "total_operators": 6,
        "comet_native_operators": 0,
        "spark_fallback_operators": 0,
        "transition_count": 2,
        "native_subtree_count": 0,
        "native_coverage_ratio": None,
        "fallback_reasons": [],
        "unknown_nodes": [],
        "scan_implementations": ["BatchScan"],
    }


def test_observed_m04_spark_operators_are_fallbacks_when_comet_is_enabled() -> None:
    result = analyze_plan(M04_BASELINE_PLAN.read_text(encoding="utf-8"))
    assert result["status"] == "complete"
    assert result["total_operators"] == 6
    assert result["spark_fallback_operators"] == 6
    assert result["comet_native_operators"] == 0
    assert result["native_coverage_ratio"] == 0.0
    assert result["transition_count"] == 2
    assert result["unknown_nodes"] == []


@pytest.mark.parametrize(
    "node",
    [
        "BroadcastExchange",
        "BroadcastExchangeExec",
        "org.apache.spark.sql.execution.exchange.BroadcastExchangeExec",
    ],
)
def test_reviewed_broadcast_exchange_counts_as_spark_operator(node: str) -> None:
    result = analyze_plan(f"{node} HashedRelationBroadcastMode(List(input[0, bigint, false]))")
    assert result["status"] == "complete"
    assert result["total_operators"] == 1
    assert result["spark_fallback_operators"] == 1
    assert result["comet_native_operators"] == 0
    assert result["transition_count"] == 0
    assert result["unknown_nodes"] == []


@pytest.mark.parametrize(
    "node",
    [
        "BroadcastExchangeUnreviewed",
        "BroadcastExchangeUnreviewedExec",
        "org.apache.spark.sql.execution.exchange.BroadcastExchangeUnreviewedExec",
    ],
)
def test_unreviewed_broadcast_exchange_near_match_remains_partial(node: str) -> None:
    result = analyze_plan(f"{node} value")
    assert result["status"] == "partial"
    assert result["total_operators"] == 0
    assert result["spark_fallback_operators"] == 0
    assert result["comet_native_operators"] == 0
    assert result["unknown_nodes"] == ["BroadcastExchangeUnreviewed"]


@pytest.mark.parametrize("plan", ["", " \n", "== Physical Plan ==\n", "ResultQueryStage 0\n"])
def test_empty_or_operator_free_plan_is_unavailable(plan: str) -> None:
    result = analyze_plan(plan)
    assert result["status"] == "unavailable"
    assert result["total_operators"] == 0
    assert result["native_coverage_ratio"] is None


@pytest.mark.parametrize("flag", ["isFinalPlan=false", ""])
def test_unfinished_or_unconfirmed_aqe_plan_is_partial(flag: str) -> None:
    result = analyze_plan(f"AdaptiveSparkPlan {flag}\n+- BatchScan table\n")
    assert result["status"] == "partial"
    assert result["total_operators"] == 1


def test_discarded_initial_aqe_section_cannot_taint_final_analysis() -> None:
    plan = """
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   ResultQueryStage 0
   +- BatchScan table
+- == Initial Plan ==
   AdaptiveSparkPlan isFinalPlan=false
   +- UnknownInitialOperator value
"""
    result = analyze_plan(plan, comet_enabled=False)
    assert result["status"] == "complete"
    assert result["total_operators"] == 1
    assert result["unknown_nodes"] == []


@pytest.mark.parametrize("node", ["ProjectMystery", "FilterUnreviewedExec", "ExchangeUnknown"])
def test_spark_prefix_lookalikes_stay_unknown(node: str) -> None:
    result = analyze_plan(f"{node} value")
    assert result["status"] == "partial"
    assert result["total_operators"] == 0
    assert result["spark_fallback_operators"] == 0


def test_unparseable_child_is_not_silently_dropped() -> None:
    result = analyze_plan("Project [id]\n+- 123Unreviewed value\n")
    assert result["status"] == "partial"
    assert result["unknown_nodes"] == ["+- 123Unreviewed value"]


@pytest.mark.parametrize(
    "node",
    [
        "CometMystery",
        "CometScan",
        "CometScanWrapper",
        "CometSinkPlaceHolder",
        "CometObjectHashAggregate",
        "CometColumnarExchangeUnreviewed",
        "ColumnarToRowFuture",
        "CometSparkRowToColumnarFuture",
    ],
)
def test_unreviewed_or_intermediate_comet_nodes_remain_partial(node: str) -> None:
    result = analyze_plan(f"{node} value")
    assert result["status"] == "partial"
    assert result["total_operators"] == 0
    assert result["transition_count"] == 0
    assert result["comet_native_operators"] == 0
    assert result["unknown_nodes"] == [node]


@pytest.mark.parametrize(
    "node", ["CometWindow", "CometWindowExec", "org.apache.spark.sql.comet.CometWindowExec"]
)
def test_known_comet_operator_uses_exact_normalized_name(node: str) -> None:
    result = analyze_plan(f"{node} [x]")
    assert result["status"] == "complete"
    assert result["comet_native_operators"] == 1
    assert result["native_coverage_ratio"] == 1.0


@pytest.mark.parametrize(
    "transition",
    ["CometSparkColumnarToColumnar", "CometSparkRowToColumnar", "CometNativeColumnarToRow"],
)
def test_comet_data_conversion_is_transition_not_native_work(transition: str) -> None:
    result = analyze_plan(f"CometProject [id]\n+- {transition}\n   +- BatchScan table\n")
    assert result["status"] == "complete"
    assert result["total_operators"] == 2
    assert result["comet_native_operators"] == 1
    assert result["spark_fallback_operators"] == 1
    assert result["transition_count"] == 1
    assert result["native_coverage_ratio"] == 0.5


def test_non_native_comet_shuffle_counts_as_fallback_and_splits_native_subtrees() -> None:
    result = analyze_plan(
        "CometSort [id]\n+- CometColumnarExchange hashpartitioning(id,16)\n"
        "   +- CometIcebergNativeScan table\n"
    )
    assert result["status"] == "complete"
    assert result["total_operators"] == 3
    assert result["comet_native_operators"] == 2
    assert result["spark_fallback_operators"] == 1
    assert result["native_subtree_count"] == 2
    assert result["native_coverage_ratio"] == 2 / 3


def test_unfinished_nested_adaptive_wrapper_in_final_tree_stays_partial() -> None:
    plan = """
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   Project [id]
   +- AdaptiveSparkPlan isFinalPlan=false
      +- BatchScan table
+- == Initial Plan ==
   BatchScan table
"""
    assert analyze_plan(plan)["status"] == "partial"


@pytest.mark.parametrize("node", ["OutputMystery", "BatchedUnreviewed", "ArgumentsFuture"])
def test_annotation_prefix_lookalikes_are_not_silently_ignored(node: str) -> None:
    result = analyze_plan(f"{node} value\n+- BatchScan table\n")
    assert result["status"] == "partial"
    assert result["unknown_nodes"] == [node]
