from benchmark.parsers.plan import analyze_plan


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
