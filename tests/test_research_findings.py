import json
from copy import deepcopy
from typing import Any

import pytest

from analysis.research_findings import ResearchFindingsError, build_research_findings
from benchmark.runner.summary import summarize_records


def _experiment(
    experiment_id: str,
    *,
    workload: str,
    query_id: str,
    speedup: float,
    coverage: float,
    fallback_operators: int,
    transitions: int,
    fallback_reasons: tuple[str, ...] = (),
    scale_factor: int | None = None,
    pairs: int = 3,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    records: list[dict[str, Any]] = []
    identity: dict[str, Any] = {
        "experiment_id": experiment_id,
        "workload": workload,
        "query_id": query_id,
        "storage_profile": "tpch_iceberg" if workload == "tpch" else "ecommerce_iceberg_rest",
    }
    if scale_factor is not None:
        identity["scale_factor"] = scale_factor
    for pair_index in range(1, pairs + 1):
        for engine, latency in (
            ("spark_baseline", 100.0 * speedup),
            ("comet_accelerated", 100.0),
        ):
            comet = engine == "comet_accelerated"
            records.append(
                {
                    **identity,
                    "run_id": f"{experiment_id}-{pair_index}-{engine}",
                    "pair_index": pair_index,
                    "phase": "measurement",
                    "status": "succeeded",
                    "engine": engine,
                    "metrics": {
                        "query_wall_time_ms": latency,
                        "collector_status": "complete",
                    },
                    "plan_analysis": {
                        "status": "complete",
                        "comet_native_operators": 10 - fallback_operators if comet else 0,
                        "spark_fallback_operators": fallback_operators if comet else 0,
                        "transition_count": transitions if comet else 0,
                        "native_coverage_ratio": coverage if comet else None,
                        "fallback_reasons": list(fallback_reasons) if comet else [],
                    },
                }
            )
    return records, summarize_records(records)


def _combine(
    experiments: list[tuple[list[dict[str, Any]], dict[str, Any]]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    records: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    for rows, summary in experiments:
        records.extend(rows)
        summaries.append(summary)
    return records, summaries


def _rq2_experiment(findings: dict[str, Any], experiment_id: str) -> dict[str, Any]:
    return next(
        row for row in findings["RQ2"]["experiments"] if row["experiment_id"] == experiment_id
    )


def test_run_ids_are_scoped_to_their_experiment() -> None:
    first = _experiment(
        "EXP-ECOM-SMALL-M02",
        workload="micro",
        query_id="M02",
        speedup=1.5,
        coverage=1.0,
        fallback_operators=0,
        transitions=0,
    )
    second = _experiment(
        "EXP-ECOM-SMALL-M04",
        workload="micro",
        query_id="M04",
        speedup=1.2,
        coverage=0.8,
        fallback_operators=2,
        transitions=1,
    )
    for rows, _summary in (first, second):
        for row in rows:
            row["run_id"] = f"measurement-p{row['pair_index']:04d}-{row['engine']}"
    records, summaries = _combine([first, second])

    findings = build_research_findings(records, summaries)

    assert findings["analysis_scope"]["experiment_count"] == 2


def test_h2_spearman_uses_average_ranks_for_ties_and_has_no_p_values() -> None:
    records, summaries = _combine(
        [
            _experiment(
                f"EXP-ECOM-SMALL-M0{index}",
                workload="micro",
                query_id=f"M0{index}",
                speedup=speedup,
                coverage=coverage,
                fallback_operators=fallback,
                transitions=transition,
            )
            for index, (speedup, coverage, fallback, transition) in enumerate(
                (
                    (1.0, 0.2, 8, 4),
                    (2.0, 0.2, 8, 4),
                    (2.0, 0.6, 4, 2),
                    (4.0, 0.8, 2, 1),
                ),
                start=1,
            )
        ]
    )

    findings = build_research_findings(records, summaries)

    native = findings["H2"]["overall"]["correlations"]["native_coverage_ratio"]
    assert native == {
        "n": 4,
        "rho": pytest.approx(5 / 6),
        "estimability": "estimable",
        "reason": None,
        "expected_direction": "positive",
    }
    assert findings["H2"]["method"] == "spearman_average_ranks_tie_aware"
    assert findings["H2"]["inferential_statistics"] == "not_computed"
    assert "p_value" not in json.dumps(findings["H2"], sort_keys=True)


def test_h2_marks_workload_strata_with_fewer_than_three_experiments_not_estimable() -> None:
    experiments = [
        _experiment(
            f"EXP-ECOM-SMALL-M0{index}",
            workload="micro",
            query_id=f"M0{index}",
            speedup=1.0 + index / 10,
            coverage=0.2 * index,
            fallback_operators=5 - index,
            transitions=index,
        )
        for index in range(1, 4)
    ]
    experiments.extend(
        [
            _experiment(
                f"EXP-ECOM-SMALL-B0{index}",
                workload="business",
                query_id=f"B0{index}",
                speedup=1.2 + index / 10,
                coverage=0.5 + index / 10,
                fallback_operators=3 - index,
                transitions=index,
            )
            for index in range(1, 3)
        ]
    )
    records, summaries = _combine(experiments)

    findings = build_research_findings(records, summaries)

    business = findings["H2"]["by_workload"]["business"]
    assert business["experiment_count"] == 2
    for correlation in business["correlations"].values():
        assert correlation["n"] == 2
        assert correlation["rho"] is None
        assert correlation["estimability"] == "not_estimable"
        assert correlation["reason"] == "fewer_than_3_experiments"
    assert findings["H2"]["by_data_scale"]["not_applicable"]["experiment_count"] == 5


def test_sf1_only_is_not_estimable_and_m08_empty_reasons_do_not_hide_fallback() -> None:
    records, summaries = _combine(
        [
            _experiment(
                "EXP-ECOM-SMALL-M08",
                workload="micro",
                query_id="M08",
                speedup=1.4,
                coverage=0.4,
                fallback_operators=6,
                transitions=2,
                fallback_reasons=(),
            ),
            _experiment(
                "EXP-TPCH-SF1-Q01",
                workload="tpch",
                query_id="Q01",
                speedup=1.3,
                coverage=0.8,
                fallback_operators=2,
                transitions=1,
                scale_factor=1,
            ),
            _experiment(
                "EXP-TPCH-SF1-Q03",
                workload="tpch",
                query_id="Q03",
                speedup=1.1,
                coverage=1.0,
                fallback_operators=0,
                transitions=1,
                scale_factor=1,
            ),
        ]
    )

    findings = build_research_findings(records, summaries)

    scale = findings["RQ3"]["scale_comparison"]
    assert scale["scales_present"] == [1]
    assert scale["estimability"] == "not_estimable"
    assert scale["reason_code"] == "sf10_absent"
    assert "SF10" in scale["reason"]
    assert findings["H3"]["assessment"] == "not_estimable"
    assert findings["H3"]["general_scalability_claim_allowed"] is False
    assert findings["H2"]["by_data_scale"]["SF1"]["experiment_count"] == 2

    m08 = _rq2_experiment(findings, "EXP-ECOM-SMALL-M08")
    comet = m08["engines"]["comet_accelerated"]
    assert comet["fallback_detected"] is True
    assert comet["fallback_reason_annotations"] == []
    assert comet["fallback_reason_annotation_status"] == "unannotated_fallback_detected"
    assert "do not mean no fallback" in comet["fallback_reason_annotation_note"]
    partial = findings["RQ3"]["partial_native_m08"]
    assert partial["status"] == "observed"
    assert partial["experiments"][0]["comet_plan"]["spark_fallback_operators"]["median"] == 6
    overhead = findings["RQ3"]["fallback_overhead"]
    assert overhead["assessment"] == "descriptive_only"
    assert overhead["causal_interpretation"] == "not_permitted"


def test_h1_classifies_fixed_summary_confidence_intervals_against_one() -> None:
    records, summaries = _combine(
        [
            _experiment(
                "EXP-SUPPORTED-M01",
                workload="micro",
                query_id="M01",
                speedup=1.5,
                coverage=1.0,
                fallback_operators=0,
                transitions=1,
            ),
            _experiment(
                "EXP-INCONCLUSIVE-M02",
                workload="micro",
                query_id="M02",
                speedup=1.0,
                coverage=0.8,
                fallback_operators=2,
                transitions=2,
            ),
            _experiment(
                "EXP-DECREASED-M03",
                workload="micro",
                query_id="M03",
                speedup=0.75,
                coverage=0.5,
                fallback_operators=5,
                transitions=3,
            ),
        ]
    )

    findings = build_research_findings(records, summaries)

    classifications = {
        row["experiment_id"]: row["classification"] for row in findings["H1"]["experiments"]
    }
    assert classifications == {
        "EXP-DECREASED-M03": "decreased",
        "EXP-INCONCLUSIVE-M02": "inconclusive",
        "EXP-SUPPORTED-M01": "supported",
    }
    assert findings["H1"]["classification_counts"] == {
        "supported": 1,
        "inconclusive": 1,
        "decreased": 1,
    }


def test_output_is_deterministic_finite_json_and_accepts_keyed_summaries() -> None:
    records, summaries = _combine(
        [
            _experiment(
                "EXP-ECOM-SMALL-M08",
                workload="micro",
                query_id="M08",
                speedup=1.4,
                coverage=0.4,
                fallback_operators=6,
                transitions=2,
            ),
            _experiment(
                "EXP-TPCH-SF1-Q01",
                workload="tpch",
                query_id="Q01",
                speedup=1.2,
                coverage=0.9,
                fallback_operators=1,
                transitions=1,
                scale_factor=1,
            ),
        ]
    )
    keyed = {summary["experiment_id"]: summary for summary in reversed(summaries)}

    first = build_research_findings(records, keyed)
    second = build_research_findings(reversed(records), reversed(summaries))

    assert first == second
    assert first["bootstrap"] == {
        "estimator": "median-paired-speedup",
        "confidence_level": 0.95,
        "resamples": 10_000,
        "seed": 20260824,
        "method": "percentile",
        "percentile_method": "linear-r7",
    }
    json.dumps(first, allow_nan=False, sort_keys=True)


@pytest.mark.parametrize("problem", ["failed", "partial", "wrong_seed"])
def test_builder_fails_closed_on_unpublishable_measurement_evidence(problem: str) -> None:
    records, summary = _experiment(
        "EXP-ECOM-SMALL-M08",
        workload="micro",
        query_id="M08",
        speedup=1.4,
        coverage=0.4,
        fallback_operators=6,
        transitions=2,
    )
    broken_records = deepcopy(records)
    broken_summary = deepcopy(summary)
    if problem == "failed":
        broken_records[0]["status"] = "failed"
    elif problem == "partial":
        broken_records[0]["plan_analysis"]["status"] = "partial"
    else:
        broken_summary["bootstrap"]["seed"] = 7

    with pytest.raises(ResearchFindingsError):
        build_research_findings(broken_records, [broken_summary])
