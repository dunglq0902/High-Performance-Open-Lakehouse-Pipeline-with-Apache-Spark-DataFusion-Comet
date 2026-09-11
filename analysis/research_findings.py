"""Deterministic, fail-closed findings for the reviewed research questions.

The public builder consumes already-admitted raw records and their rebuildable
per-experiment summaries.  It does not silently turn failed, partial, or unpaired
measurements into research evidence.  Correlations are deliberately descriptive:
no p-values or causal fallback-overhead estimates are manufactured from the small
experiment-level sample.
"""

from __future__ import annotations

import json
import math
import re
import statistics
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, TypeGuard

from benchmark.runner.statistics import (
    DEFAULT_BOOTSTRAP_RESAMPLES,
    DEFAULT_BOOTSTRAP_SEED,
    percentile,
)

_ENGINES = ("spark_baseline", "comet_accelerated")
_ENGINE_SET = frozenset(_ENGINES)
_NON_MEASUREMENT_PHASES = frozenset({"correctness", "plan_capture"})
_SCALE_PATTERN = re.compile(r"(?:^|[-_])SF(1|10)(?:[-_]|$)", re.IGNORECASE)
_PLAN_METRICS = (
    "native_coverage_ratio",
    "comet_native_operators",
    "spark_fallback_operators",
    "transition_count",
)
_CORRELATION_METRICS = (
    "native_coverage_ratio",
    "spark_fallback_operators",
    "transition_count",
)
_BOOTSTRAP = {
    "estimator": "median-paired-speedup",
    "confidence_level": 0.95,
    "resamples": DEFAULT_BOOTSTRAP_RESAMPLES,
    "seed": DEFAULT_BOOTSTRAP_SEED,
    "method": "percentile",
    "percentile_method": "linear-r7",
}

type JsonObject = dict[str, Any]
type SummaryInput = Iterable[Mapping[str, Any]] | Mapping[str, Mapping[str, Any]]


class ResearchFindingsError(ValueError):
    """A record or summary cannot safely support the research findings."""


@dataclass(frozen=True, slots=True)
class _Experiment:
    experiment_id: str
    workload: str
    query_id: str
    storage_profile: str
    scale_factor: int | None
    pair_count: int
    paired_speedup: JsonObject
    paired_speedup_ci: JsonObject
    engine_plans: dict[str, JsonObject]


def build_research_findings(
    records: Iterable[Mapping[str, Any]],
    summaries: SummaryInput,
) -> dict[str, Any]:
    """Build JSON-ready evidence for RQ1-RQ3 and H1-H3.

    Only successful measurement records with complete collector and physical-plan
    evidence are admissible.  Correctness and plan-capture rows are outside this
    analysis and are ignored.  Every admitted experiment must have exactly paired
    Spark/Comet records and exactly one complete fixed-bootstrap summary.
    """

    rows_by_experiment = _admit_measurements(records)
    summaries_by_experiment = _admit_summaries(summaries)
    record_ids = set(rows_by_experiment)
    summary_ids = set(summaries_by_experiment)
    if record_ids != summary_ids:
        missing = sorted(record_ids - summary_ids)
        extra = sorted(summary_ids - record_ids)
        raise ResearchFindingsError(
            "summary/measurement experiment sets differ "
            f"(missing summaries={missing}, extra summaries={extra})"
        )

    experiments = [
        _build_experiment(
            experiment_id,
            rows_by_experiment[experiment_id],
            summaries_by_experiment[experiment_id],
        )
        for experiment_id in sorted(rows_by_experiment)
    ]
    experiments.sort(key=_experiment_sort_key)

    rq1 = _build_rq1(experiments)
    rq2 = _build_rq2(experiments)
    scale_comparison = _build_scale_comparison(experiments)
    fallback_overhead = _build_fallback_overhead(experiments)
    partial_native_m08 = _build_partial_native_m08(experiments)
    result = {
        "schema_version": 1,
        "analysis_scope": {
            "record_filter": {
                "phase": "measurement",
                "status": "succeeded",
                "metrics.collector_status": "complete",
                "plan_analysis.status": "complete",
            },
            "grouping_keys": ["experiment_id", "workload", "query_id", "engine"],
            "measurement_record_count": sum(
                experiment.pair_count * len(_ENGINES) for experiment in experiments
            ),
            "experiment_count": len(experiments),
        },
        "bootstrap": dict(_BOOTSTRAP),
        "RQ1": rq1,
        "RQ2": rq2,
        "RQ3": {
            "analysis_type": "exploratory",
            "scale_comparison": scale_comparison,
            "fallback_overhead": fallback_overhead,
            "partial_native_m08": partial_native_m08,
        },
        "H1": _build_h1(experiments),
        "H2": _build_h2(experiments),
        "H3": _build_h3(scale_comparison),
    }
    _require_finite_json(result)
    return result


def _admit_measurements(
    records: Iterable[Mapping[str, Any]],
) -> dict[str, list[Mapping[str, Any]]]:
    rows_by_experiment: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    seen_run_ids: set[tuple[str, str]] = set()
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise ResearchFindingsError(f"record {index} must be a mapping")
        phase = record.get("phase")
        if phase in _NON_MEASUREMENT_PHASES:
            continue
        if phase != "measurement":
            raise ResearchFindingsError(f"record {index} has unsupported phase {phase!r}")
        status = record.get("status")
        if status != "succeeded":
            raise ResearchFindingsError(
                f"measurement record {index} must have status 'succeeded', got {status!r}"
            )

        experiment_id = _required_text(record.get("experiment_id"), f"record {index} experiment_id")
        _required_text(record.get("workload"), f"record {index} workload")
        _required_text(record.get("query_id"), f"record {index} query_id")
        _required_text(record.get("storage_profile"), f"record {index} storage_profile")
        engine = record.get("engine")
        if engine not in _ENGINE_SET:
            raise ResearchFindingsError(f"measurement record {index} has invalid engine {engine!r}")
        pair_index = record.get("pair_index")
        if not _is_integer(pair_index) or pair_index < 1:
            raise ResearchFindingsError(
                f"measurement record {index} must have a positive integer pair_index"
            )
        run_id = _required_text(record.get("run_id"), f"record {index} run_id")
        scoped_run_id = (experiment_id, run_id)
        if scoped_run_id in seen_run_ids:
            raise ResearchFindingsError(
                f"duplicate measurement run_id {run_id!r} in experiment {experiment_id!r}"
            )
        seen_run_ids.add(scoped_run_id)

        metrics = record.get("metrics")
        if not isinstance(metrics, Mapping):
            raise ResearchFindingsError(f"measurement record {index} has no metrics mapping")
        if metrics.get("collector_status") != "complete":
            raise ResearchFindingsError(
                f"measurement record {index} does not have complete collector evidence"
            )
        _positive_number(
            metrics.get("query_wall_time_ms"),
            f"measurement record {index} query_wall_time_ms",
        )
        _validate_plan(record.get("plan_analysis"), index=index, engine=str(engine))
        _scale_from_record(record, index=index)
        rows_by_experiment[experiment_id].append(record)

    if not rows_by_experiment:
        raise ResearchFindingsError("no successful complete measurement records were provided")
    return dict(rows_by_experiment)


def _validate_plan(value: object, *, index: int, engine: str) -> None:
    label = f"measurement record {index} plan_analysis"
    if not isinstance(value, Mapping):
        raise ResearchFindingsError(f"{label} must be a mapping")
    if value.get("status") != "complete":
        raise ResearchFindingsError(f"{label} must have status 'complete'")
    for field in ("comet_native_operators", "spark_fallback_operators", "transition_count"):
        count = value.get(field)
        if not _is_integer(count) or count < 0:
            raise ResearchFindingsError(f"{label}.{field} must be a non-negative integer")
    coverage = value.get("native_coverage_ratio")
    if coverage is not None:
        numeric = _finite_number(coverage, f"{label}.native_coverage_ratio")
        if not 0.0 <= numeric <= 1.0:
            raise ResearchFindingsError(f"{label}.native_coverage_ratio must be within [0, 1]")
    elif engine == "comet_accelerated":
        raise ResearchFindingsError(f"{label}.native_coverage_ratio is required for Comet")
    reasons = value.get("fallback_reasons")
    if not isinstance(reasons, list) or any(
        not isinstance(reason, str) or not reason.strip() for reason in reasons
    ):
        raise ResearchFindingsError(f"{label}.fallback_reasons must contain non-empty strings")


def _admit_summaries(summaries: SummaryInput) -> dict[str, Mapping[str, Any]]:
    keyed_input: Mapping[str, Mapping[str, Any]] | None = None
    if isinstance(summaries, Mapping) and "experiment_id" not in summaries:
        keyed_input = summaries
        values: list[object] = [summaries[key] for key in sorted(summaries)]
    elif isinstance(summaries, Mapping):
        values = [summaries]
    else:
        values = list(summaries)

    result: dict[str, Mapping[str, Any]] = {}
    for index, value in enumerate(values):
        if not isinstance(value, Mapping):
            raise ResearchFindingsError(f"summary {index} must be a mapping")
        experiment_id = _required_text(value.get("experiment_id"), f"summary {index} experiment_id")
        if experiment_id in result:
            raise ResearchFindingsError(f"duplicate summary for experiment {experiment_id!r}")
        result[experiment_id] = value

    if keyed_input is not None:
        for declared_id, summary in keyed_input.items():
            if not isinstance(declared_id, str) or summary.get("experiment_id") != declared_id:
                raise ResearchFindingsError(
                    "summary mapping keys must equal their summary experiment_id values"
                )
    if not result:
        raise ResearchFindingsError("no experiment summaries were provided")
    return result


def _build_experiment(
    experiment_id: str,
    rows: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
) -> _Experiment:
    identities = {
        (
            row.get("workload"),
            row.get("query_id"),
            row.get("storage_profile"),
        )
        for row in rows
    }
    if len(identities) != 1:
        raise ResearchFindingsError(f"experiment {experiment_id!r} has mixed workload identity")
    workload_value, query_value, storage_value = next(iter(identities))
    workload = _required_text(workload_value, f"experiment {experiment_id} workload")
    query_id = _required_text(query_value, f"experiment {experiment_id} query_id")
    storage_profile = _required_text(storage_value, f"experiment {experiment_id} storage_profile")

    by_pair: dict[int, dict[str, Mapping[str, Any]]] = defaultdict(dict)
    rows_by_engine: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    scales: set[int | None] = set()
    for index, row in enumerate(rows):
        pair_index = int(row["pair_index"])
        engine = str(row["engine"])
        if engine in by_pair[pair_index]:
            raise ResearchFindingsError(
                f"experiment {experiment_id!r} pair {pair_index} duplicates engine {engine!r}"
            )
        by_pair[pair_index][engine] = row
        rows_by_engine[engine].append(row)
        scales.add(_scale_from_record(row, index=index))

    for pair_index, engines in sorted(by_pair.items()):
        if set(engines) != _ENGINE_SET:
            missing = sorted(_ENGINE_SET - set(engines))
            raise ResearchFindingsError(
                f"experiment {experiment_id!r} pair {pair_index} is missing engines {missing}"
            )
    if set(rows_by_engine) != _ENGINE_SET:
        raise ResearchFindingsError(f"experiment {experiment_id!r} does not contain both engines")
    if len(scales) != 1:
        raise ResearchFindingsError(f"experiment {experiment_id!r} has inconsistent scale factors")
    scale_factor = next(iter(scales))
    if workload == "tpch" and scale_factor is None:
        raise ResearchFindingsError(
            f"TPC-H experiment {experiment_id!r} has no detectable SF1/SF10 scale"
        )

    speedups = [
        _latency(pair["spark_baseline"]) / _latency(pair["comet_accelerated"])
        for _, pair in sorted(by_pair.items())
    ]
    paired_speedup, paired_speedup_ci = _validate_summary(
        summary,
        experiment_id=experiment_id,
        workload=workload,
        query_id=query_id,
        storage_profile=storage_profile,
        scale_factor=scale_factor,
        pair_count=len(by_pair),
        raw_median=float(statistics.median(speedups)),
    )
    engine_plans = {
        engine: _summarize_engine_plan(rows_by_engine[engine], engine=engine) for engine in _ENGINES
    }
    return _Experiment(
        experiment_id=experiment_id,
        workload=workload,
        query_id=query_id,
        storage_profile=storage_profile,
        scale_factor=scale_factor,
        pair_count=len(by_pair),
        paired_speedup=paired_speedup,
        paired_speedup_ci=paired_speedup_ci,
        engine_plans=engine_plans,
    )


def _validate_summary(
    summary: Mapping[str, Any],
    *,
    experiment_id: str,
    workload: str,
    query_id: str,
    storage_profile: str,
    scale_factor: int | None,
    pair_count: int,
    raw_median: float,
) -> tuple[JsonObject, JsonObject]:
    label = f"summary for {experiment_id}"
    if summary.get("schema_version") != 1 or isinstance(summary.get("schema_version"), bool):
        raise ResearchFindingsError(f"{label} schema_version must equal 1")
    expected_identity = {
        "experiment_id": experiment_id,
        "workload": workload,
        "query_id": query_id,
        "storage_profile": storage_profile,
    }
    for field, identity_expected in expected_identity.items():
        if summary.get(field) != identity_expected:
            raise ResearchFindingsError(f"{label} has mismatched {field}")
    if "scale_factor" in summary and summary.get("scale_factor") != scale_factor:
        raise ResearchFindingsError(f"{label} has mismatched scale_factor")

    expected_records = pair_count * len(_ENGINES)
    count_expectations = {
        "n_total": expected_records,
        "n_succeeded": expected_records,
        "n_failed": 0,
    }
    for field, count_expected in count_expectations.items():
        value = summary.get(field)
        if not _is_integer(value) or value != count_expected:
            raise ResearchFindingsError(f"{label}.{field} must equal {count_expected}")
    if summary.get("paired_failures") != []:
        raise ResearchFindingsError(f"{label}.paired_failures must be empty")

    engine_summaries = summary.get("engines")
    if not isinstance(engine_summaries, Mapping) or set(engine_summaries) != _ENGINE_SET:
        raise ResearchFindingsError(f"{label}.engines must contain exactly Spark and Comet")
    for engine in _ENGINES:
        engine_summary = engine_summaries.get(engine)
        if not isinstance(engine_summary, Mapping) or engine_summary.get("n") != pair_count:
            raise ResearchFindingsError(f"{label}.engines.{engine}.n must equal {pair_count}")

    bootstrap = summary.get("bootstrap")
    if not isinstance(bootstrap, Mapping):
        raise ResearchFindingsError(f"{label}.bootstrap must be a mapping")
    for field, bootstrap_expected in _BOOTSTRAP.items():
        if bootstrap.get(field) != bootstrap_expected:
            raise ResearchFindingsError(
                f"{label}.bootstrap.{field} must equal the fixed value {bootstrap_expected!r}"
            )

    paired = summary.get("paired_speedup")
    if not isinstance(paired, Mapping) or paired.get("n") != pair_count:
        raise ResearchFindingsError(f"{label}.paired_speedup.n must equal {pair_count}")
    normalized_paired = _normalize_speedup_description(paired, label=f"{label}.paired_speedup")
    reported_median = float(normalized_paired["median"])
    if not math.isclose(reported_median, raw_median, rel_tol=1e-12, abs_tol=1e-12):
        raise ResearchFindingsError(f"{label} median paired speedup does not match raw pairs")

    interval = summary.get("paired_speedup_ci")
    if not isinstance(interval, Mapping):
        raise ResearchFindingsError(f"{label}.paired_speedup_ci must be a mapping")
    normalized_interval = _normalize_interval(interval, label=f"{label}.paired_speedup_ci")
    return normalized_paired, normalized_interval


def _normalize_speedup_description(value: Mapping[str, Any], *, label: str) -> JsonObject:
    count = value.get("n")
    if not _is_integer(count) or count < 1:
        raise ResearchFindingsError(f"{label}.n must be a positive integer")
    fields = ("median", "q1", "q3", "iqr", "min", "max")
    numbers = {
        field: _positive_or_zero_number(value.get(field), f"{label}.{field}") for field in fields
    }
    if numbers["median"] <= 0 or numbers["min"] <= 0:
        raise ResearchFindingsError(f"{label} speedups must be greater than zero")
    if not (
        numbers["min"] <= numbers["q1"] <= numbers["median"] <= numbers["q3"] <= numbers["max"]
    ):
        raise ResearchFindingsError(f"{label} quantiles are not ordered")
    if not math.isclose(
        numbers["iqr"], numbers["q3"] - numbers["q1"], rel_tol=1e-12, abs_tol=1e-12
    ):
        raise ResearchFindingsError(f"{label}.iqr does not equal q3 - q1")
    return {"n": count, **numbers}


def _normalize_interval(value: Mapping[str, Any], *, label: str) -> JsonObject:
    expected_metadata = {key: item for key, item in _BOOTSTRAP.items() if key != "estimator"}
    for field, expected in expected_metadata.items():
        if value.get(field) != expected:
            raise ResearchFindingsError(f"{label}.{field} must equal {expected!r}")
    lower = _positive_number(value.get("lower"), f"{label}.lower")
    upper = _positive_number(value.get("upper"), f"{label}.upper")
    if lower > upper:
        raise ResearchFindingsError(f"{label}.lower must not exceed upper")
    return {
        "lower": lower,
        "upper": upper,
        **expected_metadata,
    }


def _summarize_engine_plan(rows: Sequence[Mapping[str, Any]], *, engine: str) -> JsonObject:
    values: dict[str, list[float]] = {field: [] for field in _PLAN_METRICS}
    reason_counts: Counter[str] = Counter()
    fallback_detected = False
    for row in rows:
        plan = row["plan_analysis"]
        if not isinstance(plan, Mapping):
            raise AssertionError("plan mappings were validated during admission")
        coverage = plan.get("native_coverage_ratio")
        if coverage is not None:
            coverage_value = float(coverage)
            values["native_coverage_ratio"].append(coverage_value)
            fallback_detected = fallback_detected or coverage_value < 1.0
        for field in _PLAN_METRICS[1:]:
            values[field].append(float(plan[field]))
        fallback_detected = fallback_detected or int(plan["spark_fallback_operators"]) > 0
        for reason in plan["fallback_reasons"]:
            reason_counts[str(reason)] += 1

    if engine == "comet_accelerated" and len(values["native_coverage_ratio"]) != len(rows):
        raise ResearchFindingsError("all Comet measurements require native coverage")
    if reason_counts and not fallback_detected:
        raise ResearchFindingsError(
            f"{engine} has fallback reason annotations but no detected fallback"
        )
    annotations = sorted(reason_counts)
    if annotations:
        annotation_status = "present"
        annotation_note = "Fallback reason annotations are present."
    elif fallback_detected:
        annotation_status = "unannotated_fallback_detected"
        annotation_note = (
            "Empty reason annotations do not mean no fallback; operator counts or native "
            "coverage independently show fallback."
        )
    else:
        annotation_status = "not_applicable_no_fallback_detected"
        annotation_note = "No fallback was detected by coverage or fallback-operator counts."
    return {
        "measurement_count": len(rows),
        "native_coverage_ratio": _describe_plan_values(values["native_coverage_ratio"]),
        "comet_native_operators": _describe_plan_values(values["comet_native_operators"]),
        "spark_fallback_operators": _describe_plan_values(values["spark_fallback_operators"]),
        "transition_count": _describe_plan_values(values["transition_count"]),
        "fallback_detected": fallback_detected,
        "fallback_reason_annotations": annotations,
        "fallback_reason_counts": [
            {"reason": reason, "record_count": reason_counts[reason]} for reason in annotations
        ],
        "fallback_reason_annotation_status": annotation_status,
        "fallback_reason_annotation_note": annotation_note,
    }


def _describe_plan_values(values: Sequence[float]) -> JsonObject:
    if not values:
        return {"n": 0, "median": None, "min": None, "max": None}
    return {
        "n": len(values),
        "median": float(statistics.median(values)),
        "min": float(min(values)),
        "max": float(max(values)),
    }


def _build_rq1(experiments: Sequence[_Experiment]) -> JsonObject:
    entries = [_rq1_experiment(experiment) for experiment in experiments]
    by_workload: dict[str, list[_Experiment]] = defaultdict(list)
    for experiment in experiments:
        by_workload[experiment.workload].append(experiment)
    workload_groups = []
    for workload in sorted(by_workload):
        group = sorted(by_workload[workload], key=_experiment_sort_key)
        medians = [float(experiment.paired_speedup["median"]) for experiment in group]
        workload_groups.append(
            {
                "workload": workload,
                "experiment_count": len(group),
                "experiment_ids": [experiment.experiment_id for experiment in group],
                "query_ids": sorted({experiment.query_id for experiment in group}),
                "experiment_median_speedups": _describe_experiment_medians(medians),
                "aggregation_note": (
                    "Descriptive distribution of per-experiment medians; no pooled confidence "
                    "interval is inferred."
                ),
            }
        )
    return {
        "estimand": "median_paired_speedup",
        "speedup_definition": "spark_baseline_wall_time / comet_accelerated_wall_time",
        "experiments": entries,
        "workload_groups": workload_groups,
    }


def _rq1_experiment(experiment: _Experiment) -> JsonObject:
    return {
        **_identity(experiment),
        "pair_count": experiment.pair_count,
        "engine_measurement_counts": {
            engine: experiment.engine_plans[engine]["measurement_count"] for engine in _ENGINES
        },
        "paired_speedup": dict(experiment.paired_speedup),
        "paired_speedup_ci": dict(experiment.paired_speedup_ci),
    }


def _describe_experiment_medians(values: Sequence[float]) -> JsonObject:
    ordered = sorted(values)
    q1 = percentile(ordered, 0.25)
    q3 = percentile(ordered, 0.75)
    return {
        "n": len(ordered),
        "median": float(statistics.median(ordered)),
        "q1": q1,
        "q3": q3,
        "iqr": q3 - q1,
        "min": ordered[0],
        "max": ordered[-1],
    }


def _build_rq2(experiments: Sequence[_Experiment]) -> JsonObject:
    return {
        "interpretation": (
            "Count-based native coverage describes physical-plan operators, not the percentage "
            "of runtime or CPU executed natively."
        ),
        "experiments": [
            {
                **_identity(experiment),
                "engines": {
                    engine: _copy_json(experiment.engine_plans[engine]) for engine in _ENGINES
                },
            }
            for experiment in experiments
        ],
    }


def _build_h1(experiments: Sequence[_Experiment]) -> JsonObject:
    counts = {"supported": 0, "inconclusive": 0, "decreased": 0}
    evidence = []
    for experiment in experiments:
        lower = float(experiment.paired_speedup_ci["lower"])
        upper = float(experiment.paired_speedup_ci["upper"])
        if lower > 1.0:
            classification = "supported"
        elif upper < 1.0:
            classification = "decreased"
        else:
            classification = "inconclusive"
        counts[classification] += 1
        comet = experiment.engine_plans["comet_accelerated"]
        evidence.append(
            {
                **_identity(experiment),
                "classification": classification,
                "paired_speedup_median": experiment.paired_speedup["median"],
                "paired_speedup_ci": dict(experiment.paired_speedup_ci),
                "comet_plan_descriptors": {
                    "native_coverage_ratio": _copy_json(comet["native_coverage_ratio"]),
                    "spark_fallback_operators": _copy_json(comet["spark_fallback_operators"]),
                    "transition_count": _copy_json(comet["transition_count"]),
                },
            }
        )
    return {
        "decision_rule": {
            "supported": "95% bootstrap CI is entirely above 1",
            "decreased": "95% bootstrap CI is entirely below 1",
            "inconclusive": "95% bootstrap CI includes or touches 1",
        },
        "classification_counts": counts,
        "experiments": evidence,
    }


def _build_h2(experiments: Sequence[_Experiment]) -> JsonObject:
    points = [_h2_point(experiment) for experiment in experiments]
    by_workload: dict[str, list[JsonObject]] = defaultdict(list)
    by_scale: dict[str, list[JsonObject]] = defaultdict(list)
    for point in points:
        by_workload[str(point["workload"])].append(point)
        scale = point["scale_factor"]
        scale_label = f"SF{scale}" if scale is not None else "not_applicable"
        by_scale[scale_label].append(point)
    return {
        "analysis_type": "descriptive_exploratory",
        "method": "spearman_average_ranks_tie_aware",
        "response": "natural_log_median_paired_speedup",
        "minimum_n": 3,
        "inferential_statistics": "not_computed",
        "points": points,
        "overall": _correlations(points),
        "by_workload": {
            workload: _correlations(by_workload[workload]) for workload in sorted(by_workload)
        },
        "by_data_scale": {scale: _correlations(by_scale[scale]) for scale in sorted(by_scale)},
    }


def _h2_point(experiment: _Experiment) -> JsonObject:
    comet = experiment.engine_plans["comet_accelerated"]
    speedup = float(experiment.paired_speedup["median"])
    return {
        **_identity(experiment),
        "log_median_paired_speedup": math.log(speedup),
        **{
            metric: _required_descriptor_median(comet[metric], metric=metric)
            for metric in _CORRELATION_METRICS
        },
    }


def _correlations(points: Sequence[Mapping[str, Any]]) -> JsonObject:
    response = [float(point["log_median_paired_speedup"]) for point in points]
    return {
        "experiment_count": len(points),
        "correlations": {
            metric: _spearman_result(
                [float(point[metric]) for point in points], response, metric=metric
            )
            for metric in _CORRELATION_METRICS
        },
    }


def _spearman_result(x: Sequence[float], y: Sequence[float], *, metric: str) -> JsonObject:
    if len(x) != len(y):
        raise AssertionError("correlation vectors must have equal lengths")
    count = len(x)
    if count < 3:
        return {
            "n": count,
            "rho": None,
            "estimability": "not_estimable",
            "reason": "fewer_than_3_experiments",
            "expected_direction": _expected_direction(metric),
        }
    x_ranks = _average_ranks(x)
    y_ranks = _average_ranks(y)
    x_mean = statistics.fmean(x_ranks)
    y_mean = statistics.fmean(y_ranks)
    x_centered = [rank - x_mean for rank in x_ranks]
    y_centered = [rank - y_mean for rank in y_ranks]
    denominator = math.sqrt(
        sum(value * value for value in x_centered) * sum(value * value for value in y_centered)
    )
    if denominator == 0.0:
        constant = "metric" if len(set(x_ranks)) == 1 else "response"
        return {
            "n": count,
            "rho": None,
            "estimability": "not_estimable",
            "reason": f"constant_{constant}_ranks",
            "expected_direction": _expected_direction(metric),
        }
    rho = sum(left * right for left, right in zip(x_centered, y_centered, strict=True))
    rho /= denominator
    rho = min(1.0, max(-1.0, rho))
    return {
        "n": count,
        "rho": rho,
        "estimability": "estimable",
        "reason": None,
        "expected_direction": _expected_direction(metric),
    }


def _average_ranks(values: Sequence[float]) -> list[float]:
    ordered = sorted(enumerate(values), key=lambda item: (item[1], item[0]))
    ranks = [0.0] * len(values)
    start = 0
    while start < len(ordered):
        end = start + 1
        while end < len(ordered) and ordered[end][1] == ordered[start][1]:
            end += 1
        average_rank = ((start + 1) + end) / 2.0
        for position in range(start, end):
            ranks[ordered[position][0]] = average_rank
        start = end
    return ranks


def _expected_direction(metric: str) -> str:
    return "positive" if metric == "native_coverage_ratio" else "negative"


def _build_scale_comparison(experiments: Sequence[_Experiment]) -> JsonObject:
    tpch = [experiment for experiment in experiments if experiment.workload == "tpch"]
    scales = sorted({experiment.scale_factor for experiment in tpch if experiment.scale_factor})
    by_scale_query: dict[tuple[int, str], _Experiment] = {}
    for experiment in tpch:
        if experiment.scale_factor is None:
            raise AssertionError("TPC-H scales were validated during admission")
        key = (experiment.scale_factor, experiment.query_id)
        if key in by_scale_query:
            raise ResearchFindingsError(
                f"multiple TPC-H experiments exist for SF{key[0]} query {key[1]}"
            )
        by_scale_query[key] = experiment

    sf1_queries = {query for scale, query in by_scale_query if scale == 1}
    sf10_queries = {query for scale, query in by_scale_query if scale == 10}
    common = sorted(sf1_queries & sf10_queries)
    comparisons = []
    for query_id in common:
        sf1 = by_scale_query[(1, query_id)]
        sf10 = by_scale_query[(10, query_id)]
        sf1_speedup = float(sf1.paired_speedup["median"])
        sf10_speedup = float(sf10.paired_speedup["median"])
        comparisons.append(
            {
                "query_id": query_id,
                "sf1_experiment_id": sf1.experiment_id,
                "sf10_experiment_id": sf10.experiment_id,
                "sf1_median_paired_speedup": sf1_speedup,
                "sf10_median_paired_speedup": sf10_speedup,
                "sf10_to_sf1_median_speedup_ratio": sf10_speedup / sf1_speedup,
            }
        )

    if not tpch:
        estimability = "not_estimable"
        reason_code = "tpch_evidence_absent"
        reason = "No TPC-H experiment evidence is present."
    elif scales == [1]:
        estimability = "not_estimable"
        reason_code = "sf10_absent"
        reason = "Only SF1 TPC-H evidence is present; SF10 is required for comparison."
    elif scales == [10]:
        estimability = "not_estimable"
        reason_code = "sf1_absent"
        reason = "Only SF10 TPC-H evidence is present; SF1 is required for comparison."
    elif sf1_queries != sf10_queries:
        estimability = "not_estimable"
        reason_code = "unmatched_query_sets"
        reason = "SF1 and SF10 do not contain the same TPC-H query set."
    elif not common:
        estimability = "not_estimable"
        reason_code = "no_matched_queries"
        reason = "No query has both SF1 and SF10 evidence."
    else:
        estimability = "descriptive_only"
        reason_code = None
        reason = (
            "Matched SF1/SF10 query evidence supports only an exploratory two-point "
            "description, not a general scalability trend."
        )
    return {
        "analysis_type": "exploratory_two_point_descriptive",
        "required_scales": [1, 10],
        "scales_present": scales,
        "estimability": estimability,
        "reason_code": reason_code,
        "reason": reason,
        "sf1_query_ids": sorted(sf1_queries),
        "sf10_query_ids": sorted(sf10_queries),
        "matched_query_count": len(common),
        "queries": comparisons,
    }


def _build_fallback_overhead(experiments: Sequence[_Experiment]) -> JsonObject:
    observations = []
    for experiment in experiments:
        comet = experiment.engine_plans["comet_accelerated"]
        observations.append(
            {
                **_identity(experiment),
                "median_paired_speedup": experiment.paired_speedup["median"],
                "native_coverage_ratio_median": _required_descriptor_median(
                    comet["native_coverage_ratio"], metric="native_coverage_ratio"
                ),
                "spark_fallback_operators_median": _required_descriptor_median(
                    comet["spark_fallback_operators"], metric="spark_fallback_operators"
                ),
                "transition_count_median": _required_descriptor_median(
                    comet["transition_count"], metric="transition_count"
                ),
                "fallback_detected": comet["fallback_detected"],
            }
        )
    return {
        "assessment": "descriptive_only",
        "causal_interpretation": "not_permitted",
        "reason": (
            "Experiment-level fallback and transition counts are observational plan "
            "descriptors; they do not isolate conversion overhead from query complexity."
        ),
        "experiment_count": len(observations),
        "experiments": observations,
    }


def _build_partial_native_m08(experiments: Sequence[_Experiment]) -> JsonObject:
    m08 = [experiment for experiment in experiments if experiment.query_id == "M08"]
    observed = []
    for experiment in m08:
        comet = experiment.engine_plans["comet_accelerated"]
        coverage = _required_descriptor_median(
            comet["native_coverage_ratio"], metric="native_coverage_ratio"
        )
        if 0.0 < coverage < 1.0 and bool(comet["fallback_detected"]):
            observed.append(
                {
                    **_identity(experiment),
                    "median_paired_speedup": experiment.paired_speedup["median"],
                    "comet_plan": _copy_json(comet),
                }
            )
    if observed:
        return {
            "status": "observed",
            "reason": "M08 contains both native and Spark fallback operators.",
            "experiments": observed,
        }
    reason = (
        "M08 is absent from the admitted experiment set."
        if not m08
        else "No admitted M08 experiment has partial native coverage with detected fallback."
    )
    return {"status": "not_observed", "reason": reason, "experiments": []}


def _build_h3(scale_comparison: Mapping[str, Any]) -> JsonObject:
    estimability = str(scale_comparison["estimability"])
    return {
        "assessment": estimability,
        "analysis_type": "exploratory_two_point_descriptive",
        "reason_code": scale_comparison["reason_code"],
        "reason": scale_comparison["reason"],
        "matched_query_count": scale_comparison["matched_query_count"],
        "general_scalability_claim_allowed": False,
    }


def _identity(experiment: _Experiment) -> JsonObject:
    return {
        "experiment_id": experiment.experiment_id,
        "workload": experiment.workload,
        "query_id": experiment.query_id,
        "storage_profile": experiment.storage_profile,
        "scale_factor": experiment.scale_factor,
    }


def _experiment_sort_key(experiment: _Experiment) -> tuple[str, str, int, str]:
    return (
        experiment.workload,
        experiment.query_id,
        experiment.scale_factor or 0,
        experiment.experiment_id,
    )


def _required_descriptor_median(value: object, *, metric: str) -> float:
    if not isinstance(value, Mapping):
        raise AssertionError(f"{metric} descriptor must be a mapping")
    median = value.get("median")
    if median is None:
        raise ResearchFindingsError(f"Comet {metric} median is unavailable")
    return _finite_number(median, f"Comet {metric} median")


def _latency(record: Mapping[str, Any]) -> float:
    metrics = record["metrics"]
    if not isinstance(metrics, Mapping):
        raise AssertionError("metrics were validated during admission")
    return float(metrics["query_wall_time_ms"])


def _scale_from_record(record: Mapping[str, Any], *, index: int) -> int | None:
    explicit: int | None = None
    if "scale_factor" in record and record.get("scale_factor") is not None:
        value = record.get("scale_factor")
        if not _is_integer(value) or value not in {1, 10}:
            raise ResearchFindingsError(f"measurement record {index} has invalid scale_factor")
        explicit = int(value)
    experiment_id = str(record.get("experiment_id", ""))
    match = _SCALE_PATTERN.search(experiment_id)
    inferred = int(match.group(1)) if match else None
    if explicit is not None and inferred is not None and explicit != inferred:
        raise ResearchFindingsError(
            f"measurement record {index} scale_factor conflicts with experiment_id"
        )
    return explicit if explicit is not None else inferred


def _required_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ResearchFindingsError(f"{label} must be a non-empty string")
    return value


def _finite_number(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ResearchFindingsError(f"{label} must be numeric")
    number = float(value)
    if not math.isfinite(number):
        raise ResearchFindingsError(f"{label} must be finite")
    return number


def _positive_number(value: object, label: str) -> float:
    number = _finite_number(value, label)
    if number <= 0:
        raise ResearchFindingsError(f"{label} must be greater than zero")
    return number


def _positive_or_zero_number(value: object, label: str) -> float:
    number = _finite_number(value, label)
    if number < 0:
        raise ResearchFindingsError(f"{label} must be non-negative")
    return number


def _is_integer(value: object) -> TypeGuard[int]:
    return isinstance(value, int) and not isinstance(value, bool)


def _copy_json(value: object) -> Any:
    return json.loads(json.dumps(value, allow_nan=False, sort_keys=True))


def _require_finite_json(value: object) -> None:
    try:
        json.dumps(value, allow_nan=False, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError) as error:
        raise ResearchFindingsError(f"findings are not finite JSON: {error}") from error


__all__ = ["ResearchFindingsError", "build_research_findings"]
