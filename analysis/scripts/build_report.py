"""Rebuild deterministic Markdown, CSV, JSON, and SVG reports from immutable raw runs."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
import xml.etree.ElementTree as ET
from collections import defaultdict
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

from analysis.plan_insights import PlanInsightsError, build_plan_insights
from analysis.report_charts import (
    ReportChartError,
    latency_distribution_svg,
    native_coverage_distribution_svg,
    resource_profile_svg,
)
from analysis.report_publishability import assess_report_publishability
from analysis.research_findings import ResearchFindingsError, build_research_findings
from analysis.resource_profiles import ResourceProfileError, build_resource_profiles
from benchmark.runner.canonical import write_json
from benchmark.runner.statistics import (
    DEFAULT_BOOTSTRAP_RESAMPLES,
    DEFAULT_BOOTSTRAP_SEED,
    MIN_P95_SAMPLE_SIZE,
    bootstrap_percentile_interval,
    describe,
)
from benchmark.runner.summary import summarize_records

ROOT = Path(__file__).resolve().parents[2]


class ReportNotPublishableError(ValueError):
    """The diagnostic artifacts were built, but the publication policy did not pass."""


_PER_EXPERIMENT_ARTIFACT_PATTERNS = (
    "*.summary.json",
    "*.latency.svg",
    "*.native-coverage.svg",
    "*.resource-profile.json",
    "*.resource-profile.svg",
)

_ENGINES = ("spark_baseline", "comet_accelerated")
_RESOURCE_METRICS = (
    "cpu_core_seconds",
    "cgroup_memory_peak_mib",
    "jvm_gc_time_ms",
    "shuffle_read_mb",
    "shuffle_write_mb",
    "disk_spill_mb",
)
_RESOURCE_LABELS = {
    "cpu_core_seconds": "CPU core-seconds",
    "cgroup_memory_peak_mib": "Cgroup peak memory",
    "jvm_gc_time_ms": "JVM GC time",
    "shuffle_read_mb": "Shuffle read",
    "shuffle_write_mb": "Shuffle write",
    "disk_spill_mb": "Disk spill",
}
_TRANSITION_OPERATORS = frozenset(
    {
        "ColumnarToRow",
        "RowToColumnar",
        "CometColumnarToRow",
        "CometRowToColumnar",
        "CometNativeColumnarToRow",
        "CometSparkColumnarToColumnar",
        "CometSparkRowToColumnar",
        "ArrowEvalPython",
        "BatchEvalPython",
    }
)


def _prune_stale_experiment_artifacts(output_dir: Path, produced: Iterable[Path]) -> None:
    expected = {path.resolve() for path in produced}
    for pattern in _PER_EXPERIMENT_ARTIFACT_PATTERNS:
        for path in output_dir.glob(pattern):
            if path.is_file() and path.resolve() not in expected:
                path.unlink()


def _load_records(raw_root: Path) -> list[dict[str, Any]]:
    schema = json.loads(
        (ROOT / "benchmark/schemas/raw-result.schema.json").read_text(encoding="utf-8")
    )
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    records: list[dict[str, Any]] = []
    for path in sorted(raw_root.rglob("*.json")):
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError(f"raw record root must be an object: {path}")
        errors = sorted(validator.iter_errors(value), key=lambda item: list(item.path))
        if errors:
            raise ValueError(
                f"invalid raw record {path}: " + "; ".join(error.message for error in errors)
            )
        records.append(value)
    return records


def _identity(record: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(record["experiment_id"]),
        str(record["workload"]),
        str(record["query_id"]),
        str(record["storage_profile"]),
    )


def _format_number(value: object, digits: int = 3) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, int | float):
        return f"{float(value):.{digits}f}"
    return str(value)


def _format_percent(value: object, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, int | float):
        return f"{float(value) * 100:.{digits}f}%"
    return str(value)


def _format_operator_count(value: object) -> str:
    if isinstance(value, int | float):
        number = float(value)
        if number.is_integer():
            return str(int(number))
        return f"{number:.3f}"
    return str(value)


def _count_label(count: int, singular: str) -> str:
    return f"{count} {singular if count == 1 else singular + 's'}"


def _partial_native_m08_report_lines(partial: Mapping[str, Any]) -> list[str]:
    status = str(partial["status"])
    experiments = partial["experiments"]
    if status != "observed" or not isinstance(experiments, list) or not experiments:
        return [
            f"M08 partial-native evidence was not observed: {partial['reason']}",
            "",
            f"Partial-native M08 evidence status: {status}.",
        ]

    lines: list[str] = []
    for value in experiments:
        if not isinstance(value, Mapping):
            raise ValueError("partial-native M08 experiment must be an object")
        comet = value["comet_plan"]
        if not isinstance(comet, Mapping):
            raise ValueError("partial-native M08 Comet plan must be an object")
        coverage = comet["native_coverage_ratio"]
        native = comet["comet_native_operators"]
        fallback = comet["spark_fallback_operators"]
        transitions = comet["transition_count"]
        if not all(isinstance(item, Mapping) for item in (coverage, native, fallback, transitions)):
            raise ValueError("partial-native M08 plan descriptors must be objects")
        lines.append(
            f"{value['experiment_id']} is a partial-native M08 case: median coverage was "
            f"{_format_percent(coverage['median'])}, with "
            f"{_format_operator_count(native['median'])} native and "
            f"{_format_operator_count(fallback['median'])} fallback operators plus "
            f"{_format_operator_count(transitions['median'])} transitions."
        )
        annotation_status = str(comet["fallback_reason_annotation_status"])
        if annotation_status == "unannotated_fallback_detected":
            lines.append(
                "Its empty reason list means the fallback was unannotated; it does not mean "
                "that no fallback occurred. Expression-level causes cannot be identified when "
                "the captured plan contains no explicit reason annotation."
            )
        else:
            annotations = comet["fallback_reason_annotations"]
            annotation_text = (
                ", ".join(str(item) for item in annotations)
                if isinstance(annotations, list) and annotations
                else "none recorded"
            )
            lines.append(f"Recorded fallback reason annotations: {annotation_text}.")
    lines.extend(["", f"Partial-native M08 evidence status: {status}."])
    return lines


def _bootstrap_ci(values: Iterable[float]) -> dict[str, Any] | None:
    sample = [float(value) for value in values]
    if not sample:
        return None
    interval = bootstrap_percentile_interval(
        sample,
        confidence_level=0.95,
        resamples=DEFAULT_BOOTSTRAP_RESAMPLES,
        seed=DEFAULT_BOOTSTRAP_SEED,
    )
    return {
        "lower": interval.lower,
        "upper": interval.upper,
        "confidence_level": interval.confidence_level,
        "resamples": interval.resamples,
        "seed": interval.seed,
        "method": interval.method,
        "percentile_method": interval.percentile_method,
    }


def _describe_with_ci(values: Iterable[float]) -> dict[str, Any]:
    sample = [float(value) for value in values]
    result: dict[str, Any] = dict(describe(sample))
    result["median_ci_95"] = _bootstrap_ci(sample)
    return result


def _enrich_summary(summary: dict[str, Any], rows: Iterable[Mapping[str, Any]]) -> None:
    measurements = [row for row in rows if row.get("phase") == "measurement"]
    for engine in _ENGINES:
        latencies = [
            float(row["metrics"]["query_wall_time_ms"])
            for row in measurements
            if row.get("engine") == engine and row.get("status") == "succeeded"
        ]
        summary["engines"][engine]["median_ci_95"] = _bootstrap_ci(latencies)

    by_pair: dict[int, dict[str, Mapping[str, Any]]] = defaultdict(dict)
    for row in measurements:
        pair_index = row.get("pair_index")
        record_engine = row.get("engine")
        if (
            isinstance(pair_index, int)
            and not isinstance(pair_index, bool)
            and isinstance(record_engine, str)
        ):
            by_pair[pair_index][record_engine] = row
    for metric in _RESOURCE_METRICS:
        absolute: list[float] = []
        relative: list[float] = []
        for pair in by_pair.values():
            if set(pair) != set(_ENGINES):
                continue
            spark = pair["spark_baseline"]
            comet = pair["comet_accelerated"]
            if spark.get("status") != "succeeded" or comet.get("status") != "succeeded":
                continue
            spark_value = float(spark["metrics"][metric])
            comet_value = float(comet["metrics"][metric])
            delta = spark_value - comet_value
            absolute.append(delta)
            if spark_value != 0.0:
                relative.append(delta / spark_value)
        resource = summary["paired_resource_savings"][metric]
        resource["absolute_delta"]["median_ci_95"] = _bootstrap_ci(absolute)
        resource["relative_saving_ratio"]["median_ci_95"] = _bootstrap_ci(relative)


def _native_summary(records: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    comet = [
        record
        for record in records
        if record.get("engine") == "comet_accelerated" and record.get("status") == "succeeded"
    ]
    coverage = [
        float(record["plan_analysis"]["native_coverage_ratio"])
        for record in comet
        if record["plan_analysis"]["native_coverage_ratio"] is not None
    ]
    native_counts = [float(record["plan_analysis"]["comet_native_operators"]) for record in comet]
    fallback_counts = [
        float(record["plan_analysis"]["spark_fallback_operators"]) for record in comet
    ]
    transitions = [float(record["plan_analysis"]["transition_count"]) for record in comet]
    subtrees = [float(record["plan_analysis"]["native_subtree_count"]) for record in comet]
    reasons = sorted(
        {str(reason) for record in comet for reason in record["plan_analysis"]["fallback_reasons"]}
    )
    fallback_detected = bool(
        (coverage and min(coverage) < 1.0) or (fallback_counts and max(fallback_counts) > 0.0)
    )
    result = _describe_with_ci(coverage)
    result.update(
        {
            "native_operator_count": statistics.median(native_counts) if native_counts else 0.0,
            "fallback_operator_count": (
                statistics.median(fallback_counts) if fallback_counts else 0.0
            ),
            "transition_count": statistics.median(transitions) if transitions else 0.0,
            "native_subtree_count": statistics.median(subtrees) if subtrees else 0.0,
            "native_operator_count_distribution": _describe_with_ci(native_counts),
            "fallback_operator_count_distribution": _describe_with_ci(fallback_counts),
            "transition_count_distribution": _describe_with_ci(transitions),
            "native_subtree_count_distribution": _describe_with_ci(subtrees),
            "fallback_reasons": reasons,
            "fallback_detected": fallback_detected,
            "unannotated_fallback": fallback_detected and not reasons,
        }
    )
    return result


def _attempt_counts(
    publication: Mapping[str, Any], experiment_id: str, accepted_records: int
) -> tuple[int, int, bool]:
    checks = publication.get("checks")
    if isinstance(checks, Mapping):
        verification = checks.get("campaign_verifications")
        if isinstance(verification, list):
            for item in verification:
                if not isinstance(item, Mapping) or item.get("experiment_id") != experiment_id:
                    continue
                attempts = item.get("execution_attempt_count")
                failures = item.get("failed_attempt_record_count")
                if (
                    isinstance(attempts, int)
                    and not isinstance(attempts, bool)
                    and attempts >= 0
                    and isinstance(failures, int)
                    and not isinstance(failures, bool)
                    and 0 <= failures <= attempts
                ):
                    return attempts, failures, bool(item.get("attempt_counts_verified"))
    return accepted_records, 0, False


def _structure_inventory(experiment: Mapping[str, Any]) -> dict[str, Any]:
    engines = experiment.get("engines")
    if not isinstance(engines, Mapping):
        return {}
    comet = engines.get("comet_accelerated")
    if not isinstance(comet, Mapping):
        return {}
    plans = comet.get("plans")
    if not isinstance(plans, Mapping):
        return {}
    final = plans.get("final")
    if not isinstance(final, Mapping):
        return {}
    strata = final.get("hash_strata")
    if not isinstance(strata, list):
        return {}

    operators: set[str] = set()
    scans: set[str] = set()
    joins: set[str] = set()
    partitions: set[int] = set()
    for stratum in strata:
        if not isinstance(stratum, Mapping):
            continue
        structure = stratum.get("structure")
        if not isinstance(structure, Mapping):
            continue
        sequence = structure.get("operator_sequence")
        if isinstance(sequence, list):
            operators.update(str(item) for item in sequence)
        for field, target in (("scan_implementations", scans), ("join_strategies", joins)):
            descriptor = structure.get(field)
            if isinstance(descriptor, Mapping) and isinstance(descriptor.get("values"), list):
                target.update(str(item) for item in descriptor["values"])
        partition_descriptor = structure.get("partition_counts")
        if isinstance(partition_descriptor, Mapping) and isinstance(
            partition_descriptor.get("values"), list
        ):
            partitions.update(int(item) for item in partition_descriptor["values"])

    native: list[str] = []
    fallback: list[str] = []
    transitions: list[str] = []
    for operator in sorted(operators):
        if operator in _TRANSITION_OPERATORS:
            transitions.append(operator)
        elif operator == "CometColumnarExchange" or not operator.startswith("Comet"):
            fallback.append(operator)
        else:
            native.append(operator)
    return {
        "native_operators": native,
        "fallback_operators": fallback,
        "transition_operators": transitions,
        "scans": sorted(scans),
        "joins": sorted(joins),
        "partition_counts": sorted(partitions),
    }


def _write_csv(path: Path, records: Iterable[Mapping[str, Any]]) -> None:
    fields = (
        "experiment_id",
        "query_id",
        "pair_index",
        "engine",
        "status",
        "query_wall_time_ms",
        "cpu_core_seconds",
        "cgroup_memory_peak_mib",
        "jvm_gc_time_ms",
        "shuffle_read_mb",
        "shuffle_write_mb",
        "disk_spill_mb",
        "native_coverage_ratio",
        "transition_count",
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for record in records:
            metrics = record["metrics"]
            plan = record["plan_analysis"]
            writer.writerow(
                {
                    "experiment_id": record["experiment_id"],
                    "query_id": record["query_id"],
                    "pair_index": record["pair_index"],
                    "engine": record["engine"],
                    "status": record["status"],
                    **{field: metrics[field] for field in fields if field in metrics},
                    "native_coverage_ratio": plan["native_coverage_ratio"],
                    "transition_count": plan["transition_count"],
                }
            )


def _format_interval(value: object, digits: int = 3) -> str:
    if not isinstance(value, Mapping):
        return "n/a"
    lower = _format_number(value.get("lower"), digits)
    upper = _format_number(value.get("upper"), digits)
    return f"[{lower}, {upper}]"


def _format_stat_row(
    label: str,
    stats: Mapping[str, Any],
    *,
    failures: int,
    attempts: tuple[int, int, bool],
    unit: str,
    ci: Mapping[str, Any] | None = None,
) -> str:
    attempt_count, attempt_failures, verified = attempts
    interval = ci if ci is not None else stats.get("median_ci_95")
    p95 = stats.get("p95")
    p95_text = (
        _format_number(p95)
        if p95 is not None
        else f"omitted (n<{MIN_P95_SAMPLE_SIZE}; linear R-7 policy)"
    )
    return (
        "| "
        + " | ".join(
            [
                label,
                str(stats.get("n", "n/a")),
                str(failures),
                f"{attempt_failures}/{attempt_count}"
                + ("" if verified else " (unverified diagnostic fallback)"),
                _format_number(stats.get("median")),
                _format_number(stats.get("iqr")),
                _format_number(stats.get("min")),
                _format_number(stats.get("max")),
                _format_interval(interval),
                p95_text,
                unit,
            ]
        )
        + " |"
    )


def _native_matrix_rows(
    summaries: Mapping[str, Mapping[str, Any]],
    native_summaries: Mapping[str, Mapping[str, Any]],
    plan_insights: Mapping[str, Any] | None,
    attempt_metadata: Mapping[str, tuple[int, int, bool]],
) -> list[dict[str, Any]]:
    plan_experiments: Mapping[str, Any] = {}
    if isinstance(plan_insights, Mapping) and isinstance(plan_insights.get("experiments"), Mapping):
        plan_experiments = plan_insights["experiments"]
    rows: list[dict[str, Any]] = []
    for experiment_id in sorted(summaries):
        summary = summaries[experiment_id]
        native = native_summaries[experiment_id]
        experiment_plan = plan_experiments.get(experiment_id)
        inventory = (
            _structure_inventory(experiment_plan) if isinstance(experiment_plan, Mapping) else {}
        )
        attempts, failed_attempts, verified = attempt_metadata[experiment_id]
        stable: bool | None = None
        final_hashes: int | None = None
        if isinstance(experiment_plan, Mapping):
            stable_value = experiment_plan.get("paired_final_plan_stable")
            if isinstance(stable_value, bool):
                stable = stable_value
            hash_value = experiment_plan.get("paired_final_plan_hash_combination_count")
            if isinstance(hash_value, int) and not isinstance(hash_value, bool):
                final_hashes = hash_value
        rows.append(
            {
                "experiment_id": experiment_id,
                "workload": summary["workload"],
                "query_id": summary["query_id"],
                "n": native["n"],
                "measurement_failures": summary["n_failed"],
                "execution_attempt_count": attempts,
                "failed_attempt_record_count": failed_attempts,
                "attempt_counts_verified": verified,
                "coverage_median": native["median"],
                "coverage_iqr": native["iqr"],
                "coverage_min": native["min"],
                "coverage_max": native["max"],
                "coverage_ci_lower": (
                    native["median_ci_95"]["lower"] if native["median_ci_95"] else None
                ),
                "coverage_ci_upper": (
                    native["median_ci_95"]["upper"] if native["median_ci_95"] else None
                ),
                "median_native_operator_count": native["native_operator_count"],
                "median_fallback_operator_count": native["fallback_operator_count"],
                "median_transition_count": native["transition_count"],
                "median_native_subtree_count": native["native_subtree_count"],
                "native_operator_names": inventory.get("native_operators", []),
                "fallback_operator_names": inventory.get("fallback_operators", []),
                "transition_operator_names": inventory.get("transition_operators", []),
                "scan_implementations": inventory.get("scans", []),
                "join_strategies": inventory.get("joins", []),
                "partition_counts": inventory.get("partition_counts", []),
                "fallback_reason_annotations": native["fallback_reasons"],
                "fallback_detected": native["fallback_detected"],
                "unannotated_fallback": native["unannotated_fallback"],
                "paired_final_plan_stable": stable,
                "paired_final_plan_hash_combination_count": final_hashes,
            }
        )
    return rows


def _write_native_matrix_csv(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    fields = (
        "experiment_id",
        "workload",
        "query_id",
        "n",
        "measurement_failures",
        "execution_attempt_count",
        "failed_attempt_record_count",
        "attempt_counts_verified",
        "coverage_median",
        "coverage_iqr",
        "coverage_min",
        "coverage_max",
        "coverage_ci_lower",
        "coverage_ci_upper",
        "median_native_operator_count",
        "median_fallback_operator_count",
        "median_transition_count",
        "median_native_subtree_count",
        "native_operator_names",
        "fallback_operator_names",
        "transition_operator_names",
        "scan_implementations",
        "join_strategies",
        "partition_counts",
        "fallback_reason_annotations",
        "fallback_detected",
        "unannotated_fallback",
        "paired_final_plan_stable",
        "paired_final_plan_hash_combination_count",
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for source in rows:
            row = dict(source)
            for field in (
                "native_operator_names",
                "fallback_operator_names",
                "transition_operator_names",
                "scan_implementations",
                "join_strategies",
                "partition_counts",
                "fallback_reason_annotations",
            ):
                row[field] = json.dumps(row[field], ensure_ascii=False, separators=(",", ":"))
            writer.writerow(row)


def _expected_experiment_ids(publication: Mapping[str, Any]) -> list[str]:
    policy = publication.get("policy")
    if not isinstance(policy, Mapping) or not isinstance(policy.get("core_experiments"), list):
        return []
    result: list[str] = []
    for item in policy["core_experiments"]:
        if isinstance(item, Mapping) and isinstance(item.get("experiment_id"), str):
            result.append(item["experiment_id"])
    return sorted(result)


def _expected_measurement_pairs(publication: Mapping[str, Any]) -> int:
    policy = publication.get("policy")
    if isinstance(policy, Mapping):
        value = policy.get("expected_measurement_pairs_per_experiment")
        if isinstance(value, int) and not isinstance(value, bool) and value > 0:
            return value
    return 10


def _tpch_evidence_sentence(findings: Mapping[str, Any] | None) -> str:
    if not isinstance(findings, Mapping):
        return "Observed TPC-H scale coverage could not be established from the admitted records."
    rq3 = findings.get("RQ3")
    comparison = rq3.get("scale_comparison") if isinstance(rq3, Mapping) else None
    scales = comparison.get("scales_present") if isinstance(comparison, Mapping) else None
    if not isinstance(scales, list):
        return "Observed TPC-H scale coverage could not be established from the admitted records."
    normalized = sorted(
        value
        for value in scales
        if isinstance(value, int) and not isinstance(value, bool) and value > 0
    )
    if not normalized:
        return "No admitted TPC-H experiment evidence is present."
    labels = [f"SF{value}" for value in normalized]
    if normalized == [1]:
        return "Admitted TPC-H evidence covers SF1 only; SF10 is absent."
    if normalized == [10]:
        return "Admitted TPC-H evidence covers SF10 only; SF1 is absent."
    return (
        "Admitted TPC-H evidence covers "
        + ", ".join(labels[:-1])
        + f" and {labels[-1]}; matched-query comparability is evaluated in RQ3."
    )


def _scale_limit_line(findings: Mapping[str, Any] | None) -> str:
    if not isinstance(findings, Mapping):
        return "- Scale evidence is unavailable."
    rq3 = findings.get("RQ3")
    comparison = rq3.get("scale_comparison") if isinstance(rq3, Mapping) else None
    if not isinstance(comparison, Mapping):
        return "- Scale evidence is unavailable."
    scales = comparison.get("scales_present")
    if scales == [1]:
        return (
            "- No admitted SF10 evidence is present, so scale sensitivity remains unanswered "
            "rather than inferred."
        )
    if scales == [10]:
        return (
            "- No admitted SF1 evidence is present, so scale sensitivity remains unanswered "
            "rather than inferred."
        )
    if comparison.get("estimability") == "descriptive_only":
        return (
            "- Matched SF1/SF10 comparisons are two-point descriptive evidence, not a general "
            "scalability trend."
        )
    if isinstance(scales, list) and len(scales) >= 2:
        return (
            "- Multiple admitted scales are present, but scale sensitivity is not estimable: "
            f"{comparison.get('reason', 'matched-query evidence is incomplete')}"
        )
    return "- Scale evidence is unavailable."


def _pair_count_limit_line(summaries: Mapping[str, Mapping[str, Any]]) -> str:
    counts = sorted(
        {
            int(summary["paired_speedup"]["n"])
            for summary in summaries.values()
            if isinstance(summary.get("paired_speedup"), Mapping)
            and isinstance(summary["paired_speedup"].get("n"), int)
            and not isinstance(summary["paired_speedup"].get("n"), bool)
        }
    )
    if not counts:
        return (
            "- Admitted paired sample sizes are unavailable. P95 is omitted for each engine "
            f"with fewer than {MIN_P95_SAMPLE_SIZE} successful measurements."
        )
    elif len(counts) == 1:
        count_text = f"n={counts[0]} pairs per experiment"
        prefix = "Admitted paired sample size is"
    else:
        count_text = "n=" + ", ".join(str(value) for value in counts) + " pairs across experiments"
        prefix = "Admitted paired sample sizes are"
    return (
        f"- {prefix} {count_text}. P95 is omitted for each engine "
        f"with fewer than {MIN_P95_SAMPLE_SIZE} successful measurements."
    )


def _resource_saving_summary_line(
    summaries: Mapping[str, Mapping[str, Any]], metric: str, label: str
) -> str:
    values: list[float] = []
    unavailable = 0
    for summary in summaries.values():
        resources = summary.get("paired_resource_savings")
        descriptor = resources.get(metric) if isinstance(resources, Mapping) else None
        relative = (
            descriptor.get("relative_saving_ratio") if isinstance(descriptor, Mapping) else None
        )
        count = relative.get("n") if isinstance(relative, Mapping) else None
        median = relative.get("median") if isinstance(relative, Mapping) else None
        if (
            not isinstance(count, int)
            or isinstance(count, bool)
            or count < 1
            or isinstance(median, bool)
            or not isinstance(median, int | float)
            or not math.isfinite(float(median))
        ):
            unavailable += 1
            continue
        values.append(float(median))
    positive = sum(value > 0.0 for value in values)
    zero = sum(value == 0.0 for value in values)
    negative = sum(value < 0.0 for value in values)
    return (
        f"Median paired {label} saving ratios by experiment: positive={positive}, zero={zero}, "
        f"negative={negative}, unavailable={unavailable} (total={len(summaries)})."
    )


def _finding_experiment_label(experiment: Mapping[str, Any]) -> str:
    label = f"{experiment['query_id']} ({experiment['experiment_id']}"
    scale = experiment.get("scale_factor")
    if isinstance(scale, int) and not isinstance(scale, bool):
        label += f", SF{scale}"
    return label + ")"


def _rq1_result_lines(findings: Mapping[str, Any]) -> list[str]:
    rq1 = findings["RQ1"]
    h1 = findings["H1"]
    if not isinstance(rq1, Mapping) or not isinstance(h1, Mapping):
        raise ValueError("RQ1 and H1 findings must be objects")
    experiments = rq1["experiments"]
    counts = h1["classification_counts"]
    if not isinstance(experiments, list) or not experiments or not isinstance(counts, Mapping):
        raise ValueError("RQ1 experiments and H1 classification counts are incomplete")
    medians = [float(experiment["paired_speedup"]["median"]) for experiment in experiments]
    above = sum(value > 1.0 for value in medians)
    equal = sum(value == 1.0 for value in medians)
    below = sum(value < 1.0 for value in medians)
    largest = max(experiments, key=lambda item: float(item["paired_speedup"]["median"]))
    smallest = min(experiments, key=lambda item: float(item["paired_speedup"]["median"]))
    return [
        f"At the point-estimate level, median paired speedup was above 1 in {above} of "
        f"{_count_label(len(experiments), 'experiment')}, equal to 1 in {equal}, and below 1 in "
        f"{below}. Under the fixed 95% CI decision rule, H1 classification counts were "
        f"supported={counts['supported']}, inconclusive={counts['inconclusive']}, and "
        f"decreased={counts['decreased']}.",
        f"The largest observed median paired speedup was "
        f"{float(largest['paired_speedup']['median']):.3f}x for "
        f"{_finding_experiment_label(largest)}; the smallest was "
        f"{float(smallest['paired_speedup']['median']):.3f}x for "
        f"{_finding_experiment_label(smallest)}. The tables above retain IQR and full ranges, "
        "so point estimates are not presented without their run-to-run variability.",
    ]


def _rq2_result_lines(findings: Mapping[str, Any]) -> list[str]:
    rq2 = findings["RQ2"]
    if not isinstance(rq2, Mapping) or not isinstance(rq2.get("experiments"), list):
        raise ValueError("RQ2 experiments are incomplete")
    experiments = rq2["experiments"]
    coverage_rows: list[tuple[Mapping[str, Any], float, bool]] = []
    for experiment in experiments:
        if not isinstance(experiment, Mapping):
            raise ValueError("RQ2 experiment must be an object")
        engines = experiment.get("engines")
        comet = engines.get("comet_accelerated") if isinstance(engines, Mapping) else None
        if not isinstance(comet, Mapping):
            raise ValueError("RQ2 Comet plan descriptor is unavailable")
        coverage = comet.get("native_coverage_ratio")
        median = coverage.get("median") if isinstance(coverage, Mapping) else None
        if isinstance(median, bool) or not isinstance(median, int | float):
            raise ValueError("RQ2 Comet native coverage median is unavailable")
        coverage_rows.append((experiment, float(median), bool(comet.get("fallback_detected"))))
    if not coverage_rows:
        return ["No admitted experiment is available for a native-coverage assessment."]
    full = sum(value == 1.0 for _, value, _ in coverage_rows)
    partial = sum(0.0 < value < 1.0 for _, value, _ in coverage_rows)
    zero = sum(value == 0.0 for _, value, _ in coverage_rows)
    fallback = sum(detected for _, _, detected in coverage_rows)
    lowest_experiment, lowest_coverage, _ = min(coverage_rows, key=lambda item: item[1])
    return [
        f"Median count-based native coverage was 100% in {full} of "
        f"{_count_label(len(coverage_rows), 'experiment')}, partial in {partial}, and 0% in "
        f"{zero}; coverage or operator counts detected fallback in "
        f"{_count_label(fallback, 'experiment')}.",
        f"The lowest observed median native coverage was {_format_percent(lowest_coverage)} for "
        f"{_finding_experiment_label(lowest_experiment)}.",
    ]


def _report_markdown(
    *,
    publishable: bool,
    summaries: Mapping[str, Mapping[str, Any]],
    native_summaries: Mapping[str, Mapping[str, Any]],
    resource_profiles: Mapping[str, Mapping[str, Any]],
    plan_insights: Mapping[str, Any] | None,
    findings: Mapping[str, Any] | None,
    matrix_rows: Iterable[Mapping[str, Any]],
    attempt_metadata: Mapping[str, tuple[int, int, bool]],
    suite_geometric_mean: float | None,
    analysis_issues: Iterable[str],
) -> str:
    banner = (
        ["> **PUBLICATION GATE: PASSED.** Evidence and report-content contracts both passed."]
        if publishable
        else [
            "> [!WARNING]",
            "> **DIAGNOSTIC ONLY \N{EM DASH} NOT PUBLISHABLE.** Evidence or report content is "
            "incomplete.",
            "> Review `report-publishability.json` and `report-contract.json` before citation.",
        ]
    )
    lines = [
        "# Spark vs DataFusion Comet - Rebuildable Research Report",
        "",
        *banner,
        "",
        "## Scope, method, and evidence boundary",
        "",
        "This report is rebuilt from immutable raw-result records. The primary estimator is the "
        "median of paired Spark/Comet wall-time speedups. Ratio of medians is retained only as a "
        "secondary cross-check. Confidence intervals use "
        f"{DEFAULT_BOOTSTRAP_RESAMPLES:,} deterministic percentile bootstrap resamples with "
        f"seed {DEFAULT_BOOTSTRAP_SEED}. P95 is shown only when an engine has at least "
        f"{MIN_P95_SAMPLE_SIZE} successful measurements and uses linear R-7 interpolation.",
        "",
        "TPC-H workloads are derived from TPC-H and are not audited TPC-H benchmark results. "
        + _tpch_evidence_sentence(findings),
        "",
        "Suite geometric-mean speedup across per-query median paired speedups: "
        + _format_number(suite_geometric_mean)
        + f" (n={len(summaries)} experiments).",
        "",
        "## Primary paired-speedup results",
        "",
        "| Experiment | Workload | Query | n pairs | Measurement failures | "
        "Failed execution attempts/total | Median | IQR | Min | Max | 95% bootstrap CI | "
        "Ratio of medians |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for experiment_id in sorted(summaries):
        summary = summaries[experiment_id]
        stats = summary["paired_speedup"]
        attempts, attempt_failures, verified = attempt_metadata[experiment_id]
        attempt_text = f"{attempt_failures}/{attempts}" + (
            "" if verified else " (unverified diagnostic fallback)"
        )
        lines.append(
            "| "
            + " | ".join(
                [
                    experiment_id,
                    str(summary["workload"]),
                    str(summary["query_id"]),
                    str(stats["n"]),
                    str(summary["n_failed"]),
                    attempt_text,
                    _format_number(stats["median"]),
                    _format_number(stats["iqr"]),
                    _format_number(stats["min"]),
                    _format_number(stats["max"]),
                    _format_interval(summary["paired_speedup_ci"]),
                    _format_number(summary["ratio_of_medians"]),
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Latency distributions",
            "",
            "Each row below reports the complete latency distribution used by its SVG. "
            "Failures refer to accepted measurement records. Execution-attempt failures include "
            "preserved retries and are reported separately. Both failure columns are campaign-wide "
            "counts repeated on the engine rows; they are not per-engine attributions.",
            "",
            "| Experiment / engine | n | Campaign measurement failures | "
            "Campaign failed attempts/total | "
            "Median | IQR | Min | Max | 95% bootstrap CI | P95 | Unit |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|",
        ]
    )
    for experiment_id in sorted(summaries):
        summary = summaries[experiment_id]
        for engine, label in (("spark_baseline", "Spark"), ("comet_accelerated", "Comet")):
            lines.append(
                _format_stat_row(
                    f"{experiment_id} / {label}",
                    summary["engines"][engine],
                    failures=int(summary["n_failed"]),
                    attempts=attempt_metadata[experiment_id],
                    unit="ms",
                )
            )
        lines.extend(
            [
                "",
                f"![{experiment_id} latency distribution]({experiment_id}.latency.svg)",
                "",
            ]
        )

    lines.extend(
        [
            "## CPU and memory profiles over normalized elapsed time",
            "",
            "Lines show medians and shaded bands show fixed-seed 95% bootstrap CIs at every "
            "normalized time point. The accompanying JSON records n, failures, median, IQR, "
            "min/max, and CI for both CPU and current cgroup memory at all 21 grid points.",
            "",
        ]
    )
    for experiment_id in sorted(summaries):
        if experiment_id in resource_profiles:
            lines.extend(
                [
                    f"### {experiment_id}",
                    "",
                    f"![{experiment_id} CPU and memory profile]"
                    f"({experiment_id}.resource-profile.svg)",
                    "",
                    f"Machine-readable profile: `{experiment_id}.resource-profile.json`.",
                    "",
                ]
            )
        else:
            lines.extend([f"### {experiment_id}", "", "Resource profile unavailable.", ""])

    lines.extend(
        [
            "## Paired resource-saving distributions",
            "",
            "Positive values mean Comet used less resource than Spark. The paired CPU saving ratio "
            "and every other percentage use `(Spark - Comet) / Spark`. When the Spark baseline is "
            "zero, the report uses the absolute delta and does not invent a percentage.",
            "",
            _resource_saving_summary_line(summaries, "cpu_core_seconds", "CPU core-seconds"),
            _resource_saving_summary_line(summaries, "cgroup_memory_peak_mib", "peak-memory"),
            "",
            "| Experiment / metric / estimator | n | Campaign measurement failures | "
            "Campaign failed attempts/total | Median | IQR | Min | Max | 95% bootstrap CI | "
            "P95 | Unit |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|",
        ]
    )
    for experiment_id in sorted(summaries):
        summary = summaries[experiment_id]
        resources = summary["paired_resource_savings"]
        for metric in _RESOURCE_METRICS:
            descriptor = resources[metric]
            relative = descriptor["relative_saving_ratio"]
            if int(relative["n"]) > 0:
                selected = relative
                estimator = "relative saving"
                unit = "ratio"
            else:
                selected = descriptor["absolute_delta"]
                estimator = "absolute delta (zero Spark baseline)"
                unit = str(descriptor["unit"])
            lines.append(
                _format_stat_row(
                    f"{experiment_id} / {_RESOURCE_LABELS[metric]} / {estimator}",
                    selected,
                    failures=int(summary["n_failed"]),
                    attempts=attempt_metadata[experiment_id],
                    unit=unit,
                )
            )

    lines.extend(
        [
            "",
            "## Native operator coverage and fallback matrix",
            "",
            "Count-based coverage describes physical-plan operators. It is not a percentage of "
            "CPU time or wall time executed natively.",
            "",
            "| Experiment | n | Measurement failures | Failed attempts/total | "
            "Coverage median | IQR | Min | Max | 95% bootstrap CI | "
            "Native / fallback / transitions / subtrees (median) | Native operator names | "
            "Fallback operator names | Transition names | Fallback annotation |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|",
        ]
    )
    for row in matrix_rows:
        ci = {
            "lower": row["coverage_ci_lower"],
            "upper": row["coverage_ci_upper"],
        }
        coverage_ci = (
            f"[{_format_percent(ci['lower'])}, {_format_percent(ci['upper'])}]"
            if ci["lower"] is not None and ci["upper"] is not None
            else "n/a"
        )
        annotations = row["fallback_reason_annotations"]
        if row["unannotated_fallback"]:
            annotation = "fallback observed; no explicit reason annotation"
        elif annotations:
            annotation = ", ".join(str(item) for item in annotations)
        elif row["fallback_detected"]:
            annotation = "fallback observed; annotation status unavailable"
        else:
            annotation = "no fallback observed"
        native_names = ", ".join(str(item) for item in row["native_operator_names"]) or "n/a"
        fallback_names = ", ".join(str(item) for item in row["fallback_operator_names"]) or "none"
        transition_names = (
            ", ".join(str(item) for item in row["transition_operator_names"]) or "none"
        )
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row["experiment_id"]),
                    str(row["n"]),
                    str(row["measurement_failures"]),
                    f"{row['failed_attempt_record_count']}/{row['execution_attempt_count']}"
                    + (
                        ""
                        if row["attempt_counts_verified"]
                        else " (unverified diagnostic fallback)"
                    ),
                    _format_percent(row["coverage_median"]),
                    _format_percent(row["coverage_iqr"]),
                    _format_percent(row["coverage_min"]),
                    _format_percent(row["coverage_max"]),
                    coverage_ci,
                    "/".join(
                        _format_number(row[field])
                        for field in (
                            "median_native_operator_count",
                            "median_fallback_operator_count",
                            "median_transition_count",
                            "median_native_subtree_count",
                        )
                    ),
                    native_names,
                    fallback_names,
                    transition_names,
                    annotation,
                ]
            )
            + " |"
        )
        lines.extend(
            [
                "",
                f"![{row['experiment_id']} native coverage]"
                f"({row['experiment_id']}.native-coverage.svg)",
                "",
            ]
        )

    lines.extend(
        [
            "Machine-readable matrix: `native-operator-matrix.csv`.",
            "",
            "## Initial and final AQE plan analysis",
            "",
        ]
    )
    if isinstance(plan_insights, Mapping) and isinstance(plan_insights.get("experiments"), Mapping):
        lines.extend(
            [
                "The table is a categorical plan inventory, so median/IQR/bootstrap CI do not "
                "apply to hash or operator identities. Latency distributions for every hash "
                "stratum, including n, median, IQR, min/max and CI, are stored in "
                "`plan-insights.json`.",
                "",
                "| Experiment | n pairs | Spark initial/final semantic hashes | "
                "Comet initial/final semantic hashes | Paired final combinations | Stable | "
                "Joins | Partitions | Scans |",
                "|---|---:|---:|---:|---:|---|---|---|---|",
            ]
        )
        for experiment_id, experiment in sorted(plan_insights["experiments"].items()):
            engines = experiment["engines"]
            spark = engines["spark_baseline"]["plans"]
            comet = engines["comet_accelerated"]["plans"]
            inventory = _structure_inventory(experiment)
            lines.append(
                "| "
                + " | ".join(
                    [
                        experiment_id,
                        str(experiment["pair_count"]),
                        f"{spark['initial']['distinct_hash_count']}/{spark['final']['distinct_hash_count']}",
                        f"{comet['initial']['distinct_hash_count']}/{comet['final']['distinct_hash_count']}",
                        str(experiment["paired_final_plan_hash_combination_count"]),
                        "yes" if experiment["paired_final_plan_stable"] else "no - stratified",
                        ", ".join(inventory.get("joins", [])) or "none",
                        ", ".join(str(item) for item in inventory.get("partition_counts", []))
                        or "none",
                        ", ".join(inventory.get("scans", [])) or "none",
                    ]
                )
                + " |"
            )
    else:
        lines.append("Plan insights unavailable.")

    lines.extend(["", "## Answers to the research questions", ""])
    if isinstance(findings, Mapping):
        rq1 = findings["RQ1"]
        lines.extend(
            [
                "### RQ1 - Latency and variability",
                "",
                *_rq1_result_lines(findings),
                "",
            ]
        )
        for group in rq1["workload_groups"]:
            descriptor = group["experiment_median_speedups"]
            lines.append(
                f"- {group['workload']}: n={descriptor['n']} experiment medians; "
                f"median={descriptor['median']:.3f}x, IQR={descriptor['iqr']:.3f}x, "
                f"range={descriptor['min']:.3f}-{descriptor['max']:.3f}x. "
                "No pooled CI is inferred."
            )
        inventory_native = sorted(
            {str(name) for row in matrix_rows for name in row["native_operator_names"]}
        )
        inventory_fallback = sorted(
            {str(name) for row in matrix_rows for name in row["fallback_operator_names"]}
        )
        partial = findings["RQ3"]["partial_native_m08"]
        partial_lines = _partial_native_m08_report_lines(partial)
        scale_comparison = findings["RQ3"]["scale_comparison"]
        scale_assessment = str(scale_comparison["estimability"])
        if scale_assessment == "descriptive_only":
            scale_interpretation = (
                "Matched SF1/SF10 evidence supports only an exploratory two-point "
                "description, not a general scalability claim."
            )
        else:
            scale_interpretation = (
                "Scale sensitivity is not estimable, so this report makes no SF1-to-SF10 "
                "or general scalability claim."
            )
        partial_interpretation = (
            "The observed partial-native M08 result is descriptive only."
            if partial["status"] == "observed"
            else "No partial-native M08 result was observed, so none is claimed."
        )
        lines.extend(
            [
                "",
                "### RQ2 - Native suitability and fallback",
                "",
                *_rq2_result_lines(findings),
                "",
                "Observed native operators: " + (", ".join(inventory_native) or "none") + ".",
                "",
                "Observed non-native or fallback operators in Comet final plans: "
                + (", ".join(inventory_fallback) or "none")
                + ".",
                "",
                *partial_lines,
                "",
                "### RQ3 - Scale sensitivity and fallback overhead",
                "",
                scale_comparison["reason"],
                "",
                f"{scale_interpretation} {partial_interpretation} "
                f"{findings['RQ3']['fallback_overhead']['reason']} Therefore, the report does "
                "not estimate a causal Arrow/JVM conversion overhead.",
                "",
                "## Hypothesis assessment",
                "",
                f"- H1 classification counts: "
                f"supported={findings['H1']['classification_counts']['supported']}, "
                f"inconclusive={findings['H1']['classification_counts']['inconclusive']}, "
                f"decreased={findings['H1']['classification_counts']['decreased']}. "
                "Effect sizes and CIs remain in the primary table.",
            ]
        )
        h2 = findings["H2"]
        for metric, result in h2["overall"]["correlations"].items():
            rho = "not estimable" if result["rho"] is None else f"rho={result['rho']:.3f}"
            lines.append(
                f"- H2 overall {metric}: n={result['n']}, {rho}; descriptive exploratory "
                "Spearman correlation with log median speedup, no p-value and no causal claim."
            )
        for grouping_name, grouping in (
            ("workload", h2["by_workload"]),
            ("data scale", h2.get("by_data_scale", {})),
        ):
            for stratum, result in grouping.items():
                summaries_text = ", ".join(
                    f"{metric}="
                    + (
                        f"{value['rho']:.3f}"
                        if value["rho"] is not None
                        else f"not estimable ({value['reason']})"
                    )
                    for metric, value in result["correlations"].items()
                )
                lines.append(
                    f"- H2 by {grouping_name}, {stratum}, n={result['experiment_count']}: "
                    + summaries_text
                    + "."
                )
        h3 = findings["H3"]
        lines.append(
            f"- H3: {str(h3['assessment']).replace('_', ' ')}; {h3['reason']} "
            "No general scalability claim is made."
        )
    else:
        lines.extend(
            [
                "### RQ1 - Latency and variability",
                "",
                "Not estimable from the admitted diagnostic records.",
                "",
                "### RQ2 - Native suitability and fallback",
                "",
                "Not estimable from the admitted diagnostic records.",
                "",
                "### RQ3 - Scale sensitivity and fallback overhead",
                "",
                "Not estimable from the admitted diagnostic records.",
            ]
        )

    scale_limit = _scale_limit_line(findings)
    lines.extend(
        [
            "",
            "## Limitations",
            "",
            "- The admitted records do not establish multi-node behavior or portability to other "
            "hardware.",
            scale_limit,
            "- Coverage counts operators, not runtime share. Fallback correlations are "
            "observational.",
            _pair_count_limit_line(summaries),
            "- TPC-H-derived numbers are non-audited and must not be presented as official "
            "TPC-H results.",
            "",
            "## Reproducibility and artifact index",
            "",
            "- `normalized-measurements.csv`: normalized empirical measurements.",
            "- `research-findings.json`: machine-readable RQ/H results.",
            "- `plan-insights.json`: initial/final semantic plan hashes and hash-stratified "
            "latency.",
            "- `resource-profiles.json`: all per-experiment time-normalized resource profiles.",
            "- `native-operator-matrix.csv`: operator coverage/fallback matrix.",
            "- `report-contract.json`: report-content gate.",
            "- `report-publishability.json`: combined evidence and content gate.",
            "- `report-artifact-inventory.json`: size and SHA-256 for every generated report "
            "artifact.",
        ]
    )
    issues = list(analysis_issues)
    if issues:
        lines.extend(["", "## Diagnostic analysis issues", ""])
        lines.extend(f"- {issue}" for issue in issues)
    return "\n".join(lines) + "\n"


def _report_contract(
    *,
    output_dir: Path,
    expected_ids: Iterable[str],
    expected_pair_count: int,
    summaries: Mapping[str, Mapping[str, Any]],
    native_summaries: Mapping[str, Mapping[str, Any]],
    resource_profiles: Mapping[str, Mapping[str, Any]],
    plan_insights: Mapping[str, Any] | None,
    findings: Mapping[str, Any] | None,
    attempt_metadata: Mapping[str, tuple[int, int, bool]],
    report_text: str,
) -> dict[str, Any]:
    expected = set(expected_ids)
    checks: dict[str, dict[str, Any]] = {}

    def record(name: str, issues: Iterable[str]) -> None:
        values = list(issues)
        checks[name] = {"passed": not values, "issues": values}

    summary_issues: list[str] = []
    if set(summaries) != expected:
        summary_issues.append(
            f"summary experiment set mismatch: expected={sorted(expected)}, "
            f"observed={sorted(summaries)}"
        )
    required_stats = ("n", "median", "q1", "q3", "iqr", "min", "max")
    for experiment_id, summary in summaries.items():
        for engine in _ENGINES:
            stats = summary["engines"][engine]
            if stats.get("n") != expected_pair_count:
                summary_issues.append(
                    f"{experiment_id} {engine} n must equal {expected_pair_count}"
                )
            for field in required_stats:
                if stats.get(field) is None:
                    summary_issues.append(f"{experiment_id} {engine} missing {field}")
            if not isinstance(stats.get("median_ci_95"), Mapping):
                summary_issues.append(f"{experiment_id} {engine} missing median CI")
        paired = summary["paired_speedup"]
        if any(paired.get(field) is None for field in required_stats):
            summary_issues.append(f"{experiment_id} paired speedup descriptor incomplete")
        if not isinstance(summary.get("paired_speedup_ci"), Mapping):
            summary_issues.append(f"{experiment_id} paired speedup CI missing")
    record("complete_distribution_statistics", summary_issues)

    attempt_issues = [
        f"{experiment_id} execution-attempt counts are not verified"
        for experiment_id in sorted(expected)
        if experiment_id not in attempt_metadata or not attempt_metadata[experiment_id][2]
    ]
    record("measurement_vs_execution_attempt_failures", attempt_issues)

    native_issues: list[str] = []
    if set(native_summaries) != expected:
        native_issues.append("native coverage experiment set is incomplete")
    for experiment_id, native in native_summaries.items():
        if native.get("n") != expected_pair_count:
            native_issues.append(
                f"{experiment_id} native coverage n must equal {expected_pair_count}"
            )
        if any(native.get(field) is None for field in required_stats):
            native_issues.append(f"{experiment_id} native coverage statistics incomplete")
        if not isinstance(native.get("median_ci_95"), Mapping):
            native_issues.append(f"{experiment_id} native coverage CI missing")
        if (
            native.get("fallback_detected") is True
            and not native.get("fallback_reasons")
            and native.get("unannotated_fallback") is not True
        ):
            native_issues.append(f"{experiment_id} unannotated fallback is not explicit")
    record("native_coverage_and_fallback", native_issues)

    resource_issues: list[str] = []
    if set(resource_profiles) != expected:
        resource_issues.append("resource profile experiment set is incomplete")
    for experiment_id, profile in resource_profiles.items():
        engines = profile.get("engines")
        if not isinstance(engines, Mapping) or set(engines) != set(_ENGINES):
            resource_issues.append(f"{experiment_id} resource engines incomplete")
            continue
        for engine in _ENGINES:
            if engines[engine].get("run_count") != expected_pair_count:
                resource_issues.append(
                    f"{experiment_id} {engine} resource n must equal {expected_pair_count}"
                )
            rows = engines[engine].get("profiles")
            if not isinstance(rows, list) or len(rows) != 21:
                resource_issues.append(f"{experiment_id} {engine} requires 21 profile points")
                continue
            for index, row in enumerate(rows):
                for metric in ("cpu_percent_of_limit", "memory_current_mib"):
                    stats = row.get(metric)
                    if not isinstance(stats, Mapping) or any(
                        stats.get(field) is None for field in required_stats
                    ):
                        resource_issues.append(
                            f"{experiment_id} {engine} point {index} {metric} incomplete"
                        )
                        break
                    if not isinstance(stats.get("median_ci_95"), Mapping):
                        resource_issues.append(
                            f"{experiment_id} {engine} point {index} {metric} CI missing"
                        )
    record("cpu_memory_profiles", resource_issues)

    plan_issues: list[str] = []
    plan_experiments = (
        plan_insights.get("experiments") if isinstance(plan_insights, Mapping) else None
    )
    if not isinstance(plan_experiments, Mapping) or set(plan_experiments) != expected:
        plan_issues.append("plan-insights experiment set is incomplete")
    else:
        for experiment_id, experiment in plan_experiments.items():
            if experiment.get("pair_count") != expected_pair_count:
                plan_issues.append(
                    f"{experiment_id} plan pair count must equal {expected_pair_count}"
                )
            if not isinstance(experiment.get("paired_final_plan_hash_strata"), list):
                plan_issues.append(f"{experiment_id} paired plan hash strata missing")
    record("initial_final_plan_analysis", plan_issues)

    finding_issues: list[str] = []
    if not isinstance(findings, Mapping):
        finding_issues.append("research findings unavailable")
    else:
        for name in ("RQ1", "RQ2", "RQ3", "H1", "H2", "H3"):
            if not isinstance(findings.get(name), Mapping):
                finding_issues.append(f"{name} findings missing")
        scope = findings.get("analysis_scope")
        if not isinstance(scope, Mapping) or scope.get("experiment_count") != len(expected):
            finding_issues.append("research findings experiment count is incomplete")
        h2 = findings.get("H2")
        if not isinstance(h2, Mapping) or not isinstance(h2.get("by_data_scale"), Mapping):
            finding_issues.append("H2 data-scale stratification missing")
    record("research_questions_and_hypotheses", finding_issues)

    artifact_issues: list[str] = []
    for experiment_id in sorted(expected):
        for suffix in (
            ".summary.json",
            ".latency.svg",
            ".native-coverage.svg",
            ".resource-profile.json",
            ".resource-profile.svg",
        ):
            path = output_dir / f"{experiment_id}{suffix}"
            if not path.is_file() or path.stat().st_size == 0:
                artifact_issues.append(f"missing report artifact {path.name}")
            elif suffix.endswith(".svg"):
                try:
                    ET.fromstring(path.read_text(encoding="utf-8"))
                except (ET.ParseError, OSError, UnicodeError) as error:
                    artifact_issues.append(f"invalid SVG {path.name}: {error}")
    for name in (
        "normalized-measurements.csv",
        "native-operator-matrix.csv",
        "plan-insights.json",
        "research-findings.json",
        "resource-profiles.json",
        "suite-summary.json",
        "technical-report.md",
    ):
        path = output_dir / name
        if not path.is_file() or path.stat().st_size == 0:
            artifact_issues.append(f"missing report artifact {name}")
    record("artifact_set", artifact_issues)

    required_sections = (
        "## Primary paired-speedup results",
        "## Latency distributions",
        "## CPU and memory profiles over normalized elapsed time",
        "## Native operator coverage and fallback matrix",
        "## Initial and final AQE plan analysis",
        "### RQ1 - Latency and variability",
        "### RQ2 - Native suitability and fallback",
        "### RQ3 - Scale sensitivity and fallback overhead",
        "## Hypothesis assessment",
        "## Limitations",
        "derived from TPC-H and are not audited",
    )
    record(
        "technical_report_sections",
        [
            f"technical report missing {section!r}"
            for section in required_sections
            if section not in report_text
        ],
    )

    issues = [f"{name}: {issue}" for name, check in checks.items() for issue in check["issues"]]
    return {
        "schema_version": 1,
        "status": "passed" if not issues else "failed",
        "passed": not issues,
        "expected_experiment_ids": sorted(expected),
        "checks": checks,
        "issues": issues,
    }


def _write_artifact_inventory(path: Path, artifacts: Iterable[Path], output_dir: Path) -> None:
    entries = []
    for artifact in sorted({item.resolve() for item in artifacts}):
        if not artifact.is_file():
            continue
        payload = artifact.read_bytes()
        entries.append(
            {
                "path": artifact.relative_to(output_dir.resolve()).as_posix(),
                "size_bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        )
    write_json(
        path,
        {
            "schema_version": 1,
            "scope": "generated-report-artifacts-excluding-this-inventory",
            "artifact_count": len(entries),
            "artifacts": entries,
        },
        immutable=False,
    )


def build_report(
    raw_root: Path,
    output_dir: Path,
    *,
    campaign_root: Path | None = None,
    require_publishable: bool = False,
    repository_root: Path = ROOT,
) -> tuple[Path, ...]:
    records = _load_records(raw_root)
    if any(str(record["experiment_id"]).upper().startswith("SMOKE") for record in records):
        raise ValueError("smoke/readiness artifacts cannot be promoted into a research report")
    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        if record["phase"] == "measurement":
            grouped[_identity(record)].append(record)
    output_dir.mkdir(parents=True, exist_ok=True)
    for name in (
        "native-operator-matrix.csv",
        "plan-insights.json",
        "report-artifact-inventory.json",
        "report-contract.json",
        "report-publishability.json",
        "research-findings.json",
        "resource-profiles.json",
        "suite-summary.json",
        "technical-report.md",
    ):
        stale = output_dir / name
        if stale.is_file():
            stale.unlink()
    publication = assess_report_publishability(
        records,
        campaign_root if campaign_root is not None else repository_root / ".artifacts/campaigns",
        repository_root=repository_root,
    )
    evidence_publishable = bool(publication.get("publishable"))
    produced: list[Path] = []
    all_measurements: list[dict[str, Any]] = []
    primary_speedups: list[float] = []
    summaries: dict[str, dict[str, Any]] = {}
    native_summaries: dict[str, dict[str, Any]] = {}
    resource_profiles: dict[str, dict[str, Any]] = {}
    attempt_metadata: dict[str, tuple[int, int, bool]] = {}
    analysis_issues: list[str] = []
    for identity in sorted(grouped):
        experiment_id, _suite, query_id, _storage = identity
        rows = grouped[identity]
        all_measurements.extend(rows)
        try:
            summary = summarize_records(rows)
            _enrich_summary(summary, rows)
        except ValueError as error:
            analysis_issues.append(f"{experiment_id} summary could not be built: {error}")
            continue
        summaries[experiment_id] = summary
        native = _native_summary(rows)
        native_summaries[experiment_id] = native
        attempts = _attempt_counts(publication, experiment_id, int(summary["n_total"]))
        attempt_metadata[experiment_id] = attempts
        paired_median = summary["paired_speedup"]["median"]
        if paired_median is not None:
            primary_speedups.append(float(paired_median))
        summary_path = output_dir / f"{experiment_id}.summary.json"
        write_json(summary_path, summary, immutable=False)
        produced.append(summary_path)

        latency_path = output_dir / f"{experiment_id}.latency.svg"
        try:
            latency_path.write_text(
                latency_distribution_svg(
                    summary,
                    f"{query_id} latency distribution",
                    int(summary["n_failed"]),
                    attempts[0],
                    attempts[1],
                ),
                encoding="utf-8",
                newline="\n",
            )
            produced.append(latency_path)
        except ReportChartError as error:
            analysis_issues.append(f"{experiment_id} latency chart failed: {error}")

        coverage_path = output_dir / f"{experiment_id}.native-coverage.svg"
        try:
            coverage_path.write_text(
                native_coverage_distribution_svg(
                    native,
                    f"{query_id} Comet native coverage",
                    int(summary["n_failed"]),
                    attempts[0],
                    attempts[1],
                ),
                encoding="utf-8",
                newline="\n",
            )
            produced.append(coverage_path)
        except ReportChartError as error:
            analysis_issues.append(f"{experiment_id} native coverage chart failed: {error}")

        try:
            profile = build_resource_profiles(rows, repository_root)
            resource_profiles[experiment_id] = profile
            resource_json_path = output_dir / f"{experiment_id}.resource-profile.json"
            write_json(resource_json_path, profile, immutable=False)
            produced.append(resource_json_path)
            resource_svg_path = output_dir / f"{experiment_id}.resource-profile.svg"
            resource_svg_path.write_text(
                resource_profile_svg(profile, f"{query_id} CPU and memory profile"),
                encoding="utf-8",
                newline="\n",
            )
            produced.append(resource_svg_path)
        except (ResourceProfileError, ReportChartError) as error:
            analysis_issues.append(f"{experiment_id} resource profile failed: {error}")

    suite_geometric_mean = (
        math.exp(statistics.fmean(math.log(value) for value in primary_speedups))
        if primary_speedups
        else None
    )
    suite_summary_path = output_dir / "suite-summary.json"
    write_json(
        suite_summary_path,
        {
            "schema_version": 1,
            "estimator": "geometric-mean-of-per-query-median-paired-speedups",
            "n_queries": len(primary_speedups),
            "geometric_mean_speedup": suite_geometric_mean,
        },
        immutable=False,
    )
    produced.append(suite_summary_path)

    csv_path = output_dir / "normalized-measurements.csv"
    _write_csv(csv_path, sorted(all_measurements, key=lambda row: str(row["run_id"])))
    produced.append(csv_path)

    all_successful_measurements = [
        row for row in all_measurements if row.get("status") == "succeeded"
    ]
    plan_insights: dict[str, Any] | None = None
    try:
        plan_insights = build_plan_insights(all_successful_measurements, repository_root)
        plan_path = output_dir / "plan-insights.json"
        write_json(plan_path, plan_insights, immutable=False)
        produced.append(plan_path)
    except PlanInsightsError as error:
        analysis_issues.append(f"plan insights failed: {error}")

    findings: dict[str, Any] | None = None
    try:
        findings = build_research_findings(all_successful_measurements, summaries)
        findings_path = output_dir / "research-findings.json"
        write_json(findings_path, findings, immutable=False)
        produced.append(findings_path)
    except ResearchFindingsError as error:
        analysis_issues.append(f"research findings failed: {error}")

    resource_profiles_path = output_dir / "resource-profiles.json"
    write_json(
        resource_profiles_path,
        {"schema_version": 1, "experiments": resource_profiles},
        immutable=False,
    )
    produced.append(resource_profiles_path)

    matrix_rows = _native_matrix_rows(
        summaries,
        native_summaries,
        plan_insights,
        attempt_metadata,
    )
    matrix_path = output_dir / "native-operator-matrix.csv"
    _write_native_matrix_csv(matrix_path, matrix_rows)
    produced.append(matrix_path)

    report_path = output_dir / "technical-report.md"
    preliminary_report = _report_markdown(
        publishable=evidence_publishable,
        summaries=summaries,
        native_summaries=native_summaries,
        resource_profiles=resource_profiles,
        plan_insights=plan_insights,
        findings=findings,
        matrix_rows=matrix_rows,
        attempt_metadata=attempt_metadata,
        suite_geometric_mean=suite_geometric_mean,
        analysis_issues=analysis_issues,
    )
    report_path.write_text(preliminary_report, encoding="utf-8", newline="\n")
    produced.append(report_path)

    contract = _report_contract(
        output_dir=output_dir,
        expected_ids=_expected_experiment_ids(publication),
        expected_pair_count=_expected_measurement_pairs(publication),
        summaries=summaries,
        native_summaries=native_summaries,
        resource_profiles=resource_profiles,
        plan_insights=plan_insights,
        findings=findings,
        attempt_metadata=attempt_metadata,
        report_text=preliminary_report,
    )
    contract_path = output_dir / "report-contract.json"
    write_json(contract_path, contract, immutable=False)
    produced.append(contract_path)

    publication["evidence_publishable"] = evidence_publishable
    publication["report_contract"] = {
        "path": contract_path.name,
        "status": contract["status"],
        "passed": contract["passed"],
    }
    publication["publishable"] = evidence_publishable and bool(contract["passed"])
    publication["status"] = "passed" if publication["publishable"] else "failed"
    publication_issues = publication.get("issues")
    if not isinstance(publication_issues, list):
        publication_issues = []
        publication["issues"] = publication_issues
    publication_issues.extend(f"report content: {issue}" for issue in contract["issues"])
    publication_issues.extend(f"report analysis: {issue}" for issue in analysis_issues)

    final_report = _report_markdown(
        publishable=bool(publication["publishable"]),
        summaries=summaries,
        native_summaries=native_summaries,
        resource_profiles=resource_profiles,
        plan_insights=plan_insights,
        findings=findings,
        matrix_rows=matrix_rows,
        attempt_metadata=attempt_metadata,
        suite_geometric_mean=suite_geometric_mean,
        analysis_issues=analysis_issues,
    )
    report_path.write_text(final_report, encoding="utf-8", newline="\n")

    publishability_path = output_dir / "report-publishability.json"
    write_json(publishability_path, publication, immutable=False)
    produced.append(publishability_path)

    inventory_path = output_dir / "report-artifact-inventory.json"
    _write_artifact_inventory(inventory_path, produced, output_dir)
    produced.append(inventory_path)
    _prune_stale_experiment_artifacts(output_dir, produced)
    if require_publishable and not publication["publishable"]:
        raise ReportNotPublishableError(
            "report is diagnostic-only; see " + str(publishability_path)
        )
    return tuple(produced)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("raw_root", type=Path)
    parser.add_argument("--output", type=Path, default=Path("results/reports"))
    parser.add_argument("--campaign-root", type=Path, default=ROOT / ".artifacts/campaigns")
    parser.add_argument("--require-publishable", action="store_true")
    args = parser.parse_args()
    try:
        produced = build_report(
            args.raw_root,
            args.output,
            campaign_root=args.campaign_root,
            require_publishable=args.require_publishable,
        )
    except ReportNotPublishableError as error:
        raise SystemExit(str(error)) from error
    for path in produced:
        print(path)


if __name__ == "__main__":
    main()
