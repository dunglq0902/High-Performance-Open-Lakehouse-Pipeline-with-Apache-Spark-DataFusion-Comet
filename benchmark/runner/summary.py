"""Rebuildable schema-v1 summaries and paired speedup estimates."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import asdict
from typing import Any

from benchmark.runner.statistics import (
    DEFAULT_BOOTSTRAP_RESAMPLES,
    DEFAULT_BOOTSTRAP_SEED,
    PairId,
    describe,
    pair_speedups_by_id,
    paired_bootstrap_median_speedup,
    percentile,
)

__all__ = ["describe", "percentile", "summarize_records"]

_ENGINES = ("spark_baseline", "comet_accelerated")
_STATUSES = {"succeeded", "failed", "timeout", "invalid_result", "invalid_environment"}
_RESOURCE_METRICS = {
    "cpu_core_seconds": "seconds",
    "cgroup_memory_peak_mib": "MiB",
    "jvm_gc_time_ms": "ms",
    "shuffle_read_mb": "MiB",
    "shuffle_write_mb": "MiB",
    "disk_spill_mb": "MiB",
}


def summarize_records(records: Iterable[dict[str, Any]]) -> dict[str, Any]:
    rows = [row for row in records if row.get("phase") == "measurement"]
    if not rows:
        raise ValueError("no measurement records to summarize")

    identity_fields = ("experiment_id", "workload", "query_id", "storage_profile")
    identities = {tuple(row.get(field) for field in identity_fields) for row in rows}
    if len(identities) != 1 or any(value is None for value in next(iter(identities))):
        raise ValueError("raw records contain mixed or incomplete experiment identity")

    seen_run_ids: set[str] = set()
    seen_pair_engines: set[tuple[int, str]] = set()
    pair_engines: dict[int, set[str]] = defaultdict(set)
    rows_by_pair: dict[int, dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in rows:
        run_id = str(row.get("run_id", ""))
        if not run_id or run_id in seen_run_ids:
            raise ValueError(f"duplicate or missing run_id: {run_id!r}")
        seen_run_ids.add(run_id)
        pair_index = row.get("pair_index")
        engine = str(row.get("engine", ""))
        if not isinstance(pair_index, int) or isinstance(pair_index, bool):
            raise ValueError(f"measurement run {run_id!r} has no integer pair_index")
        if engine not in _ENGINES:
            raise ValueError(f"measurement run {run_id!r} has unsupported engine: {engine!r}")
        status = row.get("status")
        if status not in _STATUSES:
            raise ValueError(f"measurement run {run_id!r} has unsupported status: {status!r}")
        pair_engine = (pair_index, engine)
        if pair_engine in seen_pair_engines:
            raise ValueError(f"duplicate engine record for pair {pair_index}: {engine}")
        seen_pair_engines.add(pair_engine)
        pair_engines[pair_index].add(engine)
        rows_by_pair[pair_index][engine] = row

    required_engines = set(_ENGINES)
    for pair_id, engines in pair_engines.items():
        if engines != required_engines:
            missing = sorted(required_engines - engines)
            raise ValueError(f"pair {pair_id} is missing engine records: {', '.join(missing)}")

    succeeded = [row for row in rows if row.get("status") == "succeeded"]
    by_engine: dict[str, list[float]] = defaultdict(list)
    latency_by_engine_pair: dict[str, dict[PairId, float | None]] = {
        engine: {} for engine in _ENGINES
    }
    for row in rows:
        engine = str(row["engine"])
        pair_id = int(row["pair_index"])
        latency: float | None = None
        if row.get("status") == "succeeded":
            latency = _successful_latency(row)
            by_engine[engine].append(latency)
        latency_by_engine_pair[engine][pair_id] = latency

    paired_samples = pair_speedups_by_id(
        latency_by_engine_pair["spark_baseline"],
        latency_by_engine_pair["comet_accelerated"],
    )
    paired = paired_bootstrap_median_speedup(
        latency_by_engine_pair["spark_baseline"],
        latency_by_engine_pair["comet_accelerated"],
    )
    engine_summaries = {name: describe(by_engine[name]) for name in _ENGINES}
    spark_median = engine_summaries["spark_baseline"]["median"]
    comet_median = engine_summaries["comet_accelerated"]["median"]
    ratio_of_medians = (
        float(spark_median) / float(comet_median)
        if spark_median is not None and comet_median is not None
        else None
    )
    identity = dict(zip(identity_fields, next(iter(identities)), strict=True))
    return {
        "schema_version": 1,
        **identity,
        "n_total": len(rows),
        "n_succeeded": len(succeeded),
        "n_failed": len(rows) - len(succeeded),
        "engines": engine_summaries,
        "paired_speedup": describe(paired_samples.speedups),
        "ratio_of_medians": ratio_of_medians,
        "paired_resource_savings": _summarize_resource_savings(rows_by_pair),
        "paired_speedup_ci": (
            asdict(paired.confidence_interval) if paired.confidence_interval is not None else None
        ),
        "paired_failures": [asdict(failure) for failure in paired.failures],
        "bootstrap": {
            "estimator": "median-paired-speedup",
            "confidence_level": 0.95,
            "resamples": DEFAULT_BOOTSTRAP_RESAMPLES,
            "seed": DEFAULT_BOOTSTRAP_SEED,
            "method": "percentile",
            "percentile_method": "linear-r7",
        },
        "percentile_method": "linear-r7",
    }


def _successful_latency(row: dict[str, Any]) -> float:
    run_id = str(row.get("run_id", ""))
    metrics = row.get("metrics")
    if not isinstance(metrics, dict):
        raise ValueError(f"succeeded measurement run {run_id!r} has no metrics object")
    value = metrics.get("query_wall_time_ms")
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"succeeded measurement run {run_id!r} has no numeric wall time")
    latency = float(value)
    if not math.isfinite(latency) or latency <= 0:
        raise ValueError(f"succeeded measurement run {run_id!r} has invalid wall time: {value!r}")
    return latency


def _optional_resource_metric(row: dict[str, Any], metric: str) -> float | None:
    metrics = row.get("metrics")
    if not isinstance(metrics, dict):
        raise ValueError(f"run {row.get('run_id')!r} has no metrics object")
    value = metrics.get(metric)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"run {row.get('run_id')!r} has invalid {metric}: {value!r}")
    numeric = float(value)
    if not math.isfinite(numeric) or numeric < 0:
        raise ValueError(f"run {row.get('run_id')!r} has invalid {metric}: {value!r}")
    return numeric


def _summarize_resource_savings(
    rows_by_pair: dict[int, dict[str, dict[str, Any]]],
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for metric, unit in _RESOURCE_METRICS.items():
        absolute_deltas: list[float] = []
        relative_savings: list[float] = []
        excluded_pairs: list[int] = []
        zero_baseline_pairs: list[int] = []
        for pair_id, engine_rows in sorted(rows_by_pair.items()):
            spark = engine_rows["spark_baseline"]
            comet = engine_rows["comet_accelerated"]
            if spark.get("status") != "succeeded" or comet.get("status") != "succeeded":
                excluded_pairs.append(pair_id)
                continue
            spark_value = _optional_resource_metric(spark, metric)
            comet_value = _optional_resource_metric(comet, metric)
            if spark_value is None or comet_value is None:
                excluded_pairs.append(pair_id)
                continue
            delta = spark_value - comet_value
            absolute_deltas.append(delta)
            if spark_value == 0:
                zero_baseline_pairs.append(pair_id)
            else:
                relative_savings.append(delta / spark_value)
        result[metric] = {
            "unit": unit,
            "absolute_delta": describe(absolute_deltas),
            "relative_saving_ratio": describe(relative_savings),
            "excluded_pair_ids": excluded_pairs,
            "zero_baseline_pair_ids": zero_baseline_pairs,
        }
    return result
