"""Build deterministic, time-normalized worker resource profiles.

The public entry point in this module deliberately accepts raw-result mappings rather
than paths to raw-result files.  This keeps admission of raw records separate from
validation of the resource artifacts that successful measurement records reference.
"""

from __future__ import annotations

import bisect
import json
import math
import random
import statistics
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from functools import cache
from itertools import pairwise
from pathlib import Path
from typing import Any, TypeGuard

from benchmark.runner.statistics import (
    DEFAULT_BOOTSTRAP_RESAMPLES,
    DEFAULT_BOOTSTRAP_SEED,
    percentile,
)

_MIB = 1024.0 * 1024.0
_ACCEPTED_STATUSES = frozenset(
    {"succeeded", "failed", "timeout", "invalid_result", "invalid_environment"}
)
_ACCEPTED_ENGINES = frozenset({"spark_baseline", "comet_accelerated"})
_ARTIFACT_FIELDS = frozenset(
    {
        "schema_version",
        "scope",
        "timed_out",
        "window_started",
        "window_completed",
        "aborted",
        "samples",
        "summary",
    }
)
_SAMPLE_FIELDS = frozenset(
    {
        "timestamp_ns",
        "source",
        "status",
        "cpu_usage_ns",
        "memory_current_bytes",
        "memory_peak_bytes",
        "swap_current_bytes",
        "io_read_bytes",
        "io_write_bytes",
        "cpu_limit_cores",
        "process_count",
        "missing_metrics",
        "errors",
    }
)
_REQUIRED_COUNTERS = (
    "cpu_usage_ns",
    "memory_current_bytes",
    "memory_peak_bytes",
    "swap_current_bytes",
    "io_read_bytes",
    "io_write_bytes",
)


class ResourceProfileError(ValueError):
    """A raw record or referenced worker resource artifact is not admissible."""


def build_resource_profiles(
    records: Iterable[Mapping[str, Any]],
    repository_root: Path,
    grid_points: int = 21,
) -> dict[str, Any]:
    """Build aggregate CPU and memory profiles for successful measurements.

    Non-measurement records are outside this analysis and are ignored.  Every
    measurement status is nevertheless counted: successful records contribute a
    validated profile, while every accepted non-success status contributes to the
    explicit ``failed_accepted_records`` count.

    CPU utilization is calculated for each adjacent sample interval and placed at
    the interval's ending timestamp.  Memory is the ending sample's current cgroup
    memory.  Values before the first interval endpoint are held at that first value;
    otherwise normalization uses the full sampling window and interpolation is
    linear.  This convention also gives a well-defined constant profile for the
    minimum valid artifact of two samples.
    """

    if isinstance(grid_points, bool) or not isinstance(grid_points, int) or grid_points < 2:
        raise ResourceProfileError("grid_points must be an integer greater than or equal to 2")
    root = _validated_repository_root(repository_root)
    grid = tuple(100.0 * index / (grid_points - 1) for index in range(grid_points))

    accepted_by_engine: dict[str, int] = defaultdict(int)
    succeeded_by_engine: dict[str, int] = defaultdict(int)
    failed_by_engine: dict[str, int] = defaultdict(int)
    profiles_by_engine: dict[str, list[tuple[list[float], list[float]]]] = defaultdict(list)

    accepted_records = 0
    succeeded_records = 0
    failed_accepted_records = 0
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise ResourceProfileError(f"record {index} must be a mapping")
        phase = record.get("phase")
        if phase != "measurement":
            continue
        engine = record.get("engine")
        if not isinstance(engine, str) or engine not in _ACCEPTED_ENGINES:
            raise ResourceProfileError(f"measurement record {index} has an invalid engine")
        status = record.get("status")
        if not isinstance(status, str) or status not in _ACCEPTED_STATUSES:
            raise ResourceProfileError(f"measurement record {index} has an invalid status")

        accepted_records += 1
        accepted_by_engine[engine] += 1
        if status != "succeeded":
            failed_accepted_records += 1
            failed_by_engine[engine] += 1
            continue

        artifact_path = _resource_artifact_path(record, root, index=index)
        interval_points = _load_interval_points(artifact_path, record_index=index)
        cpu_profile = _interpolate_profile(
            interval_points[0], interval_points[1], grid, label="CPU"
        )
        memory_profile = _interpolate_profile(
            interval_points[0], interval_points[2], grid, label="memory"
        )
        profiles_by_engine[engine].append((cpu_profile, memory_profile))
        succeeded_records += 1
        succeeded_by_engine[engine] += 1

    engines = sorted(accepted_by_engine)
    engine_results: dict[str, dict[str, Any]] = {}
    per_engine_counts: dict[str, dict[str, int]] = {}
    for engine in engines:
        runs = profiles_by_engine[engine]
        per_engine_counts[engine] = {
            "accepted_records": accepted_by_engine[engine],
            "succeeded_records": succeeded_by_engine[engine],
            "failed_accepted_records": failed_by_engine[engine],
        }
        profile_rows: list[dict[str, Any]] = []
        for grid_index, elapsed_percent in enumerate(grid):
            cpu_values = [run[0][grid_index] for run in runs]
            memory_values = [run[1][grid_index] for run in runs]
            profile_rows.append(
                {
                    "elapsed_percent": elapsed_percent,
                    "cpu_percent_of_limit": _aggregate(cpu_values),
                    "memory_current_mib": _aggregate(memory_values),
                }
            )
        engine_results[engine] = {
            "run_count": len(runs),
            "profiles": profile_rows,
        }

    return {
        "schema_version": 1,
        "grid": {
            "points": list(grid),
            "point_count": grid_points,
            "elapsed_unit": "percent",
            "interpolation": "linear-with-endpoint-hold",
            "interval_position": "ending-sample",
        },
        "bootstrap": {
            "estimator": "median",
            "confidence_level": 0.95,
            "resamples": DEFAULT_BOOTSTRAP_RESAMPLES,
            "seed": DEFAULT_BOOTSTRAP_SEED,
            "method": "percentile",
            "percentile_method": "linear-r7",
        },
        "run_counts": {
            "accepted_records": accepted_records,
            "succeeded_records": succeeded_records,
            "failed_accepted_records": failed_accepted_records,
            "by_engine": per_engine_counts,
        },
        "engines": engine_results,
    }


def _validated_repository_root(repository_root: Path) -> Path:
    try:
        root = Path(repository_root).resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise ResourceProfileError(f"repository_root cannot be resolved: {error}") from error
    if not root.is_dir():
        raise ResourceProfileError("repository_root must be an existing directory")
    return root


def _resource_artifact_path(record: Mapping[str, Any], root: Path, *, index: int) -> Path:
    artifacts = record.get("artifacts")
    if not isinstance(artifacts, Mapping):
        raise ResourceProfileError(f"succeeded measurement record {index} has no artifact mapping")
    declared = artifacts.get("resource_samples")
    if not isinstance(declared, str) or not declared.strip():
        raise ResourceProfileError(
            f"succeeded measurement record {index} has no resource_samples artifact"
        )
    relative = Path(declared)
    if relative.is_absolute():
        raise ResourceProfileError(
            f"resource_samples for record {index} must be relative to repository_root"
        )
    try:
        candidate = root / relative
        resolved = candidate.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise ResourceProfileError(
            f"resource_samples for record {index} cannot be resolved: {error}"
        ) from error
    if not resolved.is_relative_to(root):
        raise ResourceProfileError(f"resource_samples for record {index} escapes repository_root")
    if candidate.is_symlink() or not resolved.is_file():
        raise ResourceProfileError(
            f"resource_samples for record {index} must be a regular non-symlink file"
        )
    return resolved


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ResourceProfileError(f"resource artifact contains duplicate key {key!r}")
        value[key] = item
    return value


def _load_interval_points(
    path: Path, *, record_index: int
) -> tuple[list[float], list[float], list[float]]:
    try:
        with path.open("r", encoding="utf-8") as stream:
            value = json.load(stream, object_pairs_hook=_reject_duplicate_keys)
    except ResourceProfileError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ResourceProfileError(
            f"resource_samples for record {record_index} is not valid JSON: {error}"
        ) from error
    label = f"resource_samples for record {record_index}"
    if not isinstance(value, Mapping):
        raise ResourceProfileError(f"{label} root must be an object")
    if set(value) != _ARTIFACT_FIELDS:
        raise ResourceProfileError(f"{label} has an unexpected field set")
    if not _is_integer(value.get("schema_version")) or value.get("schema_version") != 1:
        raise ResourceProfileError(f"{label} schema_version must equal 1")
    if value.get("scope") != "spark-worker-executor-container":
        raise ResourceProfileError(f"{label} scope is invalid")
    expected_flags = {
        "window_started": True,
        "window_completed": True,
        "timed_out": False,
        "aborted": False,
    }
    for field, expected in expected_flags.items():
        if value.get(field) is not expected:
            raise ResourceProfileError(f"{label} {field} must equal {expected!r}")

    raw_samples = value.get("samples")
    if not isinstance(raw_samples, list) or len(raw_samples) < 2:
        raise ResourceProfileError(f"{label} requires at least two samples")
    samples = [
        _validated_sample(sample, label=label, index=index)
        for index, sample in enumerate(raw_samples)
    ]
    timestamps = [sample[0] for sample in samples]
    if any(current <= previous for previous, current in pairwise(timestamps)):
        raise ResourceProfileError(f"{label} timestamps must be strictly increasing")
    _validate_summary(value.get("summary"), sample_count=len(samples), label=label)

    first_timestamp = timestamps[0]
    last_timestamp = timestamps[-1]
    duration = last_timestamp - first_timestamp
    elapsed: list[float] = []
    cpu: list[float] = []
    memory: list[float] = []
    for previous, current in pairwise(samples):
        elapsed_ns = current[0] - previous[0]
        cpu_delta_ns = current[1] - previous[1]
        if cpu_delta_ns < 0:
            raise ResourceProfileError(f"{label} cpu_usage_ns must be nondecreasing")
        cpu_percent = 100.0 * cpu_delta_ns / elapsed_ns / current[3]
        memory_mib = current[2] / _MIB
        if not math.isfinite(cpu_percent) or cpu_percent < 0:
            raise ResourceProfileError(f"{label} derives an invalid CPU interval")
        if not math.isfinite(memory_mib) or memory_mib < 0:
            raise ResourceProfileError(f"{label} derives an invalid memory interval")
        elapsed.append(100.0 * (current[0] - first_timestamp) / duration)
        cpu.append(cpu_percent)
        memory.append(memory_mib)
    return elapsed, cpu, memory


def _validated_sample(value: object, *, label: str, index: int) -> tuple[int, int, int, float]:
    if not isinstance(value, Mapping) or set(value) != _SAMPLE_FIELDS:
        raise ResourceProfileError(f"{label} sample {index} has an unexpected field set")
    timestamp = value.get("timestamp_ns")
    if not _is_integer(timestamp) or timestamp < 0:
        raise ResourceProfileError(f"{label} sample {index} timestamp_ns is invalid")
    if value.get("source") != "cgroup_v2" or value.get("status") != "complete":
        raise ResourceProfileError(f"{label} sample {index} must be a complete cgroup_v2 sample")
    if value.get("missing_metrics") != [] or value.get("errors") != []:
        raise ResourceProfileError(f"{label} sample {index} reports missing metrics or errors")

    counters: dict[str, int] = {}
    for field in _REQUIRED_COUNTERS:
        counter = value.get(field)
        if not _is_integer(counter) or counter < 0:
            raise ResourceProfileError(f"{label} sample {index} {field} is invalid")
        counters[field] = counter
    process_count = value.get("process_count")
    if process_count is not None and (not _is_integer(process_count) or process_count < 0):
        raise ResourceProfileError(f"{label} sample {index} process_count is invalid")
    cpu_limit = value.get("cpu_limit_cores")
    if (
        isinstance(cpu_limit, bool)
        or not isinstance(cpu_limit, int | float)
        or not math.isfinite(cpu_limit)
        or cpu_limit <= 0
    ):
        raise ResourceProfileError(f"{label} sample {index} cpu_limit_cores is invalid")
    return timestamp, counters["cpu_usage_ns"], counters["memory_current_bytes"], float(cpu_limit)


def _validate_summary(value: object, *, sample_count: int, label: str) -> None:
    if not isinstance(value, Mapping):
        raise ResourceProfileError(f"{label} summary must be an object")
    if value.get("status") != "complete" or value.get("source") != "cgroup_v2":
        raise ResourceProfileError(f"{label} summary must describe complete cgroup_v2 sampling")
    observed_sample_count = value.get("sample_count")
    if not _is_integer(observed_sample_count) or observed_sample_count != sample_count:
        raise ResourceProfileError(f"{label} summary sample_count is invalid")
    if value.get("missing_metrics") != [] or value.get("issues") != []:
        raise ResourceProfileError(f"{label} summary reports missing metrics or issues")


def _interpolate_profile(
    points: Sequence[float],
    values: Sequence[float],
    grid: Sequence[float],
    *,
    label: str,
) -> list[float]:
    if not points or len(points) != len(values):
        raise ResourceProfileError(f"{label} profile points are invalid")
    result: list[float] = []
    for target in grid:
        position = bisect.bisect_left(points, target)
        if position == 0:
            result.append(float(values[0]))
        elif position == len(points):
            result.append(float(values[-1]))
        elif points[position] == target:
            result.append(float(values[position]))
        else:
            left = position - 1
            width = points[position] - points[left]
            weight = (target - points[left]) / width
            result.append(float(values[left] * (1.0 - weight) + values[position] * weight))
    return result


def _aggregate(values: Sequence[float]) -> dict[str, Any]:
    if not values:
        return {
            "n": 0,
            "median": None,
            "q1": None,
            "q3": None,
            "iqr": None,
            "min": None,
            "max": None,
            "median_ci_95": None,
        }
    ordered = sorted(float(value) for value in values)
    q1 = percentile(ordered, 0.25)
    q3 = percentile(ordered, 0.75)
    ci_lower, ci_upper = _bootstrap_median_interval(ordered)
    return {
        "n": len(ordered),
        "median": float(statistics.median(ordered)),
        "q1": q1,
        "q3": q3,
        "iqr": q3 - q1,
        "min": ordered[0],
        "max": ordered[-1],
        "median_ci_95": {
            "lower": ci_lower,
            "upper": ci_upper,
        },
    }


@cache
def _bootstrap_rank_pair_counts(
    sample_size: int,
    resamples: int = DEFAULT_BOOTSTRAP_RESAMPLES,
    seed: int = DEFAULT_BOOTSTRAP_SEED,
) -> tuple[tuple[int, int, int], ...]:
    """Cache the fixed bootstrap's median order-statistic rank pairs.

    All grid-point samples with the same n use the same seed and therefore the
    same resampled index sequence. Collapsing 10,000 draws to at most n(n+1)/2
    weighted rank pairs preserves the exact percentile result while avoiding
    repeated random sampling for every time-series point.
    """

    if sample_size < 1:
        raise ResourceProfileError("bootstrap sample_size must be positive")
    rng = random.Random(seed)
    lower_middle = (sample_size - 1) // 2
    upper_middle = sample_size // 2
    counts: dict[tuple[int, int], int] = defaultdict(int)
    for _ in range(resamples):
        ranks = sorted(rng.randrange(sample_size) for _ in range(sample_size))
        counts[(ranks[lower_middle], ranks[upper_middle])] += 1
    return tuple((lower, upper, count) for (lower, upper), count in sorted(counts.items()))


def _bootstrap_median_interval(ordered: Sequence[float]) -> tuple[float, float]:
    weighted = sorted(
        (
            ((float(ordered[lower]) + float(ordered[upper])) / 2.0, count)
            for lower, upper, count in _bootstrap_rank_pair_counts(len(ordered))
        ),
        key=lambda item: item[0],
    )

    def order_statistic(index: int) -> float:
        cumulative = 0
        for estimate, count in weighted:
            cumulative += count
            if index < cumulative:
                return estimate
        raise AssertionError("bootstrap rank counts do not cover every resample")

    def weighted_percentile(probability: float) -> float:
        position = (DEFAULT_BOOTSTRAP_RESAMPLES - 1) * probability
        lower = math.floor(position)
        upper = math.ceil(position)
        lower_value = order_statistic(lower)
        if lower == upper:
            return lower_value
        weight = position - lower
        return lower_value * (1.0 - weight) + order_statistic(upper) * weight

    return weighted_percentile(0.025), weighted_percentile(0.975)


def _is_integer(value: object) -> TypeGuard[int]:
    return isinstance(value, int) and not isinstance(value, bool)
