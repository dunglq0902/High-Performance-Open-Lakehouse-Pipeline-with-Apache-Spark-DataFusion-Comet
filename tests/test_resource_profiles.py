from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from analysis.resource_profiles import (
    ResourceProfileError,
    _bootstrap_median_interval,
    build_resource_profiles,
)
from benchmark.runner.statistics import bootstrap_percentile_interval

MIB = 1024 * 1024


@pytest.mark.parametrize(
    "values",
    ([1.0], [1.0, 9.0], [1.0, 2.0, 8.0], [1.0, 1.0, 2.0, 3.0, 5.0, 8.0, 13.0, 21.0, 34.0, 55.0]),
)
def test_cached_rank_bootstrap_matches_canonical_fixed_seed(values: list[float]) -> None:
    expected = bootstrap_percentile_interval(values)

    assert _bootstrap_median_interval(values) == (expected.lower, expected.upper)


def _sample(
    timestamp_ns: int,
    cpu_usage_ns: int,
    memory_current_bytes: int,
    *,
    cpu_limit_cores: float = 2.0,
) -> dict[str, Any]:
    return {
        "timestamp_ns": timestamp_ns,
        "source": "cgroup_v2",
        "status": "complete",
        "cpu_usage_ns": cpu_usage_ns,
        "memory_current_bytes": memory_current_bytes,
        "memory_peak_bytes": memory_current_bytes,
        "swap_current_bytes": 0,
        "io_read_bytes": 0,
        "io_write_bytes": 0,
        "cpu_limit_cores": cpu_limit_cores,
        "process_count": None,
        "missing_metrics": [],
        "errors": [],
    }


def _artifact(samples: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "scope": "spark-worker-executor-container",
        "timed_out": False,
        "window_started": True,
        "window_completed": True,
        "aborted": False,
        "samples": samples,
        "summary": {
            "status": "complete",
            "source": "cgroup_v2",
            "sample_count": len(samples),
            "missing_metrics": [],
            "issues": [],
        },
    }


def _write_artifact(root: Path, name: str, artifact: dict[str, Any]) -> str:
    path = root / "artifacts" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(artifact), encoding="utf-8")
    return path.relative_to(root).as_posix()


def _record(
    artifact_path: str | None,
    *,
    engine: str = "spark_baseline",
    status: str = "succeeded",
    phase: str = "measurement",
) -> dict[str, Any]:
    return {
        "phase": phase,
        "engine": engine,
        "status": status,
        "artifacts": {"resource_samples": artifact_path},
    }


def test_builds_normalized_profiles_and_deterministic_aggregate(tmp_path: Path) -> None:
    first = _write_artifact(
        tmp_path,
        "first.json",
        _artifact(
            [
                _sample(0, 0, 10 * MIB),
                _sample(1_000_000_000, 1_000_000_000, 20 * MIB),
                _sample(2_000_000_000, 3_000_000_000, 30 * MIB),
            ]
        ),
    )
    second = _write_artifact(
        tmp_path,
        "second.json",
        _artifact(
            [
                _sample(0, 0, 30 * MIB),
                _sample(1_000_000_000, 2_000_000_000, 40 * MIB),
                _sample(2_000_000_000, 3_000_000_000, 50 * MIB),
            ]
        ),
    )
    records = [
        _record(first),
        _record(second),
        _record(None, engine="comet_accelerated", status="timeout"),
        _record(None, phase="correctness"),
    ]

    result = build_resource_profiles(records, tmp_path, grid_points=3)
    repeated = build_resource_profiles(list(reversed(records)), tmp_path, grid_points=3)

    assert result == repeated
    assert result["schema_version"] == 1
    assert result["grid"]["points"] == [0.0, 50.0, 100.0]
    assert result["bootstrap"] == {
        "estimator": "median",
        "confidence_level": 0.95,
        "resamples": 10_000,
        "seed": 20260824,
        "method": "percentile",
        "percentile_method": "linear-r7",
    }
    assert result["run_counts"] == {
        "accepted_records": 3,
        "succeeded_records": 2,
        "failed_accepted_records": 1,
        "by_engine": {
            "comet_accelerated": {
                "accepted_records": 1,
                "succeeded_records": 0,
                "failed_accepted_records": 1,
            },
            "spark_baseline": {
                "accepted_records": 2,
                "succeeded_records": 2,
                "failed_accepted_records": 0,
            },
        },
    }

    spark = result["engines"]["spark_baseline"]
    assert spark["run_count"] == 2
    at_start, at_half, at_end = spark["profiles"]
    assert at_start["cpu_percent_of_limit"] == {
        "n": 2,
        "median": 75.0,
        "q1": 62.5,
        "q3": 87.5,
        "iqr": 25.0,
        "min": 50.0,
        "max": 100.0,
        "median_ci_95": {"lower": 50.0, "upper": 100.0},
    }
    assert at_start["memory_current_mib"]["median"] == 30.0
    assert at_half == at_start | {"elapsed_percent": 50.0}
    assert at_end["cpu_percent_of_limit"]["median"] == 75.0
    assert at_end["memory_current_mib"]["median"] == 40.0

    comet = result["engines"]["comet_accelerated"]
    assert comet["run_count"] == 0
    assert all(point["cpu_percent_of_limit"]["n"] == 0 for point in comet["profiles"])
    assert all(point["cpu_percent_of_limit"]["median_ci_95"] is None for point in comet["profiles"])


def test_two_samples_produce_a_constant_profile(tmp_path: Path) -> None:
    artifact = _write_artifact(
        tmp_path,
        "minimum.json",
        _artifact(
            [
                _sample(10, 100, 2 * MIB, cpu_limit_cores=0.5),
                _sample(1_000_000_010, 500_000_100, 7 * MIB, cpu_limit_cores=0.5),
            ]
        ),
    )

    result = build_resource_profiles([_record(artifact)], tmp_path, grid_points=5)

    points = result["engines"]["spark_baseline"]["profiles"]
    assert [point["cpu_percent_of_limit"]["median"] for point in points] == [100.0] * 5
    assert [point["memory_current_mib"]["median"] for point in points] == [7.0] * 5


def test_rejects_absolute_missing_and_escaping_artifact_paths(tmp_path: Path) -> None:
    outside = tmp_path.parent / "outside-resource-profile.json"
    outside.write_text(
        json.dumps(_artifact([_sample(0, 0, 0), _sample(1, 1, 1)])), encoding="utf-8"
    )

    with pytest.raises(ResourceProfileError, match="must be relative"):
        build_resource_profiles([_record(str(outside.resolve()))], tmp_path)
    with pytest.raises(ResourceProfileError, match="escapes repository_root"):
        build_resource_profiles([_record(f"../{outside.name}")], tmp_path)
    with pytest.raises(ResourceProfileError, match="cannot be resolved"):
        build_resource_profiles([_record("missing.json")], tmp_path)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (("schema_version", 2), "schema_version"),
        (("window_completed", False), "window_completed"),
        (("timed_out", True), "timed_out"),
        (("aborted", True), "aborted"),
    ],
)
def test_rejects_incomplete_worker_artifacts(
    tmp_path: Path, mutation: tuple[str, object], message: str
) -> None:
    artifact = _artifact([_sample(0, 0, 0), _sample(1, 1, 1)])
    artifact[mutation[0]] = mutation[1]
    relative = _write_artifact(tmp_path, "invalid.json", artifact)

    with pytest.raises(ResourceProfileError, match=message):
        build_resource_profiles([_record(relative)], tmp_path)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        ("source", "complete cgroup_v2"),
        ("status", "complete cgroup_v2"),
        ("missing", "missing metrics or errors"),
        ("timestamp", "strictly increasing"),
        ("counter", "memory_current_bytes is invalid"),
        ("cpu_limit", "cpu_limit_cores is invalid"),
        ("cpu_regression", "cpu_usage_ns must be nondecreasing"),
    ],
)
def test_rejects_malformed_or_incomplete_samples(tmp_path: Path, mutate: str, message: str) -> None:
    samples = [_sample(10, 10, 10), _sample(20, 20, 20)]
    if mutate == "source":
        samples[1]["source"] = "process_tree"
    elif mutate == "status":
        samples[1]["status"] = "partial"
    elif mutate == "missing":
        samples[1]["missing_metrics"] = ["memory_current_bytes"]
    elif mutate == "timestamp":
        samples[1]["timestamp_ns"] = 10
    elif mutate == "counter":
        samples[1]["memory_current_bytes"] = -1
    elif mutate == "cpu_limit":
        samples[1]["cpu_limit_cores"] = 0
    else:
        samples[1]["cpu_usage_ns"] = 9
    relative = _write_artifact(tmp_path, "invalid-sample.json", _artifact(samples))

    with pytest.raises(ResourceProfileError, match=message):
        build_resource_profiles([_record(relative)], tmp_path)


def test_rejects_invalid_record_status_engine_and_grid(tmp_path: Path) -> None:
    with pytest.raises(ResourceProfileError, match="invalid status"):
        build_resource_profiles([_record(None, status="unknown")], tmp_path)
    with pytest.raises(ResourceProfileError, match="invalid engine"):
        build_resource_profiles([_record(None, engine="unknown")], tmp_path)
    with pytest.raises(ResourceProfileError, match="grid_points"):
        build_resource_profiles([], tmp_path, grid_points=1)
    with pytest.raises(ResourceProfileError, match="grid_points"):
        build_resource_profiles([], tmp_path, grid_points=True)


def test_summary_must_be_complete_and_match_sample_count(tmp_path: Path) -> None:
    artifact = _artifact([_sample(0, 0, 0), _sample(1, 1, 1)])
    broken = copy.deepcopy(artifact)
    broken["summary"]["sample_count"] = 3
    relative = _write_artifact(tmp_path, "bad-summary.json", broken)

    with pytest.raises(ResourceProfileError, match="summary sample_count"):
        build_resource_profiles([_record(relative)], tmp_path)
