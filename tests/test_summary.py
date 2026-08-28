import json
from copy import deepcopy
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from benchmark.runner.summary import summarize_records


def records(count: int) -> list[dict]:
    result = []
    for pair in range(1, count + 1):
        result.extend(
            [
                {
                    "experiment_id": "EXP-TEST",
                    "run_id": f"spark-{pair}",
                    "pair_index": pair,
                    "phase": "measurement",
                    "status": "succeeded",
                    "engine": "spark_baseline",
                    "workload": "micro",
                    "query_id": "M02",
                    "storage_profile": "ecommerce_iceberg_rest",
                    "metrics": {
                        "query_wall_time_ms": 200 + pair,
                        "cpu_core_seconds": 10.0,
                        "cgroup_memory_peak_mib": 100.0,
                        "jvm_gc_time_ms": 0.0,
                        "shuffle_read_mb": 50.0,
                        "shuffle_write_mb": 40.0,
                        "disk_spill_mb": 0.0,
                    },
                },
                {
                    "experiment_id": "EXP-TEST",
                    "run_id": f"comet-{pair}",
                    "pair_index": pair,
                    "phase": "measurement",
                    "status": "succeeded",
                    "engine": "comet_accelerated",
                    "workload": "micro",
                    "query_id": "M02",
                    "storage_profile": "ecommerce_iceberg_rest",
                    "metrics": {
                        "query_wall_time_ms": 100 + pair / 2,
                        "cpu_core_seconds": 8.0,
                        "cgroup_memory_peak_mib": 80.0,
                        "jvm_gc_time_ms": 0.0,
                        "shuffle_read_mb": 30.0,
                        "shuffle_write_mb": 20.0,
                        "disk_spill_mb": 0.0,
                    },
                },
            ]
        )
    return result


def test_summary_uses_paired_speedup_and_suppresses_small_sample_p95() -> None:
    summary = summarize_records(records(7))
    assert summary["n_total"] == 14
    assert summary["paired_speedup"]["n"] == 7
    assert summary["paired_speedup"]["median"] == 2.0
    assert summary["ratio_of_medians"] == 2.0
    cpu = summary["paired_resource_savings"]["cpu_core_seconds"]
    assert cpu["absolute_delta"]["median"] == 2.0
    assert cpu["relative_saving_ratio"]["median"] == pytest.approx(0.2)
    gc = summary["paired_resource_savings"]["jvm_gc_time_ms"]
    assert gc["absolute_delta"]["median"] == 0.0
    assert gc["relative_saving_ratio"]["median"] is None
    assert gc["zero_baseline_pair_ids"] == list(range(1, 8))
    assert summary["paired_speedup_ci"] == {
        "lower": 2.0,
        "upper": 2.0,
        "confidence_level": 0.95,
        "resamples": 10_000,
        "seed": 20260824,
        "method": "percentile",
        "percentile_method": "linear-r7",
    }
    assert summary["paired_failures"] == []
    assert summary["engines"]["spark_baseline"]["p95"] is None


def test_p95_is_emitted_at_twenty_successful_runs_per_engine() -> None:
    summary = summarize_records(records(20))
    assert summary["engines"]["spark_baseline"]["p95"] is not None
    assert summary["paired_speedup"]["p95"] is not None


def test_summary_rejects_mixed_identity_and_duplicate_pairs() -> None:
    mixed = records(1)
    mixed[1]["experiment_id"] = "EXP-OTHER"
    with pytest.raises(ValueError, match="mixed"):
        summarize_records(mixed)

    duplicated = records(1)
    duplicate = dict(duplicated[0])
    duplicate["run_id"] = "spark-retry"
    with pytest.raises(ValueError, match="duplicate engine"):
        summarize_records([*duplicated, duplicate])


def test_summary_ignores_non_measurement_records() -> None:
    source = records(1)
    source.append(
        {
            **source[0],
            "run_id": "correctness-spark",
            "phase": "correctness",
            "pair_index": None,
            "metrics": {"query_wall_time_ms": 1},
        }
    )
    assert summarize_records(source)["n_total"] == 2


def test_summary_counts_failed_records_and_excludes_incomplete_speedup_pairs() -> None:
    source = records(2)
    failed = source[3]
    failed["status"] = "timeout"
    failed["metrics"] = {"query_wall_time_ms": None}

    summary = summarize_records(source)

    assert summary["n_total"] == 4
    assert summary["n_succeeded"] == 3
    assert summary["n_failed"] == 1
    assert summary["engines"]["spark_baseline"]["n"] == 2
    assert summary["engines"]["comet_accelerated"]["n"] == 1
    assert summary["paired_speedup"]["n"] == 1
    assert summary["paired_resource_savings"]["cpu_core_seconds"]["excluded_pair_ids"] == [2]


def test_summary_rejects_missing_pair_member_and_invalid_success_latency() -> None:
    with pytest.raises(ValueError, match="missing engine records"):
        summarize_records(records(2)[:-1])

    invalid = records(1)
    invalid[0]["metrics"]["query_wall_time_ms"] = float("nan")
    with pytest.raises(ValueError, match="invalid wall time"):
        summarize_records(invalid)


def test_summary_remains_compatible_with_schema_v1() -> None:
    schema_path = Path(__file__).parents[1] / "benchmark" / "schemas" / "summary.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    errors = list(Draft202012Validator(schema).iter_errors(summarize_records(records(20))))
    assert errors == []


def test_summary_rejects_unknown_status_and_engine() -> None:
    unknown_status = records(1)
    unknown_status[0]["status"] = "mystery"
    with pytest.raises(ValueError, match="unsupported status"):
        summarize_records(unknown_status)

    unknown_engine = deepcopy(records(1))
    unknown_engine[0]["engine"] = "other"
    with pytest.raises(ValueError, match="unsupported engine"):
        summarize_records(unknown_engine)
