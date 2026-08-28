from __future__ import annotations

from pathlib import Path
from typing import NamedTuple

import pytest

from benchmark.collectors.resources import CollectorStatus, ResourceSample
from benchmark.runner.capacity import (
    GIBIBYTE,
    MEBIBYTE,
    CapacitySnapshot,
    CgroupSnapshot,
    FilesystemSnapshot,
    GateCheckStatus,
    capture_filesystem_snapshot,
    evaluate_capacity_gate,
)


def valid_config() -> dict[str, object]:
    return {
        "experiment": {
            "id": "EXP-CAPACITY",
            "measurement_runs": 10,
            "labels": ["primary", "laptop"],
        },
        "workload": {"suite": "micro"},
        "spark": {
            "runtime_profile": "benchmark-laptop",
            "common_conf": {
                "spark.master": "spark://spark-master:7077",
                "spark.executor.instances": 1,
                "spark.executor.cores": 2,
                "spark.executor.memory": "2g",
                "spark.executor.memoryOverhead": "1g",
                "spark.driver.memory": "1g",
                "spark.memory.offHeap.enabled": True,
                "spark.memory.offHeap.size": "1g",
                "spark.sql.shuffle.partitions": 16,
            },
        },
        "capacity": {
            "estimated_largest_shuffle_bytes": 100 * MEBIBYTE,
            "safety_margin_ratio": 0.2,
        },
    }


def _file(name: str, size_bytes: int) -> dict[str, object]:
    return {"path": name, "size_bytes": size_bytes}


def _table(name: str, sizes: list[int]) -> dict[str, object]:
    return {
        "file_count": len(sizes),
        "total_bytes": sum(sizes),
        "files": [
            _file(f"{name}/part-{index:05d}.parquet", size) for index, size in enumerate(sizes)
        ],
    }


def valid_manifest() -> dict[str, object]:
    return {
        "benchmark_eligible": True,
        "scale_profile": "small",
        "generator": {
            "name": "data.generator",
            "version": "1.0.0",
            "git_commit": "a" * 40,
            "worktree_dirty": False,
        },
        "profile": {
            "profile_id": "small",
            "benchmark_eligible": True,
        },
        "tables": {
            "customers": _table("customers", [MEBIBYTE]),
            "orders": _table("orders", [9 * MEBIBYTE]),
            "order_items": _table("order_items", [9 * MEBIBYTE]),
            "events": _table("events", [9 * MEBIBYTE]),
        },
    }


def valid_snapshot(*, free_bytes: int = 10 * GIBIBYTE) -> CapacitySnapshot:
    return CapacitySnapshot(
        filesystem=FilesystemSnapshot(
            path="/opt/lakehouse/.runtime",
            free_bytes=free_bytes,
            total_bytes=20 * GIBIBYTE,
        ),
        cgroup=CgroupSnapshot(
            memory_limit_bytes=5 * GIBIBYTE,
            cpu_limit_cores=2.0,
            swap_current_bytes=0,
            swap_delta_bytes=0,
            swap_peak_bytes=0,
            sample_status=CollectorStatus.COMPLETE,
        ),
    )


def test_valid_research_environment_passes_all_capacity_checks() -> None:
    result = evaluate_capacity_gate(valid_config(), valid_manifest(), valid_snapshot())

    assert result.passed is True
    assert len(result.diagnostics) == 10
    assert all(item.status is GateCheckStatus.PASS for item in result.diagnostics)
    assert result.failures == ()
    assert result.source_data_bytes == 28 * MEBIBYTE
    base = 28 * MEBIBYTE + 2 * 100 * MEBIBYTE
    assert result.required_free_disk_bytes == base + (base + 4) // 5
    assert result.fact_file_count == 3
    assert result.small_fact_file_count == 0
    assert result.small_fact_file_ratio == 0.0


def test_missing_inputs_fail_closed_with_structured_diagnostics() -> None:
    result = evaluate_capacity_gate(
        {},
        {},
        CapacitySnapshot(
            filesystem=FilesystemSnapshot(path=None, free_bytes=None),
            cgroup=CgroupSnapshot(
                memory_limit_bytes=None,
                cpu_limit_cores=None,
                swap_current_bytes=None,
                swap_delta_bytes=None,
            ),
        ),
    )

    assert result.passed is False
    assert len(result.failures) == len(result.diagnostics) == 10
    assert result.required_free_disk_bytes is None
    assert result.observed_free_disk_bytes is None
    assert result.fact_file_count is None
    assert all((item.message and item.details) or item.code for item in result.failures)


def test_dataset_must_be_eligible_and_generator_provenance_clean() -> None:
    manifest = valid_manifest()
    manifest["benchmark_eligible"] = False
    generator = manifest["generator"]
    assert isinstance(generator, dict)
    generator["git_commit"] = "unknown"
    generator["worktree_dirty"] = True

    result = evaluate_capacity_gate(valid_config(), manifest, valid_snapshot())

    eligibility = result.diagnostic("dataset.benchmark_eligible")
    provenance = result.diagnostic("dataset.generator_provenance")
    assert eligibility.status is GateCheckStatus.FAIL
    assert provenance.status is GateCheckStatus.FAIL
    assert any("full lowercase Git object ID" in detail for detail in provenance.details)
    assert any("worktree_dirty" in detail for detail in provenance.details)


def test_runtime_and_every_fixed_envelope_value_are_verified() -> None:
    config = valid_config()
    spark = config["spark"]
    assert isinstance(spark, dict)
    spark["runtime_profile"] = "smoke-standalone"
    common = spark["common_conf"]
    assert isinstance(common, dict)
    common["spark.executor.cores"] = 4
    common["spark.memory.offHeap.enabled"] = False

    result = evaluate_capacity_gate(config, valid_manifest(), valid_snapshot())

    assert result.diagnostic("experiment.runtime_profile").status is GateCheckStatus.FAIL
    envelope = result.diagnostic("experiment.fixed_envelope")
    assert envelope.status is GateCheckStatus.FAIL
    assert any("spark.executor.cores" in detail for detail in envelope.details)
    assert any("spark.memory.offHeap.enabled" in detail for detail in envelope.details)


def test_ecommerce_manifest_scale_and_profile_must_both_be_small() -> None:
    manifest = valid_manifest()
    manifest["scale_profile"] = "fixture"
    profile = manifest["profile"]
    assert isinstance(profile, dict)
    profile["profile_id"] = "fixture"

    diagnostic = evaluate_capacity_gate(valid_config(), manifest, valid_snapshot()).diagnostic(
        "dataset.scale_profile"
    )

    assert diagnostic.status is GateCheckStatus.FAIL
    assert len(diagnostic.details) == 2


def test_tpch_sf10_requires_matching_manifest_and_exploratory_label() -> None:
    config = valid_config()
    workload = config["workload"]
    assert isinstance(workload, dict)
    workload.update({"suite": "tpch", "scale_factor": 10})
    manifest = valid_manifest()
    manifest.update({"scale_factor": 10, "scale_profile": "sf10"})

    missing_label = evaluate_capacity_gate(config, manifest, valid_snapshot()).diagnostic(
        "dataset.scale_profile"
    )
    assert missing_label.status is GateCheckStatus.FAIL
    assert "exploratory" in missing_label.details[0]

    experiment = config["experiment"]
    assert isinstance(experiment, dict)
    experiment["labels"] = ["exploratory", "laptop"]
    matching = evaluate_capacity_gate(config, manifest, valid_snapshot()).diagnostic(
        "dataset.scale_profile"
    )
    assert matching.status is GateCheckStatus.PASS

    manifest["scale_factor"] = 1
    mismatch = evaluate_capacity_gate(config, manifest, valid_snapshot()).diagnostic(
        "dataset.scale_profile"
    )
    assert mismatch.status is GateCheckStatus.FAIL


def test_disk_reserve_uses_source_two_shuffles_and_explicit_margin() -> None:
    passing = evaluate_capacity_gate(valid_config(), valid_manifest(), valid_snapshot())
    assert passing.required_free_disk_bytes is not None
    short = valid_snapshot(free_bytes=passing.required_free_disk_bytes - 1)

    result = evaluate_capacity_gate(valid_config(), valid_manifest(), short)

    diagnostic = result.diagnostic("filesystem.capacity")
    assert diagnostic.status is GateCheckStatus.FAIL
    assert diagnostic.details == ("free disk shortfall is 1 bytes",)


def test_missing_capacity_estimate_never_becomes_zero() -> None:
    config = valid_config()
    del config["capacity"]

    result = evaluate_capacity_gate(config, valid_manifest(), valid_snapshot())

    assert result.required_free_disk_bytes is None
    disk = result.diagnostic("filesystem.capacity")
    assert disk.status is GateCheckStatus.FAIL
    assert "config.capacity is missing" in disk.details[0]


@pytest.mark.parametrize("memory_limit", [None, 4 * GIBIBYTE])
def test_cgroup_memory_must_contain_fixed_five_gib_envelope(
    memory_limit: int | None,
) -> None:
    snapshot = valid_snapshot()
    snapshot = CapacitySnapshot(
        filesystem=snapshot.filesystem,
        cgroup=CgroupSnapshot(
            memory_limit_bytes=memory_limit,
            cpu_limit_cores=2.0,
            swap_current_bytes=0,
            swap_delta_bytes=0,
        ),
    )

    diagnostic = evaluate_capacity_gate(valid_config(), valid_manifest(), snapshot).diagnostic(
        "cgroup.memory_limit"
    )

    assert diagnostic.status is GateCheckStatus.FAIL
    assert diagnostic.required == 5 * GIBIBYTE


@pytest.mark.parametrize(
    ("current", "delta", "peak"),
    [(1, 0, 1), (0, 1, 1), (0, 0, 1), (None, 0, None), (0, None, None)],
)
def test_any_observed_or_unknown_swap_fails_policy(
    current: int | None, delta: int | None, peak: int | None
) -> None:
    base = valid_snapshot()
    snapshot = CapacitySnapshot(
        filesystem=base.filesystem,
        cgroup=CgroupSnapshot(
            memory_limit_bytes=5 * GIBIBYTE,
            cpu_limit_cores=2.0,
            swap_current_bytes=current,
            swap_delta_bytes=delta,
            swap_peak_bytes=peak,
        ),
    )

    diagnostic = evaluate_capacity_gate(valid_config(), valid_manifest(), snapshot).diagnostic(
        "cgroup.swap_policy"
    )

    assert diagnostic.status is GateCheckStatus.FAIL
    assert diagnostic.details


@pytest.mark.parametrize("cpu_cores", [None, 1.0, 4.0, float("inf")])
def test_cpu_cgroup_quota_must_be_known_and_exactly_two(cpu_cores: float | None) -> None:
    base = valid_snapshot()
    snapshot = CapacitySnapshot(
        filesystem=base.filesystem,
        cgroup=CgroupSnapshot(
            memory_limit_bytes=5 * GIBIBYTE,
            cpu_limit_cores=cpu_cores,
            swap_current_bytes=0,
            swap_delta_bytes=0,
        ),
    )

    diagnostic = evaluate_capacity_gate(valid_config(), valid_manifest(), snapshot).diagnostic(
        "cgroup.cpu_allocation"
    )

    assert diagnostic.status is GateCheckStatus.FAIL


def _manifest_with_order_file_sizes(sizes: list[int]) -> dict[str, object]:
    manifest = valid_manifest()
    tables = manifest["tables"]
    assert isinstance(tables, dict)
    tables["orders"] = _table("orders", sizes)
    return manifest


def test_more_than_ten_percent_small_fact_files_fail_but_dimensions_are_exempt() -> None:
    large = 9 * MEBIBYTE
    small = MEBIBYTE
    manifest = _manifest_with_order_file_sizes([small, small, *([large] * 8)])
    tables = manifest["tables"]
    assert isinstance(tables, dict)
    tables["customers"] = _table("customers", [small] * 20)

    result = evaluate_capacity_gate(valid_config(), manifest, valid_snapshot())

    diagnostic = result.diagnostic("dataset.fact_file_distribution")
    assert diagnostic.status is GateCheckStatus.FAIL
    assert result.fact_file_count == 12
    assert result.small_fact_file_count == 2
    assert result.small_fact_file_ratio == pytest.approx(1 / 6)


def test_exactly_ten_percent_small_files_pass_and_diagnostic_label_can_exempt() -> None:
    large = 9 * MEBIBYTE
    small = MEBIBYTE
    exactly_ten = _manifest_with_order_file_sizes([small, *([large] * 9)])
    tables = exactly_ten["tables"]
    assert isinstance(tables, dict)
    tables["order_items"] = _table("order_items", [large] * 9)
    tables["events"] = _table("events", [large] * 10)
    exact_result = evaluate_capacity_gate(valid_config(), exactly_ten, valid_snapshot())
    assert exact_result.small_fact_file_ratio == pytest.approx(1 / 29)
    assert exact_result.diagnostic("dataset.fact_file_distribution").passed

    fragmented = _manifest_with_order_file_sizes([small] * 2 + [large] * 8)
    config = valid_config()
    experiment = config["experiment"]
    assert isinstance(experiment, dict)
    experiment["labels"] = ["small-file-diagnostic"]
    exempt = evaluate_capacity_gate(config, fragmented, valid_snapshot())
    assert exempt.diagnostic("dataset.fact_file_distribution").passed


def test_fact_file_accounting_mismatch_fails_instead_of_using_partial_sizes() -> None:
    manifest = valid_manifest()
    tables = manifest["tables"]
    assert isinstance(tables, dict)
    orders = tables["orders"]
    assert isinstance(orders, dict)
    orders["file_count"] = 2
    orders["total_bytes"] = 1

    result = evaluate_capacity_gate(valid_config(), manifest, valid_snapshot())

    assert result.fact_file_count is None
    diagnostic = result.diagnostic("dataset.fact_file_distribution")
    assert diagnostic.status is GateCheckStatus.FAIL
    assert len(diagnostic.details) == 2


def _resource_sample(
    timestamp: int, *, cpu_limit: float, swap: int, status: CollectorStatus
) -> ResourceSample:
    return ResourceSample(
        timestamp_ns=timestamp,
        source="cgroup_v2",
        status=status,
        cpu_limit_cores=cpu_limit,
        swap_current_bytes=swap,
    )


def test_cgroup_snapshot_is_derived_from_injected_resource_samples() -> None:
    samples = [
        _resource_sample(3, cpu_limit=2.0, swap=0, status=CollectorStatus.PARTIAL),
        _resource_sample(1, cpu_limit=2.0, swap=0, status=CollectorStatus.PARTIAL),
        _resource_sample(2, cpu_limit=2.0, swap=4, status=CollectorStatus.PARTIAL),
    ]

    snapshot = CgroupSnapshot.from_resource_samples(
        samples,
        memory_limit_bytes=5 * GIBIBYTE,
    )

    assert snapshot.cpu_limit_cores == 2.0
    assert snapshot.swap_current_bytes == 0
    assert snapshot.swap_delta_bytes == 0
    assert snapshot.swap_peak_bytes == 4
    assert snapshot.sample_status is CollectorStatus.PARTIAL
    gate = evaluate_capacity_gate(
        valid_config(),
        valid_manifest(),
        CapacitySnapshot(filesystem=valid_snapshot().filesystem, cgroup=snapshot),
    )
    assert gate.diagnostic("cgroup.swap_policy").status is GateCheckStatus.FAIL


class Usage(NamedTuple):
    total: int
    used: int
    free: int


def test_filesystem_capture_is_injectable_and_preserves_unavailable_state() -> None:
    seen: list[Path] = []

    def read_usage(path: Path) -> Usage:
        seen.append(path)
        return Usage(total=100, used=60, free=40)

    snapshot = capture_filesystem_snapshot(Path("workspace"), disk_usage=read_usage)
    assert seen == [Path("workspace")]
    assert snapshot == FilesystemSnapshot(path="workspace", free_bytes=40, total_bytes=100)

    def fail_usage(path: Path) -> Usage:
        raise OSError(path)

    unavailable = capture_filesystem_snapshot(Path("missing"), disk_usage=fail_usage)
    assert unavailable.free_bytes is None
    assert unavailable.total_bytes is None
