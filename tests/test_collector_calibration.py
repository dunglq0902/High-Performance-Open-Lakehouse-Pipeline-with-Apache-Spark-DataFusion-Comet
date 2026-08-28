from __future__ import annotations

from benchmark.collectors.resources import CollectorStatus, calibrate_overhead
from scripts.calibrate_resource_collector import calibration_artifact, deterministic_workload


def test_calibration_workload_is_deterministic_and_parameterized() -> None:
    assert deterministic_workload(10) == deterministic_workload(10)
    assert deterministic_workload(10) != deterministic_workload(11)


def test_calibration_artifact_fails_closed_on_source_or_sampling_gaps() -> None:
    accepted = calibrate_overhead([100, 100, 100], [101, 101, 101])
    passed = calibration_artifact(
        accepted,
        work_units=10,
        observed_sources={"cgroup_v2"},
        observed_statuses={CollectorStatus.COMPLETE},
        minimum_samples_per_run=2,
    )
    assert passed["status"] == "passed"

    wrong_source = calibration_artifact(
        accepted,
        work_units=10,
        observed_sources={"process_tree"},
        observed_statuses={CollectorStatus.COMPLETE},
        minimum_samples_per_run=2,
    )
    assert wrong_source["status"] == "failed"
    assert wrong_source["collector"]["source_gate_passed"] is False

    too_few_samples = calibration_artifact(
        accepted,
        work_units=10,
        observed_sources={"cgroup_v2"},
        observed_statuses={CollectorStatus.PARTIAL},
        minimum_samples_per_run=1,
    )
    assert too_few_samples["status"] == "failed"
    assert too_few_samples["collector"]["sampling_gate_passed"] is False
