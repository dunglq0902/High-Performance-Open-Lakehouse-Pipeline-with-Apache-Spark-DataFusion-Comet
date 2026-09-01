"""Calibrate the 200 ms cgroup collector before a primary campaign."""

from __future__ import annotations

import argparse
import hashlib
import platform
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from benchmark.collectors.resources import (
    DEFAULT_OVERHEAD_LIMIT_PERCENT,
    DEFAULT_SAMPLE_INTERVAL_SECONDS,
    CollectorStatus,
    OverheadCalibration,
    ResourceSampler,
    ResourceSource,
    create_resource_source,
    measure_collector_overhead,
)
from benchmark.runner.canonical import sha256_file, sha256_value, write_json

ROOT = Path(__file__).resolve().parents[1]
COLLECTOR_PATH = ROOT / "benchmark/collectors/resources.py"
SCRIPT_PATH = Path(__file__).resolve()
WORKLOAD_ID = "sha256-chain-v1"


def deterministic_workload(work_units: int) -> str:
    """Run a stable CPU workload long enough to exercise periodic sampling."""

    if work_units < 1:
        raise ValueError("work_units must be positive")
    digest = b"lakehouse-resource-collector-calibration-v1"
    for _ in range(work_units):
        digest = hashlib.sha256(digest).digest()
    return digest.hex()


def calibration_artifact(
    calibration: OverheadCalibration,
    *,
    work_units: int,
    observed_sources: set[str],
    observed_statuses: set[CollectorStatus],
    minimum_samples_per_run: int,
) -> dict[str, object]:
    source_gate = observed_sources == {"cgroup_v2"}
    status_gate = bool(observed_statuses) and CollectorStatus.UNAVAILABLE not in observed_statuses
    sampling_gate = minimum_samples_per_run >= 2
    passed = calibration.accepted and source_gate and status_gate and sampling_gate
    return {
        "schema_version": 1,
        "artifact_class": "resource-collector-calibration-v1",
        "status": "passed" if passed else "failed",
        "workload": {"id": WORKLOAD_ID, "work_units": work_units},
        "collector": {
            "sample_interval_seconds": DEFAULT_SAMPLE_INTERVAL_SECONDS,
            "module_sha256": sha256_file(COLLECTOR_PATH),
            "script_sha256": sha256_file(SCRIPT_PATH),
            "observed_sources": sorted(observed_sources),
            "observed_statuses": sorted(status.value for status in observed_statuses),
            "minimum_samples_per_run": minimum_samples_per_run,
            "source_gate_passed": source_gate,
            "status_gate_passed": status_gate,
            "sampling_gate_passed": sampling_gate,
        },
        "runtime": {
            "python_version": platform.python_version(),
            "platform": platform.platform(),
        },
        "calibration": calibration.as_dict(),
    }


def run_calibration(
    *,
    repetitions: int,
    work_units: int,
    source_factory: Callable[[], ResourceSource] = create_resource_source,
) -> dict[str, object]:
    expected_digest = deterministic_workload(work_units)
    observed_sources: set[str] = set()
    observed_statuses: set[CollectorStatus] = set()
    sample_counts: list[int] = []

    def baseline() -> None:
        if deterministic_workload(work_units) != expected_digest:
            raise RuntimeError("calibration workload was not deterministic")

    def instrumented() -> None:
        sampler = ResourceSampler(source_factory())
        sampler.start()
        try:
            if deterministic_workload(work_units) != expected_digest:
                raise RuntimeError("instrumented calibration workload changed its result")
        finally:
            samples = sampler.stop(timeout_seconds=10)
        sample_counts.append(len(samples))
        observed_sources.update(sample.source for sample in samples)
        observed_statuses.update(sample.status for sample in samples)

    calibration = measure_collector_overhead(
        baseline,
        instrumented,
        repetitions=repetitions,
        threshold_percent=DEFAULT_OVERHEAD_LIMIT_PERCENT,
    )
    return calibration_artifact(
        calibration,
        work_units=work_units,
        observed_sources=observed_sources,
        observed_statuses=observed_statuses,
        minimum_samples_per_run=min(sample_counts, default=0),
    )


def bind_calibration_environment(
    artifact: dict[str, object],
    *,
    git_commit: str,
    container_image_digest: str,
    storage_identity_sha256: str,
    cpu_model: str,
) -> dict[str, object]:
    """Bind a measured calibration to the exact reusable campaign environment."""

    value = {
        **artifact,
        "environment": {
            "git_commit": git_commit,
            "container_image_digest": container_image_digest,
            "storage_identity_sha256": storage_identity_sha256,
            "cpu_model": cpu_model,
        },
        "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }
    value["artifact_sha256"] = sha256_value(value)
    return value


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repetitions", type=int, default=7)
    parser.add_argument("--work-units", type=int, default=2_000_000)
    parser.add_argument("--git-commit")
    parser.add_argument("--container-image-digest")
    parser.add_argument("--storage-identity-sha256")
    parser.add_argument("--cpu-model")
    args = parser.parse_args()
    if args.repetitions < 3:
        parser.error("repetitions must be at least 3")
    artifact = run_calibration(repetitions=args.repetitions, work_units=args.work_units)
    identity_values = (
        args.git_commit,
        args.container_image_digest,
        args.storage_identity_sha256,
        args.cpu_model,
    )
    if any(identity_values) and not all(identity_values):
        parser.error("calibration environment identity must be supplied as one complete set")
    if all(identity_values):
        artifact = bind_calibration_environment(
            artifact,
            git_commit=args.git_commit,
            container_image_digest=args.container_image_digest,
            storage_identity_sha256=args.storage_identity_sha256,
            cpu_model=args.cpu_model,
        )
    write_json(args.output, artifact)
    if artifact["status"] != "passed":
        raise SystemExit(2)
    print(args.output)


if __name__ == "__main__":
    main()
