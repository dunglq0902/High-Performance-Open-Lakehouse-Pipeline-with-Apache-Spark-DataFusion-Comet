"""Run and resume the benchmark-laptop campaign through Docker Compose on Linux/WSL."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import re
import shutil
import subprocess
import tempfile
import time
from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from benchmark.cli import command_plan
from benchmark.parsers.eventlog import parse_event_log
from benchmark.runner.campaign import (
    CampaignError,
    CampaignReport,
    CampaignRun,
    CampaignRunner,
    run_subprocess,
)
from benchmark.runner.canonical import sha256_file, sha256_value, write_json
from benchmark.runner.capacity import (
    CapacitySnapshot,
    CgroupSnapshot,
    FilesystemSnapshot,
    evaluate_capacity_gate,
)
from benchmark.runner.config import load_document
from benchmark.runner.evidence import (
    ArtifactEvidenceError,
    RepositoryEvidenceError,
    artifact_evidence,
    clean_git_commit,
    control_artifact_evidence,
    ordered_raw_records,
    raw_records_sha256,
)
from benchmark.runner.record import (
    RawRecordContext,
    build_failure_record,
    build_raw_record,
    load_raw_schema,
)
from pipeline.benchmark.run_query import _table_identifier
from scripts.redact_logs import redact_file, sensitive_values

ROOT = Path(__file__).resolve().parents[1]
RAW_SCHEMA_PATH = ROOT / "benchmark/schemas/raw-result.schema.json"
EXPERIMENT_SCHEMA_PATH = ROOT / "benchmark/schemas/experiment-config.schema.json"
WORKLOAD_SCHEMA_PATH = ROOT / "benchmark/schemas/workload-manifest.schema.json"
LOCK_PATH = ROOT / "runtime-versions.lock"
EVENT_LOG_ROOT = ROOT / ".artifacts/spark-events"
COLLECTOR_PATH = ROOT / "benchmark/collectors/resources.py"
CALIBRATION_SCRIPT_PATH = ROOT / "scripts/calibrate_resource_collector.py"
SERVICE_EXPECTATIONS = {
    "minio": "healthy",
    "minio-init": "completed",
    "iceberg-rest": "healthy",
    "spark-master": "healthy",
    "spark-worker": "healthy",
}
MAX_RUN_ATTEMPTS = 3
ATTEMPT_ADMISSION_FILENAME = "attempt-admission.json"
_ATTEMPT_DIRECTORY_PATTERN = re.compile(r"^attempt-(?P<attempt>[0-9]{4})$")


def _run(
    command: list[str],
    *,
    timeout: float = 300,
    capture: bool = True,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=ROOT,
        check=True,
        capture_output=capture,
        text=True,
        timeout=timeout,
    )


def _service_decision(state: str, exit_code: int | None, expected: str) -> str:
    """Classify one Compose service observation as ready, waiting, or failed."""

    if expected not in {"healthy", "completed"}:
        raise ValueError(f"unsupported service expectation: {expected!r}")
    if expected == "healthy" and state == "healthy":
        return "ready"
    if expected == "completed" and state == "exited" and exit_code == 0:
        return "ready"
    if state == "unhealthy" or (state == "exited" and exit_code != 0):
        return "failed"
    if expected == "healthy" and state == "exited":
        return "failed"
    return "waiting"


def _service_observation(service: str) -> tuple[str, int | None] | None:
    try:
        container_id = _run(
            ["docker", "compose", "ps", "--all", "-q", service], timeout=15
        ).stdout.strip()
        if not container_id:
            return None
        inspection = json.loads(_run(["docker", "inspect", container_id], timeout=15).stdout)[0]
        state = inspection["State"]
        health = state.get("Health")
        observed_state = health.get("Status") if isinstance(health, dict) else state.get("Status")
        exit_code = state.get("ExitCode")
        if not isinstance(observed_state, str):
            return None
        return observed_state, exit_code if isinstance(exit_code, int) else None
    except (KeyError, IndexError, TypeError, json.JSONDecodeError, subprocess.SubprocessError):
        return None


def _wait_for_services(*, timeout_seconds: float = 600, poll_seconds: float = 2) -> None:
    deadline = time.monotonic() + timeout_seconds
    remaining = dict(SERVICE_EXPECTATIONS)
    observations: dict[str, tuple[str, int | None] | None] = {}
    while remaining and time.monotonic() < deadline:
        for service, expected in tuple(remaining.items()):
            observation = _service_observation(service)
            observations[service] = observation
            if observation is None:
                continue
            state, exit_code = observation
            decision = _service_decision(state, exit_code, expected)
            if decision == "ready":
                del remaining[service]
            elif decision == "failed":
                raise CampaignError(
                    f"Compose service {service} entered state={state!r} exit_code={exit_code!r}"
                )
        if remaining:
            time.sleep(poll_seconds)
    if remaining:
        details = ", ".join(
            f"{service}={observations.get(service)!r}" for service in sorted(remaining)
        )
        raise CampaignError(f"timed out waiting for Compose services: {details}")


def _compose_up() -> None:
    command = [
        "docker",
        "compose",
        "up",
        "-d",
        "--build",
        *SERVICE_EXPECTATIONS,
    ]
    last_error: subprocess.SubprocessError | None = None
    for attempt in range(1, 5):
        try:
            _run(command, timeout=1200, capture=False)
            break
        except subprocess.SubprocessError as error:
            last_error = error
            if attempt == 4:
                raise CampaignError("Docker Compose build/start failed after 4 attempts") from error
            time.sleep(attempt * 5)
    else:  # pragma: no cover - the loop either breaks or raises
        raise CampaignError("Docker Compose build/start failed") from last_error
    _wait_for_services()


def _container_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        relative = resolved.relative_to(ROOT)
    except ValueError as error:
        raise CampaignError(f"container path leaves repository: {path}") from error
    return "/opt/lakehouse/" + relative.as_posix()


def _storage_identity() -> str:
    """Fingerprint the exact persistent object-store/catalog volumes in use."""

    expected_mounts = {
        "iceberg-rest": "/var/lib/iceberg",
        "minio": "/data",
    }
    records: list[dict[str, str]] = []
    try:
        for service, destination in sorted(expected_mounts.items()):
            container_id = _run(
                ["docker", "compose", "ps", "--all", "-q", service], timeout=15
            ).stdout.strip()
            if not container_id:
                raise CampaignError(f"Compose service has no container identity: {service}")
            container_values: object = json.loads(
                _run(["docker", "inspect", container_id], timeout=15).stdout
            )
            if not isinstance(container_values, list) or len(container_values) != 1:
                raise CampaignError(f"Docker inspection is invalid for service: {service}")
            container = container_values[0]
            if not isinstance(container, dict) or not isinstance(container.get("Mounts"), list):
                raise CampaignError(f"Docker mount inspection is invalid for service: {service}")
            mounts = [
                mount
                for mount in container["Mounts"]
                if isinstance(mount, dict)
                and mount.get("Type") == "volume"
                and mount.get("Destination") == destination
                and isinstance(mount.get("Name"), str)
                and mount["Name"]
            ]
            if len(mounts) != 1:
                raise CampaignError(
                    f"expected one named volume for {service}:{destination}; found {len(mounts)}"
                )
            volume_name = str(mounts[0]["Name"])
            volume_values: object = json.loads(
                _run(["docker", "volume", "inspect", volume_name], timeout=15).stdout
            )
            if not isinstance(volume_values, list) or len(volume_values) != 1:
                raise CampaignError(f"Docker volume inspection is invalid: {volume_name}")
            volume = volume_values[0]
            if not isinstance(volume, dict):
                raise CampaignError(f"Docker volume inspection is invalid: {volume_name}")
            created_at = volume.get("CreatedAt")
            driver = volume.get("Driver")
            scope = volume.get("Scope")
            if any(
                not isinstance(value, str) or not value for value in (created_at, driver, scope)
            ):
                raise CampaignError(f"Docker volume identity is incomplete: {volume_name}")
            assert isinstance(created_at, str)
            assert isinstance(driver, str)
            assert isinstance(scope, str)
            records.append(
                {
                    "service": service,
                    "destination": destination,
                    "name": volume_name,
                    "created_at": created_at,
                    "driver": driver,
                    "scope": scope,
                }
            )
    except (json.JSONDecodeError, subprocess.SubprocessError) as error:
        raise CampaignError(f"cannot establish persistent storage identity: {error}") from error
    return sha256_value(records)


def _shared_directory(path: Path, *, exist_ok: bool = True) -> None:
    """Create one repo-local directory writable by the non-root Spark container user."""

    _container_path(path)
    path.mkdir(parents=True, exist_ok=exist_ok)
    path.chmod(path.stat().st_mode | 0o077)


def _utc_timestamp() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _is_utc_timestamp(value: object) -> bool:
    if not isinstance(value, str) or not value.endswith("Z"):
        return False
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError:
        return False
    return parsed.utcoffset() == timedelta(0)


def _run_attempt_directories(artifact_root: Path, run_id: str) -> tuple[tuple[int, Path], ...]:
    """Return the complete canonical attempt sequence without following links."""

    if not run_id or "/" in run_id or "\\" in run_id:
        raise CampaignError(f"unsafe run ID: {run_id!r}")
    parent = artifact_root / "runs" / run_id
    if not parent.exists():
        return ()
    if parent.is_symlink() or not parent.is_dir():
        raise CampaignError(f"run-attempt parent is not a real directory: {parent}")
    attempts: list[tuple[int, Path]] = []
    for path in parent.iterdir():
        match = _ATTEMPT_DIRECTORY_PATTERN.fullmatch(path.name)
        if match is None or path.is_symlink() or not path.is_dir():
            raise CampaignError(f"run-attempt directory contains unexpected evidence: {path}")
        attempts.append((int(match.group("attempt")), path))
    attempts.sort()
    if [attempt for attempt, _ in attempts] != list(range(1, len(attempts) + 1)):
        raise CampaignError(f"run-attempt sequence is not contiguous: {parent}")
    return tuple(attempts)


def _attempt_admission(
    run: CampaignRun,
    attempt: int,
    *,
    provenance: Mapping[str, Any],
    runtime: Mapping[str, object],
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_class": "research-run-attempt-admission-v1",
        "attempt_index": attempt,
        "run": {
            "experiment_id": run.experiment_id,
            "run_id": run.run_id,
            "phase": run.phase,
            "engine": run.engine,
            "pair_index": run.pair_index,
            "order_index": run.order_index,
            "warmup_runs": run.warmup_runs,
            "timeout_seconds": run.timeout_seconds,
        },
        "provenance": dict(provenance),
        "runtime": dict(runtime),
        "created_at": _utc_timestamp(),
    }
    value["artifact_sha256"] = sha256_value(value)
    return value


def _validate_attempt_admission(
    path: Path,
    run: CampaignRun,
    attempt: int,
    *,
    provenance: Mapping[str, Any],
    runtime: Mapping[str, object],
) -> None:
    try:
        value: object = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise CampaignError(f"run-attempt admission is unreadable: {path}: {error}") from error
    expected = _attempt_admission(
        run,
        attempt,
        provenance=provenance,
        runtime=runtime,
    )
    expected.pop("created_at")
    expected.pop("artifact_sha256")
    if not isinstance(value, dict):
        raise CampaignError(f"run-attempt admission root must be an object: {path}")
    payload = dict(value)
    artifact_sha256 = payload.pop("artifact_sha256", None)
    created_at = payload.pop("created_at", None)
    if (
        payload != expected
        or not _is_utc_timestamp(created_at)
        or not isinstance(artifact_sha256, str)
        or artifact_sha256 != sha256_value({**payload, "created_at": created_at})
    ):
        raise CampaignError(f"run-attempt admission does not match the current campaign: {path}")


def _next_run_attempt_dir(
    artifact_root: Path,
    run: CampaignRun,
    *,
    provenance: Mapping[str, Any],
    runtime: Mapping[str, object],
) -> Path:
    """Atomically admit one immutable attempt before any benchmark process can start."""

    attempts = _run_attempt_directories(artifact_root, run.run_id)
    attempt = len(attempts) + 1
    if attempt >= 10_000:
        raise CampaignError(f"too many immutable attempts for {run.run_id}")
    parent = artifact_root / "runs" / run.run_id
    _shared_directory(parent)
    staging_root = artifact_root / ".attempt-admission-staging"
    _shared_directory(staging_root)
    staging = Path(tempfile.mkdtemp(prefix=f"{run.run_id}-{attempt:04d}-", dir=staging_root))
    staging.chmod(staging.stat().st_mode | 0o077)
    admission = _attempt_admission(
        run,
        attempt,
        provenance=provenance,
        runtime=runtime,
    )
    write_json(staging / ATTEMPT_ADMISSION_FILENAME, admission)
    candidate = parent / f"attempt-{attempt:04d}"
    try:
        os.replace(staging, candidate)
    except OSError as error:
        shutil.rmtree(staging, ignore_errors=True)
        raise CampaignError(
            f"cannot atomically publish run-attempt admission: {candidate}"
        ) from error
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return candidate


def _runtime_from_lock(engine: str) -> dict[str, object]:
    lock = json.loads(LOCK_PATH.read_text(encoding="utf-8"))
    components = {item["name"]: item for item in lock["components"]}
    return {
        "spark_version": components["apache-spark"]["version"],
        "scala_version": components["scala"]["version"],
        "java_version": str(components["java"]["version"]).partition("+")[0],
        "comet_version": (
            components["datafusion-comet"]["version"] if engine == "comet_accelerated" else None
        ),
        "iceberg_version": components["apache-iceberg-runtime"]["version"],
    }


def _git_commit() -> str:
    try:
        return clean_git_commit(ROOT)
    except RepositoryEvidenceError as error:
        raise CampaignError(
            "primary campaign requires clean committed Git provenance: " + str(error)
        ) from error


def _cpu_model() -> str:
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.is_file():
        for line in cpuinfo.read_text(encoding="utf-8", errors="replace").splitlines():
            key, separator, value = line.partition(":")
            if separator and key.strip() == "model name" and value.strip():
                return value.strip()
    value = platform.processor().strip()
    return value or "unknown-cpu"


def _spark_submit_command(
    config: dict[str, Any], run: CampaignRun, medallion_path: Path, output_path: Path
) -> list[str]:
    container_name = "lakehouse-bench-" + hashlib.sha256(run.run_id.encode()).hexdigest()[:16]
    command = [
        "docker",
        "compose",
        "--profile",
        "tools",
        "run",
        "--rm",
        "--no-deps",
        "--name",
        container_name,
        "--entrypoint",
        "/opt/spark/bin/spark-submit",
        "spark-client",
        "--properties-file",
        "/opt/lakehouse/infrastructure/spark/profiles/benchmark-laptop-common.properties",
    ]
    engine_conf = next(
        item["spark_conf"] for item in config["matrix"]["engines"] if item["name"] == run.engine
    )
    for key, value in sorted(engine_conf.items()):
        rendered = str(value).lower() if isinstance(value, bool) else str(value)
        command.extend(("--conf", f"{key}={rendered}"))
    command.extend(
        [
            "/opt/lakehouse/pipeline/benchmark/run_query.py",
            "--engine",
            run.engine,
            "--experiment-config",
            "/opt/lakehouse/" + config["_config_relative_path"],
            "--snapshot-manifest",
            _container_path(medallion_path),
            "--phase",
            run.phase,
            "--experiment-id",
            run.experiment_id,
            "--run-id",
            run.run_id,
            "--warmup-runs",
            str(run.warmup_runs),
            "--worker-sampler-start-file",
            _container_path(output_path.parent / ".worker-sampler-start"),
            "--worker-sampler-started-file",
            _container_path(output_path.parent / ".worker-sampler-started"),
            "--worker-sampler-stop-file",
            _container_path(output_path.parent / ".worker-sampler-stop"),
            "--output",
            _container_path(output_path),
        ]
    )
    if run.pair_index is not None:
        command.extend(("--pair-index", str(run.pair_index)))
    return command


def _container_name(run: CampaignRun) -> str:
    return "lakehouse-bench-" + hashlib.sha256(run.run_id.encode()).hexdigest()[:16]


def _find_event_log(application_id: str, source_root: Path = EVENT_LOG_ROOT) -> Path:
    if not application_id or "/" in application_id or "\\" in application_id:
        raise CampaignError(f"unsafe Spark application ID: {application_id!r}")
    if not source_root.is_dir():
        raise CampaignError(f"Spark event-log root does not exist: {source_root}")
    candidates = [
        path
        for path in source_root.iterdir()
        if path.name in {application_id, f"{application_id}.inprogress"}
        or path.name.startswith(f"{application_id}_")
        or path.name
        in {
            f"eventlog_v2_{application_id}",
            f"eventlog_v2_{application_id}.inprogress",
        }
    ]
    logical = sorted({path.resolve() for path in candidates})
    if len(logical) != 1:
        raise CampaignError(
            f"expected one event log for {application_id}, found {[str(path) for path in logical]}"
        )
    return logical[0]


def _make_event_logs_host_readable() -> None:
    """Use the running Spark container's root user to expose UID-185 event logs to WSL."""

    try:
        _run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "--user",
                "0",
                "spark-master",
                "chmod",
                "-R",
                "a+rX",
                _container_path(EVENT_LOG_ROOT),
            ],
            timeout=30,
        )
    except subprocess.SubprocessError as error:
        raise CampaignError("failed to make Spark event logs host-readable") from error


def _copy_event_log(source: Path, destination: Path) -> Path:
    if destination.exists():
        raise CampaignError(f"event-log destination already exists: {destination}")
    if source.is_dir():
        shutil.copytree(source, destination)
    else:
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    return destination


def _wait_for_file(path: Path, process: subprocess.Popen[bytes], timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.is_file():
            return
        if process.poll() is not None:
            raise CampaignError(f"resource sampler exited before readiness: {process.returncode}")
        time.sleep(0.05)
    raise CampaignError(f"timed out waiting for resource sampler readiness: {path}")


def _terminate_sampler_process(
    process: subprocess.Popen[bytes],
    *,
    terminate_timeout: float = 10,
    kill_timeout: float = 10,
) -> None:
    """Stop a sampler deterministically, escalating from terminate to kill."""

    if process.poll() is not None:
        return
    with suppress(OSError):
        process.terminate()
    try:
        process.wait(timeout=terminate_timeout)
        return
    except subprocess.TimeoutExpired:
        pass
    with suppress(OSError):
        process.kill()
    try:
        process.wait(timeout=kill_timeout)
    except subprocess.TimeoutExpired as error:
        raise CampaignError("worker resource sampler survived terminate and kill") from error


@dataclass(frozen=True, slots=True)
class WorkerSamplerHandle:
    process: subprocess.Popen[bytes]
    stdout: Any
    stderr: Any
    output: Path
    start: Path
    started: Path
    stop: Path
    abort: Path


def _start_worker_sampler(run_dir: Path, timeout_seconds: int) -> WorkerSamplerHandle:
    output = run_dir / "worker-resource-samples.json"
    ready = run_dir / ".worker-sampler-ready"
    start = run_dir / ".worker-sampler-start"
    started = run_dir / ".worker-sampler-started"
    stop = run_dir / ".worker-sampler-stop"
    abort = run_dir / ".worker-sampler-abort"
    stdout = (run_dir / "worker-sampler.stdout.log").open("xb")
    stderr = (run_dir / "worker-sampler.stderr.log").open("xb")
    command = [
        "docker",
        "compose",
        "exec",
        "-T",
        "spark-worker",
        "/opt/lakehouse/.venv/bin/python",
        "/opt/lakehouse/scripts/sample_resources.py",
        "--output",
        _container_path(output),
        "--start-file",
        _container_path(start),
        "--started-file",
        _container_path(started),
        "--ready-file",
        _container_path(ready),
        "--stop-file",
        _container_path(stop),
        "--abort-file",
        _container_path(abort),
        "--timeout-seconds",
        str(timeout_seconds + 60),
    ]
    process: subprocess.Popen[bytes] | None = None
    try:
        process = subprocess.Popen(command, cwd=ROOT, stdout=stdout, stderr=stderr)
        _wait_for_file(ready, process, 15)
    except BaseException:
        try:
            if process is not None:
                _terminate_sampler_process(process)
        finally:
            stdout.close()
            stderr.close()
        raise
    assert process is not None
    return WorkerSamplerHandle(process, stdout, stderr, output, start, started, stop, abort)


def _stop_worker_sampler(handle: WorkerSamplerHandle) -> None:
    try:
        if handle.process.poll() is None and not handle.stop.is_file():
            with handle.abort.open("x", encoding="utf-8") as stream:
                stream.write("abort\n")
        try:
            handle.process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            handle.process.kill()
            handle.process.wait(timeout=10)
            raise CampaignError("worker resource sampler did not stop") from None
    finally:
        handle.stdout.close()
        handle.stderr.close()


def _resource_summary(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        return None
    if (
        value.get("timed_out") is not False
        or value.get("window_started") is not True
        or value.get("window_completed") is not True
        or value.get("aborted") is not False
    ):
        return None
    summary = value.get("summary")
    return summary if isinstance(summary, dict) else None


def _online_admission_failure(
    run: CampaignRun, record: Mapping[str, Any]
) -> tuple[str, str, str] | None:
    """Reject successful-looking records that the strict report cannot admit."""

    plan = record.get("plan_analysis")
    if not isinstance(plan, Mapping) or plan.get("status") != "complete":
        return (
            "invalid_result",
            "IncompletePlanEvidence",
            "final physical-plan analysis is not complete",
        )
    metrics = record.get("metrics")
    if run.phase == "measurement" and (
        not isinstance(metrics, Mapping) or metrics.get("collector_status") != "complete"
    ):
        return (
            "invalid_environment",
            "IncompleteResourceEvidence",
            "measurement resource collectors did not complete",
        )
    return None


def _image_id(service: str) -> str:
    container_id = _run(["docker", "compose", "ps", "-q", service]).stdout.strip()
    if not container_id:
        raise CampaignError(f"Compose service has no container: {service}")
    image_id = _run(["docker", "inspect", "--format", "{{.Image}}", container_id]).stdout.strip()
    if not image_id.startswith("sha256:") or len(image_id) != 71:
        raise CampaignError(f"Docker returned invalid image ID for {service}: {image_id!r}")
    return image_id


def _worker_limits() -> tuple[int, float, int]:
    container_id = _run(["docker", "compose", "ps", "-q", "spark-worker"]).stdout.strip()
    inspect = json.loads(_run(["docker", "inspect", container_id]).stdout)[0]
    memory = int(inspect["HostConfig"]["Memory"])
    nano_cpus = int(inspect["HostConfig"]["NanoCpus"])
    cpu_cores = nano_cpus / 1_000_000_000
    swap_text = _run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "spark-worker",
            "sh",
            "-c",
            "cat /sys/fs/cgroup/memory.swap.current",
        ]
    ).stdout.strip()
    return memory, cpu_cores, int(swap_text)


def _write_attempt_artifact(output: Path, value: object) -> Path:
    """Publish an immutable attempt without blocking a later retry."""

    attempts = _attempt_artifacts(output)
    if not attempts:
        write_json(output, value)
        return output
    latest_attempt, latest_path = attempts[-1]
    try:
        write_json(latest_path, value)
        return latest_path
    except FileExistsError:
        pass
    for attempt in range(latest_attempt + 1, 10_000):
        candidate = output.with_name(f"{output.stem}-attempt-{attempt:04d}{output.suffix}")
        try:
            write_json(candidate, value)
            return candidate
        except FileExistsError:
            continue
    raise CampaignError(f"too many attempt artifacts beside {output}")


def _attempt_artifacts(output: Path) -> tuple[tuple[int, Path], ...]:
    """Return immutable base/attempt artifacts in logical attempt order."""

    candidates: dict[int, Path] = {}
    if output.is_file():
        candidates[1] = output
    pattern = re.compile(rf"^{re.escape(output.stem)}-attempt-(\d{{4}}){re.escape(output.suffix)}$")
    if output.parent.is_dir():
        for path in output.parent.iterdir():
            match = pattern.fullmatch(path.name)
            if match is None or not path.is_file():
                continue
            attempt = int(match.group(1))
            if attempt < 2 or attempt in candidates:
                raise CampaignError(f"invalid duplicate attempt artifact: {path}")
            candidates[attempt] = path
    attempts = tuple(sorted(candidates.items()))
    if attempts and [attempt for attempt, _ in attempts] != list(range(1, attempts[-1][0] + 1)):
        raise CampaignError(f"artifact attempt sequence is not contiguous: {output}")
    return attempts


def _load_campaign_records(raw_root: Path, experiment_id: str) -> tuple[dict[str, Any], ...]:
    campaign_dir = raw_root / experiment_id
    records: list[dict[str, Any]] = []
    for path in sorted(campaign_dir.rglob("*.json")):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise CampaignError(f"cannot hash campaign raw record {path}: {error}") from error
        if not isinstance(value, dict):
            raise CampaignError(f"campaign raw record root is not an object: {path}")
        records.append(value)
    return tuple(records)


def _write_campaign_verification(
    output: Path,
    report: CampaignReport,
    raw_records: Sequence[Mapping[str, Any]],
    experiment_manifest: Mapping[str, Any],
    *,
    control_targets: Mapping[str, Path],
    repository_root: Path = ROOT,
) -> Path:
    """Publish one immutable campaign-completion attempt.

    A successful resume has different ``executed``/``resumed`` counters from the original run.
    Keeping those invocation facts in attempt artifacts preserves both histories without making a
    completed campaign fail merely because its base verification is immutable.
    """
    if len(raw_records) != report.planned:
        raise CampaignError(
            f"campaign verification requires {report.planned} raw records; "
            f"observed {len(raw_records)}"
        )
    run_ids = [str(record.get("run_id", "")) for record in raw_records]
    if (
        any(record.get("experiment_id") != report.experiment_id for record in raw_records)
        or any(not run_id for run_id in run_ids)
        or len(run_ids) != len(set(run_ids))
    ):
        raise CampaignError("campaign verification raw-record identity is invalid")
    manifest_for_hash = dict(experiment_manifest)
    declared_manifest_hash = manifest_for_hash.pop("manifest_sha256", None)
    if (
        experiment_manifest.get("experiment_id") != report.experiment_id
        or not isinstance(declared_manifest_hash, str)
        or declared_manifest_hash != sha256_value(manifest_for_hash)
    ):
        raise CampaignError("campaign verification experiment manifest is invalid")
    ordered_records = ordered_raw_records(raw_records)
    try:
        artifacts = artifact_evidence(ordered_records, repository_root)
        controls = control_artifact_evidence(control_targets, repository_root)
    except (ArtifactEvidenceError, OSError) as error:
        raise CampaignError(
            f"campaign verification artifact evidence is invalid: {error}"
        ) from error
    run_attempts = control_targets.get("run-attempts")
    failed_attempt_records = control_targets.get("failed-attempt-records")
    if run_attempts is None or failed_attempt_records is None:
        raise CampaignError(
            "campaign verification control evidence requires run and failed-attempt roots"
        )
    execution_attempt_count = sum(
        path.is_dir() and re.fullmatch(r"attempt-[0-9]{4}", path.name) is not None
        for path in run_attempts.glob("*/attempt-*")
    )
    failed_attempt_record_count = sum(
        path.is_file() for path in failed_attempt_records.rglob("*.json")
    )
    if execution_attempt_count != report.planned + failed_attempt_record_count:
        raise CampaignError(
            "campaign attempt evidence is incomplete: "
            f"attempts={execution_attempt_count}, planned={report.planned}, "
            f"failed_records={failed_attempt_record_count}"
        )
    return _write_attempt_artifact(
        output,
        {
            "schema_version": 1,
            "status": "passed" if report.complete else "failed",
            "report": {
                "experiment_id": report.experiment_id,
                "planned": report.planned,
                "executed": report.executed,
                "resumed": report.resumed,
                "succeeded": report.succeeded,
                "failed": report.failed,
                "complete": report.complete,
                "raw_record_count": len(ordered_records),
                "raw_records_sha256": raw_records_sha256(ordered_records),
                "artifact_file_count": artifacts["file_count"],
                "artifact_files_sha256": artifacts["sha256"],
                "control_artifacts": controls,
                "execution_attempt_count": execution_attempt_count,
                "failed_attempt_record_count": failed_attempt_record_count,
                "experiment_manifest_sha256": declared_manifest_hash,
            },
        },
    )


def _capacity_snapshot_from_artifact(value: Mapping[str, Any], *, label: str) -> CapacitySnapshot:
    observation = value.get("observation")
    if not isinstance(observation, Mapping) or set(observation) != {"filesystem", "cgroup"}:
        raise CampaignError(f"{label} has an invalid capacity observation")
    filesystem = observation.get("filesystem")
    cgroup = observation.get("cgroup")
    if not isinstance(filesystem, Mapping) or set(filesystem) != {
        "path",
        "free_bytes",
        "total_bytes",
    }:
        raise CampaignError(f"{label} has an invalid filesystem observation")
    if not isinstance(cgroup, Mapping) or set(cgroup) != {
        "memory_limit_bytes",
        "cpu_limit_cores",
        "swap_current_bytes",
        "swap_delta_bytes",
        "swap_peak_bytes",
    }:
        raise CampaignError(f"{label} has an invalid cgroup observation")
    path = filesystem.get("path")
    integer_values = (
        filesystem.get("free_bytes"),
        filesystem.get("total_bytes"),
        cgroup.get("memory_limit_bytes"),
        cgroup.get("swap_current_bytes"),
        cgroup.get("swap_delta_bytes"),
        cgroup.get("swap_peak_bytes"),
    )
    cpu_cores = cgroup.get("cpu_limit_cores")
    if (
        not isinstance(path, str)
        or not path
        or any(isinstance(item, bool) or not isinstance(item, int) for item in integer_values)
        or isinstance(cpu_cores, bool)
        or not isinstance(cpu_cores, int | float)
        or not math.isfinite(float(cpu_cores))
        or float(cpu_cores) <= 0
    ):
        raise CampaignError(f"{label} contains malformed capacity values")
    return CapacitySnapshot(
        filesystem=FilesystemSnapshot(
            path=path,
            free_bytes=filesystem["free_bytes"],
            total_bytes=filesystem["total_bytes"],
        ),
        cgroup=CgroupSnapshot(
            memory_limit_bytes=cgroup["memory_limit_bytes"],
            cpu_limit_cores=float(cpu_cores),
            swap_current_bytes=cgroup["swap_current_bytes"],
            swap_delta_bytes=cgroup["swap_delta_bytes"],
            swap_peak_bytes=cgroup["swap_peak_bytes"],
        ),
    )


def _validate_capacity_artifact(
    value: object,
    *,
    label: str,
    config: Mapping[str, Any],
    dataset_manifest: Mapping[str, Any],
    config_path: Path,
    dataset_manifest_path: Path,
    environment: Mapping[str, str],
) -> bool:
    """Recompute a persisted capacity decision against every current immutable input."""

    if not isinstance(value, dict):
        raise CampaignError(f"{label} root must be an object")
    snapshot = _capacity_snapshot_from_artifact(value, label=label)
    expected_result = evaluate_capacity_gate(config, dataset_manifest, snapshot).as_dict()
    expected_fields = {
        "schema_version",
        "artifact_class",
        "experiment_config_sha256",
        "dataset_manifest_sha256",
        "environment",
        "created_at",
        "artifact_sha256",
        "observation",
        *expected_result,
    }
    payload = dict(value)
    artifact_sha256 = payload.pop("artifact_sha256", None)
    declared_result = {field: value.get(field) for field in expected_result}
    if (
        set(value) != expected_fields
        or value.get("schema_version") != 1
        or value.get("artifact_class") != "research-capacity-gate-v1"
        or value.get("experiment_config_sha256") != sha256_file(config_path)
        or value.get("dataset_manifest_sha256") != sha256_file(dataset_manifest_path)
        or value.get("environment") != dict(environment)
        or not _is_utc_timestamp(value.get("created_at"))
        or not isinstance(artifact_sha256, str)
        or artifact_sha256 != sha256_value(payload)
        or declared_result != expected_result
    ):
        raise CampaignError(f"{label} does not match current capacity inputs and semantics")
    return expected_result["passed"] is True


def _capacity_gate(
    config: dict[str, Any],
    dataset_manifest: dict[str, Any],
    output: Path,
    *,
    environment: Mapping[str, str],
) -> Path:
    config_path = ROOT / config["_config_relative_path"]
    dataset_manifest_path = ROOT / config["workload"]["dataset_manifest"]
    attempts = _attempt_artifacts(output)
    passed_attempts: list[Path] = []
    for attempt, path in attempts:
        try:
            value: object = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise CampaignError(
                f"capacity gate attempt {attempt:04d} is unreadable: {error}"
            ) from error
        if _validate_capacity_artifact(
            value,
            label=f"capacity gate attempt {attempt:04d}",
            config=config,
            dataset_manifest=dataset_manifest,
            config_path=config_path,
            dataset_manifest_path=dataset_manifest_path,
            environment=environment,
        ):
            passed_attempts.append(path)
    if passed_attempts:
        if passed_attempts[-1] != attempts[-1][1]:
            raise CampaignError("a failed capacity attempt unexpectedly follows a passed attempt")
        return passed_attempts[-1]

    memory, cpu_cores, swap = _worker_limits()
    usage = shutil.disk_usage(ROOT / ".runtime")
    snapshot = CapacitySnapshot(
        filesystem=FilesystemSnapshot(
            path=str(ROOT / ".runtime"), free_bytes=usage.free, total_bytes=usage.total
        ),
        cgroup=CgroupSnapshot(
            memory_limit_bytes=memory,
            cpu_limit_cores=cpu_cores,
            swap_current_bytes=swap,
            swap_delta_bytes=0,
            swap_peak_bytes=swap,
        ),
    )
    result = evaluate_capacity_gate(config, dataset_manifest, snapshot)
    artifact = {
        "schema_version": 1,
        "artifact_class": "research-capacity-gate-v1",
        "experiment_config_sha256": sha256_file(config_path),
        "dataset_manifest_sha256": sha256_file(dataset_manifest_path),
        "environment": dict(environment),
        "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "observation": {
            "filesystem": {
                "path": snapshot.filesystem.path,
                "free_bytes": snapshot.filesystem.free_bytes,
                "total_bytes": snapshot.filesystem.total_bytes,
            },
            "cgroup": {
                "memory_limit_bytes": snapshot.cgroup.memory_limit_bytes,
                "cpu_limit_cores": snapshot.cgroup.cpu_limit_cores,
                "swap_current_bytes": snapshot.cgroup.swap_current_bytes,
                "swap_delta_bytes": snapshot.cgroup.swap_delta_bytes,
                "swap_peak_bytes": snapshot.cgroup.swap_peak_bytes,
            },
        },
        **result.as_dict(),
    }
    artifact["artifact_sha256"] = sha256_value(artifact)
    artifact_path = _write_attempt_artifact(output, artifact)
    if not result.passed:
        codes = [diagnostic.code for diagnostic in result.failures]
        raise CampaignError(f"benchmark capacity gate failed: {codes}")
    return artifact_path


def _validate_collector_calibration(
    value: object,
    *,
    expected_environment: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise CampaignError("collector calibration root must be an object")
    collector = value.get("collector")
    calibration = value.get("calibration")
    if not isinstance(collector, dict) or not isinstance(calibration, dict):
        raise CampaignError("collector calibration is missing collector/calibration details")
    checks = {
        "artifact class": value.get("artifact_class") == "resource-collector-calibration-v1",
        "status": value.get("status") == "passed",
        "sample interval": collector.get("sample_interval_seconds") == 0.2,
        "collector source hash": collector.get("module_sha256") == sha256_file(COLLECTOR_PATH),
        "calibration script hash": collector.get("script_sha256")
        == sha256_file(CALIBRATION_SCRIPT_PATH),
        "cgroup source": collector.get("observed_sources") == ["cgroup_v2"],
        "source gate": collector.get("source_gate_passed") is True,
        "status gate": collector.get("status_gate_passed") is True,
        "sampling gate": collector.get("sampling_gate_passed") is True,
        "overhead threshold": calibration.get("threshold_percent") == 2.0,
        "overhead accepted": calibration.get("accepted") is True,
    }
    failed = [name for name, passed in checks.items() if not passed]
    if expected_environment is not None:
        environment = value.get("environment")
        created_at = value.get("created_at")
        artifact_sha256 = value.get("artifact_sha256")
        payload_for_hash = dict(value)
        payload_for_hash.pop("artifact_sha256", None)
        checks = {
            "environment identity": environment == dict(expected_environment),
            "creation timestamp": isinstance(created_at, str) and created_at.endswith("Z"),
            "artifact self hash": isinstance(artifact_sha256, str)
            and artifact_sha256 == sha256_value(payload_for_hash),
        }
        failed.extend(name for name, passed in checks.items() if not passed)
    if failed:
        raise CampaignError("collector calibration gate failed: " + ", ".join(failed))
    return value


def _collector_calibration(
    output: Path,
    *,
    expected_environment: Mapping[str, str] | None = None,
) -> tuple[dict[str, Any], Path]:
    candidates = _attempt_artifacts(output)
    target = output
    if candidates:
        latest_attempt, latest_path = candidates[-1]
        try:
            existing = json.loads(latest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise CampaignError(f"cannot read collector calibration artifact: {error}") from error
        try:
            return (
                _validate_collector_calibration(
                    existing,
                    expected_environment=expected_environment,
                ),
                latest_path,
            )
        except CampaignError:
            if not isinstance(existing, dict) or existing.get("status") != "failed":
                raise
            if latest_attempt >= 9_999:
                raise CampaignError(f"too many calibration artifacts beside {output}") from None
            target = output.with_name(
                f"{output.stem}-attempt-{latest_attempt + 1:04d}{output.suffix}"
            )
    if not target.is_file():
        command = [
            "docker",
            "compose",
            "exec",
            "-T",
            "spark-worker",
            "/opt/lakehouse/.venv/bin/python",
            "/opt/lakehouse/scripts/calibrate_resource_collector.py",
            "--output",
            _container_path(target),
        ]
        if expected_environment is not None:
            command.extend(
                [
                    "--git-commit",
                    expected_environment["git_commit"],
                    "--container-image-digest",
                    expected_environment["container_image_digest"],
                    "--storage-identity-sha256",
                    expected_environment["storage_identity_sha256"],
                    "--cpu-model",
                    expected_environment["cpu_model"],
                ]
            )
        _run(command, timeout=300, capture=False)
    try:
        value = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise CampaignError(f"cannot read collector calibration artifact: {error}") from error
    return (
        _validate_collector_calibration(value, expected_environment=expected_environment),
        target,
    )


def _prepare_medallion(
    config: dict[str, Any],
    output: Path,
    *,
    dataset_attestation: Path | None = None,
    expected_git_commit: str | None = None,
) -> None:
    dataset_manifest = ROOT / config["workload"]["dataset_manifest"]
    expected_manifest_hash = sha256_file(dataset_manifest)
    expected_attestation_hash = (
        sha256_file(dataset_attestation) if dataset_attestation is not None else None
    )
    if output.is_file():
        existing = json.loads(output.read_text(encoding="utf-8"))
        if (
            existing.get("status") == "passed"
            and existing.get("dataset_manifest_sha256") == expected_manifest_hash
            and existing.get("dataset_validation_attestation_sha256") == expected_attestation_hash
        ):
            return
        raise CampaignError(f"existing Medallion artifact identity differs: {output}")
    command = [
        "docker",
        "compose",
        "--profile",
        "tools",
        "run",
        "--rm",
        "--no-deps",
        "--entrypoint",
        "/opt/spark/bin/spark-submit",
        "spark-client",
        "--properties-file",
        "/opt/lakehouse/infrastructure/spark/profiles/benchmark-laptop-common.properties",
        "/opt/lakehouse/pipeline/medallion/build.py",
        "--dataset-manifest",
        "/opt/lakehouse/" + config["workload"]["dataset_manifest"],
        "--output",
        _container_path(output),
    ]
    if dataset_attestation is not None:
        if expected_git_commit is None:
            raise CampaignError("attested Medallion build requires the current Git commit")
        command.extend(
            [
                "--dataset-validation-attestation",
                _container_path(dataset_attestation),
                "--expected-git-commit",
                expected_git_commit,
            ]
        )
    outcome = _run(command, timeout=3600, capture=False)
    if outcome.returncode != 0 or not output.is_file():
        raise CampaignError("Medallion build did not produce its audit artifact")


class DockerCampaignExecutor:
    def __init__(
        self,
        *,
        config: dict[str, Any],
        manifest: dict[str, Any],
        medallion_path: Path,
        artifact_root: Path,
        git_commit: str,
        spark_image_id: str,
        cpu_model: str,
        secrets: tuple[str, ...],
    ) -> None:
        self.config = config
        self.manifest = manifest
        self.medallion_path = medallion_path
        self.artifact_root = artifact_root
        self.git_commit = git_commit
        self.spark_image_id = spark_image_id
        self.secrets = secrets
        self.raw_schema = load_raw_schema(RAW_SCHEMA_PATH)
        self.cpu_model = cpu_model
        self.allocated_cores = 2
        self.cgroup_memory_limit_mib = 5120
        self.executor_heap_mib = 2048
        self.off_heap_mib = 1024
        self.medallion = json.loads(medallion_path.read_text(encoding="utf-8"))
        workload_path = ROOT / config["workload"]["manifest_file"]
        self.workload = load_document(workload_path, WORKLOAD_SCHEMA_PATH)

    def _spark_conf_sha256(self, run: CampaignRun) -> str:
        engine_conf = next(
            item["spark_conf"]
            for item in self.config["matrix"]["engines"]
            if item["name"] == run.engine
        )
        return sha256_value({"common": self.config["spark"]["common_conf"], "engine": engine_conf})

    def _iceberg_snapshot_ids(self) -> list[int]:
        snapshot_ids: set[int] = set()
        for binding in self.workload["relation_bindings"].values():
            _, snapshot_key = _table_identifier(
                binding["logical_table"], suite=self.workload["suite"]
            )
            try:
                snapshot_id = int(self.medallion["snapshots"][snapshot_key]["snapshot_id"])
            except (KeyError, TypeError, ValueError) as error:
                raise CampaignError(
                    f"current campaign has no pinned snapshot for {snapshot_key}"
                ) from error
            snapshot_ids.add(snapshot_id)
        return sorted(snapshot_ids)

    def expected_provenance(self, run: CampaignRun) -> dict[str, Any]:
        """Bind resume to the current immutable campaign and runtime inputs."""

        return {
            "git_commit": self.git_commit,
            "container_image_digest": self.spark_image_id,
            "dataset_manifest_sha256": self.manifest["input_hashes"]["dataset_manifest_sha256"],
            "spark_conf_sha256": self._spark_conf_sha256(run),
            "sql_sha256": self.manifest["input_hashes"]["workload_sql_sha256"],
            "iceberg_snapshot_ids": self._iceberg_snapshot_ids(),
            "resources": {
                "cpu_model": self.cpu_model,
                "allocated_cores": self.allocated_cores,
                "cgroup_memory_limit_mib": self.cgroup_memory_limit_mib,
                "executor_heap_mib": self.executor_heap_mib,
                "off_heap_mib": self.off_heap_mib,
            },
        }

    def _context(
        self,
        run: CampaignRun,
        run_dir: Path,
        *,
        event_log: Path | None,
        executor_resources: dict[str, Any] | None,
    ) -> RawRecordContext:
        def relative(path: Path | None) -> str | None:
            if path is None or not path.exists():
                return None
            return path.relative_to(ROOT).as_posix()

        return RawRecordContext(
            git_commit=self.git_commit,
            container_image_digest=self.spark_image_id,
            spark_conf_sha256=self._spark_conf_sha256(run),
            cpu_model=self.cpu_model,
            allocated_cores=self.allocated_cores,
            cgroup_memory_limit_mib=self.cgroup_memory_limit_mib,
            executor_heap_mib=self.executor_heap_mib,
            off_heap_mib=self.off_heap_mib,
            event_log=relative(event_log),
            physical_plan=relative(run_dir / "final-plan.txt"),
            resource_samples=relative(run_dir / "worker-resource-samples.json"),
            stdout=relative(run_dir / "stdout.log"),
            stderr=relative(run_dir / "stderr.log"),
            executor_resources=executor_resources,
        )

    def _failure(
        self,
        run: CampaignRun,
        context: RawRecordContext,
        *,
        status: str,
        failure_class: str,
        message: str,
    ) -> dict[str, Any]:
        return build_failure_record(
            run,
            context,
            timestamp=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            status=status,
            failure_class=failure_class,
            failure_message=message[:1_000],
            workload=self.config["workload"]["suite"],
            query_id=self.config["workload"]["query_id"],
            storage_profile=self.config["workload"]["storage_profile"],
            dataset_manifest_sha256=self.manifest["input_hashes"]["dataset_manifest_sha256"],
            sql_sha256=self.manifest["input_hashes"]["workload_sql_sha256"],
            iceberg_snapshot_ids=self._iceberg_snapshot_ids(),
            runtime=_runtime_from_lock(run.engine),
            raw_schema=self.raw_schema,
        )

    def recover_interrupted_attempt(
        self,
        run: CampaignRun,
        *,
        raw_path: Path,
        failure_root: Path,
    ) -> None:
        """Close one admitted attempt that was interrupted before its terminal JSON publish."""

        attempts = _run_attempt_directories(self.artifact_root, run.run_id)
        failure_directory = failure_root / run.experiment_id / run.engine
        failure_paths = tuple(sorted(failure_directory.glob(f"{run.run_id}-attempt-*.json")))
        for attempt, path in enumerate(failure_paths, 1):
            if path.name != f"{run.run_id}-attempt-{attempt:04d}.json" or not path.is_file():
                raise CampaignError(f"failed-attempt sequence is not contiguous: {path}")

        provenance = self.expected_provenance(run)
        runtime = _runtime_from_lock(run.engine)
        for attempt, run_dir in attempts:
            _validate_attempt_admission(
                run_dir / ATTEMPT_ADMISSION_FILENAME,
                run,
                attempt,
                provenance=provenance,
                runtime=runtime,
            )

        closed_attempts = len(failure_paths) + int(raw_path.is_file())
        if len(attempts) < closed_attempts:
            raise CampaignError(
                f"terminal records outnumber admitted run attempts for {run.run_id}"
            )
        if raw_path.exists() and len(attempts) != closed_attempts:
            raise CampaignError(f"run attempts unexpectedly continue after success: {run.run_id}")
        orphan_count = len(attempts) - closed_attempts
        if orphan_count == 0:
            return
        if orphan_count != 1 or raw_path.exists():
            raise CampaignError(
                f"run {run.run_id} has {orphan_count} interrupted attempts; "
                "only one sequential crash can be recovered"
            )

        attempt, run_dir = attempts[-1]
        for path in (
            run_dir / "stdout.log",
            run_dir / "stderr.log",
            run_dir / "worker-sampler.stdout.log",
            run_dir / "worker-sampler.stderr.log",
        ):
            if path.is_file():
                redact_file(path, self.secrets)
        try:
            resources = _resource_summary(run_dir / "worker-resource-samples.json")
        except (OSError, ValueError, json.JSONDecodeError):
            resources = None
        event_log = run_dir / "event-log"
        context = self._context(
            run,
            run_dir,
            event_log=event_log if event_log.exists() else None,
            executor_resources=resources,
        )
        record = self._failure(
            run,
            context,
            status="failed",
            failure_class="InterruptedAttempt",
            message="admitted benchmark attempt ended before terminal record publication",
        )
        write_json(
            failure_directory / f"{run.run_id}-attempt-{attempt:04d}.json",
            record,
        )

    def __call__(self, run: CampaignRun) -> dict[str, Any]:
        run_dir = _next_run_attempt_dir(
            self.artifact_root,
            run,
            provenance=self.expected_provenance(run),
            runtime=_runtime_from_lock(run.engine),
        )
        application_path = run_dir / "application-result.json"
        worker_sampler: WorkerSamplerHandle | None = None
        try:
            worker_sampler = _start_worker_sampler(run_dir, run.timeout_seconds)
            try:
                outcome = run_subprocess(
                    _spark_submit_command(self.config, run, self.medallion_path, application_path),
                    timeout_seconds=run.timeout_seconds,
                    stdout_path=run_dir / "stdout.log",
                    stderr_path=run_dir / "stderr.log",
                    cwd=ROOT,
                )
            finally:
                _stop_worker_sampler(worker_sampler)
        except (CampaignError, OSError, subprocess.SubprocessError) as error:
            context = self._context(run, run_dir, event_log=None, executor_resources=None)
            return self._failure(
                run,
                context,
                status="failed",
                failure_class=type(error).__name__,
                message=f"benchmark launcher failed: {error}",
            )
        finally:
            for path in (
                run_dir / "stdout.log",
                run_dir / "stderr.log",
                run_dir / "worker-sampler.stdout.log",
                run_dir / "worker-sampler.stderr.log",
            ):
                if path.is_file():
                    redact_file(path, self.secrets)

        assert worker_sampler is not None
        try:
            resources = _resource_summary(worker_sampler.output)
        except (OSError, ValueError, json.JSONDecodeError) as error:
            context = self._context(run, run_dir, event_log=None, executor_resources=None)
            return self._failure(
                run,
                context,
                status="failed",
                failure_class=type(error).__name__,
                message=f"resource evidence is unreadable: {error}",
            )
        context = self._context(run, run_dir, event_log=None, executor_resources=resources)
        if outcome.timed_out:
            container = _container_name(run)
            subprocess.run(
                ["docker", "rm", "-f", container],
                cwd=ROOT,
                capture_output=True,
                check=False,
            )
            return self._failure(
                run,
                context,
                status="timeout",
                failure_class="ApplicationTimeout",
                message=f"Spark application exceeded {run.timeout_seconds}s",
            )
        if not application_path.is_file():
            return self._failure(
                run,
                context,
                status="failed",
                failure_class="MissingApplicationResult",
                message=f"spark-submit exited {outcome.return_code} without result artifact",
            )
        try:
            application = json.loads(application_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            return self._failure(
                run,
                context,
                status="invalid_result",
                failure_class=type(error).__name__,
                message=f"application result is unreadable: {error}",
            )
        if not isinstance(application, dict):
            return self._failure(
                run,
                context,
                status="invalid_result",
                failure_class="MalformedApplicationResult",
                message="application result root is not an object",
            )
        application_id = application.get("application_id")
        if not isinstance(application_id, str):
            return self._failure(
                run,
                context,
                status="failed",
                failure_class="MissingApplicationId",
                message="application result has no Spark application ID",
            )
        try:
            _make_event_logs_host_readable()
            source_event_log = _find_event_log(application_id)
            copied_event_log = _copy_event_log(source_event_log, run_dir / "event-log")
        except (CampaignError, OSError, subprocess.SubprocessError) as error:
            return self._failure(
                run,
                context,
                status="failed",
                failure_class=type(error).__name__,
                message=f"Spark event-log collection failed: {error}",
            )
        context = self._context(
            run,
            run_dir,
            event_log=copied_event_log,
            executor_resources=resources,
        )
        if application.get("status") == "failed":
            failure = application.get("failure") or {}
            return self._failure(
                run,
                context,
                status="failed",
                failure_class=str(failure.get("class", "SparkApplicationFailure")),
                message=str(failure.get("message", "Spark application failed")),
            )
        try:
            report = parse_event_log(copied_event_log)
        except (OSError, ValueError) as error:
            return self._failure(
                run,
                context,
                status="failed",
                failure_class=type(error).__name__,
                message=f"Spark event-log parsing failed: {error}",
            )
        try:
            record = build_raw_record(
                run,
                application,
                report,
                context,
                raw_schema=self.raw_schema,
            )
        except (CampaignError, KeyError, TypeError, ValueError) as error:
            return self._failure(
                run,
                context,
                status="invalid_result",
                failure_class=type(error).__name__,
                message=f"application result admission failed: {error}",
            )
        swap_peak = resources.get("swap_peak_bytes") if resources else None
        if swap_peak not in {0, None}:
            return self._failure(
                run,
                context,
                status="invalid_environment",
                failure_class="SwapPolicyViolation",
                message=f"worker cgroup used {swap_peak} bytes of swap",
            )
        admission_failure = _online_admission_failure(run, record)
        if admission_failure is not None:
            status, failure_class, message = admission_failure
            return self._failure(
                run,
                context,
                status=status,
                failure_class=failure_class,
                message=message,
            )
        return record


def _plan(
    config_relative: str,
    output: Path,
    *,
    dataset_attestation: Path,
) -> dict[str, Any]:
    namespace = argparse.Namespace(
        config=config_relative,
        output=output.relative_to(ROOT).as_posix(),
        root=str(ROOT),
        dataset_attestation=dataset_attestation.relative_to(ROOT).as_posix(),
    )
    command_plan(namespace)
    value = json.loads(output.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise CampaignError("experiment plan root must be an object")
    return value


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="benchmark/configs/benchmark-laptop-m02.yaml")
    parser.add_argument("--keep-services", action="store_true")
    parser.add_argument("--dataset-attestation", type=Path, required=True)
    args = parser.parse_args()

    if os.name == "nt":
        raise SystemExit("run the research campaign from Ubuntu/WSL, not PowerShell")
    _run(["docker", "info"], timeout=30)
    config_path = (ROOT / args.config).resolve()
    config = load_document(config_path, EXPERIMENT_SCHEMA_PATH)
    config["_config_relative_path"] = config_path.relative_to(ROOT).as_posix()
    dataset_path = ROOT / config["workload"]["dataset_manifest"]
    if not dataset_path.is_file():
        raise SystemExit("research dataset is missing; run make research-data first")
    dataset_manifest = json.loads(dataset_path.read_text(encoding="utf-8"))
    commit = _git_commit()
    dataset_attestation = args.dataset_attestation.resolve()
    try:
        dataset_attestation.relative_to(ROOT)
    except ValueError as error:
        raise SystemExit("dataset attestation leaves the repository") from error
    if not dataset_attestation.is_file():
        raise SystemExit(f"dataset attestation is missing: {dataset_attestation}")
    experiment_id = config["experiment"]["id"]
    artifact_root = ROOT / ".artifacts/campaigns" / experiment_id
    for shared in (
        ROOT / ".artifacts",
        EVENT_LOG_ROOT,
        ROOT / ".runtime",
        ROOT / ".runtime/spark-local",
        artifact_root,
    ):
        _shared_directory(shared)
    plan_path = artifact_root / "experiment-manifest.json"
    manifest = _plan(
        config["_config_relative_path"],
        plan_path,
        dataset_attestation=dataset_attestation,
    )

    compose_attempted = False
    try:
        compose_attempted = True
        _compose_up()
        spark_image_id = _image_id("spark-worker")
        storage_identity = _storage_identity()
        cpu_model = _cpu_model()
        control_environment = {
            "git_commit": commit,
            "container_image_digest": spark_image_id,
            "storage_identity_sha256": storage_identity,
            "cpu_model": cpu_model,
        }
        capacity_path = _capacity_gate(
            config,
            dataset_manifest,
            artifact_root / "capacity-gate.json",
            environment=control_environment,
        )
        shared_runtime = (
            ROOT
            / ".artifacts/research-shared"
            / commit
            / spark_image_id.removeprefix("sha256:")
            / storage_identity
        )
        _shared_directory(shared_runtime)
        calibration_base = shared_runtime / "collector-calibration.json"
        _, calibration_path = _collector_calibration(
            calibration_base,
            expected_environment=control_environment,
        )
        dataset_validation_identity = sha256_file(dataset_attestation)
        medallion_path = (
            shared_runtime
            / "datasets"
            / manifest["input_hashes"]["dataset_manifest_sha256"]
            / dataset_validation_identity
            / "medallion.json"
        )
        _shared_directory(medallion_path.parent)
        _prepare_medallion(
            config,
            medallion_path,
            dataset_attestation=dataset_attestation,
            expected_git_commit=commit,
        )
        executor = DockerCampaignExecutor(
            config=config,
            manifest=manifest,
            medallion_path=medallion_path,
            artifact_root=artifact_root,
            git_commit=commit,
            spark_image_id=spark_image_id,
            cpu_model=cpu_model,
            secrets=sensitive_values(ROOT / ".env"),
        )
        failure_root = artifact_root / "failed-attempts"
        _shared_directory(failure_root)
        report = CampaignRunner.from_schema_path(RAW_SCHEMA_PATH).run(
            manifest,
            ROOT / "results/raw",
            executor,
            max_attempts=MAX_RUN_ATTEMPTS,
            failure_root=failure_root,
            expected_provenance=executor.expected_provenance,
        )
        control_targets: dict[str, Path] = {
            "run-attempts": artifact_root / "runs",
            "failed-attempt-records": failure_root,
            "medallion-audit": medallion_path,
        }
        control_targets["dataset-validation-attestation"] = dataset_attestation
        capacity_attempts = _attempt_artifacts(artifact_root / "capacity-gate.json")
        if capacity_path not in {path for _, path in capacity_attempts}:
            raise CampaignError("selected capacity-gate artifact is absent from its attempt set")
        for attempt, path in capacity_attempts:
            control_targets[f"capacity-gate-{attempt:04d}"] = path
        calibration_attempts = _attempt_artifacts(calibration_base)
        if calibration_path not in {path for _, path in calibration_attempts}:
            raise CampaignError("selected collector calibration is absent from its attempt set")
        for attempt, path in calibration_attempts:
            control_targets[f"collector-calibration-{attempt:04d}"] = path
        _write_campaign_verification(
            artifact_root / "campaign-verification.json",
            report,
            _load_campaign_records(ROOT / "results/raw", report.experiment_id),
            manifest,
            control_targets=control_targets,
        )
        if not report.complete:
            raise CampaignError("campaign did not complete successfully")
    finally:
        if compose_attempted and not args.keep_services:
            subprocess.run(["docker", "compose", "down"], cwd=ROOT, check=False)


if __name__ == "__main__":
    main()
