"""Run and resume the benchmark-laptop campaign through Docker Compose on Linux/WSL."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import subprocess
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from benchmark.cli import command_plan
from benchmark.parsers.eventlog import parse_event_log
from benchmark.runner.campaign import CampaignError, CampaignRun, CampaignRunner, run_subprocess
from benchmark.runner.canonical import sha256_file, sha256_value, write_json
from benchmark.runner.capacity import (
    CapacitySnapshot,
    CgroupSnapshot,
    FilesystemSnapshot,
    evaluate_capacity_gate,
)
from benchmark.runner.config import load_document
from benchmark.runner.record import (
    RawRecordContext,
    build_failure_record,
    build_raw_record,
    load_raw_schema,
)
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


def _shared_directory(path: Path, *, exist_ok: bool = True) -> None:
    """Create one repo-local directory writable by the non-root Spark container user."""

    _container_path(path)
    path.mkdir(parents=True, exist_ok=exist_ok)
    path.chmod(path.stat().st_mode | 0o077)


def _runtime_from_lock(engine: str) -> dict[str, object]:
    lock = json.loads(LOCK_PATH.read_text(encoding="utf-8"))
    components = {item["name"]: item for item in lock["components"]}
    return {
        "spark_version": components["apache-spark"]["version"],
        "scala_version": components["scala"]["version"],
        "java_version": components["java"]["version"],
        "comet_version": (
            components["datafusion-comet"]["version"] if engine == "comet_accelerated" else None
        ),
        "iceberg_version": components["apache-iceberg-runtime"]["version"],
    }


def _git_commit() -> str:
    commit = _run(["git", "rev-parse", "HEAD"]).stdout.strip()
    dirty = _run(["git", "status", "--porcelain", "--untracked-files=normal"]).stdout.strip()
    if dirty:
        raise CampaignError(
            "primary campaign requires a clean committed worktree; commit the implementation first"
        )
    if len(commit) != 40:
        raise CampaignError(f"unexpected Git commit identity: {commit!r}")
    return commit


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
    candidates = [
        path
        for path in source_root.rglob(f"{application_id}*")
        if path.name in {application_id, f"{application_id}.inprogress"}
        or path.name.startswith(f"{application_id}_")
    ]
    logical = sorted({path.resolve() for path in candidates})
    if len(logical) != 1:
        raise CampaignError(
            f"expected one event log for {application_id}, found {[str(path) for path in logical]}"
        )
    return logical[0]


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
    process = subprocess.Popen(command, cwd=ROOT, stdout=stdout, stderr=stderr)
    try:
        _wait_for_file(ready, process, 15)
    except BaseException:
        process.terminate()
        process.wait(timeout=10)
        stdout.close()
        stderr.close()
        raise
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


def _write_admission_artifact(output: Path, value: object) -> Path:
    """Publish an immutable admission attempt without blocking a later retry."""

    try:
        write_json(output, value)
        return output
    except FileExistsError:
        pass
    for attempt in range(2, 10_000):
        candidate = output.with_name(f"{output.stem}-attempt-{attempt:04d}{output.suffix}")
        try:
            write_json(candidate, value)
            return candidate
        except FileExistsError:
            continue
    raise CampaignError(f"too many admission artifacts beside {output}")


def _capacity_gate(config: dict[str, Any], dataset_manifest: dict[str, Any], output: Path) -> Path:
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
    artifact_path = _write_admission_artifact(output, {"schema_version": 1, **result.as_dict()})
    if not result.passed:
        codes = [diagnostic.code for diagnostic in result.failures]
        raise CampaignError(f"benchmark capacity gate failed: {codes}")
    return artifact_path


def _validate_collector_calibration(value: object) -> dict[str, Any]:
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
    if failed:
        raise CampaignError("collector calibration gate failed: " + ", ".join(failed))
    return value


def _collector_calibration(output: Path) -> dict[str, Any]:
    target = output
    if output.is_file():
        try:
            existing = json.loads(output.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise CampaignError(f"cannot read collector calibration artifact: {error}") from error
        try:
            return _validate_collector_calibration(existing)
        except CampaignError:
            if not isinstance(existing, dict) or existing.get("status") != "failed":
                raise
            for attempt in range(2, 10_000):
                candidate = output.with_name(f"{output.stem}-attempt-{attempt:04d}{output.suffix}")
                if not candidate.exists():
                    target = candidate
                    break
            else:
                raise CampaignError(f"too many calibration artifacts beside {output}")
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
        _run(command, timeout=300, capture=False)
    try:
        value = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise CampaignError(f"cannot read collector calibration artifact: {error}") from error
    return _validate_collector_calibration(value)


def _prepare_medallion(config: dict[str, Any], output: Path) -> None:
    if output.is_file():
        existing = json.loads(output.read_text(encoding="utf-8"))
        if existing.get("status") == "passed":
            return
        raise CampaignError(f"existing Medallion artifact did not pass: {output}")
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
        self.medallion = json.loads(medallion_path.read_text(encoding="utf-8"))
        workload_path = ROOT / config["workload"]["manifest_file"]
        self.workload = load_document(workload_path, WORKLOAD_SCHEMA_PATH)

    def _context(
        self,
        run: CampaignRun,
        run_dir: Path,
        *,
        event_log: Path | None,
        executor_resources: dict[str, Any] | None,
    ) -> RawRecordContext:
        engine_conf = next(
            item["spark_conf"]
            for item in self.config["matrix"]["engines"]
            if item["name"] == run.engine
        )
        conf_hash = sha256_value(
            {"common": self.config["spark"]["common_conf"], "engine": engine_conf}
        )

        def relative(path: Path | None) -> str | None:
            if path is None or not path.exists():
                return None
            return path.relative_to(ROOT).as_posix()

        return RawRecordContext(
            git_commit=self.git_commit,
            container_image_digest=self.spark_image_id,
            spark_conf_sha256=conf_hash,
            cpu_model=_cpu_model(),
            allocated_cores=2,
            cgroup_memory_limit_mib=5120,
            executor_heap_mib=2048,
            off_heap_mib=1024,
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
        snapshot_ids = sorted(
            {
                int(value["snapshot_id"])
                for value in self.medallion.get("snapshots", {}).values()
                if isinstance(value, dict) and "snapshot_id" in value
            }
        )
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
            iceberg_snapshot_ids=snapshot_ids,
            runtime=_runtime_from_lock(run.engine),
            raw_schema=self.raw_schema,
        )

    def __call__(self, run: CampaignRun) -> dict[str, Any]:
        run_dir = self.artifact_root / "runs" / run.run_id
        try:
            _shared_directory(run_dir, exist_ok=False)
        except FileExistsError as error:
            raise CampaignError(
                f"run artifacts exist without a resumable raw record: {run_dir}"
            ) from error
        application_path = run_dir / "application-result.json"
        worker_sampler = _start_worker_sampler(run_dir, run.timeout_seconds)
        outcome = None
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
        for path in (
            run_dir / "stdout.log",
            run_dir / "stderr.log",
            run_dir / "worker-sampler.stdout.log",
            run_dir / "worker-sampler.stderr.log",
        ):
            if path.is_file():
                redact_file(path, self.secrets)

        resources = _resource_summary(worker_sampler.output)
        context = self._context(run, run_dir, event_log=None, executor_resources=resources)
        assert outcome is not None
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
        application = json.loads(application_path.read_text(encoding="utf-8"))
        application_id = application.get("application_id")
        if not isinstance(application_id, str):
            return self._failure(
                run,
                context,
                status="failed",
                failure_class="MissingApplicationId",
                message="application result has no Spark application ID",
            )
        source_event_log = _find_event_log(application_id)
        copied_event_log = _copy_event_log(source_event_log, run_dir / "event-log")
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
        report = parse_event_log(copied_event_log)
        record = build_raw_record(
            run,
            application,
            report,
            context,
            raw_schema=self.raw_schema,
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
        return record


def _plan(config_relative: str, output: Path) -> dict[str, Any]:
    namespace = argparse.Namespace(
        config=config_relative,
        output=output.relative_to(ROOT).as_posix(),
        root=str(ROOT),
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
    manifest = _plan(config["_config_relative_path"], plan_path)

    compose_attempted = False
    try:
        compose_attempted = True
        _compose_up()
        _capacity_gate(config, dataset_manifest, artifact_root / "capacity-gate.json")
        _collector_calibration(artifact_root / "collector-calibration.json")
        medallion_path = artifact_root / "medallion.json"
        _prepare_medallion(config, medallion_path)
        executor = DockerCampaignExecutor(
            config=config,
            manifest=manifest,
            medallion_path=medallion_path,
            artifact_root=artifact_root,
            git_commit=commit,
            spark_image_id=_image_id("spark-worker"),
            secrets=sensitive_values(ROOT / ".env"),
        )
        report = CampaignRunner.from_schema_path(RAW_SCHEMA_PATH).run(
            manifest,
            ROOT / "results/raw",
            executor,
        )
        write_json(
            artifact_root / "campaign-verification.json",
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
                },
            },
        )
        if not report.complete:
            raise CampaignError("campaign did not complete successfully")
    finally:
        if compose_attempted and not args.keep_services:
            subprocess.run(["docker", "compose", "down"], cwd=ROOT, check=False)


if __name__ == "__main__":
    main()
