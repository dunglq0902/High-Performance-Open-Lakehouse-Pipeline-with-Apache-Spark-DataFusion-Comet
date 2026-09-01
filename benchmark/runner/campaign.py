"""Fail-closed campaign scheduling, resume, and subprocess timeout primitives.

The native Spark launcher is intentionally kept outside this module.  This module owns the
protocol invariants that must remain true regardless of whether runs are launched through Docker,
WSL, or a dedicated Linux runner: deterministic run identities, immutable raw records, strict
resume validation, and correctness/plan gates before measurements are admitted.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol

from jsonschema import Draft202012Validator, FormatChecker

from benchmark.runner.canonical import write_json

EngineName = Literal["spark_baseline", "comet_accelerated"]
RunPhase = Literal["correctness", "plan_capture", "measurement"]

_RETRYABLE_FAILURE_STATUSES = frozenset({"failed", "timeout"})
_NON_RETRYABLE_FAILURE_STATUSES = frozenset({"invalid_result", "invalid_environment"})


class CampaignError(ValueError):
    """A campaign cannot proceed without violating its immutable protocol."""


@dataclass(frozen=True, slots=True)
class CampaignRun:
    """One materialized Spark application in a campaign."""

    experiment_id: str
    run_id: str
    phase: RunPhase
    engine: EngineName
    pair_index: int | None
    order_index: int
    warmup_runs: int
    timeout_seconds: int

    def raw_path(self, raw_root: Path) -> Path:
        return raw_root / self.experiment_id / self.engine / f"{self.run_id}.json"


@dataclass(frozen=True, slots=True)
class CampaignReport:
    """Campaign counters after one invocation.

    ``executed`` counts planned run slots successfully executed and published during this
    invocation, not individual attempts. Consequently, every complete campaign satisfies
    ``executed + resumed == planned`` even when retries occurred. ``failed`` counts planned runs
    whose final attempt failed; intermediate failed attempts are evidence, not additional runs.
    """

    experiment_id: str
    planned: int
    executed: int
    resumed: int
    succeeded: int
    failed: int

    @property
    def complete(self) -> bool:
        return self.succeeded == self.planned and self.failed == 0


@dataclass(frozen=True, slots=True)
class ProcessOutcome:
    command: tuple[str, ...]
    return_code: int | None
    timed_out: bool
    wall_time_ms: float
    stdout_path: Path
    stderr_path: Path


class CampaignExecutor(Protocol):
    """Adapter implemented by the native runtime launcher."""

    def __call__(self, run: CampaignRun) -> Mapping[str, Any]: ...


class CampaignAttemptRecovery(Protocol):
    """Optional executor hook that closes admitted attempts interrupted before publication."""

    def recover_interrupted_attempt(
        self,
        run: CampaignRun,
        *,
        raw_path: Path,
        failure_root: Path,
    ) -> None: ...


RunProvenanceResolver = Callable[[CampaignRun], Mapping[str, Any]]

_RESUME_PROVENANCE_FIELDS = (
    "git_commit",
    "container_image_digest",
    "dataset_manifest_sha256",
    "spark_conf_sha256",
    "sql_sha256",
    "iceberg_snapshot_ids",
)
_RESUME_RESOURCE_FIELDS = (
    "cpu_model",
    "allocated_cores",
    "cgroup_memory_limit_mib",
    "executor_heap_mib",
    "off_heap_mib",
)


def _assert_complete_expected_provenance(expected_provenance: Mapping[str, Any]) -> None:
    missing = [field for field in _RESUME_PROVENANCE_FIELDS if field not in expected_provenance]
    if missing:
        raise CampaignError("current campaign provenance is incomplete: " + ", ".join(missing))
    resources = expected_provenance.get("resources")
    if not isinstance(resources, Mapping):
        raise CampaignError("current campaign provenance is missing resource identity")
    missing_resources = [field for field in _RESUME_RESOURCE_FIELDS if field not in resources]
    if missing_resources:
        raise CampaignError(
            "current campaign resource identity is incomplete: " + ", ".join(missing_resources)
        )


def _engine(value: object) -> EngineName:
    if value == "spark_baseline":
        return "spark_baseline"
    if value == "comet_accelerated":
        return "comet_accelerated"
    raise CampaignError(f"unknown campaign engine: {value!r}")


def plan_campaign(manifest: Mapping[str, Any]) -> tuple[CampaignRun, ...]:
    """Expand an immutable experiment manifest into deterministic run identities."""

    try:
        experiment_id = str(manifest["experiment_id"])
        config = manifest["resolved_config"]
        experiment = config["experiment"]
        timeout_seconds = int(experiment["timeout_seconds"])
        warmup_runs = int(experiment["warmup_runs"])
        engines = tuple(_engine(item["name"]) for item in config["matrix"]["engines"])
        schedule = manifest["schedule"]
    except (KeyError, TypeError, ValueError) as error:
        raise CampaignError("experiment manifest is missing campaign planning fields") from error

    if not experiment_id:
        raise CampaignError("experiment_id cannot be empty")
    if timeout_seconds < 1 or warmup_runs < 0:
        raise CampaignError("invalid timeout_seconds or warmup_runs")
    if set(engines) != {"spark_baseline", "comet_accelerated"} or len(engines) != 2:
        raise CampaignError("campaign requires exactly the baseline and Comet engines")
    if not isinstance(schedule, list) or not schedule:
        raise CampaignError("campaign schedule must be a non-empty array")

    runs: list[CampaignRun] = []
    gate_order: tuple[EngineName, EngineName] = ("spark_baseline", "comet_accelerated")
    gate_phases: tuple[tuple[RunPhase, str], ...] = (
        ("correctness", "correctness"),
        ("plan_capture", "plan"),
    )
    for phase, prefix in gate_phases:
        for order_index, engine in enumerate(gate_order, 1):
            runs.append(
                CampaignRun(
                    experiment_id=experiment_id,
                    run_id=f"{prefix}-{engine}",
                    phase=phase,
                    engine=engine,
                    pair_index=None,
                    order_index=order_index,
                    warmup_runs=0,
                    timeout_seconds=timeout_seconds,
                )
            )

    expected_pair_index = 1
    for raw_pair in schedule:
        if not isinstance(raw_pair, Mapping):
            raise CampaignError("campaign schedule entry must be an object")
        pair_index = raw_pair.get("pair_index")
        order = raw_pair.get("order")
        if pair_index != expected_pair_index:
            raise CampaignError("campaign pair indexes must be contiguous and start at one")
        if not isinstance(order, list) or len(order) != 2:
            raise CampaignError(f"pair {pair_index} must contain two engines")
        pair_engines = tuple(_engine(value) for value in order)
        if set(pair_engines) != set(gate_order):
            raise CampaignError(f"pair {pair_index} does not contain both engines")
        for order_index, engine in enumerate(pair_engines, 1):
            runs.append(
                CampaignRun(
                    experiment_id=experiment_id,
                    run_id=f"measurement-p{pair_index:04d}-o{order_index}-{engine}",
                    phase="measurement",
                    engine=engine,
                    pair_index=pair_index,
                    order_index=order_index,
                    warmup_runs=warmup_runs,
                    timeout_seconds=timeout_seconds,
                )
            )
        expected_pair_index += 1
    return tuple(runs)


def _record_errors(record: Mapping[str, Any], validator: Draft202012Validator) -> list[str]:
    return [
        f"{'/'.join(map(str, error.path)) or '<root>'}: {error.message}"
        for error in sorted(validator.iter_errors(record), key=lambda item: list(item.path))
    ]


def validate_run_record(
    record: Mapping[str, Any],
    run: CampaignRun,
    validator: Draft202012Validator,
    *,
    expected_provenance: Mapping[str, Any] | None = None,
) -> None:
    """Validate the raw schema plus fields bound by the campaign plan."""

    errors = _record_errors(record, validator)
    if errors:
        raise CampaignError("invalid raw run record:\n- " + "\n- ".join(errors))
    expected: dict[str, object] = {
        "experiment_id": run.experiment_id,
        "run_id": run.run_id,
        "phase": run.phase,
        "engine": run.engine,
        "pair_index": run.pair_index,
    }
    mismatches = [
        f"{field}={record.get(field)!r} (expected {value!r})"
        for field, value in expected.items()
        if record.get(field) != value
    ]
    if mismatches:
        raise CampaignError("raw record disagrees with planned run: " + "; ".join(mismatches))
    if expected_provenance is None:
        return
    _assert_complete_expected_provenance(expected_provenance)
    provenance = record["provenance"]
    provenance_mismatches = [
        f"{field}={provenance.get(field)!r} (expected {expected_provenance[field]!r})"
        for field in _RESUME_PROVENANCE_FIELDS
        if provenance.get(field) != expected_provenance[field]
    ]
    if provenance_mismatches:
        raise CampaignError(
            "raw record provenance disagrees with current campaign: "
            + "; ".join(provenance_mismatches)
        )
    expected_resources = expected_provenance["resources"]
    resource_mismatches = [
        f"{field}={record['resources'].get(field)!r} (expected {expected_resources[field]!r})"
        for field in _RESUME_RESOURCE_FIELDS
        if record["resources"].get(field) != expected_resources[field]
    ]
    if resource_mismatches:
        raise CampaignError(
            "raw record resource identity disagrees with current campaign: "
            + "; ".join(resource_mismatches)
        )


def _load_record(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise CampaignError(f"cannot resume invalid raw artifact {path}: {error}") from error
    if not isinstance(value, dict):
        raise CampaignError(f"raw artifact root must be an object: {path}")
    return value


def _failure_attempt_path(failure_root: Path, run: CampaignRun) -> Path:
    """Return the next immutable failed-attempt path for a planned run."""

    directory = failure_root / run.experiment_id / run.engine
    attempt = 1
    while True:
        candidate = directory / f"{run.run_id}-attempt-{attempt:04d}.json"
        if not candidate.exists():
            return candidate
        attempt += 1


def _assert_correctness_gate(records: Mapping[str, Mapping[str, Any]]) -> None:
    selected = [
        records.get("correctness-spark_baseline"),
        records.get("correctness-comet_accelerated"),
    ]
    if any(record is None for record in selected):
        raise CampaignError("measurement is blocked until both correctness runs exist")
    baseline, comet = selected
    assert baseline is not None and comet is not None
    if baseline.get("status") != "succeeded" or comet.get("status") != "succeeded":
        raise CampaignError("measurement is blocked by a failed correctness run")
    identity_fields = ("schema_sha256", "row_count", "canonical_result_sha256")
    for field in identity_fields:
        if baseline["correctness"].get(field) != comet["correctness"].get(field):
            raise CampaignError(f"Spark/Comet correctness mismatch: {field}")
    if baseline["provenance"].get("iceberg_snapshot_ids") != comet["provenance"].get(
        "iceberg_snapshot_ids"
    ):
        raise CampaignError("Spark/Comet correctness runs used different Iceberg snapshots")


def _assert_plan_gate(records: Mapping[str, Mapping[str, Any]]) -> None:
    baseline = records.get("plan-spark_baseline")
    comet = records.get("plan-comet_accelerated")
    if baseline is None or comet is None:
        raise CampaignError("measurement is blocked until both plan-capture runs exist")
    if baseline.get("status") != "succeeded" or comet.get("status") != "succeeded":
        raise CampaignError("measurement is blocked by a failed plan-capture run")
    incomplete = [
        name
        for name, record in (("spark_baseline", baseline), ("comet_accelerated", comet))
        if record["plan_analysis"].get("status") != "complete"
    ]
    if incomplete:
        raise CampaignError(
            "measurement is blocked by incomplete plan analysis: " + ", ".join(incomplete)
        )
    if baseline["plan_analysis"].get("comet_native_operators") != 0:
        raise CampaignError("baseline plan unexpectedly contains a Comet native operator")
    if int(comet["plan_analysis"].get("comet_native_operators", 0)) < 1:
        raise CampaignError("Comet plan contains no native operator")


class CampaignRunner:
    """Execute or resume a manifest while preserving every raw artifact."""

    def __init__(self, raw_schema: Mapping[str, Any]) -> None:
        self._validator = Draft202012Validator(raw_schema, format_checker=FormatChecker())

    @classmethod
    def from_schema_path(cls, path: Path) -> CampaignRunner:
        schema = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(schema, dict):
            raise CampaignError("raw-result schema root must be an object")
        return cls(schema)

    def run(
        self,
        manifest: Mapping[str, Any],
        raw_root: Path,
        executor: CampaignExecutor,
        *,
        continue_on_failure: bool = False,
        expected_provenance: RunProvenanceResolver | None = None,
        failure_root: Path | None = None,
        max_attempts: int = 1,
    ) -> CampaignReport:
        """Execute a campaign with bounded attempts and immutable publication.

        The default single-attempt mode retains the original behavior, including publishing a
        failed terminal record to ``raw_root``. Retry mode requires ``failure_root`` so every
        failed attempt is preserved outside ``raw_root`` while only a succeeded record is
        published as a resumable raw result. Only transient ``failed`` and ``timeout`` statuses
        are retried; invalid results and environments are preserved once and hard-stop the
        campaign.
        """

        if isinstance(max_attempts, bool) or not isinstance(max_attempts, int) or max_attempts < 1:
            raise CampaignError("max_attempts must be an integer greater than or equal to one")
        if max_attempts > 1 and failure_root is None:
            raise CampaignError("failure_root is required when max_attempts is greater than one")
        if failure_root is not None:
            resolved_raw_root = raw_root.resolve()
            resolved_failure_root = failure_root.resolve()
            if resolved_failure_root == resolved_raw_root or resolved_failure_root.is_relative_to(
                resolved_raw_root
            ):
                raise CampaignError("failure_root must be outside raw_root")

        plan = plan_campaign(manifest)
        records: dict[str, Mapping[str, Any]] = {}
        executed = 0
        resumed = 0

        expected_paths = {run.raw_path(raw_root): run for run in plan}
        experiment_root = raw_root / plan[0].experiment_id
        existing_files = (
            {path for path in experiment_root.rglob("*") if path.is_file()}
            if experiment_root.exists()
            else set()
        )
        unexpected_files = sorted(existing_files - expected_paths.keys())
        if unexpected_files:
            rendered = ", ".join(path.as_posix() for path in unexpected_files)
            raise CampaignError(f"raw campaign directory contains unexpected artifacts: {rendered}")
        existing_paths = {run.run_id: path for path, run in expected_paths.items() if path.exists()}
        if existing_paths and expected_provenance is None:
            raise CampaignError(
                "resume requires current campaign provenance before existing raw artifacts "
                "can be used"
            )
        expected_by_run = (
            {run.run_id: dict(expected_provenance(run)) for run in plan}
            if expected_provenance is not None
            else {}
        )
        for provenance in expected_by_run.values():
            _assert_complete_expected_provenance(provenance)
        recovery = getattr(executor, "recover_interrupted_attempt", None)
        if failure_root is not None and callable(recovery):
            if expected_provenance is None:
                raise CampaignError(
                    "attempt recovery requires current campaign provenance before interrupted "
                    "evidence can be closed"
                )
            for run in plan:
                recovery(
                    run,
                    raw_path=run.raw_path(raw_root),
                    failure_root=failure_root,
                )
        failure_history: dict[str, tuple[Mapping[str, Any], ...]] = {}
        if failure_root is not None:
            expected_failure_paths: set[Path] = set()
            for run in plan:
                directory = failure_root / run.experiment_id / run.engine
                paths = tuple(sorted(directory.glob(f"{run.run_id}-attempt-*.json")))
                expected_failure_paths.update(paths)
                records_for_run: list[Mapping[str, Any]] = []
                for attempt_index, failure_path in enumerate(paths, 1):
                    if failure_path.name != f"{run.run_id}-attempt-{attempt_index:04d}.json":
                        raise CampaignError(
                            f"failed-attempt sequence is not contiguous: {failure_path}"
                        )
                    failure_record = _load_record(failure_path)
                    validate_run_record(
                        failure_record,
                        run,
                        self._validator,
                        expected_provenance=expected_by_run.get(run.run_id),
                    )
                    if failure_record.get("status") == "succeeded":
                        raise CampaignError(
                            f"failed-attempt history contains a successful record: {failure_path}"
                        )
                    records_for_run.append(failure_record)
                failure_history[run.run_id] = tuple(records_for_run)
            failure_campaign_root = failure_root / plan[0].experiment_id
            actual_failure_paths = (
                {path for path in failure_campaign_root.rglob("*.json") if path.is_file()}
                if failure_campaign_root.is_dir()
                else set()
            )
            if actual_failure_paths and expected_provenance is None:
                raise CampaignError(
                    "resume requires current campaign provenance before failed-attempt history "
                    "can be used"
                )
            unexpected_failure_paths = sorted(actual_failure_paths - expected_failure_paths)
            if unexpected_failure_paths:
                rendered = ", ".join(path.as_posix() for path in unexpected_failure_paths)
                raise CampaignError(
                    f"failed-attempt directory contains unexpected artifacts: {rendered}"
                )
        resumable_records: dict[str, Mapping[str, Any]] = {}
        for run in plan:
            path = existing_paths.get(run.run_id)
            if path is None:
                continue
            existing_record = _load_record(path)
            validate_run_record(
                existing_record,
                run,
                self._validator,
                expected_provenance=expected_by_run[run.run_id],
            )
            if failure_root is not None and existing_record.get("status") != "succeeded":
                raise CampaignError(f"retry-enabled raw artifact is not successful: {path}")
            resumable_records[run.run_id] = existing_record

        for run in plan:
            prior_non_retryable = [
                record.get("status")
                for record in failure_history.get(run.run_id, ())
                if record.get("status") in _NON_RETRYABLE_FAILURE_STATUSES
            ]
            if prior_non_retryable:
                raise CampaignError(
                    f"campaign remains hard-stopped by prior non-retryable status for "
                    f"{run.run_id}: {prior_non_retryable[-1]!r}"
                )
            if run.phase == "measurement":
                _assert_correctness_gate(records)
                _assert_plan_gate(records)

            path = run.raw_path(raw_root)
            if run.run_id in resumable_records:
                record = resumable_records[run.run_id]
                resumed += 1
            else:
                prior_failures = failure_history.get(run.run_id, ())
                remaining_attempts = max_attempts - len(prior_failures)
                record = dict(prior_failures[-1]) if prior_failures else {}
                for _attempt in range(remaining_attempts):
                    record = dict(executor(run))
                    validate_run_record(
                        record,
                        run,
                        self._validator,
                        expected_provenance=expected_by_run.get(run.run_id),
                    )
                    status = record.get("status")
                    if status == "succeeded":
                        write_json(path, record)
                        executed += 1
                        break
                    if failure_root is None:
                        write_json(path, record)
                    else:
                        write_json(_failure_attempt_path(failure_root, run), record)
                    if failure_root is not None and status in _NON_RETRYABLE_FAILURE_STATUSES:
                        raise CampaignError(
                            f"campaign hard-stopped after {run.run_id} produced non-retryable "
                            f"status={status!r}; max_attempts does not apply"
                        )
                    if status not in _RETRYABLE_FAILURE_STATUSES:
                        break
            records[run.run_id] = record
            if record.get("status") != "succeeded" and not continue_on_failure:
                raise CampaignError(
                    f"campaign stopped after {run.run_id} exhausted its global budget of "
                    f"{max_attempts} attempt(s); "
                    f"status={record.get('status')!r}"
                )

        succeeded = sum(record.get("status") == "succeeded" for record in records.values())
        return CampaignReport(
            experiment_id=plan[0].experiment_id,
            planned=len(plan),
            executed=executed,
            resumed=resumed,
            succeeded=succeeded,
            failed=len(records) - succeeded,
        )


def _terminate_process(process: subprocess.Popen[bytes], grace_seconds: float) -> None:
    if process.poll() is not None:
        return
    try:
        if os.name == "nt":
            process.terminate()
        else:
            os.killpg(process.pid, signal.SIGTERM)
        process.wait(timeout=grace_seconds)
    except (OSError, subprocess.TimeoutExpired):
        try:
            if os.name == "nt":
                process.kill()
            else:
                os.killpg(process.pid, signal.SIGKILL)
        except OSError:
            pass
        process.wait()


def run_subprocess(
    command: Sequence[str],
    *,
    timeout_seconds: float,
    stdout_path: Path,
    stderr_path: Path,
    cwd: Path | None = None,
    environment: Mapping[str, str] | None = None,
    clock_ns: Callable[[], int] = time.perf_counter_ns,
    termination_grace_seconds: float = 5.0,
) -> ProcessOutcome:
    """Run one application with immutable logs and a hard timeout.

    A new process group/session prevents a timed-out Spark launcher from leaving child processes
    alive.  Existing logs are rejected so resume can never overwrite evidence from an earlier
    attempt.
    """

    if not command or timeout_seconds <= 0 or termination_grace_seconds < 0:
        raise ValueError("command, timeout_seconds, and grace period must be valid")
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    stderr_path.parent.mkdir(parents=True, exist_ok=True)
    started = clock_ns()
    timed_out = False
    creationflags = (
        int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)) if os.name == "nt" else 0
    )
    with stdout_path.open("xb") as stdout, stderr_path.open("xb") as stderr:
        process = subprocess.Popen(
            list(command),
            cwd=cwd,
            env=None if environment is None else dict(environment),
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
            start_new_session=os.name != "nt",
            creationflags=creationflags,
        )
        try:
            process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            timed_out = True
            _terminate_process(process, termination_grace_seconds)
    elapsed_ms = (clock_ns() - started) / 1_000_000
    return ProcessOutcome(
        command=tuple(command),
        return_code=process.returncode,
        timed_out=timed_out,
        wall_time_ms=elapsed_ms,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
    )
