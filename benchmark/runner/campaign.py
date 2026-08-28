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


def _load_record(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise CampaignError(f"cannot resume invalid raw artifact {path}: {error}") from error
    if not isinstance(value, dict):
        raise CampaignError(f"raw artifact root must be an object: {path}")
    return value


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
    ) -> CampaignReport:
        plan = plan_campaign(manifest)
        records: dict[str, Mapping[str, Any]] = {}
        executed = 0
        resumed = 0

        for run in plan:
            if run.phase == "measurement":
                _assert_correctness_gate(records)
                _assert_plan_gate(records)

            path = run.raw_path(raw_root)
            if path.exists():
                record = _load_record(path)
                validate_run_record(record, run, self._validator)
                resumed += 1
            else:
                record = dict(executor(run))
                validate_run_record(record, run, self._validator)
                write_json(path, record)
                executed += 1
            records[run.run_id] = record
            if record.get("status") != "succeeded" and not continue_on_failure:
                raise CampaignError(
                    f"campaign stopped after {run.run_id} status={record.get('status')!r}"
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
