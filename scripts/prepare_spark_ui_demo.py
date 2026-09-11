"""Stage one traceable Spark/Comet pair for a Spark History Server demo."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from benchmark.runner.evidence import (
    ArtifactEvidenceError,
    artifact_evidence,
    control_artifact_evidence,
    raw_records_sha256,
)

ROOT = Path(__file__).resolve().parents[1]

_ENGINES = ("spark_baseline", "comet_accelerated")
_APP_ID_PATTERN = re.compile(r"(?:^|_)(app-[A-Za-z0-9-]+)$")
_MANIFEST_SCHEMA_VERSION = 2
_HISTORY_SERVER_URL = "http://127.0.0.1:18080"
_SQL_START_EVENT = "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart"
_SQL_END_EVENT = "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd"


class DemoPreparationError(ValueError):
    """The requested Spark UI demo cannot be tied to admissible evidence."""


@dataclass(frozen=True, slots=True)
class _EventLogInspection:
    application_id: str
    application_name: str
    event_count: int
    sql_execution_count: int
    application_start_time_ms: int
    application_end_time_ms: int
    measured_execution_id: int
    measured_execution_description: str
    measured_execution_start_time_ms: int
    measured_execution_end_time_ms: int
    measured_execution_duration_ms: int
    inventory: tuple[dict[str, object], ...]


@dataclass(frozen=True, slots=True)
class _SelectedRun:
    engine: str
    path: Path
    record: dict[str, Any]
    event_log: Path
    inspection: _EventLogInspection


def _load_object(path: Path, *, label: str) -> dict[str, Any]:
    if not path.is_file() or path.is_symlink():
        raise DemoPreparationError(f"{label} must be a regular file: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise DemoPreparationError(f"cannot read {label} {path}: {error}") from error
    if not isinstance(value, dict):
        raise DemoPreparationError(f"{label} root must be an object: {path}")
    return value


def _relative_to_root(path: Path, repository_root: Path, *, label: str) -> str:
    try:
        return path.resolve().relative_to(repository_root.resolve()).as_posix()
    except ValueError as error:
        raise DemoPreparationError(f"{label} escapes repository root: {path}") from error


def _resolve_artifact_path(
    repository_root: Path,
    value: object,
    *,
    label: str,
) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise DemoPreparationError(f"{label} must be a non-empty repository-relative path")
    candidate = Path(value)
    if candidate.is_absolute():
        raise DemoPreparationError(f"{label} must be repository-relative: {value}")
    supplied = repository_root / candidate
    if supplied.is_symlink():
        raise DemoPreparationError(f"{label} cannot be a symbolic link: {value}")
    resolved = supplied.resolve()
    _relative_to_root(resolved, repository_root, label=label)
    return resolved


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _directory_inventory(root: Path) -> list[dict[str, object]]:
    if not root.is_dir() or root.is_symlink():
        raise DemoPreparationError(f"event log must be a regular directory: {root}")
    entries: list[dict[str, object]] = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise DemoPreparationError(f"event log cannot contain symbolic links: {path}")
        if path.is_dir():
            continue
        if not path.is_file():
            raise DemoPreparationError(f"event log contains unsupported path: {path}")
        entries.append(
            {
                "path": path.relative_to(root).as_posix(),
                "size_bytes": path.stat().st_size,
                "sha256": _sha256_file(path),
            }
        )
    if not entries:
        raise DemoPreparationError(f"event log directory is empty: {root}")
    return entries


def _event_file_application_id(path: Path) -> str | None:
    if path.name.startswith(".") or path.suffix == ".crc":
        return None
    match = _APP_ID_PATTERN.search(path.name)
    return match.group(1) if match else None


def _integer(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise DemoPreparationError(f"{label} must be an integer")
    return value


def _inspect_event_log(root: Path, *, expected_run_id: str) -> _EventLogInspection:
    inventory = _directory_inventory(root)
    files = [root / str(entry["path"]) for entry in inventory]
    event_files = [path for path in files if path.name.startswith("events_")]
    status_files = [path for path in files if path.name.startswith("appstatus_")]
    if not event_files:
        raise DemoPreparationError(f"rolling Spark event log has no events_* segment: {root}")
    if not status_files:
        raise DemoPreparationError(f"rolling Spark event log has no appstatus_* marker: {root}")

    application_ids = {
        application_id
        for path in event_files + status_files
        if (application_id := _event_file_application_id(path)) is not None
    }
    if len(application_ids) != 1:
        raise DemoPreparationError(
            f"rolling Spark event log must contain exactly one application id: {root}"
        )
    application_id = next(iter(application_ids))

    application_name: str | None = None
    start_time: int | None = None
    end_time: int | None = None
    event_count = 0
    sql_execution_starts: dict[int, tuple[str, int]] = {}
    sql_execution_ends: dict[int, tuple[int, object]] = {}
    for path in sorted(event_files):
        try:
            with path.open(encoding="utf-8") as handle:
                for line_number, line in enumerate(handle, start=1):
                    if not line.strip():
                        continue
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError as error:
                        raise DemoPreparationError(
                            f"invalid JSON event {path}:{line_number}: {error}"
                        ) from error
                    if not isinstance(event, dict):
                        raise DemoPreparationError(
                            f"event root must be an object at {path}:{line_number}"
                        )
                    event_count += 1
                    event_type = event.get("Event")
                    if event_type == "SparkListenerApplicationStart":
                        observed_id = event.get("App ID")
                        observed_name = event.get("App Name")
                        if observed_id != application_id:
                            raise DemoPreparationError(
                                f"event-log application id mismatch in {path}: {observed_id!r}"
                            )
                        if not isinstance(observed_name, str) or not observed_name.strip():
                            raise DemoPreparationError(
                                f"event log has an invalid application name: {path}"
                            )
                        if application_name not in (None, observed_name):
                            raise DemoPreparationError(
                                f"event log has conflicting application names: {root}"
                            )
                        application_name = observed_name
                        start_time = _integer(
                            event.get("Timestamp"), label="application start time"
                        )
                    elif event_type == "SparkListenerApplicationEnd":
                        end_time = _integer(event.get("Timestamp"), label="application end time")
                    elif event_type == _SQL_START_EVENT:
                        execution_id = _integer(event.get("executionId"), label="SQL execution id")
                        description = event.get("description")
                        if not isinstance(description, str) or not description:
                            raise DemoPreparationError(
                                f"SQL execution {execution_id} has no description: {root}"
                            )
                        execution_start = _integer(
                            event.get("time"), label="SQL execution start time"
                        )
                        if execution_id in sql_execution_starts:
                            raise DemoPreparationError(
                                f"event log contains duplicate SQL execution start "
                                f"{execution_id}: {root}"
                            )
                        sql_execution_starts[execution_id] = (description, execution_start)
                    elif event_type == _SQL_END_EVENT:
                        execution_id = _integer(event.get("executionId"), label="SQL execution id")
                        execution_end = _integer(event.get("time"), label="SQL execution end time")
                        if execution_id in sql_execution_ends:
                            raise DemoPreparationError(
                                f"event log contains duplicate SQL execution end "
                                f"{execution_id}: {root}"
                            )
                        sql_execution_ends[execution_id] = (
                            execution_end,
                            event.get("executionFailure"),
                        )
        except (OSError, UnicodeError) as error:
            raise DemoPreparationError(
                f"cannot inspect event log segment {path}: {error}"
            ) from error

    if application_name is None or start_time is None:
        raise DemoPreparationError(f"event log has no SparkListenerApplicationStart: {root}")
    if end_time is None:
        raise DemoPreparationError(f"event log has no SparkListenerApplicationEnd: {root}")
    if end_time < start_time:
        raise DemoPreparationError(f"event log application end precedes start: {root}")
    if not sql_execution_starts:
        raise DemoPreparationError(f"event log contains no SQL execution: {root}")
    measured_description = f"measured terminal action for {expected_run_id}"
    measured_ids = [
        execution_id
        for execution_id, (description, _) in sql_execution_starts.items()
        if description == measured_description
    ]
    if len(measured_ids) != 1:
        raise DemoPreparationError(
            "event log must contain exactly one measured SQL execution described as "
            f"{measured_description!r}: {root}"
        )
    measured_execution_id = measured_ids[0]
    measured_end = sql_execution_ends.get(measured_execution_id)
    if measured_end is None:
        raise DemoPreparationError(
            f"measured SQL execution {measured_execution_id} has no matching end: {root}"
        )
    measured_start_time = sql_execution_starts[measured_execution_id][1]
    measured_end_time, measured_failure = measured_end
    if measured_end_time < measured_start_time:
        raise DemoPreparationError(
            f"measured SQL execution {measured_execution_id} ends before it starts: {root}"
        )
    if measured_start_time < start_time or measured_end_time > end_time:
        raise DemoPreparationError(
            f"measured SQL execution {measured_execution_id} lies outside the application "
            f"lifetime: {root}"
        )
    if measured_failure not in (None, ""):
        raise DemoPreparationError(
            f"measured SQL execution {measured_execution_id} records a failure: {root}"
        )
    return _EventLogInspection(
        application_id=application_id,
        application_name=application_name,
        event_count=event_count,
        sql_execution_count=len(sql_execution_starts),
        application_start_time_ms=start_time,
        application_end_time_ms=end_time,
        measured_execution_id=measured_execution_id,
        measured_execution_description=measured_description,
        measured_execution_start_time_ms=measured_start_time,
        measured_execution_end_time_ms=measured_end_time,
        measured_execution_duration_ms=measured_end_time - measured_start_time,
        inventory=tuple(inventory),
    )


def _report_gate(
    path: Path,
    *,
    allow_diagnostic: bool,
) -> tuple[dict[str, Any], str]:
    report = _load_object(path, label="report publishability artifact")
    contract = report.get("report_contract")
    if not isinstance(contract, Mapping) or contract.get("passed") is not True:
        raise DemoPreparationError("report content contract must pass before staging a demo")
    publishable = report.get("publishable") is True and report.get("status") == "passed"
    if not publishable and not allow_diagnostic:
        raise DemoPreparationError(
            "report is not publishable; rerun with fresh admitted evidence or use "
            "--allow-diagnostic for a clearly labelled rehearsal"
        )
    return report, "publishable" if publishable else "diagnostic"


def _select_runs(
    raw_root: Path,
    repository_root: Path,
    *,
    experiment_id: str,
    pair_index: int,
) -> tuple[_SelectedRun, _SelectedRun, list[dict[str, Any]]]:
    selected: dict[str, _SelectedRun] = {}
    campaign_records: list[dict[str, Any]] = []
    for path in sorted(raw_root.rglob("*.json")):
        record = _load_object(path, label="raw result")
        if record.get("experiment_id") == experiment_id:
            campaign_records.append(record)
        if (
            record.get("experiment_id") != experiment_id
            or record.get("phase") != "measurement"
            or record.get("pair_index") != pair_index
        ):
            continue
        engine = record.get("engine")
        if engine not in _ENGINES:
            continue
        if engine in selected:
            raise DemoPreparationError(
                f"multiple {engine} records found for {experiment_id} pair {pair_index}"
            )
        if record.get("status") != "succeeded":
            raise DemoPreparationError(f"selected {engine} record did not succeed: {path}")
        run_id = record.get("run_id")
        if not isinstance(run_id, str) or not run_id:
            raise DemoPreparationError(f"selected {engine} record has no run ID: {path}")
        correctness = record.get("correctness")
        if not isinstance(correctness, Mapping) or correctness.get("status") != "passed":
            raise DemoPreparationError(f"selected {engine} record failed correctness: {path}")
        artifacts = record.get("artifacts")
        if not isinstance(artifacts, Mapping):
            raise DemoPreparationError(f"selected {engine} record has no artifact map: {path}")
        event_log = _resolve_artifact_path(
            repository_root,
            artifacts.get("event_log"),
            label=f"{engine} event log",
        )
        selected[engine] = _SelectedRun(
            engine=engine,
            path=path.resolve(),
            record=record,
            event_log=event_log,
            inspection=_inspect_event_log(event_log, expected_run_id=run_id),
        )

    missing = [engine for engine in _ENGINES if engine not in selected]
    if missing:
        raise DemoPreparationError(
            f"missing measurement record(s) for {experiment_id} pair {pair_index}: "
            + ", ".join(missing)
        )

    baseline = selected["spark_baseline"]
    comet = selected["comet_accelerated"]
    for field in ("experiment_id", "workload", "query_id", "storage_profile", "pair_index"):
        if baseline.record.get(field) != comet.record.get(field):
            raise DemoPreparationError(f"selected pair disagrees on {field}")
    for field in ("git_commit", "dataset_manifest_sha256", "sql_sha256", "iceberg_snapshot_ids"):
        baseline_provenance = baseline.record.get("provenance")
        comet_provenance = comet.record.get("provenance")
        if not isinstance(baseline_provenance, Mapping) or not isinstance(
            comet_provenance, Mapping
        ):
            raise DemoPreparationError("selected pair has invalid provenance")
        if baseline_provenance.get(field) != comet_provenance.get(field):
            raise DemoPreparationError(f"selected pair disagrees on provenance.{field}")
    for field in ("schema_sha256", "row_count", "canonical_result_sha256"):
        baseline_correctness = baseline.record["correctness"]
        comet_correctness = comet.record["correctness"]
        if not isinstance(baseline_correctness, Mapping) or not isinstance(
            comet_correctness, Mapping
        ):
            raise DemoPreparationError("selected pair has invalid correctness evidence")
        if baseline_correctness.get(field) != comet_correctness.get(field):
            raise DemoPreparationError(f"selected pair disagrees on correctness.{field}")
    if baseline.inspection.application_id == comet.inspection.application_id:
        raise DemoPreparationError("baseline and Comet event logs reuse the same application id")
    for run in (baseline, comet):
        metrics = run.record.get("metrics")
        if not isinstance(metrics, Mapping):
            raise DemoPreparationError(f"selected {run.engine} record has no metrics")
        measured_duration = _integer(
            metrics.get("sql_execution_time_ms"),
            label=f"{run.engine} metrics.sql_execution_time_ms",
        )
        if measured_duration != run.inspection.measured_execution_duration_ms:
            raise DemoPreparationError(
                f"selected {run.engine} SQL duration does not match its measured event-log "
                "execution"
            )
    return baseline, comet, campaign_records


def _verify_report_campaign_binding(
    report: Mapping[str, Any],
    *,
    experiment_id: str,
    campaign_records: list[dict[str, Any]],
) -> None:
    checks = report.get("checks")
    if not isinstance(checks, Mapping):
        raise DemoPreparationError("report publishability artifact has no checks")
    values = checks.get("campaign_records")
    if not isinstance(values, list):
        raise DemoPreparationError("report publishability artifact has no campaign record checks")
    matches = [
        value
        for value in values
        if isinstance(value, Mapping) and value.get("experiment_id") == experiment_id
    ]
    if len(matches) != 1:
        raise DemoPreparationError(
            f"report must contain exactly one campaign record check for {experiment_id}"
        )
    check = matches[0]
    if check.get("passed") is not True:
        raise DemoPreparationError(f"report campaign record check did not pass for {experiment_id}")
    observed_hash = raw_records_sha256(campaign_records)
    if check.get("raw_records_sha256") != observed_hash:
        raise DemoPreparationError(
            f"raw records for {experiment_id} no longer match the report publishability artifact"
        )


def _verify_report_campaign_evidence(
    report: Mapping[str, Any],
    *,
    experiment_id: str,
    campaign_records: list[dict[str, Any]],
    repository_root: Path,
    mode: str,
) -> None:
    """Recompute campaign artifacts and controls recorded by the report gate."""

    checks = report.get("checks")
    if not isinstance(checks, Mapping):
        raise DemoPreparationError("report publishability artifact has no checks")
    values = checks.get("campaign_verifications")
    if not isinstance(values, list):
        raise DemoPreparationError(
            "report publishability artifact has no campaign verification checks"
        )
    matches = [
        value
        for value in values
        if isinstance(value, Mapping) and value.get("experiment_id") == experiment_id
    ]
    if len(matches) != 1:
        raise DemoPreparationError(
            f"report must contain exactly one campaign verification for {experiment_id}"
        )
    verification = matches[0]
    if mode == "publishable" and verification.get("passed") is not True:
        raise DemoPreparationError(f"report campaign verification did not pass for {experiment_id}")
    if verification.get("attempt_counts_verified") is not True:
        raise DemoPreparationError(
            f"report campaign attempt controls were not verified for {experiment_id}"
        )

    declared_artifacts = verification.get("artifact_evidence")
    if not isinstance(declared_artifacts, Mapping):
        raise DemoPreparationError(
            f"report campaign artifact evidence is missing for {experiment_id}"
        )
    try:
        observed_artifacts = artifact_evidence(campaign_records, repository_root)
    except (ArtifactEvidenceError, OSError) as error:
        raise DemoPreparationError(
            f"cannot reconstruct campaign artifact evidence for {experiment_id}: {error}"
        ) from error
    if dict(declared_artifacts) != observed_artifacts:
        raise DemoPreparationError(
            f"campaign artifacts for {experiment_id} no longer match the report"
        )

    declared_controls = verification.get("control_artifact_evidence")
    if not isinstance(declared_controls, Mapping):
        raise DemoPreparationError(
            f"report campaign control evidence is missing for {experiment_id}"
        )
    raw_targets = declared_controls.get("targets")
    if not isinstance(raw_targets, list) or not raw_targets:
        raise DemoPreparationError(
            f"report campaign control targets are missing for {experiment_id}"
        )
    targets: dict[str, Path] = {}
    for index, raw_target in enumerate(raw_targets):
        if not isinstance(raw_target, Mapping) or set(raw_target) != {
            "label",
            "path",
            "kind",
        }:
            raise DemoPreparationError(
                f"report campaign control target {index} is invalid for {experiment_id}"
            )
        label = raw_target.get("label")
        kind = raw_target.get("kind")
        if not isinstance(label, str) or not label or label in targets:
            raise DemoPreparationError(
                f"report campaign control target label is invalid for {experiment_id}"
            )
        if kind not in {"file", "directory"}:
            raise DemoPreparationError(
                f"report campaign control target kind is invalid for {experiment_id}/{label}"
            )
        target = _resolve_artifact_path(
            repository_root,
            raw_target.get("path"),
            label=f"{experiment_id} control target {label}",
        )
        if (kind == "file" and not target.is_file()) or (
            kind == "directory" and not target.is_dir()
        ):
            raise DemoPreparationError(
                f"report campaign control target no longer has kind {kind}: {experiment_id}/{label}"
            )
        targets[label] = target
    try:
        observed_controls = control_artifact_evidence(targets, repository_root)
    except (ArtifactEvidenceError, OSError) as error:
        raise DemoPreparationError(
            f"cannot reconstruct campaign control evidence for {experiment_id}: {error}"
        ) from error
    if dict(declared_controls) != observed_controls:
        raise DemoPreparationError(
            f"campaign controls for {experiment_id} no longer match the report"
        )


def _write_json(path: Path, value: object) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def _run_manifest(
    run: _SelectedRun,
    repository_root: Path,
    staged_root: Path,
) -> dict[str, object]:
    record = run.record
    metrics = record.get("metrics")
    plan = record.get("plan_analysis")
    provenance = record.get("provenance")
    if (
        not isinstance(metrics, Mapping)
        or not isinstance(plan, Mapping)
        or not isinstance(provenance, Mapping)
    ):
        raise DemoPreparationError(f"selected {run.engine} record lacks reportable evidence")
    staged_name = f"eventlog_v2_{run.inspection.application_id}"
    source_inventory = _directory_inventory(run.event_log)
    inspected_inventory = list(run.inspection.inventory)
    staged_inventory = _directory_inventory(staged_root / staged_name)
    if source_inventory != inspected_inventory:
        raise DemoPreparationError(
            f"source event log changed after inspection for {run.record['run_id']}"
        )
    if staged_inventory != source_inventory:
        raise DemoPreparationError(
            f"staged event log differs from its source for {run.record['run_id']}"
        )
    measured_execution_url = (
        f"{_HISTORY_SERVER_URL}/history/{run.inspection.application_id}/"
        f"SQL/execution/?id={run.inspection.measured_execution_id}"
    )
    return {
        "engine": run.engine,
        "run_id": record["run_id"],
        "raw_record": _relative_to_root(run.path, repository_root, label="raw record"),
        "raw_record_sha256": _sha256_file(run.path),
        "source_event_log": _relative_to_root(
            run.event_log, repository_root, label="source event log"
        ),
        "source_event_log_inventory": source_inventory,
        "staged_event_log": (Path("event-logs") / staged_name).as_posix(),
        "staged_event_log_inventory": staged_inventory,
        "application_id": run.inspection.application_id,
        "application_name": run.inspection.application_name,
        "event_count": run.inspection.event_count,
        "sql_execution_count": run.inspection.sql_execution_count,
        "application_start_time_ms": run.inspection.application_start_time_ms,
        "application_end_time_ms": run.inspection.application_end_time_ms,
        "measured_sql_execution_id": run.inspection.measured_execution_id,
        "measured_sql_execution_description": run.inspection.measured_execution_description,
        "measured_sql_execution_start_time_ms": (run.inspection.measured_execution_start_time_ms),
        "measured_sql_execution_end_time_ms": run.inspection.measured_execution_end_time_ms,
        "measured_sql_execution_duration_ms": run.inspection.measured_execution_duration_ms,
        "measured_sql_execution_url": measured_execution_url,
        "query_wall_time_ms": metrics.get("query_wall_time_ms"),
        "sql_execution_time_ms": metrics.get("sql_execution_time_ms"),
        "spark_conf_sha256": provenance.get("spark_conf_sha256"),
        "native_coverage_ratio": plan.get("native_coverage_ratio"),
        "native_operator_count": plan.get("comet_native_operators"),
        "fallback_operator_count": plan.get("spark_fallback_operators"),
        "transition_count": plan.get("transition_count"),
    }


def _existing_bundle_matches(path: Path, manifest: Mapping[str, object]) -> bool:
    manifest_path = path / "demo-manifest.json"
    if not manifest_path.is_file() or manifest_path.is_symlink():
        return False
    try:
        observed = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return False
    if observed != manifest:
        return False
    applications = observed.get("applications") if isinstance(observed, Mapping) else None
    if not isinstance(applications, list) or len(applications) != len(_ENGINES):
        return False
    event_log_root = path / "event-logs"
    if not event_log_root.is_dir() or event_log_root.is_symlink():
        return False
    expected_directories: set[str] = set()
    try:
        for application in applications:
            if not isinstance(application, Mapping):
                return False
            declared = application.get("staged_event_log")
            declared_inventory = application.get("staged_event_log_inventory")
            source_inventory = application.get("source_event_log_inventory")
            if (
                not isinstance(declared, str)
                or not declared
                or not isinstance(declared_inventory, list)
                or declared_inventory != source_inventory
            ):
                return False
            relative = Path(declared)
            if (
                relative.is_absolute()
                or ".." in relative.parts
                or len(relative.parts) != 2
                or relative.parts[0] != "event-logs"
            ):
                return False
            staged = path / relative
            if staged.resolve().parent != event_log_root.resolve():
                return False
            expected_directories.add(staged.name)
            if _directory_inventory(staged) != declared_inventory:
                return False
        observed_entries = list(event_log_root.iterdir())
        if any(not entry.is_dir() or entry.is_symlink() for entry in observed_entries):
            return False
        if (
            len(expected_directories) != len(applications)
            or {entry.name for entry in observed_entries} != expected_directories
        ):
            return False
    except (DemoPreparationError, OSError, RuntimeError):
        return False
    return True


def prepare_spark_ui_demo(
    *,
    raw_root: Path,
    report_publishability: Path,
    output_root: Path,
    experiment_id: str,
    pair_index: int,
    allow_diagnostic: bool = False,
    repository_root: Path = ROOT,
) -> Path:
    """Create an immutable two-application event-log bundle and activate it for Compose."""

    if pair_index < 1:
        raise DemoPreparationError("pair index must be positive")
    repository_root = repository_root.resolve()
    raw_root = raw_root.resolve()
    output_root = output_root.resolve()
    _relative_to_root(raw_root, repository_root, label="raw root")
    _relative_to_root(output_root, repository_root, label="demo output root")
    report, mode = _report_gate(report_publishability.resolve(), allow_diagnostic=allow_diagnostic)
    baseline, comet, campaign_records = _select_runs(
        raw_root,
        repository_root,
        experiment_id=experiment_id,
        pair_index=pair_index,
    )
    _verify_report_campaign_binding(
        report,
        experiment_id=experiment_id,
        campaign_records=campaign_records,
    )
    _verify_report_campaign_evidence(
        report,
        experiment_id=experiment_id,
        campaign_records=campaign_records,
        repository_root=repository_root,
        mode=mode,
    )

    provenance = baseline.record.get("provenance")
    correctness = baseline.record.get("correctness")
    if not isinstance(provenance, Mapping) or not isinstance(correctness, Mapping):
        raise DemoPreparationError("selected pair has invalid evidence maps")
    commit = provenance.get("git_commit")
    if not isinstance(commit, str) or not commit:
        raise DemoPreparationError("selected pair has no Git commit")
    bundle_id = (
        f"{experiment_id.lower()}-p{pair_index:04d}-{commit[:12]}-{mode}"
        f"-v{_MANIFEST_SCHEMA_VERSION}"
    )
    bundle_root = output_root / "bundles" / bundle_id
    output_root.mkdir(parents=True, exist_ok=True)

    temporary_parent = output_root / ".staging"
    temporary_parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f"{bundle_id}-", dir=temporary_parent))
    temporary_published = False
    try:
        staged_event_logs = temporary / "event-logs"
        staged_event_logs.mkdir()
        for run in (baseline, comet):
            destination = staged_event_logs / f"eventlog_v2_{run.inspection.application_id}"
            shutil.copytree(run.event_log, destination, copy_function=shutil.copy2)

        applications = [
            _run_manifest(run, repository_root, staged_event_logs) for run in (baseline, comet)
        ]
        manifest: dict[str, object] = {
            "schema_version": _MANIFEST_SCHEMA_VERSION,
            "status": mode,
            "experiment_id": experiment_id,
            "pair_index": pair_index,
            "workload": baseline.record["workload"],
            "query_id": baseline.record["query_id"],
            "storage_profile": baseline.record["storage_profile"],
            "git_commit": commit,
            "dataset_manifest_sha256": provenance.get("dataset_manifest_sha256"),
            "sql_sha256": provenance.get("sql_sha256"),
            "iceberg_snapshot_ids": provenance.get("iceberg_snapshot_ids"),
            "correctness": {
                "status": correctness.get("status"),
                "schema_sha256": correctness.get("schema_sha256"),
                "row_count": correctness.get("row_count"),
                "canonical_result_sha256": correctness.get("canonical_result_sha256"),
            },
            "report_publishability": {
                "path": _relative_to_root(
                    report_publishability.resolve(),
                    repository_root,
                    label="report publishability artifact",
                ),
                "sha256": _sha256_file(report_publishability.resolve()),
                "publishable": report.get("publishable") is True,
                "report_contract_passed": True,
            },
            "applications": applications,
            "history_server": {
                "url": _HISTORY_SERVER_URL,
                "event_log_uri": (
                    "file:///opt/lakehouse/"
                    + _relative_to_root(
                        bundle_root / "event-logs",
                        repository_root,
                        label="staged event log root",
                    )
                ),
                "application_urls": [
                    f"{_HISTORY_SERVER_URL}/history/{run.inspection.application_id}/SQL/"
                    for run in (baseline, comet)
                ],
                "measured_execution_urls": [
                    (
                        f"{_HISTORY_SERVER_URL}/history/{run.inspection.application_id}/"
                        f"SQL/execution/?id={run.inspection.measured_execution_id}"
                    )
                    for run in (baseline, comet)
                ],
            },
            "demo_disclosure": (
                "Publication evidence" if mode == "publishable" else "DIAGNOSTIC REHEARSAL ONLY"
            ),
        }
        _write_json(temporary / "demo-manifest.json", manifest)

        bundle_root.parent.mkdir(parents=True, exist_ok=True)
        if bundle_root.exists():
            if not _existing_bundle_matches(bundle_root, manifest):
                raise DemoPreparationError(
                    f"existing demo bundle does not match its immutable identity: {bundle_root}"
                )
        else:
            os.replace(temporary, bundle_root)
            temporary_published = True

        event_log_uri = manifest["history_server"]
        if not isinstance(event_log_uri, Mapping):
            raise DemoPreparationError("internal history-server manifest error")
        current_env = output_root / "current.env"
        temporary_env = output_root / ".current.env.tmp"
        temporary_env.write_text(
            f"SPARK_HISTORY_EVENT_LOG_DIR={event_log_uri['event_log_uri']}\n",
            encoding="utf-8",
            newline="\n",
        )
        os.replace(temporary_env, current_env)
        current_pointer = output_root / "current.json"
        temporary_pointer = output_root / ".current.json.tmp"
        _write_json(
            temporary_pointer,
            {
                "schema_version": 1,
                "bundle": _relative_to_root(bundle_root, repository_root, label="demo bundle"),
                "manifest_sha256": _sha256_file(bundle_root / "demo-manifest.json"),
                "status": mode,
            },
        )
        os.replace(temporary_pointer, current_pointer)
        return bundle_root
    finally:
        if not temporary_published and temporary.is_dir():
            shutil.rmtree(temporary)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Stage one paired Spark/Comet campaign run for Spark History Server."
    )
    parser.add_argument("--raw-root", type=Path, default=ROOT / "results/raw")
    parser.add_argument(
        "--report-publishability",
        type=Path,
        default=ROOT / "results/reports/report-publishability.json",
    )
    parser.add_argument("--output-root", type=Path, default=ROOT / ".artifacts/demo/spark-ui")
    parser.add_argument("--experiment", default="EXP-TPCH-SF1-Q01")
    parser.add_argument("--pair", type=int, default=1)
    parser.add_argument("--allow-diagnostic", action="store_true")
    args = parser.parse_args()
    try:
        bundle = prepare_spark_ui_demo(
            raw_root=args.raw_root,
            report_publishability=args.report_publishability,
            output_root=args.output_root,
            experiment_id=args.experiment,
            pair_index=args.pair,
            allow_diagnostic=args.allow_diagnostic,
        )
    except DemoPreparationError as error:
        raise SystemExit(str(error)) from error
    print(bundle)


if __name__ == "__main__":
    main()
