"""Strict, dependency-free attribution of Spark event-log task metrics.

Spark event logs are newline-delimited JSON, but the JSON protocol is not a stable public schema.
This module therefore accepts the field spellings used by Spark 3 and Spark 4 while remaining
conservative about attribution:

* a task is assigned to a SQL execution only when its stage has one unambiguous owner;
* work with no, conflicting, or non-SQL ownership evidence is retained in a separate bucket;
* absent metric fields are never silently converted to zero; every aggregate reports coverage;
* syntactically invalid JSON raises :class:`EventLogParseError`, while incomplete Spark events are
  returned as a partial report with diagnostics.

Task metrics include every observed ``TaskEnd`` attempt, including failed attempts.  This reflects
the executor resources actually consumed by an execution rather than only its successful output.
All paths are supplied by the caller.  Plain UTF-8 and gzip-compressed event-log files are supported
using only the Python standard library; a directory is expanded into its visible files in lexical
order, which also supports Spark rolling event logs.
"""

from __future__ import annotations

import gzip
import json
import os
import re
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator, Mapping
from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import IO, Literal, cast

type ParseStatus = Literal["complete", "partial"]
type WorkKind = Literal["background", "ambiguous", "unresolved"]
type LinkStatus = Literal["linked", "background", "unknown"]
type MetricName = Literal[
    "executor_cpu_time_ns",
    "executor_run_time_ms",
    "jvm_gc_time_ms",
    "shuffle_read_bytes",
    "shuffle_write_bytes",
    "input_bytes",
    "output_bytes",
    "memory_spill_bytes",
    "disk_spill_bytes",
]
type PathLike = str | os.PathLike[str]
type EventKind = Literal[
    "sql_start",
    "sql_end",
    "job_start",
    "job_end",
    "stage_submitted",
    "stage_completed",
    "task_end",
    "log_start",
    "other",
]

_MISSING = object()
_METRIC_NAMES: tuple[MetricName, ...] = (
    "executor_cpu_time_ns",
    "executor_run_time_ms",
    "jvm_gc_time_ms",
    "shuffle_read_bytes",
    "shuffle_write_bytes",
    "input_bytes",
    "output_bytes",
    "memory_spill_bytes",
    "disk_spill_bytes",
)
_METRIC_UNITS: dict[MetricName, str] = {
    "executor_cpu_time_ns": "ns",
    "executor_run_time_ms": "ms",
    "jvm_gc_time_ms": "ms",
    "shuffle_read_bytes": "bytes",
    "shuffle_write_bytes": "bytes",
    "input_bytes": "bytes",
    "output_bytes": "bytes",
    "memory_spill_bytes": "bytes",
    "disk_spill_bytes": "bytes",
}


class EventLogParseError(ValueError):
    """A source is not a valid newline-delimited Spark JSON event log."""

    def __init__(self, path: Path, line_number: int, message: str) -> None:
        self.path = path
        self.line_number = line_number
        self.message = message
        super().__init__(f"{path}:{line_number}: {message}")


@dataclass(frozen=True, slots=True)
class MetricAggregate:
    """One additive task metric and the coverage of its reported value."""

    value: int | None
    unit: str
    observed_tasks: int
    missing_tasks: int
    complete: bool


@dataclass(frozen=True, slots=True)
class TaskMetricTotals:
    """Task-attempt totals with completeness tracked independently per metric."""

    task_count: int
    executor_cpu_time_ns: MetricAggregate
    executor_run_time_ms: MetricAggregate
    jvm_gc_time_ms: MetricAggregate
    shuffle_read_bytes: MetricAggregate
    shuffle_write_bytes: MetricAggregate
    input_bytes: MetricAggregate
    output_bytes: MetricAggregate
    memory_spill_bytes: MetricAggregate
    disk_spill_bytes: MetricAggregate

    @property
    def complete(self) -> bool:
        return all(getattr(self, name).complete for name in _METRIC_NAMES)


@dataclass(frozen=True, slots=True)
class FailureSummary:
    """Failures observed at each Spark listener level."""

    sql_execution_failures: int
    job_failures: int
    stage_failures: int
    task_failures: int
    reasons: tuple[str, ...]

    @property
    def total(self) -> int:
        return (
            self.sql_execution_failures
            + self.job_failures
            + self.stage_failures
            + self.task_failures
        )


@dataclass(frozen=True, slots=True)
class ExecutionCompleteness:
    """Why an execution attribution is or is not suitable for reporting."""

    sql_start_seen: bool
    sql_end_seen: bool
    stage_attribution_complete: bool
    task_metrics_complete: bool
    complete: bool


@dataclass(frozen=True, slots=True)
class SQLExecutionAttribution:
    """Metrics and listener identifiers safely attributable to one SQL execution ID."""

    execution_id: int
    root_execution_id: int | None
    job_group_id: str | None
    description: str | None
    start_time_ms: int | None
    end_time_ms: int | None
    duration_ms: int | None
    job_ids: tuple[int, ...]
    stage_ids: tuple[int, ...]
    stage_attempts: tuple[tuple[int, int], ...]
    metrics: TaskMetricTotals
    failures: FailureSummary
    completeness: ExecutionCompleteness
    status: ParseStatus
    issues: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return cast(dict[str, object], asdict(self))


@dataclass(frozen=True, slots=True)
class WorkSummary:
    """Work deliberately withheld from SQL executions because attribution is unsafe."""

    kind: WorkKind
    job_ids: tuple[int, ...]
    stage_ids: tuple[int, ...]
    candidate_execution_ids: tuple[int, ...]
    metrics: TaskMetricTotals
    failures: FailureSummary


@dataclass(frozen=True, slots=True)
class ParseDiagnostics:
    """Protocol coverage and recoverable semantic problems found while parsing."""

    lines_read: int
    events_read: int
    event_counts: dict[str, int]
    unknown_fields: tuple[str, ...]
    issues: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class EventLogReport:
    """Complete output of parsing one logical Spark event log."""

    schema_version: int
    source_paths: tuple[str, ...]
    spark_version: str | None
    status: ParseStatus
    executions: tuple[SQLExecutionAttribution, ...]
    background_work: WorkSummary
    ambiguous_work: WorkSummary
    unresolved_work: WorkSummary
    diagnostics: ParseDiagnostics

    def execution(self, execution_id: int) -> SQLExecutionAttribution | None:
        """Return an execution by ID without requiring callers to rebuild an index."""

        return next(
            (execution for execution in self.executions if execution.execution_id == execution_id),
            None,
        )

    def to_dict(self) -> dict[str, object]:
        return cast(dict[str, object], asdict(self))


@dataclass(slots=True)
class _ExecutionState:
    execution_id: int
    root_execution_id: int | None = None
    job_group_id: str | None = None
    description: str | None = None
    start_time_ms: int | None = None
    end_time_ms: int | None = None
    start_seen: bool = False
    end_seen: bool = False
    failure_reason: str | None = None
    issues: list[str] = field(default_factory=list)


@dataclass(slots=True)
class _JobRecord:
    job_id: int
    stage_ids: set[int]
    link_status: LinkStatus
    execution_id: int | None
    failure_reason: str | None = None
    issues: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class _StageEvidence:
    stage_id: int
    attempt_id: int | None
    link_status: LinkStatus
    execution_id: int | None
    issues: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _StageFailure:
    stage_id: int
    attempt_id: int | None
    reason: str


@dataclass(frozen=True, slots=True)
class _TaskRecord:
    stage_id: int | None
    stage_attempt_id: int | None
    task_id: int | None
    task_attempt: int | None
    metrics: dict[MetricName, int | None]
    failure_reason: str | None
    unknown_fields: tuple[str, ...]
    issues: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _StageAssignment:
    kind: Literal["execution", "background", "ambiguous", "unresolved"]
    execution_id: int | None
    candidates: tuple[int, ...]


@dataclass(slots=True)
class _MutableMetrics:
    task_count: int = 0
    totals: dict[MetricName, int] = field(
        default_factory=lambda: {name: 0 for name in _METRIC_NAMES}
    )
    observed: dict[MetricName, int] = field(
        default_factory=lambda: {name: 0 for name in _METRIC_NAMES}
    )
    missing: dict[MetricName, int] = field(
        default_factory=lambda: {name: 0 for name in _METRIC_NAMES}
    )

    def add(self, values: Mapping[MetricName, int | None]) -> None:
        self.task_count += 1
        for name in _METRIC_NAMES:
            value = values.get(name)
            if value is None:
                self.missing[name] += 1
            else:
                self.totals[name] += value
                self.observed[name] += 1

    def freeze(self) -> TaskMetricTotals:
        aggregates: dict[MetricName, MetricAggregate] = {}
        for name in _METRIC_NAMES:
            observed = self.observed[name]
            value = self.totals[name] if observed > 0 or self.task_count == 0 else None
            aggregates[name] = MetricAggregate(
                value=value,
                unit=_METRIC_UNITS[name],
                observed_tasks=observed,
                missing_tasks=self.missing[name],
                complete=self.missing[name] == 0,
            )
        return TaskMetricTotals(
            task_count=self.task_count,
            executor_cpu_time_ns=aggregates["executor_cpu_time_ns"],
            executor_run_time_ms=aggregates["executor_run_time_ms"],
            jvm_gc_time_ms=aggregates["jvm_gc_time_ms"],
            shuffle_read_bytes=aggregates["shuffle_read_bytes"],
            shuffle_write_bytes=aggregates["shuffle_write_bytes"],
            input_bytes=aggregates["input_bytes"],
            output_bytes=aggregates["output_bytes"],
            memory_spill_bytes=aggregates["memory_spill_bytes"],
            disk_spill_bytes=aggregates["disk_spill_bytes"],
        )


@dataclass(slots=True)
class _MutableFailures:
    sql_execution_failures: int = 0
    job_failures: int = 0
    stage_failures: int = 0
    task_failures: int = 0
    reasons: list[str] = field(default_factory=list)

    def add_reason(self, level: str, identifier: str, reason: str) -> None:
        rendered = f"{level} {identifier}: {reason}"
        if rendered not in self.reasons:
            self.reasons.append(rendered)

    def freeze(self) -> FailureSummary:
        return FailureSummary(
            sql_execution_failures=self.sql_execution_failures,
            job_failures=self.job_failures,
            stage_failures=self.stage_failures,
            task_failures=self.task_failures,
            reasons=tuple(self.reasons),
        )


@dataclass(slots=True)
class _MutableWork:
    kind: WorkKind
    job_ids: set[int] = field(default_factory=set)
    stage_ids: set[int] = field(default_factory=set)
    candidates: set[int] = field(default_factory=set)
    metrics: _MutableMetrics = field(default_factory=_MutableMetrics)
    failures: _MutableFailures = field(default_factory=_MutableFailures)

    def freeze(self) -> WorkSummary:
        return WorkSummary(
            kind=self.kind,
            job_ids=tuple(sorted(self.job_ids)),
            stage_ids=tuple(sorted(self.stage_ids)),
            candidate_execution_ids=tuple(sorted(self.candidates)),
            metrics=self.metrics.freeze(),
            failures=self.failures.freeze(),
        )


def _normalize_key(key: str) -> str:
    return "".join(character.lower() for character in key if character.isalnum())


def _lookup(mapping: Mapping[str, object], *names: str) -> object:
    normalized = {_normalize_key(name) for name in names}
    for key, value in mapping.items():
        if _normalize_key(key) in normalized:
            return value
    return _MISSING


def _object(value: object) -> dict[str, object] | None:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        return None
    return cast(dict[str, object], value)


def _integer(value: object, *, allow_string: bool = True) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float) and value.is_integer() and value >= 0:
        return int(value)
    if allow_string and isinstance(value, str):
        try:
            parsed = int(value.strip())
        except ValueError:
            return None
        return parsed if parsed >= 0 else None
    return None


def _optional_text(value: object) -> str | None:
    if value is _MISSING or value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return None


def _reason_text(value: object) -> str | None:
    direct = _optional_text(value)
    if direct is not None:
        return direct
    mapping = _object(value)
    if mapping is None:
        return None
    parts: list[str] = []
    for name in ("Result", "Reason", "Message", "Description", "Class Name", "Exception"):
        candidate = _optional_text(_lookup(mapping, name))
        if candidate is not None and candidate not in parts:
            parts.append(candidate)
    if parts:
        return ": ".join(parts)
    try:
        return json.dumps(mapping, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    except TypeError:
        return None


def _is_success(reason: str) -> bool:
    normalized = _normalize_key(reason)
    return normalized in {"success", "jobsucceeded", "stagesucceeded", "tasksucceeded"}


def _short_event_name(raw_name: str) -> str:
    return raw_name.rsplit(".", maxsplit=1)[-1].rstrip("$")


def _event_kind(raw_name: str) -> EventKind:
    normalized = _normalize_key(_short_event_name(raw_name))
    kinds: dict[str, EventKind] = {
        "sparklistenersqlexecutionstart": "sql_start",
        "sqlexecutionstart": "sql_start",
        "sparklistenersqlexecutionend": "sql_end",
        "sqlexecutionend": "sql_end",
        "sparklistenerjobstart": "job_start",
        "jobstart": "job_start",
        "sparklistenerjobend": "job_end",
        "jobend": "job_end",
        "sparklistenerstagesubmitted": "stage_submitted",
        "stagesubmitted": "stage_submitted",
        "sparklistenerstagecompleted": "stage_completed",
        "stagecompleted": "stage_completed",
        "sparklistenertaskend": "task_end",
        "taskend": "task_end",
        "sparklistenerlogstart": "log_start",
        "logstart": "log_start",
    }
    return kinds.get(normalized, "other")


_KNOWN_EVENT_FIELDS: dict[EventKind, frozenset[str]] = {
    "sql_start": frozenset(
        {
            "event",
            "eventname",
            "executionid",
            "rootexecutionid",
            "description",
            "details",
            "physicalplandescription",
            "sparkplaninfo",
            "time",
            "modifiedconfigs",
            "jobtags",
            "jobgroupid",
            "queryid",
        }
    ),
    "sql_end": frozenset(
        {
            "event",
            "eventname",
            "executionid",
            "time",
            "errormessage",
            "executionfailure",
            "failurereason",
            "queryid",
        }
    ),
    "job_start": frozenset(
        {"event", "eventname", "jobid", "submissiontime", "stageinfos", "stageids", "properties"}
    ),
    "job_end": frozenset({"event", "eventname", "jobid", "completiontime", "jobresult"}),
    "stage_submitted": frozenset({"event", "eventname", "stageinfo", "properties"}),
    "stage_completed": frozenset({"event", "eventname", "stageinfo"}),
    "task_end": frozenset(
        {
            "event",
            "eventname",
            "stageid",
            "stageattemptid",
            "tasktype",
            "taskendreason",
            "taskinfo",
            "executormetrics",
            "taskexecutormetrics",
            "taskmetrics",
        }
    ),
    "log_start": frozenset({"event", "eventname", "sparkversion"}),
    "other": frozenset(),
}

_KNOWN_TASK_METRIC_FIELDS = frozenset(
    {
        "executordeserializetime",
        "executordeserializecputime",
        "executorruntime",
        "executorcputime",
        "peakexecutionmemory",
        "peakonheapexecutionmemory",
        "peakoffheapexecutionmemory",
        "resultsize",
        "jvmgctime",
        "resultserializationtime",
        "memorybytesspilled",
        "diskbytesspilled",
        "shufflereadmetrics",
        "shufflewritemetrics",
        "inputmetrics",
        "outputmetrics",
        "updatedblocks",
    }
)
_KNOWN_SHUFFLE_READ_FIELDS = frozenset(
    {
        "remoteblocksfetched",
        "localblocksfetched",
        "fetchwaittime",
        "remotebytesread",
        "remotebytesreadtodisk",
        "localbytesread",
        "totalbytesread",
        "recordsread",
        "totalrecordsread",
        "remotereqsduration",
        "remoterequestsduration",
        "pushbasedshuffle",
        "remotemergedblocksfetched",
        "localmergedblocksfetched",
        "remotemergedchunksfetched",
        "localmergedchunksfetched",
        "remotemergedbytesread",
        "localmergedbytesread",
        "remotemergedreqsduration",
        "corruptmergedblockchunks",
        "mergedfetchfallbackcount",
    }
)
_KNOWN_SHUFFLE_PUSH_FIELDS = frozenset(
    {
        "corruptmergedblockchunks",
        "mergedfetchfallbackcount",
        "mergedremoteblocksfetched",
        "mergedlocalblocksfetched",
        "mergedremotechunksfetched",
        "mergedlocalchunksfetched",
        "mergedremotebytesread",
        "mergedlocalbytesread",
        "mergedremoterequestsduration",
        # Early push-shuffle JSON prototypes used the inverse word ordering.
        "remotemergedblocksfetched",
        "localmergedblocksfetched",
        "remotemergedchunksfetched",
        "localmergedchunksfetched",
        "remotemergedbytesread",
        "localmergedbytesread",
        "remotemergedreqsduration",
    }
)
_KNOWN_SHUFFLE_WRITE_FIELDS = frozenset(
    {
        "shufflebyteswritten",
        "byteswritten",
        "shufflewritetime",
        "shufflerecordswritten",
        "recordswritten",
    }
)
_KNOWN_IO_FIELDS = frozenset({"bytesread", "byteswritten", "recordsread", "recordswritten"})


class _EventLogParser:
    def __init__(self) -> None:
        self.lines_read = 0
        self.events_read = 0
        self.event_counts: Counter[str] = Counter()
        self.spark_version: str | None = None
        self.executions: dict[int, _ExecutionState] = {}
        self.jobs: dict[int, _JobRecord] = {}
        self.orphan_job_failures: dict[int, str] = {}
        self.stage_evidence: dict[tuple[int, int | None], list[_StageEvidence]] = defaultdict(list)
        self.stage_failures: list[_StageFailure] = []
        self.completed_stages: set[tuple[int, int | None]] = set()
        self.tasks: list[_TaskRecord] = []
        self.seen_tasks: set[tuple[int, int | None, int, int | None]] = set()
        self.unknown_fields: set[str] = set()
        self.issues: list[str] = []

    def _issue(self, location: str, message: str) -> None:
        rendered = f"{location}: {message}"
        if rendered not in self.issues:
            self.issues.append(rendered)

    def _unknown_event_fields(
        self, event: Mapping[str, object], kind: EventKind
    ) -> tuple[str, ...]:
        if kind == "other":
            return ()
        known = _KNOWN_EVENT_FIELDS[kind]
        unknown = tuple(sorted(key for key in event if _normalize_key(key) not in known))
        for key in unknown:
            self.unknown_fields.add(f"{kind}.{key}")
        return unknown

    def consume(self, event: dict[str, object], location: str) -> None:
        event_value = _lookup(event, "Event", "eventName")
        if not isinstance(event_value, str) or not event_value.strip():
            self._issue(location, "event has no non-empty Event/eventName field")
            return
        raw_name = event_value.strip()
        short_name = _short_event_name(raw_name)
        kind = _event_kind(raw_name)
        self.events_read += 1
        self.event_counts[short_name] += 1
        unknown = self._unknown_event_fields(event, kind)

        if kind == "sql_start":
            self._consume_sql_start(event, location, unknown)
        elif kind == "sql_end":
            self._consume_sql_end(event, location, unknown)
        elif kind == "job_start":
            self._consume_job_start(event, location, unknown)
        elif kind == "job_end":
            self._consume_job_end(event, location)
        elif kind == "stage_submitted":
            self._consume_stage_submitted(event, location, unknown)
        elif kind == "stage_completed":
            self._consume_stage_completed(event, location, unknown)
        elif kind == "task_end":
            self._consume_task_end(event, location, unknown)
        elif kind == "log_start":
            version = _optional_text(_lookup(event, "Spark Version", "sparkVersion"))
            if version is not None:
                if self.spark_version is not None and self.spark_version != version:
                    self._issue(location, "conflicting Spark versions in one logical event log")
                self.spark_version = version

    def _execution_id(self, event: Mapping[str, object], location: str) -> int | None:
        raw = _lookup(event, "executionId", "Execution ID", "execution_id")
        execution_id = _integer(raw)
        if execution_id is None:
            self._issue(location, "SQL event has no valid non-negative execution ID")
        return execution_id

    def _consume_sql_start(
        self, event: Mapping[str, object], location: str, unknown: tuple[str, ...]
    ) -> None:
        execution_id = self._execution_id(event, location)
        if execution_id is None:
            return
        state = self.executions.setdefault(execution_id, _ExecutionState(execution_id))
        if state.start_seen:
            state.issues.append("duplicate SQL execution start event")
            self._issue(location, f"duplicate SQL execution start for {execution_id}")
        state.start_seen = True
        raw_time = _lookup(event, "time", "Time")
        state.start_time_ms = _integer(raw_time)
        if state.start_time_ms is None:
            state.issues.append("start event has no valid time")
            self._issue(location, f"SQL execution {execution_id} start has invalid time")
        root_raw = _lookup(event, "rootExecutionId", "Root Execution ID")
        if root_raw is not _MISSING and root_raw is not None:
            state.root_execution_id = _integer(root_raw)
            if state.root_execution_id is None:
                state.issues.append("start event has invalid root execution ID")
        job_group_raw = _lookup(event, "jobGroupId", "Job Group ID")
        if job_group_raw is not _MISSING and job_group_raw is not None:
            state.job_group_id = _optional_text(job_group_raw)
            if state.job_group_id is None:
                state.issues.append("start event has invalid job group ID")
        description_raw = _lookup(event, "description", "Description")
        if description_raw is not _MISSING and description_raw is not None:
            state.description = _optional_text(description_raw)
            if state.description is None:
                state.issues.append("start event has invalid description")
        if unknown:
            state.issues.append(f"unknown start-event fields: {', '.join(unknown)}")

    def _consume_sql_end(
        self, event: Mapping[str, object], location: str, unknown: tuple[str, ...]
    ) -> None:
        execution_id = self._execution_id(event, location)
        if execution_id is None:
            return
        state = self.executions.setdefault(execution_id, _ExecutionState(execution_id))
        if state.end_seen:
            state.issues.append("duplicate SQL execution end event")
            self._issue(location, f"duplicate SQL execution end for {execution_id}")
        state.end_seen = True
        state.end_time_ms = _integer(_lookup(event, "time", "Time"))
        if state.end_time_ms is None:
            state.issues.append("end event has no valid time")
            self._issue(location, f"SQL execution {execution_id} end has invalid time")
        failure_raw = _lookup(event, "errorMessage", "executionFailure", "failureReason")
        failure_reason = _reason_text(failure_raw)
        if failure_reason is not None and not _is_success(failure_reason):
            state.failure_reason = failure_reason
        if unknown:
            state.issues.append(f"unknown end-event fields: {', '.join(unknown)}")

    def _link_from_properties(
        self,
        properties_value: object,
        location: str,
        owner: str,
    ) -> tuple[LinkStatus, int | None]:
        if properties_value is _MISSING or properties_value is None:
            self._issue(location, f"{owner} has no readable Properties object")
            return "unknown", None
        properties = _object(properties_value)
        if properties is None:
            self._issue(location, f"{owner} Properties is not a JSON object")
            return "unknown", None
        raw_execution_id = _lookup(properties, "spark.sql.execution.id")
        if raw_execution_id is _MISSING or raw_execution_id is None:
            return "background", None
        execution_id = _integer(raw_execution_id)
        if execution_id is None:
            self._issue(location, f"{owner} has an invalid spark.sql.execution.id property")
            return "unknown", None
        return "linked", execution_id

    def _stage_ids(self, event: Mapping[str, object], location: str) -> set[int]:
        stage_ids: set[int] = set()
        raw_ids = _lookup(event, "Stage IDs", "stageIds")
        if raw_ids is not _MISSING and raw_ids is not None:
            if isinstance(raw_ids, list):
                for raw_id in raw_ids:
                    stage_id = _integer(raw_id)
                    if stage_id is None:
                        self._issue(location, "JobStart contains an invalid stage ID")
                    else:
                        stage_ids.add(stage_id)
            else:
                self._issue(location, "JobStart Stage IDs is not an array")
        raw_infos = _lookup(event, "Stage Infos", "stageInfos")
        if raw_infos is not _MISSING and raw_infos is not None:
            if isinstance(raw_infos, list):
                for raw_info in raw_infos:
                    info = _object(raw_info)
                    if info is None:
                        self._issue(location, "JobStart Stage Infos contains a non-object")
                        continue
                    stage_id = _integer(_lookup(info, "Stage ID", "stageId"))
                    if stage_id is None:
                        self._issue(location, "JobStart Stage Info has no valid stage ID")
                    else:
                        stage_ids.add(stage_id)
            else:
                self._issue(location, "JobStart Stage Infos is not an array")
        return stage_ids

    def _consume_job_start(
        self, event: Mapping[str, object], location: str, unknown: tuple[str, ...]
    ) -> None:
        job_id = _integer(_lookup(event, "Job ID", "jobId"))
        if job_id is None:
            self._issue(location, "JobStart has no valid non-negative job ID")
            return
        if job_id in self.jobs:
            self._issue(location, f"duplicate JobStart for job {job_id}")
            return
        link_status, execution_id = self._link_from_properties(
            _lookup(event, "Properties", "properties"), location, f"job {job_id}"
        )
        issues = [f"unknown JobStart fields: {', '.join(unknown)}"] if unknown else []
        record = _JobRecord(
            job_id=job_id,
            stage_ids=self._stage_ids(event, location),
            link_status=link_status,
            execution_id=execution_id,
            failure_reason=self.orphan_job_failures.pop(job_id, None),
            issues=issues,
        )
        self.jobs[job_id] = record

    def _consume_job_end(self, event: Mapping[str, object], location: str) -> None:
        job_id = _integer(_lookup(event, "Job ID", "jobId"))
        if job_id is None:
            self._issue(location, "JobEnd has no valid non-negative job ID")
            return
        result_value = _lookup(event, "Job Result", "jobResult")
        reason = _reason_text(result_value)
        if reason is None:
            self._issue(location, f"JobEnd for job {job_id} has no readable result")
            return
        failure = None if _is_success(reason) else reason
        record = self.jobs.get(job_id)
        if record is None:
            if failure is not None:
                self.orphan_job_failures[job_id] = failure
            self._issue(location, f"JobEnd for job {job_id} has no matching JobStart")
            return
        if record.failure_reason is not None:
            self._issue(location, f"duplicate JobEnd failure for job {job_id}")
        record.failure_reason = failure

    def _stage_info(
        self, event: Mapping[str, object], location: str
    ) -> tuple[dict[str, object], int, int | None] | None:
        info = _object(_lookup(event, "Stage Info", "stageInfo"))
        if info is None:
            self._issue(location, "stage event has no readable Stage Info object")
            return None
        stage_id = _integer(_lookup(info, "Stage ID", "stageId"))
        if stage_id is None:
            self._issue(location, "Stage Info has no valid non-negative stage ID")
            return None
        attempt_raw = _lookup(info, "Stage Attempt ID", "stageAttemptId", "attemptId")
        attempt_id = None if attempt_raw is _MISSING else _integer(attempt_raw)
        if attempt_raw is not _MISSING and attempt_id is None:
            self._issue(location, f"stage {stage_id} has an invalid attempt ID")
        return info, stage_id, attempt_id

    def _consume_stage_submitted(
        self, event: Mapping[str, object], location: str, unknown: tuple[str, ...]
    ) -> None:
        parsed = self._stage_info(event, location)
        if parsed is None:
            return
        info, stage_id, attempt_id = parsed
        properties = _lookup(event, "Properties", "properties")
        if properties is _MISSING:
            properties = _lookup(info, "Properties", "properties")
        link_status, execution_id = self._link_from_properties(
            properties, location, f"stage {stage_id}"
        )
        issues = (f"unknown StageSubmitted fields: {', '.join(unknown)}",) if unknown else ()
        evidence = _StageEvidence(stage_id, attempt_id, link_status, execution_id, issues)
        key = (stage_id, attempt_id)
        if evidence in self.stage_evidence[key]:
            self._issue(location, f"duplicate StageSubmitted for stage {stage_id}.{attempt_id}")
        else:
            self.stage_evidence[key].append(evidence)

    def _consume_stage_completed(
        self, event: Mapping[str, object], location: str, unknown: tuple[str, ...]
    ) -> None:
        parsed = self._stage_info(event, location)
        if parsed is None:
            return
        info, stage_id, attempt_id = parsed
        key = (stage_id, attempt_id)
        if key in self.completed_stages:
            self._issue(location, f"duplicate StageCompleted for stage {stage_id}.{attempt_id}")
            return
        self.completed_stages.add(key)
        raw_failure = _lookup(info, "Failure Reason", "failureReason")
        reason = _reason_text(raw_failure)
        if reason is not None and not _is_success(reason):
            self.stage_failures.append(_StageFailure(stage_id, attempt_id, reason))
        if unknown:
            self._issue(location, f"stage {stage_id} has unknown fields: {', '.join(unknown)}")

    def _unknown_nested_fields(
        self, mapping: Mapping[str, object], known: frozenset[str], prefix: str
    ) -> tuple[str, ...]:
        unknown = tuple(sorted(key for key in mapping if _normalize_key(key) not in known))
        for key in unknown:
            self.unknown_fields.add(f"{prefix}.{key}")
        return unknown

    def _metric_integer(
        self,
        metrics: Mapping[str, object],
        aliases: tuple[str, ...],
        location: str,
        path: str,
        issues: list[str],
    ) -> int | None:
        raw = _lookup(metrics, *aliases)
        if raw is _MISSING or raw is None:
            return None
        value = _integer(raw, allow_string=False)
        if value is None:
            issue = f"{path} is not a non-negative integer"
            issues.append(issue)
            self._issue(location, issue)
        return value

    def _metric_block(
        self,
        metrics: Mapping[str, object],
        aliases: tuple[str, ...],
        location: str,
        path: str,
        issues: list[str],
    ) -> dict[str, object] | None:
        raw = _lookup(metrics, *aliases)
        if raw is _MISSING or raw is None:
            return None
        block = _object(raw)
        if block is None:
            issue = f"{path} is not a JSON object"
            issues.append(issue)
            self._issue(location, issue)
        return block

    def _shuffle_read_bytes(
        self,
        metrics: Mapping[str, object],
        location: str,
        issues: list[str],
    ) -> tuple[int | None, tuple[str, ...]]:
        block = self._metric_block(
            metrics,
            ("Shuffle Read Metrics", "shuffleReadMetrics"),
            location,
            "Shuffle Read Metrics",
            issues,
        )
        if block is None:
            return None, ()
        unknown = self._unknown_nested_fields(
            block, _KNOWN_SHUFFLE_READ_FIELDS, "Task Metrics.Shuffle Read Metrics"
        )
        push_value = _lookup(block, "Push Based Shuffle", "pushBasedShuffle")
        if push_value is not _MISSING and push_value is not None:
            push_metrics = _object(push_value)
            if push_metrics is None:
                issue = "Shuffle Read Metrics.Push Based Shuffle is not a JSON object"
                issues.append(issue)
                self._issue(location, issue)
            else:
                unknown += self._unknown_nested_fields(
                    push_metrics,
                    _KNOWN_SHUFFLE_PUSH_FIELDS,
                    "Task Metrics.Shuffle Read Metrics.Push Based Shuffle",
                )
        total = self._metric_integer(
            block,
            ("Total Bytes Read", "totalBytesRead"),
            location,
            "Shuffle Read Metrics.Total Bytes Read",
            issues,
        )
        if total is not None:
            return total, unknown
        remote = self._metric_integer(
            block,
            ("Remote Bytes Read", "remoteBytesRead"),
            location,
            "Shuffle Read Metrics.Remote Bytes Read",
            issues,
        )
        local = self._metric_integer(
            block,
            ("Local Bytes Read", "localBytesRead"),
            location,
            "Shuffle Read Metrics.Local Bytes Read",
            issues,
        )
        if remote is None or local is None:
            return None, unknown
        return remote + local, unknown

    def _nested_metric(
        self,
        metrics: Mapping[str, object],
        block_aliases: tuple[str, ...],
        value_aliases: tuple[str, ...],
        known_fields: frozenset[str],
        location: str,
        path: str,
        issues: list[str],
    ) -> tuple[int | None, tuple[str, ...]]:
        block = self._metric_block(metrics, block_aliases, location, path, issues)
        if block is None:
            return None, ()
        unknown = self._unknown_nested_fields(block, known_fields, f"Task Metrics.{path}")
        value = self._metric_integer(
            block, value_aliases, location, f"{path}.{value_aliases[0]}", issues
        )
        return value, unknown

    def _task_metrics(
        self, event: Mapping[str, object], location: str
    ) -> tuple[dict[MetricName, int | None], tuple[str, ...], tuple[str, ...]]:
        issues: list[str] = []
        unknown: list[str] = []
        raw_metrics = _lookup(event, "Task Metrics", "taskMetrics")
        metrics = _object(raw_metrics)
        if metrics is None:
            issue = "TaskEnd has no readable Task Metrics object"
            issues.append(issue)
            self._issue(location, issue)
            return {name: None for name in _METRIC_NAMES}, (), tuple(issues)
        unknown.extend(
            self._unknown_nested_fields(metrics, _KNOWN_TASK_METRIC_FIELDS, "Task Metrics")
        )
        values: dict[MetricName, int | None] = {
            "executor_cpu_time_ns": self._metric_integer(
                metrics,
                ("Executor CPU Time", "executorCpuTime"),
                location,
                "Task Metrics.Executor CPU Time",
                issues,
            ),
            "executor_run_time_ms": self._metric_integer(
                metrics,
                ("Executor Run Time", "executorRunTime"),
                location,
                "Task Metrics.Executor Run Time",
                issues,
            ),
            "jvm_gc_time_ms": self._metric_integer(
                metrics,
                ("JVM GC Time", "jvmGcTime"),
                location,
                "Task Metrics.JVM GC Time",
                issues,
            ),
            "memory_spill_bytes": self._metric_integer(
                metrics,
                ("Memory Bytes Spilled", "memoryBytesSpilled"),
                location,
                "Task Metrics.Memory Bytes Spilled",
                issues,
            ),
            "disk_spill_bytes": self._metric_integer(
                metrics,
                ("Disk Bytes Spilled", "diskBytesSpilled"),
                location,
                "Task Metrics.Disk Bytes Spilled",
                issues,
            ),
            "shuffle_read_bytes": None,
            "shuffle_write_bytes": None,
            "input_bytes": None,
            "output_bytes": None,
        }
        values["shuffle_read_bytes"], nested_unknown = self._shuffle_read_bytes(
            metrics, location, issues
        )
        unknown.extend(nested_unknown)
        values["shuffle_write_bytes"], nested_unknown = self._nested_metric(
            metrics,
            ("Shuffle Write Metrics", "shuffleWriteMetrics"),
            ("Shuffle Bytes Written", "shuffleBytesWritten", "Bytes Written", "bytesWritten"),
            _KNOWN_SHUFFLE_WRITE_FIELDS,
            location,
            "Shuffle Write Metrics",
            issues,
        )
        unknown.extend(nested_unknown)
        values["input_bytes"], nested_unknown = self._nested_metric(
            metrics,
            ("Input Metrics", "inputMetrics"),
            ("Bytes Read", "bytesRead"),
            _KNOWN_IO_FIELDS,
            location,
            "Input Metrics",
            issues,
        )
        unknown.extend(nested_unknown)
        values["output_bytes"], nested_unknown = self._nested_metric(
            metrics,
            ("Output Metrics", "outputMetrics"),
            ("Bytes Written", "bytesWritten"),
            _KNOWN_IO_FIELDS,
            location,
            "Output Metrics",
            issues,
        )
        unknown.extend(nested_unknown)
        return values, tuple(sorted(set(unknown))), tuple(issues)

    def _consume_task_end(
        self, event: Mapping[str, object], location: str, event_unknown: tuple[str, ...]
    ) -> None:
        issues: list[str] = []
        stage_id = _integer(_lookup(event, "Stage ID", "stageId"))
        if stage_id is None:
            issue = "TaskEnd has no valid non-negative stage ID"
            issues.append(issue)
            self._issue(location, issue)
        raw_stage_attempt = _lookup(event, "Stage Attempt ID", "stageAttemptId")
        stage_attempt = None if raw_stage_attempt is _MISSING else _integer(raw_stage_attempt)
        if raw_stage_attempt is _MISSING:
            issue = "TaskEnd has no stage attempt ID; retry attribution is incomplete"
            issues.append(issue)
            self._issue(location, issue)
        elif stage_attempt is None:
            issue = "TaskEnd has an invalid stage attempt ID; retry attribution is incomplete"
            issues.append(issue)
            self._issue(location, issue)

        task_info = _object(_lookup(event, "Task Info", "taskInfo"))
        task_id: int | None = None
        task_attempt: int | None = None
        if task_info is None:
            issue = "TaskEnd has no readable Task Info object"
            issues.append(issue)
            self._issue(location, issue)
        else:
            task_id = _integer(_lookup(task_info, "Task ID", "taskId"))
            task_attempt_raw = _lookup(task_info, "Attempt", "attempt", "attemptNumber")
            task_attempt = None if task_attempt_raw is _MISSING else _integer(task_attempt_raw)
            if task_id is None:
                issue = "Task Info has no valid task ID; duplicate detection is incomplete"
                issues.append(issue)
                self._issue(location, issue)
            if task_attempt_raw is _MISSING:
                issue = "Task Info has no attempt number; retry attribution is incomplete"
                issues.append(issue)
                self._issue(location, issue)
            elif task_attempt is None:
                issue = "Task Info has an invalid attempt number; retry attribution is incomplete"
                issues.append(issue)
                self._issue(location, issue)

        values, metric_unknown, metric_issues = self._task_metrics(event, location)
        issues.extend(metric_issues)
        all_unknown = tuple(sorted(set(event_unknown) | set(metric_unknown)))
        if event_unknown:
            issues.append(f"unknown TaskEnd fields: {', '.join(event_unknown)}")
        if metric_unknown:
            issues.append(f"unknown task metric fields: {', '.join(metric_unknown)}")

        reason = _reason_text(_lookup(event, "Task End Reason", "taskEndReason"))
        failure_reason: str | None = None
        if reason is None:
            issue = "TaskEnd has no readable task end reason"
            issues.append(issue)
            self._issue(location, issue)
        elif not _is_success(reason):
            failure_reason = reason

        if stage_id is not None and task_id is not None:
            task_key = (stage_id, stage_attempt, task_id, task_attempt)
            if task_key in self.seen_tasks:
                self._issue(location, f"duplicate TaskEnd for task identity {task_key}")
                return
            self.seen_tasks.add(task_key)
        self.tasks.append(
            _TaskRecord(
                stage_id=stage_id,
                stage_attempt_id=stage_attempt,
                task_id=task_id,
                task_attempt=task_attempt,
                metrics=values,
                failure_reason=failure_reason,
                unknown_fields=all_unknown,
                issues=tuple(issues),
            )
        )

    def _classify_stage(
        self,
        stage_id: int,
        attempt_id: int | None,
        owners: Mapping[int, list[_JobRecord]],
    ) -> _StageAssignment:
        exact = self.stage_evidence.get((stage_id, attempt_id), [])
        if not exact and attempt_id is not None:
            exact = self.stage_evidence.get((stage_id, None), [])
        direct_linked = {
            evidence.execution_id
            for evidence in exact
            if evidence.link_status == "linked" and evidence.execution_id is not None
        }
        direct_background = any(evidence.link_status == "background" for evidence in exact)
        jobs = owners.get(stage_id, [])
        job_linked = {
            job.execution_id
            for job in jobs
            if job.link_status == "linked" and job.execution_id is not None
        }
        job_background = any(job.link_status == "background" for job in jobs)
        job_unknown = any(job.link_status == "unknown" for job in jobs)
        candidates = cast(set[int], direct_linked | job_linked)

        if direct_linked:
            if (
                len(direct_linked) == 1
                and not direct_background
                and not (job_linked - direct_linked)
                and not job_background
            ):
                execution_id = next(iter(direct_linked))
                return _StageAssignment("execution", execution_id, tuple(sorted(candidates)))
            return _StageAssignment("ambiguous", None, tuple(sorted(candidates)))
        if direct_background:
            if job_linked:
                return _StageAssignment("ambiguous", None, tuple(sorted(candidates)))
            return _StageAssignment("background", None, ())
        if len(job_linked) == 1 and not job_background and not job_unknown:
            execution_id = next(iter(job_linked))
            return _StageAssignment("execution", execution_id, (execution_id,))
        if len(job_linked) > 1 or (job_linked and (job_background or job_unknown)):
            return _StageAssignment("ambiguous", None, tuple(sorted(candidates)))
        if job_background and not job_unknown:
            return _StageAssignment("background", None, ())
        return _StageAssignment("unresolved", None, tuple(sorted(candidates)))

    def finish(self, source_paths: tuple[str, ...]) -> EventLogReport:
        stage_owners: dict[int, list[_JobRecord]] = defaultdict(list)
        for job in self.jobs.values():
            for stage_id in job.stage_ids:
                stage_owners[stage_id].append(job)

        stage_keys: set[tuple[int, int | None]] = set(self.stage_evidence)
        stage_keys.update((failure.stage_id, failure.attempt_id) for failure in self.stage_failures)
        stage_keys.update(
            (task.stage_id, task.stage_attempt_id)
            for task in self.tasks
            if task.stage_id is not None
        )
        for stage_id in stage_owners:
            if not any(key[0] == stage_id for key in stage_keys):
                stage_keys.add((stage_id, None))
        assignments = {
            key: self._classify_stage(key[0], key[1], stage_owners) for key in stage_keys
        }

        execution_ids = set(self.executions)
        execution_ids.update(
            job.execution_id
            for job in self.jobs.values()
            if job.link_status == "linked" and job.execution_id is not None
        )
        execution_ids.update(
            assignment.execution_id
            for assignment in assignments.values()
            if assignment.kind == "execution" and assignment.execution_id is not None
        )
        for execution_id in execution_ids:
            self.executions.setdefault(execution_id, _ExecutionState(execution_id))

        execution_metrics = {execution_id: _MutableMetrics() for execution_id in execution_ids}
        execution_failures = {execution_id: _MutableFailures() for execution_id in execution_ids}
        execution_jobs: dict[int, set[int]] = defaultdict(set)
        execution_stages: dict[int, set[int]] = defaultdict(set)
        execution_attempts: dict[int, set[tuple[int, int]]] = defaultdict(set)
        execution_issues: dict[int, list[str]] = defaultdict(list)
        stage_attribution_complete = {execution_id: True for execution_id in execution_ids}
        work = {
            "background": _MutableWork("background"),
            "ambiguous": _MutableWork("ambiguous"),
            "unresolved": _MutableWork("unresolved"),
        }

        for job in self.jobs.values():
            if job.link_status == "linked" and job.execution_id is not None:
                execution_id = job.execution_id
                execution_jobs[execution_id].add(job.job_id)
                execution_issues[execution_id].extend(job.issues)
                if job.failure_reason is not None:
                    failures = execution_failures[execution_id]
                    failures.job_failures += 1
                    failures.add_reason("job", str(job.job_id), job.failure_reason)
            else:
                bucket_name: WorkKind = (
                    "background" if job.link_status == "background" else "unresolved"
                )
                bucket = work[bucket_name]
                bucket.job_ids.add(job.job_id)
                if job.failure_reason is not None:
                    bucket.failures.job_failures += 1
                    bucket.failures.add_reason("job", str(job.job_id), job.failure_reason)

        for job_id, reason in self.orphan_job_failures.items():
            bucket = work["unresolved"]
            bucket.job_ids.add(job_id)
            bucket.failures.job_failures += 1
            bucket.failures.add_reason("job", str(job_id), reason)

        # Any stage referenced by a linked job must resolve back to that same execution.  A
        # conflict does not move metrics into that execution, and explicitly makes it partial.
        keys_by_stage: dict[int, list[tuple[int, int | None]]] = defaultdict(list)
        for key in stage_keys:
            keys_by_stage[key[0]].append(key)
        for job in self.jobs.values():
            if job.link_status != "linked" or job.execution_id is None:
                continue
            for stage_id in job.stage_ids:
                keys = keys_by_stage.get(stage_id, [(stage_id, None)])
                if any(
                    assignments[key].kind != "execution"
                    or assignments[key].execution_id != job.execution_id
                    for key in keys
                ):
                    stage_attribution_complete[job.execution_id] = False
                    issue = (
                        f"stage {stage_id} referenced by job {job.job_id} "
                        "is not uniquely attributable"
                    )
                    if issue not in execution_issues[job.execution_id]:
                        execution_issues[job.execution_id].append(issue)

        for key, assignment in assignments.items():
            stage_id, attempt_id = key
            if assignment.kind == "execution" and assignment.execution_id is not None:
                execution_stages[assignment.execution_id].add(stage_id)
                if attempt_id is not None:
                    execution_attempts[assignment.execution_id].add((stage_id, attempt_id))
                for evidence in self.stage_evidence.get(key, []):
                    execution_issues[assignment.execution_id].extend(evidence.issues)
            else:
                bucket_name = cast(WorkKind, assignment.kind)
                bucket = work[bucket_name]
                bucket.stage_ids.add(stage_id)
                bucket.candidates.update(assignment.candidates)
                for owner in stage_owners.get(stage_id, []):
                    bucket.job_ids.add(owner.job_id)
                for candidate in assignment.candidates:
                    stage_attribution_complete[candidate] = False

        task_quality: dict[int, bool] = {execution_id: True for execution_id in execution_ids}
        for task in self.tasks:
            if task.stage_id is None:
                assignment = _StageAssignment("unresolved", None, ())
            else:
                assignment = assignments[(task.stage_id, task.stage_attempt_id)]
            if assignment.kind == "execution" and assignment.execution_id is not None:
                execution_id = assignment.execution_id
                execution_metrics[execution_id].add(task.metrics)
                if task.unknown_fields or task.issues:
                    task_quality[execution_id] = False
                    for issue in task.issues:
                        rendered = f"task metrics incomplete: {issue}"
                        if rendered not in execution_issues[execution_id]:
                            execution_issues[execution_id].append(rendered)
                if task.failure_reason is not None:
                    failures = execution_failures[execution_id]
                    failures.task_failures += 1
                    identifier = (
                        f"{task.task_id}.{task.task_attempt}"
                        if task.task_id is not None and task.task_attempt is not None
                        else "unknown"
                    )
                    failures.add_reason("task", identifier, task.failure_reason)
            else:
                bucket_name = cast(WorkKind, assignment.kind)
                bucket = work[bucket_name]
                bucket.metrics.add(task.metrics)
                bucket.candidates.update(assignment.candidates)
                if task.stage_id is not None:
                    bucket.stage_ids.add(task.stage_id)
                if task.failure_reason is not None:
                    bucket.failures.task_failures += 1
                    identifier = (
                        f"{task.task_id}.{task.task_attempt}"
                        if task.task_id is not None and task.task_attempt is not None
                        else "unknown"
                    )
                    bucket.failures.add_reason("task", identifier, task.failure_reason)

        for failure in self.stage_failures:
            assignment = assignments[(failure.stage_id, failure.attempt_id)]
            if assignment.kind == "execution" and assignment.execution_id is not None:
                failures = execution_failures[assignment.execution_id]
                failures.stage_failures += 1
                identifier = f"{failure.stage_id}.{failure.attempt_id}"
                failures.add_reason("stage", identifier, failure.reason)
            else:
                bucket_name = cast(WorkKind, assignment.kind)
                bucket = work[bucket_name]
                bucket.failures.stage_failures += 1
                identifier = f"{failure.stage_id}.{failure.attempt_id}"
                bucket.failures.add_reason("stage", identifier, failure.reason)

        public_executions: list[SQLExecutionAttribution] = []
        for execution_id in sorted(execution_ids):
            state = self.executions[execution_id]
            metrics = execution_metrics[execution_id].freeze()
            failures = execution_failures[execution_id]
            if state.failure_reason is not None:
                failures.sql_execution_failures += 1
                failures.add_reason("SQL execution", str(execution_id), state.failure_reason)
            issues = list(state.issues)
            for issue in execution_issues[execution_id]:
                if issue not in issues:
                    issues.append(issue)
            duration_ms: int | None = None
            if state.start_time_ms is not None and state.end_time_ms is not None:
                if state.end_time_ms >= state.start_time_ms:
                    duration_ms = state.end_time_ms - state.start_time_ms
                else:
                    issues.append("SQL execution end time precedes start time")
            task_metrics_complete = metrics.complete and task_quality[execution_id]
            stage_complete = stage_attribution_complete[execution_id]
            is_complete = (
                state.start_seen
                and state.end_seen
                and stage_complete
                and task_metrics_complete
                and not issues
            )
            completeness = ExecutionCompleteness(
                sql_start_seen=state.start_seen,
                sql_end_seen=state.end_seen,
                stage_attribution_complete=stage_complete,
                task_metrics_complete=task_metrics_complete,
                complete=is_complete,
            )
            public_executions.append(
                SQLExecutionAttribution(
                    execution_id=execution_id,
                    root_execution_id=state.root_execution_id,
                    job_group_id=state.job_group_id,
                    description=state.description,
                    start_time_ms=state.start_time_ms,
                    end_time_ms=state.end_time_ms,
                    duration_ms=duration_ms,
                    job_ids=tuple(sorted(execution_jobs[execution_id])),
                    stage_ids=tuple(sorted(execution_stages[execution_id])),
                    stage_attempts=tuple(sorted(execution_attempts[execution_id])),
                    metrics=metrics,
                    failures=failures.freeze(),
                    completeness=completeness,
                    status="complete" if is_complete else "partial",
                    issues=tuple(issues),
                )
            )

        diagnostics = ParseDiagnostics(
            lines_read=self.lines_read,
            events_read=self.events_read,
            event_counts=dict(sorted(self.event_counts.items())),
            unknown_fields=tuple(sorted(self.unknown_fields)),
            issues=tuple(self.issues),
        )
        background = work["background"].freeze()
        unresolved = work["unresolved"].freeze()
        ambiguous = work["ambiguous"].freeze()
        report_complete = (
            not diagnostics.issues
            and not diagnostics.unknown_fields
            and all(execution.status == "complete" for execution in public_executions)
            and background.metrics.complete
            and ambiguous.metrics.complete
            and unresolved.metrics.complete
            and not ambiguous.stage_ids
            and ambiguous.failures.total == 0
            and not unresolved.stage_ids
            and unresolved.failures.total == 0
            and not unresolved.job_ids
        )
        return EventLogReport(
            schema_version=1,
            source_paths=source_paths,
            spark_version=self.spark_version,
            status="complete" if report_complete else "partial",
            executions=tuple(public_executions),
            background_work=background,
            ambiguous_work=ambiguous,
            unresolved_work=unresolved,
            diagnostics=diagnostics,
        )


def _expand_paths(paths: Iterable[PathLike]) -> tuple[Path, ...]:
    expanded: list[Path] = []
    seen: set[Path] = set()
    for supplied in paths:
        path = Path(supplied).expanduser().resolve()
        if path.is_dir():
            visible_files = [
                candidate
                for candidate in path.iterdir()
                if candidate.is_file()
                and not candidate.name.startswith(".")
                and not candidate.name.endswith(".crc")
            ]
            # A Spark rolling directory also contains a zero-byte appstatus_* marker.  When
            # canonical events_* segments exist, metadata and unrelated files are excluded.
            event_segments = [
                candidate for candidate in visible_files if candidate.name.startswith("events_")
            ]
            candidates = sorted(event_segments or visible_files, key=_natural_path_key)
        else:
            candidates = [path]
        for candidate in candidates:
            resolved = candidate.resolve()
            if resolved not in seen:
                expanded.append(resolved)
                seen.add(resolved)
    if not expanded:
        raise ValueError("no event-log files were supplied")
    return tuple(expanded)


def _natural_path_key(path: Path) -> tuple[str, ...]:
    """Order events_2 before events_10 while retaining deterministic lexical fallback."""

    return tuple(
        part.zfill(20) if part.isdecimal() else part.casefold()
        for part in re.split(r"(\d+)", path.name)
    )


def _open_event_log(path: Path) -> AbstractContextManager[IO[str]]:
    suffix = path.suffix.lower()
    if suffix == ".gz":
        return gzip.open(path, mode="rt", encoding="utf-8", newline="")
    if suffix in {".zst", ".zstd", ".lz4", ".lzf", ".snappy"}:
        raise ValueError(
            f"unsupported event-log compression for {path}; decompress it before parsing"
        )
    return path.open(mode="rt", encoding="utf-8", newline="")


def _events(path: Path, parser: _EventLogParser) -> Iterator[tuple[dict[str, object], str]]:
    with _open_event_log(path) as stream:
        for line_number, raw_line in enumerate(stream, start=1):
            parser.lines_read += 1
            line = raw_line.strip()
            if not line:
                continue
            try:
                decoded: object = json.loads(line)
            except json.JSONDecodeError as error:
                raise EventLogParseError(path, line_number, f"invalid JSON: {error.msg}") from error
            event = _object(decoded)
            if event is None:
                raise EventLogParseError(path, line_number, "event must be a JSON object")
            yield event, f"{path}:{line_number}"


def parse_event_logs(paths: Iterable[PathLike]) -> EventLogReport:
    """Parse ordered event-log files as one logical Spark application.

    ``paths`` may contain regular files, gzip files, and rolling-log directories.  Files are parsed
    in caller order; files discovered inside a directory are parsed in lexical order.
    """

    expanded = _expand_paths(paths)
    parser = _EventLogParser()
    for path in expanded:
        for event, location in _events(path, parser):
            parser.consume(event, location)
    return parser.finish(tuple(str(path) for path in expanded))


def parse_event_log(path: PathLike) -> EventLogReport:
    """Parse one event-log file or rolling-log directory supplied by the caller."""

    return parse_event_logs((path,))


__all__ = [
    "EventLogParseError",
    "EventLogReport",
    "ExecutionCompleteness",
    "FailureSummary",
    "MetricAggregate",
    "ParseDiagnostics",
    "SQLExecutionAttribution",
    "TaskMetricTotals",
    "WorkSummary",
    "parse_event_log",
    "parse_event_logs",
]
