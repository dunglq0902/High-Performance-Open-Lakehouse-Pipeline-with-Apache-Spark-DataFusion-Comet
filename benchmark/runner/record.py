"""Build strict raw-result records from application, event-log, and cgroup artifacts."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

from benchmark.parsers.eventlog import (
    EventLogReport,
    ExecutionCompleteness,
    FailureSummary,
    MetricAggregate,
    SQLExecutionAttribution,
    TaskMetricTotals,
)
from benchmark.runner.campaign import CampaignError, CampaignRun

_DECIMAL_MB = 1_000_000
_BINARY_MIB = 1024 * 1024


@dataclass(frozen=True, slots=True)
class RawRecordContext:
    git_commit: str
    container_image_digest: str
    spark_conf_sha256: str
    cpu_model: str
    allocated_cores: int
    cgroup_memory_limit_mib: int
    executor_heap_mib: int
    off_heap_mib: int
    event_log: str | None
    physical_plan: str | None
    resource_samples: str | None
    stdout: str | None
    stderr: str | None
    executor_resources: Mapping[str, Any] | None = None


_TASK_METRIC_NAMES = (
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


def _root_execution_id(execution: SQLExecutionAttribution) -> int:
    return (
        execution.root_execution_id
        if execution.root_execution_id is not None
        else execution.execution_id
    )


def _aggregate_metric(executions: list[SQLExecutionAttribution], name: str) -> MetricAggregate:
    metrics = [getattr(execution.metrics, name) for execution in executions]
    values = [metric.value for metric in metrics]
    return MetricAggregate(
        value=sum(values) if all(value is not None for value in values) else None,
        unit=metrics[0].unit,
        observed_tasks=sum(metric.observed_tasks for metric in metrics),
        missing_tasks=sum(metric.missing_tasks for metric in metrics),
        complete=all(metric.complete for metric in metrics),
    )


def _aggregate_execution_family(
    root_id: int,
    run_id: str,
    executions: list[SQLExecutionAttribution],
) -> SQLExecutionAttribution:
    metrics = TaskMetricTotals(
        task_count=sum(execution.metrics.task_count for execution in executions),
        **{name: _aggregate_metric(executions, name) for name in _TASK_METRIC_NAMES},
    )
    start_time_ms = min(
        execution.start_time_ms for execution in executions if execution.start_time_ms is not None
    )
    end_time_ms = max(
        execution.end_time_ms for execution in executions if execution.end_time_ms is not None
    )
    completeness = ExecutionCompleteness(
        sql_start_seen=all(execution.completeness.sql_start_seen for execution in executions),
        sql_end_seen=all(execution.completeness.sql_end_seen for execution in executions),
        stage_attribution_complete=all(
            execution.completeness.stage_attribution_complete for execution in executions
        ),
        task_metrics_complete=metrics.complete,
        complete=all(execution.completeness.complete for execution in executions),
    )
    failures = FailureSummary(
        sql_execution_failures=sum(
            execution.failures.sql_execution_failures for execution in executions
        ),
        job_failures=sum(execution.failures.job_failures for execution in executions),
        stage_failures=sum(execution.failures.stage_failures for execution in executions),
        task_failures=sum(execution.failures.task_failures for execution in executions),
        reasons=tuple(
            sorted({reason for execution in executions for reason in execution.failures.reasons})
        ),
    )
    root = next(
        (execution for execution in executions if execution.execution_id == root_id),
        executions[0],
    )
    return SQLExecutionAttribution(
        execution_id=root_id,
        root_execution_id=root_id,
        job_group_id=run_id,
        description=root.description,
        start_time_ms=start_time_ms,
        end_time_ms=end_time_ms,
        duration_ms=end_time_ms - start_time_ms,
        job_ids=tuple(sorted({item for execution in executions for item in execution.job_ids})),
        stage_ids=tuple(sorted({item for execution in executions for item in execution.stage_ids})),
        stage_attempts=tuple(
            sorted({item for execution in executions for item in execution.stage_attempts})
        ),
        metrics=metrics,
        failures=failures,
        completeness=completeness,
        status="complete" if completeness.complete else "partial",
        issues=tuple(sorted({item for execution in executions for item in execution.issues})),
    )


def select_measured_execution(report: EventLogReport, run_id: str) -> SQLExecutionAttribution:
    """Select the execution family explicitly tagged with the measured run ID."""

    tagged = [
        execution
        for execution in report.executions
        if execution.job_group_id == run_id
        and execution.start_time_ms is not None
        and execution.end_time_ms is not None
    ]
    if not tagged:
        raise CampaignError(f"event log has no completed SQL execution tagged run_id={run_id!r}")
    roots = {_root_execution_id(execution) for execution in tagged}
    if len(roots) != 1:
        raise CampaignError(f"event log has multiple root execution families tagged {run_id!r}")
    root_id = next(iter(roots))
    family = [
        execution
        for execution in report.executions
        if _root_execution_id(execution) == root_id
        and execution.start_time_ms is not None
        and execution.end_time_ms is not None
        and execution.metrics.task_count > 0
    ]
    if not family:
        raise CampaignError(f"measured SQL execution family {root_id} has no task metrics")
    if len(family) == 1:
        return family[0]
    return _aggregate_execution_family(root_id, run_id, family)


def _metric_value(execution: SQLExecutionAttribution, name: str) -> int | None:
    aggregate = getattr(execution.metrics, name)
    value = aggregate.value
    return int(value) if value is not None else None


def _divide(value: int | float | None, divisor: int) -> float | None:
    return None if value is None else float(value) / divisor


def _collector_status(
    execution: SQLExecutionAttribution,
    executor_resources: Mapping[str, Any] | None,
) -> str:
    if execution.metrics.task_count == 0:
        return "unavailable"
    required_event_metrics = (
        execution.metrics.executor_cpu_time_ns.value,
        execution.metrics.jvm_gc_time_ms.value,
        execution.metrics.shuffle_read_bytes.value,
        execution.metrics.shuffle_write_bytes.value,
        execution.metrics.disk_spill_bytes.value,
    )
    resources_complete = (
        executor_resources is not None
        and executor_resources.get("status") == "complete"
        and executor_resources.get("memory_peak_bytes") is not None
        and executor_resources.get("cpu_peak_percent_of_limit") is not None
    )
    if (
        execution.completeness.complete
        and all(metric is not None for metric in required_event_metrics)
        and resources_complete
    ):
        return "complete"
    return "partial"


def _runtime(app: Mapping[str, Any]) -> dict[str, object]:
    runtime = app["runtime"]
    return {
        "spark_version": runtime["spark_version"],
        "scala_version": runtime["scala_version"],
        "java_version": runtime["java_version"],
        "comet_version": runtime["comet_version"],
        "iceberg_version": runtime["iceberg_version"],
    }


def build_raw_record(
    run: CampaignRun,
    application_result: Mapping[str, Any],
    event_report: EventLogReport,
    context: RawRecordContext,
    *,
    raw_schema: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Merge independently captured evidence into one schema-valid immutable run record."""

    bound_fields = {
        "experiment_id": run.experiment_id,
        "run_id": run.run_id,
        "phase": run.phase,
        "pair_index": run.pair_index,
        "engine": run.engine,
    }
    mismatches = [
        field
        for field, expected in bound_fields.items()
        if application_result.get(field) != expected
    ]
    if mismatches:
        raise CampaignError(f"application result disagrees with campaign fields: {mismatches}")
    status = application_result.get("status")
    if status not in {"succeeded", "invalid_result"}:
        raise CampaignError(f"application result cannot be promoted to raw record: {status!r}")
    execution = select_measured_execution(event_report, run.run_id)
    terminal_failures = execution.failures.sql_execution_failures + execution.failures.job_failures
    if terminal_failures and status == "succeeded":
        raise CampaignError("successful application has terminal SQL/job failures")

    executor_resources = context.executor_resources
    cpu_peak = (
        executor_resources.get("cpu_peak_percent_of_limit")
        if executor_resources is not None
        else None
    )
    memory_peak_bytes = (
        executor_resources.get("memory_peak_bytes") if executor_resources is not None else None
    )
    succeeded = status == "succeeded"
    record: dict[str, Any] = {
        "schema_version": 1,
        **bound_fields,
        "timestamp": application_result["timestamp"],
        "status": status,
        "failure": application_result.get("failure"),
        "workload": application_result["workload"],
        "query_id": application_result["query_id"],
        "storage_profile": application_result["storage_profile"],
        "provenance": {
            "git_commit": context.git_commit,
            "container_image_digest": context.container_image_digest,
            "dataset_manifest_sha256": application_result["dataset_manifest_sha256"],
            "spark_conf_sha256": context.spark_conf_sha256,
            "sql_sha256": application_result["sql_sha256"],
            "iceberg_snapshot_ids": application_result["iceberg_snapshot_ids"],
        },
        "runtime": _runtime(application_result),
        "resources": {
            "cpu_model": context.cpu_model,
            "allocated_cores": context.allocated_cores,
            "cgroup_memory_limit_mib": context.cgroup_memory_limit_mib,
            "executor_heap_mib": context.executor_heap_mib,
            "off_heap_mib": context.off_heap_mib,
        },
        "metrics": {
            "sql_execution_id": execution.execution_id,
            "query_wall_time_ms": application_result["query_wall_time_ms"],
            "sql_execution_time_ms": execution.duration_ms,
            "cpu_core_seconds": _divide(
                _metric_value(execution, "executor_cpu_time_ns"), 1_000_000_000
            ),
            "cpu_peak_percent_of_limit": cpu_peak,
            "cgroup_memory_peak_mib": _divide(memory_peak_bytes, _BINARY_MIB),
            "jvm_gc_time_ms": _metric_value(execution, "jvm_gc_time_ms"),
            "shuffle_read_mb": _divide(_metric_value(execution, "shuffle_read_bytes"), _DECIMAL_MB),
            "shuffle_write_mb": _divide(
                _metric_value(execution, "shuffle_write_bytes"), _DECIMAL_MB
            ),
            "disk_spill_mb": _divide(_metric_value(execution, "disk_spill_bytes"), _DECIMAL_MB),
            "collector_status": _collector_status(execution, executor_resources),
        },
        "plan_analysis": application_result["plan_analysis"],
        "correctness": {
            "status": "passed" if succeeded else "failed",
            "schema_sha256": application_result.get("schema_sha256"),
            "row_count": application_result.get("row_count"),
            "canonical_result_sha256": application_result.get("canonical_result_sha256"),
        },
        "artifacts": {
            "event_log": context.event_log,
            "physical_plan": context.physical_plan,
            "resource_samples": context.resource_samples,
            "stdout": context.stdout,
            "stderr": context.stderr,
        },
    }
    if "scale_factor" in application_result:
        record["scale_factor"] = application_result["scale_factor"]
    if raw_schema is not None:
        errors = sorted(
            Draft202012Validator(raw_schema, format_checker=FormatChecker()).iter_errors(record),
            key=lambda item: list(item.path),
        )
        if errors:
            raise CampaignError(
                "built raw record is invalid: "
                + "; ".join(
                    f"{'/'.join(map(str, error.path)) or '<root>'}: {error.message}"
                    for error in errors
                )
            )
    return record


def load_raw_schema(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise CampaignError("raw-result schema root must be an object")
    return value


def build_failure_record(
    run: CampaignRun,
    context: RawRecordContext,
    *,
    timestamp: str,
    status: str,
    failure_class: str,
    failure_message: str,
    workload: str,
    query_id: str,
    storage_profile: str,
    dataset_manifest_sha256: str,
    sql_sha256: str,
    iceberg_snapshot_ids: list[int],
    runtime: Mapping[str, object],
    raw_schema: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a lossless raw record when no complete application result exists."""

    if status not in {"failed", "timeout", "invalid_result", "invalid_environment"}:
        raise CampaignError(f"invalid failure status: {status!r}")
    record: dict[str, Any] = {
        "schema_version": 1,
        "experiment_id": run.experiment_id,
        "run_id": run.run_id,
        "pair_index": run.pair_index,
        "phase": run.phase,
        "timestamp": timestamp,
        "status": status,
        "failure": {"class": failure_class, "message": failure_message},
        "engine": run.engine,
        "workload": workload,
        "query_id": query_id,
        "storage_profile": storage_profile,
        "provenance": {
            "git_commit": context.git_commit,
            "container_image_digest": context.container_image_digest,
            "dataset_manifest_sha256": dataset_manifest_sha256,
            "spark_conf_sha256": context.spark_conf_sha256,
            "sql_sha256": sql_sha256,
            "iceberg_snapshot_ids": iceberg_snapshot_ids,
        },
        "runtime": dict(runtime),
        "resources": {
            "cpu_model": context.cpu_model,
            "allocated_cores": context.allocated_cores,
            "cgroup_memory_limit_mib": context.cgroup_memory_limit_mib,
            "executor_heap_mib": context.executor_heap_mib,
            "off_heap_mib": context.off_heap_mib,
        },
        "metrics": {
            "sql_execution_id": None,
            "query_wall_time_ms": None,
            "sql_execution_time_ms": None,
            "cpu_core_seconds": None,
            "cpu_peak_percent_of_limit": None,
            "cgroup_memory_peak_mib": None,
            "jvm_gc_time_ms": None,
            "shuffle_read_mb": None,
            "shuffle_write_mb": None,
            "disk_spill_mb": None,
            "collector_status": "unavailable",
        },
        "plan_analysis": {
            "status": "unavailable",
            "total_operators": 0,
            "comet_native_operators": 0,
            "spark_fallback_operators": 0,
            "transition_count": 0,
            "native_subtree_count": 0,
            "native_coverage_ratio": None,
            "fallback_reasons": [],
            "unknown_nodes": [],
            "scan_implementations": [],
        },
        "correctness": {
            "status": "not_checked",
            "schema_sha256": None,
            "row_count": None,
            "canonical_result_sha256": None,
        },
        "artifacts": {
            "event_log": context.event_log,
            "physical_plan": context.physical_plan,
            "resource_samples": context.resource_samples,
            "stdout": context.stdout,
            "stderr": context.stderr,
        },
    }
    if raw_schema is not None:
        errors = list(
            Draft202012Validator(raw_schema, format_checker=FormatChecker()).iter_errors(record)
        )
        if errors:
            raise CampaignError(
                "built failure record is invalid: " + "; ".join(error.message for error in errors)
            )
    return record
