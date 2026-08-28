from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

from benchmark.parsers.eventlog import (
    EventLogParseError,
    EventLogReport,
    parse_event_log,
    parse_event_logs,
)


def _write_events(path: Path, events: list[dict[str, object]]) -> Path:
    path.write_text(
        "".join(json.dumps(event, separators=(",", ":")) + "\n" for event in events),
        encoding="utf-8",
    )
    return path


def _complete_task_metrics(*, cpu_ns: int = 100, run_ms: int = 20) -> dict[str, object]:
    return {
        "Executor CPU Time": cpu_ns,
        "Executor Run Time": run_ms,
        "JVM GC Time": 3,
        "Memory Bytes Spilled": 5,
        "Disk Bytes Spilled": 7,
        "Shuffle Read Metrics": {
            "Remote Bytes Read": 11,
            "Local Bytes Read": 13,
            "Records Read": 2,
        },
        "Shuffle Write Metrics": {
            "Shuffle Bytes Written": 17,
            "Shuffle Write Time": 1,
            "Shuffle Records Written": 2,
        },
        "Input Metrics": {"Bytes Read": 19, "Records Read": 2},
        "Output Metrics": {"Bytes Written": 23, "Records Written": 2},
    }


def _sql_start(execution_id: int, time: int = 1_000) -> dict[str, object]:
    return {
        "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
        "executionId": execution_id,
        "rootExecutionId": execution_id,
        "description": f"query-{execution_id}",
        "time": time,
    }


def _sql_end(execution_id: int, time: int = 1_100) -> dict[str, object]:
    return {
        "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd",
        "executionId": execution_id,
        "time": time,
        "errorMessage": "",
    }


def _job_start(job_id: int, execution_id: int | None, stage_id: int) -> dict[str, object]:
    properties = {} if execution_id is None else {"spark.sql.execution.id": str(execution_id)}
    return {
        "Event": "SparkListenerJobStart",
        "Job ID": job_id,
        "Stage IDs": [stage_id],
        "Stage Infos": [{"Stage ID": stage_id, "Stage Attempt ID": 0}],
        "Properties": properties,
    }


def _task_end(
    stage_id: int,
    task_id: int,
    *,
    metrics: dict[str, object] | None = None,
    reason: object = None,
) -> dict[str, object]:
    return {
        "Event": "SparkListenerTaskEnd",
        "Stage ID": stage_id,
        "Stage Attempt ID": 0,
        "Task Type": "ResultTask",
        "Task End Reason": {"Reason": "Success"} if reason is None else reason,
        "Task Info": {"Task ID": task_id, "Attempt": 0},
        "Task Metrics": _complete_task_metrics() if metrics is None else metrics,
    }


def test_attributes_jobs_stages_tasks_and_all_requested_metrics(tmp_path: Path) -> None:
    event_log = _write_events(
        tmp_path / "events",
        [
            {"Event": "SparkListenerLogStart", "Spark Version": "4.0.1"},
            _sql_start(7),
            _job_start(12, 7, 3),
            {
                "Event": "SparkListenerStageSubmitted",
                "Stage Info": {"Stage ID": 3, "Stage Attempt ID": 0},
                "Properties": {"spark.sql.execution.id": "7"},
            },
            _task_end(3, 31),
            _task_end(3, 32, metrics=_complete_task_metrics(cpu_ns=200, run_ms=30)),
            {
                "Event": "SparkListenerStageCompleted",
                "Stage Info": {
                    "Stage ID": 3,
                    "Stage Attempt ID": 0,
                    "Failure Reason": None,
                },
            },
            {
                "Event": "SparkListenerJobEnd",
                "Job ID": 12,
                "Job Result": {"Result": "JobSucceeded"},
            },
            _sql_end(7),
        ],
    )

    report = parse_event_log(event_log)

    assert isinstance(report, EventLogReport)
    assert report.status == "complete"
    assert report.spark_version == "4.0.1"
    execution = report.execution(7)
    assert execution is not None
    assert execution.status == "complete"
    assert execution.duration_ms == 100
    assert execution.job_ids == (12,)
    assert execution.stage_ids == (3,)
    assert execution.stage_attempts == ((3, 0),)
    assert execution.metrics.task_count == 2
    assert execution.metrics.executor_cpu_time_ns.value == 300
    assert execution.metrics.executor_cpu_time_ns.unit == "ns"
    assert execution.metrics.executor_run_time_ms.value == 50
    assert execution.metrics.jvm_gc_time_ms.value == 6
    assert execution.metrics.shuffle_read_bytes.value == 48
    assert execution.metrics.shuffle_write_bytes.value == 34
    assert execution.metrics.input_bytes.value == 38
    assert execution.metrics.output_bytes.value == 46
    assert execution.metrics.memory_spill_bytes.value == 10
    assert execution.metrics.disk_spill_bytes.value == 14
    assert execution.metrics.complete
    assert execution.failures.total == 0
    assert report.diagnostics.events_read == 9
    assert report.diagnostics.unknown_fields == ()
    assert report.to_dict()["schema_version"] == 1


def test_background_work_is_preserved_without_contaminating_sql_metrics(tmp_path: Path) -> None:
    event_log = _write_events(
        tmp_path / "events",
        [
            _sql_start(1),
            _job_start(10, 1, 2),
            _task_end(2, 20, metrics=_complete_task_metrics(cpu_ns=40)),
            _sql_end(1),
            _job_start(11, None, 9),
            _task_end(9, 90, metrics=_complete_task_metrics(cpu_ns=900)),
        ],
    )

    report = parse_event_log(event_log)

    execution = report.execution(1)
    assert execution is not None
    assert execution.metrics.task_count == 1
    assert execution.metrics.executor_cpu_time_ns.value == 40
    assert report.background_work.job_ids == (11,)
    assert report.background_work.stage_ids == (9,)
    assert report.background_work.metrics.task_count == 1
    assert report.background_work.metrics.executor_cpu_time_ns.value == 900
    assert report.ambiguous_work.metrics.task_count == 0
    assert report.status == "complete"


def test_stage_properties_can_attribute_tasks_when_job_events_are_absent(tmp_path: Path) -> None:
    event_log = _write_events(
        tmp_path / "events",
        [
            _sql_start(14),
            {
                "Event": "SparkListenerStageSubmitted",
                "Stage Info": {"Stage ID": 4, "Stage Attempt ID": 1},
                "Properties": {"spark.sql.execution.id": "14"},
            },
            {
                **_task_end(4, 40),
                "Stage Attempt ID": 1,
            },
            _sql_end(14),
        ],
    )

    report = parse_event_log(event_log)

    execution = report.execution(14)
    assert execution is not None
    assert execution.job_ids == ()
    assert execution.stage_ids == (4,)
    assert execution.stage_attempts == ((4, 1),)
    assert execution.metrics.task_count == 1
    assert execution.status == "complete"


def test_shared_stage_with_conflicting_execution_owners_is_never_misattributed(
    tmp_path: Path,
) -> None:
    event_log = _write_events(
        tmp_path / "events",
        [
            _sql_start(1),
            _sql_start(2),
            _job_start(10, 1, 5),
            _job_start(20, 2, 5),
            _task_end(5, 50, metrics=_complete_task_metrics(cpu_ns=999)),
            _sql_end(1),
            _sql_end(2),
        ],
    )

    report = parse_event_log(event_log)

    first = report.execution(1)
    second = report.execution(2)
    assert first is not None and second is not None
    assert first.metrics.task_count == 0
    assert second.metrics.task_count == 0
    assert first.status == "partial"
    assert second.status == "partial"
    assert not first.completeness.stage_attribution_complete
    assert not second.completeness.stage_attribution_complete
    assert report.ambiguous_work.job_ids == (10, 20)
    assert report.ambiguous_work.stage_ids == (5,)
    assert report.ambiguous_work.candidate_execution_ids == (1, 2)
    assert report.ambiguous_work.metrics.executor_cpu_time_ns.value == 999
    assert report.status == "partial"


def test_missing_and_future_metric_fields_are_visible_not_zero_filled(tmp_path: Path) -> None:
    incomplete_metrics = _complete_task_metrics()
    del incomplete_metrics["Executor CPU Time"]
    del incomplete_metrics["Output Metrics"]
    incomplete_metrics["Future Counter"] = 42
    event_log = _write_events(
        tmp_path / "events",
        [
            _sql_start(4),
            _job_start(40, 4, 6),
            _task_end(6, 60, metrics=incomplete_metrics),
            _sql_end(4),
        ],
    )

    report = parse_event_log(event_log)

    execution = report.execution(4)
    assert execution is not None
    cpu = execution.metrics.executor_cpu_time_ns
    assert cpu.value is None
    assert cpu.observed_tasks == 0
    assert cpu.missing_tasks == 1
    assert not cpu.complete
    assert execution.metrics.output_bytes.value is None
    assert not execution.completeness.task_metrics_complete
    assert execution.status == "partial"
    assert "Task Metrics.Future Counter" in report.diagnostics.unknown_fields
    assert report.status == "partial"


def test_spark_4_camel_case_variants_and_fully_qualified_names(tmp_path: Path) -> None:
    camel_metrics: dict[str, object] = {
        "executorCpuTime": 123,
        "executorRunTime": 8,
        "jvmGcTime": 1,
        "peakExecutionMemory": 100,
        "peakOnHeapExecutionMemory": 60,
        "peakOffHeapExecutionMemory": 40,
        "memoryBytesSpilled": 2,
        "diskBytesSpilled": 3,
        "shuffleReadMetrics": {
            "totalBytesRead": 44,
            "recordsRead": 1,
            "remoteRequestsDuration": 2,
            "pushBasedShuffle": {
                "mergedRemoteBytesRead": 0,
                "mergedLocalBytesRead": 0,
                "mergedFetchFallbackCount": 0,
            },
        },
        "shuffleWriteMetrics": {"shuffleBytesWritten": 45},
        "inputMetrics": {"bytesRead": 46},
        "outputMetrics": {"bytesWritten": 47},
    }
    events: list[dict[str, object]] = [
        {
            "eventName": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
            "executionId": 88,
            "time": 2_000,
            "jobGroupId": "benchmark-88",
            "queryId": {"leastSignificantBits": 1, "mostSignificantBits": 2},
        },
        {
            "eventName": "org.apache.spark.scheduler.SparkListenerJobStart",
            "jobId": 8,
            "stageIds": [9],
            "properties": {"spark.sql.execution.id": 88},
        },
        {
            "eventName": "org.apache.spark.scheduler.SparkListenerTaskEnd",
            "stageId": 9,
            "stageAttemptId": 0,
            "taskType": "ResultTask",
            "taskEndReason": {"reason": "Success"},
            "taskInfo": {"taskId": 99, "attemptNumber": 0},
            "taskExecutorMetrics": {"JVMHeapMemory": 1000},
            "taskMetrics": camel_metrics,
        },
        {
            "eventName": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd",
            "executionId": 88,
            "time": 2_010,
            "queryId": {"leastSignificantBits": 1, "mostSignificantBits": 2},
        },
    ]

    report = parse_event_log(_write_events(tmp_path / "events", events))

    execution = report.execution(88)
    assert execution is not None
    assert execution.status == "complete"
    assert execution.metrics.executor_cpu_time_ns.value == 123
    assert execution.metrics.shuffle_read_bytes.value == 44
    assert execution.metrics.shuffle_write_bytes.value == 45
    assert execution.metrics.input_bytes.value == 46
    assert execution.metrics.output_bytes.value == 47
    assert report.diagnostics.unknown_fields == ()


def test_failures_are_aggregated_at_each_listener_level(tmp_path: Path) -> None:
    event_log = _write_events(
        tmp_path / "events",
        [
            _sql_start(5),
            _job_start(50, 5, 7),
            _task_end(7, 70, reason={"Reason": "ExceptionFailure", "Message": "boom"}),
            {
                "Event": "SparkListenerStageCompleted",
                "Stage Info": {
                    "Stage ID": 7,
                    "Stage Attempt ID": 0,
                    "Failure Reason": "fetch failed",
                },
            },
            {
                "Event": "SparkListenerJobEnd",
                "Job ID": 50,
                "Job Result": {"Result": "JobFailed", "Message": "stage failed"},
            },
            {
                "Event": "SparkListenerSQLExecutionEnd",
                "executionId": 5,
                "time": 1_100,
                "errorMessage": "query failed",
            },
        ],
    )

    report = parse_event_log(event_log)

    execution = report.execution(5)
    assert execution is not None
    assert execution.failures.sql_execution_failures == 1
    assert execution.failures.job_failures == 1
    assert execution.failures.stage_failures == 1
    assert execution.failures.task_failures == 1
    assert execution.failures.total == 4
    assert len(execution.failures.reasons) == 4
    # Failure is an execution outcome, not a parser-completeness failure.
    assert execution.status == "complete"


def test_missing_job_properties_put_work_in_unresolved_bucket(tmp_path: Path) -> None:
    job_without_properties = {
        "Event": "SparkListenerJobStart",
        "Job ID": 3,
        "Stage IDs": [4],
    }
    event_log = _write_events(
        tmp_path / "events",
        [job_without_properties, _task_end(4, 44)],
    )

    report = parse_event_log(event_log)

    assert report.executions == ()
    assert report.unresolved_work.job_ids == (3,)
    assert report.unresolved_work.stage_ids == (4,)
    assert report.unresolved_work.metrics.task_count == 1
    assert report.status == "partial"
    assert any("no readable Properties" in issue for issue in report.diagnostics.issues)


def test_multiple_files_directory_and_gzip_are_supported(tmp_path: Path) -> None:
    rolling = tmp_path / "rolling"
    rolling.mkdir()
    _write_events(rolling / "events_1", [_sql_start(6), _job_start(60, 6, 8)])
    second_payload = "".join(json.dumps(event) + "\n" for event in [_task_end(8, 80), _sql_end(6)])
    with gzip.open(rolling / "events_2.gz", "wt", encoding="utf-8") as stream:
        stream.write(second_payload)
    (rolling / "appstatus_application-1").write_bytes(b"")
    (rolling / ".events_2.gz.crc").write_text("ignored", encoding="utf-8")

    report = parse_event_log(rolling)

    execution = report.execution(6)
    assert execution is not None
    assert execution.metrics.task_count == 1
    assert len(report.source_paths) == 2
    assert report.status == "complete"

    direct = parse_event_logs((rolling / "events_1", rolling / "events_2.gz"))
    assert direct.execution(6) == execution


def test_invalid_ndjson_fails_with_exact_source_line(tmp_path: Path) -> None:
    event_log = tmp_path / "events"
    event_log.write_text(
        json.dumps(_sql_start(1)) + "\n" + '{"Event":"SparkListenerJobStart"\n',
        encoding="utf-8",
    )

    with pytest.raises(EventLogParseError) as captured:
        parse_event_log(event_log)

    assert captured.value.path == event_log.resolve()
    assert captured.value.line_number == 2
    assert "invalid JSON" in str(captured.value)


def test_duplicate_task_end_is_not_double_counted_and_marks_report_partial(
    tmp_path: Path,
) -> None:
    task = _task_end(2, 20)
    event_log = _write_events(
        tmp_path / "events",
        [_sql_start(1), _job_start(10, 1, 2), task, task, _sql_end(1)],
    )

    report = parse_event_log(event_log)

    execution = report.execution(1)
    assert execution is not None
    assert execution.metrics.task_count == 1
    assert report.status == "partial"
    assert any("duplicate TaskEnd" in issue for issue in report.diagnostics.issues)
