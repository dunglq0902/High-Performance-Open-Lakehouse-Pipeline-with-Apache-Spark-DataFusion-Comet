from __future__ import annotations

import json
from pathlib import Path

from benchmark.parsers.eventlog import parse_event_log
from benchmark.runner.campaign import CampaignRun
from benchmark.runner.record import (
    RawRecordContext,
    build_failure_record,
    build_raw_record,
    load_raw_schema,
    select_measured_execution,
)

ROOT = Path(__file__).resolve().parents[1]
HASH = "a" * 64


def _event_log(path: Path) -> None:
    events = [
        {
            "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
            "executionId": 7,
            "rootExecutionId": 7,
            "jobGroupId": "measurement-p0001-o1-spark_baseline",
            "description": "SELECT",
            "time": 1000,
        },
        {
            "Event": "SparkListenerJobStart",
            "Job ID": 1,
            "Submission Time": 1001,
            "Stage IDs": [2],
            "Properties": {"spark.sql.execution.id": "7"},
        },
        {
            "Event": "SparkListenerStageSubmitted",
            "Stage Info": {"Stage ID": 2, "Stage Attempt ID": 0},
            "Properties": {"spark.sql.execution.id": "7"},
        },
        {
            "Event": "SparkListenerTaskEnd",
            "Stage ID": 2,
            "Stage Attempt ID": 0,
            "Task Type": "ResultTask",
            "Task End Reason": {"Reason": "ExceptionFailure", "Message": "retryable"},
            "Task Info": {"Task ID": 2, "Attempt": 0},
            "Task Metrics": {
                "Executor CPU Time": 1_000_000_000,
                "Executor Run Time": 50,
                "JVM GC Time": 2,
                "Memory Bytes Spilled": 0,
                "Disk Bytes Spilled": 0,
                "Shuffle Read Metrics": {
                    "Remote Bytes Read": 0,
                    "Local Bytes Read": 0,
                },
                "Shuffle Write Metrics": {"Shuffle Bytes Written": 0},
                "Input Metrics": {"Bytes Read": 0},
                "Output Metrics": {"Bytes Written": 0},
            },
        },
        {
            "Event": "SparkListenerTaskEnd",
            "Stage ID": 2,
            "Stage Attempt ID": 0,
            "Task Type": "ResultTask",
            "Task End Reason": {"Reason": "Success"},
            "Task Info": {"Task ID": 3, "Attempt": 0},
            "Task Metrics": {
                "Executor CPU Time": 2_000_000_000,
                "Executor Run Time": 100,
                "JVM GC Time": 5,
                "Memory Bytes Spilled": 0,
                "Disk Bytes Spilled": 0,
                "Shuffle Read Metrics": {
                    "Remote Bytes Read": 1_000_000,
                    "Local Bytes Read": 0,
                },
                "Shuffle Write Metrics": {"Shuffle Bytes Written": 2_000_000},
                "Input Metrics": {"Bytes Read": 3_000_000},
                "Output Metrics": {"Bytes Written": 0},
            },
        },
        {
            "Event": "SparkListenerStageCompleted",
            "Stage Info": {"Stage ID": 2, "Stage Attempt ID": 0, "Completion Time": 1090},
        },
        {
            "Event": "SparkListenerJobEnd",
            "Job ID": 1,
            "Completion Time": 1095,
            "Job Result": {"Result": "JobSucceeded"},
        },
        {
            "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd",
            "executionId": 7,
            "time": 1100,
        },
    ]
    path.write_text(
        "".join(json.dumps(event, separators=(",", ":")) + "\n" for event in events),
        encoding="utf-8",
    )


def test_raw_record_merges_event_metrics_and_resource_evidence(tmp_path: Path) -> None:
    event_path = tmp_path / "eventlog"
    _event_log(event_path)
    report = parse_event_log(event_path)
    run = CampaignRun(
        experiment_id="EXP-M02",
        run_id="measurement-p0001-o1-spark_baseline",
        phase="measurement",
        engine="spark_baseline",
        pair_index=1,
        order_index=1,
        warmup_runs=2,
        timeout_seconds=30,
    )
    application = {
        "experiment_id": run.experiment_id,
        "run_id": run.run_id,
        "phase": run.phase,
        "pair_index": run.pair_index,
        "engine": run.engine,
        "timestamp": "2026-08-27T00:00:00Z",
        "status": "succeeded",
        "failure": None,
        "workload": "micro",
        "query_id": "M02",
        "storage_profile": "ecommerce_iceberg_rest",
        "dataset_manifest_sha256": HASH,
        "sql_sha256": HASH,
        "iceberg_snapshot_ids": [1],
        "query_wall_time_ms": 120.0,
        "schema_sha256": HASH,
        "row_count": 1,
        "canonical_result_sha256": HASH,
        "runtime": {
            "spark_version": "4.1.3",
            "scala_version": "2.13.17",
            "java_version": "17.0.19+10",
            "comet_version": None,
            "iceberg_version": "1.11.0",
        },
        "plan_analysis": {
            "status": "complete",
            "total_operators": 1,
            "comet_native_operators": 0,
            "spark_fallback_operators": 0,
            "transition_count": 0,
            "native_subtree_count": 0,
            "native_coverage_ratio": None,
            "fallback_reasons": [],
            "unknown_nodes": [],
            "scan_implementations": ["BatchScan"],
        },
    }
    context = RawRecordContext(
        git_commit="abcdef0",
        container_image_digest=f"sha256:{HASH}",
        spark_conf_sha256=HASH,
        cpu_model="test",
        allocated_cores=2,
        cgroup_memory_limit_mib=5120,
        executor_heap_mib=2048,
        off_heap_mib=1024,
        event_log="eventlog",
        physical_plan="final-plan.txt",
        resource_samples="resources.json",
        stdout="stdout.log",
        stderr="stderr.log",
        executor_resources={
            "status": "complete",
            "memory_peak_bytes": 512 * 1024 * 1024,
            "cpu_peak_percent_of_limit": 75.0,
        },
    )

    record = build_raw_record(
        run,
        application,
        report,
        context,
        raw_schema=load_raw_schema(ROOT / "benchmark/schemas/raw-result.schema.json"),
    )

    assert record["metrics"]["sql_execution_id"] == 7
    # Failed/retried attempts consumed real resources and remain in the attribution.
    assert record["metrics"]["cpu_core_seconds"] == 3.0
    assert record["metrics"]["shuffle_read_mb"] == 1.0
    assert record["metrics"]["shuffle_write_mb"] == 2.0
    assert record["metrics"]["cgroup_memory_peak_mib"] == 512.0
    assert record["metrics"]["collector_status"] == "complete"


def test_execution_selection_uses_run_tag_and_aggregates_nested_root_family(
    tmp_path: Path,
) -> None:
    run_id = "measurement-p0001-o1-comet_accelerated"

    def execution_events(
        execution_id: int, root_id: int, stage_id: int, cpu_ns: int
    ) -> list[dict[str, object]]:
        return [
            {
                "Event": "SparkListenerSQLExecutionStart",
                "executionId": execution_id,
                "rootExecutionId": root_id,
                "jobGroupId": run_id,
                "time": 1_000 + execution_id,
            },
            {
                "Event": "SparkListenerJobStart",
                "Job ID": execution_id,
                "Stage IDs": [stage_id],
                "Properties": {"spark.sql.execution.id": str(execution_id)},
            },
            {
                "Event": "SparkListenerTaskEnd",
                "Stage ID": stage_id,
                "Stage Attempt ID": 0,
                "Task End Reason": {"Reason": "Success"},
                "Task Info": {"Task ID": execution_id, "Attempt": 0},
                "Task Metrics": {
                    "Executor CPU Time": cpu_ns,
                    "Executor Run Time": 10,
                    "JVM GC Time": 0,
                    "Memory Bytes Spilled": 0,
                    "Disk Bytes Spilled": 0,
                    "Shuffle Read Metrics": {"Remote Bytes Read": 0, "Local Bytes Read": 0},
                    "Shuffle Write Metrics": {"Shuffle Bytes Written": 0},
                    "Input Metrics": {"Bytes Read": 0},
                    "Output Metrics": {"Bytes Written": 0},
                },
            },
            {
                "Event": "SparkListenerSQLExecutionEnd",
                "executionId": execution_id,
                "time": 1_100 + execution_id,
            },
        ]

    events = [
        *execution_events(10, 10, 20, 1_000_000_000),
        *execution_events(11, 10, 21, 2_000_000_000),
        # A later execution from a warm-up/other job group must not be selected.
        *[
            {
                **event,
                **(
                    {"jobGroupId": f"{run_id}:warmup:1"}
                    if "SQLExecutionStart" in str(event.get("Event"))
                    else {}
                ),
            }
            for event in execution_events(99, 99, 29, 9_000_000_000)
        ],
    ]
    path = tmp_path / "nested-eventlog"
    path.write_text(
        "".join(json.dumps(event, separators=(",", ":")) + "\n" for event in events),
        encoding="utf-8",
    )

    execution = select_measured_execution(parse_event_log(path), run_id)
    assert execution.execution_id == 10
    assert execution.metrics.task_count == 2
    assert execution.metrics.executor_cpu_time_ns.value == 3_000_000_000


def test_timeout_record_remains_schema_valid() -> None:
    run = CampaignRun(
        experiment_id="EXP-M02",
        run_id="measurement-p0001-o1-spark_baseline",
        phase="measurement",
        engine="spark_baseline",
        pair_index=1,
        order_index=1,
        warmup_runs=2,
        timeout_seconds=30,
    )
    context = RawRecordContext(
        git_commit="abcdef0",
        container_image_digest=f"sha256:{HASH}",
        spark_conf_sha256=HASH,
        cpu_model="test",
        allocated_cores=2,
        cgroup_memory_limit_mib=5120,
        executor_heap_mib=2048,
        off_heap_mib=1024,
        event_log=None,
        physical_plan=None,
        resource_samples=None,
        stdout="stdout.log",
        stderr="stderr.log",
    )
    record = build_failure_record(
        run,
        context,
        timestamp="2026-08-27T00:00:00Z",
        status="timeout",
        failure_class="ApplicationTimeout",
        failure_message="deadline exceeded",
        workload="micro",
        query_id="M02",
        storage_profile="ecommerce_iceberg_rest",
        dataset_manifest_sha256=HASH,
        sql_sha256=HASH,
        iceberg_snapshot_ids=[1],
        runtime={
            "spark_version": "4.1.3",
            "scala_version": "2.13.17",
            "java_version": "17.0.19+10",
            "comet_version": None,
            "iceberg_version": "1.11.0",
        },
        raw_schema=load_raw_schema(ROOT / "benchmark/schemas/raw-result.schema.json"),
    )
    assert record["status"] == "timeout"
    assert record["metrics"]["collector_status"] == "unavailable"
