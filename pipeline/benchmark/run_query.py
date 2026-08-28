"""Execute one reviewed workload in a fresh Spark application."""

from __future__ import annotations

import argparse
import json
import os
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from benchmark.collectors.resources import ResourceSampler, create_resource_source
from benchmark.parsers.plan import analyze_plan
from benchmark.runner.canonical import sha256_file, write_json
from benchmark.runner.config import SECRET_KEY_PATTERN, load_document, load_experiment
from benchmark.runner.sql import canonical_result_hash, render_sql, schema_hash
from pipeline.smoke.runtime_check import fingerprint

_BRONZE_TABLES = frozenset({"customers", "products", "orders", "order_items", "events"})
_TPCH_TABLES = frozenset(
    {"region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"}
)


def _table_identifier(logical_table: str, *, suite: str = "ecommerce") -> tuple[str, str]:
    """Return the catalog identifier and snapshot-manifest key for a logical binding."""

    if suite == "tpch":
        if logical_table not in _TPCH_TABLES:
            raise RuntimeError(f"invalid TPC-H logical table binding: {logical_table!r}")
        key = f"tpch.{logical_table}"
        return f"lakehouse.{key}", key
    if "." in logical_table:
        layer, separator, name = logical_table.partition(".")
        if not separator or layer not in {"bronze", "silver", "gold"} or not name:
            raise RuntimeError(f"invalid logical table binding: {logical_table!r}")
    elif logical_table in _BRONZE_TABLES:
        layer, name = "bronze", logical_table
    else:
        raise RuntimeError(
            f"derived logical table must include bronze/silver/gold layer: {logical_table!r}"
        )
    key = f"{layer}.{name}"
    return f"lakehouse.{key}", key


def _validate_binding(frame: Any, relation_binding: Mapping[str, Any]) -> None:
    fields = {field.name: field for field in frame.schema.fields}
    for required in relation_binding["required_columns"]:
        field = fields.get(required["name"])
        if field is None:
            raise RuntimeError(f"bound relation misses column {required['name']!r}")
        actual_type = field.dataType.simpleString().upper()
        if actual_type != required["type"].upper():
            raise RuntimeError(
                f"bound column {field.name!r} has type {actual_type}, expected {required['type']}"
            )
        if field.nullable != required["nullable"]:
            raise RuntimeError(
                f"bound column {field.name!r} nullable={field.nullable}, "
                f"expected {required['nullable']}"
            )


def _load_snapshot(spark: Any, table: str, snapshot_id: int) -> Any:
    return spark.read.format("iceberg").option("versionAsOf", str(snapshot_id)).load(table)


def _write_text_immutable(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = value.rstrip("\n") + "\n"
    try:
        with path.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(payload)
    except FileExistsError:
        if path.read_text(encoding="utf-8") != payload:
            raise FileExistsError(f"refusing to overwrite immutable artifact: {path}") from None


def _arm_worker_sampler(
    start_path: Path,
    started_path: Path,
    *,
    timeout_seconds: float = 15.0,
    clock: Any = time.monotonic,
    sleep: Any = time.sleep,
) -> None:
    """Synchronize the worker cgroup sampler before the measured terminal action."""

    if timeout_seconds <= 0:
        raise ValueError("worker sampler acknowledgement timeout must be positive")
    _write_text_immutable(start_path, "start")
    deadline = clock() + timeout_seconds
    while not started_path.is_file():
        if clock() >= deadline:
            raise TimeoutError(f"worker resource sampler did not acknowledge {start_path}")
        sleep(min(0.05, max(0.0, deadline - clock())))


def _safe_failure_message(error: BaseException) -> str:
    message = str(error).strip() or type(error).__name__
    sensitive = {
        value
        for key, value in os.environ.items()
        if value and SECRET_KEY_PATTERN.search(key) and len(value) >= 4
    }
    for value in sorted(sensitive, key=lambda item: (-len(item), item)):
        message = message.replace(value, "<redacted>")
    return message[:1_000]


def _execute_collect(
    spark: Any,
    rendered_sql: str,
    *,
    run_id: str,
) -> tuple[Any, list[Any], float, str, str]:
    spark.sparkContext.setJobGroup(
        run_id,
        f"measured terminal action for {run_id}",
        interruptOnCancel=True,
    )
    started = time.perf_counter_ns()
    frame = spark.sql(rendered_sql)
    initial_plan = frame._jdf.queryExecution().sparkPlan().toString()
    rows = frame.collect()
    wall_time_ms = (time.perf_counter_ns() - started) / 1_000_000
    final_plan = frame._jdf.queryExecution().executedPlan().toString()
    return frame, rows, wall_time_ms, initial_plan, final_plan


def _run_warmups(spark: Any, rendered_sql: str, *, run_id: str, warmup_runs: int) -> None:
    for index in range(warmup_runs):
        spark.sparkContext.setJobGroup(
            f"{run_id}:warmup:{index + 1}",
            f"non-measured warmup {index + 1}/{warmup_runs} for {run_id}",
            interruptOnCancel=True,
        )
        spark.sql(rendered_sql).collect()
    spark.catalog.clearCache()


def _failure_artifact(
    args: argparse.Namespace,
    error: BaseException,
    *,
    application_id: str | None,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "artifact_class": "benchmark-application-run-v1",
        "experiment_id": args.experiment_id,
        "run_id": args.run_id,
        "phase": args.phase,
        "pair_index": args.pair_index,
        "engine": args.engine,
        "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "status": "failed",
        "failure": {"class": type(error).__name__, "message": _safe_failure_message(error)},
        "application_id": application_id,
    }


def main() -> None:
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=["spark_baseline", "comet_accelerated"], required=True)
    parser.add_argument("--experiment-config", type=Path, required=True)
    parser.add_argument("--snapshot-manifest", type=Path, required=True)
    parser.add_argument(
        "--phase", choices=["correctness", "plan_capture", "measurement"], required=True
    )
    parser.add_argument("--experiment-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--pair-index", type=int)
    parser.add_argument("--warmup-runs", type=int, default=0)
    parser.add_argument("--worker-sampler-start-file", type=Path, required=True)
    parser.add_argument("--worker-sampler-started-file", type=Path, required=True)
    parser.add_argument("--worker-sampler-stop-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    if (args.phase == "measurement") != (args.pair_index is not None):
        parser.error("pair-index is required exactly for measurement runs")
    if args.warmup_runs < 0 or (args.phase != "measurement" and args.warmup_runs):
        parser.error("warmup-runs is non-negative and only valid for measurement")

    root = Path("/opt/lakehouse")
    schema_dir = root / "benchmark/schemas"
    spark: Any | None = None
    application_id: str | None = None
    try:
        config = load_experiment(args.experiment_config, schema_dir)
        if config["experiment"]["id"] != args.experiment_id:
            raise RuntimeError("application experiment ID differs from immutable config")
        workload_path = (root / config["workload"]["manifest_file"]).resolve()
        workload = load_document(workload_path, schema_dir / "workload-manifest.schema.json")
        sql_path = (root / config["workload"]["sql_file"]).resolve()
        if (workload_path.parent / workload["sql_file"]).resolve() != sql_path:
            raise RuntimeError("experiment and workload manifests select different SQL files")
        if workload["result_mode"] != "collect":
            raise RuntimeError("research application v1 supports only reviewed collect workloads")
        rendered_sql = render_sql(
            sql_path.read_text(encoding="utf-8"),
            workload["parameters"],
            config["workload"]["parameters"],
        )
        snapshot_manifest = json.loads(args.snapshot_manifest.read_text(encoding="utf-8"))
        if snapshot_manifest.get("status") != "passed":
            raise RuntimeError("Medallion snapshot manifest did not pass")
        dataset_manifest = root / config["workload"]["dataset_manifest"]
        if snapshot_manifest.get("dataset_manifest_sha256") != sha256_file(dataset_manifest):
            raise RuntimeError("Medallion snapshot manifest refers to different source data")

        spark = SparkSession.builder.appName(f"lakehouse-benchmark-{args.run_id}").getOrCreate()
        application_id = str(spark.sparkContext.applicationId)
        runtime = fingerprint(spark, engine=args.engine)
        snapshot_ids: list[int] = []
        for view_name, binding in workload["relation_bindings"].items():
            table, snapshot_key = _table_identifier(
                binding["logical_table"], suite=workload["suite"]
            )
            try:
                snapshot_id = int(snapshot_manifest["snapshots"][snapshot_key]["snapshot_id"])
            except (KeyError, TypeError, ValueError) as error:
                raise RuntimeError(f"missing pinned snapshot for {snapshot_key}") from error
            frame = _load_snapshot(spark, table, snapshot_id)
            _validate_binding(frame, binding)
            frame.createOrReplaceTempView(view_name)
            snapshot_ids.append(snapshot_id)

        _run_warmups(spark, rendered_sql, run_id=args.run_id, warmup_runs=args.warmup_runs)
        _arm_worker_sampler(
            args.worker_sampler_start_file,
            args.worker_sampler_started_file,
        )
        resource_sampler = ResourceSampler(
            create_resource_source(root_pid=os.getpid(), cpu_limit_cores=2.0)
        )
        try:
            resource_sampler.start()
            frame, rows, wall_time_ms, initial_plan, final_plan = _execute_collect(
                spark,
                rendered_sql,
                run_id=args.run_id,
            )
        finally:
            _write_text_immutable(args.worker_sampler_stop_file, "stop")
            resource_sampler.stop(timeout_seconds=10)
        resource_summary = resource_sampler.summary()
        actual_schema_json = frame.schema.json()
        actual_schema_hash = schema_hash(actual_schema_json)
        result_hash = canonical_result_hash(
            rows,
            ordered=workload["correctness"]["ordering"] == "ordered",
        )
        plan_analysis = analyze_plan(final_plan, comet_enabled=args.engine == "comet_accelerated")
        failures: list[str] = []
        if actual_schema_hash != workload["expected_schema_hash"]:
            failures.append(
                f"schema hash {actual_schema_hash} != expected {workload['expected_schema_hash']}"
            )
        if args.engine == "comet_accelerated" and plan_analysis["comet_native_operators"] == 0:
            failures.append("final plan contains no Comet native operator")

        artifact_dir = args.output.parent
        initial_path = artifact_dir / "initial-plan.txt"
        final_path = artifact_dir / "final-plan.txt"
        resource_path = artifact_dir / "driver-resource-samples.json"
        _write_text_immutable(initial_path, initial_plan)
        _write_text_immutable(final_path, final_plan)
        write_json(
            resource_path,
            {
                "schema_version": 1,
                "scope": "spark-client-driver-container",
                "samples": [sample.as_dict() for sample in resource_sampler.samples],
                "summary": resource_summary.as_dict(),
            },
        )
        result = {
            "schema_version": 1,
            "artifact_class": "benchmark-application-run-v1",
            "experiment_id": args.experiment_id,
            "run_id": args.run_id,
            "phase": args.phase,
            "pair_index": args.pair_index,
            "engine": args.engine,
            "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "status": "succeeded" if not failures else "invalid_result",
            "failure": (
                None
                if not failures
                else {"class": "CorrectnessGateError", "message": "; ".join(failures)}
            ),
            "application_id": application_id,
            "runtime": runtime,
            "workload": workload["suite"],
            "query_id": workload["id"],
            "storage_profile": workload["storage_profile"],
            "sql_sha256": sha256_file(sql_path),
            "workload_manifest_sha256": sha256_file(workload_path),
            "dataset_manifest_sha256": sha256_file(dataset_manifest),
            "iceberg_snapshot_ids": sorted(set(snapshot_ids)),
            "query_wall_time_ms": wall_time_ms,
            "driver_resources": resource_summary.as_dict(),
            "schema_json": actual_schema_json,
            "schema_sha256": actual_schema_hash,
            "row_count": len(rows),
            "canonical_result_sha256": result_hash,
            "plan_analysis": plan_analysis,
            "artifacts": {
                "initial_plan": initial_path.name,
                "final_plan": final_path.name,
                "driver_resource_samples": resource_path.name,
            },
        }
        write_json(args.output, result)
        if failures:
            raise SystemExit(1)
    except SystemExit:
        raise
    except BaseException as error:
        write_json(
            args.output,
            _failure_artifact(args, error, application_id=application_id),
        )
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
