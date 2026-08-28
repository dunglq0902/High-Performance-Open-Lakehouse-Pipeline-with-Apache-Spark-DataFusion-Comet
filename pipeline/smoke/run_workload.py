"""Run one correctness/plan smoke in a fresh Spark application."""

from __future__ import annotations

import argparse
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from benchmark.parsers.plan import analyze_plan
from benchmark.runner.canonical import sha256_file, write_json
from benchmark.runner.config import load_document, load_experiment
from benchmark.runner.sql import canonical_result_hash, render_sql, schema_hash
from pipeline.smoke.runtime_check import fingerprint


def _validate_binding(frame: Any, relation_binding: Mapping[str, Any]) -> None:
    schema = frame.schema
    fields = {field.name: field for field in schema.fields}
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


def _load_iceberg_snapshot(spark: Any, table: str, snapshot_id: int) -> Any:
    """Use Spark's built-in time-travel option required by Spark 4.1/Iceberg 1.11."""

    return spark.read.format("iceberg").option("versionAsOf", str(snapshot_id)).load(table)


def main() -> None:
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=["spark_baseline", "comet_accelerated"], required=True)
    parser.add_argument("--experiment-config", type=Path, required=True)
    parser.add_argument("--snapshot-id", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    root = Path("/opt/lakehouse")
    schema_dir = root / "benchmark/schemas"
    config = load_experiment(args.experiment_config, schema_dir)
    workload_path = (root / config["workload"]["manifest_file"]).resolve()
    workload = load_document(workload_path, schema_dir / "workload-manifest.schema.json")
    sql_path = (root / config["workload"]["sql_file"]).resolve()
    if (workload_path.parent / workload["sql_file"]).resolve() != sql_path:
        raise RuntimeError("experiment and workload manifests select different SQL files")
    if config["workload"]["storage_profile"] != "ecommerce_iceberg_rest":
        raise RuntimeError("native readiness smoke requires ecommerce_iceberg_rest storage")
    if config["workload"]["data_path"] != "lakehouse.bronze.orders":
        raise RuntimeError("native readiness smoke requires lakehouse.bronze.orders")
    rendered_sql = render_sql(
        sql_path.read_text(encoding="utf-8"),
        workload["parameters"],
        config["workload"]["parameters"],
    )

    spark = SparkSession.builder.appName(f"lakehouse-smoke-{args.engine}").getOrCreate()
    try:
        runtime = fingerprint(spark, engine=args.engine)
        bound_orders = _load_iceberg_snapshot(
            spark,
            config["workload"]["data_path"],
            args.snapshot_id,
        )
        _validate_binding(bound_orders, workload["relation_bindings"]["bench_orders"])
        bound_orders.createOrReplaceTempView("bench_orders")
        started = time.perf_counter_ns()
        frame = spark.sql(rendered_sql)
        initial_plan = frame._jdf.queryExecution().sparkPlan().toString()
        rows = frame.collect()
        wall_time_ms = (time.perf_counter_ns() - started) / 1_000_000
        final_plan = frame._jdf.queryExecution().executedPlan().toString()

        actual_schema_json = frame.schema.json()
        actual_schema_hash = schema_hash(actual_schema_json)
        result_hash = canonical_result_hash(
            rows,
            ordered=workload["correctness"]["ordering"] == "ordered",
        )
        plan_analysis = analyze_plan(final_plan, comet_enabled=args.engine == "comet_accelerated")
        status = "passed"
        failures: list[str] = []
        if actual_schema_hash != workload["expected_schema_hash"]:
            status = "failed"
            failures.append(
                f"schema hash {actual_schema_hash} != expected {workload['expected_schema_hash']}"
            )
        if args.engine == "comet_accelerated" and plan_analysis["comet_native_operators"] == 0:
            status = "failed"
            failures.append("final plan contains no Comet native operator")

        artifact_dir = args.output.parent
        artifact_dir.mkdir(parents=True, exist_ok=True)
        initial_path = artifact_dir / "initial-plan.txt"
        final_path = artifact_dir / "final-plan.txt"
        initial_path.write_text(initial_plan + "\n", encoding="utf-8", newline="\n")
        final_path.write_text(final_plan + "\n", encoding="utf-8", newline="\n")
        write_json(
            args.output,
            {
                "schema_version": 1,
                "artifact_class": "readiness-smoke-not-benchmark-data",
                "status": status,
                "failures": failures,
                "engine": args.engine,
                "runtime": runtime,
                "iceberg_snapshot_id": args.snapshot_id,
                "workload_id": workload["id"],
                "sql_sha256": sha256_file(sql_path),
                "workload_manifest_sha256": sha256_file(workload_path),
                "schema_json": actual_schema_json,
                "schema_sha256": actual_schema_hash,
                "row_count": len(rows),
                "canonical_result_sha256": result_hash,
                "plan_analysis": plan_analysis,
                "diagnostic_wall_time_ms": wall_time_ms,
                "artifacts": {
                    "initial_plan": initial_path.name,
                    "final_plan": final_path.name,
                },
            },
        )
        if status != "passed":
            raise SystemExit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
