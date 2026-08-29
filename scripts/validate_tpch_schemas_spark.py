"""Analyze the reviewed TPC-H-derived SQL against explicit empty Spark schemas."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from benchmark.runner.config import load_document, load_experiment
from benchmark.runner.sql import canonical_schema_json, render_sql
from pipeline.medallion.build import _tpch_spark_schema

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_ROOT = ROOT / "benchmark/schemas"
CONFIGS = tuple(sorted((ROOT / "benchmark/configs").glob("benchmark-laptop-tpch-*.yaml")))


def _configure_schema_check(builder: Any) -> Any:
    """Configure the isolated analyzer without creating research event logs.

    The container inherits ``spark.eventLog.enabled=true`` from ``spark-defaults.conf``. That is
    required for measured campaigns, but this local, read-only analyzer has no event-log consumer.
    Disabling it also avoids Hadoop trying to chmod a newly created event-log directory on a
    Docker Desktop bind mount, which Windows filesystems do not support.
    """
    return (
        builder.master("local[1]")
        .appName("validate-tpch-workload-schemas")
        .config("spark.eventLog.enabled", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
    )


def main() -> None:
    from pyspark.sql import SparkSession

    spark = _configure_schema_check(SparkSession.builder).getOrCreate()
    results: dict[str, dict[str, Any]] = {}
    try:
        for config_path in CONFIGS:
            config = load_experiment(config_path, SCHEMA_ROOT)
            manifest_path = ROOT / config["workload"]["manifest_file"]
            manifest = load_document(
                manifest_path,
                SCHEMA_ROOT / "workload-manifest.schema.json",
            )
            for view_name, binding in manifest["relation_bindings"].items():
                spark.createDataFrame(
                    [],
                    _tpch_spark_schema(binding["logical_table"]),
                ).createOrReplaceTempView(view_name)
            sql_path = ROOT / config["workload"]["sql_file"]
            rendered = render_sql(
                sql_path.read_text(encoding="utf-8"),
                manifest["parameters"],
                config["workload"]["parameters"],
            )
            actual = canonical_schema_json(spark.sql(rendered).schema.json())
            expected = manifest["expected_schema"]["canonical_json"]
            results[manifest["id"]] = {
                "status": "passed" if actual == expected else "failed",
                "expected": expected,
                "actual": actual,
            }
        failed = [query_id for query_id, value in results.items() if value["status"] != "passed"]
        print(json.dumps(results, indent=2, sort_keys=True))
        if failed:
            raise SystemExit(f"TPC-H Spark schema mismatch: {', '.join(failed)}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
