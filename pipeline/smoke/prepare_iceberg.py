"""Register the generated fixture as an Iceberg v2 table through the REST catalog."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from benchmark.runner.canonical import sha256_file, write_json
from pipeline.smoke.runtime_check import fingerprint

ORDERS_TABLE_DDL = """
CREATE OR REPLACE TABLE lakehouse.bronze.orders (
    order_id BIGINT NOT NULL,
    customer_id BIGINT NOT NULL,
    order_time TIMESTAMP NOT NULL,
    status STRING NOT NULL,
    payment_method STRING NOT NULL
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy'
)
"""


def _table_files(manifest_path: Path, table_manifest: dict[str, Any]) -> list[Path]:
    """Resolve only manifest-declared Parquet files below the immutable dataset root."""

    dataset_root = manifest_path.parent.resolve()
    paths: list[Path] = []
    for record in table_manifest["files"]:
        relative = Path(record["path"])
        candidate = (dataset_root / relative).resolve()
        if relative.is_absolute() or not candidate.is_relative_to(dataset_root):
            raise RuntimeError(f"dataset file escapes its immutable root: {relative}")
        if candidate.suffix != ".parquet" or not candidate.is_file():
            raise RuntimeError(f"declared Parquet file is missing: {candidate}")
        paths.append(candidate)
    if not paths:
        raise RuntimeError("orders manifest declares no Parquet files")
    return paths


def main() -> None:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("lakehouse-prepare-iceberg-smoke").getOrCreate()
    try:
        runtime = fingerprint(spark, engine="spark_baseline")
        manifest = json.loads(args.dataset_manifest.read_text(encoding="utf-8"))
        if manifest["benchmark_eligible"]:
            raise RuntimeError("smoke preparation refuses a benchmark-eligible dataset")
        sources = [
            path.as_posix()
            for path in _table_files(args.dataset_manifest, manifest["tables"]["orders"])
        ]
        orders_schema = StructType(
            [
                StructField("order_id", LongType(), nullable=False),
                StructField("customer_id", LongType(), nullable=False),
                StructField("order_time", TimestampType(), nullable=False),
                StructField("status", StringType(), nullable=False),
                StructField("payment_method", StringType(), nullable=False),
            ]
        )
        parquet_orders = spark.read.schema(orders_schema).parquet(*sources)
        if (
            parquet_orders.filter(
                "order_id IS NULL OR customer_id IS NULL OR order_time IS NULL "
                "OR status IS NULL OR payment_method IS NULL"
            )
            .limit(1)
            .count()
        ):
            raise RuntimeError("orders source violates its required-field contract")
        # Spark deliberately relaxes Parquet fields to nullable. Rebuilding this tiny readiness
        # fixture through its RDD reapplies the reviewed required-field contract before Iceberg
        # table creation. This preparation is outside every measured query boundary.
        orders = spark.createDataFrame(parquet_orders.rdd, schema=orders_schema, verifySchema=True)
        null_types = [
            field.name for field in orders.schema.fields if field.dataType.typeName() == "void"
        ]
        if null_types:
            raise RuntimeError(
                f"Comet Spark 4.1 cannot read persisted NullType columns: {null_types}"
            )

        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
        # A CTAS/DataFrameWriterV2 create operation loses NOT NULL in Spark's Iceberg
        # schema. Define the table explicitly, then append the already validated frame so
        # the immutable binding contract survives a round trip through the REST catalog.
        spark.sql(ORDERS_TABLE_DDL)
        orders.writeTo("lakehouse.bronze.orders").append()
        snapshot = spark.sql(
            "SELECT snapshot_id, manifest_list FROM lakehouse.bronze.orders.snapshots "
            "ORDER BY committed_at DESC LIMIT 1"
        ).collect()[0]
        row_count = spark.table("lakehouse.bronze.orders").count()
        expected = manifest["tables"]["orders"]["row_count"]
        if row_count != expected:
            raise RuntimeError(f"Iceberg row count {row_count} != manifest {expected}")
        write_json(
            args.output,
            {
                "schema_version": 1,
                "status": "passed",
                "runtime": runtime,
                "dataset_manifest_sha256": sha256_file(args.dataset_manifest),
                "table": "lakehouse.bronze.orders",
                "row_count": row_count,
                "snapshot_id": int(snapshot["snapshot_id"]),
                "manifest_list": snapshot["manifest_list"],
            },
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
