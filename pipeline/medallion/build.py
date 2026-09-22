"""Build reviewed E-commerce Medallion or TPC-H-derived Iceberg tables."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]

from benchmark.runner.canonical import sha256_file, sha256_value, write_json
from benchmark.runner.dataset_attestation import verify_attestation
from benchmark.runner.runtime import validate_runtime_lock
from data.generator.constants import TABLE_ORDER as ECOMMERCE_TABLE_ORDER
from data.generator.validation import validate_dataset
from data.tpch.contract import TABLE_ORDER as TPCH_TABLE_ORDER
from data.tpch.contract import TPCH_SCHEMAS
from data.tpch.dataset import validate_tpch_dataset
from pipeline.smoke.runtime_check import fingerprint

BRONZE_TABLE_DDLS: dict[str, str] = {
    "customers": """
        CREATE OR REPLACE TABLE lakehouse.bronze.customers (
            customer_id BIGINT NOT NULL,
            customer_name STRING NOT NULL,
            email STRING NOT NULL,
            region STRING NOT NULL,
            segment STRING NOT NULL,
            signup_time TIMESTAMP NOT NULL
        ) USING iceberg
        TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='snappy')
    """,
    "products": """
        CREATE OR REPLACE TABLE lakehouse.bronze.products (
            product_id BIGINT NOT NULL,
            product_name STRING NOT NULL,
            category STRING NOT NULL,
            base_price DECIMAL(18,2) NOT NULL,
            created_at TIMESTAMP NOT NULL
        ) USING iceberg
        TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='snappy')
    """,
    "orders": """
        CREATE OR REPLACE TABLE lakehouse.bronze.orders (
            order_id BIGINT NOT NULL,
            customer_id BIGINT NOT NULL,
            order_time TIMESTAMP NOT NULL,
            status STRING NOT NULL,
            payment_method STRING NOT NULL
        ) USING iceberg
        PARTITIONED BY (months(order_time))
        TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='snappy')
    """,
    "order_items": """
        CREATE OR REPLACE TABLE lakehouse.bronze.order_items (
            order_id BIGINT NOT NULL,
            line_number INT NOT NULL,
            product_id BIGINT NOT NULL,
            quantity INT NOT NULL,
            unit_price DECIMAL(18,2) NOT NULL,
            discount DECIMAL(5,4) NOT NULL
        ) USING iceberg
        TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='snappy')
    """,
    "events": """
        CREATE OR REPLACE TABLE lakehouse.bronze.events (
            event_id BIGINT NOT NULL,
            session_id STRING NOT NULL,
            customer_id BIGINT,
            event_time TIMESTAMP NOT NULL,
            event_type STRING NOT NULL,
            product_id BIGINT,
            order_id BIGINT,
            device_type STRING NOT NULL
        ) USING iceberg
        PARTITIONED BY (months(event_time))
        TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='snappy')
    """,
}

SILVER_SALES_SQL = """
CREATE OR REPLACE TABLE lakehouse.silver.sales_enriched
USING iceberg
PARTITIONED BY (months(order_time))
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='snappy')
AS
SELECT
    o.order_id,
    o.customer_id,
    c.customer_name,
    c.region,
    c.segment,
    o.order_time,
    o.status,
    o.payment_method,
    oi.line_number,
    oi.product_id,
    p.product_name,
    p.category,
    oi.quantity,
    oi.unit_price,
    oi.discount,
    CAST(oi.quantity * oi.unit_price * (CAST(1 AS DECIMAL(5,4)) - oi.discount)
         AS DECIMAL(20,4)) AS net_revenue
FROM lakehouse.bronze.orders o
JOIN lakehouse.bronze.customers c ON o.customer_id = c.customer_id
JOIN lakehouse.bronze.order_items oi ON o.order_id = oi.order_id
JOIN lakehouse.bronze.products p ON oi.product_id = p.product_id
WHERE o.status = 'COMPLETED'
  AND oi.quantity > 0
  AND oi.unit_price >= CAST(0 AS DECIMAL(18,2))
  AND oi.discount BETWEEN CAST(0 AS DECIMAL(5,4)) AND CAST(1 AS DECIMAL(5,4))
"""

SILVER_EVENTS_SQL = """
CREATE OR REPLACE TABLE lakehouse.silver.events
USING iceberg
PARTITIONED BY (months(event_time))
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='snappy')
AS
SELECT event_id, session_id, customer_id, event_time, event_type, product_id, order_id,
       device_type
FROM lakehouse.bronze.events
WHERE event_type IN ('view_product', 'add_to_cart', 'checkout', 'purchase')
"""

GOLD_TABLE_SQL: dict[str, str] = {
    "daily_revenue": """
        CREATE OR REPLACE TABLE lakehouse.gold.daily_revenue
        USING iceberg
        TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='snappy')
        AS
        SELECT CAST(order_time AS DATE) AS revenue_date,
               region,
               COUNT(DISTINCT order_id) AS order_count,
               CAST(SUM(net_revenue) AS DECIMAL(38,4)) AS net_revenue
        FROM lakehouse.silver.sales_enriched
        GROUP BY CAST(order_time AS DATE), region
    """,
    "customer_ltv": """
        CREATE OR REPLACE TABLE lakehouse.gold.customer_ltv
        USING iceberg
        TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='snappy')
        AS
        SELECT customer_id,
               MAX(customer_name) AS customer_name,
               MAX(region) AS region,
               MAX(segment) AS segment,
               COUNT(DISTINCT order_id) AS order_count,
               CAST(SUM(net_revenue) AS DECIMAL(38,4)) AS lifetime_revenue,
               MAX(order_time) AS last_order_time
        FROM lakehouse.silver.sales_enriched
        GROUP BY customer_id
    """,
    "product_ranking": """
        CREATE OR REPLACE TABLE lakehouse.gold.product_ranking
        USING iceberg
        TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='snappy')
        AS
        WITH product_sales AS (
            SELECT category, product_id, MAX(product_name) AS product_name,
                   CAST(SUM(net_revenue) AS DECIMAL(38,4)) AS net_revenue
            FROM lakehouse.silver.sales_enriched
            GROUP BY category, product_id
        )
        SELECT category, product_id, product_name, net_revenue,
               ROW_NUMBER() OVER (
                   PARTITION BY category ORDER BY net_revenue DESC, product_id ASC
               ) AS category_rank
        FROM product_sales
    """,
    "category_growth": """
        CREATE OR REPLACE TABLE lakehouse.gold.category_growth
        USING iceberg
        TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='snappy')
        AS
        WITH monthly AS (
            SELECT DATE_TRUNC('MONTH', order_time) AS revenue_month,
                   category,
                   CAST(SUM(net_revenue) AS DECIMAL(38,4)) AS net_revenue
            FROM lakehouse.silver.sales_enriched
            GROUP BY DATE_TRUNC('MONTH', order_time), category
        ), lagged AS (
            SELECT revenue_month, category, net_revenue,
                   LAG(net_revenue) OVER (
                       PARTITION BY category ORDER BY revenue_month
                   ) AS previous_revenue
            FROM monthly
        )
        SELECT revenue_month, category, net_revenue, previous_revenue,
               CASE WHEN previous_revenue IS NULL OR previous_revenue = 0 THEN NULL
                    ELSE CAST(
                        (net_revenue - previous_revenue) / previous_revenue
                        AS DECIMAL(18,6)
                    )
               END AS growth_rate
        FROM lagged
    """,
}


def _safe_table_files(
    manifest_path: Path, table_name: str, table_manifest: Mapping[str, Any]
) -> list[Path]:
    dataset_root = manifest_path.parent.resolve()
    paths: list[Path] = []
    for record in table_manifest["files"]:
        relative = Path(record["path"])
        candidate = (dataset_root / relative).resolve()
        if relative.is_absolute() or not candidate.is_relative_to(dataset_root):
            raise RuntimeError(f"dataset file escapes immutable root: {relative}")
        if candidate.suffix != ".parquet" or not candidate.is_file():
            raise RuntimeError(f"declared Parquet file is missing: {candidate}")
        if relative.parts[0] != table_name:
            raise RuntimeError(f"{table_name} manifest points into another table: {relative}")
        paths.append(candidate)
    if not paths:
        raise RuntimeError(f"{table_name} manifest declares no Parquet files")
    return paths


def _spark_schema(table_name: str) -> Any:
    from pyspark.sql.types import (
        DecimalType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    definitions: dict[str, list[tuple[str, Any, bool]]] = {
        "customers": [
            ("customer_id", LongType(), False),
            ("customer_name", StringType(), False),
            ("email", StringType(), False),
            ("region", StringType(), False),
            ("segment", StringType(), False),
            ("signup_time", TimestampType(), False),
        ],
        "products": [
            ("product_id", LongType(), False),
            ("product_name", StringType(), False),
            ("category", StringType(), False),
            ("base_price", DecimalType(18, 2), False),
            ("created_at", TimestampType(), False),
        ],
        "orders": [
            ("order_id", LongType(), False),
            ("customer_id", LongType(), False),
            ("order_time", TimestampType(), False),
            ("status", StringType(), False),
            ("payment_method", StringType(), False),
        ],
        "order_items": [
            ("order_id", LongType(), False),
            ("line_number", IntegerType(), False),
            ("product_id", LongType(), False),
            ("quantity", IntegerType(), False),
            ("unit_price", DecimalType(18, 2), False),
            ("discount", DecimalType(5, 4), False),
        ],
        "events": [
            ("event_id", LongType(), False),
            ("session_id", StringType(), False),
            ("customer_id", LongType(), True),
            ("event_time", TimestampType(), False),
            ("event_type", StringType(), False),
            ("product_id", LongType(), True),
            ("order_id", LongType(), True),
            ("device_type", StringType(), False),
        ],
    }
    return StructType(
        [
            StructField(name, data_type, nullable)
            for name, data_type, nullable in definitions[table_name]
        ]
    )


def _is_tpch_manifest(manifest: Mapping[str, Any]) -> bool:
    storage = manifest.get("storage")
    tables = manifest.get("tables")
    return (
        type(manifest.get("scale_factor")) is int
        and manifest.get("scale_factor") in (1, 10)
        and isinstance(storage, Mapping)
        and storage.get("profile") == "tpch_parquet"
        and isinstance(tables, Mapping)
        and set(tables) == set(TPCH_TABLE_ORDER)
    )


def _tpch_spark_type(data_type: pa.DataType) -> Any:
    from pyspark.sql.types import DateType, DecimalType, IntegerType, LongType, StringType

    if pa.types.is_int64(data_type):
        return LongType()
    if pa.types.is_int32(data_type):
        return IntegerType()
    if pa.types.is_string(data_type):
        return StringType()
    if pa.types.is_date32(data_type):
        return DateType()
    if pa.types.is_decimal(data_type):
        return DecimalType(data_type.precision, data_type.scale)
    raise TypeError(f"unsupported TPC-H Spark type: {data_type}")


def _tpch_spark_schema(table_name: str) -> Any:
    from pyspark.sql.types import StructField, StructType

    return StructType(
        [
            StructField(field.name, _tpch_spark_type(field.type), field.nullable)
            for field in TPCH_SCHEMAS[table_name]
        ]
    )


def _tpch_sql_type(data_type: pa.DataType) -> str:
    if pa.types.is_int64(data_type):
        return "BIGINT"
    if pa.types.is_int32(data_type):
        return "INT"
    if pa.types.is_string(data_type):
        return "STRING"
    if pa.types.is_date32(data_type):
        return "DATE"
    if pa.types.is_decimal(data_type):
        return f"DECIMAL({data_type.precision},{data_type.scale})"
    raise TypeError(f"unsupported TPC-H SQL type: {data_type}")


def _tpch_table_ddl(table_name: str) -> str:
    columns = ",\n".join(
        f"    {field.name} {_tpch_sql_type(field.type)}" + ("" if field.nullable else " NOT NULL")
        for field in TPCH_SCHEMAS[table_name]
    )
    return f"""
        CREATE OR REPLACE TABLE lakehouse.tpch.{table_name} (
        {columns}
        ) USING iceberg
        TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='snappy')
    """


def _build_tpch_tables(
    spark: Any, manifest_path: Path, manifest: Mapping[str, Any]
) -> tuple[dict[str, int], dict[str, dict[str, Any]]]:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.tpch")
    counts: dict[str, int] = {}
    snapshots: dict[str, dict[str, Any]] = {}
    tables = manifest["tables"]
    for table_name in TPCH_TABLE_ORDER:
        table_manifest = tables[table_name]
        sources = [
            path.as_posix() for path in _safe_table_files(manifest_path, table_name, table_manifest)
        ]
        source = spark.read.schema(_tpch_spark_schema(table_name)).parquet(*sources)
        expected = int(table_manifest["row_count"])
        actual = source.count()
        if actual != expected:
            raise RuntimeError(f"{table_name} source count {actual} != manifest {expected}")
        view = f"source_tpch_{table_name}"
        source.createOrReplaceTempView(view)
        spark.sql(_tpch_table_ddl(table_name))
        spark.sql(f"INSERT INTO lakehouse.tpch.{table_name} SELECT * FROM {view}")
        persisted = int(spark.table(f"lakehouse.tpch.{table_name}").count())
        if persisted != expected:
            raise RuntimeError(
                f"lakehouse.tpch.{table_name} count {persisted} != manifest {expected}"
            )
        counts[table_name] = persisted
        snapshots[f"tpch.{table_name}"] = _latest_snapshot(spark, f"lakehouse.tpch.{table_name}")
    return counts, snapshots


def _latest_snapshot(spark: Any, table: str) -> dict[str, Any]:
    row = spark.sql(
        f"SELECT snapshot_id, manifest_list FROM {table}.snapshots "
        "ORDER BY committed_at DESC LIMIT 1"
    ).collect()[0]
    return {"snapshot_id": int(row["snapshot_id"]), "manifest_list": row["manifest_list"]}


def _quality_audit(spark: Any) -> dict[str, int]:
    queries = {
        "duplicate_customers": (
            "SELECT COUNT(*)-COUNT(DISTINCT customer_id) FROM lakehouse.bronze.customers"
        ),
        "duplicate_products": (
            "SELECT COUNT(*)-COUNT(DISTINCT product_id) FROM lakehouse.bronze.products"
        ),
        "duplicate_orders": "SELECT COUNT(*)-COUNT(DISTINCT order_id) FROM lakehouse.bronze.orders",
        "orphan_orders": """
            SELECT COUNT(*) FROM lakehouse.bronze.orders o
            LEFT ANTI JOIN lakehouse.bronze.customers c ON o.customer_id=c.customer_id
        """,
        "invalid_order_items": """
            SELECT COUNT(*) FROM lakehouse.bronze.order_items
            WHERE quantity <= 0 OR unit_price < 0 OR discount < 0 OR discount > 1
        """,
        "orphan_order_items": """
            SELECT COUNT(*) FROM lakehouse.bronze.order_items oi
            LEFT JOIN lakehouse.bronze.orders o ON oi.order_id=o.order_id
            LEFT JOIN lakehouse.bronze.products p ON oi.product_id=p.product_id
            WHERE o.order_id IS NULL OR p.product_id IS NULL
        """,
    }
    return {name: int(spark.sql(sql).collect()[0][0]) for name, sql in queries.items()}


def _validate_source_dataset(
    manifest_path: Path,
    *,
    tpch_dataset: bool,
    attestation_path: Path | None,
    expected_git_commit: str | None,
) -> str | None:
    """Use full semantics or a current content-bound attestation before Spark import."""

    if (attestation_path is None) != (expected_git_commit is None):
        raise RuntimeError(
            "dataset validation attestation and expected Git commit must be supplied together"
        )
    if attestation_path is not None:
        root = Path(__file__).resolve().parents[2]
        runtime_lock = validate_runtime_lock(
            root / "runtime-versions.lock",
            root / "benchmark/schemas/runtime-lock.schema.json",
        )
        components = {component["name"]: component for component in runtime_lock["components"]}
        # Host plan creation proves Git lineage and binds this receipt's hash. The image has
        # no Git database; it still verifies all runtime/content and direct-origin bindings.
        verify_attestation(
            root,
            manifest_path,
            attestation_path,
            expected_python_version=str(components["python"]["version"]),
            expected_git_commit=str(expected_git_commit),
            require_git_lineage=False,
        )
        return sha256_file(attestation_path)
    if tpch_dataset:
        validate_tpch_dataset(manifest_path.parent)
    else:
        validate_dataset(manifest_path.parent)
    return None


def _write_audit(path: Path, payload: Mapping[str, Any]) -> None:
    value = {
        **payload,
        "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }
    value["artifact_sha256"] = sha256_value(value)
    write_json(path, value)


def main() -> None:
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--allow-non-benchmark", action="store_true")
    parser.add_argument("--dataset-validation-attestation", type=Path)
    parser.add_argument("--expected-git-commit")
    args = parser.parse_args()

    manifest = json.loads(args.dataset_manifest.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict):
        raise RuntimeError("dataset manifest root must be an object")
    if not manifest.get("benchmark_eligible") and not args.allow_non_benchmark:
        raise RuntimeError("Medallion research build requires a benchmark-eligible dataset")
    tpch_dataset = _is_tpch_manifest(manifest)
    attestation_sha256 = _validate_source_dataset(
        args.dataset_manifest,
        tpch_dataset=tpch_dataset,
        attestation_path=args.dataset_validation_attestation,
        expected_git_commit=args.expected_git_commit,
    )

    spark = SparkSession.builder.appName("lakehouse-medallion-build").getOrCreate()
    try:
        runtime = fingerprint(spark, engine="spark_baseline")
        if tpch_dataset:
            table_counts, tpch_snapshots = _build_tpch_tables(
                spark, args.dataset_manifest, manifest
            )
            _write_audit(
                args.output,
                {
                    "schema_version": 1,
                    "status": "passed",
                    "pipeline": "tpch-derived-iceberg-import-v1",
                    "runtime": runtime,
                    "dataset_id": manifest["dataset_id"],
                    "dataset_manifest_sha256": sha256_file(args.dataset_manifest),
                    "dataset_validation_attestation_sha256": attestation_sha256,
                    "benchmark_eligible": bool(manifest["benchmark_eligible"]),
                    "scale_factor": manifest["scale_factor"],
                    "table_counts": table_counts,
                    "quality": manifest["validation"],
                    "snapshots": tpch_snapshots,
                },
            )
            return
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

        bronze_counts: dict[str, int] = {}
        snapshots: dict[str, dict[str, Any]] = {}
        for table_name in ECOMMERCE_TABLE_ORDER:
            table_manifest = manifest["tables"][table_name]
            sources = [
                path.as_posix()
                for path in _safe_table_files(args.dataset_manifest, table_name, table_manifest)
            ]
            source = spark.read.schema(_spark_schema(table_name)).parquet(*sources)
            expected = int(table_manifest["row_count"])
            actual = source.count()
            if actual != expected:
                raise RuntimeError(f"{table_name} source count {actual} != manifest {expected}")
            view = f"source_{table_name}"
            source.createOrReplaceTempView(view)
            spark.sql(BRONZE_TABLE_DDLS[table_name])
            spark.sql(f"INSERT INTO lakehouse.bronze.{table_name} SELECT * FROM {view}")
            persisted = int(spark.table(f"lakehouse.bronze.{table_name}").count())
            if persisted != expected:
                raise RuntimeError(
                    f"lakehouse.bronze.{table_name} count {persisted} != manifest {expected}"
                )
            bronze_counts[table_name] = persisted
            snapshots[f"bronze.{table_name}"] = _latest_snapshot(
                spark, f"lakehouse.bronze.{table_name}"
            )

        quality = _quality_audit(spark)
        if any(quality.values()):
            raise RuntimeError(f"Bronze data-quality gate failed: {quality}")

        spark.sql(SILVER_SALES_SQL)
        spark.sql(SILVER_EVENTS_SQL)
        for table_name in ("sales_enriched", "events"):
            snapshots[f"silver.{table_name}"] = _latest_snapshot(
                spark, f"lakehouse.silver.{table_name}"
            )

        for table_name, sql in GOLD_TABLE_SQL.items():
            spark.sql(sql)
            snapshots[f"gold.{table_name}"] = _latest_snapshot(
                spark, f"lakehouse.gold.{table_name}"
            )

        derived_counts = {
            "silver.sales_enriched": int(spark.table("lakehouse.silver.sales_enriched").count()),
            "silver.events": int(spark.table("lakehouse.silver.events").count()),
            **{
                f"gold.{table_name}": int(spark.table(f"lakehouse.gold.{table_name}").count())
                for table_name in GOLD_TABLE_SQL
            },
        }
        _write_audit(
            args.output,
            {
                "schema_version": 1,
                "status": "passed",
                "pipeline": "ecommerce-medallion-v1",
                "runtime": runtime,
                "dataset_id": manifest["dataset_id"],
                "dataset_manifest_sha256": sha256_file(args.dataset_manifest),
                "dataset_validation_attestation_sha256": attestation_sha256,
                "benchmark_eligible": bool(manifest["benchmark_eligible"]),
                "bronze_counts": bronze_counts,
                "derived_counts": derived_counts,
                "quality": quality,
                "snapshots": snapshots,
            },
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
