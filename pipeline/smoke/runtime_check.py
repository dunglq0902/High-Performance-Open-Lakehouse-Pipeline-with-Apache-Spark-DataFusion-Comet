"""Runtime fingerprint checks executed inside Spark driver applications."""

from __future__ import annotations

import platform
import sys
from pathlib import Path
from typing import Any

from benchmark.runner.canonical import sha256_file
from benchmark.runner.runtime import validate_runtime_lock

RUNTIME_ROOT = Path("/opt/lakehouse")


def _spark_conf(spark: Any, key: str) -> str | None:
    try:
        return str(spark.conf.get(key))
    except Exception:  # Spark raises a JVM exception when no default exists.
        return None


def _class_resources(jvm: Any, resource_name: str) -> list[str]:
    """Return every classpath origin visible to the Spark driver for one resource."""

    loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
    resources = loader.getResources(resource_name)
    origins: list[str] = []
    while resources.hasMoreElements():
        origins.append(str(resources.nextElement().toString()))
    return sorted(origins)


def _require_single_origin(
    resources: dict[str, list[str]],
    *,
    key: str,
    expected_jar: str,
) -> None:
    origins = resources[key]
    if len(origins) != 1:
        raise RuntimeError(f"expected exactly one classpath origin for {key}, got {origins}")
    if expected_jar not in origins[0]:
        raise RuntimeError(f"expected {key} to come from {expected_jar}, got {origins[0]}")


def _runtime_contract() -> tuple[dict[str, dict[str, Any]], str]:
    lock_path = RUNTIME_ROOT / "runtime-versions.lock"
    lock = validate_runtime_lock(
        lock_path,
        RUNTIME_ROOT / "benchmark/schemas/runtime-lock.schema.json",
    )
    components = {component["name"]: component for component in lock["components"]}
    return components, sha256_file(lock_path)


def _verify_artifact_hashes(
    components: dict[str, dict[str, Any]],
) -> tuple[dict[str, str], dict[str, Path]]:
    jars = Path("/opt/spark/jars")
    paths = {
        "scala": jars / f"scala-library-{components['scala']['version']}.jar",
        "datafusion-comet": jars
        / f"comet-spark-spark4.1_2.13-{components['datafusion-comet']['version']}.jar",
        "apache-iceberg-runtime": jars
        / (f"iceberg-spark-runtime-4.1_2.13-{components['apache-iceberg-runtime']['version']}.jar"),
        "apache-iceberg-aws-bundle": jars
        / f"iceberg-aws-bundle-{components['apache-iceberg-aws-bundle']['version']}.jar",
    }
    actual: dict[str, str] = {}
    for component_name, path in paths.items():
        if not path.is_file():
            raise RuntimeError(f"runtime artifact is missing: {path}")
        actual_hash = sha256_file(path)
        expected_hash = str(components[component_name]["sha256_or_digest"]).removeprefix("sha256:")
        if actual_hash != expected_hash:
            raise RuntimeError(
                f"runtime artifact hash mismatch for {component_name}: "
                f"expected {expected_hash}, got {actual_hash}"
            )
        actual[component_name] = actual_hash
    return actual, paths


def fingerprint(spark: Any, *, engine: str) -> dict[str, Any]:
    jvm = spark.sparkContext._jvm
    components, runtime_lock_sha256 = _runtime_contract()
    artifact_hashes, artifact_paths = _verify_artifact_hashes(components)
    class_resources = {
        "scala_properties": _class_resources(jvm, "scala/util/Properties.class"),
        "iceberg_build": _class_resources(jvm, "org/apache/iceberg/IcebergBuild.class"),
        "iceberg_s3_file_io": _class_resources(jvm, "org/apache/iceberg/aws/s3/S3FileIO.class"),
        "aws_s3_client": _class_resources(jvm, "software/amazon/awssdk/services/s3/S3Client.class"),
        "comet_plugin": _class_resources(jvm, "org/apache/spark/CometPlugin.class"),
        "hadoop_s3a": _class_resources(jvm, "org/apache/hadoop/fs/s3a/S3AFileSystem.class"),
    }
    iceberg_version = str(jvm.org.apache.iceberg.IcebergBuild.version())
    values: dict[str, Any] = {
        "spark_version": str(spark.version),
        "scala_version": str(jvm.scala.util.Properties.versionNumberString()),
        "java_version": str(jvm.java.lang.System.getProperty("java.version")),
        "java_runtime_version": str(jvm.java.lang.System.getProperty("java.runtime.version")),
        "python_version": platform.python_version(),
        "comet_version": _spark_conf(spark, "spark.comet.version"),
        "iceberg_version": iceberg_version,
        "iceberg_full_version": str(jvm.org.apache.iceberg.IcebergBuild.fullVersion()),
        "machine": platform.machine(),
        "runtime_lock_sha256": runtime_lock_sha256,
        "artifact_sha256": artifact_hashes,
        "class_resources": class_resources,
    }
    expected_spark = str(components["apache-spark"]["version"])
    expected_scala = str(components["scala"]["version"])
    expected_java = str(components["java"]["version"])
    expected_python = str(components["python"]["version"])
    expected_iceberg = str(components["apache-iceberg-runtime"]["version"])
    expected_python_tuple = tuple(int(part) for part in expected_python.split("."))
    if values["spark_version"] != expected_spark:
        raise RuntimeError(f"expected Spark {expected_spark}, got {values['spark_version']}")
    if values["scala_version"] != expected_scala:
        raise RuntimeError(f"expected Scala {expected_scala}, got {values['scala_version']}")
    if not str(values["java_version"]).startswith(expected_java.partition("+")[0]):
        raise RuntimeError(f"expected Java {expected_java}, got {values['java_version']}")
    if not str(values["java_runtime_version"]).startswith(expected_java):
        raise RuntimeError(
            f"expected Java runtime {expected_java}, got {values['java_runtime_version']}"
        )
    if (
        platform.python_version() != expected_python
        or sys.version_info[:3] != expected_python_tuple
    ):
        raise RuntimeError(f"expected Python {expected_python}, got {values['python_version']}")
    if iceberg_version != expected_iceberg:
        raise RuntimeError(f"expected Iceberg {expected_iceberg}, got {iceberg_version}")

    _require_single_origin(
        class_resources,
        key="scala_properties",
        expected_jar=artifact_paths["scala"].name,
    )
    _require_single_origin(
        class_resources,
        key="iceberg_build",
        expected_jar=artifact_paths["apache-iceberg-runtime"].name,
    )
    _require_single_origin(
        class_resources,
        key="iceberg_s3_file_io",
        expected_jar=artifact_paths["apache-iceberg-runtime"].name,
    )
    _require_single_origin(
        class_resources,
        key="aws_s3_client",
        expected_jar=artifact_paths["apache-iceberg-aws-bundle"].name,
    )
    _require_single_origin(
        class_resources,
        key="comet_plugin",
        expected_jar=artifact_paths["datafusion-comet"].name,
    )
    if class_resources["hadoop_s3a"]:
        raise RuntimeError(
            "this Iceberg S3FileIO slice must not load hadoop-aws/S3A classes: "
            f"{class_resources['hadoop_s3a']}"
        )

    plugins = spark.sparkContext.getConf().get("spark.plugins", "")
    if engine == "spark_baseline":
        if plugins:
            raise RuntimeError(f"baseline application unexpectedly loads plugins: {plugins}")
        if values["comet_version"] is not None:
            raise RuntimeError("baseline application exposes spark.comet.version")
    elif engine == "comet_accelerated":
        if plugins != "org.apache.spark.CometPlugin":
            raise RuntimeError("Comet application did not load org.apache.spark.CometPlugin")
        expected_comet = str(components["datafusion-comet"]["version"])
        if values["comet_version"] != expected_comet:
            raise RuntimeError(f"expected Comet {expected_comet}, got {values['comet_version']}")
        if _spark_conf(spark, "spark.comet.nativeLoadRequired") != "true":
            raise RuntimeError("Comet nativeLoadRequired is not true")
    else:
        raise ValueError(f"unknown engine: {engine}")
    return values
