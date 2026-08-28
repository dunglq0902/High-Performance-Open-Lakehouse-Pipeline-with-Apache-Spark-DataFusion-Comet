import pytest

from pipeline.smoke.runtime_check import _require_single_origin


def test_runtime_origin_gate_accepts_one_expected_jar() -> None:
    _require_single_origin(
        {
            "iceberg_build": [
                "jar:file:/opt/spark/jars/iceberg-spark-runtime-4.1_2.13-1.11.0.jar!/"
                "org/apache/iceberg/IcebergBuild.class"
            ]
        },
        key="iceberg_build",
        expected_jar="iceberg-spark-runtime-4.1_2.13-1.11.0.jar",
    )


@pytest.mark.parametrize("origins", [[], ["first.jar", "second.jar"]])
def test_runtime_origin_gate_rejects_missing_or_duplicate_classes(
    origins: list[str],
) -> None:
    with pytest.raises(RuntimeError, match="exactly one classpath origin"):
        _require_single_origin(
            {"iceberg_build": origins},
            key="iceberg_build",
            expected_jar="iceberg-runtime.jar",
        )


def test_runtime_origin_gate_rejects_unexpected_jar() -> None:
    with pytest.raises(RuntimeError, match="expected iceberg_build to come from"):
        _require_single_origin(
            {"iceberg_build": ["jar:file:/opt/spark/jars/iceberg-old.jar!/Class.class"]},
            key="iceberg_build",
            expected_jar="iceberg-runtime.jar",
        )
