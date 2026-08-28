import pytest

from benchmark.runner.config import ConfigurationError
from benchmark.runner.sql import canonical_result_hash, render_sql, schema_hash

DEFINITIONS = {
    "start": {"type": "timestamp", "required": True},
    "bucket": {"type": "integer", "required": True, "allowed_values": [1, 10, 50]},
}


def test_render_sql_validates_typed_values() -> None:
    sql = "SELECT * FROM t WHERE ts >= TIMESTAMP '{{start}}' AND id % 100 < {{bucket}}"
    rendered = render_sql(
        sql,
        DEFINITIONS,
        {"start": "2025-01-01 00:00:00", "bucket": 10},
    )
    assert "{{" not in rendered
    assert "TIMESTAMP '2025-01-01 00:00:00'" in rendered
    assert rendered.endswith("< 10")


def test_render_sql_rejects_fragments_and_unknown_parameters() -> None:
    with pytest.raises(ConfigurationError, match="integer"):
        render_sql(
            "SELECT {{bucket}}",
            DEFINITIONS,
            {"start": "2025-01-01 00:00:00", "bucket": "1 OR 1=1"},
        )
    with pytest.raises(ConfigurationError, match="unknown"):
        render_sql(
            "SELECT {{bucket}}",
            DEFINITIONS,
            {"start": "2025-01-01 00:00:00", "bucket": 1, "extra": 2},
        )


def test_render_sql_rejects_non_finite_numbers_and_impossible_timestamps() -> None:
    number_definition = {"value": {"type": "number", "required": True}}
    with pytest.raises(ConfigurationError, match="finite"):
        render_sql("SELECT {{value}}", number_definition, {"value": float("nan")})
    with pytest.raises(ConfigurationError, match="valid timestamp"):
        render_sql(
            "SELECT TIMESTAMP '{{start}}'",
            {"start": {"type": "timestamp", "required": True}},
            {"start": "2025-02-30 00:00:00"},
        )


def test_unordered_result_hash_uses_multiset_semantics() -> None:
    left = [{"id": 1}, {"id": 2}, {"id": 2}]
    right = list(reversed(left))
    assert canonical_result_hash(left, ordered=False) == canonical_result_hash(right, ordered=False)
    assert canonical_result_hash(left, ordered=True) != canonical_result_hash(right, ordered=True)


def test_schema_hash_ignores_spark_object_key_order() -> None:
    contract_order = (
        '{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}'
    )
    spark_41_order = (
        '{"fields":[{"metadata":{},"name":"id","nullable":false,"type":"long"}],"type":"struct"}'
    )
    assert schema_hash(contract_order) == schema_hash(spark_41_order)
