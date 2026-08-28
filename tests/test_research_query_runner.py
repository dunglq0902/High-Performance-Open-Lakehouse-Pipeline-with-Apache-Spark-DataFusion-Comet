from pathlib import Path

import pytest

from pipeline.benchmark.run_query import (
    _arm_worker_sampler,
    _safe_failure_message,
    _table_identifier,
    _write_text_immutable,
)


@pytest.mark.parametrize(
    ("logical", "identifier", "snapshot_key"),
    [
        ("orders", "lakehouse.bronze.orders", "bronze.orders"),
        ("silver.sales_enriched", "lakehouse.silver.sales_enriched", "silver.sales_enriched"),
        ("gold.daily_revenue", "lakehouse.gold.daily_revenue", "gold.daily_revenue"),
    ],
)
def test_table_binding_is_explicit_and_snapshot_addressable(
    logical: str, identifier: str, snapshot_key: str
) -> None:
    assert _table_identifier(logical) == (identifier, snapshot_key)


def test_unknown_derived_table_requires_layer() -> None:
    with pytest.raises(RuntimeError, match="must include"):
        _table_identifier("sales_enriched")
    with pytest.raises(RuntimeError, match="invalid logical"):
        _table_identifier("unknown.table")


def test_tpch_bindings_are_isolated_in_the_tpch_namespace() -> None:
    assert _table_identifier("lineitem", suite="tpch") == (
        "lakehouse.tpch.lineitem",
        "tpch.lineitem",
    )
    assert _table_identifier("orders", suite="tpch") == (
        "lakehouse.tpch.orders",
        "tpch.orders",
    )
    with pytest.raises(RuntimeError, match="TPC-H"):
        _table_identifier("events", suite="tpch")


def test_plan_artifacts_are_immutable(tmp_path: Path) -> None:
    path = tmp_path / "plan.txt"
    _write_text_immutable(path, "Plan")
    _write_text_immutable(path, "Plan\n")
    with pytest.raises(FileExistsError):
        _write_text_immutable(path, "Different")


def test_worker_sampler_is_armed_before_the_query(tmp_path: Path) -> None:
    start = tmp_path / "start"
    started = tmp_path / "started"

    def acknowledge(_: float) -> None:
        started.write_text("started\n", encoding="utf-8")

    _arm_worker_sampler(start, started, sleep=acknowledge)
    assert start.read_text(encoding="utf-8") == "start\n"


def test_failure_message_redacts_sensitive_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "private-value")
    assert _safe_failure_message(RuntimeError("bad private-value")) == "bad <redacted>"
