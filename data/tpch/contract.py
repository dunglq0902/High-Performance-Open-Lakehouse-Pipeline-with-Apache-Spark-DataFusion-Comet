"""Explicit TPC-H table, key, and date contracts used by DBGEN import."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]

TABLE_ORDER = (
    "region",
    "nation",
    "supplier",
    "customer",
    "part",
    "partsupp",
    "orders",
    "lineitem",
)

SF1_ROW_COUNTS: dict[str, int] = {
    "region": 5,
    "nation": 25,
    "supplier": 10_000,
    "customer": 150_000,
    "part": 200_000,
    "partsupp": 800_000,
    "orders": 1_500_000,
    "lineitem": 6_001_215,
}

# DBGEN cardinalities are exact; lineitem is not a linear multiple of SF1.
# Independent reference: https://duckdb.org/2025/05/23/arrow-ipc-support-in-duckdb
SF10_ROW_COUNTS: dict[str, int] = {
    **{name: count * 10 for name, count in SF1_ROW_COUNTS.items()},
    "region": 5,
    "nation": 25,
    "lineitem": 59_986_052,
}


def row_counts_for_scale(scale_factor: int) -> dict[str, int]:
    if type(scale_factor) is not int or scale_factor not in (1, 10):
        raise ValueError("TPC-H scale factor must be 1 or 10")
    return dict(SF1_ROW_COUNTS if scale_factor == 1 else SF10_ROW_COUNTS)


TPCH_SCHEMAS: dict[str, pa.Schema] = {
    "region": pa.schema(
        [
            pa.field("r_regionkey", pa.int64(), nullable=False),
            pa.field("r_name", pa.string(), nullable=False),
            pa.field("r_comment", pa.string(), nullable=False),
        ]
    ),
    "nation": pa.schema(
        [
            pa.field("n_nationkey", pa.int64(), nullable=False),
            pa.field("n_name", pa.string(), nullable=False),
            pa.field("n_regionkey", pa.int64(), nullable=False),
            pa.field("n_comment", pa.string(), nullable=False),
        ]
    ),
    "supplier": pa.schema(
        [
            pa.field("s_suppkey", pa.int64(), nullable=False),
            pa.field("s_name", pa.string(), nullable=False),
            pa.field("s_address", pa.string(), nullable=False),
            pa.field("s_nationkey", pa.int64(), nullable=False),
            pa.field("s_phone", pa.string(), nullable=False),
            pa.field("s_acctbal", pa.decimal128(15, 2), nullable=False),
            pa.field("s_comment", pa.string(), nullable=False),
        ]
    ),
    "customer": pa.schema(
        [
            pa.field("c_custkey", pa.int64(), nullable=False),
            pa.field("c_name", pa.string(), nullable=False),
            pa.field("c_address", pa.string(), nullable=False),
            pa.field("c_nationkey", pa.int64(), nullable=False),
            pa.field("c_phone", pa.string(), nullable=False),
            pa.field("c_acctbal", pa.decimal128(15, 2), nullable=False),
            pa.field("c_mktsegment", pa.string(), nullable=False),
            pa.field("c_comment", pa.string(), nullable=False),
        ]
    ),
    "part": pa.schema(
        [
            pa.field("p_partkey", pa.int64(), nullable=False),
            pa.field("p_name", pa.string(), nullable=False),
            pa.field("p_mfgr", pa.string(), nullable=False),
            pa.field("p_brand", pa.string(), nullable=False),
            pa.field("p_type", pa.string(), nullable=False),
            pa.field("p_size", pa.int32(), nullable=False),
            pa.field("p_container", pa.string(), nullable=False),
            pa.field("p_retailprice", pa.decimal128(15, 2), nullable=False),
            pa.field("p_comment", pa.string(), nullable=False),
        ]
    ),
    "partsupp": pa.schema(
        [
            pa.field("ps_partkey", pa.int64(), nullable=False),
            pa.field("ps_suppkey", pa.int64(), nullable=False),
            pa.field("ps_availqty", pa.int32(), nullable=False),
            pa.field("ps_supplycost", pa.decimal128(15, 2), nullable=False),
            pa.field("ps_comment", pa.string(), nullable=False),
        ]
    ),
    "orders": pa.schema(
        [
            pa.field("o_orderkey", pa.int64(), nullable=False),
            pa.field("o_custkey", pa.int64(), nullable=False),
            pa.field("o_orderstatus", pa.string(), nullable=False),
            pa.field("o_totalprice", pa.decimal128(15, 2), nullable=False),
            pa.field("o_orderdate", pa.date32(), nullable=False),
            pa.field("o_orderpriority", pa.string(), nullable=False),
            pa.field("o_clerk", pa.string(), nullable=False),
            pa.field("o_shippriority", pa.int32(), nullable=False),
            pa.field("o_comment", pa.string(), nullable=False),
        ]
    ),
    "lineitem": pa.schema(
        [
            pa.field("l_orderkey", pa.int64(), nullable=False),
            pa.field("l_partkey", pa.int64(), nullable=False),
            pa.field("l_suppkey", pa.int64(), nullable=False),
            pa.field("l_linenumber", pa.int32(), nullable=False),
            pa.field("l_quantity", pa.decimal128(15, 2), nullable=False),
            pa.field("l_extendedprice", pa.decimal128(15, 2), nullable=False),
            pa.field("l_discount", pa.decimal128(15, 2), nullable=False),
            pa.field("l_tax", pa.decimal128(15, 2), nullable=False),
            pa.field("l_returnflag", pa.string(), nullable=False),
            pa.field("l_linestatus", pa.string(), nullable=False),
            pa.field("l_shipdate", pa.date32(), nullable=False),
            pa.field("l_commitdate", pa.date32(), nullable=False),
            pa.field("l_receiptdate", pa.date32(), nullable=False),
            pa.field("l_shipinstruct", pa.string(), nullable=False),
            pa.field("l_shipmode", pa.string(), nullable=False),
            pa.field("l_comment", pa.string(), nullable=False),
        ]
    ),
}

PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "region": ("r_regionkey",),
    "nation": ("n_nationkey",),
    "supplier": ("s_suppkey",),
    "customer": ("c_custkey",),
    "part": ("p_partkey",),
    "partsupp": ("ps_partkey", "ps_suppkey"),
    "orders": ("o_orderkey",),
    "lineitem": ("l_orderkey", "l_linenumber"),
}


@dataclass(frozen=True, slots=True)
class ForeignKey:
    columns: tuple[str, ...]
    parent_table: str
    parent_columns: tuple[str, ...]


FOREIGN_KEYS: dict[str, tuple[ForeignKey, ...]] = {
    "region": (),
    "nation": (ForeignKey(("n_regionkey",), "region", ("r_regionkey",)),),
    "supplier": (ForeignKey(("s_nationkey",), "nation", ("n_nationkey",)),),
    "customer": (ForeignKey(("c_nationkey",), "nation", ("n_nationkey",)),),
    "part": (),
    "partsupp": (
        ForeignKey(("ps_partkey",), "part", ("p_partkey",)),
        ForeignKey(("ps_suppkey",), "supplier", ("s_suppkey",)),
    ),
    "orders": (ForeignKey(("o_custkey",), "customer", ("c_custkey",)),),
    "lineitem": (
        ForeignKey(("l_orderkey",), "orders", ("o_orderkey",)),
        ForeignKey(("l_partkey",), "part", ("p_partkey",)),
        ForeignKey(("l_suppkey",), "supplier", ("s_suppkey",)),
        ForeignKey(
            ("l_partkey", "l_suppkey"),
            "partsupp",
            ("ps_partkey", "ps_suppkey"),
        ),
    ),
}

DATE_RANGES: dict[tuple[str, str], tuple[date, date]] = {
    ("orders", "o_orderdate"): (date(1992, 1, 1), date(1998, 8, 2)),
    ("lineitem", "l_shipdate"): (date(1992, 1, 1), date(1998, 12, 1)),
    ("lineitem", "l_commitdate"): (date(1992, 1, 1), date(1998, 10, 31)),
    ("lineitem", "l_receiptdate"): (date(1992, 1, 1), date(1998, 12, 31)),
}


def arrow_type_name(data_type: pa.DataType) -> str:
    if pa.types.is_int64(data_type):
        return "int64"
    if pa.types.is_int32(data_type):
        return "int32"
    if pa.types.is_string(data_type):
        return "utf8"
    if pa.types.is_decimal(data_type):
        return f"decimal128({data_type.precision},{data_type.scale})"
    if pa.types.is_date32(data_type):
        return "date32"
    raise TypeError(f"unsupported TPC-H Arrow type: {data_type}")


def schema_contract(schema: pa.Schema) -> list[dict[str, Any]]:
    return [
        {
            "name": field.name,
            "type": arrow_type_name(field.type),
            "nullable": field.nullable,
        }
        for field in schema
    ]


def schema_sha256(schema: pa.Schema) -> str:
    payload = json.dumps(
        schema_contract(schema),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def validate_contract() -> None:
    if tuple(TPCH_SCHEMAS) != TABLE_ORDER:
        raise AssertionError("TPC-H schemas are not in deterministic table order")
    if set(PRIMARY_KEYS) != set(TABLE_ORDER) or set(FOREIGN_KEYS) != set(TABLE_ORDER):
        raise AssertionError("TPC-H key contracts do not cover every table")
    for table_name in TABLE_ORDER:
        names = set(TPCH_SCHEMAS[table_name].names)
        if not set(PRIMARY_KEYS[table_name]) <= names:
            raise AssertionError(f"{table_name} primary key is outside its schema")
        for foreign_key in FOREIGN_KEYS[table_name]:
            if not set(foreign_key.columns) <= names:
                raise AssertionError(f"{table_name} foreign key is outside its schema")
            parent_names = set(TPCH_SCHEMAS[foreign_key.parent_table].names)
            if not set(foreign_key.parent_columns) <= parent_names:
                raise AssertionError(f"{table_name} foreign key parent columns are invalid")


validate_contract()
