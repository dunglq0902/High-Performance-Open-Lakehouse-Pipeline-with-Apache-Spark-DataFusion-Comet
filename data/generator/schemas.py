"""Authoritative Arrow schemas for the five E-commerce source tables."""

from __future__ import annotations

import hashlib
import json
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]

CUSTOMERS_SCHEMA = pa.schema(
    [
        pa.field("customer_id", pa.int64(), nullable=False),
        pa.field("customer_name", pa.string(), nullable=False),
        pa.field("email", pa.string(), nullable=False),
        pa.field("region", pa.string(), nullable=False),
        pa.field("segment", pa.string(), nullable=False),
        pa.field("signup_time", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)

PRODUCTS_SCHEMA = pa.schema(
    [
        pa.field("product_id", pa.int64(), nullable=False),
        pa.field("product_name", pa.string(), nullable=False),
        pa.field("category", pa.string(), nullable=False),
        pa.field("base_price", pa.decimal128(18, 2), nullable=False),
        pa.field("created_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)

ORDERS_SCHEMA = pa.schema(
    [
        pa.field("order_id", pa.int64(), nullable=False),
        pa.field("customer_id", pa.int64(), nullable=False),
        pa.field("order_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("payment_method", pa.string(), nullable=False),
    ]
)

ORDER_ITEMS_SCHEMA = pa.schema(
    [
        pa.field("order_id", pa.int64(), nullable=False),
        pa.field("line_number", pa.int32(), nullable=False),
        pa.field("product_id", pa.int64(), nullable=False),
        pa.field("quantity", pa.int32(), nullable=False),
        pa.field("unit_price", pa.decimal128(18, 2), nullable=False),
        pa.field("discount", pa.decimal128(5, 4), nullable=False),
    ]
)

EVENTS_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.int64(), nullable=False),
        pa.field("session_id", pa.string(), nullable=False),
        pa.field("customer_id", pa.int64(), nullable=True),
        pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("event_type", pa.string(), nullable=False),
        pa.field("product_id", pa.int64(), nullable=True),
        pa.field("order_id", pa.int64(), nullable=True),
        pa.field("device_type", pa.string(), nullable=False),
    ]
)

TABLE_SCHEMAS: dict[str, pa.Schema] = {
    "customers": CUSTOMERS_SCHEMA,
    "products": PRODUCTS_SCHEMA,
    "orders": ORDERS_SCHEMA,
    "order_items": ORDER_ITEMS_SCHEMA,
    "events": EVENTS_SCHEMA,
}

PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "customers": ("customer_id",),
    "products": ("product_id",),
    "orders": ("order_id",),
    "order_items": ("order_id", "line_number"),
    "events": ("event_id",),
}


def arrow_type_name(data_type: pa.DataType) -> str:
    """Map Arrow types to stable contract names rather than repr strings."""

    if pa.types.is_int64(data_type):
        return "int64"
    if pa.types.is_int32(data_type):
        return "int32"
    if pa.types.is_string(data_type):
        return "utf8"
    if pa.types.is_decimal(data_type):
        return f"decimal128({data_type.precision},{data_type.scale})"
    if pa.types.is_timestamp(data_type):
        timezone_name = data_type.tz or ""
        return f"timestamp[{data_type.unit},{timezone_name}]"
    raise TypeError(f"unsupported Arrow type in generator contract: {data_type}")


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
    encoded = json.dumps(
        schema_contract(schema),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def ecommerce_contract() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "tables": {
            table_name: {
                "primary_key": list(PRIMARY_KEYS[table_name]),
                "fields": schema_contract(schema),
                "schema_sha256": schema_sha256(schema),
            }
            for table_name, schema in TABLE_SCHEMAS.items()
        },
    }
