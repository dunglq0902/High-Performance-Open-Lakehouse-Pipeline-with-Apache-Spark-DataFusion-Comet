"""Pure row functions for the deterministic E-commerce source tables."""

from __future__ import annotations

import math
from collections.abc import Iterator
from datetime import timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from data.generator.constants import (
    CATEGORIES,
    DEVICE_TYPES,
    EVENT_TYPES,
    ORDER_STATUSES,
    PAYMENT_METHODS,
    REGIONS,
    SEGMENTS,
)
from data.generator.prf import (
    choose,
    field_uint64,
    integer_inclusive,
    timestamp_inclusive,
    token,
    weighted_choice,
)
from data.generator.profiles import GeneratorProfile

Row = dict[str, Any]

_CENT = Decimal("0.01")
_DISCOUNT_UNIT = Decimal("0.0001")


def _money_from_cents(cents: int) -> Decimal:
    return (Decimal(cents) / Decimal(100)).quantize(_CENT, rounding=ROUND_HALF_UP)


def _discount_from_basis_points(basis_points: int) -> Decimal:
    return (Decimal(basis_points) / Decimal(10_000)).quantize(
        _DISCOUNT_UNIT, rounding=ROUND_HALF_UP
    )


def customer_row(profile: GeneratorProfile, customer_id: int) -> Row:
    seed = profile.seed
    latest_signup = profile.dataset_end - timedelta(minutes=5)
    name_token = token(seed, "customers", customer_id, "customer_name", length=12)
    email_token = token(seed, "customers", customer_id, "email", length=16)
    return {
        "customer_id": customer_id,
        "customer_name": f"Synthetic Customer {name_token}",
        "email": f"customer-{customer_id}-{email_token}@example.test",
        "region": choose(seed, "customers", customer_id, "region", REGIONS),
        "segment": weighted_choice(
            seed,
            "customers",
            customer_id,
            "segment",
            ((SEGMENTS[0], 55), (SEGMENTS[1], 20), (SEGMENTS[2], 20), (SEGMENTS[3], 5)),
        ),
        "signup_time": timestamp_inclusive(
            seed,
            "customers",
            customer_id,
            "signup_time",
            profile.dataset_start,
            latest_signup,
        ),
    }


def product_base_price_cents(profile: GeneratorProfile, product_id: int) -> int:
    return integer_inclusive(
        profile.seed, "products", product_id, "base_price", minimum=100, maximum=50_000
    )


def product_row(profile: GeneratorProfile, product_id: int) -> Row:
    seed = profile.seed
    created_start = profile.dataset_start - timedelta(days=365)
    name_token = token(seed, "products", product_id, "product_name", length=12)
    return {
        "product_id": product_id,
        "product_name": f"Synthetic Product {name_token}",
        "category": choose(seed, "products", product_id, "category", CATEGORIES),
        "base_price": _money_from_cents(product_base_price_cents(profile, product_id)),
        "created_at": timestamp_inclusive(
            seed,
            "products",
            product_id,
            "created_at",
            created_start,
            profile.dataset_start,
        ),
    }


def order_row(profile: GeneratorProfile, order_id: int) -> Row:
    seed = profile.seed
    customer_id = integer_inclusive(
        seed,
        "orders",
        order_id,
        "customer_id",
        1,
        profile.counts["customers"],
    )
    signup_time = customer_row(profile, customer_id)["signup_time"]
    if not hasattr(signup_time, "tzinfo"):
        raise AssertionError("customer signup_time is not a datetime")
    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "order_time": timestamp_inclusive(
            seed,
            "orders",
            order_id,
            "order_time",
            signup_time + timedelta(minutes=3),
            profile.dataset_end,
        ),
        "status": weighted_choice(
            seed,
            "orders",
            order_id,
            "status",
            (
                (ORDER_STATUSES[0], 70),
                (ORDER_STATUSES[1], 10),
                (ORDER_STATUSES[2], 10),
                (ORDER_STATUSES[3], 10),
            ),
        ),
        "payment_method": choose(seed, "orders", order_id, "payment_method", PAYMENT_METHODS),
    }


def _coprime_multiplier(profile: GeneratorProfile, modulus: int) -> int:
    candidate = (
        field_uint64(profile.seed, "order_items", "allocation", "extra_line_multiplier") % modulus
    )
    if candidate == 0:
        candidate = 1
    while math.gcd(candidate, modulus) != 1:
        candidate = candidate + 1 if candidate + 1 < modulus else 1
    return candidate


def line_count_for_order(profile: GeneratorProfile, order_id: int) -> int:
    """Allocate the exact requested item total with a hash-derived permutation."""

    order_count = profile.counts["orders"]
    item_count = profile.counts["order_items"]
    base, remainder = divmod(item_count, order_count)
    if remainder == 0:
        return base
    multiplier = _coprime_multiplier(profile, order_count)
    offset = (
        field_uint64(profile.seed, "order_items", "allocation", "extra_line_offset") % order_count
    )
    rank = (multiplier * (order_id - 1) + offset) % order_count
    return base + int(rank < remainder)


def order_item_row(profile: GeneratorProfile, order_id: int, line_number: int) -> Row:
    primary_key = (order_id, line_number)
    product_id = integer_inclusive(
        profile.seed,
        "order_items",
        primary_key,
        "product_id",
        1,
        profile.counts["products"],
    )
    base_cents = product_base_price_cents(profile, product_id)
    price_factor = integer_inclusive(
        profile.seed, "order_items", primary_key, "unit_price", 9_000, 11_000
    )
    snapshot_cents = max(0, (base_cents * price_factor + 5_000) // 10_000)
    return {
        "order_id": order_id,
        "line_number": line_number,
        "product_id": product_id,
        "quantity": integer_inclusive(profile.seed, "order_items", primary_key, "quantity", 1, 20),
        "unit_price": _money_from_cents(snapshot_cents),
        "discount": _discount_from_basis_points(
            integer_inclusive(profile.seed, "order_items", primary_key, "discount", 0, 3_000)
        ),
    }


def event_row(profile: GeneratorProfile, event_id: int) -> Row:
    """Generate ordered four-step order sessions, then independent browse events."""

    flow_event_count = profile.counts["orders"] * 4
    if event_id <= flow_event_count:
        order_id = ((event_id - 1) // 4) + 1
        step = (event_id - 1) % 4
        order = order_row(profile, order_id)
        order_time = order["order_time"]
        if not hasattr(order_time, "tzinfo"):
            raise AssertionError("order_time is not a datetime")
        product_id = order_item_row(profile, order_id, 1)["product_id"]
        return {
            "event_id": event_id,
            "session_id": "session-"
            + token(profile.seed, "events", f"order:{order_id}", "session_id", length=24),
            "customer_id": order["customer_id"],
            "event_time": order_time - timedelta(minutes=3 - step),
            "event_type": EVENT_TYPES[step],
            "product_id": product_id if step < 2 else None,
            "order_id": order_id if step == 3 else None,
            "device_type": choose(profile.seed, "events", event_id, "device_type", DEVICE_TYPES),
        }

    customer_selector = field_uint64(profile.seed, "events", event_id, "customer_id") % 4
    customer_id = (
        None
        if customer_selector == 0
        else integer_inclusive(
            profile.seed,
            "events",
            event_id,
            "customer_id_value",
            1,
            profile.counts["customers"],
        )
    )
    return {
        "event_id": event_id,
        "session_id": "session-" + token(profile.seed, "events", event_id, "session_id", length=24),
        "customer_id": customer_id,
        "event_time": timestamp_inclusive(
            profile.seed,
            "events",
            event_id,
            "event_time",
            profile.dataset_start,
            profile.dataset_end,
        ),
        "event_type": choose(profile.seed, "events", event_id, "event_type", EVENT_TYPES[:2]),
        "product_id": integer_inclusive(
            profile.seed,
            "events",
            event_id,
            "product_id",
            1,
            profile.counts["products"],
        ),
        "order_id": None,
        "device_type": choose(profile.seed, "events", event_id, "device_type", DEVICE_TYPES),
    }


def iter_customers(profile: GeneratorProfile) -> Iterator[Row]:
    for customer_id in range(1, profile.counts["customers"] + 1):
        yield customer_row(profile, customer_id)


def iter_products(profile: GeneratorProfile) -> Iterator[Row]:
    for product_id in range(1, profile.counts["products"] + 1):
        yield product_row(profile, product_id)


def iter_orders(profile: GeneratorProfile) -> Iterator[Row]:
    for order_id in range(1, profile.counts["orders"] + 1):
        yield order_row(profile, order_id)


def iter_order_items(profile: GeneratorProfile) -> Iterator[Row]:
    for order_id in range(1, profile.counts["orders"] + 1):
        for line_number in range(1, line_count_for_order(profile, order_id) + 1):
            yield order_item_row(profile, order_id, line_number)


def iter_events(profile: GeneratorProfile) -> Iterator[Row]:
    for event_id in range(1, profile.counts["events"] + 1):
        yield event_row(profile, event_id)


def iter_table_rows(table_name: str, profile: GeneratorProfile) -> Iterator[Row]:
    generators = {
        "customers": iter_customers,
        "products": iter_products,
        "orders": iter_orders,
        "order_items": iter_order_items,
        "events": iter_events,
    }
    try:
        generator = generators[table_name]
    except KeyError as error:
        raise ValueError(f"unknown E-commerce table: {table_name}") from error
    return generator(profile)
