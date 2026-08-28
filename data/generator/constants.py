"""Versioned constants that participate in dataset identity."""

from __future__ import annotations

GENERATOR_NAME = "data.generator"
GENERATOR_VERSION = "1.0.0"
DATASET_SCHEMA_VERSION = 1

# Content hashes are deliberately independent from Parquet layout. The first
# primary-key component selects a fixed range; composite keys remain ordered
# within the leaf.
PRIMARY_KEY_RANGE_SIZE = 10_000
CONTENT_HASH_ALGORITHM = "sha256-merkle-pk-range-v1"
CANONICAL_ROW_ENCODING = "length-prefixed-arrow-scalars-v1"

TABLE_ORDER = ("customers", "products", "orders", "order_items", "events")

REGIONS = (
    "AFRICA",
    "ASIA_EAST",
    "ASIA_SOUTH",
    "EUROPE_EAST",
    "EUROPE_WEST",
    "LATIN_AMERICA",
    "MIDDLE_EAST",
    "NORTH_AMERICA",
)
SEGMENTS = ("CONSUMER", "CORPORATE", "SMALL_BUSINESS", "VIP")
CATEGORIES = tuple(f"CAT{index:02d}" for index in range(1, 21))
ORDER_STATUSES = ("COMPLETED", "CANCELLED", "REFUNDED", "PENDING")
PAYMENT_METHODS = ("CARD", "BANK_TRANSFER", "DIGITAL_WALLET", "CASH_ON_DELIVERY")
EVENT_TYPES = ("view_product", "add_to_cart", "checkout", "purchase")
DEVICE_TYPES = ("mobile", "desktop", "tablet")
