# Reviewed workload contracts

Every workload has one SQL file and one YAML manifest. SQL is the query-logic source of truth;
the manifest locks relation bindings, typed parameters, operator intent, bounded materialization,
expected Spark schema, ordering, and correctness rules. Engine-specific SQL hints are prohibited.

## Core catalog

| ID | Suite | Primary operator/shape | Iceberg inputs |
|---|---|---|---|
| M02 | Micro | filter/selectivity | Bronze orders |
| M04 | Micro | join | Bronze orders + order items |
| M05 | Micro | low-cardinality aggregate | Bronze orders |
| M08 | Micro | window | Silver events |
| M10 | Micro | shuffle aggregation | Silver events |
| B01 | Business | join + aggregate | Bronze orders/customers/order items |
| Q01 | TPC-H-derived | scan/filter/aggregate | TPC-H lineitem |
| Q03 | TPC-H-derived | three-way join/aggregate | customer/orders/lineitem |
| Q06 | TPC-H-derived | selective scan/reduction | TPC-H lineitem |
| Q12 | TPC-H-derived | join/CASE aggregate | orders/lineitem |

The four Q workloads are a reviewed SF1 subset and are always labeled derived/non-audited. They
are not an official TPC-H power or throughput result.

## Binding and snapshot rules

Before executing SQL, the runner creates every temporary view declared in `relation_bindings`.
E-commerce relations bind to `lakehouse.bronze`, `lakehouse.silver`, or `lakehouse.gold`; TPC-H
relations bind only to `lakehouse.tpch`. Every view is read with its recorded Iceberg `versionAsOf`
snapshot. Baseline and Comet correctness runs must use the same snapshot set, SQL bytes, manifest,
dataset hash, and parameters before measurements are admitted.

The runner validates every required column's Spark type and nullability. A storage representation
or schema change creates a distinct comparison identity; it cannot silently replace an existing
binding.

## Parameter rendering

The renderer accepts only values admitted by the manifest type/format/allowlist and substitutes
them into fixed literal positions. It rejects missing, extra, malformed, or arbitrary SQL values.
Time intervals are UTC and normally half-open. Result limits and complete stable tie-breaker
ordering keep every `collect` workload bounded and canonically hashable.

M02's selectivity expression uses positive one-based identifiers:
`((order_id - 1) % 100) < bucket`. Dataset validation independently checks its 1%, 10%, and 50%
buckets within the specified tolerance.

## Expected schema and correctness

`expected_schema.canonical_json` is minified Spark struct JSON; `expected_schema_hash` is the
lowercase SHA-256 of those exact UTF-8 bytes. The application compares the analyzed schema to this
contract, then hashes every ordered result row with type-aware canonical encoding. Dataset-specific
result hashes belong to immutable run artifacts, not the reusable workload manifest.

Expected schemas and result evidence must be generated or verified with the locked Spark runtime.
They must never be edited merely to make a failing workload pass.
