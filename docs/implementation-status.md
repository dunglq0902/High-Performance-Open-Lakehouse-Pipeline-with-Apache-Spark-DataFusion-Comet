# Implementation status — 2026-08-30

The research software is implemented end to end, but the empirical study is not complete. No
performance claim is publishable until both primary datasets, all ten reviewed campaigns, and the
rebuildable report exist and pass their fail-closed gates.

## Implemented and locally verified

- Locked Spark 4.1.3, Scala 2.13.17, Java 17.0.19, Comet 1.0.0, Iceberg 1.11.0, Python 3.12.13,
  OCI images, Maven artifacts, Python lock, and TPC-H DBGEN source/archive checksum.
- Docker Compose topology for MinIO, REST Iceberg catalog, one Spark master, one 2-core/5-GiB
  worker, and a bounded client container.
- Deterministic E-commerce generation with revisioned dataset IDs, immutable manifests, exact
  CPython version/implementation provenance, content hashes, PK/FK/date/funnel checks, and a
  benchmark-eligible `small` profile.
- TPC-H-derived SF1 acquisition/conversion implementation for all eight tables. The locked fork is
  built from its `Makefile` in source-era GNU C89 mode, emits the fork's no-trailing-delimiter
  format, and is converted to explicit Snappy Parquet schemas. Validation enforces source/build
  provenance (including exact locked CPython), SF1 counts, ordered PKs, all declared FKs, date
  bounds, raw/Parquet hashes, and the derived/non-audited notice. Incomplete DBGEN output is
  removed atomically for a clean retry. This describes the implemented path; the full SF1 output
  is not yet materialized.
- E-commerce Bronze/Silver/Gold Iceberg pipeline and TPC-H-to-Iceberg import with explicit
  `NOT NULL` schemas, persisted counts, source-manifest binding, and pinned snapshot IDs.
- Six reviewed E-commerce workloads (M02, M04, M05, M08, M10, B01) and four reviewed
  TPC-H-derived SF1 workloads (Q01, Q03, Q06, Q12).
- Deterministic paired AB/BA campaigns with correctness and complete-plan gates, warm-ups, hard
  process-group timeouts, clean-commit provenance, zero-swap enforcement, and immutable raw
  records. Resume now preflights every planned artifact before execution against the current Git
  commit, image digest, dataset-manifest hash, SparkConf hash, SQL hash, relevant Iceberg snapshot
  IDs, and host/resource identity; unplanned raw artifacts are rejected.
- Attempt-aware, immutable campaign-verification publication binds the exact 24 raw records, the
  current experiment manifest/config, and hashes of every referenced event log, plan, resource
  sample, stdout, and stderr artifact. A successful resume records its executed/resumed counters
  without overwriting earlier evidence.
- Spark event-log attribution by measured `jobGroupId`, 200-ms worker/driver resource sampling,
  collector calibration below 2%, conservative final-AQE plan analysis, exact result hashes,
  descriptive statistics, paired speedups/resources, and deterministic bootstrap 95% CIs.
- Rebuildable Markdown, JSON, CSV, and SVG reporting. `make report` is fail-closed: publication
  requires exactly the ten current core campaigns, the latest valid 24/24 verification for each,
  all four correctness/plan admission records, ten complete Spark/Comet pairs per workload,
  complete collectors/plan analysis, and no resource-metric exclusions. Partial evidence can only
  be rendered through the explicitly diagnostic path and receives a non-publishable banner;
  rebuilds prune stale per-experiment side artifacts.
- Hosted unit/static CI plus scheduled/manual native Linux smoke workflow.

Latest code-level evidence on this workstation:

- Exact local test runtime: CPython 3.12.13, compiled from the checksum-locked upstream source.
- `ruff check` and `ruff format --check`: passed for 94 Python files.
- `mypy`: passed for 65 source files.
- `pytest`: 238 passed.
- `docker compose config --quiet`: passed.
- The checksum-locked 21,867,962-byte TPC-H archive matches
  `d0d92c4191c776bcc7bce84e0d2156c3a744c115fb9a9ccbcaac908313708c96`; its real DBGEN target
  builds successfully with the reviewed command.
- A real Spark analyzer run confirmed the expected schemas for Q01, Q03, Q06, and Q12.

## Primary data state

- E-commerce `ecommerce-small-uniform-seed-20260827-v1` exists under `data/generated` and was
  content-valid under the previous contract from clean commit
  `80dd6676627e0cb100538be196bb0aab125d616f`. It contains 100,000 customers, 10,000 products,
  1,000,000 orders, 4,000,000 order items, and 5,000,000 events in 22 Parquet files (241,936,114
  bytes). Its legacy manifest still says `benchmark_eligible=true`, but it lacks Python provenance
  and was generated with Python 3.12.11, so the current fail-closed contract treats it as diagnostic
  only.
- E-commerce revision `ecommerce-small-uniform-seed-20260827-v2` is the configured primary and
  must be generated from a clean committed tree with CPython 3.12.13. It does not yet exist.
- The locked TPC-H archive is cached under `.runtime/tpch-dbgen`, but
  `data/generated/tpch-derived-sf1-v1` does not yet exist. Download/checksum, compiler availability,
  archive layout, and the DBGEN build itself have been verified; full generation, conversion, and
  dataset validation remain.
- `results/raw` and `results/reports` contain no research result artifacts yet, and no core campaign
  verification artifacts exist.

## Native evidence and its boundary

Native Linux smoke passed on Docker Desktop/WSL2 on 2026-08-28. Artifact
`.artifacts/smoke/smoke-20260828T141856Z-3279/verification.json` records 21 passed checks for equal
schema/result/row count, the same Iceberg snapshot, runtime locks, baseline absence of native
operators, Comet native operators, and golden-plan semantics. The TPC-H Spark analyzer also passed
for all four reviewed queries.

Those artifacts prove readiness and the Spark/Comet/Iceberg vertical slice, not primary
performance. On 2026-08-30 the Docker Desktop Linux engine was available again (server 29.7.2) and
the repository's Compose configuration remained valid; every new native run still performs its own
service/readiness gates.

## Remaining before the project is empirically complete

1. Review and commit the pending provenance, TPC-H compatibility, schema-check, campaign-evidence,
   and report-gate changes so primary generation has a clean 40-character Git identity.
2. Materialize and validate E-commerce revision v2 with `make research-data-ecommerce`.
3. Materialize and validate TPC-H-derived SF1 with `make research-data-tpch`.
4. Start Docker Desktop/WSL integration, then run the ten config validations and immutable research
   plans after primary data generation is complete.
5. Run all ten campaigns; every campaign must pass capacity, calibration, correctness,
   complete-plan, snapshot, zero-swap, timeout, provenance, and raw-schema gates.
6. Run strict `make report`, review the publishability artifact and fallback explanations, and only
   then draw or publish performance conclusions.

SF10, a larger backlog catalog, multi-node scale-out, continuous operation, and audited TPC-H
claims are outside the primary completion criterion.
