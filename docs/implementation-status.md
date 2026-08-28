# Implementation status — 2026-08-28

The research software is implemented end to end, but the empirical study is not complete. No
performance claim is publishable until the primary datasets, all reviewed campaigns, and the
rebuildable report exist and pass their fail-closed gates.

## Implemented and locally verified

- Locked Spark 4.1.3, Scala 2.13.17, Java 17.0.19, Comet 1.0.0, Iceberg 1.11.0, Python 3.12.13,
  OCI images, Maven artifacts, Python lock, and TPC-H DBGEN source/archive checksum.
- Docker Compose topology for MinIO, REST Iceberg catalog, one Spark master, one 2-core/5-GiB
  worker, and a bounded client container.
- Deterministic E-commerce generator profiles, including the benchmark-eligible `small` profile
  (100k customers, 10k products, 1m orders, 4m order items, and 5m events), immutable manifests,
  content hashes, PK/FK/date checks, and capacity admission.
- TPC-H-derived SF1 acquisition and conversion for all eight tables. The importer locks the DBGEN
  commit and 21-MiB archive SHA-256, writes explicit Snappy Parquet schemas, verifies SF1 row
  counts, ordered PKs, all declared FKs and date bounds, and labels the data/result non-audited.
- E-commerce Bronze/Silver/Gold Iceberg pipeline and TPC-H-to-Iceberg import, with explicit
  `NOT NULL` schemas, persisted counts, source-manifest hash binding, and pinned snapshot IDs.
- Six reviewed E-commerce workloads: M02 filter, M04 join, M05 low-cardinality aggregate, M08
  window, M10 shuffle aggregation, and B01 business join/aggregation.
- Four reviewed TPC-H-derived workloads: Q01, Q03, Q06, and Q12 at primary SF1. SF10 remains an
  optional exploratory extension after its separate capacity gate.
- Deterministic paired AB/BA campaign planning, separate correctness and plan gates, warm-ups,
  hard process-group timeout, immutable raw records, strict resume, clean-commit provenance, and
  sequential execution of all ten core workloads.
- Spark event-log attribution keyed by the measured `jobGroupId`, including Spark 4 nested root
  execution families. Failed/retried task attempts remain in consumed-resource totals; only
  terminal SQL/job failures contradict a successful application.
- Driver and worker/cgroup sampling at 200 ms, with an explicit start/acknowledge/stop protocol so
  worker CPU/RAM covers the measured terminal action rather than startup or warm-up. The campaign
  also calibrates and rejects collector overhead at or above 2%.
- Conservative final-AQE plan analysis, Comet native/fallback/transition coverage, exact
  correctness hashes, same-Iceberg-snapshot enforcement, resource metrics, descriptive statistics,
  paired speedups, deterministic bootstrap 95% CIs, paired resource savings, ratio of medians, and
  suite geometric-mean reporting.
- Rebuildable Markdown, JSON, CSV, and SVG reporting from immutable raw records. Smoke artifacts
  are explicitly rejected as research-report inputs.
- Hosted unit/static CI plus scheduled/manual native Linux smoke workflow.

Latest code-level evidence on this workstation:

- `ruff check` and `ruff format --check`: passed for 90 Python files.
- `mypy`: passed for 63 source files.
- `pytest`: 186 passed.
- `docker compose config --quiet`: passed.
- Locked TPC-H archive SHA-256 independently downloaded and matched
  `d0d92c4191c776bcc7bce84e0d2156c3a744c115fb9a9ccbcaac908313708c96`.

## Native evidence and its boundary

The native Linux smoke passed on Docker Desktop/WSL2 on 2026-08-27. Artifact
`.artifacts/smoke/smoke-20260827T072326Z-1701/verification.json` records passed checks for equal
schema/result/row count, the same Iceberg snapshot, runtime locks, baseline absence of native
operators, Comet native operators, and golden-plan semantics. The stack was cleanly removed after
the run.

That artifact proves the Spark/Comet/Iceberg vertical slice, not primary performance. The Docker
image containing the subsequent TPC-H and measurement-window changes could not be rebuilt on
2026-08-28 because Docker Desktop 4.86 repeatedly crashed before engine startup while removing a
stale Windows AF_UNIX `dockerInference` socket. Project code did not reach execution in those
attempts; Docker settings were restored and temporary sockets were cleaned afterward.

## Remaining before the project is empirically complete

1. Review and commit the current implementation so primary provenance has a clean 40-character
   Git identity. The runner intentionally refuses the present dirty/untracked worktree.
2. Restore or update Docker Desktop, then rebuild the image and rerun native smoke plus the Spark
   analyzer check for all four TPC-H-derived expected schemas.
3. Run `make research-data` to materialize and validate E-commerce `small` plus TPC-H-derived SF1.
   TPC-H generation requires at least 20 GiB free and a local C compiler/`make`.
4. Run `make validate-research`, `make research-plan`, and `make benchmark`. Every one of the ten
   campaigns must pass capacity, calibration, correctness, complete-plan, snapshot, zero-swap,
   timeout, and raw-schema gates.
5. Run `make report`, review exclusions/failures/fallback explanations, and only then draw or
   publish performance conclusions.

SF10, the larger backlog catalog, multi-node scale-out, continuous operation, and audited TPC-H
claims are outside the primary completion criterion.
