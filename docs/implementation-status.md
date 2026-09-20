# Implementation status — 2026-09-08

The research software and both primary datasets are implemented and materialized. The primary
E-commerce dataset is revision v3, generated from a reviewed clean generator commit.
The empirical publication state is deliberately not hard-coded in this document: it is determined
by `results/reports/report-publishability.json`, rebuilt from the current immutable campaign
evidence. Unless that artifact exists and contains `publishable: true`, no performance claim is
publishable.

## Implemented controls and pre-campaign evidence

- Locked Spark 4.1.3, Scala 2.13.17, Java 17.0.19, Comet 1.0.0, Iceberg 1.11.0, Python 3.12.13,
  OCI images, Maven artifacts, Python lock, and TPC-H DBGEN source/archive checksum.
- Docker Compose topology for MinIO, REST Iceberg catalog, one Spark master, one 2-core/5-GiB
  worker, and a bounded client container. Each Medallion and benchmark driver is pinned through an
  immutable per-attempt Compose override to the admitted worker image ID, with pulling disabled.
  Master/worker image equality is checked before admission. A fresh suite builds for its first
  campaign only, then requires that same admitted image for every remaining campaign without
  rebuilding. After a process or WSL interruption, the suite first verifies a completed campaign,
  its manifest and raw-record digest, current-commit provenance, and capacity admission; it resumes
  the very first campaign with `--no-build` only when that exact image ID still exists locally.
  Missing images, incomplete evidence without a verified completed prefix, and non-contiguous suite
  state stop before any rebuild can replace the admitted image.
- Deterministic E-commerce generation with revisioned dataset IDs, immutable manifests, exact
  CPython version/implementation provenance, physical and logical content hashes, PK/FK/date/funnel
  checks, and a benchmark-eligible `small` profile.
- TPC-H-derived SF1 acquisition and conversion for all eight tables. The locked fork is compiled in
  source-era GNU C89 mode and converted to explicit Snappy Parquet schemas. The validator enforces
  source/build provenance, exact SF1 counts, ordered PKs, all declared FKs, date bounds,
  raw/Parquet hashes, and the derived/non-audited notice. Its row-scale PK, FK, and date comparisons
  now use Arrow compute kernels while preserving cross-batch and cross-file ordering checks; that
  validator revision is committed at `44304edaa5e2dd9920abed28256387f22a64f9bc`.
- Content-bound dataset validation attestations. Suite preparation performs the expensive semantic
  validation once per unique dataset, then binds the result to the clean Git commit, locked Python
  and PyArrow runtimes, runtime lock, manifest bytes, and the SHA-256/size/row inventory of every
  current Parquet file. V2 receipts may rebind only from a full receipt at an ancestor commit when
  the suite-specific semantic-validator Git tree and every runtime/content identity are unchanged;
  TPC-H also rehashes all eight source `.tbl` files, and rebound receipts cannot form a chain. This
  is a trusted-local-workspace cache receipt, not a cryptographic signature or external
  attestation; strict publication independently verifies the receipt, lineage, and current physical
  inventory. Receipts are verified in staging before immutable publication; Git provenance rejects
  replacement refs, grafts, and inherited repository redirects. The Spark container rechecks the
  host-bound content/runtime/origin evidence without requiring a mounted Git object database;
  host preparation and strict reporting always resolve the actual Git lineage.
- E-commerce Bronze/Silver/Gold Iceberg pipeline and TPC-H-to-Iceberg import with explicit
  `NOT NULL` schemas, persisted counts, source-manifest/attestation binding, and pinned snapshot IDs.
  For one clean commit, Spark image, persistent Docker-volume identity, dataset, and attestation,
  the six E-commerce workloads share one Medallion build and the four TPC-H workloads share
  another; collector calibration is also shared for that runtime identity. Recreated object-store
  or catalog volumes force a new build.
- Six reviewed E-commerce workloads (M02, M04, M05, M08, M10, B01) and four reviewed
  TPC-H-derived SF1 workloads (Q01, Q03, Q06, Q12). Suite preparation creates all ten deterministic,
  immutable experiment manifests after validating the two unique datasets.
- Deterministic paired AB/BA campaigns with correctness and complete-plan gates, warm-ups, hard
  process-group timeouts, clean-commit provenance, zero-swap enforcement, and immutable evidence.
  Each planned run has at most three attempts. Failed attempt records and logs remain immutable
  under the campaign artifact tree; only a successful terminal record is published to `results/raw`.
  Only `failed`/`timeout` are retryable; invalid result/environment gates hard-stop across resumes.
- Resume preflights every planned raw artifact against the current Git commit, image digest,
  dataset-manifest hash, SparkConf hash, SQL hash, relevant Iceberg snapshot IDs, and host/resource
  identity. Unplanned or unsuccessful raw artifacts are rejected.
- Attempt-aware campaign verification binds the exact 24 successful raw records, experiment
  manifest/config, every run-attempt tree and failed record, dataset receipt, capacity gate,
  collector calibration, Medallion audit, and every referenced event log, plan, resource sample,
  stdout, and stderr file. A successful resume records executed/resumed and total/failed-attempt
  counters without overwriting earlier evidence.
- Spark 4 rolling event-log V2 directories use a separate durable native Linux `/var/tmp` mount for
  each Medallion or campaign application, avoiding both the Windows/WSL bind-mount `chmod` failure
  and loss from WSL's tmpfs-backed `/tmp` across a distro restart, while keeping Spark non-root and
  its event-log configuration unchanged. New recovery pointers use schema v2; the reader maps
  legacy schema v1 only to `/tmp` and v2 only to `/var/tmp`; evidence supplies only a validated
  basename token, never an arbitrary root or absolute path.
  After the client stops, a
  scoped container-side permission handoff precedes immutable archival into that attempt's evidence
  tree. Native cleanup starts only after the complete archive is published and its exact owned
  source is revalidated. Failures before cleanup preserve the source and pointer; any failure blocks
  admission, recovery availability still depends on host/filesystem state, and retries do not
  overwrite earlier event logs. Benchmark profiles explicitly disable event-log
  compression so the strict dependency-free JSON parser consumes the exact emitted segments.
- Spark event-log attribution by measured `jobGroupId`, 200-ms worker/driver resource sampling,
  collector calibration below 2%, conservative final-AQE plan analysis, exact result hashes,
  descriptive statistics, paired speedups/resources, and deterministic bootstrap 95% CIs.
- Plan classification uses reviewed exact operator names after package/`Exec` normalization.
  Spark `BroadcastExchange` is a real operator, not a wrapper. Comet's JVM
  `CometColumnarExchange` counts as non-native fallback, while data-conversion nodes count as
  transitions. Empty plans, unconfirmed/unfinished adaptive trees (including nested retained
  trees), unparseable content, planning placeholders, and unknown lookalike names cannot pass
  admission. The fail-closed campaign and publication gates remain unchanged.
- Rebuildable Markdown, JSON, CSV, and SVG reporting. `make report` is fail-closed: publication
  requires exactly the ten current core campaigns, the latest verification attempt for each to
  pass 24/24, all correctness/plan gates, ten complete Spark/Comet pairs per workload, complete
  collectors/plan analysis, no resource-metric exclusions, current input hashes, and one current
  clean Git HEAD across all raw records. `make report-diagnostic` can render partial evidence but
  marks it non-publishable.
- The report now includes complete latency distributions and fixed-seed median CIs, normalized
  CPU/RAM profiles over time, a native/fallback operator matrix, initial/final AQE plan stability,
  explicit RQ1/RQ2/RQ3 and H1/H2/H3 findings, attempt-aware failure counts, and report artifact
  inventory. Its independent content contract must pass in addition to the campaign-evidence gate.
- A generated 12-slide deck is independently checked for package shape, 16:9 geometry, editable
  native charts/tables, speaker notes, diagnostic labels, binding to the current report, and an
  explicit full-deck visual-review attestation. The
  existing local deck is deliberately diagnostic because its campaign provenance predates the
  current tracked changes; the final deck must be regenerated after the fresh campaign.
- A Spark History Server demo profile and staging tool replay one exact baseline/Comet measurement
  pair from immutable event logs. It verifies the current raw campaign hash, pair identity,
  correctness, completed applications, exact measured SQL start/end, and report status. A separate
  live readiness gate then requires exactly those two completed applications and probes their REST,
  SQL-index, and direct measured-execution pages before recording. Diagnostic evidence is visibly
  labelled and cannot be finalized as publication video evidence.
- The video finalizer checks the MP4 container, duration, resolution, explicit visual-review
  confirmation, application IDs, event-log source, report hash, and Git commit. A strict ZIP64
  release builder then collects the Git history bundle, raw/report/campaign/control evidence, both
  full datasets and attestations, slide/video/demo artifacts, restore guide, exact SHA-256 inventory,
  exact empty control directories, canonical manifest self-hash, and outer archive checksum. It
  reconstructs control fingerprints at packaging time, refuses dirty/stale/diagnostic input, and
  never overwrites an existing release.
- A separate evidence-rotation command now dry-runs by default, accepts only the exact core
  experiment directories, verifies raw/manifest commit provenance and a byte-level inventory,
  preserves root sentinels, refuses overwrite, and stages moves with automatic and journaled
  rollback before a fresh commit's immutable plans are prepared.
- Hosted unit/static CI plus scheduled/manual native Linux smoke workflow.

Fixed test-count claims are intentionally omitted because they become stale as gates are added. On
the final clean pre-campaign commit, `make lint test compose-config` is the executable code-level
gate. The native readiness evidence remains:

- Exact local test runtime: checksum-locked CPython 3.12.13 through `uv` 0.8.15.
- Docker Desktop Linux engine 29.7.2 with cgroup v2; Compose configuration validates locally.
- The checksum-locked 21,867,962-byte TPC-H archive matches
  `d0d92c4191c776bcc7bce84e0d2156c3a744c115fb9a9ccbcaac908313708c96`; its DBGEN target builds
  successfully with the reviewed command.
- A real Spark analyzer run confirmed the expected schemas for Q01, Q03, Q06, and Q12.
- On 2026-09-04, a default non-root Spark client completed a local count query with the benchmark
  event-log settings and the then-current native staging mount. Its rolling V2 log was archived and
  rediscovered successfully; `.artifacts/diagnostics/spark-event-log-probe-20260904/verification.json`
  records that historical permission-handoff check. It predates the `/var/tmp` durability change
  and is not research performance evidence.

## Primary data state

The configured primary datasets currently have these states:

| Dataset | Rows | Parquet files | Parquet bytes | Generator provenance |
|---|---:|---:|---:|---|
| `ecommerce-small-uniform-seed-20260827-v3` | 10,110,000 | 18 | 241,845,093 | generator v1.0.0, clean `701883a6e8977310986d7e0cba26cae498d7b340`, CPython 3.12.13/PyArrow 21.0.0 |
| `tpch-derived-sf1-852ad0a5ee31-v1` | 8,661,245 | 9 | 370,681,574 | generator v1.0.2, clean `be0a4981ab047a6d8b3926cb818be1b2932e8f5c`, CPython 3.12.13 |

E-commerce v3 keeps 100,000 customers, 10,000 products, 1,000,000 orders, 4,000,000 order items,
and 5,000,000 events. Its physical change raises the `order_items` target from 500,000 to 1,000,000
rows per file, reducing that table from eight to four Parquet files. The generator contract now
rejects a benchmark-eligible profile that reintroduces the fragmented target. All six core
E-commerce configs point to v3. Its 18 Parquet files total 241,845,093 bytes, all 16 fact files are
larger than 8 MiB, and the manifest file SHA-256 is
`05ab005cc05d6c95cf7e976d2ece4c7a2e6b07efa7d329a5d893d1b156686bce`.

TPC-H-derived SF1 contains 25 nations, 5 regions, 10,000 suppliers, 150,000 customers, 200,000
parts, 800,000 partsupp rows, 1,500,000 orders, and 6,001,215 lineitems. Its manifest file SHA-256
is `4aff1a0bc09c2b02f09e1c91f30cb0abb287b03976c2d24c5f211c8cf068a552`, and its manifest
self-hash is `0e563dd019407560858df4701cd4d5fc69d215348ddc499bbaf9b59bc31de1f8`. DBGEN is locked to
source commit `852ad0a5ee31ebefeed884cea4188781dd9613a3` and archive SHA-256
`d0d92c4191c776bcc7bce84e0d2156c3a744c115fb9a9ccbcaac908313708c96`; the generated raw
`.tbl` inputs total 1,092,031,885 bytes.

Dataset attestations and experiment plans are generated for the current clean control-plane commit,
not for the older generator commits. Every changed commit requires a new canonical outer
attestation. A v2 receipt may avoid repeating the semantic scan only when its full origin is an
ancestor and the semantic-validator Git tree, runtime lock/runtime, manifest, Parquet inventory and
TPC-H source inventory remain exact; otherwise preparation falls back to full validation.

## Native evidence and its boundary

Native Linux smoke passed on Docker Desktop/WSL2 on 2026-08-28. Artifact
`.artifacts/smoke/smoke-20260828T141856Z-3279/verification.json` records 21 passed checks for equal
schema/result/row count, the same Iceberg snapshot, runtime locks, baseline absence of native
operators, Comet native operators, and golden-plan semantics. The Spark analyzer also passed for
all four reviewed TPC-H-derived queries.

Those artifacts prove readiness and the Spark/Comet/Iceberg vertical slice, not primary
performance. This document likewise makes no claim that the ten core campaigns have passed; only
the latest strict report publishability artifact may make that determination.

The diagnostic preflight in `.artifacts/diagnostics/plan-preflight-20260904-01/` executed all ten
core workloads in both engines. Every pair matched schema, row count, and canonical result hash.
These were shared-session, non-publishable diagnostic runs, not the independent paired performance
campaigns. Its original comparison used an earlier parser; the retained plans are reviewed again
under the corrected classifier. The versioned core plan regression corpus records all twenty
captured final AQE plans and their reviewed analyses, alongside exact runtime/config/SQL/workload
identities. In the observed M08 Comet plan, four of ten operators are native and six are non-native;
this is an operator-count ratio, not a timing or speedup claim.

The shuffle distinction follows the locked Comet source's
[native versus JVM shuffle branches](https://github.com/apache/datafusion-comet/blob/3a7a2c437cc771621b6040a308657573dbc1b9c2/spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometShuffleExchangeExec.scala).
Earlier M02 measurements and the M04 parser rejection are preserved as historical evidence. The
later `c33e1f0` campaign completed M02, M04, and M05 before its admitted Docker image became
unavailable after an interruption; its 76 raw records, three passed campaign verifications, and
one formally closed M08 `InterruptedAttempt` are byte-verified under
`.artifacts/campaign-archives/c33e1f0ff938a9e5beabc5ebbf20da0f492f0852/lost-pinned-image-20260905/`.
That campaign exposed and motivated the cross-process no-rebuild resume gate above. A committed
control-plane correction requires fresh current-commit campaigns; neither an old failure nor an old
successful raw record is rewritten or relabeled to satisfy publication.

## Shortest reviewed campaign workflow

Run from Ubuntu/WSL with Docker Desktop integration, no competing workload, and a clean committed
tree:

1. Run `make lint test compose-config` on the final commit, then verify (or materialize when absent)
   E-commerce v3 with `make research-data-ecommerce`.
2. Optionally run `make research-plan` to create/review the ten plans without starting campaigns.
   It full-validates a dataset without a reusable origin, or exact-rebinds a reviewed full receipt,
   and publishes content-bound attestations for the current clean commit.
3. Run `make benchmark`. This includes the same suite preparation, so the shortest workflow does
   **not** run `make validate-research` or `make research-plan` first. Existing current attestations
   are verified rather than semantically rescanned.
4. Run `make report`, then inspect `results/reports/report-publishability.json`. Draw or publish
   performance conclusions only when it records `publishable: true`.
5. Generate and visually inspect the final PPTX from that admitted report, then run
   `make presentation-finalize PRESENTATION_VISUAL_REVIEW_ATTESTATION=1`.
6. Run `make demo-ui`, record and review the paired Spark History Server comparison, then run
   `make demo-video-finalize VIDEO_VISUAL_REVIEW_ATTESTATION=1` (and add the full-playback
   attestation only when automated complete-frame decoding is unavailable).
7. Run `make evidence-bundle` followed by `make verify-evidence-bundle`, or use the sequential
   aggregate target `make release` once both final media files exist.

All tracked code and documentation must be finalized before the first campaign. The strict report
requires raw provenance to equal the current clean HEAD; a later tracked commit intentionally
invalidates publication until matching campaign evidence is produced.

SF10, a larger backlog catalog, multi-node scale-out, continuous operation, and audited TPC-H
claims remain outside the primary completion criterion.

At this checkpoint, the report content contract, diagnostic slide deck, Spark UI replay mechanism,
media sidecar gates, and release packager are implemented. The project is still **not 100% complete**:
the final tracked commit, fresh ten-campaign rerun at that commit, strict publishable report, final
non-diagnostic PPTX/MP4, and verified full release ZIP remain outstanding execution artifacts.
