# ADR 0001: First implementation slice

- Status: Accepted
- Date: 2026-08-24

## Context

The repository initially contained an implementation-ready research specification but no
executable artifacts. The specification's Definition of Ready spans infrastructure, deterministic
data, one workload, correctness, plan capture, schemas, and runtime verification. Implementing
only an empty directory tree would not prove any of those contracts.

Two external constraints affect the runtime design:

1. Comet 1.0.0 publishes Linux-only native libraries. The amd64 binary targets x86-64-v3, so the
   native smoke is a Linux container gate with a CPU preflight.
2. Spark 4.1.3 S3A uses Hadoop 3.4.2 and AWS SDK 2.29.52, while Iceberg 1.11.0's AWS bundle exposes
   AWS SDK 2.33.4 classes. Both bundles must not be placed on one unreviewed classpath.

The last public MinIO server image predates the fix for CVE-2025-62506. The upstream project asks
container users to build the fixed release from source.

## Decision

The first slice is a vertical, non-research smoke path:

- Docker-independent validation generates a tiny non-benchmark fixture, validates JSON/YAML
  contracts, creates a deterministic paired AB/BA schedule, and writes an immutable dry-run
  manifest.
- The native integration profile uses Iceberg `S3FileIO` only and therefore installs
  `iceberg-aws-bundle` but not `hadoop-aws` or its AWS SDK bundle.
- A future `parquet-s3a` profile will install Hadoop 3.4.2's `hadoop-aws` with SDK 2.29.52 and will
  not install `iceberg-aws-bundle`. Results from these storage profiles remain separate.
- The REST fixture is pinned at 1.10.1 as protocol-compatible test infrastructure. It is not
  represented as an Iceberg 1.11.0 server.
- MinIO is compiled from the fixed upstream tag `RELEASE.2025-10-15T17-29-55Z` into a scratch
  runtime image, with the source archive checksum locked. This keeps an older vulnerable binary
  out of lower runtime-image layers. Its final local OCI digest is captured by the native-smoke
  provenance step before any campaign can be promoted.
- CPython 3.12.13 is compiled from the checksum-locked upstream source inside the Spark image;
  `uv` only installs the hash-locked project environment and cannot substitute another interpreter.
- Smoke artifacts live outside `results/raw`; they are readiness evidence, not performance data.

## Consequences

The first native smoke proves Spark/Comet correctness and native Iceberg reading without accepting
an ambiguous AWS SDK classpath. Direct `s3a://` Parquet benchmarking is deliberately deferred until
its separate runtime target and duplicate-class gate are implemented. MinIO image builds are slower,
but the final runtime contains only the source-built server, a static health probe, CA certificates,
and its writable directories.
