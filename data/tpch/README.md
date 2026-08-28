# TPC-H-derived SF1 data path

This package builds a research dataset derived from the pinned `gregrahn/tpch-kit` DBGEN source.
It is not an audited or official TPC-H result.

The source commit, codeload URL, TPC license notice, and archive SHA-256 are locked in
`runtime-versions.lock`. Generation requires Linux/WSL, `gcc`, `make`, at least 20 GiB free, and a
clean committed worktree:

```bash
make research-data-tpch
```

The command builds DBGEN with fixed flags and locale/timezone, produces the eight SF1 `.tbl`
tables, parses them with explicit Arrow `DECIMAL`/`DATE` schemas, and writes deterministic Snappy
Parquet under `data/generated/tpch-derived-sf1-v1`. Output is immutable.

Validation fails closed on source or file hash drift, wrong schemas, SF1 row counts, unordered or
duplicate primary keys, orphan foreign keys, invalid date bounds, unexpected files, dirty
provenance, a missing derived/non-audited notice, or a non-benchmark-eligible manifest. The
research pipeline then imports all eight tables into `lakehouse.tpch` and records pinned Iceberg
snapshot IDs for every query run.
