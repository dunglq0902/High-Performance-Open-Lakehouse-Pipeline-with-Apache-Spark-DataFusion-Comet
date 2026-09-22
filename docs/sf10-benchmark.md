# Exploratory TPC-H-derived SF10

Run Q01, Q03, Q06 and Q12 with two warm-up pairs and five measured pairs,
the same fixed laptop Spark/Comet allocation as SF1, and the `exploratory`
and `non-audited` labels. Each campaign has its own `EXP-TPCH-SF10-*`
identity. SF10 results are separate from the primary ten-campaign SF1 report.

Prerequisites: a passed SF1 report, clean committed checkout, CPython 3.12.13,
Linux/WSL, Docker, at least 60 GiB free for generation, and a quiet benchmark
window. The existing capacity, correctness, collector, and zero-swap gates
remain mandatory. Use a separate checkout and Compose project to preserve
the original SF1 catalog, data, and evidence.

```bash
export COMPOSE_PROJECT_NAME=lakehouse-comet-sf10
uv sync --all-extras --frozen --python /opt/cpython-3.12.13/bin/python3.12
uv run python scripts/ensure_tpch_data.py --generate --scale-factor 10 \
  --cache /tmp/lakehouse-sf10/dbgen --scratch /tmp/lakehouse-sf10/raw
uv run python scripts/run_research_suite.py \
  --config benchmark/configs/benchmark-laptop-tpch-sf10-q01.yaml \
  --config benchmark/configs/benchmark-laptop-tpch-sf10-q03.yaml \
  --config benchmark/configs/benchmark-laptop-tpch-sf10-q06.yaml \
  --config benchmark/configs/benchmark-laptop-tpch-sf10-q12.yaml
```

Generation uses the existing pinned DBGEN archive, with `-s 10`, explicit
schemas, source hashes, and exact SF10 cardinalities. Lineitem has 59,986,052
rows, not ten times its SF1 count; see the independent
[DuckDB SF10 example](https://duckdb.org/2025/05/23/arrow-ipc-support-in-duckdb).
Foreign keys for lineitem are validated per Parquet file to bound memory;
primary-key ordering is still checked across file boundaries.
On WSL, place DBGEN cache and scratch on the Linux filesystem: DBGEN performs
many small writes, which are slow across the Windows filesystem boundary.
Both final-output and scratch capacity are checked before generation.

Resume by repeating the same suite command from the same commit and image.
Do not regenerate or overwrite existing raw results. A failed capacity or
correctness gate is a stopped campaign, never a successful measurement.
