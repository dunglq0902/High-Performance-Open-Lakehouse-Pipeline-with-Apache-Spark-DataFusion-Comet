# Exploratory TPC-H-derived SF10

Run Q01, Q03, Q06 and Q12 with five measured pairs and two untimed warmups in each measurement application,
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

Spark driver and worker containers set memory-plus-swap limits equal to their
5 GiB memory limits, so their cgroups cannot use swap. CPU and Spark heap/off-heap
allocations remain unchanged. The zero-swap collector gate remains mandatory.
A prior run that observed swap is preserved as diagnostic evidence and excluded
from accepted measurements; rerun the suite from a clean, committed checkout.

The I/O collector accepts a cgroup `io.stat` line containing only a valid device
number as an observed zero. The WSL kernel omits standard counters when that
device has no read/write bytes or operations; see
[`blkcg_print_one_stat`](https://github.com/microsoft/WSL2-Linux-Kernel/blob/linux-msft-wsl-6.18.y/block/blk-cgroup.c).
Malformed device identifiers, partially populated counter rows, unreadable files,
and counter resets still fail the resource-evidence gate.

## Second round with ten measured pairs

The independent second round uses `EXP-TPCH-SF10-R2-*` identities, ten measured
pairs per query, and schedule seed `20260923`. It retains the same dataset,
queries, two untimed warmups per measurement application, runtime settings,
resource allocation, and admission gates. There are 24 runs per query and 96
runs in total, including 80 measured runs. Keep its results separate from the
completed five-pair round; do not pool the two rounds into one campaign.

Use the existing validated SF10 dataset and a separate Compose project so the
first round's Iceberg catalog and object storage remain available:

```bash
export COMPOSE_PROJECT_NAME=lakehouse-comet-sf10-r2
uv run python scripts/run_research_suite.py \
  --config benchmark/configs/benchmark-laptop-tpch-sf10-r2-q03.yaml \
  --config benchmark/configs/benchmark-laptop-tpch-sf10-r2-q06.yaml \
  --config benchmark/configs/benchmark-laptop-tpch-sf10-r2-q12.yaml \
  --config benchmark/configs/benchmark-laptop-tpch-sf10-r2-q01.yaml
```

Summarize each `results/raw/EXP-TPCH-SF10-R2-*` directory separately. On WSL,
pass an absolute output path, such as
`--output "$PWD/results/reports/sf10-r2-Q03-summary.json"`, to avoid shared
filesystem rename errors between relative and absolute paths. Resume with
the same query order, committed checkout, and admitted image.
