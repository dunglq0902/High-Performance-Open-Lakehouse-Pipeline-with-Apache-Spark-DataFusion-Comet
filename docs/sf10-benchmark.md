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

`make benchmark-sf10` is the equivalent round-2 command, with that query order
and `lakehouse-comet-sf10-r2` as its default Compose project. Resume with the
same query order, committed checkout, and admitted image. A new measurement
at a different commit requires a fresh checkout/evidence namespace; never
reuse completed canonical records as measurements of the new code.

## Rebuild a report without running Spark

The tracked reporting command verifies the latest completion checkpoint,
planned run identities, raw-record and artifact/control hashes, dataset
manifest/attestation binding, source commit, image and resource/runtime
consistency. It also requires complete driver/worker resource windows with
zero swap, then recomputes paired statistics from the raw records.

From this final code checkout, point it at the preserved SF10 evidence root
(or an intact restored copy). The root must contain `results/raw`, the
campaign/control trees under `.artifacts`, and the dataset manifest at
`data/generated/tpch-derived-sf10-v1/manifest.json`:

```bash
make report-sf10 \
  SF10_EVIDENCE_ROOT="/path/to/preserved-sf10-checkout" \
  SF10_SOURCE_COMMIT=e8c0c5c834b6ff2fd792f2220f79a5ae9db8728e \
  SF10_OUTPUT_DIR=results/sf10/rebuilt-r2
```

The output directory must be new. It contains four query summaries,
`summary.json`, `report.md` and `verification.json` with output hashes.
No Docker service is started and no source evidence is modified. The command
does not rescan all Parquet data or confer the core publication status.
For round 1, supply `SF10_ROUND=1`, source commit
`412e949e76de838d58385ccc135dce579be94df9` and a different output directory.
For a new campaign, use its actual measurement commit, not these historical IDs.

Round 2 includes a disclosed WSL restart between completed query groups and
three archived Q01 launcher failures before query execution. When present,
the resume receipt and the archived incident inventory are checked and
included in the rebuilt report. Keep these files when restoring evidence.
The original round-2 result has 96 successful records, 80 measured runs,
192 complete zero-swap resource windows and 21,443 samples. Its Q03 confidence
interval contains 1; the report must describe that result as inconclusive.

The [documentation snapshots](benchmarks/sf10/README.md) are small, byte-hash-bound
report copies for review; they do not contain all raw/control evidence needed
by this command. Their bytes are preserved by `.gitattributes` across checkouts.
The final code commit and the historical measurement commit serve different
purposes and must remain distinct in reports.
