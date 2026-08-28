# ADR 0002: Laptop-bounded research scope

- Status: Accepted
- Date: 2026-08-26

## Context

The project is an eight-week, single-researcher study executed on a shared Dell Precision 5550
with six physical CPU cores, 16 GiB RAM, Docker Desktop and WSL2. The original specification
included an 8-core/16-GiB executor profile, multi-node execution, and larger scale factors.
That envelope cannot run without CPU oversubscription or memory pressure on the available host and
would make completion and repeatable measurement unrealistic.

The laptop is already proven sufficient for the non-research native smoke. Performance campaigns
can run in controlled windows, but the machine is also used for browser, IDE and other daily work.

## Decision

- Execution is manual and batch-oriented. Continuous operation and scale-out are outside scope.
- `smoke-local` and `smoke-standalone` retain their existing verified settings.
- The only planned research runtime is `benchmark-laptop`: one Spark worker, one executor, two
  executor cores, 2 GiB executor heap, 1 GiB memory overhead, 1 GiB off-heap and 1 GiB driver heap.
- TPC-H-derived SF1 is primary. SF10 is optional after SF1 correctness/stability and capacity gates.
- All scale factors above SF10 are excluded from active configuration and deliverables.
- The M01–M10 and B01–B10 lists remain design catalogs. Only a reviewed core subset with complete
  SQL, manifest, correctness and plan evidence is required.
- Core SF1 workloads use at least 10 paired measurements. Optional SF10 workloads use at least 5
  and are labelled exploratory. P95 is not a deliverable for these reduced sample sizes.
- Accepted measurements require a controlled benchmark window and no observed WSL cgroup swap.
  Runs affected by paging, thermal throttling or substantial background contention are diagnostic.

## Consequences

- Active experiment and result schemas accept only scale factors 1 and 10 and reject the retired
  runtime profiles.
- The project can answer relative performance, correctness, native coverage and fallback questions
  on representative single-node workloads. It cannot claim broad scalability or production
  capacity.
- Not running SF10 does not fail the project; the report then omits scale-sensitivity conclusions.
- Expanding scale, resource envelope, runtime profile or mandatory workload set requires a new ADR
  and a new experiment identity.
