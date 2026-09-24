# Research presentation output

`build_presentation.mjs` generates the 13-slide, 16:9 research deck from the admitted files in
`results/reports`. It uses editable native charts/tables and runs the bundled presentation
integrity/layout finalizer. Because that builder depends on the Codex presentation artifact runtime,
invoke it from a Codex session with the presentation skill/runtime paths loaded.

The current clean-campaign output name is:

`lakehouse-comet-research-v2.pptx`

After generation, render and inspect every slide, then explicitly record that review:

```bash
make presentation-finalize PRESENTATION_VISUAL_REVIEW_ATTESTATION=1
```

The finalizer independently checks the package shape, slide count/size, charts, tables, speaker
notes, diagnostic labels, and the exact report-artifact inventory, then creates
`lakehouse-comet-research.manifest.json` for release packaging. Every
inventory entry must be sorted, unique, path-safe, and match the current report file size and
SHA-256; unlisted files in the report directory also block finalization.

Presentation sidecar schema version 2 adds `artifact_total_bytes`, `artifact_set_sha256`, and
`artifact_set_canonicalization` under `report_inventory`. The artifact-set hash uses compact UTF-8
JSON with sorted object keys over the already path-sorted array of `{path, sha256, size_bytes}`
entries. This binds the deck to the verified report artifact set independently of the inventory
file's own byte hash. The root `visual_review` record is mandatory for a publishable deck and
attests that all rendered slides were checked for clipping, overlap, legibility, chart/table
rendering, and the correct publication label. The Make target never infers this attestation.

When report evidence is not publishable, the builder requires `--allow-diagnostic`, visibly labels
all non-cover slides, and the finalizer writes a `diagnostic` sidecar. Such a deck is for rehearsal
only and cannot enter the final evidence bundle.


## Updated SF10 research deck (2026-09-24)

The combined **15-slide** deck is `lakehouse-comet-research-sf10-20260924.pptx`.
It retains the core results, adds an editable SF10 round 2 results table and an editable
SF1/SF10 comparison chart, and updates the scope, research answers and next steps.
The talk script, one-page summary and defense Q&A include the new findings. Demo video files
and the previously generated core report artifacts are unchanged.

To build it with the same bundled runtime, pass `--include-sf10` and a new output filename:

```text
build_presentation.mjs --include-sf10 --output deliverables/presentation/lakehouse-comet-research-sf10-20260924.pptx
```

The builder checks the copied SF10 evidence hashes in `docs/benchmarks/sf10/evidence-index.json`
and the round 2 final verification receipt. See [the integrated report](../../docs/research-report.md)
for the two-session disclosure and the limits of the descriptive SF1/SF10 comparison.
The 1.508x geometric mean still refers only to the original 10-workload core matrix.

This combined deck includes an exploratory supplement. Its bundled layout/package validation
is separate from the existing 13-slide core-release sidecar workflow. Do not replace the core
release manifest with this deck or treat the supplement as an automatic extension of the core
publication gate. The 13-slide default build and existing release finalizer remain available.
