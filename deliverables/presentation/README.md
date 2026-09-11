# Research presentation output

`build_presentation.mjs` generates the 12-slide, 16:9 research deck from the admitted files in
`results/reports`. It uses editable native charts/tables and runs the bundled presentation
integrity/layout finalizer. Because that builder depends on the Codex presentation artifact runtime,
invoke it from a Codex session with the presentation skill/runtime paths loaded.

The final clean-campaign output name is:

`lakehouse-comet-research.pptx`

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
