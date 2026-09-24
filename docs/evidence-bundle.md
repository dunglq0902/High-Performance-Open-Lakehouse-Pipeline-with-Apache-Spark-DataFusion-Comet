# Final evidence bundle

The final release is a self-contained ZIP64 package for review and offline report reconstruction.
Packaging is deliberately fail-closed: a dirty repository, an unpublishable report, a diagnostic
slide/video sidecar, a stale hash, a missing control artifact, or an incomplete dataset blocks the
archive before any final file is published.

## Required inputs

- A clean final Git commit and the fresh ten-campaign evidence for that exact commit.
- `results/reports/report-publishability.json` with `publishable: true` and a passed report-content
  contract.
- The exact report directory described by `report-artifact-inventory.json`.
- A final 13-slide core PPTX plus its `publishable` presentation manifest.
- A reviewed Spark History Server MP4 plus its `publishable` video manifest and paired demo bundle.
- Both complete primary dataset roots and current/origin validation attestations.

The 15-slide SF10 presentation is a separate exploratory supplement. Its
verification and the historical SF10 reports do not replace these core inputs.
The existing release ZIP predates SF10; updating code, docs and slides does not
rebuild that archive. See [SF10 reproduction](sf10-benchmark.md) for the separate
report command and required evidence trees.

The full payload is expected to be roughly 2 GiB before the final video. The dataset roots are not
optional: excluding them would prevent offline semantic verification and report reconstruction.

## Build and verify

With the final media already present at their default paths and both rendered artifacts reviewed,
run:

```bash
make release \
  PRESENTATION_VISUAL_REVIEW_ATTESTATION=1 \
  VIDEO_VISUAL_REVIEW_ATTESTATION=1
```

This reruns the strict report gate, finalizes both media sidecars, builds
`deliverables/release/lakehouse-comet-evidence.zip`, writes the independent
`.zip.sha256` sidecar, and performs a complete read-back verification. Existing archive or checksum
files are never overwritten.

To use different names:

```bash
make release \
  PRESENTATION_VISUAL_REVIEW_ATTESTATION=1 \
  VIDEO_VISUAL_REVIEW_ATTESTATION=1 \
  PRESENTATION=deliverables/presentation/final-defense.pptx \
  DEMO_VIDEO=deliverables/video/final-spark-ui.mp4 \
  RELEASE_BUNDLE=deliverables/release/final-evidence.zip
```

If no trusted `ffprobe` is available, add `VIDEO_FULL_PLAYBACK_ATTESTATION=1` only after watching
the exported MP4 from beginning to end without a playback/decode error. Omitting either visual-
review variable makes the aggregate release fail closed; `make release` does not auto-attest.

## Package contract

The archive contains:

- `repository.bundle`: all reachable Git refs/history required to validate attestation ancestry;
- `repository/results/raw` and the exact generated report directory;
- all ten campaign trees and every shared control target declared by their latest verification;
- current and origin dataset attestations plus both full primary dataset roots;
- final presentation/video files, their sidecars, and the Spark UI demo event-log bundle;
- `RESTORE.md`, `RELEASE-MANIFEST.json`, and `SHA256SUMS`.

Immediately before collection, the builder reconstructs every declared campaign-control
fingerprint and requires it to equal the value admitted by the report. A control file changed after
report generation therefore blocks release instead of being silently packaged.

The media closure is schema-v2 only. The presentation sidecar must bind the exact report inventory
and carry the explicit rendered-slide visual-review attestation. The demo and video sidecars must
bind the same report publishability artifact and the same baseline/Comet event logs. Each video
application carries exactly the measured SQL execution description and duration from its demo
application; these values, its raw-record hash, SQL execution ID, URLs, and event-log inventories
are revalidated rather than trusted as labels.

`RELEASE-MANIFEST.json` records the 40-character commit/tree IDs, report and media bindings,
campaign/attempt totals, dataset identities, a sorted size/SHA-256 file inventory, and the exact
empty control directories that prove a campaign has no failed-attempt record. Empty directories are
manifest entries rather than unsafe ZIP directory members; the verified extractor recreates them.
The manifest has a canonical self-hash. The verifier rejects path traversal, symbolic links,
encryption, duplicate or case-colliding names, empty-directory/file conflicts, unexpected/missing
entries, nested report-inventory drift, media/report/commit mismatches, and any byte-hash mismatch.
It also reconstructs every accepted raw campaign and its `raw_records_sha256`, then reconstructs
run IDs, contiguous attempt sequences, failed-attempt counts, and control-artifact fingerprints
from the archived bytes. Consequently, editing evidence and merely recomputing the outer inventory,
manifest self-hash, and `SHA256SUMS` still fails the semantic cross-binding checks.
Candidate textual evidence is also scanned for current local credential values before packaging.

The checksum establishes integrity, not third-party authenticity. Transfer the outer `.sha256`
through an independently trusted channel if authenticity is required.

## Restore

The archive's `RESTORE.md` is authoritative. In summary, verify and extract only into a new
directory, clone `repository.bundle`, check out the recorded commit, overlay the extracted
`repository/` tree, install the locked runtime, and rerun `make report`. Ignored evidence/data/media
should not dirty the checkout, and strict publication must resolve to the same commit.
