# Demo video finalization

This step does not create or edit a recording. It validates the exported MP4 and writes the
immutable provenance sidecar consumed by the release bundle.

## Required source evidence

The finalizer accepts only Spark UI demo manifest schema v2. Before inspecting the video, it
revalidates all of the following against live repository files:

- the exact `results/reports/report-publishability.json` path, bytes, SHA-256, report-content
  contract, and publication status;
- the canonical raw-record path for each baseline/Comet measurement and its SHA-256;
- experiment, pair, engine, query, workload, storage, Git commit, input, SQL, snapshot, and
  correctness identity in both raw records;
- the measured SQL execution ID, description, start/end timestamps, duration, query wall time,
  Spark configuration, and plan counters copied from each raw record;
- canonical source event-log roots under `.artifacts/campaigns/` and canonical staged roots under
  the immutable demo bundle, including a full size/SHA-256 inventory of every file; and
- the exact loopback History Server URL and direct measured-execution URLs under
  `http://127.0.0.1:18080`.

Any source drift blocks finalization. The same sources are checked again before the sidecar is
written to close the validation/write window.

## Publication modes

First watch the exported recording and confirm that it shows both declared application IDs, both
measured SQL pages, the relevant physical plans, and the publication/diagnostic label. Then use
the preferred automated path:

```bash
make demo-video-finalize VIDEO_VISUAL_REVIEW_ATTESTATION=1
```

When `ffprobe` is on `PATH`, the finalizer scans the complete video stream, rejects reported decode
errors, requires one H.264/AVC video stream in the MP4, and checks dimensions, duration, frame
rate, and the decoded frame count. It also performs a separate ISO-BMFF container check. A
successful container check alone does **not** establish that frames can be decoded.

If `ffprobe` is unavailable, play the exported MP4 from beginning to end in a native player. Only
after that full playback succeeds, record the explicit reviewer attestation:

```bash
make demo-video-finalize \
  VIDEO_VISUAL_REVIEW_ATTESTATION=1 \
  VIDEO_FULL_PLAYBACK_ATTESTATION=1
```

This is a human attestation, not automated codec validation. It is separate from the required
visual-content review. Without either a complete automated frame scan or this explicit full-
playback attestation, a synthetic/structural MP4 can never receive `publishable` status.
The Make target never adds either human attestation unless its corresponding variable is set.

For rehearsals, use:

```bash
make demo-video-finalize-diagnostic
```

That target intentionally omits the visual-review confirmation, so it always produces a
`diagnostic` sidecar. Structural inspection may still run, but it cannot upgrade the artifact to a
release video.

## Output contract

The sidecar is written next to the MP4 as `*.manifest.json`. It binds the video hash and measured
metadata to the live report, schema-v2 demo manifest, both raw records, both application IDs, and
both source/staged event-log inventories. The `playback_validation` block states whether evidence
came from an automated frame scan, an explicit full-playback attestation, or a diagnostic-only
container inspection.

SHA-256 provides change detection and provenance binding; it is not an independent authenticity
certificate.
