# Spark Web UI comparison demo

This runbook produces the mandatory visual comparison between a Spark baseline application and
its paired Spark + DataFusion Comet application. The History Server replays the **actual completed
event logs**; it does not simulate a query or substitute report charts for the Spark UI.

## Publication boundary

The final workflow is fail-closed. `make demo-prepare` accepts a pair only when the latest
`results/reports/report-publishability.json` says both the evidence gate and report-content gate
passed. It then verifies that the current raw campaign hash still equals the one recorded in that
report, recomputes the campaign artifact and control-artifact fingerprints recorded by the report,
and verifies that the two records share the same experiment, pair, input, SQL, snapshots, and
result. Each rolling event log must contain a completed application and exactly one SQL start event
whose description is `measured terminal action for <run_id>`, plus its matching successful SQL end
event. The event-derived duration must equal the raw record's `sql_execution_time_ms`.

Before the final fresh campaign, only this explicitly labelled rehearsal is permitted:

```bash
make demo-ui-diagnostic DEMO_EXPERIMENT=EXP-TPCH-SF1-Q01 DEMO_PAIR=1
```

The resulting manifest says `diagnostic` and `DIAGNOSTIC REHEARSAL ONLY`. It must not be submitted
as the final video. After the final clean-HEAD campaign and strict report pass, use:

```bash
make demo-ui DEMO_EXPERIMENT=EXP-TPCH-SF1-Q01 DEMO_PAIR=1
```

The preparation step copies, but never alters, the two source event-log directories. Each immutable
bundle is stored below `.artifacts/demo/spark-ui/bundles/`; `current.json` and `current.env` point
Compose at the selected bundle. The History Server mounts `.artifacts` read-only and is exposed only
on `http://127.0.0.1:18080`. A reused immutable bundle is accepted only after every staged event-log
file is rehashed and its complete inventory still equals the manifest; any drift blocks activation.

The `demo-ui` targets wait for the service health check and then run an independent readiness gate.
That gate requires the REST API to expose exactly the two declared, completed applications; checks
their application lifetimes and SQL counts; verifies the measured execution ID, description,
duration, completion status, and empty failed-job list; and probes both the SQL index and direct
execution pages. It prints `"status": "ready"` only after all checks pass. Re-run the same check with
`make demo-ui-verify` (or `make demo-ui-verify-diagnostic` for a rehearsal).

## Recording sequence

Use a 1920x1080 browser window and record at least 30 seconds. Windows Snipping Tool screen
recording or Xbox Game Bar is sufficient; avoid notifications and unrelated windows.

1. Open `http://127.0.0.1:18080` and show that exactly the selected baseline and Comet application
   IDs are available. The exact IDs and direct SQL-page URLs are in the bundle's
   `demo-manifest.json`. Each application records `measured_sql_execution_id` and
   `measured_sql_execution_url`; the same ordered URLs are also available at
   `history_server.measured_execution_urls`.
2. Open the baseline application. On **Environment**, show the Comet-related Spark properties; on
   **SQL**, open the measured execution and show its duration and physical-plan section. Briefly
   show Jobs/Stages so this is visibly a real Spark application.
3. Open the paired Comet application and repeat the same path. Keep the query, pair index, dataset,
   and result identity visible in the narration or title card. Point out native `Comet*` plan nodes
   and any real fallback/transition nodes; never infer “no fallback” from an empty reason list.
4. End with a short side-by-side summary using only values in the same `demo-manifest.json` and the
   strict report. State that TPC-H data is derived/non-audited and that SF10 sensitivity is not
   estimable when SF10 evidence is absent.

Q01 is the default because it is a clear native-acceleration example. M08 may be recorded as an
additional diagnostic segment to explain partial native coverage, but it must retain its observed
fallback wording.

## Finalize and bind the video

Save the reviewed recording as:

`deliverables/video/spark-ui-comparison.mp4`

After watching the exported file and confirming both application IDs, both SQL pages, both plans,
and the publication/diagnostic label, run the automated finalizer:

```bash
make demo-video-finalize
```

When `ffprobe` is available, this scans the complete video stream as well as checking the MP4
container, duration, and minimum 1280x720 resolution, then writes
`deliverables/video/spark-ui-comparison.manifest.json`. The sidecar binds the video SHA-256 to the
selected raw records, application IDs, source event logs, report gate, query, pair, and Git commit.
The hash is an integrity record, not an independent authenticity certificate.

If `ffprobe` is unavailable, first play the exported file from beginning to end in a native player,
then use `make demo-video-finalize VIDEO_FULL_PLAYBACK_ATTESTATION=1` to record that explicit
attestation. A structural MP4 check by itself can only produce a diagnostic artifact. See
[`demo-video.md`](demo-video.md) for the full acceptance contract.

For a rehearsal video only, use `make demo-video-finalize-diagnostic`; that target deliberately
omits final-review confirmation, so its sidecar remains `diagnostic`. Stop only the History Server
afterward with:

```bash
make demo-ui-down
```
