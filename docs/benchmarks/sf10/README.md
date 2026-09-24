# SF10 evidence for documentation and slides

These files are byte-identical copies of the completed SF10 round 1 and round 2 reports and verification receipts. `evidence-index.json` records each original relative path, size and SHA-256, together with the source worktree. The copies let the documentation and presentation resolve their SF10 sources without depending on an absolute worktree path.

The raw campaigns, dataset, resource samples, calibration files and incident archives remain in that source worktree. Paths embedded inside the copied JSON files refer to the original workspace. This folder is a documentation snapshot, not a full evidence bundle and not a new core publication-gate receipt.

Use [the integrated research report](../../research-report.md) for the current conclusions. Round 2 has 10 pairs per query and is the principal SF10 result. Round 1 has 5 pairs per query and is retained for independent descriptive comparison. Do not pool these measurements or add them to the 10-workload core geometric mean.

The presentation builder's `--include-sf10` option checks these file hashes and the final verification receipt before using the round 2 summary. Rebuilding the combined deck does not rewrite the original generated core reports or change their publication status.
