# Partial research incident archive

Use this workflow only when a core research suite hard-stops before all ten campaigns finish and
the immutable failure history correctly prevents a normal resume. It preserves the interrupted
state before the canonical evidence roots are reused by a fresh suite.

This is **diagnostic evidence only**. A partial incident archive can never replace a completed
research archive, satisfy report publishability, or appear as benchmark evidence in a release.
Completed suites must use [`research-evidence-archive.md`](research-evidence-archive.md).

## Safety contract

`scripts/archive_partial_research_incident.py` is separate from the completed-suite archiver and
does not relax any of its gates. The partial workflow:

- resolves the exact ten experiment IDs from the reviewed core-suite configuration;
- rejects unexpected files or directories at the two canonical source roots;
- verifies every present experiment manifest, its self-hash, and its source-commit binding;
- verifies every present raw JSON record and its source-commit binding;
- classifies a campaign as `verified` only when the latest contiguous verification artifact is a
  passed completion bound to that experiment manifest and its exact raw-record count;
- records `missing`, `empty`, `records`, `plan-only`, `started`, or `verified` state for every
  experiment;
- hashes the state matrix as well as every present file and empty directory;
- refuses a suite whose ten campaigns are already verified, keeping the completed archiver as the
  only valid path for complete evidence;
- pins the filesystem identity of both source roots, every present experiment root, and the
  transaction scaffold in a self-hashed v2 journal;
- moves only present experiment roots through a journaled same-filesystem transaction, using
  atomic no-replace renames on Linux and Windows (including the Windows host fallback required by
  WSL DrvFS);
- re-hashes the staged payload before publication and supports automatic or explicit rollback;
- verifies a restored payload completely before removing its recovery journal;
- marks the resulting manifest `diagnostic-only` and `publication_eligible: false`.

The archive destination is:

```text
.artifacts/research-incident-archives/<source-commit>/<label>/
├── incident-manifest.json
├── raw/
└── campaigns/
```

Missing experiment roots stay absent. Their absence is explicit in `suite_state` and bound by
`suite_state_sha256` plus the manifest self-hash.

## Archive an interrupted suite

First stop any surviving one-off benchmark container and recover its native Spark event-log staging
with the campaign recovery helper. Do not delete, edit, or reclassify the terminal failure JSON.
Ensure no campaign process is still writing to `results/raw` or `.artifacts/campaigns`.

Run the read-only plan and review the destination, per-experiment state, counts, and hashes:

```bash
make archive-partial-research-incident-dry-run \
  EVIDENCE_SOURCE_COMMIT=0123456789abcdef0123456789abcdef01234567 \
  EVIDENCE_ARCHIVE_LABEL=docker-desktop-pause-q06-20260912
```

Then execute the exact same identity:

```bash
make archive-partial-research-incident \
  EVIDENCE_SOURCE_COMMIT=0123456789abcdef0123456789abcdef01234567 \
  EVIDENCE_ARCHIVE_LABEL=docker-desktop-pause-q06-20260912
```

The command leaves the parent source roots and `results/raw/.gitkeep` in place, but moves every
present core experiment directory. Verify the completed archive independently with:

```bash
uv run python -c \
  'from pathlib import Path; from scripts.archive_partial_research_incident import verify_incident_archive; print(verify_incident_archive(Path(".artifacts/research-incident-archives/0123456789abcdef0123456789abcdef01234567/docker-desktop-pause-q06-20260912"))["manifest_sha256"])'
```

After verification, start the replacement suite only from a clean committed worktree. Its raw
records, plans, capacity gates, image, and shared runtime will bind to that new commit.

## Recover an interrupted archive transaction

Before any evidence move, the tool removes an unjournaled scaffold only when its pinned identities
and exact empty shape still match. If that cleanup is refused or remains incomplete, no evidence
has moved; preserve the reported staging path for manual integrity review. After the journal is
established, ordinary errors trigger automatic rollback. If a journaled rollback is interrupted or
the process or host stops mid-rename, do not move or merge directories manually. Use the exact
original identity:

```bash
make archive-partial-research-incident-rollback \
  EVIDENCE_SOURCE_COMMIT=0123456789abcdef0123456789abcdef01234567 \
  EVIDENCE_ARCHIVE_LABEL=docker-desktop-pause-q06-20260912
```

Rollback accepts exactly one self-hashed journal in either the staging or just-published
destination. It requires every planned root to exist in exactly one location, restores moves in
reverse order, and verifies the restored state against the journal.
