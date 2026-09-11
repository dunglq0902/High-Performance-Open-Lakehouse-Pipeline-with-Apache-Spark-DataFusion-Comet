# Research evidence archive and rotation

Fresh research plans are immutable and use the same ten experiment paths as the preceding suite.
Archive the preceding evidence before preparing a suite at a new commit. The archive operation is
deliberately separate from `make benchmark`; a benchmark never moves older evidence implicitly.

## Safety contract

`scripts/archive_research_evidence.py` loads the ten experiment IDs from the canonical core-suite
configuration and fails closed unless both `results/raw` and `.artifacts/campaigns` contain exactly
those directories. A root-level `.gitkeep` is allowed and is never moved. Unexpected, missing,
case-colliding, symbolic-link, or non-regular entries are rejected.

The caller must provide the exact 40-character commit recorded in the old raw records and a new,
lowercase archive label. Every raw JSON record and every campaign experiment manifest must bind to
that commit. The experiment manifest's own canonical hash is checked before any move.

The default command is a read-only dry run. It hashes every regular file and records every
directory, including empty `failed-attempts` directories. Review its destination, counts, byte
total, and `inventory_sha256` before executing:

```bash
make archive-research-evidence-dry-run \
  EVIDENCE_SOURCE_COMMIT=0123456789abcdef0123456789abcdef01234567 \
  EVIDENCE_ARCHIVE_LABEL=before-final-rerun

make archive-research-evidence \
  EVIDENCE_SOURCE_COMMIT=0123456789abcdef0123456789abcdef01234567 \
  EVIDENCE_ARCHIVE_LABEL=before-final-rerun
```

Replace the example commit with the value actually present in the old raw records. Never use the
new HEAD merely because it is current.

## Transaction and verification

Execution re-hashes the complete source snapshot, then atomically renames only the twenty exact
experiment directories into a staging directory on the same filesystem. The raw and campaign root
directories remain in place, and `results/raw/.gitkeep` remains untouched. The staged payload is
re-hashed before publication. The completed location is:

```text
.artifacts/campaign-archives/<source-commit>/<label>/
├── archive-manifest.json
├── raw/<ten exact experiment directories>/
└── campaigns/<ten exact experiment directories>/
```

`archive-manifest.json` contains the original roots, ordered experiment IDs, each directory, each
file's byte size and SHA-256, aggregate counts and bytes, a full inventory SHA-256, and a canonical
manifest self-hash. The script performs another complete read-back verification after the staging
directory is published. An existing destination is never merged with or overwritten.

Normal errors trigger an automatic reverse-order rollback. Before the first move, the script also
writes a self-hashed transaction journal. If the operating system or process stops abruptly and a
hidden `.staging` directory remains, restore the already moved directories with:

```bash
make archive-research-evidence-rollback \
  EVIDENCE_SOURCE_COMMIT=0123456789abcdef0123456789abcdef01234567 \
  EVIDENCE_ARCHIVE_LABEL=before-final-rerun
```

Rollback refuses ambiguous states where both the original and staged copy exist, either copy is
missing, the journal was changed, or the requested experiment set differs. A completed archive is
not treated as an interrupted transaction.

## Restore boundary

Keep a completed archive immutable. To restore it, first verify the manifest with the script's
`verify_archive()` entry point, then copy or move only its listed experiment directories into empty
original roots. Never merge a historical experiment directory into current evidence. Raw records
retain their original artifact paths, so restoring `raw` and `campaigns` together recreates those
path relationships; commit-scoped shared controls remain under their existing `.artifacts`
locations.
