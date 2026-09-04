# Core plan regression corpus

These are the exact final physical plans from the 10 core queries run on both
Spark baseline and Comet in `plan-preflight-20260904-01`. They cover the locked
Spark 4.1.3, Comet 1.0.0, and Iceberg 1.11.0 runtime.

This is **non-publishable diagnostic evidence**, not a benchmark campaign. Each
engine reused one diagnostic session, and there were no benchmark repetitions.
The paired schema hashes, canonical result hashes, and row counts record only
what that diagnostic observed. They establish neither primary correctness
admission nor performance claims and must never populate `results/raw`.

`contract.json` binds all 20 plan files to their original byte hashes, reviewed
semantic hashes, operator sequences, and complete parser analyses. It also pins
the checked-in runtime lock, configurations, workload manifests, and SQL files.
The captured session parser identity is kept separately from the earlier launch
notice identity and the parser used to review the corpus. Tests pin behavior,
not the whole current parser file, so a non-semantic refactor needs no fixture
refresh.

Dataset manifest and source diagnostic file hashes are provenance declarations,
not a substitute for dataset validation or an assertion that the original
diagnostic artifacts are installed. The regression tests need neither Docker
nor generated datasets. They cannot replay result hashes without data.

Any deliberate input, runtime, or parser-behavior change requires review of the
affected fixture contract; do not replace it with synthetic plans or silently
promote unknown operators. The separate M04 failure fixture remains available
for testing the original admission failure.
