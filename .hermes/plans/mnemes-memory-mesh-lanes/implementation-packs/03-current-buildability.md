# Current Buildability Probe — 2026-07-30

> **Evidence class:** environment/buildability probe only. These commands ran in dirty canonical planning checkouts and therefore cannot certify any future worker output.

| Worktree | Command | Result | Notes |
|---|---|---|---|
| `/home/sikmindz/Coding/mnemes-replication` | `cargo test -p mnemes --test device_shards --no-run` | PASS, 33.16s | Build-directory lock was contended, then the test executable was produced. |
| `/home/sikmindz/Coding/Libraries` | `cargo test -p semantic-memory --test chunk_manifest_ingest --no-run` | PASS, 0.87s | Cargo emitted existing non-root package profile warning for `quant-governor`. |
| `/home/sikmindz/Coding/Libraries` | `cargo test -p claim-ledger --no-run` | PASS, 9.36s | Cargo emitted the same existing profile warning and built listed unit/integration targets. |

## Use by workers

These are candidate first compile gates after an isolated worktree is created. A worker/controller must rerun the relevant command on the final worktree generation and record the result in its receipt. Lock contention is infrastructure telemetry, not a test failure; concurrent source modification is a reason to invalidate a test receipt.
