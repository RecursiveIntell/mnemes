# Dispatch Order and Model-Tier Policy

> **Purpose:** Spend high-reasoning capacity where ambiguity or authority is real; use lower-cost models only for bounded, test-pinned implementation tasks.

## Do not delegate these to a cheap model as primary author

| Area | Why | Required owner |
|---|---|---|
| Gate 1 operation vocabulary, signed preimage, schema evolution, independent golden vectors | A one-byte or ordering mistake breaks all future compatibility; type-checking cannot prove interoperability. | Controller / strongest model plus independent review |
| Semantic `ReplicationTxn` guard and atomic mutation+journal/apply decision | This defines canonical truth and crash behavior; a plausible adapter can silently create a second write lane. | Controller / strongest model |
| Mnemes trusted-key admission, scope/device/fence check order, duplicate/fork decision, durable ACK boundary | Security and idempotency failures may look successful in happy-path tests. | Controller / strongest model |
| Bootstrap paired-generation promotion and compaction/recovery retention | Mistakes can lose replica evidence or make stale state appear active. | Controller / strongest model |
| Any cross-lane integration in `server.rs`, `store.rs`, protocol module registration, public API promotion, or production configuration | These are collision points and must reconcile actual completed work. | Controller only |
| Canary, device/server deployment, key provisioning/revocation, rollback | External and stateful side effects. | Controller only |

## Good bounded tasks for a cheaper model — only after the prerequisite gate

| Dispatch ID | Prerequisite | Task boundary | Expected files | Required test shape | Controller review focus |
|---|---|---|---|---|---|
| L1-Facts | Gate 1 + owner scaffold | Add one named fact mutation family to the pre-existing guarded operation seam | one mutation owner + one dedicated test | source → export → apply → duplicate → reopen | no direct DB bypass; exact operation payload |
| L1-Documents | Gate 1 + owner scaffold | Add atomic document/chunk aggregate family | `documents` owner + test fixture | valid ingest, duplicate external chunk ID, injected failure no partial rows | chunk ordering and atomicity |
| L1-Conversations | Gate 1 + owner scaffold | Add session/message aggregate family | conversation owner + test | append/replay/reopen/tombstone | stable sequence and session scope |
| L1-Episodes/edges | Gate 1 + owner scaffold | Add one tightly bounded aggregate at a time | owner + one test | change/replay/no duplicate effect | complete replacement semantics |
| L1-Projection fixture | Gate 1 + owner scaffold | Build malformed/duplicate projection import fixtures, no core refactor | test-only files | RED cases named in pack | fixtures actually hit public path |
| L2-HTTP test harness | Gate 1 + controller-defined handler shape | Add integration tests around a frozen route and existing test helper | test file only | valid/duplicate/tamper/no durable mutation | no route implementation edits |
| L2-Sender spool tests | controller-defined spool trait | Add restart/byte-identity tests using fixture transport | tests + fixture transport only | crash point matrix | raw bytes and no regenerated payload |
| L3-Evidence crash tests | component object API frozen | Add temp/fsync/rename failure tests | component tests only | each crash state/readback | no direct storage write bypass |
| L4-Freshness pure functions | status DTO frozen | Implement/classify pure freshness policy and unit tests | `search_freshness.rs` + unit tests | all status matrix rows | no network or store edits |
| L4-Router fault tests | read-only status interface frozen | Add stale/timeout/conflict/incomplete behavior tests | test files only | per-shard isolation | empty result never authoritative |
| L5-Disposable harness | runtime config schema frozen | Build an isolated test topology/harness without live endpoints | scripts/tests/docs only | manifest generation + teardown | cannot target real paths |

## Task sizing rule

A cheap-model task must be executable in one worktree with:

- one behavioral statement;
- at most three production source files plus dedicated tests;
- fixed public types and exact error cases named in the pack;
- no new dependency, feature flag, database migration, route registration, or public schema unless explicitly controller-approved;
- one RED test and at least one negative durable-state assertion;
- a 20–45 minute budget. If it exceeds that, split it or retain controller ownership.

## Controller checkpoints

1. **Before dispatch:** fill the source-anchor pack, run worker preflight, pin head/allowlist, and verify the test can reach the intended behavior.
2. **After each worker:** run final guard, reread diff, independently rerun focused gates, and record the result in the finding/acceptance ledger.
3. **After a family of workers:** controller integrates in a clean integration worktree, fixes mechanical seams only, reruns all affected tests, and publishes a new handoff digest.
4. **On any contract drift:** stop dependent workers; do not “adapt” one worker’s output into a hidden compatibility layer.

## Honest capacity statement

This policy makes lower-cost implementation viable for bounded coding and test tasks. It does not make the protocol, transaction, admission, or operational guarantees safe to delegate blindly. Those remain reasoning-heavy controller gates.
