# Lane 1 — Semantic-Memory Canonical Owner

> **For Hermes:** Work only in the Libraries implementation worktree. Consume Gate 1 types; do not modify Mnemes runtime files.

**Goal:** Make every shareable semantic-memory mutation produce exactly one replayable V2 operation atomically, and expose owner-controlled apply, snapshot, root, and status APIs.

**Architecture:** Extend the existing `operation_journal`; do not create a second outbox. A `ReplicationTxn` carries either `LocalAuthoring`, `ReplicaApply`, `BootstrapApply`, or `MigrationPermit`. Guarded canonical writes fail closed without that context. Derived indexes remain rebuildable; carried embeddings are optional artifacts, never semantic truth.

**Files owned:** `/home/sikmindz/Coding/Libraries/semantic-memory/src/**` and semantic-memory replication tests only.

**Prerequisite:** Gate 1 V2 schema, operation families, and canonical preimage are frozen.

## Task 1.1 — Add owner-level replication scaffold

1. Create `src/replication/{mod.rs,operation.rs,guard.rs,snapshot.rs,state_digest.rs,artifact.rs}`.
2. Extend `src/journal.rs`; retain the existing fact-create V1 compatibility APIs/tests.
3. Add V40+ migration in `src/db.rs` for operation family/schema/dependency/source-commit metadata, canonical content generation, and durable apply/inbox state.
4. Define a single `ReplicationTxn` construction API; private raw write helpers must not become a second mutation path.

**RED:** a replicated-mode naked canonical table write fails deterministically.

**GREEN:** `ReplicationTxn::local_authoring` can atomically change one fact and append one V2 operation.

## Task 1.2 — Make source/replay transaction boundaries proveable

1. Implement source export: `export_verified_contiguous(stream, start, limit)`.
2. Implement replay: `apply_verified_operation(envelope)` with duplicate/gap/fork decisions persisted in the semantic database transaction.
3. Inject failure before mutation, after mutation before journal, after journal before commit, and after commit before return.
4. Prove source rows and journal never diverge; prove duplicate exact input has no second semantic effect.

**Run:** focused journal/replication tests plus reopened-store tests.

## Task 1.3 — Cover core content aggregates

Refactor these owners one at a time, always RED → GREEN → regression:

- `knowledge.rs`: fact create/update/supersede/tombstone;
- `documents.rs`: atomic document + ordered chunks ingest/tombstone;
- `conversation.rs`: session create/rename/message append/tombstone;
- `episodes.rs`: episode version/outcome and complete ordered cause-set replacement;
- `graph_edges.rs`: add/invalidate.

Each aggregate payload contains stable IDs, scope, temporal/lineage fields, canonical text/content digest, and tombstone/supersession semantics. Never transport physical deletes as ordinary replay operations.

**Acceptance per owner:** source aggregate → exact export → empty receiver apply → family root equal; exact duplicate is no-op; changed sequence payload is fork; unknown dependency fails with no mutation.

## Task 1.4 — Cover projections, provenance, and governance composites

1. Refactor `projection_lane.rs`, `projection_storage.rs`, and projection import paths into atomic import/failure operations rather than unrelated row payloads.
2. Refactor `provenance.rs` append/revise ownership.
3. Refactor authority, origin/transition contracts as one governed mutation/receipt aggregate.
4. Model selective forgetting and namespace scope as a signed `forgetting.closure` with deterministic affected-ID manifest, not a physical delete loop.
5. Refactor procedural-memory artifacts/events/receipts and shadow-policy proposal/promotion/rollback aggregates.

**RED:** each composite’s injected failure leaves no partial projection, authority pointer, forgetting closure, or procedure receipt.

**GREEN:** each produces one typed operation and replay reaches same root.

## Task 1.5 — Snapshot and anti-entropy closure

1. Implement deterministic logical `SnapshotRecordV1` families in FK-safe dependency order.
2. Add `export_snapshot_pages(cut, options)`, `apply_snapshot_page(page, permit)`, `canonical_family_roots()`, and `replication_status()`.
3. Exclude FTS/maps, ANN sidecars, sparse/q8/compressed pools, caches, and search receipts from canonical roots.
4. Support optional derived artifact manifests only when source root/profile/digest validate.
5. Test concurrent source writes: snapshot cut followed by live tail must converge without missing/dependent records.

**Gate 3 commands:**
```bash
cargo test -p semantic-memory replication -- --nocapture
cargo test -p semantic-memory snapshot -- --nocapture
cargo test -p semantic-memory forgetting -- --nocapture
cargo fmt --check
```

**Gate 3 acceptance:** every Tier-A mutation family is covered; all guarded canonical writes require a permit; fresh receiver roots equal after live replay, snapshot+tail, restart, duplicate, gap, fork, and offline catch-up.

## Handoff

Publish an API compatibility note, test fixture store, operation coverage matrix, root samples, and source artifact-profile contract to Lanes 2, 4, and 5.

**Rollback:** replication enforcement stays feature/config-gated. Disable the gate and retain journal evidence; do not delete journals or tombstones.
