# Lane 3 — Claim Ledger and Evidence Component

> **For Hermes:** This lane owns the non-SQLite canonical claim/evidence component. It must not edit semantic-memory core or Mnemes server/control-store files.

**Goal:** Replicate claim-ledger truth and immutable evidence objects with the same crash safety, ordering, bootstrap, compaction, and anti-resurrection guarantees as semantic mutations.

**Architecture:** Evidence objects are content-addressed immutable files. Write/rename/fsync the object before appending the ledger event that references it. The claim stream has its own ordered head and checkpoint/compaction semantics; ordinary semantic search remains available when trust enrichment is stale or unavailable.

**Files owned:**
- `/home/sikmindz/Coding/Libraries/claim-ledger/src/**` and its tests
- `/home/sikmindz/Coding/Libraries/semantic-memory-mcp/src/**` claim/evidence bridge/server paths
- `mnemes-replication/src/replication/claim_component.rs` and dedicated component tests only

**Do not edit:** `semantic-memory/src/**`, Mnemes `replication/mod.rs`, `server.rs`, `store.rs`, `shards.rs`.

## Task 3.1 — Establish one claim/evidence owner

1. Inventory all MCP claim/evidence write paths.
2. Introduce `ClaimComponentStore` behind the semantic-memory-MCP bridge so tools cannot write JSONL/evidence files directly.
3. Define exactly which existing claim ledger append receipt/head/checkpoint APIs are used; do not invent a parallel ledger.
4. Add component status: ledger identity, verified head/sequence, compaction checkpoint, evidence object root, and outstanding blobs.

**RED:** direct tool/file path bypass is rejected in replicated mode.

**GREEN:** one claim or evidence write passes only through the component store and returns an immutable receipt.

## Task 3.2 — Make evidence objects crash-safe

1. Define digest-addressed object layout under a managed evidence directory; forbid caller-selected paths.
2. Write to a same-filesystem temporary name, fsync data and directory, atomically rename, then append the `EvidenceAttached` event carrying object digest/size/media metadata.
3. On startup, identify safe unreferenced temporary/orphan objects without deleting referenced content.
4. Make event apply verify every referenced object exists and matches digest before advancing claim watermark.

**Fault tests:** crash after temp write; after file fsync; after rename; after ledger append; during receiver object transfer; duplicate object/event; corrupted object.

**Acceptance:** state is either an unreferenced removable object or a verified event with complete object—never a verified reference to missing/mismatched bytes.

## Task 3.3 — Export, apply, and compact the ledger component

1. Implement verified contiguous export by existing ledger sequence/head, including exact event bytes/digests and referenced object manifest.
2. Implement receiver apply that verifies predecessor/head, exact duplicate, gap, fork, and trusted ledger state before logical projection changes.
3. Implement checkpoint/snapshot export for receivers below retained tail; preserve stable ledger ordering through compaction.
4. Verify compaction cannot discard state needed by an admitted receiver without a snapshot/checkpoint recovery path.

**RED:** behind receiver after compaction, changed sequence digest, wrong previous head, and malformed ledger snapshot all fail closed.

**GREEN:** bootstrap from checkpoint plus tail reaches the same verified head and projection as source.

## Task 3.4 — Provide Mnemes component adapter without shared-file edits

1. Create `claim_component.rs` implementing the Lane 0 V2 component trait/DTO mapping and component-specific validation tests.
2. Do not self-register it in Mnemes `mod.rs`/server/store; provide a one-file integration checklist to Lane 2.
3. Define component bootstrap page/object transfer requirements and status fields consumed by Lane 2.
4. Coordinate one reviewed integration commit owned by Lane 2 after its generic receiver interface is ready.

## Task 3.5 — Truth-aware degradation behavior

1. Add clear `trust_current`, `trust_lagging`, `trust_unavailable`, and `trust_quarantined` result metadata at the semantic-memory-MCP boundary.
2. Prove semantic content search still works if claim verification/component sync fails.
3. Prove trust-enhanced assertions/actions fail closed when required claim head or evidence object is unavailable.

**Gate 4 commands:**
```bash
cargo test -p claim-ledger
cargo test -p semantic-memory-mcp claim -- --nocapture
cargo test -p semantic-memory-mcp evidence -- --nocapture
cargo fmt --check
```

**Gate 4 acceptance:** source/receiver claim heads and trust projections match; all referenced evidence objects validate; compaction/restart/behind-receiver recovery passes; semantic recall remains available while trust states remain explicit.

## Handoff and rollback

Publish component protocol fixture, object manifest format, verified head samples, crash matrix, and Lane 2 registration checklist. Rollback disables only the claim component stream, retains immutable objects and ledger/checkpoint evidence, and makes trust state degraded—not silently current.
