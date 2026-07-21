# Device-Owned Replication Implementation Plan

> **For Hermes:** Use this document as the execution contract. Implement sequentially with strict RED/GREEN tests; do not claim continuous replication until the admission gates pass.

**Goal:** Implement a device-owned semantic-memory primary with authenticated, replayable server replicas and authorization-aware sparse retrieval without introducing a second semantic authority.

**Architecture:** Each home device retains the canonical `semantic-memory` SQLite primary and mutation journal. `msi` stores one independently addressable replayed replica per device plus pooled control-plane metadata. The server may route and serve replica observations, but only a fenced canonical device writer can originate semantic mutations.

**Tech Stack:** Rust 2021, SQLite/WAL, `semantic-memory`, Axum, Tokio, `ed25519-dalek`, SHA-256, HMAC receipts, serde, deterministic fixed-field wire encoding, Python migration/benchmark tooling.

---

## 0. Current evidence and hard boundary

**Observed 2026-07-20:** `/home/sikmindz/Coding/pooled-memory` is on `master` at `7f5d23cae659c8572c5e2d2e35f58f79ebb87f71` with an uncommitted central-shard implementation. `cargo fmt --check` and `git diff --check` pass. Existing shard/router tests and migration tooling are prior evidence, not proof of replication.

**Dependency boundary:** `semantic-memory = { path = "../Libraries/semantic-memory" }`. Do not edit `/home/sikmindz/Coding/Libraries` in this worktree. Any canonical journal/replay implementation belongs in a separately reviewed semantic-memory worktree and must be consumed through an explicit dependency revision.

**Production boundary:** Do not touch `127.0.0.1:1738`, production data, systemd units, Hermes configuration, Codex configuration, or the MSI production repository. Do not restart the retired `17380/17480` smoke candidate.

**Non-goals for this tranche:** no production deployment; no arbitrary semantic replay; no live-owner queries; no direct SQLite/WAL/SHM synchronization; no promotion; no claim of continuous sync.

## 1. Authority invariants

1. Device primary is canonical for its shard; server replica is an observation/cache, never mutation authority.
2. One `(device_id, store_epoch, writer_epoch, sequence)` stream is accepted; stale epochs/tokens quarantine.
3. Every accepted envelope binds actor, namespace, requested effect, ACL snapshot, policy, authority receipt, payload digest, predecessor digest, and signer key/version.
4. Canonical wire bytes are fixed-field, length-delimited, deterministic; signed/digested bytes never come from unordered JSON maps.
5. Identical retries are idempotent; identity collisions with changed digest fail closed.
6. Authorization precedes catalog lookup, ranking, shard open, and detailed search.
7. Replica observations cannot authorize assertion, adjudication, redaction, forgetting, promotion, or recovery.
8. Unknown protocol versions, signer roles, replica states, and uncovered semantic writes fail closed.

## 2. Implementation dependency graph

```text
Phase 0 evidence/inventory
  -> Phase 1 protocol core
     -> Phase 2 pooled control-plane persistence
        -> Phase 3 semantic-memory journal/replay contract
           -> Phase 4 mechanical read-only replica boundary
              -> Phase 5 sync service + device agent
                 -> Phase 6 snapshot/recovery/promotion
                    -> Phase 7 freshness-aware routing
                       -> Phase 8 two-device fault simulator/benchmark
                          -> Phase 9 Hermes/Codex and production admission
```

Phases 1–2 may land in pooled-memory as verification-only infrastructure. Phases 3–9 remain blocked until their predecessors are independently reviewed.

## 3. Exact work packages

### Phase 0 — Mutation and authority inventory

**Files/artifacts:** `docs/REPLICATION_MUTATION_MATRIX.md`, `/home/sikmindz/Coding/semantic-memory-replication/` review worktree, inventory receipt outside Git.

Inventory facts, documents/chunks, conversations/messages, episodes, graph edges, imports, authority/adjudication, supersession, revocation, redaction/forgetting, projection/index writes, and pooled transitions. For each record canonical owner, transaction boundary, typed payload, authorization receipt, idempotency identity, replay semantics, and strict-mode support. Unknown/direct SQL writes are blockers.

**Gate:** every admitted write is journal-covered atomically or explicitly unavailable in replicated mode.

### Phase 1 — Protocol core (this implementation tranche)

**Files:**
- Create: `src/replication/mod.rs`, `src/replication/types.rs`, `src/replication/canonical.rs`, `src/replication/state_machine.rs`.
- Modify: `src/lib.rs`, `src/error.rs`, `Cargo.toml`, `Cargo.lock`.
- Test: `tests/replication_protocol.rs`.

Implement versioned envelopes/manifests/batches/ACKs, closed signer roles, replica state transitions, deterministic fixed-field encoding, SHA-256 digests, identity collision validation, and authorization-binding validation. Add Ed25519 only when the dependency and key lifecycle are reviewed; do not invent key storage in this tranche.

**RED/GREEN gate:** focused protocol tests fail before implementation and pass after; then full Rust tests, Clippy, and format checks.

### Phase 2 — Control-plane persistence

**Files:** `src/store.rs`, new `src/replication/catalog.rs`, `tests/replication_catalog.rs`.

Add only replica metadata, watermarks, apply ledger, sync sessions, quarantine, manifest, and certificate tables. Use the existing schema-generation gate and one immediate transaction. Never duplicate semantic content in `pooled.db`.

**Gate:** idempotent ACK replay, conflict quarantine, stale generation rejection, unsupported schema immutability, unauthorized metadata denial.

### Phase 3 — Canonical semantic-memory contract

**Separate worktree:** `/home/sikmindz/Coding/semantic-memory-replication`, based on the exact revision consumed by this crate.

Propose and independently review `MutationJournalOwner`: transactional mutation+journal+authority receipt, export-after-sequence, origin-aware replay, consistent snapshot, and manifest validation. No pooled adapter may infer semantic operations from row diffs.

**Gate:** mutation matrix complete, transaction rollback fault tests, replay equivalence, no replay loop.

### Phase 4 — Mechanical replica boundary

Replace writable server shard handles with `ReplicaStore::open_read_only` and private `ReplicaApplier::apply_batch`. Read-only/query-only mode must reject schema migration and ordinary mutators.

**Gate:** direct fact/document/message/authority/import mutations fail typed and leave files unchanged.

### Phase 5 — Sync transport and device agent

Add `src/sync/{protocol,auth,replica,client,server}.rs` and a separate device sync binary/service. Implement challenge, signed bounded batches, ACKs, retries, exact gaps, fork quarantine, backpressure, and session revocation. Server is outbound-connection-friendly; no inbound device listener is required.

**Gate:** duplicate/reorder/gap/divergence/revocation/offline reconnect/crash-after-commit-before-ACK tests.

### Phase 6 — Bootstrap, recovery, and promotion

Use SQLite Online Backup for sealed snapshots. Bind source digest/device/epoch/sequence/head/schema/ACL into an operator bootstrap certificate. Install with active/installing generation CAS; replay exact tail. Promotion atomically closes old epoch, advances fencing, revokes old sessions, and emits a signed certificate.

**Gate:** interrupted install preserves prior generation; snapshot+tail equals continuous replay; competing promotions cannot create two writers; catalog and per-replica restore drills pass.

### Phase 7 — Freshness-aware sparse routing

Routing order: authorization/lifecycle mask -> schema/key/fork/freshness mask -> grant-filtered summary rank -> bounded selection -> read-only replica search -> deterministic merge -> authenticated receipt. Bind location, generation, epoch, sequence/head, lag, ACL, summary digest, child receipt, and completeness.

**Gate:** unauthorized shards never lookup/open; incomplete searches cannot claim not-found; sparse versus exhaustive authorized benchmark meets predeclared Recall@K/overlap/latency thresholds.

### Phase 8 — Fault simulator and redaction

Build deterministic two-device simulator with fault injection around content/projection/journal/ACK/snapshot publication. Add redaction/forgetting propagation across queryable replicas, indexes, summaries, caches, and backup retention.

**Gate:** every failure mode yields durable typed evidence and no silent authority change.

### Phase 9 — Runtime integration and production admission

Verify Hermes/Codex parity, local-first device operation, MSI server replica routing, rollback, disk headroom, loopback binding, real-binary path, and operator receipts. Production remains blocked until all negative gates in `DEVICE_OWNED_REPLICATED_MEMORY.md` pass.

**Rollback:** retain the current central runtime and archived migration evidence; deployment is atomic and reversible. Never delete source primaries or old rollback generations during rehearsal.

## 4. Phase 1 protocol acceptance matrix

| Test | Expected failure/acceptance |
|---|---|
| deterministic wire encoding | same fields produce identical bytes/digest |
| unknown version/role/state | typed rejection before trusting fields |
| changed payload/ACL/effect/receipt | digest/signature binding mismatch |
| same operation/idempotency + same digest | duplicate accepted/idempotent |
| same identity + changed digest | conflict/quarantine |
| sequence gap or predecessor mismatch | quarantine; no state advance |
| stale epoch/fencing token | reject; no state advance |
| illegal replica transition | reject |
| bootstrap/promotion field omission | reject |
| replica observation used as authority | reject by evidence-state contract |

## 5. Required commands and receipts

Before each mutating phase refresh: `git status --short --branch`, `git rev-parse HEAD`, dependency revision, applicable `AGENTS.md`, and listener/process state. Preserve `/tmp/pooled-memory-pre-replication.patch` and its digest as rollback evidence.

Per-phase gates:

```bash
cargo fmt --all -- --check
cargo test --all-features --no-fail-fast
cargo clippy --all-targets --all-features -- -D warnings
git diff --check
python3 tests/test_device_shard_migration.py -v
python3 tests/test_shard_benchmark.py -v
```

Do not run multi-hour benchmarks or GPU recovery without explicit scope. Do not expose credentials, signing material, or raw semantic queries in receipts.

## 6. Claim boundary

- After Phase 1: “protocol validation primitives implemented and tested”; not synchronization.
- After Phase 2: “control-plane replication metadata implemented”; not replica correctness.
- After Phase 3: “canonical journal/replay contract exists in the dependency”; only then may sync implementation begin.
- After Phase 4: “server query handles are mechanically read-only”; not production-ready.
- After Phase 5: “bounded synchronization vertical slice is tested”; not full mutation coverage.
- Only after Phase 9 gates: production admission may be considered.

## 7. Hard no list

No live DB/WAL/SHM copying, rsync, Syncthing database mirroring, generic LWW/CRDT conflict authority, flattened pooled semantic store, row-diff inferred journal, server-side unrestricted writes, relevance-based authorization, silent partial search, stale receipt-as-authority, production mutation before gates, commit/push before audit, or claims beyond witnessed evidence.

## 8. Handoff

An auditor must re-run the live baseline, inspect every changed file, run focused and full gates, verify no production listener/data mutation, and confirm that the implementation claim matches the phase actually completed.
