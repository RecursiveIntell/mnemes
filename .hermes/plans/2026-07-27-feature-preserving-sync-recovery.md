# Feature-Preserving Semantic Memory + Mnemes Sync Recovery Plan

> **Status:** proposed recovery plan — no service, database, Git ref, or crates.io release has been changed by this plan.

**Goal:** Preserve the complete `semantic-memory` 0.5-era MCP contract and add durable device-primary → authority replication as a narrow Mnemes-owned outbox sidecar.

**Architecture:** `semantic-memory` remains the canonical local mutation owner and emits replayable mutation records atomically. `mnemes` owns signed envelope transport, sender ACK projection, authority admission, idempotent apply, and the `mnemes-syncd` service. `semantic-memory-mcp` only supplies immutable local stream identity when opening the canonical store; it is not a transport worker.

**Hard rule:** No feature or MCP tool family may be silently retired to enable sync. The published 0.6.0 archive is quarantined as an experimental compressed-scoring branch until it is feature-reintegrated and compatibility-certified.

---

## Evidence snapshot — 2026-07-27

### Frozen release artifacts

| Artifact | Observation | Witness |
|---|---|---|
| crates.io `semantic-memory` 0.5.14 | Full feature surface required by MCP; includes `journal.rs`, mutation journal migration V37, config identity, and canonical fact write journaling. | `~/.cache/mnemes-replication-evidence-2026-07-27/semantic-memory-crates-0.5.14`, archive SHA-256 `edf50f8bc36ba288c997ad23e6c6b3f8778c489c3546e3792e089dbcd4e11d92` |
| crates.io `semantic-memory` 0.6.0 | Removes 40 source modules, including `journal.rs`, authority, provenance, routing, decoder, graph reasoning, topology, integration, and usearch. Adds FibQuant/per-dimension scoring features. | `~/.cache/mnemes-replication-evidence-2026-07-27/semantic-memory-crates-0.6.0`, archive SHA-256 `a68fb7b92b56dc23fefbf2fa19e7b04b982a868082e39a89cd803a365ae102a0` |
| Public source provenance | `semantic-memory` 0.6.0 was pushed in the public **Libraries monorepo** as `origin/fix/hostile-remediation-20260715` commit `fd6bdb7a5ff9b1755ae223a3b325a866366211d2`; its message says it was published ~1 hour earlier. The standalone `RecursiveIntell/semantic-memory` repository has no corresponding 0.6 ref. The committed monorepo source has the reduced manifest; the local full modules are untracked, so neither location alone is a reproducible full-contract release source. | local `git show`, `git branch -vv`, and `git status` captured 2026-07-27 |

### Current-source findings

- `mnemes/Cargo.toml` currently accepts `semantic-memory = "0.5"`; recovery must pin the certified baseline exactly before worker deployment.
- `mnemes/src/sync.rs` and `sync_handler.rs` directly query/forward `mutation_journal` raw payloads and explicitly defer envelope admission.
- `mnemes/src/sync_handler.rs` computes `next_sequence` as `start + synced + errors`, which can acknowledge past an error. That violates contiguous-ACK safety.
- `mnemes/src/replica.rs` detects same-sequence payload mismatch but does not check the expected next sequence before applying a new record.
- `mnemes` has no `mnemes-syncd` binary yet; its manifest exposes only `mnemes-server` and `mnemes-admin`.
- The existing Mnemes closure plan at `.hermes/plans/2026-07-27-mnemes-replication-closure.md` is directionally correct and remains a source input. This plan replaces its inaccurate assertion that published 0.6 contains a compatible journal.

## Canonical version decision

1. **Runtime baseline:** certify and pin the full `semantic-memory` 0.5.14 source contract for the MCP and Mnemes integration. Do not point MCP at the published 0.6.0 archive.
2. **0.6 status:** quarantine it as a non-runtime experimental branch. Its compressed scoring additions are candidates to port into the full baseline behind independent feature flags and benchmark gates.
3. **Next public release:** create a new source-attested recovery release after the full contract is restored and tested. Its Git revision/tag, `cargo package` archive digest, and crates.io archive must agree. The existing 0.6.0 archive is linked to a public monorepo commit, but the full-contract source set is not captured by that commit; do not present it as a reproducible full-contract release. Do not yank or alter the published crate without explicit operator approval.
4. **No mixed identities:** all crates sharing `stack-ids`/semantic-memory types must resolve each package from one Cargo source per build graph. Never mix registry and local path copies of the same package version.

## Required invariants

1. A local canonical mutation and its replayable outbox entry commit in one SQLite transaction.
2. Every outbox record has a stream key `(home_device_id, store_id)`, contiguous sequence, typed operation/version, exact replay payload, and payload digest.
3. The worker reads the outbox through semantic-memory’s public export API only. It never inspects facts to synthesize mutations and never builds a second outbox.
4. The authority verifies device binding, signature, operation schema, payload digest, and expected sequence before replay.
5. On the authority, semantic replay + inbox/idempotency row + stream-head advancement commit in one transaction.
6. Same stream/sequence/digest is idempotent and returns the original durable ACK. Same sequence with another digest is a typed conflict. A gap is a hard degraded state and is never skipped.
7. The local sender advances its persisted ACK projection only after it validates the authority’s durable contiguous ACK.
8. Sync may be retried indefinitely with bounded backoff, but authority, identity, conflict, or gap errors must stop normal sending and make the worker degraded.
9. Existing MCP tools, governed writes, provenance, temporal, routing, graph, and evidence features remain present and behaviorally compatible.

---

## Phase 0 — provenance quarantine and compatibility baseline

### Task 0.1: Preserve release evidence and create a recovery worktree

**Files:**
- Preserve: `~/.cache/mnemes-replication-evidence-2026-07-27/**`
- Create: dedicated clean worktree/branch for the recovery source set; do not reuse the current dirty `Libraries` tree.

**Steps:**
1. Retain the existing witness directory unchanged.
2. Create a recovery branch from an exact full-feature source matching the 0.5.14 package witness.
3. Record package archive digests, Git HEADs, Cargo.lock digest, and `cargo metadata` output in the recovery branch.
4. Reconcile every package source against the Cargo resolution graph before editing.

**Gate:** every dependency path is explicit; no target package resolves twice from different sources.

**Rollback:** delete only the new worktree/branch. Do not reset shared dirty source trees.

### Task 0.2: Freeze the MCP capability contract before source changes

**Files:**
- Create: `semantic-memory-mcp/tests/full_profile_contract.rs`
- Create: `semantic-memory-mcp/tests/fixtures/full-tools-list.json`

**RED:** write a test that starts a disposable full-profile MCP store and asserts the currently admitted tool families remain discoverable: governed writes, authority, provenance, temporal, graph, routing, decoder, evidence, replay, and maintenance.

**GREEN:** generate the fixture from the known-good installed binary only after recording its binary hash and startup arguments. Compare candidate `tools/list` by tool name and schema-required fields; allow additive tools only.

**Commands:**
```bash
cargo test -p semantic-memory-mcp --test full_profile_contract
cargo test -p semantic-memory-mcp --all-features
```

**Gate:** candidate loss of any known tool is a release blocker.

---

## Phase 1 — restore a full-feature semantic-memory source contract

### Task 1.1: Restore/retain feature declarations and public exports

**Files:**
- Modify: `Libraries/semantic-memory/Cargo.toml`
- Modify: `Libraries/semantic-memory/src/lib.rs`
- Test: new compile feature-contract test or CI matrix

**Objective:** preserve the 0.5.14 feature set and all public exports consumed by MCP before integrating any 0.6 scoring work.

**RED:** compile MCP `--all-features` against the recovery source with each MCP-requested semantic-memory feature enabled.

**GREEN:** restore only source-backed modules, dependencies, feature flags, re-exports, migrations, and tests. No stub modules, no hidden tools, and no fallback implementation.

**Commands:**
```bash
cargo check -p semantic-memory --all-features
cargo check -p semantic-memory-mcp --all-features
cargo test -p semantic-memory-mcp --all-features
```

**Gate:** full MCP compile and contract test pass against a disposable store.

### Task 1.2: Port 0.6 compressed-scoring additions as opt-in additions

**Files:**
- Add/retain: `src/scoring/fib_scorer.rs`, `src/scoring/mod.rs`
- Modify: `Cargo.toml`, `src/vector_codec.rs`, `src/lib.rs`
- Test: existing/new codec-specific tests and benchmark gates

**Objective:** integrate FibQuant/per-dimension work without changing the default vector backend or removing any 0.5 feature.

**Rules:**
- Keep `usearch-backend` as the certified default until a reproducible benchmark and migration gate authorize a change.
- Add codecs behind distinct opt-in features.
- Require exact rerank behavior and storage compatibility tests.

**Rollback:** disable only the new codec feature; do not alter semantic-memory data or MCP tool availability.

---

## Phase 2 — canonical typed outbox in semantic-memory

### Task 2.1: Version the existing journal rather than inventing another outbox

**Files:**
- Modify: `Libraries/semantic-memory/src/journal.rs`
- Modify: `Libraries/semantic-memory/src/db.rs`
- Add: `Libraries/semantic-memory/tests/journal_outbox_contract.rs`

**Objective:** evolve `mutation_journal` into the sole replayable replication outbox.

**Schema proposal:** add a new migration (never rewrite V37) for immutable fields needed by transport: `payload_sha256`, `payload_schema_version`, and a stable operation ID if the existing payload does not already contain one. Add a per-stream sequence allocator table rather than relying on `MAX(sequence)+1` under concurrent writers.

**RED tests:**
- concurrent writers on one stream receive distinct contiguous sequences;
- semantic mutation failure produces no outbox record;
- journal append failure rolls back the semantic mutation;
- payload digest mismatch is detected;
- export stops at the first gap and reports a typed gap state.

**GREEN:** make each admitting mutation use one transaction/connection context for semantic writes, sequence allocation, payload construction, digest creation, and journal append.

**Gate:** no direct facts-table event synthesis; every admitted mutation has exactly one typed record or is rejected when Mnemes-required mode is enabled.

### Task 2.2: Define typed replay payloads for every admitted mutation

**Files:**
- Add: `Libraries/semantic-memory/src/replication_payload.rs`
- Modify: canonical mutation owners in `knowledge.rs`, governed authority paths, deletion/supersession owners, and namespace deletion owner.
- Test: `tests/journal_outbox_contract.rs`

**Operation families:** `fact.create.v1`, fact update/supersede, governed forget/delete, namespace delete, and any other mutation currently admitted by MCP.

**Rules:**
- Payloads are versioned typed data with deterministic serialization and explicit source IDs.
- A write without a replay form is rejected in Mnemes-required mode; it is not silently unreplicated.
- Replay payloads are canonical semantic intent, not copied SQL or a database snapshot.

**Acceptance:** one table-driven test enumerates every public mutation route and asserts its journal outcome or explicit rejection.

---

## Phase 3 — Mnemes protocol and authority admission

### Task 3.1: Evolve the existing envelope family; do not create parallel identity semantics

**Files:**
- Inspect first: `mnemes/src/replication/types.rs`, `canonical.rs`, `trusted_key.rs`, `state_machine.rs`
- Modify only after field-level ownership review.
- Test: `mnemes/tests/replication_protocol.rs`

**Objective:** use the existing `MemoryMutationEnvelopeV1`, trusted-key registry, canonical digest, and replica watermark mechanisms as the protocol owners. Create V2 only if V1 cannot carry the required typed payload digest, operation/version, stream identity, and signature without ambiguity.

**RED tests:** invalid signer, wrong device binding, altered payload, altered digest, stale key, and mismatched home/store identity all fail before replica mutation.

### Task 3.2: Replace raw `/v1/sync` semantics with admitted contiguous batches

**Files:**
- Modify: `mnemes/src/sync_handler.rs`
- Modify: `mnemes/src/server.rs`
- Modify: `mnemes/src/replica.rs`
- Add: `mnemes/tests/authority_sync_admission.rs`

**Current defects to remove:**
- ignored `TrustedKeyRegistry`;
- raw hex payload admitted without envelope validation;
- errors accumulated while later records are processed;
- `next_sequence = start + synced + errors` can skip a failed sequence;
- replica apply does not enforce the expected contiguous sequence.

**Protocol proposal:**
1. Client POSTs a versioned, signed batch envelope.
2. Authority resolves the stream head first.
3. If first new sequence is not exactly `head + 1`, return typed `Gap` with no mutation.
4. For a duplicate sequence, compare the stored digest; same digest returns prior ACK, different digest returns typed `Conflict`.
5. For each new sequence in the contiguous prefix, verify digest and schema, replay via semantic-memory’s typed replay API, insert inbox receipt, and advance stream head inside one authority SQLite transaction.
6. Reply with `acknowledged_through`, per-entry outcome IDs, and an authority receipt digest. Never return an inferred next sequence.

**Gate:** failure or gap at N leaves N+1 and later entries unapplied and unacknowledged.

### Task 3.3: Make replica apply a transaction participant, not a nested authority owner

**Files:**
- Modify: `mnemes/src/replica.rs`
- Test: `mnemes/tests/authority_sync_admission.rs`

**Objective:** refactor `ReplicaApplier` so the authority admission layer controls one transaction containing replay, durable inbox/idempotency row, mutation-journal record, and stream-head advancement.

**RED tests:** injected replay/inbox/head failure rolls back all four records; duplicate returns original receipt; conflict changes nothing.

**Rollback:** retain the old endpoint only as a deprecated fail-closed compatibility response. Do not keep a raw-payload bypass endpoint active.

---

## Phase 4 — Mnemes-owned sender worker

### Task 4.1: Add `mnemes-syncd`

**Files:**
- Modify: `mnemes/Cargo.toml`
- Create: `mnemes/src/bin/mnemes-syncd.rs`
- Create: `mnemes/src/sender_state.rs`
- Modify: `mnemes/src/sync.rs`
- Add: `mnemes/tests/sync_worker_e2e.rs`

**Objective:** create a Rust worker owned by Mnemes, not the MCP daemon and not a Python fallback.

**Worker responsibilities:**
- require device ID, store ID, authority URL, trusted credential/key material, and local store path at startup;
- use semantic-memory’s public contiguous journal export API;
- persist only sender projection state: stream key, last verified authority ACK, last receipt digest, retry/degraded condition;
- send bounded contiguous batches;
- verify response stream identity, receipt/digest, and `acknowledged_through` before advancing local state;
- use bounded exponential backoff with jitter for transport failures;
- stop normal sends and expose degraded status for gap, conflict, signature, schema, or authority errors.

**Forbidden:** direct facts-table reads, direct mutation-journal SQL from Mnemes, local event reconstruction, watermarks manually changed by operators, or a `--disable-sync` bypass for a Mnemes-required store.

**RED tests:** authority unavailable/recovery, process kill after send before local ACK persist, duplicate resend after restart, remote duplicate, gap, conflict, tampered receipt, and no pending entries.

**GREEN gate:** each scenario has a deterministic status/exit code and leaves the authority with exactly-once semantic effects.

### Task 4.2: Add a constrained systemd user unit only after worker tests pass

**Files:**
- Create: `mnemes/scripts/install-mnemes-syncd-service.sh`
- Create at deploy time only: `~/.config/systemd/user/mnemes-syncd.service`

**Unit rules:** `Restart=always`, bounded restart policy, private 0600 credential file, no secret in argv, explicit working directory, environment file path, and `ExecStart` pinning an exact binary path/hash.

**Gate:** `systemd-run --user` disposable smoke test succeeds; service restart proves duplicate-safe continuation.

---

## Phase 5 — MCP configuration boundary

### Task 5.1: Configure journal identity at store construction

**Files:**
- Modify: `Libraries/semantic-memory-mcp/src/main.rs`
- Modify: `Libraries/semantic-memory-mcp/src/bridge.rs`
- Test: `semantic-memory-mcp/tests/integration.rs`

**Objective:** pass `--mnemes-device-id` and `--mnemes-store-id` into `MemoryConfig` before the store opens, using the existing 0.5.14 `journal_device_id`/`journal_store_id` fields.

**RED tests:**
- `--mnemes-required` with missing identity fails before opening SQLite;
- partial, empty, or whitespace-bearing identity fails;
- a configured fact write produces one atomic journal entry;
- an unconfigured normal MCP remains feature-compatible but is visibly non-replicating.

**Rule:** do not introduce a second mutable `configure_replication` method after startup. Store identity is construction-time configuration.

---

## Phase 6 — certification, cutover, and rollback

### Task 6.1: Full build and compatibility certification

**Commands:**
```bash
cargo check -p semantic-memory --all-features
cargo test -p semantic-memory --all-features
cargo check -p semantic-memory-mcp --all-features
cargo test -p semantic-memory-mcp --all-features
cargo test --manifest-path /home/sikmindz/Coding/mnemes/Cargo.toml --all-features
```

**Additional gates:**
- candidate MCP `tools/list` matches the frozen full-profile fixture;
- candidate opens a copy of the existing store in read-only smoke mode;
- source lockfile contains no duplicate package identities for shared typed crates;
- all replay/failure-injection tests pass.

### Task 6.2: Controlled deployment

1. Take SQLite online backups and record hashes.
2. Build and hash candidate MCP, Mnemes authority, and worker binaries.
3. Replace no active binary until the full tool inventory and journal test gates pass.
4. Install candidate binaries by staged temporary path and atomic rename while retaining the old binary and prior unit files.
5. Enable `mnemes-syncd` only after authority admission and worker restart tests pass.
6. Retire the existing Python worker only after one acknowledged end-to-end receipt and a restart proof.

**End-to-end acceptance receipt:** source witnesses, exact source HEADs/diffs, Cargo.lock hashes, binary hashes, local fact ID, stream identity, local sequence, payload digest, authority receipt/ACK, remote semantic verification query, worker restart evidence, authority restart evidence, and rollback locations.

### Rollback triggers and procedure

**Triggers:** MCP tool loss, failed compatibility gate, journal gap, digest conflict, failed authority replay, invalid authority receipt, or any worker degraded state.

**Procedure:** stop the candidate worker; restore only the previous tested executable/unit from staged backup; restart; preserve all journal/inbox/sender-state rows and evidence. Never edit sequence heads or ACK watermarks manually. The retained outbox permits a later corrected retry.

---

## Claims licensed by each milestone

| Milestone | Safe claim | Not yet licensed |
|---|---|---|
| Phase 0 complete | 0.6 package/Git provenance has been frozen and quarantined for runtime use. | 0.5 recovery source is production-ready. |
| Phases 1–2 complete | Full MCP contract is preserved and local writes have an atomic replayable outbox. | Remote replication works. |
| Phases 3–4 complete | Typed authority admission and a restart-safe worker pass disposable integration tests. | Live UNO Q replication is deployed. |
| Phase 6 complete | A specific local mutation was durably acknowledged and verified after restarts. | General production reliability beyond the tested scope. |

## Hard no list

- Do not downgrade the live service to published 0.6.0.
- Do not erase or reset dirty shared source trees.
- Do not generate sync events from `facts`, copy SQLite files, or use raw SQL as a mutation replay interface.
- Do not let any authority endpoint accept an unverified raw payload.
- Do not treat a returned `next_sequence` as an ACK.
- Do not skip a gap, auto-resolve a digest conflict, or manually bump a watermark.
- Do not publish or claim a recovered release until the public Git revision/tag, crate archive, source witness, feature contract, and full test gate agree.
