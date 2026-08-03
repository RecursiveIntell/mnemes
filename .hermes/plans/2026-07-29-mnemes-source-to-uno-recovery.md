# Mnemes + semantic-memory Source-to-UNO Recovery Plan

**Date:** 2026-07-29  
**Status:** accepted for execution; phase-gated  
**Scope:** restore Mnemes shard correctness and prove one device-primary → UNO fact-create replication slice  
**Non-claim:** this plan does not establish full-memory, bidirectional, multi-writer, or all-mutation synchronization.

## 1. Verdict

Proceed in this dependency order:

1. preserve and certify the in-progress semantic-memory V39 receiver owner API;
2. eliminate Mnemes' contradictory legacy global-store accessor and route all semantic operations through authenticated device shards;
3. bind Mnemes to one exact semantic-memory source revision containing V38 outbox + V39 receiver apply;
4. add one closed, signed fact-create admission route that maps the authenticated device to a server-owned shard;
5. add one Rust sender that exports only through semantic-memory's verified contiguous API and persists only sender ACK state;
6. prove duplicate/fork/gap/epoch/restart behavior on disposable stores;
7. deploy by online backup + atomic binary replacement, then perform a single production canary and restart proof;
8. commit and push only after every phase gate passes.

The running UNO authority remains on the known-old healthy binary until Phases 1–5 pass locally and the ARM64 candidate is admitted.

## 2. Current observed state

| Surface | Observed state | Authority |
|---|---|---|
| UNO authority | `mnemes-authority.service` active; `/livez` returns `{"service":"up"}` | live SSH probe |
| UNO executable | previous binary SHA-256 `b8912f31b9ef4abf121304c1ea343d73cd6ab71c3848e24ec29f58b65eeded7d` | live host hash |
| Legacy global DB | absent from active UNO tree after reversible quarantine; quarantined DB was structurally clean and empty | live filesystem + SQLite probe |
| Mnemes source | HEAD `118121260899028b8d367398d813643dcd03fdcb`; tracked tree clean; untracked plans/evidence preserved | Git |
| Legacy-store defect | startup rejects `memory/memory.db`, but `MnemesStore::memory()` lazily creates it; six callers remain | current source |
| semantic-memory source | HEAD `bd53ac3ac7e0e55618c15cc3b8da7a52588f6349`; protected uncommitted V39 work in `db.rs`, `journal.rs`, `knowledge.rs`, and a new receiver contract test | current Git/source |
| semantic-memory outbox | V38 typed fact-create stream identity, digest chain, and contiguous export exist | current source/tests |
| semantic-memory receiver | V39 work adds closed fact-create apply, inbox/stream state, durable duplicate decision, and rollback contract; not yet admitted until tests pass | protected current diff |
| MCP source | HEAD `e6add90ab56d80cc01fda6644c6b0fdeae5f9b60`; clean | Git |
| MCP runtime identity | current user units contain no `--mnemes-device-id`, `--mnemes-store-id`, `--mnemes-stream-epoch`, or `--mnemes-required` flags | live unit inspection |
| Mnemes dependency | `Cargo.lock` resolves registry `semantic-memory 0.5.13`, not the current V38/V39 source | Cargo.lock |
| Old sync routes | `/v1/sync` and `/v1/sync/facts` intentionally return `501 SYNC_DISABLED` | current source/tests |
| Mnemes transport | generic signed-envelope prototypes exist, but trusted keys are in-memory/test-only and raw sync/applier code is not an admitted boundary | current source |
| ARM64 build | direct GNU cross failed for missing target libc headers; containerized `cross build` succeeded previously | build receipts |

## 3. Agent Graph advisory

Graph: `mnemes-sync-recovery-council-20260729`  
Run: `run-19fac482195-1`  
Graph version: `sha256:0c78cfc51d4b1d6fca12c49577c8ad82d3e59afb60bce53d3963f141209ee361`  
Output digest: `sha256:0b669284ac2acc8fc5aa0c056f8439af073166cbd181fade20424c0baefe4dea`  
Model: `glm-5.2:cloud`

The council converged on the correct ordering: store invariant → dependency identity → bounded transport → local/UNO verification → commit/push. Its receipt is advisory only: `evidence_authority=structural_unverified` and volatile persistence failed with `INTEGRITY_KEY_REQUIRED`. Invented details in its synthesis—such as absent sequencing or new duplicate device-ID abstractions—are rejected where current source contradicts them.

## 4. Governing invariants

### Authority

- The device semantic-memory store is primary.
- Each Mnemes semantic replica lives only at the server-derived path `memory/shards/<device_uuid>/memory.db`.
- No global compatibility store, caller-selected filesystem path, shadow outbox, or adapter-owned replay semantics.
- semantic-memory owns canonical mutation payloads, digest chains, contiguous export, and semantic replay.
- Mnemes owns transport authentication, trusted signer admission, device-to-shard mapping, receiver invocation, and sender supervision.

### Stream

- Stream identity is `(home_device_id, store_id, stream_epoch)`.
- Sequence is contiguous and starts at 1 for an admitted epoch.
- Same stream/sequence/envelope digest returns `Duplicate`.
- Same stream/sequence with another digest returns `Fork` with no mutation.
- Sequence above expected returns `Gap` with no mutation or head advance.
- Wrong predecessor or epoch returns a typed terminal decision.
- Semantic row, derived write bookkeeping, receiver inbox, stream head, and durable decision commit atomically in semantic-memory.

### Security

- The bearer-authenticated device must equal the envelope's home device.
- The receiver resolves the shard from its own catalog; the caller never supplies a path.
- A persistent admitted public key must match signer principal, key version, device, store, role, lifecycle, and revocation state.
- Exact payload/digest/signature/size/version checks happen before semantic apply.
- Secrets are loaded from mode-0600 files, never printed or placed in Git.
- Legacy raw routes stay disabled before auth/body parsing.

### Claims

- Before all local protocol tests pass: **contained and under implementation**.
- After disposable local + UNO tests pass: **fact-create replication vertical slice verified on disposable stores**.
- After one production canary and both-side restart proof: **one-device, one-store, one-way fact-create canary verified**.
- Never call this full synchronization until every admitted mutation family has its own atomic owner/replay/transport tests.

## 5. Dependency-enforced implementation DAG

### Phase 0 — Protect and certify current source

**Surfaces**

- `/home/sikmindz/Coding/Libraries/semantic-memory`
- `/home/sikmindz/Coding/Libraries/semantic-memory-mcp`
- `/home/sikmindz/Coding/mnemes`

**Actions**

1. Preserve the current semantic-memory V39 diff and record its patch digest.
2. Run V38, V39, journal E2E, full semantic-memory tests, format, and strict Clippy.
3. Inspect failures before modifying protected work.
4. Record exact Git heads, dirty paths, lockfile digests, and toolchain versions.

**Gate**

```bash
cargo test -p semantic-memory --all-features --test replication_v38_contract
cargo test -p semantic-memory --all-features --test journal_replication_e2e
cargo test -p semantic-memory --all-features --test replica_fact_create_v39_contract
cargo test -p semantic-memory --all-features
cargo clippy -p semantic-memory --all-targets --all-features -- -D warnings
cargo fmt -p semantic-memory --all -- --check
git diff --check
```

**Rollback**

No reset or deletion. If the V39 work fails, patch only the demonstrated defect and retain the original patch receipt.

### Phase 1 — Remove the legacy global-store contradiction

**Files**

- `mnemes/src/store.rs`
- `mnemes/src/server.rs`
- `mnemes/tests/server.rs`

**RED tests**

1. Authenticated witnessed search on a registered device creates/uses only that device shard.
2. `sm_stats` reports aggregate shard catalog statistics and never opens `memory/memory.db`.
3. `sm_verify_integrity` verifies the authenticated device shard and never opens the global path.
4. Two devices remain isolated.
5. After search/stats/integrity, drop and reopen `MnemesStore`; `memory/memory.db` remains absent.
6. A pre-existing legacy DB is rejected byte-for-byte without modification.

**Minimal GREEN**

- Remove `legacy_memory: OnceLock<MemoryStore>` and `MnemesStore::memory()` entirely.
- Pass authenticated `DeviceId` into `run_witnessed_search`; remove first-device selection and global fallback.
- Use `routed_search(authenticated_device, ...)` for witnessed search.
- Use `aggregate_shard_stats()` for pool-level `sm_stats`.
- Use `device_memory(&context.device.device_id)` for device-scoped integrity.
- Seed tests through `device_memory`, never a compatibility store.

**Gate**

```bash
MNEMES_EMBEDDER=ollama cargo test --locked --no-default-features --features server --test server
cargo fmt --all -- --check
cargo clippy --all-targets --no-default-features --features server -- -D warnings
```

**Rollback trigger**

Any global DB appears, cross-device data bleeds, or existing authenticated MCP behavior regresses. Revert only the Phase 1 patch; preserve the empty legacy artifact quarantine. Do not restore the global accessor in production code.

### Phase 2 — Admit one semantic-memory revision

**Decision**

Mnemes must resolve one exact source containing both V38 writer/export and V39 receiver apply. A registry range such as `0.5` is insufficient.

**Actions**

1. After Phase 0 passes, commit and push the semantic-memory V39 owner change separately.
2. Pin Mnemes to that exact immutable Git revision, or to a newly published source-attested crate if publication is explicitly authorized. Do not use an absolute/local path in committed Mnemes source.
3. Update `Cargo.lock` and prove one `semantic-memory` package identity in `cargo tree -d`/metadata.
4. Remove or quarantine Mnemes raw SQL exporter, closure-based replica applier, and raw sync handler from the admitted code path. Keep legacy HTTP routes hard-disabled.

**Gate**

```bash
cargo metadata --locked --format-version 1
cargo tree -d
cargo check --locked --no-default-features --features server
```

The resolved semantic-memory source must equal the admitted revision and expose `export_verified_contiguous`, `FactCreateReplicaEnvelopeV1`, and `apply_verified_fact_create`.

### Phase 3 — Persistent, closed authority admission

**Files/surfaces**

- `mnemes/src/store.rs`: persistent trusted replication-key records in pooled control DB
- `mnemes/src/replication/*`: operation-specific signed wrapper around semantic-memory's canonical replica envelope
- `mnemes/src/server.rs`: one authenticated route, `/v1/replication/fact-create`
- `mnemes/src/bin/mnemes-admin.rs`: provision/revoke public keys without exposing private material
- integration tests for admission and failure semantics

**RED tests**

- unknown key, embedded-key mismatch, revoked key, wrong role, expired/not-yet-active key;
- bearer device does not match home device;
- wrong store, epoch, sequence, predecessor, payload digest, envelope digest, signature, operation, or schema;
- unknown JSON field, oversized body, malformed arrays/hex/base64, or trailing data;
- same envelope returns durable `Duplicate` after authority restart;
- changed same-sequence envelope returns `Fork`;
- gap returns expected/received and changes nothing;
- injected receiver inbox failure rolls back fact and stream head;
- caller cannot choose a shard path.

**Minimal GREEN**

- Define a closed `#[serde(deny_unknown_fields)]` transport DTO that contains semantic-memory's canonical fact-create envelope plus signer identity/version/time/signature.
- Sign a domain-separated, length-prefixed preimage over the exact canonical semantic envelope fields/digests.
- Persist only public trusted-key records and lifecycle state in `pooled.db`.
- Authorize bearer device, validate trusted key and signature, resolve `device_memory` from the server catalog, call `apply_verified_fact_create`, and return a typed decision.
- Do not populate unrelated generic governance fields with zeros or fabricated values.

**Gate**

Focused protocol tests, full Mnemes tests, format, strict Clippy, body-size test, and authority restart test all pass.

### Phase 4 — One Rust sender

**Files/surfaces**

- new `mnemes-syncd` Rust binary
- sender state module
- systemd user unit installer and mode-0600 environment/credential contract

**RED tests**

- empty outbox is distinct from end/gap;
- contiguous batch export through `semantic_memory::journal::export_verified_contiguous` only;
- authority unavailable then recovers;
- process dies after remote commit but before local ACK persistence;
- duplicate retry after restart;
- gap/fork/epoch/signature decisions stop normal sending and enter degraded state;
- no file-descriptor growth in a bounded soak;
- fake `python3` in `PATH` cannot affect execution.

**Minimal GREEN**

- Open the source DB read-only and call the owner export API; do not query journal tables directly.
- Convert each `JournalEntry` losslessly to the canonical replica envelope.
- Sign with a device-owned Ed25519 key loaded from a mode-0600 file.
- POST bounded batches or one record at a time to the private tunnel URL.
- Persist only stream identity, last durable contiguous ACK, last receipt/digest, and degraded state via atomic file replace or a dedicated sender-state SQLite DB.
- Use bounded exponential backoff with jitter only for retryable transport failures.

**Gate**

Disposable sender/authority end-to-end test proves applied → duplicate-on-retry → restart continuity without manual watermark edits.

### Phase 5 — MCP construction-time identity and disposable pilot

**Actions**

1. Build the current MCP from clean source containing construction-time identity support.
2. Configure a disposable source store with explicit device ID, store ID, stream epoch, and required fact-create mode.
3. Write one fact through the real MCP/API.
4. Observe exactly one verified V38 journal record.
5. Deliver it through the Rust sender and admitted authority route to a disposable UNO shard.
6. Verify exact fact ID/content/source/metadata, inbox state, stream head, and ACK.
7. Repeat duplicate/fork/gap/epoch tests and restart both sides.

**Gate**

No production unit changes until this pilot passes locally and on UNO disposable paths.

### Phase 6 — Controlled deployment and one production canary

**Preconditions**

- All source/test gates pass.
- All repos have reviewed, scoped diffs.
- ARM64 `cross build` succeeds from exact lock/source revisions.
- Candidate runs on UNO (`--help` or disposable smoke) and linked ABI requirements are satisfied.

**Deployment sequence**

1. Verify UNO host identity, service unit, active executable path/hash, loopback binding, and filesystem capacity.
2. Take SQLite Online Backup API snapshots of `pooled.db` and all shard DBs that candidate code could mutate; run `PRAGMA quick_check` on backups.
3. Back up executable, service unit, public-key registry state, and hashes.
4. Install candidate with same-filesystem temporary path + atomic rename.
5. Restart authority and verify active PID executable/hash, `/livez`, authenticated health, MCP reads, integrity, legacy route `501`, and absence of `memory/memory.db`.
6. Install/start the Rust sender only after authority checks pass.
7. Create one uniquely tagged production fact through the real local semantic-memory API.
8. Verify local journal identity/sequence/digest, remote typed ACK, exact remote fact parity, duplicate retry, sender restart, authority restart, and unchanged single semantic effect.
9. Observe for a bounded soak; verify lag, errors, FDs, and service restarts.

**Rollback**

- Stop/disable only the new sender first; local primary remains available.
- If authority behavior regresses, stop candidate, atomically restore the prior executable/unit, restart, and verify original SHA/liveness.
- Preserve all outbox/inbox/ACK rows and candidate shard evidence. Never decrement a stream head or edit an ACK/watermark manually.

### Phase 7 — Git closure

Separate commits:

1. semantic-memory V39 canonical receiver owner + contract;
2. Mnemes global-store removal;
3. Mnemes exact dependency + signed admission;
4. Rust sender/service;
5. documentation and rerunnable verification script.

Push only after local and UNO gates pass. Verify fetched remote refs equal local HEADs.

## 6. Hard no list

- No data deletion, sequence reset, manual ACK/watermark bump, or forced gap skip.
- No global compatibility database or fallback-first-device routing.
- No raw SQL/payload sync endpoint, caller-supplied replica path, or replay closure crossing the transport boundary.
- No Python synchronization fallback.
- No ongoing SQLite file copy presented as replication.
- No mutable post-open stream identity.
- No fabricated policy/authorization fields just to satisfy an over-broad generic envelope.
- No committed local absolute path dependency.
- No public “full sync” or production-reliability claim from a fact-create canary.

## 7. Completion receipt

Closure requires:

- changed files and scoped diff digests;
- all Git refs and dependency source identities;
- exact test/check/Clippy/build commands with pass/fail/skip states;
- controller and ARM64 binary hashes plus active process-bound hash;
- online backup paths/hashes and rollback commands;
- disposable and production fact IDs, stream identity, sequence, payload/envelope digests, ACK decision, and normalized remote parity;
- both-side restart evidence;
- unresolved mutation families and risks;
- one auditor-rerunnable verification script.
