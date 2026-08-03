# Mnemes Device-Primary Replication Closure v2

**Date:** 2026-07-28  
**Authority:** controller-reconciled current source and live-runtime evidence  
**Evidence packet:** `.hermes/evidence/2026-07-28-mnemes-sync-plan-evidence.md`  
**Evidence SHA-256:** `7c578eb41250bc6696fc64331e1f3526b1e8ae870ed6ebaccff52da139452a29`  
**Scope:** one-device, one-store, one-way **fact-create canary** from a device-primary semantic-memory store to an UNO Q Mnemes replica  
**Non-claim:** this plan does not establish full-memory convergence, failover, multi-writer replication, or production maturity.

## 1. Verdict

The old synchronization system is rejected. Two incompatible Python workers, raw SQL replay, direct fact snapshot ingestion, caller-selected replica paths, mutable source identity, sequence-only idempotency, and writable “read-only” replicas cannot be repaired by incremental route wiring.

The highest-ROI closure is:

1. contain every legacy synchronization writer and route;
2. make semantic-memory the only owner of canonical typed mutation payloads and the local transactional outbox;
3. admit only signed typed envelopes through one Rust Mnemes route;
4. apply one envelope per transaction into a server-owned shard while atomically recording inbox, stream head, and ACK;
5. run one supervised Rust sender;
6. prove the fact-create path on disposable data and then a one-device canary;
7. keep every other mutation family out of scope or fail-closed until its own mutation-policy gate passes.

Complexity must pay rent. This closure deliberately rejects CRDTs, multi-master authority, TLS/mTLS over the already private loopback/SSH path, raw SQLite file replication, embedding replication, and broad mutation coverage before the first vertical slice is sound.

## 2. Evidence state at plan creation

| Surface | State | Evidence |
|---|---|---|
| Local legacy workers | **Contained** | `mnemes-syncd.service` and `sm-sync-watcher.service` disabled/inactive; no residual processes |
| Worker resource behavior | **Failed** | each reached the 1,024-FD ceiling through leaked SQLite handles |
| Canonical Mnemes source | **Patched/tested locally** | both legacy routes point to an unconditional `501 SYNC_DISABLED` handler; focused RED→GREEN test passed |
| Mnemes local suite | **Verified** | 125 tests passed; strict Clippy failures were fixed; focused server/replication tests and strict Clippy pass |
| UNO authority artifact | **Unsafe at capture** | running AArch64 binary still exposes `/v1/sync` and `/v1/sync/facts`; hash `11e682…732` |
| UNO transport | **Verified** | admitted account is `arduino` over SSH port 2222 with pinned key; service is `mnemes-authority.service`, loopback port 1738 |
| ARM containment source | **Staged** | local and remote normalized source archive SHA-256 both `2ace88…f91` |
| ARM containment build | **In progress** | isolated native build directory; no running-service mutation before build/test passes |
| semantic-memory source | **Prototype only** | schema 37, mutable post-open identity, `MAX(sequence)+1`, no digest chain, changed-payload retry accepted |
| semantic-memory MCP runtime | **Stale** | installed 0.5.5 lacks Mnemes flags; active journal contains zero rows |
| Data parity | **Degraded/unproved** | source 1,057 facts; existing UNO mirror 1,056 facts; no ID/content/provenance parity proof |
| Agent Graph council | **Advisory only** | useful protocol recommendation but one empty lane, invented provenance content, and volatile unkeyed receipt |

## 3. Governing invariants

### Authority

- The device semantic-memory SQLite database is primary.
- UNO Q is a read-only query replica except through the private admitted apply capability.
- No replica mutation is promoted back to the device.
- No content-based deduplication substitutes for operation identity.

### Stream

- Stream identity is `(home_device_id, store_id, stream_epoch)`.
- Writer identity/configuration is immutable for a store lifetime.
- Sequence starts at 1 and advances only as part of the mutation transaction.
- Every verified record binds exact payload bytes, operation/schema identifiers, predecessor digest, and envelope digest.
- Existing V37 rows are `legacy_unverified`; verified sequence starts at an explicit new epoch boundary.

### Failure semantics

- Same stream/sequence/digest: `Duplicate`.
- Same stream/sequence with a different digest or operation: `Fork`; no mutation or head advance.
- Sequence greater than expected: `Gap`; no mutation or head advance.
- Wrong predecessor: `Fork` or typed chain error; no mutation.
- Stale store/writer epoch: rejected.
- Unknown operation/schema/key/role: rejected.
- Apply failure: transaction rolls back semantic state, inbox, stream head, and ACK.
- Crash after commit and before sender observes ACK: retry returns the same durable ACK.

### Security

- Wire data never becomes SQL or a filesystem path.
- Bearer authentication identifies the calling device; the signed envelope must bind the same identity.
- The server maps that device to a server-owned shard path.
- Trusted signing keys are persistent, scoped, versioned, revocable, and lifecycle-bounded.
- Strict decode rejects unknown versions, unknown fields/kinds, duplicate fields, trailing bytes, and oversized payloads before allocation.
- Legacy routes remain disabled even after the secure route is enabled.

## 4. Scope and mutation policy

### Admitted in this closure

- `semantic_memory.fact.create.v1`
- Exact canonical fact ID, namespace, content, source, and metadata
- Embeddings, FTS, HNSW, sparse vectors, caches, and summaries are derived locally on the replica and are not transported as authority

### Explicitly not admitted

- fact update/delete or namespace deletion
- messages/sessions
- documents/chunks
- graph edges
- episodes
- procedural memories and skills
- provenance/support/governance records
- projection imports or derived projections
- credentials, key-registry mutations, and device administration

The production memory store must not be placed in a mode that implies complete replication. The first live proof uses a disposable canary store. Broader production activation requires a policy record and tests for every public mutation entry point.

## 5. Dependency-enforced implementation DAG

### Phase A — containment

**Current:** local worker containment complete; source patch complete; UNO deployment pending.

Deliverables:

- keep both Python workers disabled;
- preserve scripts and state as quarantine evidence, but remove them from supervision;
- legacy `/v1/sync` and `/v1/sync/facts` return `501 SYNC_DISABLED` before auth/body parsing;
- preserve old binary, service unit, credentials, data, and source hashes;
- do not delete the 1,056-row legacy replica.

Acceptance:

- no Python sync process;
- no descriptor growth from synchronization;
- malicious-looking raw SQL/path traversal bodies receive `501` and create no files/change no DB;
- UNO running PID hash equals the admitted containment artifact.

Rollback:

- restore the backed-up binary atomically only if the containment artifact breaks non-sync health/read paths;
- never restart either old worker as part of rollback.

### Phase B — canonical writer/outbox

Owner: `semantic-memory`.

Deliverables:

- explicit `Disabled` and honestly scoped `FactCreateRequired` replication modes;
- construction-time validated device/store/epoch; no mutable post-open identity;
- V38 stream state and verified outbox fields;
- domain-separated SHA-256 over length-prefixed fields and exact payload bytes;
- fact, FTS bookkeeping, derived-index work item, stream sequence/head, and outbox row in one SQLite transaction;
- typed export with empty/end/gap/corruption distinctions;
- same-sequence changed-payload conflict rejection;
- no external SQL/replay closure at the public replication boundary.

Acceptance:

- required mode without complete identity fails store open;
- disabled mode intentionally emits no outbox;
- first record has genesis predecessor;
- concurrent file-backed connections produce exactly `1..N` with no drops/duplicates;
- failed mutation/outbox append consumes no sequence;
- real `MemoryStore::add_fact` commits fact and one outbox row atomically;
- duplicate identical envelope is idempotent; changed envelope conflicts;
- full tests, format, check, and strict Clippy pass.

Rollback:

- schema additions remain readable; disable fact canary mode;
- never reinterpret verified V38 records as V37 or remove audit rows.

### Phase C — strict Mnemes admission

Owner: Mnemes transport/admission; semantic-memory owns typed replay.

Deliverables:

- one new route: `/v1/replication/sync`;
- strict versioned binary or closed JSON wire decoder with `deny_unknown_fields`, exact size limits, and canonical signature preimage;
- durable trusted-key registry in the pooled control DB;
- operator provisioning/revocation commands that never print private keys;
- authenticated device/signing principal/store bindings;
- server-owned shard catalog lookup;
- one-envelope apply transaction containing semantic replay, inbox identity/digest, stream head, replica state, and durable ACK;
- typed decisions: `Applied`, `Duplicate`, `Fork`, `Gap`, `StaleEpoch`, `Fenced`, `Unauthorized`, `Quarantined`, `Malformed`;
- true read-only replica query handle with no mutation-capable `MemoryStore` escape hatch.

Acceptance:

- key survives reopen; revocation is immediate/durable;
- embedded untrusted key is rejected;
- caller IDs never choose a path;
- raw SQL and old JSON journal bodies cannot decode;
- crash/retry and concurrent duplicate/fork tests pass;
- read-only handle cannot perform insert/update/delete;
- legacy route test remains green.

Rollback:

- disable only the new route and worker;
- preserve inbox/head/ACK evidence and candidate shard in quarantine;
- do not roll stream head backward in place.

### Phase D — one Rust sender

Deliverables:

- `mnemes-replication-worker` Rust binary;
- reads verified semantic-memory export API, never raw tables when an owner API exists;
- signs exact envelope with a device-owned Ed25519 key;
- batches conservatively but ACKs only a contiguous committed prefix;
- bounded exponential backoff with jitter and terminal quarantine classes;
- durable sender ACK projection; retry is safe;
- no Python execution or fallback;
- systemd hardening, restart policy, FD/resource limits, and structured logs.

Acceptance:

- fake `python3` in `PATH` cannot affect flow;
- restart resumes from durable ACK;
- no FD growth in a 30-minute soak;
- offline operation never blocks primary mutation;
- lag/error classes are observable.

Rollback:

- stop/disable the Rust worker; device primary continues local operation;
- preserve sender state and keys; never reactivate Python workers.

### Phase E — isolated end-to-end pilot

Use disposable device and UNO shard stores.

Test sequence:

1. create one fact through the real semantic-memory API;
2. observe one verified outbox record;
3. sign/deliver through the real worker and HTTP route;
4. verify exact fact ID/content/source/metadata and stream/receipt state;
5. retry identical envelope: `Duplicate`, no second fact;
6. alter same sequence: `Fork`, no state change;
7. skip sequence: `Gap`, no head advance;
8. stale epoch/key: reject;
9. kill after receiver commit before sender persists ACK: retry returns durable duplicate ACK;
10. restart both sides and repeat;
11. verify read/search results and rebuildable derived indexes;
12. remove disposable stores only after receipt capture.

Gate: no production canary until every step is reproduced locally and on UNO Q.

### Phase F — bootstrap and narrow canary

- Create an online SQLite backup of the canonical source after required journaling is active.
- Record snapshot digest, schema, stream epoch, outbox high-water sequence, and source artifact manifest.
- Install into a new server-owned shard; do not overwrite the legacy mirror.
- Rebuild derived indexes on UNO.
- Start stream delivery from the snapshot fence.
- Canary one device/store and fact-create only.

SLO targets—not current claims:

- zero silent gaps/forks/ACK overrun;
- acknowledged durability 100%;
- retry idempotency 100%;
- p95 freshness under 60 seconds, alert over 5 minutes;
- restart/reconnect recovery p95 under 2 minutes;
- no descriptor growth during 30-minute soak;
- normalized admitted fact/receipt parity 100%.

### Phase G — mutation-family expansion

For each family, require:

- canonical owner and payload schema;
- local atomic outbox binding;
- closed replica dispatcher;
- idempotency/conflict/gap tests;
- bootstrap semantics;
- deletion/redaction and rollback policy;
- public claim update.

No family is enabled by default because fact-create passes.

## 6. Build and deployment manifest

Every candidate must record:

- component/package/version;
- repository path, Git HEAD, dirty state, and scoped diff digest;
- normalized source archive digest;
- Cargo.toml/Cargo.lock digests and resolved semantic-memory revision;
- enabled features, target triple, libc, profile, Rust/Cargo versions;
- binary path, size, SHA-256, BuildID;
- service-unit and configuration digests;
- database schema ceiling and protocol version;
- signing key identifier/version only;
- test/format/check/Clippy/process/fault receipts;
- deployment host, timestamp, PID, executable path/hash;
- backup paths/hashes and exact rollback command.

No invented SBOM or reproducibility claim is permitted. Generate an SBOM only when a real tool output and stored artifact exist.

## 7. Host deployment sequence

1. Verify transport and host identity.
2. Build/test in an isolated source directory.
3. Capture artifact/source/toolchain manifest.
4. Capture service unit and running binary hash.
5. Stop the user service.
6. Use SQLite Online Backup API for each database that could be mutated by the candidate.
7. Copy old binary to a timestamped immutable backup.
8. Install candidate via same-filesystem temporary file + atomic rename.
9. Restart service.
10. Verify PID, executable path, running/on-disk hash, loopback binding, `/livez`, authenticated health/integrity, legacy route disablement, and existing MCP read behavior.
11. On failure, stop candidate, restore binary/unit/database generation as required, restart previous process, and verify hashes/health.

## 8. Final release gate

A closure statement must include:

- changed files;
- source revisions and dirty-state disposition;
- commands run and exact pass/fail/skip state;
- source/archive/binary/unit/database backup hashes;
- local and UNO process-boundary evidence;
- fact-create end-to-end receipt and normalized parity evidence;
- unresolved mutation families and risks;
- rollback instructions;
- a rerunnable auditor command/script.

Until Phases A–E pass, the only accurate status is **contained and under implementation**. After Phase F, the strongest accurate status is **one-device fact-create canary**. “Full synchronization” remains prohibited until Phase G covers every admitted truth-bearing mutation family.
