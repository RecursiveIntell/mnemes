# Mnemes Full-Surface Memory Mesh — Normative Implementation Plan and Specification

> **Status:** Proposed implementation specification; no implementation is claimed by this document.
> **15-second claim state:** BLOCKED/UNPROVEN until freshness-aware routing, query-visible watermarks, deadline isolation, fault tests, and the 24/72-hour certification soaks in this specification pass on the supervised device/server deployment.
> **Date:** 2026-07-30
> **Target repositories:** `/home/sikmindz/Coding/Libraries` and `/home/sikmindz/Coding/mnemes-replication`
> **Observed baselines:** `Libraries@fd6bdb7` with 184 dirty/untracked paths; `mnemes-replication@1181212` with 15 dirty/untracked paths. Re-establish and record exact baselines before editing.
> **Primary objective:** Every device remains the sole writer of its own local semantic-memory shard. Mnemes maintains a verified server replica of every shareable canonical memory/governance surface, normally no more than 15 seconds behind under explicitly healthy connected conditions, while local and federated search remain available and never wait on replication I/O.

---

## 1. Verdict

Build a **hub-and-spoke, device-primary/server-replica memory mesh**, not multi-master database replication and not byte-for-byte directory mirroring.

Each device owns one or more named primary stores. The server owns only verified replicas plus routing/control state. A server-hosted agent is treated as another device with its own primary shard; it never writes into another device's replica. Other devices and agents search remote memories through Mnemes federation rather than acquiring writable copies.

The complete design has three ordered component streams per `(device_id, store_id)`:

1. **Semantic stream** — all shareable SQLite-backed semantic-memory and governance operations, recorded atomically with the source mutation.
2. **Claim stream** — the append-only `claim-ledger` sequence, including verified snapshot/retained-tail compaction semantics.
3. **Blob stream** — content-addressed evidence bundles and oversized document/artifact bodies referenced by semantic or claim operations.

Derived indexes and caches are rebuilt or safely reused by fingerprint. They are never authority. Search receipts, replay inputs, adaptive routing state, local sync state, WAL/SHM files, and ANN sidecars are device-local/runtime projections and are not replicated as shared truth.

### Why this is the minimum correct architecture

- SQLite page replication would copy derived state, runtime receipts, and device-local policy while bypassing semantic-memory's owner APIs.
- Row-by-row table replication would break composite invariants across authority, forgetting, documents/chunks/episodes, projection imports, and procedural policy.
- Multi-master writes would introduce conflict semantics that the current store does not own and that are unnecessary for cross-device recall.
- Synchronous server-side embedding on the ACK path makes a hard latency objective dependent on an external/slow model.
- Replicating ANN sidecars would couple devices to provider, model, dimensions, library version, and platform-specific index layouts.

---

## 2. Evidence baseline and current gaps

### 2.1 Observed live local store

Read-only inspection on 2026-07-30 found:

- Memory directory: `/home/sikmindz/.hermes/semantic-memory.db/`
- Canonical SQLite file: `memory.db`
- Schema version: `V39`
- 78 tables
- Representative live rows: 1,118 facts; 26 documents; 684 chunks; 3 sessions; 139 messages; 980 graph edges; 496 authority lineages; 496 authority versions; 496 authority receipts; 530 operation-journal entries; 10 provenance records; 1 forgotten-fact tombstone; 1 origin revocation.
- Additional durable families exist even when currently empty: projection imports/rows, procedures/lifecycle receipts, shadow policies/promotions, evidence/authority transition rows, and forgetting closures.
- Sidecars include ANN files and `recall-admission.jsonl`; `semantic-memory-mcp` can additionally own `claim-ledger.jsonl` or a verified manifest-selected snapshot/tail generation and `evidence_bundles.jsonl`.

### 2.2 Current useful implementation

Reuse these sources rather than replacing them:

- `/home/sikmindz/Coding/Libraries/semantic-memory/src/journal.rs`
  - `FactCreatePayloadV1`
  - deterministic payload/envelope digests
  - verified contiguous export
  - inbox/head checks and idempotent `fact.create` apply
- `/home/sikmindz/Coding/Libraries/semantic-memory/src/knowledge.rs`
  - source-side `fact.create` append and receiver-side semantic apply
- `/home/sikmindz/Coding/mnemes-replication/src/replication/types.rs`
  - `MemoryMutationEnvelopeV1`, closed signer/artifact enums, fixed-order signing preimage
- `/home/sikmindz/Coding/mnemes-replication/src/replication/canonical.rs`
  - canonical digest and signature validation boundary
- `/home/sikmindz/Coding/mnemes-replication/src/replication/state_machine.rs`
  - `ReplicaState` and `ReplicaWatermarkV1`
- `/home/sikmindz/Coding/mnemes-replication/src/replication/fact_create.rs`
  - strict bounded JSON projection, exact signing preimage, Ed25519 verification
- `/home/sikmindz/Coding/mnemes-replication/src/store.rs`
  - trusted admission records, deterministic device shard paths, shared configured embedder, cached per-device `MemoryStore`, routed search, durable V1 ACKs
- `/home/sikmindz/Coding/mnemes-replication/src/shards.rs`
  - device-shard catalog, active-state mask, deterministic routing and merge
- `/home/sikmindz/Coding/mnemes-replication/docs/adr/ADR-0001-historical-bootstrap-and-live-epoch-rotation-v1.md`
  - adopted bootstrap safety principles and epoch rotation requirement

### 2.3 Current blocking gaps

1. Only `fact.create` has a production replay payload/apply path. Other public mutators do not append replayable operations.
2. `/v1/sync` remains disabled; `sync.rs::export_operation_journal` correctly refuses to invent payloads. Do not re-enable the raw-SQL legacy path.
3. The generic `MemoryMutationEnvelopeV1` is not a strict wire decoder and is not connected to semantic-memory's owner apply API.
4. V1 accepts exactly one fact-create entry and has fact-specific admission/ACK tables.
5. Receiver fact apply performs embedding before semantic commit/ACK. That cannot be a dependency of the 15-second canonical convergence objective.
6. Historical bootstrap is specified for the first vertical slice, not all canonical families or claim/evidence state.
7. `semantic-memory-mcp` claim integration owns a second durable truth surface outside SQLite. `sm_create_claim` and `sm_judge_support` append claim-ledger events; `sm_add_evidence` appends a separate JSONL file.
8. No persistent sender/spool, catch-up worker, multi-stream watermark, heartbeat, lag state, or connected-state SLO monitor exists.
9. Existing worktrees are materially dirty. Implementation must not be performed in-place without first isolating/recording the baseline.
10. Agent Graph advisory review was unavailable: the configured cloud model returned HTTP 403, while the local Ollama alias was routed to OpenRouter and rejected. Run receipt `e549844f-e2b9-49cb-bc24-e9c11c3b4208` is failed evidence, not a council result.
11. Current design documents explicitly remain pre-production, and the active shard catalog exposes generation/count/refresh fields but not source/applied heads, event lag, schema generation, embedding profile, or replica state. A stale shard can therefore remain route-eligible today.
12. Current merge behavior can turn one cross-shard ID/content conflict into a global routed-search failure, and bounded sparse shard selection can produce a stale false-negative unless incompleteness is explicit.

---

## 3. Governing invariants

These are release-blocking, not aspirations.

1. **One canonical writer per stream epoch.** A device primary is the sole writer of its store's semantic and claim streams. Writer transfer requires an explicit fenced epoch change.
2. **No remote writes into another device's replica.** Server agents receive their own primary identity/store.
3. **Owner APIs only.** Mnemes never writes semantic-memory canonical tables or claim-ledger files directly.
4. **Atomic source mutation + semantic journal.** Every shareable SQLite-backed canonical mutation and its exact replay operation commit in one SQLite transaction.
5. **At-least-once delivery, exactly-once semantic effect.** Duplicate transport is normal; `(stream_id, epoch, sequence, digest)` makes apply idempotent. Same identity with a different digest is a fork and quarantines the stream.
6. **Strict contiguous order per stream.** A gap blocks later entries in that stream. Independent device/store/component streams may progress in parallel.
7. **No silent scope widening.** Device, store, namespace/scope, principal, role, key version, validity window, epoch, fencing token, operation family, and payload schema are admitted explicitly.
8. **Canonical ACK only after durable semantic effect.** An ACK is generated from the receiver's durable inbox/head after owner apply. If the process dies after semantic apply but before control ACK persistence, retry reconstructs the same ACK.
9. **No embedding or ANN dependency for canonical ACK.** Expensive derived work occurs after canonical/FTS commit. Source-produced artifacts may be stored during apply only after profile/digest validation.
10. **Search never waits on replication.** Local search has no network call. Federated search uses bounded concurrent shard calls and returns partial/degraded results with freshness evidence.
11. **Derived state stays rebuildable.** FTS mappings, ANN/usearch/HNSW state, q8/sparse/compressed vectors, routing models, caches, pending-index queues, and receipt indexes are not independent truth.
12. **Forgetting cannot resurrect.** Governed forgetting emits durable tombstone/closure operations; bootstrap and replay preserve tombstones and reject stale resurrection.
13. **Unknown versions fail closed.** No fallback from V2 to legacy sync, no untyped passthrough, and no raw SQL compatibility adapter.
14. **Schema/coverage are explicit.** Every stream carries semantic schema generation and mutation-coverage version. A receiver that cannot apply either quarantines before state change.
15. **Canonical convergence is proved by roots.** Periodic deterministic family roots compare source and replica while excluding explicitly local/derived tables.

---

## 4. Scope taxonomy

“Full surface” means every shareable canonical semantic/governance capability, not every byte in the memory directory.

### 4.1 Tier A — canonical and live-replicated

| Family | Canonical content | Required operation families |
|---|---|---|
| Facts and authority | facts, immutable origin labels/revocations, authority lineages/versions/receipts, transition records, semantic operation journal | `fact.create`, `authority.append`, `authority.supersede`, `authority.redact`, `authority.revoke_origin`, `authority.forget` |
| Documents | document metadata, stable chunk IDs/order/text/token counts, source metadata | `document.ingest`, `document.ingest_manifest`, `document.delete_governed` |
| Conversations | session identity/metadata/channel, message identity/order/role/content/token metadata | `session.create`, `session.rename`, `message.append`, `session.delete_governed` |
| Episodes | stable episode identity, document relation, cause IDs, effect/outcome/confidence/verification state and history | `episode.create`, `episode.update`, `episode.outcome` |
| Knowledge graph | typed edges and append-only invalidation/supersession state | `graph_edge.add`, `graph_edge.invalidate` |
| Projection lane | claim/relation versions, aliases, evidence references, episode links, derivation edges, admitted/failure import receipts | `projection.import_batch`, `projection.failure_record` |
| Provenance | append-only provenance values/support chains/receipts | `provenance.set`, `provenance.combine` |
| Forgetting | forgotten facts, closure receipts, artifact invalidations, revocations, affected derivations | `forgetting.apply_closure` |
| Procedural memory | immutable artifacts, derivation links, lifecycle events and receipts | `procedure.compile`, `procedure.test`, `procedure.promote`, `procedure.quarantine`, `procedure.revoke`, `procedure.rollback` |
| Shadow policy | proposals, immutable versions, active pointer changes, promotion/rollback receipts | `shadow.submit`, `shadow.promote`, `shadow.rollback` |

### 4.2 Tier B — independent canonical component streams

| Component | Owner | Replication rule |
|---|---|---|
| Claim/trust ledger | `semantic-memory-mcp` + `claim-ledger` | Replicate exact verified ledger entries by sequence/head. Bootstrap from a verified snapshot + retained tail. Preserve compaction receipts and trusted-head binding. |
| Evidence bundles | `semantic-memory-mcp` | Replace append-only `evidence_bundles.jsonl` as the live authority path with immutable content-addressed objects. Fsync/rename object first, then append an `EvidenceAttached` ledger event referencing its digest. Orphan blobs are safe; missing referenced blobs fail closed. |
| Oversized document/artifact bodies | semantic-memory replication owner | Upload immutable digest-addressed chunks before the referring operation; apply only after all dependencies verify. |

### 4.3 Tier C — derived but search-critical

These may travel as signed artifacts to accelerate search, but a receiver can discard and rebuild them:

- f32 embeddings
- q8 vectors
- sparse vectors/representations
- late-interaction vectors
- compressed vector artifacts and generations
- FTS row maps/index rows
- ANN/usearch/HNSW sidecars and manifests
- pending index operations

Every reusable artifact carries `EmbeddingProfileV1 { provider, model, revision, dimensions, normalization, tokenizer_digest }`, item ID, content digest, artifact digest, and producer version. The receiver reuses it only on exact profile/content match. Otherwise it schedules local derivation.

### 4.4 Tier D — intentionally device/server-local

Do not include these in canonical convergence roots or replay them as shared memory:

- `search_receipts`
- privacy-sensitive `replay_inputs`
- learned `routing_policy` and local outcome labels
- `sync_state`, sender spool, receiver control ACK tables
- WAL/SHM files
- ANN sidecars themselves
- in-memory claim trust index
- server-generated federated-search receipts
- local admission/audit logs such as `recall-admission.jsonl` (archive/witness separately if required)

Reason: these describe local execution, privacy choices, delivery state, or rebuildable acceleration. Replicating them would create feedback loops, privacy leakage, or false byte-equality expectations.

### 4.5 Admin-only mutation rule

`update_fact` and `delete_namespace` are physically mutating admin operations. When replication enforcement is enabled:

- ordinary fact updates become append/supersede operations;
- namespace deletion becomes one signed, idempotent `forgetting.closure` with namespace scope and a deterministic affected-ID manifest—not thousands of row deletes;
- only a genuine schema/data migration may use an explicit migration permit and typed `admin.migration` closure.

No unjournaled admin operation may succeed in replicated mode.

Every tombstone carries entity kind/ID, prior content digest, reason, closure ID, recorded/valid time, and authorizing receipt digest. Physical garbage collection is allowed only after every admitted replica has acknowledged the tombstone, an independently verified recovery point covers it, and retention policy permits purge.

---

## 5. Protocol design

### 5.1 Reuse and evolve the existing generic core

Do not create a third envelope hierarchy.

- Preserve `MemoryMutationEnvelopeV1` and V1 fact transport for compatibility tests.
- Add `MemoryMutationEnvelopeV2` in `mnemes-replication/src/replication/types.rs` with explicit `stream_id`, `stream_kind`, dependency digests, source commit time, and mutation coverage version.
- Reuse the existing fixed-order, length-prefixed, domain-separated signing/digest strategy in `canonical.rs`.
- Add a strict bounded decoder in `replication/wire_v2.rs`; serde JSON remains only a projection/debug format.
- Generalize the fact batch into `SignedMutationBatchV2` with bounded contiguous entries and no payload reserialization. Each entry binds exact journal bytes; the batch binds ordering and admission fields.
- Add `ReplicationStreamId` and `BootstrapId` to `stack-ids`; reuse `ContentDigest`, `ScopeKey`, and `TraceCtx`.
- Generate committed JSON schemas through `contract-schema-gen` and gate compatibility in CI.

Required V2 identity:

```text
(device_id, store_id, stream_id, stream_epoch, sequence)
```

Required V2 digest chain:

```text
entry_digest = H(domain || identity || operation_kind || payload_schema ||
                 payload_digest || predecessor_digest || dependency_digests ||
                 semantic_schema_generation || mutation_coverage_version)
```

The signature covers the entry digest plus principal, role, key version, observed/source-commit time, validity window, and fencing token.

### 5.2 Stream kinds

```rust
enum ReplicationStreamKindV2 {
    Semantic,
    ClaimLedger,
    BlobManifest,
}
```

Each kind has an independent epoch, sequence, predecessor digest, ACK watermark, health state, and quarantine state. A stale claim stream must not stall semantic content apply; it degrades trust enrichment only. A missing blob dependency blocks only referring operations.

### 5.3 Batch limits

Initial production limits:

- max 64 entries
- max 1 MiB encoded batch body for ordinary operations
- max 256 KiB inline payload per entry
- larger bodies use 1 MiB content-addressed blob chunks, up to a configured operation total
- max 32 in-flight blob chunks per device
- per-stream ordering remains serial; different device/store streams run concurrently

Reject before signature/admission work if bounds fail.

### 5.4 Sender spool

Create a dedicated SQLite spool in Mnemes, not a second semantic outbox:

```text
streams(stream_id, epoch, source_head, ack_head, ack_digest, state, heartbeat_at, ...)
batches(batch_id, stream_id, epoch, first_seq, last_seq, exact_bytes, digest,
        status, attempts, next_retry_at, created_at, last_error)
blob_objects(digest, path, size, status, attempts, next_retry_at, ...)
leases(stream_id, worker_id, lease_until, heartbeat_at)
```

Rules:

- Semantic-memory's operation journal is the durable source outbox.
- The spool stores exact signed delivery bytes and retry state only; it is rebuildable from source journal + receiver ACK.
- A deterministic batch ID derives from stream identity, sequence range, and batch digest.
- Retries resend exact bytes. Never regenerate/sign a new payload for the same batch ID.
- Persist receiver ACK before advancing/deleting spool state.
- Compact only entries at or below a durable contiguous ACK and retain a configurable forensic tail.
- Compaction additionally requires receiver recovery evidence and a separately verified local backup/checkpoint covering the same head; HTTP success or ACK alone is insufficient.
- If the spool is lost, query receiver status, rebuild from the owner journal, and safely resend duplicates.

`job-queue` is **not** the primary spool: it has useful SQLite leases/retries, but it does not own contiguous stream watermarks, exact-byte replay, fork semantics, or ACK recovery. Reuse its tested lease/backoff ideas or small utilities only if this avoids copying code without importing its job semantics.

### 5.5 Receiver apply

Generalize `apply_fact_create_request` into a per-stream receiver worker:

1. Decode strict wire bytes and enforce size/version bounds.
2. Validate payload digest, predecessor chain, and entry/batch signatures.
3. Resolve trusted key admission; validate device/store/stream/scope/role/key window/epoch/fence.
4. Check existing receiver inbox/head.
5. On exact duplicate, return the durable prior outcome.
6. On gap, report expected/received and block the stream.
7. On same identity/different digest or predecessor fork, quarantine.
8. Verify all blob dependencies.
9. Dispatch to semantic-memory or claim-ledger owner apply API.
10. Owner transaction commits semantic effect + receiver inbox/head + FTS/pending-derived markers.
11. Construct ACK from durable receiver state.
12. Persist control-plane ACK/freshness. If this last step crashes, retry reconstructs the same ACK from owner state.

Do not claim cross-database atomicity between a shard database and the Mnemes control DB. Correctness comes from owner-side idempotent apply and ACK reconstruction.

### 5.6 Closed mutation gate

Add a `ReplicationTxn` owner abstraction in semantic-memory:

```rust
transact_replicated(context, operation, |tx| apply_semantic_effect(tx))
```

It must:

- validate the typed payload before the transaction
- allocate the next stream sequence inside the transaction
- apply all canonical rows and transactional FTS/pending-derived rows
- append exact payload/digests to `operation_journal`
- commit or roll back all of the above together

Add a connection-local replication context function and triggers on every Tier-A table. In replicated mode, a direct INSERT/UPDATE/DELETE without `LocalAuthoring`, `ReplicaApply`, `BootstrapApply`, or `MigrationPermit` aborts. Tests enumerate the canonical table allowlist and prove naked SQL cannot bypass journaling.

Receiver apply uses `ReplicaApply` and does not append a new outbound source event. Bootstrap uses `BootstrapApply` against a new staged generation.

---

## 6. Bootstrap and anti-entropy

### 6.1 Logical, signed, multi-component snapshot

Add `MemoryMeshBootstrapManifestV1` with:

- device/store IDs
- bootstrap ID and target shard generation
- semantic schema and mutation-coverage versions
- source stream epochs/cuts/heads
- ordered semantic snapshot pages and Merkle/root digest
- claim-ledger verified snapshot/retained-tail/receipt/head
- evidence/blob manifest and roots
- embedding profile(s)
- created/expiry times
- recovery-authority signer/key version/signature

Semantic snapshot pages contain typed logical `CanonicalSnapshotRecordV1` values, not SQL and not SQLite pages. Reuse the same stable payload DTOs used by live operations. Page records are ordered by dependency:

1. store metadata and identities
2. facts, documents, sessions
3. chunks and messages
4. episodes/causes/links
5. graph/provenance/derivations
6. authority/origin/transition records
7. projection rows/import receipts
8. procedural artifacts/lifecycle
9. shadow proposals/versions/receipts/active pointers
10. forgetting/tombstones/revocations

### 6.2 Bootstrap algorithm

1. Enter a short owner-coordinated bootstrap barrier; drain active mutators.
2. Record semantic, claim, and blob component cuts and heads.
3. Export deterministic pages and component manifests through owner APIs.
4. Release the barrier immediately; live writes continue in the old epoch/journal.
5. Upload to a new server staging directory.
6. Verify signatures, page chains, roots, schema/coverage versions, claim compaction, blobs, referential closure, and forgetting closure.
7. Apply to a fresh staged `MemoryStore` using `BootstrapApply`.
8. Run semantic-memory full integrity plus deterministic family-root comparison.
9. Build/reconcile FTS and derived indexes without making the shard active.
10. Atomically promote the staged shard generation in the Mnemes catalog.
11. Rotate source live stream epoch, bind the new epoch to the snapshot cut/root, and catch up mutations committed after the barrier.
12. Mark routed search eligible only after catch-up and freshness gates pass.
13. Retain the previous server generation for rollback; never overwrite it in place.

### 6.3 Claim/evidence bootstrap

- Verify the current claim-ledger trusted head or manifest-selected snapshot + retained tail before export.
- Copy immutable evidence objects by digest and verify every referenced object.
- If a lagged receiver asks for a claim sequence compacted out of the active tail, send a verified claim checkpoint rather than fabricating old entries.
- A claim referring to an absent fact is allowed only when a corresponding governed forgetting/tombstone state explains the absence. Otherwise bootstrap fails and is retried from a coherent barrier.

### 6.4 Periodic anti-entropy

Every five minutes in healthy state:

- source signs semantic family roots and component heads
- receiver computes the same roots over the canonical allowlist
- compare family-by-family, not one opaque database hash
- on mismatch: stop stream promotion, retain local search, quarantine affected remote shard from federation, and request bounded family repair or a fresh staged bootstrap

Never repair by blindly replacing the active shard.

---

## 7. The 15-second contract

### 7.1 Exact definition

For an eligible semantic operation, measure both:

```text
canonical_replication_lag =
    sender_clock_time_when_durable_ACK_is_observed
    - sender_clock_time_when(source canonical rows + journal entry committed)

query_visibility_lag =
    sender_clock_time_when(receiver reports the entry query-visible and
                           the freshness catalog publishes that watermark)
    - sender_clock_time_when(source canonical rows + journal entry committed)
```

Both timestamps are observed on the source device, avoiding cross-device clock-skew errors. Receiver latency and server timestamps are separate diagnostics.

The user-facing convergence claim is based on `query_visibility_lag`, not request acceptance or ACK alone. “Query-visible” means canonical text/metadata and FTS are visible; when a valid matching source embedding is supplied, vector visibility is provided by the recent-applied exact overlay or ANN. If no compatible embedding exists, the item is explicitly `lexical_only/vector_pending`, and the shard cannot claim full semantic-vector currency.

The contract is:

- **Hard operational objective:** every eligible operation becomes query-visible in `<= 15s` while the stream is `ConnectedHealthy`.
- **Warning:** oldest unacked eligible operation reaches 10s.
- **Degraded:** it reaches 15s; state immediately becomes `DegradedLag`, emits an alert/receipt, and search freshness exposes the breach.
- **Certification:** 72-hour soak with zero eligible breaches, zero lost operations, zero unexplained root mismatch, and p99.9 below 10s.

Do not market this as an unconditional distributed-systems guarantee. The bound excludes explicitly surfaced states: device suspended/offline, partition/TLS/tunnel unavailable, key revoked/expired, stream quarantined/forked/gapped, receiver disk full/read-only, bootstrap in progress, or a bulk operation exceeding the eligible size/network envelope.

Initial eligible envelope:

- operation plus required blobs `<= 8 MiB`
- effective source-to-server throughput `>= 10 Mbit/s`
- network RTT `<= 100ms` and packet loss `<= 1%`
- disk free `>= 20%` and p99 fsync `<= 100ms`
- receiver queue depth below the certified threshold (initially 1,000 entries)
- no migration, backup, vacuum, re-embedding, or recovery operation on the active shard
- matching schema, key, and admitted embedding-profile versions
- no operator pause/revocation/quarantine

Larger transfers are `BulkTransfer`, expose progress, preserve RPO 0, and do not silently count as healthy.

### 7.2 Latency budget

| Stage | Maximum healthy budget |
|---|---:|
| source commit detection (1s poll fallback; immediate in-process notify preferred) | 1.0s |
| export/spool/sign | 1.0s |
| local queue + transport | 3.0s |
| strict decode/signature/admission | 1.0s |
| owner canonical/FTS apply | 3.0s |
| durable ACK return/persist | 1.0s |
| headroom/retry jitter | 5.0s |
| **Total** | **15.0s** |

Implementation rules:

- poll source head at 1s maximum interval; optional post-commit notification is an optimization, never correctness
- heartbeat every 5s with signed source head and component heads
- retry backoff 250ms, 500ms, 1s, 2s while an entry is under the 15s deadline; cap at 5s only after state is already degraded
- request timeouts and retries are deadline-aware
- bounded per-device streams prevent one device/document from starving others

### 7.3 Freshness model

Persist the following in the routing catalog, not just metrics:

- store epoch and writer epoch
- source/owner-reported sequence and receiver-applied sequence
- applied head digest and event lag
- last sync time and last signed owner-report time
- semantic schema generation and mutation-coverage version
- embedding profile digest
- routing-summary base sequence and summary digest
- replica state, lexical-visible sequence, and ANN-indexed sequence

Bind every routing summary to its `summary_base_sequence`; a summary cannot route as current beyond the canonical sequence from which it was derived.

Expose separately:

- semantic source head / receiver ACK head / sequence gap
- oldest unacked semantic age
- claim source head / receiver head / trust lag
- blob dependency count/bytes
- receiver applied sequence / lexical-visible sequence / ANN-indexed sequence
- last signed heartbeat age
- canonical root status and last anti-entropy time

A device with no new mutations is fresh only while signed heartbeats confirm its source head has not advanced.

Initial classes:

- `Current`: owner heartbeat age `<= 5s`, no blocked gap, and query-visible head equals owner-reported head
- `Lagging`: healthy but behind, with oldest eligible mutation `> 5s` and `<= 15s`
- `OfflineUsable`: owner unavailable and last verified replica still within explicit caller policy
- `Stale`: `> 15s`; excluded from strict/default requests
- `Blocked`/`Quarantined`: never queryable

---

## 8. Search isolation and parity

### 8.1 Local search

- The local `semantic-memory` search API never calls Mnemes or reads the sender spool.
- Journaling adds only one indexed append and small digest work inside existing mutation transactions.
- Replication workers use separate read connections and short transactions under WAL.
- Acceptance: local p95 search latency regression `<= 5%`, p99 regression `<= 10%`, and no replication-network frame in a local-search trace.

### 8.2 Server/federated search

Preserve the current architecture:

- per-device `Arc<MemoryStore>` cache owned by `MnemesStore`
- bounded concurrent shard queries
- active/quarantined/revoked policy mask
- deterministic merge and conflict detection, revised so one conflicting item/shard is isolated and reported rather than aborting every healthy result
- partial results rather than global failure

Extend every `RoutedSearchResult`/receipt with:

- device/store/shard generation
- semantic freshness state and ACK head
- trust freshness state/head
- lexical and ANN indexed heads
- degradation reason codes
- `incomplete` plus selected, skipped, failed, stale, and timed-out shard IDs

Default policy:

- query `Current` shards normally
- include `Lagging` shards only within a configurable grace period and mark results stale
- exclude `GapDetected`, `Quarantined`, `Revoked`, bootstrap-staging, and root-mismatch shards
- exhaustive mode may include explicitly permitted stale shards but cannot hide status

If any potentially relevant shard is stale, unavailable, skipped by budget, timed out, or quarantined, the response must set `incomplete=true`. An empty incomplete response is **not** authoritative “not found.” Shard selection must use freshness-bound summaries; sparse-routing fallback and strict/exhaustive modes remain available so routing heuristics cannot silently establish absence.

Federated search has a bounded p99 deadline (initial certification target: `<= 2s`). Slow shards are cut off, identified, and excluded from the completion claim; they do not consume permits needed by healthy shards or local search.

### 8.3 Canonical ACK vs vector readiness

Receiver apply must synchronously commit canonical text/metadata and transactional FTS state, then ACK. Embeddings/ANN are decoupled:

1. If the operation carries a source-produced embedding artifact with exact profile/content/digest match, store it without invoking a model.
2. Queue ANN insertion durably.
3. Until ANN insertion catches up, merge ANN candidates with an exact bounded scan of rows in `(indexed_sequence, applied_sequence]`.
4. If no valid item embedding exists yet, lexical/FTS remains available and the result receipt says `semantic_vector_pending`.
5. If pending exact-tail rows exceed the bounded threshold, prioritize the index worker and expose degradation; never block all search on a global rebuild.

Acceptance:

- a newly applied text item is lexical-visible by the canonical ACK
- a valid carried embedding is semantically visible through exact-tail overlay before ANN catches up
- ANN outage does not block canonical replication or lexical search
- embedder outage does not block canonical replication
- server search cache keys include shard generation and canonical content generation, preventing stale cache hits after apply
- catalog query-visible watermark advances only after the exact-tail/ANN/FTS visibility state it reports is durable

---

## 9. Libraries reuse matrix

| Library/capability | Decision | Use |
|---|---|---|
| `semantic-memory` | **Mandatory owner** | canonical mutation/apply/snapshot/root/index APIs; no raw Mnemes SQL |
| `semantic-memory-mcp` | **Mandatory owner for claim integration** | claim/evidence durable publish and component export/apply |
| `claim-ledger` | **Mandatory** | exact ledger events, sequence/head verification, snapshot + retained-tail compaction |
| `stack-ids` | **Mandatory** | `ContentDigest`, `ScopeKey`, `TraceCtx`; add replication stream/bootstrap ID newtypes |
| `semantic-memory::vector_snapshot` | **Mandatory seam for optional vector transfer** | reuse deterministic embedding snapshot rows/source digest/profile validation; snapshots remain discardable derived artifacts, not semantic authority |
| existing Mnemes replication core | **Mandatory** | Ed25519 roles/admission, fixed preimages, canonical digest, watermark/state machine, shard routing |
| `contract-schema-gen` | **Mandatory release-tool gate, never runtime dependency** | generate/check committed V2 mutation/batch/ACK/bootstrap/heartbeat schemas outside deployed crates |
| `bitemporal-runtime` | **Preserve current use** | semantic valid/recorded-time contracts; not transport ordering |
| `boundary-compiler` | **Preserve existing authority use** | transition/authority policy only; no new sync ownership |
| `job-queue` | **Selective reuse only** | lease/backoff patterns/utilities; do not use as canonical sender spool |
| `continuity-runtime` | **Optional reporting** | SLO profile, incident/forensic freeze, recovery-plan artifacts after core path works |
| `attestation-exchange` | **Optional later** | external bootstrap/transparency packaging; existing Ed25519 admission remains operational authority |
| `authority-delegation` | **Reject from hot path** | typed governance vocabulary only; no need to duplicate current scoped key/fence admission |
| `remote-oracle-admission` | **Reject** | unrelated remote-oracle trust semantics |
| compressed vector crates / ANN sidecars | **Derived lane only** | optional search acceleration after exact profile/digest admission; never canonical sync |
| legacy `replica.rs`/raw `sync.rs` dispatch | **Reject** | bypasses owner semantics and lacks full payload/export contracts |

Complexity must pay rent. Do not import a typed artifact crate merely because its names resemble a replication concern.

---

## 10. File-level implementation plan

### Phase 0 — isolate baseline and freeze contracts (2–3 engineer-days)

**No code mutation before this gate.**

- [ ] Record `git status --short`, HEAD, Rust toolchain, enabled features, schema version, current canonical family roots, and installed/runtime binary digests for both repositories.
- [ ] Create clean dedicated worktrees from an explicitly selected base; preserve existing dirty trees untouched.
- [ ] Copy this plan into the implementation worktree and record any source drift since `fd6bdb7` / `1181212`.
- [ ] Add a complete public-mutator/canonical-table coverage matrix to the plan or an adjacent generated test fixture.
- [ ] Freeze V2 wire/preimage/schema fields and operation-family enum before implementation fan-out.

**Gate 0:** reviewer signs off that every shareable mutator and non-SQLite owner is classified; no `TBD` operation family remains.

### Phase 1 — IDs, schemas, strict protocol core (4–6 days)

**Libraries**

- [ ] Modify `/home/sikmindz/Coding/Libraries/stack-ids/src/lib.rs` (and owning ID module) to add `ReplicationStreamId` and `BootstrapId` only; do not duplicate `ContentDigest`, `ScopeKey`, or `TraceCtx`.
- [ ] Add replication schema exports to `/home/sikmindz/Coding/Libraries/contract-schema-gen/src/lib.rs` and committed schemas under `/home/sikmindz/Coding/Libraries/schemas/`.

**Mnemes**

- [ ] Extend `src/replication/types.rs` with V2 stream-aware types while retaining V1 compatibility.
- [ ] Extend `src/replication/canonical.rs` with V2 fixed-order preimage/digest.
- [ ] Add `src/replication/wire_v2.rs` for strict bounded decode/encode and unknown/trailing-byte rejection.
- [ ] Extend `src/replication/state_machine.rs` with explicit sender/receiver states and per-component watermarks.
- [ ] Add `src/replication/batch_v2.rs`, `ack_v2.rs`, `heartbeat_v1.rs`, and `admission_v2.rs`.

**Tests**

- [ ] golden preimage/digest/signature vectors
- [ ] truncation/overflow/unknown enum/trailing bytes
- [ ] wrong scope/role/key/version/epoch/fence
- [ ] duplicate vs identity collision/fork
- [ ] schema-generation and mutation-coverage rejection

**Gate 1:** independent Rust and non-Rust decoder fixtures produce identical digests; committed schemas match generated output.

### Phase 2 — semantic-memory closed mutation framework (5–7 days)

- [ ] Extend `semantic-memory/src/journal.rs`; do not create a second semantic outbox.
- [ ] Add:
  - `semantic-memory/src/replication/mod.rs`
  - `operation.rs`
  - `guard.rs`
  - `snapshot.rs`
  - `state_digest.rs`
  - `artifact.rs`
- [ ] Extend `semantic-memory/src/db.rs` with V40+ migrations for operation family/version/dependency/source-commit metadata and canonical content generation.
- [ ] Add `ReplicationTxn` and guarded table triggers/context.
- [ ] Add public owner APIs:
  - `export_verified_contiguous(stream, start, limit)`
  - `apply_verified_operation(envelope)`
  - `export_snapshot_pages(cut, options)`
  - `apply_snapshot_page(page, permit)`
  - `canonical_family_roots()`
  - `replication_status()`
- [ ] Keep current `FactCreatePayloadV1`/apply tests as compatibility fixtures.

**Gate 2:** every guarded canonical table rejects naked writes in replicated mode; source mutation and journal cannot diverge under injected transaction failures.

### Phase 3 — full SQLite mutation coverage (8–12 days)

Refactor each owner module to construct one typed operation and use `ReplicationTxn`:

- [ ] `knowledge.rs` — facts, authority-compatible supersession, governed delete paths
- [ ] `documents.rs` — document/chunk aggregate ingest/delete
- [ ] `conversation.rs` — session/message lifecycle
- [ ] `episodes.rs` — create/update/outcome and cause synchronization
- [ ] `graph_edges.rs` — add/invalidate
- [ ] `projection_lane.rs`, `projection_storage.rs`, `projection_import.rs` — atomic import/failure records
- [ ] `provenance.rs` — append/combine receipts
- [ ] `authority.rs`, `authority_contracts.rs`, `origin_authority.rs`, `transition_compiler.rs` — authority aggregate operations
- [ ] `forgetting.rs` — complete closure/tombstone operation
- [ ] `procedural_memory.rs` — immutable artifact + lifecycle event/receipt operations
- [ ] `shadow_policy.rs` — proposal/promotion/rollback aggregate operations
- [ ] `lib.rs` — ensure every public mutator routes through a covered owner method

**Gate 3:** for every operation family: source state → exported exact entry → empty receiver apply → canonical family roots equal; duplicate is a no-op; gap/fork/tamper fail before semantic change.

### Phase 4 — claim ledger and evidence objects (5–7 days)

- [ ] In `semantic-memory-mcp/src/server.rs`, centralize claim/evidence writes behind a `ClaimComponentStore`; no tool writes files directly.
- [ ] Convert evidence bundles to immutable digest-addressed files in a managed evidence directory.
- [ ] Fsync/atomic-rename evidence object before appending an `EvidenceAttached` claim-ledger event.
- [ ] Add verified contiguous claim export/status/checkpoint APIs and exact ledger-entry apply.
- [ ] Bind compaction manifests/snapshots/tails/receipts to component status and export.
- [ ] Add bootstrap validation for missing blobs, malformed ledger, wrong trusted head, and compaction while a receiver is behind.
- [ ] Keep ordinary semantic search available if claim verification fails; return explicit `trust_enrichment_unavailable`.

**Gate 4:** crash at every file/ledger boundary leaves either an unreferenced removable blob or a complete verified event; never a referenced missing blob. Source/receiver claim heads and trust projections match.

### Phase 5 — full bootstrap and generation promotion (5–8 days)

**semantic-memory**

- [ ] Implement deterministic logical pages and canonical family roots.
- [ ] Add dependency/forgetting/authority closure validator.

**Mnemes**

- [ ] Add `src/replication/bootstrap.rs` and control migrations for sessions/pages/generations/promotions.
- [ ] Change shard catalog paths to generation directories without breaking deterministic device ownership.
- [ ] Add routes:
  - `POST /v2/replication/bootstrap/manifests`
  - `PUT /v2/replication/bootstrap/{id}/pages/{n}`
  - `POST /v2/replication/bootstrap/{id}/verify`
  - `POST /v2/replication/bootstrap/{id}/promote`
- [ ] Promote the active catalog pointer atomically; retain prior generation.
- [ ] Bind live epoch rotation to snapshot cut/root.

**Gate 5:** bootstrap the complete current 78-table canonical subset plus claim/evidence state into a fresh shard; roots/heads match; concurrent writes after the cut catch up; a failed verify leaves active generation unchanged.

### Phase 6 — persistent sender and receiver workers (5–8 days)

- [ ] Add Mnemes client modules:
  - `src/client/config.rs`
  - `source.rs`
  - `spool.rs`
  - `transport.rs`
  - `worker.rs`
  - `status.rs`
- [ ] Add `src/bin/mnemes-sync-agent.rs`.
- [ ] Open source semantic-memory through a lightweight owner export handle, not direct SQL.
- [ ] Implement exact-byte spool, per-stream lease, batching, deadline-aware retry, ACK recovery, heartbeat, and offline catch-up.
- [ ] Add server generic mutation route: `POST /v2/replication/mutations`.
- [ ] Generalize fact-specific admission/ACK state to stream admission/receiver watermarks while retaining V1 tables/routes until deprecation.
- [ ] Parallelize across device/store streams; serialize within each stream.

**Gate 6:** kill/restart sender and receiver at every boundary; no lost semantic effect, no duplicate effect, exact ACK recovery, and offline catch-up to matching roots.

### Phase 7 — search-safe derived lane and freshness (4–6 days)

- [ ] Add embedding-profile registration/validation to device/store admission.
- [ ] Remove embedding model invocation from canonical receiver ACK path.
- [ ] Persist carried derived artifacts only on exact profile/content/digest match.
- [ ] Add durable ANN-indexed watermark and exact recent-tail overlay.
- [ ] Key/invalidate search caches by shard generation + canonical content generation.
- [ ] Extend routed results/receipts with freshness/trust/index heads and reason codes.
- [ ] Add current/lagging/quarantined route policy and bounded stale grace.

**Gate 7:** embedder and ANN outage do not block replication or lexical search; fresh valid embeddings remain semantically searchable before ANN catch-up; local search regression stays within budget.

### Phase 8 — hostile verification and rollout (6–10 days)

- [ ] Add conformance fixtures for every operation/snapshot family.
- [ ] Fault inject: gap, fork, tamper, wrong key/scope, revocation, expiry, stale fence, duplicate, reordered batch, unknown schema, malformed blob, disk full, read-only DB, process crash, partial network, slow receiver, compaction race, concurrent search/write.
- [ ] Backlog tests: 100k small mutations, 8 MiB eligible object, larger bulk transfer, multi-device contention.
- [ ] Run a 24-hour two-device connected soak, then a 72-hour multi-device soak, plus at least one 24-hour offline catch-up; include 5× normal mutation bursts and concurrent p95/p99 search load.
- [ ] Prove canonical family roots and claim heads at start/end and after restarts.
- [ ] Produce operator runbook, systemd units, metrics, alerts, key rotation/revocation, bootstrap/rollback commands, and evidence archive.

**Gate 8:** all acceptance criteria in §11 pass on real device/server paths. Only then mark V2 active and begin V1 deprecation.

---

## 11. Acceptance criteria

### Correctness

- [ ] 100% of Tier-A public mutators produce one typed journal operation in replicated mode.
- [ ] 100% of guarded Tier-A table writes are attributable to LocalAuthoring, ReplicaApply, BootstrapApply, or MigrationPermit.
- [ ] Source/replica canonical family roots match after bootstrap, live apply, restart, and offline catch-up.
- [ ] Same identity/same digest is idempotent; same identity/different digest quarantines.
- [ ] No applied sequence after a gap.
- [ ] Governed forgetting survives duplicate replay, old bootstrap, and reconnect without resurrection.
- [ ] Claim ledger verifies to the same trusted head; all referenced evidence blobs exist and match digest.
- [ ] Unknown operation/schema/version cannot change semantic state.

### Latency and availability

- [ ] 24/72-hour healthy-connected soaks: max eligible query-visibility lag `< 15s`, p99.9 `< 10s`; request acceptance or ACK alone does not count.
- [ ] Local search makes no network call and p95/p99 regressions stay within 5%/10%.
- [ ] Canonical/FTS apply and ACK succeed with embedder/ANN disabled.
- [ ] Valid source-produced embeddings are visible via exact-tail overlay before ANN catch-up.
- [ ] One slow/offline/quarantined shard does not prevent partial federated results from healthy shards.
- [ ] Freshness reason codes accurately reflect semantic, trust, blob, and index states.
- [ ] Any stale/skipped/failed/timed-out potentially relevant shard forces `incomplete=true`; empty incomplete output cannot be interpreted as authoritative “not found.”
- [ ] Federated routed search meets its bounded p99 deadline and isolates conflicting item/shard evidence without discarding healthy results.

### Operations/security

- [ ] Device/store/component stream admission is scoped and versioned.
- [ ] Key revocation and fencing take effect before apply; duplicates of previously admitted exact entries remain auditable without new effects.
- [ ] Server remains loopback-only behind the existing authenticated tunnel or an explicitly approved confidential transport; payload signatures are still enforced.
- [ ] Spool/database/file permissions protect memory content and key material; keys never enter logs or payload files.
- [ ] Old and staged shard generations have explicit retention/deletion policy.
- [ ] Rollback removes a bad shard generation from routing without touching device primary truth.

---

## 12. Observability

Required per device/store/component metrics:

- source head sequence/digest
- receiver ACK sequence/digest
- sequence gap and oldest unacked age
- signed heartbeat age
- spool entries/bytes/oldest age/retry count
- transport request latency and failures
- admission/apply/ACK latency
- canonical content generation
- lexical-visible and ANN-indexed sequence
- pending derived rows and oldest age
- claim head/trust lag
- missing blob count/bytes
- last canonical-root comparison and mismatched families
- shard state/generation/search timeout/error count

Required alerts:

- warning at 10s eligible lag
- degraded at 15s
- heartbeat missing 15s while expected online
- gap/fork/quarantine immediately
- root mismatch immediately
- disk free below safe threshold
- pending derived tail above bound
- claim verification disabled
- spool growth/backlog beyond configured limits

Every state transition emits a typed receipt with prior/new state, reason, affected watermark, and trace context.

---

## 13. Rollout and rollback

1. **Journaling shadow mode:** enable coverage/guards locally but do not send. Verify no mutator bypasses the journal.
2. **Staged bootstrap:** build a complete server generation and compare roots; keep it out of routed search.
3. **Synthetic namespace canary:** exercise every operation family, duplicate, restart, revocation, and forgetting.
4. **Default-store bootstrap:** stage current device store, verify, promote, then rotate/catch up.
5. **Live sender:** enable semantic stream, then claim/blob streams. Monitor 24 hours before enabling routed search for the shard.
6. **Search enablement:** only `Current`/root-matched shards enter default federation.
7. **One device at a time:** repeat bootstrap/catch-up/soak before adding another device.
8. **V1 retention:** keep `/v1/replication/fact-create/v1` and the previous production binary/config during V2 soak. Do not route new full-surface traffic through `/v1/sync`.
9. **V1 deprecation:** after V2 conformance and 72-hour soak, freeze V1 admission, retain read-only evidence, then remove in a separate reviewed change.

Rollback procedure:

- stop/pause the affected sender stream
- mark the replica shard generation quarantined so federation excludes it
- restore/switch the control catalog and replica tree as one prior verified paired generation; never combine older metadata with newer shard files
- restore prior Mnemes binary/config/service unit
- leave device primary untouched
- retain spool, signed requests, ACKs, roots, and failed generation for forensics
- re-bootstrap rather than editing replica rows in place

---

## 14. Parallel execution plan and estimate

### Parallel lanes after Gate 1

- **Lane A — semantic owner:** Phases 2–3 and semantic snapshot/root APIs.
- **Lane B — transport/runtime:** Mnemes V2 admission, sender/spool, receiver, heartbeat/freshness.
- **Lane C — claim/search:** claim/evidence component store, derived artifact lane, recent-tail search, performance tests.

Shared gates prevent the lanes from inventing incompatible contracts.

### Honest estimate

The previous 27–48 day estimate covered the obvious semantic-memory surfaces but not the now-confirmed external claim/evidence authority, closed mutation enforcement, generic wire completion, and search-safe derived lane. Revised estimate:

- **42–65 engineer-days** of implementation and certification
- **solo:** approximately 9–13 focused weeks
- **two strong Rust engineers/agents with independent review:** approximately 6–8 calendar weeks
- **three parallel lanes:** approximately 5–7 calendar weeks, with Gates 1, 5, and 8 still serial

Do not compress the schedule by skipping coverage enumeration, bootstrap closure, fault injection, or soak. Speed comes from reusing the current fact vertical slice, generic protocol core, shard/search runtime, claim-ledger compaction, and stack IDs—not from weakening proof.

---

## 15. Auditor-rerunnable handoff

Before claiming completion, archive:

- exact repo commits/worktree diffs
- generated schema bundle and compatibility result
- mutation/table coverage report
- operation/snapshot conformance fixtures
- source/replica family roots and claim heads before/after bootstrap and soak
- sender/receiver service files and configs with secrets redacted
- metrics export proving the 15-second objective
- fault-injection matrix with pass/fail/skip reasons
- local and federated search latency/quality comparison
- rollback drill transcript
- production binary/config digests and active shard generation
- unresolved risks and explicit deferred scope

Completion requires real outputs from the target devices/server. Local tests alone do not certify deployment, and server-reported success is not a locally reproduced benchmark.
