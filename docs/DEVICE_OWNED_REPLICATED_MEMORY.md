# Device-owned replicated memory mesh

**Status:** Architecture decision adopted; protocol implementation and production admission **BLOCKED**. Researched, mapped to the current codebase, and hostile-reviewed on 2026-07-20. The current server runtime is not a replica implementation.

## Decision

Each device owns and writes its own canonical `semantic-memory` SQLite database. The pooled server keeps a durable, independently addressable replica of each device database and acts as the authorization, synchronization, sparse-routing, and cross-shard merge brain.

```text
Laptop                                      MSI brain/server
┌──────────────────────────────┐            ┌──────────────────────────────────────┐
│ semantic-memory primary      │            │ pooled.db control plane              │
│ device_id = laptop           │──sync─────▶│ device/actor/grant registry          │
│ memory.db                    │            │ sync watermarks + receipts           │
│ mutation journal + outbox    │◀─ack/inbox─│ sparse routing catalog               │
└──────────────────────────────┘            ├──────────────────────────────────────┤
                                            │ replica/laptop/memory.db (read-only*) │
MSI device-local primary                    │ replica/msi/memory.db    (read-only*) │
┌──────────────────────────────┐            └──────────────────────────────────────┘
│ semantic-memory primary      │──sync─────▶
│ device_id = msi              │
│ memory.db                    │
│ mutation journal + outbox    │
└──────────────────────────────┘

*Only the canonical replay applier may mutate a server replica.
```

The server does **not** flatten device stores into one database and does **not** become an unrestricted second writer. A replica remains namespaced by stable device identity and store epoch. Relevance controls routing, never authority.

MSI has two roles: brain host and registered memory device. Its device-local primary and its server replica remain logically distinct stores even when they are on the same machine; the server must not open one file under both authority roles. Co-location reduces latency but is not failure-domain redundancy, so MSI still requires backup outside that host.

This changes the meaning of the current server-side shard tree:

```text
BASE/memory/shards/<device_uuid>/memory.db
```

Those files should be treated as **server replicas of device-owned primaries**, not as the only canonical copies. The current staged per-device SQLite backups are suitable bootstrap replicas; the existing work is retained rather than replaced.

## Why this is the best fit

The target behavior is local-first:

- a device reads and writes its own memory without the server;
- work survives network loss and server loss;
- the server can share authorized memories while devices are offline by searching their replicas;
- synchronization resumes after intermittent connectivity;
- each store remains independently addressable and recoverable;
- sparse routing activates a bounded relevant subset rather than broadcasting to every device;
- every query states whether it used a live owner or a replica and exactly how fresh that replica was.

This matches the local-first goals described by Ink & Switch—offline work, multi-device collaboration, data ownership, and long-term preservation—without adopting generic CRDT semantics for evidence-bearing memory records.

## Authority map

| Surface | Owner | Material state |
|---|---|---|
| Device semantic content and history | `semantic-memory` on the home device | Canonical |
| Device mutation journal | `semantic-memory` on the home device | Canonical change witness |
| Server copy of a device store | Replayable per-device replica | Durable derived copy |
| Device identity, lifecycle, actors, grants | `pooled-memory` control plane | Canonical pooling metadata |
| Sync watermarks, gaps, acknowledgements, receipts | `pooled-memory` | Canonical synchronization evidence |
| Routing summaries and summary embeddings | `pooled-memory` | Rebuildable projection |
| Sparse route and global merge receipt | `pooled-memory` | Durable query witness |
| Server-originated request to change a device store | Device inbox proposal | Not admitted until home device accepts it |
| Claims/evidence adjudication | Adopted claim/evidence authority | Separate from retrieval and sync |

A replica is never silently promoted to authority. Disaster-recovery promotion requires an explicit operator operation, a new writer epoch, and fencing of the old primary.

## Research findings

### Raw SQLite file synchronization is rejected

Copying a live `memory.db`, WAL, or SHM file between peers is not a replication protocol. It risks inconsistent snapshots and has no semantic conflict, provenance, schema, authorization, or replay contract. SQLite's Online Backup API is appropriate for a consistent bootstrap snapshot, not continuous multi-writer synchronization.

Primary source: SQLite Online Backup API: <https://sqlite.org/backup.html>

### SQLite Session changesets are transport primitives, not the authority protocol

SQLite's Session Extension can record and apply table changes, but its own documented limits include:

- no virtual-table capture;
- only tables with declared primary keys;
- ignored changes for rows with `NULL` in primary-key columns;
- target schema and starting data compatibility requirements;
- application-defined conflict handling.

`semantic-memory` contains FTS/vector projections, embeddings, receipts, imports, multi-row document operations, authority state, and schema migrations. A table-level changeset cannot distinguish canonical content from rebuildable projection state and would bypass canonical semantic write rules.

Primary source: SQLite Session Extension: <https://sqlite.org/sessionintro.html>

### Single-primary SQLite replication is insufficient

Litestream is well suited to streaming backup and disaster recovery for a single SQLite writer. Its documented write mode assumes one writer. It does not provide independently writable offline device primaries with semantic conflict handling.

Primary sources: <https://litestream.io/> and <https://litestream.io/guides/vfs-write-mode/>

libSQL/Turso embedded replicas provide local reads and primary-serialized writes. This is useful for edge caches but makes the remote primary the write authority; it does not satisfy independent offline device ownership without accepting its primary model.

Primary sources: <https://docs.turso.tech/features/embedded-replicas/introduction> and <https://docs.turso.tech/libsql/client-access>

### Generic local-first sync systems are references, not drop-in owners

PowerSync and ElectricSQL demonstrate useful patterns: local SQLite reads/writes, upload queues, backend synchronization, and explicit handling when server policy rejects offline writes. They target application/backend row synchronization and introduce their own backend and conflict semantics. Replacing `semantic-memory` authority with either system would create a shadow owner and still require custom handling for provenance, supersession, forgetting, embeddings, receipts, and schema compatibility.

Primary sources:

- PowerSync client architecture: <https://docs.powersync.com/architecture/client-architecture>
- ElectricSQL writes: <https://electric-sql.com/docs/guides/writes>

### SQLite Sync is the strongest off-the-shelf candidate, but still the wrong authority owner

SQLite.ai's `sqlite-sync` directly targets offline-first SQLite and agent memory. Its current README documents CRDT convergence, matching initialized table schemas on all participating devices, whole-cell replacement for standard concurrent column edits, and SQLite Cloud/PostgreSQL/Supabase backends. It is licensed under Elastic License 2.0 with additional production/commercial-license language in the project README.

This could be useful for ordinary collaborative application tables. It is not admitted as the canonical `semantic-memory` replication layer because generic CRDT/LWW/delete-wins table convergence would decide conflicts below the governed mutation, provenance, contradiction, redaction, and forgetting contracts. It would also require a carefully curated table subset to avoid treating embeddings, FTS/vector indexes, and receipts as equal authority. Using it only as transport would retain those semantic risks while adding an extension/backend/license dependency.

Primary source: <https://github.com/sqliteai/sqlite-sync>

### Raft is the wrong availability tradeoff

Raft-based SQLite systems preserve one replicated log and require quorum for authoritative writes. A disconnected minority cannot continue independent writes. That conflicts with laptop/device offline operation.

Reference: rqlite quorum behavior: <https://rqlite.io/docs/faq/>

### Checkpoints and change feeds are proven replication patterns

CouchDB's replication design demonstrates resumable change feeds, checkpoints, common-ancestry detection, and full replication when no common checkpoint exists. Its revision/conflict model should not replace semantic-memory, but its synchronization mechanics are applicable.

Primary sources:

- <https://docs.couchdb.org/en/stable/replication/protocol.html>
- <https://docs.couchdb.org/en/stable/replication/intro.html>

### Decision matrix

| Option | Offline writes on every home device | Preserves `semantic-memory` authority semantics | Keeps per-device independently addressable stores | Avoids new mandatory backend/license dependency | Verdict |
|---|---:|---:|---:|---:|---|
| Copy live SQLite/WAL files | Unsafe | No | Superficially | Yes | Reject |
| SQLite Session changesets | Yes | No; table diffs miss/blur engine semantics | Yes | Yes | Reject as canonical protocol |
| Litestream | No multi-writer sync | No semantic replay | Yes | Yes | Backup/DR only |
| libSQL/Turso replicas | Not under the required independent-primary model | Central-primary semantics | Yes | No | Reject for this authority model |
| PowerSync/ElectricSQL | Yes | Their backend/conflict model becomes owner | Usually backend-shaped | No | Reference patterns only |
| SQLite Sync CRDT | Yes | No; generic table convergence is below governed memory semantics | Possible | No for admitted production use | Reject as canonical protocol |
| Device-owned semantic journal + server replicas | Yes | **Yes**; canonical owner defines mutations/replay | **Yes** | **Yes** | **Adopt** |

## Core invariant: one writer authority per shard

For a device-owned shard:

```text
home_device_id + store_epoch + writer_epoch
```

identifies the accepted writer generation.

- `home_device_id`: stable registered `DeviceId`.
- `store_epoch`: UUID created when a logical store is initialized or restored as a new lineage.
- `writer_epoch`: monotonically increasing fencing generation admitted by the pooled control plane.
- `sequence`: contiguous mutation sequence within that epoch.

The home device is the only normal writer. Multiple local actors may write through the same `semantic-memory` owner, which serializes them transactionally. The server replica is writable only through the canonical replay applier.

This boundary must be mechanical, not conventional:

- `ReplicaStore::open_read_only(...)` is the only query/routing handle exposed to pooled code;
- `ReplicaApplier::apply_batch(...)` is the only server-replica mutation handle;
- ordinary `MemoryStore` mutation APIs are unavailable or fail typed in replica mode;
- query handles open SQLite with read-only/query-only enforcement and cannot run schema migration; the applier capability is private to the sync service;
- the current `PooledMemoryStore::device_memory()` writable handle is not admissible as the replica API.

If a restored device and its former instance both attempt to write, the server does not merge them with last-write-wins. The stale writer epoch is rejected and quarantined. Recovery either:

1. discards the stale fork after preserving evidence; or
2. imports reviewed operations into a new epoch through explicit reconciliation.

This avoids using CRDTs where contradictions, revocations, redactions, and provenance require domain-specific meaning.

### Promotion and writer fencing

Promotion is one linearizable control-plane transaction authorized by an admitted operator grant:

```text
store_epoch
prior_writer_epoch
new_writer_epoch
promoted_device_id
promotion_reason
authorization_snapshot_id
authorization_snapshot_digest
fencing_token
promotion_receipt_id
```

The transaction atomically closes the prior epoch, advances the current writer epoch, revokes prior sync sessions, registers the replacement writer, and emits a signed promotion certificate. Every write and sync session presents the current fencing token. A valid signature from an older epoch is still rejected and receipted as stale-writer evidence. A replacement device is explicitly registered; it is never an implicit continuation of a lost device identity.

## Canonical mutation journal

The missing primitive belongs in `semantic-memory`, because only that crate knows which changes are canonical and how multi-row mutations remain atomic.

Every supported semantic mutation must:

1. validate authority and idempotency;
2. mutate canonical content/history;
3. update required local projections;
4. append one immutable replication envelope;
5. advance the local sequence and journal head;
6. commit all five effects in the same SQLite transaction.

No after-the-fact observer or pooled adapter may infer mutations from changed rows.

### `MemoryMutationEnvelopeV1`

```text
schema_version
home_device_id
store_epoch
writer_epoch
sequence
operation_id
caller_idempotency_key
actor_id
namespace
mutation_kind
payload_schema
canonical_payload
optional_projection_payload
content_digest
previous_envelope_digest
authority_receipt_id
authority_receipt_digest
authorization_snapshot_id
authorization_snapshot_digest
policy_version
requested_effect_digest
device_key_version
fencing_token
valid_time
committed_at
device_signature
```

Properties:

- `(home_device_id, store_epoch, writer_epoch, sequence)` is unique.
- `operation_id` and caller idempotency key collapse retries.
- `operation_id` is the canonical semantic operation identity. A projected pooled `OperationEnvelope` may reference the same ID after sync, but the pooled record is acceptance/provenance evidence, not the canonical mutation journal.
- `previous_envelope_digest` creates a per-epoch hash chain and exposes gaps/reordering.
- Digests and signatures cover one specified deterministic wire encoding (for example, canonical CBOR with fixed field rules), never implementation-dependent JSON serialization.
- `canonical_payload` contains the semantic operation, not arbitrary SQL.
- projection payloads are explicitly typed as rebuildable and may include embedding bytes plus model/dimension/digest metadata to avoid silent re-embedding drift.
- delete, revoke, redact, supersede, and forgetting actions are explicit tombstone/history operations; omission is never deletion.
- the envelope is signed by the device identity key. Transport authentication alone is insufficient for durable cross-host evidence.
- the signed preimage binds actor, namespace, exact requested effect, policy version, authorization snapshot, authority receipt digest, key version, fencing token, canonical payload, and all temporal/lineage fields.
- the authority receipt identifies authorized actor, issuer, grant, validity interval, requested effect, decision, and deterministic receipt digest.
- device `valid_time`/`committed_at` and server `replicated_at` are separate. Sync arrival order and server wall-clock time do not rewrite the device's temporal claim.

### Mutation coverage gate

The protocol is not production-complete until every active write class is represented, including:

- governed fact append/supersede/redact/revoke;
- documents and their chunks as one aggregate operation;
- messages and conversation history;
- episodes and causal metadata;
- authority and forgetting receipts;
- imports with original provenance;
- deletions/tombstones;
- projection rebuild/version events where needed.

Unsupported write paths must fail the replication-readiness gate rather than silently mutate unjournaled state.

## Snapshot plus journal-tail synchronization

### Bootstrap or unrecoverable gap

1. Device creates a consistent SQLite snapshot with the Online Backup API.
2. After sealing the snapshot, the device reads `store_epoch`, `writer_epoch`, and `snapshot_sequence` from the snapshot itself. Sampling the live source before or after backup would create an unproven boundary.
3. Device emits a content-free table/count/schema manifest and whole-snapshot digest. The signed snapshot envelope binds `snapshot_id`, `snapshot_digest`, `store_epoch`, `writer_epoch`, `snapshot_sequence`, `journal_head_digest`, `prior_replica_generation`, `schema_generation`, and `acl_snapshot_digest`.
4. Device signs the snapshot envelope.
5. Server verifies identity, lifecycle, key version, ACL snapshot, signature, schema compatibility, quick-check, manifest, ancestry, expected prior generation, and digest. Snapshot admission never advances writer authority or grant state.
6. Server installs it atomically as a new replica generation under a per-replica install lease containing `active_generation`, `installing_generation`, `install_base_sequence`, and `install_base_digest`. Batch application is frozen for the final conditional swap.
7. Server requests journal events after `snapshot_sequence`.

The snapshot is a transport/recovery artifact. Its authority comes from the admitted home device and signed envelope, not from its filename.

### Incremental catch-up

1. Device opens an outbound authenticated sync session.
2. Device and server exchange signed reports binding `(store_epoch, writer_epoch, fencing_token, contiguous_sequence, head_digest, schema_generation, key_version)`.
3. Device sends bounded event batches after the server's highest contiguous acknowledgement.
4. Server validates lifecycle, grant, signature, schema, sequence, previous digest, idempotency, and payload limits.
5. Server applies the batch through `semantic-memory`'s canonical replay API in one replica transaction. Replay preserves the original envelope/signature in an applied-event ledger but must not emit a new local-origin outbox event or create a replication loop.
6. Server writes a signed sync receipt binding the exact batch digest, prior head, resulting highest contiguous sequence/head, replica generation, ACL snapshot, and durable commit identity.
7. Device retains journal data until both acknowledged and covered by independently verified recovery points. Batch acknowledgement alone never authorizes compaction. Events through sequence C may be compacted only when both (a) the server has signed an active replica/snapshot receipt at sequence >= C and (b) a sealed local recovery snapshot at sequence >= C has been verified in a separate failure domain. The device retains the configured event/time safety window below `min(server_verified_sequence, local_recovery_sequence)`.
8. Gaps request a specific missing range. Epoch mismatch, divergent digest, or impossible ancestry quarantines the replica; it never triggers guessed repair.

Retries are safe. Out-of-order batches are bounded or rejected. Acknowledgement means durable replica commit, not merely receipt by the HTTP process.

Replay enforces immutable collision semantics:

- `operation_id` is unique;
- `(home_device_id, caller_idempotency_key)` is unique;
- `(store_epoch, writer_epoch, sequence)` is unique;
- an identical envelope digest returns the original acknowledgement;
- any colliding identity with a different digest quarantines the stream and emits a conflict receipt;
- an accepted event is never updated or replaced.

### Server-to-device direction

The server must not mutate a device replica and later push that state back as authority. Server-originated writes are proposals in a separate inbox:

```text
proposal_id
target_device_id
target_store_epoch
target_writer_epoch
requesting_actor
requested_mutation
authority/evidence references
requested_effect_digest
proposal_digest
proposal_nonce
acl_snapshot_id
authorization_snapshot_digest
issuer_key_version
issuer_role
expires_at
server_signature
```

The device verifies issuer authorization, target lineage, expiry, nonce uniqueness, and current local policy, then accepts or rejects through its normal canonical mutation path. If accepted, the resulting device-authorized mutation later replicates to the server. A proposal receipt is never a mutation receipt. Rejection is also receipted.

## Sparse routing brain

Routing and synchronization are separate planes.

### Routing pipeline

```text
authorization mask
  → lifecycle/schema/freshness mask
  → compact-summary relevance score
  → bounded top-K/top-P shard selection
  → execution-location selection
  → canonical per-shard search
  → deterministic merge/conflict scan
  → global receipt
```

The router may select one of two execution locations per shard:

1. **Live owner:** query the home device over its outbound session. This is freshest and avoids replica lag.
2. **Server replica:** query the local replica when the owner is offline, slow, or outside the request deadline.

The first **admitted** routing phase should use server replicas only, after read-only replica, grant, freshness, journal, and replay gates pass. The current writable server-shard path does not satisfy this contract. Live-owner execution is a later optimization after synchronization is stable.

For live-owner execution, the server sends a short-lived signed query capability binding requester, authorized namespaces, query digest, routing-receipt parent, deadline, and grant snapshot. The device verifies that capability before search and returns a device-signed child receipt. The server cannot substitute a replica result and label it as live-owner evidence.

### Freshness contract

Every replica has:

```text
store_epoch
writer_epoch
applied_sequence
head_digest
owner_reported_sequence
lag_events
last_sync_at
schema_generation
state
```

Every routing summary is bound to the same store/writer epoch and applied sequence. A summary from sequence N must not route a query as though it described replica sequence M.

States should include:

- `bootstrapping`
- `installing_snapshot`
- `catching_up`
- `awaiting_gap`
- `current`
- `lagging`
- `offline_usable`
- `schema_blocked`
- `gap_detected`
- `forked`
- `quarantined`
- `key_blocked`
- `retention_unsafe`
- `rejected`
- `promoted`
- `retired`

Only `current`, policy-admitted `lagging`, and policy-admitted `offline_usable` are queryable. `catching_up` is queryable only against its still-active prior generation. All other states are non-queryable by default. Legal transitions and guards are versioned; no arbitrary string transition is accepted.

| State | Permitted next states | Sync behavior |
|---|---|---|
| `bootstrapping` | `installing_snapshot`, `rejected`, `quarantined` | Snapshot admission only |
| `installing_snapshot` | `catching_up`, `rejected`, `quarantined` | Incremental batches frozen |
| `catching_up` | `current`, `awaiting_gap`, `schema_blocked`, `key_blocked`, `quarantined` | Contiguous tail only |
| `current` | `lagging`, `offline_usable`, `installing_snapshot`, `retired`, `quarantined` | Normal contiguous batches |
| `lagging` / `offline_usable` | `current`, `awaiting_gap`, `installing_snapshot`, `retired`, `quarantined` | Normal contiguous batches within policy |
| `awaiting_gap` | `catching_up`, `gap_detected`, `quarantined` | Requested missing range only |
| `gap_detected` | `installing_snapshot`, `quarantined`, `retired` | No incremental apply |
| `schema_blocked` / `key_blocked` / `retention_unsafe` | `catching_up`, `installing_snapshot`, `quarantined`, `retired` | No apply until the typed blocker is resolved |
| `forked` / `quarantined` | `installing_snapshot`, `promoted`, `retired` | Explicit operator reconciliation only |
| `promoted` | `retired` | Old generation closed |
| `rejected` / `retired` | none | Terminal; a new admission creates a new generation |

Freshness can affect whether a replica is usable, but cannot grant authority. Requests specify maximum lag plus `completeness = require_all | allow_partial` and optional minimum sequence per required shard. Partial output carries `incomplete=true`, failed/skipped shard IDs and watermarks. A caller may not infer “not found” when required shards were not searched.

### Routing receipt additions

Each selected-shard outcome must add:

```text
home_device_id
execution_location = live_owner | server_replica
store_epoch
writer_epoch
applied_sequence
head_digest
observed_lag_events
last_sync_at
child_receipt_id
acl_snapshot_id
acl_snapshot_digest
evidence_state = canonical | replica_observation | stale_replica_observation
latency/error
```

The global receipt must bind the authorization snapshot and routing-summary generation. Replica results are non-authoritative observations. Stale or replica observations cannot authorize mutation, adjudication, supersession, revocation, forgetting, or promotion without independent revalidation against the owner or an admitted authority receipt.

Epoch, sequence, head, schema, ACL snapshot, and summary identity must be captured in the same read transaction/snapshot as the searched data. Routing summaries are grant-filtered projections bound to `summary_base_sequence`, `summary_digest`, `acl_snapshot_digest`, store/writer epoch, and source generation; they are invalidated on epoch, snapshot, schema, authorization, redaction, or forgetting changes.

## Sharing semantics

“Share device memory” means the server may search an authorized device replica on behalf of another device. It does not mean copying all records into every device's canonical store.

Default topology:

```text
one canonical primary on home device
one durable replica on server
zero canonical copies in other device stores
```

An optional later feature may materialize selected remote memories for offline use on another device. Those records must live in a separate imported-projection/cache namespace with immutable origin metadata and a rebuild path. They do not become local assertions merely because they were cached.

## Cross-device duplicates and contradictions

- Same item ID and same content may deduplicate at merge.
- Same item ID and different content fails closed.
- Similar content from different device IDs remains distinct evidence unless an explicit canonical identity links it.
- Contradictions are appended and adjudicated; they are not resolved by timestamp or last-writer-wins.
- Search rank and sync arrival order never decide truth.

## Security and privacy

### Initial trust model

The MSI server is a trusted local infrastructure host. It stores plaintext SQLite replicas protected by filesystem permissions and encrypted storage where available. Transport uses TLS/mTLS or an SSH-protected channel. Device status and actor grants are checked on every sync and query operation.

### Device signatures

Each device has an Ed25519 signing key separate from its bearer/API credential. The server has a separate signing identity for acknowledgements, proposals, authorization snapshots, promotion certificates, and receipts. Append-only key registries store principal, role, key version, predecessor version, activation time, revocation time/reason, and rotation receipt. Device mutations/snapshots/summaries/live-query children are device-signed; server acknowledgements/proposals/grant snapshots/promotions are server-signed. Every envelope identifies signer principal, role, and key version.

Rotation is dual-signed by the currently valid key and new key, establishes an explicit cutoff for queued envelopes, and invalidates old-key sessions at revocation. Old signatures remain verifiable as historical evidence but cannot authorize new access. Loss of the routing-receipt HMAC key and loss/compromise of a device signing key are distinct typed incidents.

#### Closed signer-role matrix

Signer roles are a closed versioned enum. A verifier rejects unknown roles and any signature over an artifact not permitted below.

| Signer role | May sign | Must not sign |
|---|---|---|
| `operator_root` | Role assignments/revocations, emergency key recovery, delegation to grant/recovery authorities | Semantic mutations, routine sync ACKs, search results |
| `device_writer` | Device mutations, snapshots, summaries, live-query child receipts, dual-signed rotation of its own active key | Grants, promotions, bootstrap admission, server ACKs/proposals |
| `semantic_authority_issuer` | Device-local governed authority receipts referenced by a device mutation | Device registration, grants, sync admission, promotion |
| `sync_service` | Sync session challenges, durable batch/snapshot ACKs and sync receipts, live-query capabilities | Device mutations, grants, promotions, semantic authority decisions |
| `grant_authority` | Grant/revoke operations and ACL/authorization snapshots | Semantic mutations, replica apply, query results |
| `proposal_issuer` | Server-to-device proposals under an admitted ACL snapshot | Device mutation acceptance, grants, promotions |
| `recovery_authority` | Bootstrap certificates, replica admission, promotion/fencing certificates | Normal semantic writes, search results, routine grants unless separately assigned |
| `routing_service` | Global routing receipts and routing-summary admission receipts | Semantic mutations, grants, promotion, device child receipts |

One principal/key may hold multiple roles only through explicit signed role assignments; every signature names exactly one role and is checked against that artifact type. Role assignment/revocation is append-only and cannot be inferred from possession of another role. Compromised/lost-key recovery requires `operator_root` or an explicitly delegated recovery policy, establishes a non-backdatable cutoff, and cannot retroactively validate artifacts created while the key was invalid.

### Authorization

The server applies authorization before ranking and again before opening/querying a replica. Every mutation, sync admission, snapshot, proposal, shard query, child receipt, and global receipt binds `acl_snapshot_id`, `acl_snapshot_digest`, evaluation time, and grant validity interval. Current revocation is checked for live access even when historical evidence refers to an older valid snapshot. Grants are versioned by namespace, operation, requesting device/actor, target device, and validity interval.

Routing summaries are constructed only from grant-filtered projections. Revocation and redaction invalidate affected summaries. API-level tests must prove unauthorized-shard indistinguishability within the declared leakage model; shard existence, namespaces, scores, and timing are not silently exposed.

#### Declared cross-device leakage model

The protected adversary is an authenticated but unauthorized device/actor. The server host itself is trusted under the plaintext-server boundary below.

**Permitted leakage:** service/version availability; the requester's own identity and grant decision; aggregate response latency; aggregate result count/size for evidence the requester is authorized to receive; and documented coarse transport/status buckets. This design does not claim cryptographic aggregate timing or traffic-analysis resistance.

**Forbidden leakage:** existence or number of unauthorized devices/shards; protected labels or namespaces; item/count statistics; routing terms, vectors, summary/head digests, scores, rank/selection membership, freshness, per-shard errors, child receipts, or per-shard timing for unauthorized targets.

Enforcement rules:

- authorization is evaluated from the requester's grant view before target catalog lookup, ranking, summary access, or shard open;
- unauthorized and nonexistent explicit targets return the same status class and public response schema;
- receipts enumerate only authorized eligible/ranked/selected/skipped shards and never report the hidden-shard count;
- no padding claim is made for authorized result payloads, but denial responses use one documented size/status bucket and perform no unauthorized target-dependent I/O;
- revocation invalidates affected summaries and cached route plans before subsequent authorization decisions.

Test oracle: run paired stores that differ only by unauthorized shards and assert identical status class, public JSON fields, receipt-visible sets, and denial bucket; instrument catalog/file opens to prove no unauthorized target lookup/open. Aggregate timing is measured only to detect target-dependent I/O regressions, not to claim constant-time network behavior.

### End-to-end encryption boundary

If the server cannot be trusted with plaintext, it cannot perform normal semantic search over replicas. End-to-end encrypted storage would require either:

- authorized shard keys at the server;
- live-owner query execution only;
- or specialized searchable-encryption techniques with substantial leakage/complexity.

Do not claim server-side semantic routing over opaque encrypted content. The trusted-local-server model is the practical first deployment. Server compromise is explicitly outside the protection boundary. Production still requires encrypted host storage and backups, per-device/namespace isolation, access auditing, and prohibition of unaudited plaintext replica exports.

### Redaction and forgetting propagation

A signed redaction/forgetting envelope binds `redaction_id`, origin/item identity, target content digest, policy basis, effective time, retention exception, and propagation scope. On admission, affected content is immediately excluded from replica search, indexes, summaries, caches, and materialized subscriptions. Durable propagation receipts cover replicas and backup generations. A retained historical generation is sealed non-queryable; retained tombstones contain no recoverable sensitive payload unless an explicit retention authority permits it.

## Failure behavior

| Failure | Required behavior |
|---|---|
| Device offline | Local reads/writes continue; server serves bounded-staleness replica |
| Server offline | Device local memory continues; outbox accumulates |
| Duplicate event batch | Idempotent no-op with original acknowledgement |
| Missing sequence | Request exact gap; do not advance watermark |
| Divergent previous digest | Quarantine as fork |
| Unsupported schema | Mark `schema_blocked`; preserve bytes; no partial apply |
| Revoked device | Reject sync and live query immediately; retain sealed replica per policy |
| Interrupted snapshot | Keep old replica active; discard incomplete generation |
| Interrupted batch | Replica transaction rolls back; acknowledgement not advanced |
| Server disk loss | Restore replicas from device primaries or sealed backups |
| Device loss | Restore from server replica only through explicit promotion/new epoch |
| Routing-receipt HMAC-key loss | Fail closed for receipt authentication; restore the matching key generation with data |
| Device signing-key compromise/loss | Revoke key version and sessions, quarantine post-cutoff envelopes, rotate through authorized recovery, preserve historical verification |

## Implementation map

### Existing work retained

Current `pooled-memory` already supplies:

- device and actor identity;
- lifecycle states;
- idempotent operation envelopes;
- bitemporal provenance edges;
- separate per-device SQLite stores;
- sparse routing and exhaustive comparison;
- bounded shard cache;
- global routing receipts;
- migration snapshots and rollback evidence.

The staged per-device stores are useful bootstrap candidates, but their current manifests prove operator-selected source identity and integrity—not device-signed ancestry. They become admitted initial replicas only after an explicit operator bootstrap certificate binds source digest, device ID, store/writer epoch, initial sequence/head, schema generation, ACL snapshot, and migration authority. Otherwise they remain test/migration evidence. No new global semantic database is needed.

### Required changes

A static audit on 2026-07-20 found 40 public async mutation candidates in `semantic-memory`, spanning authority, conversations, documents, episodes, facts, graph edges, imports, procedures, shadow policy, and epistemic state. Several are trace/embedding wrappers over shared internals, so this is not 40 independent implementations; it is nevertheless the mandatory coverage inventory. Phase 0 must trace each public method to its transactional owner and prove that no direct write bypasses the journal.

#### `semantic-memory`

- Add typed `MemoryMutationEnvelopeV1` and mutation-kind contracts.
- Add immutable `replication_journal` and `store_replication_state` tables.
- Make canonical write paths append journal events in the same transaction.
- Add bounded journal export and canonical replay APIs.
- Make replay origin-aware: preserve the source envelope in an applied-event ledger without adding it to the replica's outbound journal.
- Distinguish canonical payload from optional derived projection payload.
- Add snapshot identity/sequence APIs.
- Add mutation-coverage conformance tests.

#### `pooled-memory`

- Reclassify `device_shards` as device primaries plus server replica records.
- Replace ordinary writable shard handles with mechanically read-only `ReplicaStore` query handles and a separate `ReplicaApplier`; prove direct server-side semantic mutation fails.
- Add `store_epoch`, `writer_epoch`, applied/owner sequence, head digest, last sync, replica state, schema state, and execution location.
- Add sync session, batch acknowledgement, snapshot admission, gap, fork, and proposal-inbox contracts.
- Add device public signing keys and key versions.
- Add authenticated bounded sync endpoints and receipts.
- Project accepted device operations into `pooled.db` as signed synchronization/provenance evidence while retaining the device journal as the canonical mutation witness; record server `replicated_at` separately.
- Add freshness to routing eligibility/outcomes.
- Add grant snapshots and apply authorization before ranking and before opening any replica.
- Keep routing projections rebuildable.

#### Device runtime

- Run a small outbound sync agent beside local `semantic-memory`.
- Never expose SQLite files directly.
- Push journal batches and summaries; pull acknowledgements/proposals.
- Back off with jitter, retain durable checkpoints, and work fully offline.
- Provide local health: journal head, oldest retained event, server acknowledgement, lag, last error.

## Phase plan and acceptance gates

### Phase 0 — Contract freeze and simulator

Outputs:

- versioned mutation/snapshot/sync/freshness contracts;
- authority matrix;
- deterministic two-device network-loss simulator;
- explicit list of all semantic write paths.
- explicit list of authority-sensitive read/transition paths: adjudication, promotion, redaction/forgetting, grant change, cache materialization, snapshot admission, and recovery import.

Gate:

- every active write path is classified as journaled, projection-only, or blocked;
- no raw SQLite/WAL synchronization path exists;
- simulator reproduces disconnect, duplicate, reorder, gap, and fork cases deterministically.
- production remains blocked while current writable shard handles, missing cross-device grants, or absent journal/replay APIs remain.

### Phase 1 — Canonical semantic mutation journal

Outputs:

- atomic journal append in `semantic-memory`;
- hash-chain and sequence state;
- canonical replay API;
- snapshot sequence API.

Gate:

- fault injection before/after content, projection, journal, epoch, and receipt leaves either the complete mutation or no mutation;
- replay into an empty same-schema store yields equal canonical manifests and witnessed retrieval IDs;
- unsupported write paths fail closed.

### Phase 2 — Device-to-server sync

Outputs:

- signed hello/batch/ack contracts;
- pooled sync state and receipts;
- outbound device agent;
- bounded retry/checkpoint logic.

Gate:

- 10,000 offline mutations synchronize exactly once after reconnect;
- duplicates, interruption, and reordering do not change final canonical state;
- gaps and divergent hash chains fail closed;
- revoked/quarantined devices cannot continue an existing session.
- duplicate IDs/sequences with differing digests quarantine; stale fencing tokens and revoked key versions fail with durable receipts.

### Phase 3 — Snapshot bootstrap and recovery

Outputs:

- signed Online Backup snapshot envelope;
- atomic replica-generation install;
- journal-tail catch-up;
- retention/compaction policy.

Gate:

- snapshot at sequence N plus tail N+1..M equals continuous replay through M;
- interrupted install leaves the prior replica active;
- restore drill proves device-primary → server and server-replica → replacement-device with a new fenced epoch.
- concurrent batch/snapshot races cannot lose or duplicate tail events; compaction requires a signed verified-snapshot receipt.

### Phase 4 — Freshness-aware sparse routing

Outputs:

- replica freshness state;
- freshness policy in route requests;
- expanded routing receipts;
- stale/gap/schema masks.

Gate:

- Recall@K remains at the admitted target versus exhaustive search;
- no result is attributed to a sequence newer than the searched replica;
- stale/degraded results expose exact lag;
- revoked, forked, quarantined, or schema-blocked replicas are never opened.
- active-but-unauthorized devices cannot infer or search protected shards; incomplete search cannot support a false “not found” claim.

### Phase 5 — Live-owner execution

Outputs:

- server-to-device query messages over the outbound session;
- deadline/fallback policy;
- live-owner child receipts.

Gate:

- online owner queries use the owner-reported epoch/sequence;
- timeout fallback uses the replica and records the different watermark;
- duplicate content from live owner and replica cannot appear silently;
- no inbound device listener is required.

### Phase 6 — Optional offline remote subscriptions

Implement only if demand proves it.

Gate:

- remote records are stored as projections/cache with immutable origin;
- cache loss is rebuildable;
- cached evidence never becomes local assertion authority;
- namespace grants and revocations remove access without rewriting history.

### Cross-phase negative admission gates

Production remains blocked until executable tests prove:

- direct ordinary mutation of a server replica fails;
- every active semantic write path atomically journals or fails closed;
- old-key envelopes/sessions and stale fencing tokens fail after cutoff;
- stale ACL snapshots cannot authorize new mutations or queries;
- replica observations cannot trigger authority transitions;
- replayed proposals/snapshots and conflicting idempotency identities are rejected;
- redacted/forgotten content is absent from queryable replicas, indexes, summaries, and caches; retained backups are either purged or cryptographically sealed and non-queryable under an explicit retention exception;
- unauthorized routing reveals no protected content under the declared leakage model;
- sync arrival/ACK time never changes semantic authority or device commit time;
- recovery cannot create two accepted writers for one store epoch;
- every accepted mutation has a verifiable actor → grant snapshot → authority receipt → device-key lineage.

## Do not do

- Do not rsync or Syncthing live SQLite/WAL files.
- Do not make the server and device concurrent writers to the same logical shard.
- Do not use last-write-wins for evidence, contradictions, redactions, or forgetting.
- Do not replicate derived FTS/vector tables as untyped authority.
- Do not re-embed silently with a different model and call the replica equivalent.
- Do not let routing relevance widen authorization.
- Do not auto-promote a server replica after device loss.
- Do not retain a second global semantic store as compatibility fallback.
- Do not adopt Raft if disconnected devices must continue writing.
- Do not claim end-to-end privacy while the server performs plaintext semantic search.

## Recommended next decision

Freeze production cutover in its current server-authoritative wording. Keep the isolated two-shard candidate and V4 staged databases as verified bootstrap evidence, but change the admitted architecture to device-owned primaries with server replicas.

The highest-ROI implementation sequence is:

1. enumerate every `semantic-memory` write path;
2. build the atomic mutation journal and replay conformance harness;
3. implement one-way device-primary → server-replica synchronization;
4. add snapshot bootstrap plus journal tail;
5. make routing freshness-aware;
6. add live-owner execution only after replica synchronization is proven.

This preserves all completed routing and migration work while correcting the authority direction before production deployment.
