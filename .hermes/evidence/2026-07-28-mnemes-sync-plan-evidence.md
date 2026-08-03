# Mnemes Synchronization Plan — Controller Evidence Packet

**Captured:** 2026-07-28T20:41:38-05:00  
**Task class:** design/plan with read-only hostile verification; no runtime, database, Git ref, credential, or service state changed  
**Authority order:** live runtime and current source outrank earlier plans, summaries, and council output

## 1. Scope and intended trust boundary

The intended system is device-primary, one-way replication:

- `semantic-memory` owns canonical local semantic mutation and the transactional outbox.
- `mnemes` owns device identity, signed transport envelopes, admission, replica apply state, acknowledgements, and replica lifecycle.
- The server copy is a replica/read surface, not an ordinary multi-writer peer.
- Derived embeddings, HNSW structures, caches, summaries, and routing projections are rebuildable; semantic facts, provenance, governance state, and replication receipts are not.
- This packet does not license continuous sync or production-readiness claims.

## 2. Canonical source state

### Local Mnemes checkout

- Path: `/home/sikmindz/Coding/mnemes`
- Package: `mnemes 0.1.1`
- Current checkout is dirty; nine tracked files are modified.
- `Cargo.toml` resolves registry `semantic-memory 0.5.13` with `hnsw`; it does not build against the repaired local source.
- Production binaries declared: `mnemes-server`, `mnemes-admin`. No Rust `mnemes-syncd` binary exists.
- `cargo test --all-features`: **125 tests passed** across library/integration suites.
- `cargo clippy --all-targets --all-features -- -D warnings`: **failed** on four lints in `replica.rs`, `sync.rs`, and `sync_handler.rs`.
- Green protocol/sync tests are synthetic and do not prove a real public semantic-memory mutation reaches a signed remote replica.

### Local semantic-memory checkout

- Path: `/home/sikmindz/Coding/Libraries/semantic-memory`
- HEAD: `ebd2d53fafc4c3b401fae7c425f79ace27df7ba6`
- Package manifest: `semantic-memory 0.6.0`
- Dirty recovery state: eight tracked files modified and one untracked receipt adapter.
- Restored schema ceiling: 37.
- Earlier same-session gates passed: formatting, library tests (117 passed; three intentionally ignored), integration tests, strict all-target/all-feature Clippy, and full MCP compile.
- Those gates certify the scoped dirty source, not a committed release or installed runtime.

### Local semantic-memory MCP checkout

- Path: `/home/sikmindz/Coding/Libraries/semantic-memory-mcp`
- HEAD: `9d9535584a693eca1dd99dcd308c1ff85bb5a551`
- Package: `semantic-memory-mcp 0.5.6`
- Dirty recovery state includes Mnemes CLI/config additions.
- Source currently opens `MemoryStore` first and then calls mutable `configure_replication()`.
- This is source-present and compile-verified only; it is not the installed runtime.

## 3. Installed local runtime

### Installed semantic-memory MCP

- Binary: `/home/sikmindz/.local/bin/semantic-memory-mcp`
- Version: `0.5.5`
- SHA-256: `02c12fa2ae05aaf8c2727923b1fcf218f76b9dea27f9579df30e673635f9a6c7`
- Binary contains no `mnemes-required`, `mnemes-device-id`, or `configure_replication` marker.
- Active Hermes config does not pass any Mnemes identity/required-mode flags.
- Eleven MCP processes currently share `/home/sikmindz/.hermes/semantic-memory.db` through HTTP, desktop/gateway, and watchdog-owned stdio sessions.
- This process multiplicity is observed; a single-writer ownership contract has not been established.

### Canonical local database

- SQLite quick check: `ok`
- Schema/user version: 37
- Facts: 1,057
- `mutation_journal` exists but contains **zero rows**.
- Therefore the installed runtime has produced no replayable Mnemes stream.

### Python fact snapshot worker

- Unit: `mnemes-syncd.service`
- Installed script SHA-256: `3d3b28705fdc1d1d2340350e3b4511fd8ad693a84d526955949d680479d1ba9d`
- Active process has reached the 1,024-descriptor soft limit:
  - 510 `memory.db` descriptors
  - 509 `memory.db-wal` descriptors
  - two `memory.db-shm` descriptors
  - two sockets
- Journal evidence: 228 `EMFILE` failures in the last two hours, recurring every 30 seconds.
- Root cause: Python `with sqlite3.connect(...)` commits/rolls back but does not close the connection; the long-running loop repeatedly leaks SQLite handles.
- The script is not present in the current Mnemes repo source tree.
- It chooses whichever of two databases has the most facts, reads `facts` by SQLite `rowid`, sends `/v1/sync/facts`, and persists a manually resettable JSON rowid watermark.
- It cannot represent updates, deletes, supersession, provenance, graph edges, sessions/messages, episodes, evidence, governance, or operation receipts.
- State watermark: source rowid 12,801; it is not a semantic mutation sequence.

### Python journal watcher

- Unit: `sm-sync-watcher.service`
- Script: `/home/sikmindz/.hermes/sync/sync.py watch`
- Active process also has 1,024 descriptors:
  - 510 `memory.db`
  - 510 `memory.db-wal`
  - one `memory.db-shm`
  - two sockets
- It independently leaks SQLite connections through the same Python context-manager pattern.
- It reads `mutation_journal` directly, owns a second `sync_state` table, and posts `fact.create.v1` entries to `/v1/sync/facts`.
- The active journal is empty, so this worker cannot make progress.
- Two independent workers, two watermark models, and two source interpretations violate single-owner sync semantics.

## 4. Live UNO Q authority

### Process/artifact

- Host identity: `uno-q`, Linux aarch64.
- Listener: `127.0.0.1:1738`; remote access is through an SSH local-forward tunnel.
- Health: authenticated `/v1/health` returned HTTP 200 with `ready=true`, schema `mnemes.server.v1`, Nomic 768-dimensional embedding configuration.
- Running binary: `/home/arduino/.local/bin/mnemes-server`
- Running/on-disk SHA-256: `11e682345fac935dcdba72149ba4d413f5fc177670d1211cf891ec0e9b9cb732`
- The running artifact contains registry source paths for `semantic-memory 0.5.13`.
- The remote source checkout is detached at `43cbf485ed5df6412ae00351979073562d94215c`, dirty, and has an untracked backup file.
- The remote checkout contains an unbuilt edit that replaces `/v1/sync` with HTTP 501, but it does not disable `/v1/sync/facts`.
- The running artifact does not match the checkout’s release target hash and predates the disable edit.

### Deployed route truth

Unauthenticated, schema-valid empty requests returned HTTP 401 from both:

- `/v1/sync`
- `/v1/sync/facts`

This proves both routes are registered in the running artifact. The running artifact also contains the strings:

- `payload not valid UTF-8 SQL`
- `replay SQL batch failed`
- `PRAGMA query_only = ON`

The committed handler for `/v1/sync`:

1. authenticates a bearer token but discards the returned device context;
2. accepts caller-supplied `home_device_id` and `store_id`;
3. ignores `TrustedKeyRegistry`;
4. decodes caller-supplied payload bytes as UTF-8 SQL;
5. executes them with `Connection::execute_batch`;
6. computes `next_sequence = start + synced + errors`, allowing acknowledgement past failures.

This path is **live, authenticated arbitrary SQL execution against a caller-selected replica target**. Because the target filename is built from unsanitized `store_id`, path traversal/confinement must also be treated as unproven.

### Authority databases

All enumerated SQLite files passed `PRAGMA quick_check`.

- Pooled control DB: three devices; `synced_facts` contains 1,056 rows.
- Primary populated device shard: 1,056 facts, schema/user version 36, zero mutation-journal entries.
- Other device shards contain zero facts; one is schema 36, one schema 37.
- Current local source has 1,057 facts, so the snapshot mirror is already at least one fact behind.
- Matching counts are not semantic parity: content IDs, metadata, provenance, graph state, receipts, ordering, embeddings, and retrieval results were not compared.

## 5. Current semantic-memory journal defects

Source: `semantic-memory/src/journal.rs` and `knowledge.rs`.

1. V37 journal fields are only device, store, sequence, operation kind, payload, timestamp.
2. No payload digest, operation schema version, store epoch, writer epoch, predecessor digest, or durable authority ACK is stored.
3. Sequence allocation uses `MAX(sequence)+1`; correctness under multiple processes is not explicitly contracted or stress-tested.
4. Only the `add_fact` insertion path appends a journal record.
5. Payload is ad hoc JSON text, not an admitted canonical byte codec.
6. `replay_journal_entry` treats any existing sequence as `AlreadyApplied` without comparing operation kind or payload digest.
7. The integration test explicitly blesses same-sequence/different-payload as `AlreadyApplied`; this must be reversed to a typed conflict.
8. Replay remains caller-closure-driven instead of using a closed semantic-memory-owned dispatcher.
9. Export signals a gap only indirectly (`has_more=false`) and does not expose a typed gap/fork state.
10. Runtime identity is mutable after store open, while construction-time config fields already exist.
11. Mutation-route coverage is not enumerated; unjournaled writes can silently diverge.

## 6. Current Mnemes protocol assets worth preserving

The following source-present assets have focused test coverage and should be evolved rather than duplicated:

- `MemoryMutationEnvelopeV1`
- fixed-order domain-separated Ed25519 signing preimage
- canonical envelope digest
- payload digest/length checks
- closed signer-role/artifact matrix
- trusted-key scope and lifecycle validation
- replica state machine
- contiguous watermark primitive with predecessor digest
- same-identity/different-digest conflict detection

Limitations:

- Serde is explicitly projection-only; strict wire decoding is not implemented.
- Trusted-key registry is in-memory/test-only and empty in the default server.
- Protocol assets are not wired into the deployed sync endpoint.
- The envelope includes governance fields whose source authority and lifecycle are not yet wired; avoid fabricating placeholders merely to satisfy V1.

## 7. Transaction and authority constraint

The Mnemes pooled control DB and each semantic-memory device shard are separate SQLite databases. A correct authority apply must atomically bind:

- canonical semantic replay;
- inbox/idempotency evidence;
- contiguous stream-head advancement;
- payload/envelope digest;
- durable ACK material.

Do not fake cross-database atomicity. The high-ROI default is to keep replica inbox and stream-head tables in the same semantic-memory shard transaction as replay. Pooled control-plane/audit rows can be an append-after projection, reconciled from the shard receipt. If a different architecture is chosen, it must prove atomic commit or deterministic recovery across crashes.

## 8. Decisions the council must make

1. Exact immediate containment order for the two leaking local workers and two live remote sync routes.
2. One canonical plan/repository and how older plans become superseded without shadow truth.
3. Whether to evolve Envelope V1 or introduce V2; no placeholder governance fields.
4. Exact source outbox schema, sequence allocator, payload codec, stream epoch, and digest chain.
5. Exact authority inbox/head schema and same-transaction replay boundary.
6. Device-key provisioning, rotation, revocation, secure storage, and persistent registry.
7. Transport baseline: retain loopback + SSH confidentiality initially unless TLS adds a proved threat reduction.
8. Bootstrap strategy for the existing 1,056-row partial fact mirror.
9. Narrowest canary scope that proves real value without implying full-memory convergence.
10. Mutation coverage gate for expanding from `fact.create.v1` to broader semantics.
11. Observability/SLOs that distinguish healthy, lagging, gap, conflict, quarantined, and disabled.
12. Release/source/binary/lockfile parity and rollback requirements before live re-enable.

## 9. Non-negotiable acceptance properties

- No raw SQL or generic closure crosses the transport boundary.
- No caller-selected path escapes a server-owned shard map.
- Authenticated device identity must bind to envelope, stream, shard, and trusted key.
- Same identity + same digest is idempotent; same identity + different digest is a conflict.
- Gaps stop the stream; ACK advances only through the contiguous committed prefix.
- Replay + inbox + head + ACK material are one shard transaction.
- Local mutation + outbox record are one local transaction.
- Uncovered mutation routes are visibly rejected in required mode, not silently omitted.
- Replica read APIs cannot expose a mutable `MemoryStore` handle.
- Existing derived indexes are rebuilt, not replicated as authority.
- A running process or green unit suite does not license sync/reliability claims.
