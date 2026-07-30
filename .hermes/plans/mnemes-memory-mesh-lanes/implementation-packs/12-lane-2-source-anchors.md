# Lane 2 Source-Anchor Pack — Mnemes Runtime

**Pinned source:** Mnemes `118121260899028b8d367398d813643dcd03fdcb`. Use a clean detached Mnemes worktree; current checkout has 17 pre-existing porcelain entries.

## Current runtime seams

| Concern | Current anchors | Existing behavior to preserve |
|---|---|---|
| Router and limits | `src/server.rs:368-420` | Axum; 4 MiB fact-create body cap, 30s timeout, concurrency 64 |
| V1 canary route | `server.rs:404`, `436-539` | `POST /v1/replication/fact-create/v1`; decode → bearer/device → store apply → ACK |
| Disabled raw sync | `server.rs:405-406`; `sync.rs:1-86`; `sync_handler.rs:1-86` | `/v1/sync*` remain disabled; no raw journal payload revival |
| ACK projection | `server.rs:422-433` | `mnemes.fact-create.v1` currently uses accepted disposition for applied/duplicate |
| Control vs semantic store | `store.rs:370-396` | `pooled.db` control state; device shard `memory.db` canonical semantic state; no cross-DB atomicity claim |
| Admission | `store.rs:351-368`, `667-711`, `1554-1610` | privileged local/operator admission/revocation; never private key storage |
| Receiver | `store.rs:1612-1728` | request collision check, semantic owner `apply_verified_fact_create`, ACK projection afterward |
| Shard paths | `store.rs:1731-1760`; `shards.rs:49-95` | deterministic validated shard path; status/generation catalog |
| Legacy replica helper | `replica.rs:18-212` | helper is not a complete read-only guarantee |

## Absent features — create only under an approved Lane 2 task

No current `src/client/**`, `src/bin/mnemes-sync-agent.rs`, or `src/replication/bootstrap.rs` exists. Do not make a cheap worker assume or import them. The controller must first freeze interfaces, file ownership, persistence schema, and test fixtures.

## Test seams

```bash
cargo test --no-run
cargo test --test replication_fact_create_wire
cargo test --test replication_protocol
cargo test --test server
cargo test --test admin_cli
cargo test --test device_shards
cargo test --test canonical_journal_fact_create_canary
cargo test --test remote_candidate_fact_create_canary
cargo test --test replication_sync
```

## Safe cheap-worker tasks after interface freeze

- test-only route matrices around existing handler;
- test-only sender-spool restart/byte-identity fixture against a frozen trait;
- status DTO pure conversions;
- bootstrap malformed-manifest or rollback fixture once controller supplies a manifest API.

## Hard stops

Do not modify Lane 0 signed field order or V1 canary. Do not write semantic-memory SQL. Do not claim ACK is query visibility or Mnemes control ACK is one transaction with semantic apply. Do not re-enable legacy sync, derive payloads, select shard paths from the client, or create an unbounded client/spool worker without controller ownership.
