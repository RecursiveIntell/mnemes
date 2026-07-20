# pooled-memory

Multi-device pooled memory server with device/actor provenance, bitemporal lineage, and idempotent operation envelopes.

## What this crate does

`pooled-memory` adds a multi-device identity and operation layer on top of [`semantic-memory`](https://github.com/RecursiveIntell/semantic-memory). It enables a central memory server where multiple devices (laptops, servers, edge devices, phones) can share a single memory store while preserving:

- **Device identity** — which device observed or submitted each memory item
- **Actor identity** — which agent, process, or human was responsible
- **Operation provenance** — what operation was performed, when, and with what idempotency key
- **Bitemporal lineage** — when the observation was made versus when the server recorded it
- **Server-owned timestamps** — `recorded_at` is always stamped by the accepting server

## Storage boundary

`pooled-memory` is additive metadata on top of `semantic-memory`:

- `pooled.db` — devices, actors, operation envelopes, and provenance edges.
- `memory.db` (`semantic-memory`) — facts/documents/episodes/chunks/messages/projections, embeddings, search indexes, and semantic content.

`pooled-memory` does **not** duplicate memory payload rows into `pooled.db`.

## What this crate does NOT do

- It does **not** replace `semantic-memory`. Devices that prefer local-only memory use `semantic-memory` directly without this crate.
- It does **not** duplicate claim-ledger trust authority. Claim/evidence adjudication remains in `claim-ledger`.
- It does **not** automatically trust model-extracted memories. Observations must be explicitly asserted or adjudicated.
- It does **not** replicate databases. One central server owns canonical state; clients read and write through the service.

## Architecture

```
pooled.db (device/actor/operation registry)
  ├── devices
  ├── actors
  └── operation_envelopes

memory.db (semantic-memory canonical store)
  ├── facts, documents, episodes, conversations
  ├── embeddings, FTS, vector indexes
  ├── provenance, temporal, authority
  └── search receipts
```

The two databases are separate. `pooled.db` owns device/actor/operation and provenance metadata. `memory.db` owns memory content through `semantic-memory`.

### Provenance schema

`provenance_edges` is added as an additive migration table:

```sql
CREATE TABLE IF NOT EXISTS provenance_edges (
  edge_id TEXT PRIMARY KEY,
  edge_type TEXT NOT NULL CHECK (
    edge_type IN ('observed_by', 'recorded_by', 'derived_from', 'supports',
                 'contradicts', 'supersedes', 'retrieved_from')
  ),
  source_kind TEXT NOT NULL,
  source_id TEXT NOT NULL,
  target_kind TEXT NOT NULL,
  target_id TEXT NOT NULL,
  operation_id TEXT REFERENCES operation_envelopes(operation_id),
  actor_id TEXT REFERENCES actors(actor_id),
  device_id TEXT REFERENCES devices(device_id),
  valid_from TEXT,
  valid_to TEXT,
  observed_at TEXT,
  recorded_at TEXT NOT NULL,
  content_digest TEXT,
  metadata TEXT,
  supersedes_edge_id TEXT REFERENCES provenance_edges(edge_id),
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  CHECK (length(source_kind) > 0 AND length(source_id) > 0),
  CHECK (length(target_kind) > 0 AND length(target_id) > 0),
  CHECK (valid_to IS NULL OR valid_from IS NULL OR valid_to >= valid_from),
  CHECK (metadata IS NULL OR json_valid(metadata)),
  CHECK (source_kind || ':' || source_id <> target_kind || ':' || target_id)
);
```

The bitemporal query predicate is:

```sql
recorded_at <= :as_of_recorded
AND (:as_of_valid IS NULL OR
     ((valid_from IS NULL OR valid_from <= :as_of_valid)
      AND (valid_to IS NULL OR :as_of_valid < valid_to)))
```

## Quick start

```rust
use pooled_memory::{PooledMemoryStore, Device, DeviceId, Actor, ActorKind, ActorId};
use semantic_memory::{MemoryConfig, EmbeddingConfig, MockEmbedder};
use tempfile::TempDir;

#[tokio::main]
async fn main() {
    let dir = TempDir::new().unwrap();
    let config = MemoryConfig {
        base_dir: dir.path().to_path_buf(),
        embedding: EmbeddingConfig { dimensions: 768, ..Default::default() },
        ..Default::default()
    };

    let store = PooledMemoryStore::open_with_embedder(
        dir.path().to_path_buf(),
        config,
        Box::new(MockEmbedder::new(768)),
    ).unwrap();

    // Register a device
    let dev_id = DeviceId::new();
    store.register_device(Device::new(dev_id.clone(), "laptop", "linux", "nobara-pc"))
        .await.unwrap();

    // Register an actor
    let actor_id = ActorId::new();
    store.register_actor(Actor::new(actor_id, dev_id.clone(), ActorKind::Hermes))
        .await.unwrap();

    // Access the underlying semantic-memory store
    let memory = store.memory();
    memory.add_fact("general", "Rust was first released in 2015", None, None)
        .await.unwrap();
}
```

## Local bootstrap admin

Use `pooled-memory-admin` for an offline bootstrap of a brand-new store:

```bash
pooled-memory-admin bootstrap <DATA_DIR> <LABEL> <PLATFORM> <HOSTNAME> [ACTOR_KIND]
```

`<ACTOR_KIND>` defaults to `human` when omitted.
The command exits non-zero on failure, including cases where a device already
exists in the data directory, and prints one JSON object on success:

```json
{"device_id":"...","actor_id":"...","credential":"...","profile":"operator","created_at":"..."}
```

Security guidance:

- Keep `<DATA_DIR>` under a directory that is owned by the local operator.
- Restrict directory permissions to owner-only access (`0700`), especially when
  secrets are present in memory snapshots or logs.
- Keep the credential output from untrusted terminals or logs; it is only ever
  shown on standard output for this command and should be treated as
  single-use sensitive material.

Example:

```bash
chmod 700 /var/lib/pooled-memory
pooled-memory-admin bootstrap /var/lib/pooled-memory laptop linux host.example.com
```

## License

Apache-2.0
