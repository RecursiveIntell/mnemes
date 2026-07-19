# pooled-memory

Multi-device pooled memory server with device/actor provenance, bitemporal lineage, and idempotent operation envelopes.

## What this crate does

`pooled-memory` adds a multi-device identity and operation layer on top of [`semantic-memory`](https://github.com/RecursiveIntell/semantic-memory). It enables a central memory server where multiple devices (laptops, servers, edge devices, phones) can share a single memory store while preserving:

- **Device identity** — which device observed or submitted each memory item
- **Actor identity** — which agent, process, or human was responsible
- **Operation provenance** — what operation was performed, when, and with what idempotency key
- **Bitemporal lineage** — when the observation was made versus when the server recorded it
- **Server-owned timestamps** — `recorded_at` is always stamped by the accepting server

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

The two databases are separate. `pooled.db` owns device/actor/operation metadata. `memory.db` owns memory content through `semantic-memory`.

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

## License

Apache-2.0
