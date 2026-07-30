# Lane 0 Source-Anchor Pack — V2 Contract Control

**Pinned sources:** Libraries `fd6bdb7a5ff9b1755ae223a3b325a866366211d2`; Mnemes `118121260899028b8d367398d813643dcd03fdcb`. Revalidate in a clean isolated worktree.

## Current anchors

| Responsibility | Current source anchors | Preserve |
|---|---|---|
| Generic V1 envelope and roles | `mnemes/src/replication/types.rs:73-346` | `MemoryMutationEnvelopeV1`; existing closed signer/artifact matrix |
| Generic canonical validation | `replication/canonical.rs:26-267` | fixed-order V1 digest/preimage and validation; registry admission remains caller-owned |
| V1 fact-create transport | `replication/fact_create.rs:17-25`, `83-286`, `289-361` | one-entry V1 limit; exact journal bytes; fixed binary signature preimage, not JSON order |
| Module registration | `replication/mod.rs:1-22` | all V1 exports and behavior |
| Semantic owner closed fact operation | `semantic-memory/src/journal.rs:13-127`, `227-300` | fact V1 semantics; V2 must add, not rename |

## Allowed initial task shape

A controller-grade worker may add **only** V2 DTO/schema/vector scaffolding after a written ADR fixes: field order, domain tags, length encoding, component identity, ACK/error variants, and V1 coexistence. Suggested allowed files must be explicit per task: `types.rs`, `canonical.rs`, `error.rs`, `mod.rs`, new `wire_v2.rs`, dedicated fixtures/tests, `docs/adr/**`, schema export files.

## Required tests

```bash
cargo test --test replication_protocol
cargo test --test replication_fact_create_wire
cargo test --no-run
```

Add a true independent golden vector before any V2 compatibility claim. Required negatives: altered ordered field, payload substitution, declared-length mismatch, unknown operation/schema, truncated/oversized/trailing frame, invalid role/artifact, changed scoped identity digest.

## Hard stops

- Do not modify `server.rs`, `store.rs`, `shards.rs`, sender/receiver/bootstrap, or semantic-memory mutation owners.
- Do not merge generic governed V1 envelope semantics into fact-create journal bytes.
- Do not sign serde JSON, add gRPC/QUIC/TLS scope, or make V2 active/default.
- If an API requires new identity/digest wrappers outside `stack-ids`, stop and return it to controller.

## Handoff artifact

Publish schema-bundle digest, exact vector directory, public DTO/error list, V1 compatibility test output, and a source-path manifest. Lane 1/2/3/4 cannot integrate before controller certifies these bytes.
