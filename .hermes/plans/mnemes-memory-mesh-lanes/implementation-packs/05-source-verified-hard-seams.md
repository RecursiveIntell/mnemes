# Source-Verified Hard Seams — Read Before Any Worker Edit

> **Evidence source:** direct read of the dirty planning checkouts on 2026-07-30. Re-verify line anchors in the isolated worktree before patching; this document does not override source.

## 1. Existing fact-create transport is deliberately narrow

- `mnemes-replication/src/replication/mod.rs:1-22` exports the current V1 primitives.
- `src/replication/fact_create.rs:1-25` states this transport is separate from `MemoryMutationEnvelopeV1`, carries the closed semantic-memory fact-create journal contract, and is capped at **one record per request** because receiver atomic batching does not yet exist.
- `fact_create.rs:83-132` copies exact journal payload/digests without reserializing and maps it into semantic-memory’s receiver envelope.
- `fact_create.rs:203-286` decodes strict JSON projection but signs a fixed-order length-prefixed preimage; JSON field order/format is not signed.

**Worker implication:** Do not widen this V1 type opportunistically. V2 must be additive and must not merge operator-governed `MemoryMutationEnvelopeV1` semantics into semantic-memory journal payloads.

## 2. Semantic-memory already owns fact journal truth and receiver atomicity

- `Libraries/semantic-memory/src/journal.rs:1-6` states exact typed payload bytes are canonical while embeddings/indexes are derived.
- `journal.rs:13-61` defines the present closed fact-create operation/schema, owner envelope, and durable outcomes (`Applied`, `Duplicate`, `Fork`, `Gap`, `EpochConflict`).
- `journal.rs:147-200` defines V38 stream state and V39 receiver inbox tables. The comments at `167-170` explicitly require fact state, stream advancement, inbox evidence, and ACK decision to commit in **one SQLite transaction**.
- `journal.rs:227-300` validates identity, operation/schema, payload/envelope digest, strict payload, and fact/namespace constraints before apply.

**Worker implication:** Mnemes authenticates/admit-checks transport, then calls canonical owner APIs. It must not replay semantic changes with raw SQL or maintain an independent authoritative head.

## 3. Current shard routing is not freshness-safe yet

- `mnemes-replication/src/shards.rs:49-68` catalog rows expose generation and `last_refreshed_at`, but no owner/applied/lexical/ANN sequence or head fields.
- `shards.rs:92-94` currently considers only active device/shard status for eligibility.
- `shards.rs:150-181` has shard-budget/exhaustive request controls.
- `shards.rs:193-230` currently returns a **global error** when equal item ID has different content across shards.

**Worker implication:** Lane 4 must not call a globally failed merge “safe.” Its contract is per-shard/item quarantine plus `incomplete=true`, freshness states, and explicit skipped/stale/failed outcomes. It must never make local search invoke replication/network work.

## 4. Existing test seams that build in the planning checkout

- `mnemes`: `cargo test -p mnemes --test device_shards --no-run` passed on 2026-07-30.
- `semantic-memory`: `cargo test -p semantic-memory --test chunk_manifest_ingest --no-run` passed.
- `claim-ledger`: `cargo test -p claim-ledger --no-run` passed, including artifact-envelope, audit-hardening, compaction, and ledger test targets.

These are starting probes only. Each future worker must rerun focused tests in an isolated worktree after its final source changes.
