# Device-sharded semantic memory

> **Architecture status:** This document records the implemented server-side shard/router mechanics. Its server-authority interpretation is superseded by [Device-owned replicated memory mesh](DEVICE_OWNED_REPLICATED_MEMORY.md). The current writable shard handles are not admitted replicas; continuous synchronization is not implemented and production admission is **BLOCKED**.

## Decision

The current candidate uses one independently addressable semantic-memory SQLite file per registered device and a small routing catalog in `pooled.db`:

```text
BASE/pooled.db
BASE/memory/shards/<device_uuid>/memory.db
```

`semantic-memory` remains the engine owner of facts, documents, chunks, messages, embeddings, authority state, and child search receipts inside each shard. Under the target architecture, the home-device copy is canonical and this server-side file is its replica. `mnemes` owns device lifecycle, synchronization evidence, the derived shard catalog, sparse routing, global merge, and cross-shard routing receipts.

The active sharded runtime refuses to start if `BASE/memory/memory.db` exists. A legacy global store may exist only in a sealed rollback generation outside the live runtime directory.

## Research basis and claim boundary

This design combines established patterns without claiming that their published benchmark results transfer directly:

1. **Federated search resource selection.** A broker uses compact collection descriptions to select collections, performs local retrieval only in selected collections, and merges the returned rankings. See Shokouhi and Si, *Federated Search*, Foundations and Trends in Information Retrieval (2011): <https://www.microsoft.com/en-us/research/wp-content/uploads/2011/01/now.pdf>.
2. **Sparse mixture-of-experts routing.** Switch Transformers select a bounded expert subset per input to keep activation cost sparse; the paper also identifies routing instability and load balancing as real risks. See Fedus, Zoph, and Shazeer (JMLR 2022): <https://www.jmlr.org/papers/v23/21-0998.html>.
3. **Agent-memory shard routing.** ShardMemo models agent memory routing as masked sparse MoE: policy/scope eligibility before top-K/top-P activation, then per-shard retrieval and merge. Its latency/scan/F1 values are source-reported and are not evidence for this implementation until reproduced locally. See arXiv:2601.21545v1 (January 2026): <https://arxiv.org/abs/2601.21545>.
4. **SQLite atomicity constraint.** SQLite documents that transactions spanning attached databases are not crash-atomic as a set when the main database uses WAL; each file remains individually atomic. This runtime therefore does not use `ATTACH` for cross-shard writes or pretend that several shard commits are one transaction. See <https://www.sqlite.org/lang_attach.html>.

Here, “sparse attention” is an analogy for deterministic shard selection. It does not mean the router is a trained attention layer.

## Ownership and lifecycle

### `pooled.db`

`device_shards` is a rebuildable routing projection:

- validated device ID and deterministic relative path;
- active/quarantined/revoked state;
- generation;
- advisory routing terms and namespaces;
- content-free item counts;
- search count and EWMA latency.

`shard_routing_receipts` records:

- SHA-256 of the query, never raw query text;
- requester device;
- requested initial budget, actual selected-shard count, and exhaustive flag;
- eligible, ranked, selected, and skipped shards;
- each shard generation, latency, result count, child receipt ID, and error;
- fallback reason;
- ordered final result IDs, merge digest, and a canonical HMAC-SHA-256 receipt authenticator.

Receipts are validated before persistence and after retrieval. Eligibility, ranking, selection, skipping, outcome coverage, activation count, result-ID uniqueness, digest shape, and the keyed receipt authenticator must agree or the receipt is rejected as invalid. The 32-byte key is generated atomically at `BASE/.routing-receipt-hmac.key`, must remain mode `0600`, and is never stored in SQLite. A coordinated SQLite-row rewrite cannot forge the authenticator without that external key.

### Device shards

Registration/bootstrap creates one catalog row. Existing devices are backfilled additively. Paths are derived only from the validated UUID:

```text
memory/shards/<canonical-rfc4122-v4-uuid>
```

Revoked or quarantined devices are masked before scoring or opening. State changes update both the device and catalog state in one `pooled.db` transaction.

## Sparse routed search contract

1. Read the content-free catalog without opening shard databases.
2. Mask any non-active device or shard.
3. Tokenize the query, routing terms, and namespaces as lowercase ASCII alphanumeric terms.
4. Score lexical overlap plus a requester-locality prior.
5. Sort by score descending and device UUID ascending.
6. Select the explicit shard budget, or `min(2, eligible)` by default. Exhaustive mode selects all eligible shards.
7. Lazily open only selected shards through a bounded LRU (default capacity 4). All stores share one embedder instance.
8. Search the selected shards concurrently through canonical `MemoryStore::search_with_context` with child receipts.
9. Merge by score descending and canonical item ID. Identical duplicate ID/content values deduplicate. Same ID with different content fails closed.
10. If fewer than requested top-K results remain, expand through ranked shards one at a time and record `insufficient_results_expand`.
11. Persist the global routing receipt before returning.

Shard errors are visible in the routing receipt. The response can contain partial evidence, but it cannot silently claim every selected shard succeeded.

## API changes

Witnessed-search requests accept optional:

```json
{
  "shard_budget": 1,
  "exhaustive": false
}
```

Responses preserve existing result fields and add:

- `device_id`
- `shard_generation`
- `child_receipt_id`
- global `routing_receipt`

The old single child `receipt` field is intentionally removed because one shard receipt cannot witness a global merge.

Operator MCP adds:

- `sm_list_shards`
- `sm_refresh_shard_summary`

Integrity output is per shard. Health and normal stats use the catalog and cache metrics without opening every database.

## Migration

The staging command uses SQLite online backup, holds `BEGIN IMMEDIATE` on the pooled registry from membership capture through durable publication, requires an exact source for every registered device, validates schema generation, quick-checks source and target databases, compares content-free table counts, refuses overwrite, fsyncs files/directories, and atomically renames a complete staging tree followed by a parent-directory fsync:

```bash
python3 scripts/device-shard-migrate.py stage \
  --registry /path/to/pooled.db \
  --out /path/to/staged-device-shards \
  --expected-schema 36 \
  --source <laptop-device-uuid>=/path/to/laptop-final.db \
  --source <msi-device-uuid>=/path/to/msi-final.db
```

The manifest contains hashes, including a canonical SHA-256 witness of the locked registered-device set, schema generation, paths, quick-check status, and table counts only. It does not contain semantic content. Subset migrations are rejected; an intentional exclusion requires a future explicit exclusion/quarantine contract rather than omission.

### Cutover

1. Build and test release binaries.
2. Stage shards outside the live data directory.
3. Verify stage manifest and source envelope hashes.
4. Stop pooled-memory and prove zero DB handles.
5. Archive the complete data directory, including `.routing-receipt-hmac.key` when present, and current binaries.
6. Move old `memory/` to the rollback directory.
7. Move staged `memory/` into the live data directory.
8. Install new binaries atomically.
9. Start service. Opening the store transactionally admits pooled schema generation 1, validates required shard/receipt columns, and adds/backfills catalog rows.
10. Refresh content-free shard summaries.
11. Verify pooled and every shard integrity, authenticated MCP inventory, sparse and exhaustive retrieval, and receipt persistence.

### Rollback

1. Stop the sharded service.
2. Quarantine the sharded `memory/` directory and new `pooled.db` generation.
3. Restore the archived pre-cutover data directory and binaries together.
4. Start the old service and verify health, integrity, and a witnessed query.

Do not restore only `pooled.db` or only `memory/`; that would mix generations.

## Benchmark acceptance

Run identical query sets in sparse and exhaustive mode. Store raw machine-readable receipts locally. Report:

- result-ID recall@K against exhaustive baseline;
- exact top-K agreement;
- p50 and p95 latency;
- selected/opened shards per query;
- fallback rate;
- SQLite file descriptor count;
- process RSS;
- per-shard error rate;
- routing receipt completeness.

Acceptance gates:

- no same-ID/content conflict;
- 100% receipt completeness;
- no revoked/quarantined shard opened;
- cache occupancy never exceeds capacity;
- no unreported shard failure;
- sparse Recall@K target declared before measuring (initial real-data gate: 1.0 for the admitted query fixture);
- performance claims only when sparse mode beats exhaustive on measured p50 or p95 without violating recall.

Two real device shards prove runtime behavior but not scaling. An 8/16-shard synthetic fixture is required for scaling evidence. Published ShardMemo or MoE benchmark numbers remain external source reports.
