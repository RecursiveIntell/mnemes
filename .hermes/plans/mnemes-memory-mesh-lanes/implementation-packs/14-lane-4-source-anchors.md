# Lane 4 Source-Anchor Pack — Search and Freshness

**Pinned sources:** Mnemes `118121260899028b8d367398d813643dcd03fdcb`; Libraries `fd6bdb7a5ff9b1755ae223a3b325a866366211d2`.

## Actual search seams and current gaps

| Concern | Anchors | Current state / required change |
|---|---|---|
| Local hybrid search | `semantic-memory/src/search.rs:1-170` | BM25 + vector + RRF; must remain fully local |
| Witnessed response | `semantic-memory-mcp/src/server_stable.rs:585-825` | durable receipt/current state/replay availability; do not overclaim replayability |
| Supersession filter | `server_stable.rs:523-576` | stale results filtered before truncation |
| Shard eligibility | `mnemes/src/shards.rs:49-95` | active device/shard only; lacks source/applied/lexical/ANN heads |
| Deterministic routing | `shards.rs:104-181` | lexical overlap/locality, shard budget/exhaustive mode |
| Merge | `shards.rs:193-235` | same ID/different content currently fails globally; future behavior must isolate/quarantine and mark incomplete |
| Routing receipt | `shards.rs:238-275` | outcomes/selected/skipped/error are available but no freshness fields |
| Server conversion | `mnemes/src/server.rs:1892-1991` | routed result is converted to witnessed items with `receipt: None`, `receipt_stored: false`—must not be called durable global witnessed search |
| Requester binding | `server.rs:1923-1929` | explicit TODO: first registered device becomes requester; certification blocker |

## Test seams

- `mnemes/tests/device_shards.rs:97-242,334-553`
- `mnemes/tests/test_shard_benchmark.py:27-42`
- `mnemes/tests/server.rs:994-1042,1239-1361`
- `semantic-memory/tests/hostile_benchmark_receipt.rs:37-39`
- `semantic-memory/tests/import_ugly_cases.rs:192-193`

## Safe bounded worker tasks

After Lane 2 publishes a read-only status DTO: pure freshness classification; status/receipt serialization tests; stale/skipped/timeout/conflict test fixtures; exact-tail merge algorithm against mocked vectors. Workers may own `shards.rs`, new `search_freshness.rs`, new `search_tail.rs`, and dedicated tests only if the explicit task names no `server.rs`/`store.rs` edit.

## Hard stops

No local-search network client or sender/spool access. No embedding generation on query path. No complete/current/not-found result if any relevant shard is stale/skipped/failed. No direct claim ledger file read. No server/store registration edit by this lane—Lane 2 performs the reviewed integration. Do not solve conflict by silently picking one copy; preserve evidence and scoped degradation.
