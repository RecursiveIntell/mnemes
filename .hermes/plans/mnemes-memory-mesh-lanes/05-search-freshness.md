# Lane 4 — Search Isolation, Freshness, and Derived Index Readiness

> **For Hermes:** Own routed-search policy and freshness modules only. Do not change Mnemes server/control storage, sender/receiver logic, or semantic-memory internals.

**Goal:** Keep local search independent of replication while making federated search honest about replica freshness, partial coverage, vector readiness, and conflicts.

**Architecture:** Local semantic-memory reads local canonical state directly. Server federated search consumes Lane 2’s read-only replica status interface and Lane 1 content-generation API. It uses ANN plus a bounded exact recent-applied tail; it never waits for replication or triggers embedding work in the query path.

**Files owned:**
- `mnemes-replication/src/shards.rs`
- `mnemes-replication/src/search_freshness.rs` (new)
- `mnemes-replication/src/search_tail.rs` (new)
- routed-search/freshness test files

**Do not edit:** `src/store.rs`, `src/server.rs`, `src/replication/**`, `src/client/**`, Libraries files.

**Prerequisites:** Lane 0 Gate 1 for shared DTOs. Integration code begins only when Lane 2 publishes `ReplicaStatusReader`; full behavior needs Lane 1 content generation and Lane 2 durable visibility watermarks.

## Task 4.1 — Define freshness and completeness DTOs

1. Add typed freshness state: `Current`, `Lagging`, `OfflineUsable`, `Stale`, `Blocked`, `Quarantined`.
2. Extend routed response/receipt with device/store/generation, owner/applied/lexical/ANN heads, claim/trust head, reason codes, and `incomplete` plus selected/skipped/failed/stale/timed-out shard IDs.
3. Bind summary metadata to `summary_base_sequence` and summary digest; a summary cannot assert current routing beyond its source sequence.
4. Add strict/default/exhaustive policy behavior in isolated pure functions.

**RED:** a stale or skipped potentially relevant shard yields `incomplete=true`; empty incomplete result cannot serialize as authoritative not-found.

**GREEN:** current-only response remains complete only when all required current shards report compatible heads.

## Task 4.2 — Isolate per-shard failures and conflicts

1. Change global merge behavior so an item ID/digest conflict quarantines/reports only the conflicting item or shard; healthy shards still return results.
2. Ensure timeouts, open failures, cache invalidation failures, and unknown state become explicit shard outcomes rather than global error/empty response.
3. Add bounded federated deadline target of 2 seconds; slow shard cancellation must not consume local-search or healthy-shard permits.

**Tests:** one slow shard, one bad shard, one conflicting item, and one stale high-relevance shard with healthy result shards.

## Task 4.3 — Implement recent-tail semantic visibility

1. Consume applied/lexical/ANN status from Lane 2; do not write the status tables.
2. For rows in `(ann_indexed_sequence, lexical_visible_sequence]` with a matching embedding profile/digest, scan a bounded exact vector tail and merge with ANN candidates.
3. For no matching embedding, keep lexical visibility and mark `semantic_vector_pending`; never invoke an embedding model from a search request.
4. Bound tail row/byte/CPU cost. If the bound is exceeded, expose degradation and let the index worker catch up; never block all search on a global rebuild.
5. Key caches by shard generation plus canonical content generation and invalidate on generation/head changes.

**RED:** ANN outage or queue backlog makes fresh text disappear from semantic results despite a valid carried embedding.

**GREEN:** valid embedding is returned through exact-tail before ANN catches up; no embedding returns lexical result with explicit pending state.

## Task 4.4 — Prove local-search isolation and truthful routing

1. Add a local-search trace test proving no Mnemes client, sender spool, remote request, or federation permit is touched.
2. Add performance harness for baseline vs replication-load local p95/p99 and federated p99 deadline.
3. Add clock-skew test: sequence/head dominates freshness classification; wall-clock is secondary diagnostics.
4. Add trust lag behavior: ordinary recall remains available; trust-required action/assertion surfaces show unavailable/stale state.

**Gate 7 commands:**
```bash
cargo test -p mnemes-replication routed_search -- --nocapture
cargo test -p mnemes-replication freshness -- --nocapture
cargo test -p mnemes-replication search_tail -- --nocapture
cargo fmt --check
```

**Gate 7 acceptance:** local p95/p99 latency regression stays within 5%/10%; server results are complete only when coverage is current; stale/skipped results are explicit; ANN/embedder disruption cannot block canonical or lexical search; valid carried embeddings are query-visible via the exact tail before ANN catch-up.

## Handoff and rollback

Deliver a documented routing status matrix and performance harness to Lane 5. A rollback marks the generation stale/quarantined and disables it from default federation; it never makes local search dependent on the server or deletes device-primary data.
