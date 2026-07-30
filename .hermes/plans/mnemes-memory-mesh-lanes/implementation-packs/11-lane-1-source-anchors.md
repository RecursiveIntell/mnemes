# Lane 1 Source-Anchor Pack — Semantic-Memory Canonical Owner

**Pinned source:** Libraries `fd6bdb7a5ff9b1755ae223a3b325a866366211d2`. Work only in a clean isolated Libraries worktree.

## Actual aggregate owners and test seams

| Aggregate | Current anchors | Focused tests | Worker boundary |
|---|---|---|---|
| Store/authority | `semantic-memory/src/lib.rs:675`; `knowledge.rs:873`, `961-962`, `1050-1052`, `1232-1234`, `1500-1502` | `authority_transactions.rs:25-239`; `origin_authority.rs` | Owner transaction only; no new storage authority |
| Journal/fact replay | `db.rs:538-557`; `journal.rs:22-114`, `345-347`, `452-453`, `547`, `729-730`, `769-770` | `journal_replication_e2e.rs`; `replica_fact_create_v39_contract.rs`; `replication_v38_contract.rs` | Use verified append/export APIs; legacy paths are compatibility-only |
| Facts/supersession | `types.rs:976`; `knowledge.rs:898-1017`, `1528`, `1633-1642` | `knowledge_tests.rs`; `authority_transactions.rs:77-166`; `forgetting_closure.rs` | supersede/tombstone, not physical ordinary delete |
| Documents/chunks | `types.rs:995`; `documents.rs:130-132`, `602-605` | `chunk_manifest_ingest.rs`; `chunker_tests.rs`; `storage_lifecycle.rs` | document+chunks is one aggregate; preserve cleanup |
| Conversations | `conversation.rs:14-339`, `456-693` | `conversation_tests.rs`; `conversation_search_tests.rs` | preserve chronological/token-budget behavior |
| Episodes | `types.rs:1148`; `episodes.rs:724-738` | `episode_identity.rs`; `step4_verification.rs:25-221` | stable explicit identity, trace/outcome fields |
| Graph edges | `types.rs:1398`, `1415`; `lib.rs:1819-1848`; `graph.rs`; `graph_edges.rs` | `integration_tests.rs`; `hardening_semantics.rs` | typed/evidence-bound edges only |
| Projection import | `projection_import.rs:57-129`, `225+`; `lib.rs:3621-3660`; `types.rs:779-926` | `projection_v11_tests.rs`; `import_boundary_tests.rs`; `import_ugly_cases.rs` | owner imports atomically; keep recorded/transformed/exported time separate |
| Provenance | `db.rs:380-403`; `provenance.rs:322-420`, `614-616` | `provenance_test.rs`; `trace_id_write_seam.rs` | append-only; provenance is not authority |
| Forgetting/root | `forgetting.rs:18-153`, `330+`, `928-937` | `forgetting_closure.rs`; `origin_authority.rs` | closure + derived invalidation + typed receipt |
| Procedural | `db.rs:779-847`; procedural module; public governed API | `procedural_memory.rs`; `transition_compiler.rs`; `state_epistemics.rs` | immutable artifacts and append-only event/receipt triggers |

## First worker tasks allowed

Only after Gate 1 + controller-created replication guard scaffold: one aggregate at a time, maximum three production files plus tests. Start with test-only fixtures or a single fact/document/conversation aggregate; do not ask a cheap worker to refactor journal, DB migrations, and several aggregate owners together.

## Compile and acceptance start

```bash
cargo test -p semantic-memory --no-run
cargo test -p semantic-memory --test <named-test>
```

Every replay task must show: source mutation → verified exact export → fresh receiver apply → duplicate no-op → reopen durable readback; negative malformed/gap/fork path changes neither aggregate nor stream head.

## Hard stops

No raw SQL replay, shadow journal, direct MCP-owned write path, fact deletion used as forgetting closure, payload-derived episode identity, projection timestamps collapsed, or derived index treated as canonical root. Missing permit/owner transaction is a block—not a reason to add adapter-local state.
