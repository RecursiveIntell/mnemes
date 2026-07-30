# Mnemes Memory Mesh — Execution Lane Map

> **Status:** Execution decomposition of `../2026-07-30-mnemes-full-surface-memory-mesh.md`; implementation remains blocked until Gate 1.
> **Observed planning baseline:** `Libraries@fd6bdb7` and `mnemes-replication@1181212` were dirty on 2026-07-30. Re-capture scoped baselines in clean worktrees before edits.

**Goal:** Deliver full-surface, device-primary memory replication without overlapping file ownership or weakening the 15-second query-visibility and search-isolation gates.

## Governing decisions

- Local device stores remain canonical; Mnemes stores authenticated replicas.
- V2 semantic mutations, claim-ledger events, and content-addressed blobs are separate ordered components with independent watermarks.
- `query_visibility_lag`, not HTTP acceptance or ACK alone, is the 15-second measure.
- No lane may add raw-SQL replica replay, reconstruct payloads from digests, or make local search call the network.
- No production route is enabled until Lane 6 certification passes.

## Lane graph

```text
Lane 0: contract + worktree control
                │ Gate 1
     ┌──────────┼──────────┐
     ▼          ▼          ▼
Lane 1       Lane 2      Lane 3
semantic     Mnemes       claim/evidence
owner        runtime      component
     │          │          │
     └──────┬───┴──────┬───┘
            ▼          ▼
       bootstrap    Lane 4: search/freshness
            └──────┬──────┘
                   ▼
          Lane 5: certification + rollout
```

## Ownership and gates

| Lane | Role | May start | Exclusive source ownership | Exit gate |
|---|---|---|---|---|
| 0 | Contract control | immediately | `stack-ids` replication IDs; generated schemas; Mnemes V2 protocol core | Gate 1: fixed V2 bytes/schema vectors |
| 1 | Semantic owner | Gate 1 | `Libraries/semantic-memory/**` | Gate 3: all canonical families replay/root-equal |
| 2 | Mnemes runtime | Gate 1, integration after Lane 1 API release | `mnemes/src/replication/**`, `src/client/**`, `src/server.rs`, `src/store.rs`, `src/bin/mnemes-sync-agent.rs` | Gate 6: restart-safe durable send/apply/ACK |
| 3 | Claims/evidence | Gate 1 | `claim-ledger/**`, `semantic-memory-mcp/**`, Mnemes claim component module only | Gate 4: blob/ledger crash safety and equal heads |
| 4 | Search/freshness | design after Gate 1; code integration after Lanes 1+2 expose interfaces | `mnemes/src/shards.rs`, `src/search_freshness.rs`, `src/search_tail.rs`, routed-search tests | Gate 7: no local-search network dependency; freshness-safe partial results |
| 5 | Certification/rollout | test harness early; full execution after Gates 3–7 | dedicated e2e tests, fixtures, systemd/runbook/evidence docs | Gate 8: real canary plus required soaks |

## No-shared-file rules

1. Lane 0 is the only writer of V2 envelope field order, ID names, schema JSON, fixed preimage, and cross-language golden vectors.
2. Lane 1 is the only writer in `Libraries/semantic-memory`. Lanes 2–4 consume public APIs or fixtures; they do not patch semantic-memory internals.
3. Lane 2 is the only writer of Mnemes server admission/control storage and sender/receiver runtime. It publishes a narrow `ReplicaStatusReader` / `ReplicaApplyNotifier` interface for Lane 4.
4. Lane 3 must not edit `mnemes/src/replication/mod.rs`, `server.rs`, or `store.rs`; it creates only its component module/tests and sends an integration checklist to Lane 2.
5. Lane 4 must not edit `server.rs` or `store.rs`; it consumes Lane 2 status interfaces and owns router behavior. Lane 2 performs the one planned registration/call-site integration after review.
6. Lane 5 does not repair production code opportunistically. A failing acceptance test is returned to the owning lane with a minimized receipt.

## Interface handoffs

| Producer | Consumer | Versioned handoff | Admission requirement |
|---|---|---|---|
| Lane 0 | 1–4 | `MemoryMutationEnvelopeV2`, schema bundle, vectors, explicit operation enum | Gate 1 digest/schema parity |
| Lane 1 | 2, 4 | owner `apply/export/snapshot/root/status` APIs and test fixture store | Gate 3 for full-surface use; Gate 2 for narrow receiver integration |
| Lane 2 | 3, 4 | component admission, durable receiver status, applied/lexical/ANN watermark reader | contract tests plus restart evidence |
| Lane 3 | 2, 5 | verified claim heads, blob manifest, compaction/bootstrap component | Gate 4 crash matrix |
| Lane 4 | 5 | routed-result completeness/freshness contract and latency harness | Gate 7 strict/partial behavior |

## Integration cadence

- **Daily:** each lane publishes only compile/test receipts and changed-path manifest; no cross-lane cherry-pick without owner review.
- **At each gate:** controller runs the published contract fixture against all consuming lanes.
- **After Gate 1:** Lanes 1, 2, and 3 work in parallel. Lane 4 may write isolated router tests and DTOs but cannot claim integration until the Lane 1/2 handoff exists.
- **After Gates 3, 4, and 6:** perform bootstrap promotion and search integration in a fresh integration worktree.
- **After Gate 7:** Lane 5 owns fault injection, canary, and soak execution.

## Shared abort conditions

Stop the affected lane and return to Lane 0 or the relevant owner if any of these occur:

- a requested feature requires a second truth store, untyped replay, or client-selected path;
- a new field changes signed bytes without regenerated vectors/schema approval;
- a source mutator cannot produce an exact typed operation atomically;
- a receiver ACK can advance without canonical apply plus durable inbox decision;
- a route can return authoritative empty results while potentially relevant shards are stale/skipped;
- any change would require editing another lane’s exclusive files.

## Council reconciliation

Agent Graph council `run-19fb51571dd-3` completed all three analyst lanes and synthesis in 52.8 seconds, but its receipt is `structural_unverified` and contains no source-witness envelopes. It is advisory only.

| Council suggestion | Controller decision | Reason |
|---|---|---|
| Six separated lanes | **Accepted** | It independently agrees with the master-plan decomposition and removes cross-lane ownership collisions. |
| Keep protocol control separate from semantic mutation ownership | **Accepted** | Gate 1 field/preimage/schema authority must not be edited concurrently with replay semantics. |
| Keep Mnemes sender/receiver/bootstrap together | **Accepted** | These share admission, spool, control-store, and restart semantics; splitting them would create a false ownership boundary. |
| Keep claims/evidence, search, and certification separate | **Accepted** | They have distinct truth, read-only query, and destructive-test concerns. |
| gRPC, QUIC, mTLS, gossip bootstrap, new standalone WAL directories, formal verification, 50k–100k TPS, and arbitrary latency claims | **Rejected** | None is in the verified current design or required for the stated device-primary HTTP/Ed25519/SQLite architecture. Adding them would create scope and ownership bloat. |
| Prohibit Mnemes from importing semantic-memory owner APIs | **Rejected** | The architecture explicitly requires canonical semantic-memory apply/export APIs; raw SQL is prohibited, not owner API use. |
| Search reads sender spool or ledger files directly | **Rejected** | Search consumes Lane 2’s read-only status interface and canonical replica state; it never reaches into spools/WALs. |

## Completion boundary

Lane completion is not production readiness. Only Gate 8, with real device/server canary, fault matrix, 24-hour two-device soak, 72-hour multi-device soak, and rollback drill, licenses an operationally accepted replication claim.
