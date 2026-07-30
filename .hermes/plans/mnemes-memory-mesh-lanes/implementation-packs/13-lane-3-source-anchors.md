# Lane 3 Source-Anchor Pack — Claim Ledger and Evidence

**Pinned source:** Libraries `fd6bdb7a5ff9b1755ae223a3b325a866366211d2`. This lane has two truths: claim-ledger canonical events/evidence and semantic-memory derived trust enrichment. Do not conflate them.

## Canonical source anchors

| Concern | Anchors | Required invariant |
|---|---|---|
| Evidence models | `claim-ledger/src/types.rs:120-174` | source artifact/span/link/bundle retain digest and claim binding |
| Support/proof | `types.rs:189-249` | judgment binds evidence bundle, method, rationale, contradictions, proof debt |
| Event vocabulary | `ledger.rs:25-98` | ordered append-only events including `EvidenceAttached` and support/contradiction/proof-debt events |
| Head verification | `ledger.rs:597-676` | continuity + predecessor + recomputed entry digest + expected head |
| Artifact envelope | `envelope.rs:40-170` | digest before signature; signer/timestamp/policy admission distinct; only all-pass is fully verified |
| Compaction | `ledger.rs:914-1064` | expected pre-head, snapshot anchor, retained tail all verify |
| Provenance bridge | `semantic-memory/src/provenance.rs:362-424` | append-only provenance receipt; never a replacement ledger |
| MCP enrichment | `semantic-memory-mcp/src/server_stable.rs:222-264`, `727-729` | `ClaimTrustIndex` is derived; unavailable verification yields explicit `persisted_unjudged`/degraded trust while recall remains |

## Existing tests

```bash
cargo test -p claim-ledger --no-run
cargo test -p claim-ledger --test ledger_tests
cargo test -p claim-ledger --test audit_hardening
cargo test -p claim-ledger --test artifact_envelope
cargo test -p claim-ledger --test compaction
```

Also use `semantic-memory/tests/provenance_test.rs`, `evidence_gap.rs`, `authority_transactions.rs:24-68,110-130`, and `import_ugly_cases.rs:66-193` for bridge behavior.

## Safe bounded dispatches

After component storage API is frozen: evidence temp/fsync/rename crash tests; exact event/head verification test cases; compaction checkpoint + tail negative fixtures; trust-degradation response tests. Do not delegate `claim-ledger/src/ledger.rs` compaction or `server_stable.rs` integration changes without controller review.

## Hard stops

No direct JSONL/blob writes that bypass the component owner. No trust status inferred from a search score or from unverified evidence. No ordinary-recall outage when claim component is stale. No direct edit to Mnemes module registration/server/store by this lane; provide component DTO/tests and let Lane 2 integrate one reviewed call site.
