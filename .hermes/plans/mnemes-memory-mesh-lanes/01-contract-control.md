# Lane 0 — Contract Control and Worktree Freeze

> **For Hermes:** Implement only in a dedicated clean worktree. This lane is the schema/protocol authority and must finish before parallel feature lanes edit code.

**Goal:** Freeze one strict, versioned V2 contract and reproducible baselines so every other lane interoperates without duplicate IDs, incompatible signature preimages, or shadow schemas.

**Architecture:** Evolve the existing Mnemes generic envelope and canonical signer into V2; retain V1 read/admission compatibility. `stack-ids` remains identity owner, `contract-schema-gen` is build/release tooling only, and semantic-memory/Mnemes runtime behavior is out of scope.

**Files owned:**
- `/home/sikmindz/Coding/Libraries/stack-ids/src/**` — only new replication/bootstrap opaque IDs and exports
- `/home/sikmindz/Coding/Libraries/contract-schema-gen/src/**`, `/home/sikmindz/Coding/Libraries/schemas/**`
- `/home/sikmindz/Coding/mnemes-replication/src/replication/types.rs`
- `/home/sikmindz/Coding/mnemes-replication/src/replication/canonical.rs`
- `/home/sikmindz/Coding/mnemes-replication/src/replication/wire_v2.rs`
- `/home/sikmindz/Coding/mnemes-replication/src/replication/error.rs`
- dedicated V2 protocol tests/fixtures only

**Do not edit:** semantic-memory mutation owners, Mnemes `store.rs`/`server.rs`, claim-ledger, search router, sender, or bootstrap runtime.

## Task 0.1 — Capture isolated baselines

1. Create separate clean worktrees from explicitly recorded commits for Libraries and Mnemes; do not reset the existing dirty trees.
2. Record scoped `git status --short`, HEAD, `Cargo.lock` digest, package feature list, and Rust version in `evidence/lane-0-baseline.md` inside the implementation worktree.
3. Record current V1 protocol fixture behavior before changing any public types.

**Evidence:** target-only status manifests and command output.

**Abort:** baseline cannot be cleanly isolated or a selected path dependency resolves outside the recorded worktree.

## Task 0.2 — Freeze V2 field ownership

1. Write `docs/adr/ADR-REP-V2-WIRE.md` in Mnemes defining `stream_kind`, component/store identity, epoch, sequence, predecessor digest, operation kind/schema, payload digest/length, dependency manifest digest, source commit time, scope, signer/key/fence, and signature.
2. Define one domain-separated fixed-order preimage using `stack_ids::DigestBuilder`; do not sign ad-hoc JSON map serialization.
3. Define exact size limits and unknown-field/trailing-byte rejection.
4. Define `Applied`, `Duplicate`, `Gap`, `Fork`, `EpochConflict`, `Blocked`, and `SchemaRejected` ACK states and their no-state-change rules.

**RED:** Add a test that changes one ordered field or payload byte and proves signature/digest verification fails.

**GREEN:** Implement the V2 preimage and validation only after the test is red.

## Task 0.3 — Add IDs without shadow ownership

1. Add `ReplicationStreamId` and `BootstrapId` opaque types in the owning `stack-ids` module.
2. Export them from `stack-ids/src/lib.rs` beside existing identity types.
3. Add parsing/display/serde round-trip tests; reject empty/invalid canonical forms.
4. Verify no new local `DeviceId`, `ScopeKey`, digest, trace, or bootstrap string-wrapper types are introduced in Mnemes.

**Run:** focused stack-ids tests, then `cargo test -p stack-ids` from Libraries.

## Task 0.4 — Implement strict V2 DTOs and decoder

1. Add `MemoryMutationEnvelopeV2` and typed component/operation discriminators to `types.rs`; preserve V1 types/readers unchanged.
2. Add `wire_v2.rs` with bounded frame decoding, declared payload length check, canonical payload digest check, strict version dispatch, and trailing-byte rejection.
3. Make `canonical.rs` the sole V2 signing/digest implementation.
4. Update `replication/mod.rs` exports only after the focused tests pass; coordinate this one registration edit with Lane 2 before it begins imports.

**RED tests:** unknown operation/schema, truncated frame, oversized length, wrong digest, signer scope mismatch, duplicate identity with changed digest.

**GREEN tests:** valid known fixture decodes, validates, signs, and round-trips with byte-identical re-encoding.

## Task 0.5 — Generate and lock schemas/vectors

1. Add schema export registrations in `contract-schema-gen`; do not add that crate to deployed runtime dependencies.
2. Generate committed JSON schemas for envelope, batch, ACK, heartbeat, bootstrap manifest/page, component status, and error outcomes.
3. Add Rust golden vectors plus one independent non-Rust verifier/vector producer under `tests/fixtures/replication-v2/`.
4. Add CI check that generated files equal committed files.

**Gate 1 commands:**
```bash
cargo test -p stack-ids
cargo test -p mnemes-replication replication_v2
cargo test -p contract-schema-gen
cargo fmt --check
```

**Gate 1 acceptance:** independent decoder/encoder fixtures yield identical digest/signature decisions; V1 fixtures still pass; no consumer begins runtime integration on an uncommitted V2 schema.

## Handoff to other lanes

Publish:
- ADR digest and schema-bundle digest;
- fixture directory and command receipt;
- public Rust API snippet for envelope validation and ACK interpretation;
- explicit list of intentionally deferred runtime behavior.

**Rollback:** V2 remains unadvertised and unused by production routes. Revert Lane 0 commits as one unit; V1 fact-create behavior remains intact.
