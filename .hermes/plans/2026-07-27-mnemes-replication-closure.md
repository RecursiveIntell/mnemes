# Mnemes Replication Closure Plan

**Status:** executing

## Objective

Make Mnemes replication a required, durable, source-of-truth-safe capability for the registered local semantic-memory primary and the UNO Q authority. Completion means a fresh canonical fact mutation is atomically journaled, delivered in source-sequence order, applied by the authority exactly once, durably acknowledged, and observable after restart.

## Verified starting state

- The running `semantic-memory.service` is a known-good installed binary:
  `/home/sikmindz/.local/bin/semantic-memory-mcp`, SHA-256
  `02c12fa2ae05aaf8c2727923b1fcf218f76b9dea27f9579df30e673635f9a6c7`.
- Its process is active and its unit currently runs a full HTTP tool profile against
  `/home/sikmindz/.hermes/semantic-memory.db`.
- Source checkouts are heavily pre-existing dirty; no workspace-wide reset, format, or dependency rewrite is permitted.
- `semantic-memory` source is 0.6.0 and contains journaling/configuration work, but its manifest and public module exports no longer match the capability contract consumed by `semantic-memory-mcp`.
- `semantic-memory-mcp` points at that local 0.6.0 source and declares 0.5-era feature names. It cannot be deployed until the full advertised surface compiles and contract tests pass.
- `mnemes` is still linked to the 0.5 registry API. Its sync module intentionally refuses to replay digest-only `operation_journal` entries, which is correct fail-closed behavior.

## Non-negotiable invariants

1. The local semantic-memory primary remains the canonical writer for its device shard.
2. Every replicated mutation has exact canonical replay bytes plus a digest; digest-only records are never reconstructed into payloads.
3. Semantic mutation + source outbox insertion commit in one SQLite transaction.
4. Authority semantic apply + inbox/idempotency record + contiguous stream-head/ACK commit in one authority SQLite transaction.
5. Duplicate same-digest replay returns the original ACK; same sequence with a different digest is a typed conflict; a sequence gap is a hard degradation, never skipped.
6. The sender advances its persisted acknowledgement only after the authority ACK is verified.
7. The active service is replaced only by a build that preserves its admitted full MCP capability surface. A reduced `stable` profile is not a substitute.
8. No direct SQL fact replay and no live SQLite file copying.

## Allowlisted implementation surfaces

- `Libraries/semantic-memory/{Cargo.toml,src/lib.rs,src/journal.rs,src/db.rs,src/knowledge.rs,tests/*journal*,tests/knowledge_tests.rs}`
- `Libraries/semantic-memory-mcp/{Cargo.toml,src/main.rs,src/bridge.rs,src/server.rs,tests/integration.rs}`
- `mnemes/{Cargo.toml,src/sync.rs,src/sync_handler.rs,src/server.rs,src/store.rs,src/bin/mnemes-syncd.rs,tests/*sync*,install.sh,scripts/*service*}`
- `~/.config/systemd/user/mnemes-syncd.service` only during final deployment

## Phase 0 — preserve and establish a buildable contract

1. Capture per-repo scoped diffs and hashes for only allowlisted files.
2. Produce a machine-readable API/feature inventory from the MCP source and the semantic-memory public exports.
3. Reintroduce/port only the semantic-memory modules, dependencies, feature flags, and re-exports required by the currently advertised MCP full tool profile. Do not use stubs or hide tools.
4. Add compile-time feature-contract tests and run:
   - `cargo check` for semantic-memory
   - `cargo test --bin semantic-memory-mcp`
   - full MCP integration tests against a disposable store
5. Build a candidate binary and compare its MCP `tools/list` inventory against the running binary before any service change.

**Gate:** exact advertised tool family remains present; candidate opens the existing DB in read-only smoke mode; no service change yet.

## Phase 1 — complete canonical local outbox semantics

1. Treat `mutation_journal` as a replayable Mnemes outbox distinct from digest-only `operation_journal`.
2. Keep the existing `fact.create.v1` write-in-transaction behavior.
3. Inventory every canonical mutation API. Either:
   - emit its typed replayable journal entry atomically, or
   - reject it for Mnemes-required stores until its replay form exists.
4. Cover creation, in-place update/supersession, deletion/forgetting, and namespace deletion with explicit operation types and payload versions.
5. Add rollback, contiguity, duplicate, and unknown-operation tests.

**Gate:** a fresh disposable semantic-memory DB proves every admitted write leaves exactly one journal sequence; injected transaction failure leaves neither semantic change nor journal row.

## Phase 2 — authority typed apply plus durable ACK

1. Replace the current fact-list-only sync endpoint with a typed journal batch admission request that carries device ID, store ID, sequence, operation type, exact payload, and payload digest.
2. On the authority, authenticate/bind the sender to the home device and enforce expected sequence.
3. In one authority transaction: validate payload digest, detect duplicate/conflict, replay through the authority semantic API, write inbox/apply receipt, and advance contiguous ACK head.
4. Return an ACK containing the durable contiguous sequence and result IDs. Never acknowledge a gap or failed apply.
5. Add tests for duplicate delivery, altered duplicate, gap, apply failure rollback, and restart/retry.

**Gate:** a disposable authority replica survives duplicate and interrupted delivery with a single semantic result and a correct persisted stream head.

## Phase 3 — Mnemes-owned required sync worker

1. Add `mnemes-syncd` as a Rust binary in the Mnemes crate.
2. It reads the semantic-memory replayable outbox only through the canonical journal export API; it does not inspect `facts` to manufacture events.
3. It persists local send/ACK state, polls contiguous batches, uses bounded exponential retry with jitter, and marks gaps/conflicts as degraded exit state.
4. It has no `--disable-sync` path. Configuration absence is an explicit startup failure.
5. Add install/unit generation to Mnemes installation scripts. The unit uses `Restart=always` and credentials from mode-0600 files.

**Gate:** forced authority outage then recovery produces eventual ordered ACK without a skipped sequence; worker restart produces no duplicate semantic mutation.

## Phase 4 — cutover, deployment, and receipt

1. Build the MCP candidate and Mnemes server/worker from the exact reviewed sources.
2. Run all tests, formatting, and targeted lints. Cross-build the UNO Q server candidate.
3. Take SQLite online backups before replacements.
4. Deploy candidate server by stop → temp install → atomic rename → restart, retaining the old binary and unit state.
5. Update the semantic-memory service with required Mnemes identity only after the candidate MCP passes the compatibility and journal gate.
6. Enable the Mnemes-owned worker; retire the Python worker only after the new worker has a successful acknowledged end-to-end receipt.
7. Create one uniquely identifiable test fact through the real MCP write interface; verify local outbox sequence, remote authority record/count, durable ACK, and behavior after both worker and authority restart.

**Acceptance receipt:** exact source HEADs/scoped diffs, binary hashes, commands/results, local sequence, authority ACK, fact ID, remote verification query, restart proof, and rollback locations.

## Rollback

- Do not remove the current active MCP binary before candidate health and tool inventory checks pass.
- Preserve an online SQLite backup and the old executable before authority deployment.
- On any MCP capability regression, journal gap, replay conflict, or failed smoke test: stop the candidate, restore the previous executable/unit, restart, and retain journal rows for later diagnosis. Do not rewrite watermarks or stream heads manually.
