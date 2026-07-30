# ADR-0001: Historical Bootstrap and Live Epoch Rotation V1

**Status:** Proposed — design frozen; implementation and deployment blocked pending tests and review.

**Date:** 2026-07-30

## Context

The canonical local semantic-memory store contains a historical population of 1,113 facts, but its verified mutation journal contains zero verified mutation entries for that population. The typed Mnemes `fact.create` transport carries only canonical `FactCreateReplicaEnvelopeV1` values emitted by `semantic_memory::journal::export_verified_contiguous`. It must never synthesize equivalent journal records from historic fact rows.

`MemoryStore::apply_verified_fact_create` atomically owns a receiver inbox plus one active `(home_device_id, store_id)` stream epoch. It rejects an epoch change as `EpochConflict`. Therefore neither the generic Mnemes administrative bootstrap nor the typed fact-create HTTP route can safely implement historic data import or epoch rotation without an explicit authority-owned state transition.

## Decision

Create a separate, signed, quarantined **historical snapshot** protocol. It is not a fact-create journal protocol and does not reuse `SignedFactCreateBatchV1`.

### Canonical artifacts

```text
BootstrapManifestV1
  protocol_version = 1
  manifest_id
  source_store_id
  destination_device_id
  destination_store_id
  namespace_policy_digest
  source_schema_version
  ordered_fact_count
  ordered_fact_root
  page_count
  page_size
  source_exported_at
  signer_principal_id
  signer_role = RecoveryAuthority
  signer_key_version
  signer_public_key
  observed_at
  fencing_token
  signature

BootstrapPageV1
  protocol_version = 1
  manifest_id
  page_index
  page_digest
  entries: bounded ordered source facts with canonical fact identity and content digest
  signature binding to manifest digest

BootstrapAckV1
  protocol_version = 1
  manifest_id
  manifest_digest
  accepted_pages_root
  received_fact_count
  receiver_verified_root
  bootstrap_state = Installing | Complete | Rejected | Quarantined
  authority_receipt_id
  authority_observed_at
  signature

BootstrapRotationV1
  protocol_version = 1
  manifest_digest
  destination_device_id
  destination_store_id
  prior_bootstrap_root
  new_live_stream_epoch
  genesis_predecessor = semantic_memory::journal::GENESIS_PREDECESSOR
  fencing_token
  signer_principal_id
  signer_role = RecoveryAuthority
  signer_key_version
  signer_public_key
  observed_at
  signature
```

All signatures use fixed-order, length-prefixed binary preimages with distinct domain tags. JSON, if offered, is a bounded projection only.

### Receiver isolation

1. Bootstrap pages write only to bootstrap staging tables and a receiver-owned staging store. They do **not** populate the live fact table, live replication inbox, or live stream head while incomplete.
2. Each page transaction stores the page identity, page digest, page index, canonical fact rows, and count progress atomically.
3. The receiver verifies exact page ordering, no omitted/duplicate page index, no divergent duplicate fact ID, page roots, count, and final ordered fact root before exposure.
4. A manifest-digest collision with different signed content is a terminal rejection. A byte-equivalent replay is an idempotent duplicate.
5. The completion transaction promotes staged data only if all aggregate checks pass. It writes a durable `bootstrap_complete` receipt but writes no `FactCreateReplicaEnvelopeV1` inbox row.
6. A failed or incomplete bootstrap remains quarantined; it is not queryable through normal remote replica APIs.

### Live epoch rotation

1. Rotation is accepted only after an exact `BootstrapAckV1(Complete)` for the referenced manifest/root.
2. Rotation is an explicit semantic-memory receiver-owner API, not raw Mnemes SQL. It installs the new live epoch exactly once, with `next_sequence = 1` and `GENESIS_PREDECESSOR` as head.
3. The receiver records the manifest digest, root, previous active state, new epoch, fencing token, and rotation receipt durably in one semantic-memory transaction.
4. The first post-bootstrap typed fact-create request must be epoch `new_live_stream_epoch`, sequence `1`, predecessor `GENESIS_PREDECESSOR`; all other combinations reject.
5. No historical page becomes a live journal record. The live stream starts at the defined rotation boundary with a genuinely newly authored fact.

## Authority and scope

| Concern | Owner |
|---|---|
| Historical source enumeration and deterministic root | local semantic-memory bootstrap export API |
| Snapshot staging, completion, and receiver visibility | semantic-memory receiver bootstrap API |
| Bootstrap/rotation transport, authenticated device binding, public-key admission, durable transport receipts | Mnemes |
| Local signing key and sender spool | local controlled client |
| Production enablement | explicit human approval after canary evidence |

No private signing key is stored in Mnemes. Bootstrap admission is scoped to the destination device/store, namespace policy digest, one manifest or operator-approved export interval, lifecycle window, and fencing token.

## Required hostile tests before implementation can be certified

1. Altered manifest count/root/page size, signer, scope, or signature rejects before staging writes.
2. Reordered, omitted, duplicated, or divergent page rejects with no live visibility.
3. Same manifest identity with different digest rejects; exact retry is idempotent.
4. Interrupted page import resumes only from persisted page evidence.
5. Completion with count matching but root mismatch rejects.
6. Bootstrap data is invisible to normal receiver reads until completion.
7. Rotation before `Complete`, with wrong root, stale fence, wrong key role, expired/revoked key, or duplicate divergent receipt rejects.
8. Epoch rotation transaction survives a fresh semantic-memory reopen.
9. The first post-rotation fact-create is accepted only as `(epoch, sequence=1, GENESIS_PREDECESSOR)`; a previous epoch or different predecessor has no mutation effect.
10. Local canonical fact count/content remains unchanged through all remote bootstrap/canary tests.

## Consequences

- The 1,113 historical facts remain intentionally unsynchronized until this separate protocol is implemented and tested.
- This adds a semantic-memory receiver-state API and schema, so it requires a semver-published dependency before Mnemes release promotion.
- The existing Mnemes `bootstrap` CLI remains device/operator provisioning only and must not be extended into historical fact import.
- `mnemes-syncd.service` stays disabled. A future client can be enabled only after a post-rotation live fact completes signed transport, durable ACK, duplicate retry, authority restart, and fresh-process readback.

## Rejected alternatives

- **Fabricate V1 journal entries for old facts:** false provenance and unverifiable original sequence/time.
- **Bulk POST through typed fact-create:** violates the one-entry atomicity boundary and live-journal semantics.
- **Raw SQLite copy or SQL import:** bypasses semantic-memory ownership, receiver inbox, index, and receipt semantics.
- **Use count-only reconciliation:** cannot detect omission, reordering, or divergent duplicate content.
- **Silently accept epoch conflict:** would permit a forked receiver history.
