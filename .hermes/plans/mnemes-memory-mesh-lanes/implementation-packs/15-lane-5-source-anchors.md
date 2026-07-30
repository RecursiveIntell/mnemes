# Lane 5 Source-Anchor Pack — Certification and Rollout

Certification is controller-owned. Cheap models may build disposable test harnesses and manifests only; they may not operate a real device/server, key lifecycle, supervisor, or production data.

## Existing evidence and release seams

| Surface | Anchors | Certification meaning |
|---|---|---|
| Semantic-memory governance gate | `semantic-memory/RELEASE_GATES.md:1-13`; `scripts/check_governance_test_gate.sh:13-20` | suite presence is not hosted CI or runtime proof |
| Current Mnemes limitation | `mnemes/docs/DEVICE_SHARDED_MEMORY.md:1-3` | writable shards are not admitted replicas; continuous sync/production admission blocked |
| Routing contract | `DEVICE_SHARDED_MEMORY.md:42-78` | partial evidence must not pretend all selected shards succeeded |
| Migration cutover | `docs/PHASE5_MIGRATION.md:3-34` | conflicting/secondary-only rows and quick-check failure block cutover |
| Bootstrap/epoch | `docs/adr/ADR-0001-historical-bootstrap-and-live-epoch-rotation-v1.md:84-120` | visibility receipt, incomplete quarantine, epoch rotation and hostile tests |
| Ownership | `ADR-0001:101-107` | semantic-memory exports/receives; Mnemes transports/binds devices/receipts; human approval required after canary |

## Required receipt binding

A certification row must bind actual source heads, package/dependency closure, semantic/claim roots, expected claim head/snapshot/tail, evidence object digests, device/store/key/epoch/fence, selected/skipped shard outcomes, local/routed receipt IDs, requester identity, state/retrieval epoch, model/config digest, query visibility measurements, and rollback generation.

## Test/harness tasks allowed for cheaper workers

- immutable acceptance-matrix generator;
- disposable store topology and teardown that refuses real paths;
- raw latency/lag measurement parser with no synthetic pass values;
- fault-test fixture scripts that target only temp directories;
- report generator that labels missing evidence `blocked` rather than green.

## Hard stops

No production deployment, admission/revocation, binary replacement, service restart, data migration, real primary database, or final pass/fail decision. Do not mistake a unit suite or healthy endpoint for replication/canary proof. Gate 8 requires real copied-data canary, negative admission/replay/reopen tests, 24-hour two-device soak, 24-hour offline catch-up, 72-hour multi-device soak, and paired-generation rollback drill.
