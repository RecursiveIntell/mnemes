# Lane 5 — Certification, Canary, Rollout, and Rollback

> **For Hermes:** This lane owns test harnesses, receipts, operational docs, and release gates. It does not silently repair lane-owned source; return minimized failures to the owner.

**Goal:** Convert component-level implementation evidence into a real device/server canary and a defensible production-readiness decision.

**Architecture:** Certification observes the same canonical owner APIs, signed protocol, replicas, and supervised deployment that production will use. It proves durable effect/readback, recovery, freshness, and rollback—not merely unit tests or health endpoints.

**Files owned:** dedicated Mnemes e2e tests/fixtures, `docs/` runbooks, `scripts/` test helpers, deployment manifests/service examples, and evidence archives. Any source fix belongs to the owning lane.

**Prerequisites:** Gate 3 semantic owner, Gate 4 claim/evidence, Gate 6 runtime, and Gate 7 search must all pass in their own worktrees before certification changes a real server candidate.

## Task 5.1 — Build the acceptance matrix and disposable topology

1. Turn master §11 into one executable row per behavior: exact apply, duplicate, changed payload, gap, restart, key revocation, bootstrap, compaction, blob integrity, stale routing, ANN outage, local-search isolation, and rollback.
2. Create disposable primary/replica/control state directories with explicit IDs, keys, schema versions, and teardown policy.
3. Record package/dependency closure, source commits, candidate binary/config digests, service unit, data paths, disk headroom, and rollback binary before test deployment.

**Evidence:** a versioned `certification-manifest.json` with no secrets and a source/runtime identity receipt.

## Task 5.2 — Run canonical protocol and recovery matrix

1. Apply one real source mutation through Lane 1 journal export, Lane 2 V2 route, and fresh receiver readback.
2. Replay exact bytes; require durable `Duplicate` after sender and receiver restart.
3. Submit changed digest at same identity, gap, predecessor mismatch, malformed body, wrong scope/key/device/fence, and revoked key. Assert no semantic/inbox/head mutation.
4. Run claim/evidence object crash cases and claim checkpoint plus tail recovery.
5. Run bootstrap staging/verify/promote, live tail after cut, interrupted bootstrap, and paired-generation rollback.

**Gate:** every matrix row has command, expected outcome, durable state assertion, and saved receipt. A passing HTTP code is insufficient.

## Task 5.3 — Run search and load validation

1. Compare local search baseline against replication-load p50/p95/p99. Require no network frame in a local query trace and 5%/10% p95/p99 regression limits.
2. Measure federated p99 against the 2-second deadline with current, lagging, stale, slow, failed, and conflicting shards.
3. Verify fresh valid embedding is returned from exact tail before ANN catch-up; verify lexical-only pending state under embedding outage.
4. Verify incomplete results never become authoritative absence.

**Evidence:** raw latency distributions, workload seed, configuration/profile digest, per-shard outcomes, and current freshness heads.

## Task 5.4 — Run the 15-second SLO soaks

1. Execute a 24-hour two-device connected soak with representative mutation rate plus 5× bursts and concurrent p95/p99 search load.
2. Execute at least a 24-hour offline partition/catch-up test.
3. Execute a 72-hour multi-device soak after two-device acceptance.
4. Record every eligible `query_visibility_lag`; warn at 10 seconds, transition to degraded at 15 seconds, and treat any eligible breach as certification failure until root-caused/retested.

**Pass condition:** max eligible query visibility <15s, p99.9 <10s, zero lost mutations, zero false ACKs, zero unexplained root mismatch, and no unauthorized replica opening.

## Task 5.5 — Canary and supervised rollout

1. Deploy only a copied-data, loopback/tunnel-confined candidate first; do not touch device-primary stores.
2. Verify supervisor unit, PID/executable hash, ports, authenticated health, disabled legacy sync routes, negative admission, and actual candidate data path.
3. Admit one narrowly scoped test key, run the full canary matrix, revoke it, and prove new writes are rejected.
4. Enable shadow journaling, then a synthetic namespace, then one default store/device. Wait 24 hours before admitting its shard to default federated search.
5. Add devices one at a time. Keep V1 fact-create and prior binary/config until V2 passes the 72-hour soak.

## Task 5.6 — Rollback drill and final decision

1. Pause sender first; preserve local primary and spool.
2. Quarantine bad replica generation; restore control catalog and replica tree together to prior verified paired generation.
3. Restore prior supervised binary/config, verify process/data identity, and re-run local-only search checks.
4. Reconcile head/watermark/root before any retry; re-bootstrap rather than manually editing replica rows.
5. Publish a final gate table: verified, failed, skipped, or blocked. Only all-green Gate 8 permits the operationally accepted claim.

**Gate 8 commands:** run the exact package test suites, disposable canary harness, SLO report generator, and rollback drill script recorded in the manifest. Do not substitute a local unit-test pass for device/server evidence.

**Claim boundary:** before Gate 8 the strongest valid claim is protocol/component readiness or candidate-canary verification. After Gate 8 the claim is full-surface device-primary replication operationally accepted under the stated healthy-envelope assumptions—not an unconditional distributed-systems guarantee.
