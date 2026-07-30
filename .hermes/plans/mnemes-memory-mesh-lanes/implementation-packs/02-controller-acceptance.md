# Controller Acceptance Checklist — Required After Every Cheap-Model Task

A worker saying “done” is not evidence. The controller must complete this checklist against the exact worker worktree before considering a task integrated.

## A. Identity and containment

- [ ] Worktree is an isolated clean worktree created from the receipt’s recorded commit.
- [ ] `git rev-parse HEAD` equals the worker’s initial and final recorded HEAD. Any changed HEAD means no-commit violation and invalidates results.
- [ ] Run `bash worker-final-guard.sh <worktree> <head> <receipt> -- <allowlist>`; save stdout/stderr.
- [ ] Read each changed source/test file. Confirm every edit belongs to the assigned behavior—not a cleanup, refactor, dependency upgrade, or scope expansion.
- [ ] Re-read any shared interface before applying controller integration. A later lane may have changed its state since the pack was generated.

## B. Contract verification

- [ ] Compare payload/envelope/operation changes to Gate 1 schema digest and golden vectors; rerun independent vector test if V2 bytes are involved.
- [ ] Confirm all canonical semantic writes use the owner transaction/guard and append/replay exactly one typed operation where required.
- [ ] Confirm each receiver ACK follows durable semantic effect plus inbox/head decision; HTTP response alone is not evidence.
- [ ] Confirm duplicate exact bytes are no-op and changed bytes at same scoped identity produce typed fork/quarantine.
- [ ] Confirm no raw SQL semantic replay, local identity/digest duplicates, client-selected shard path, silent fallback, or second journal/outbox entered the diff.

## C. Evidence ladder

- [ ] Review actual RED receipt or explicit reason a true RED could not be obtained.
- [ ] Re-run focused test(s) from the final source generation.
- [ ] Run package compile/check, relevant formatter check, and named regression suite in the prescribed order.
- [ ] Run one disposable process/store boundary test for stateful changes: create/apply/reopen/readback plus a negative no-mutation case.
- [ ] Record whether each claim is compile-verified, test-verified, process-verified, or still unverified. Do not promote tiers.

## D. Cross-lane handoff

- [ ] Verify public API/schema names match the lane-map handoff table.
- [ ] Produce a compact handoff manifest: API/signature, error/ACK variants, feature flags, test fixture path, schema/fixture digest, and compatibility note.
- [ ] Send it to consuming lane only after controller acceptance. Consumers must not infer missing behavior from types alone.
- [ ] If contract differs, stop both sides and return to Lane 0. Do not patch adapters that hide a mismatch.

## E. Lane-specific minimums

| Lane | Controller must additionally prove |
|---|---|
| 0 Contract | independent bytes/digest fixture, malformed/trailing decoder rejection, V1 compatibility, no unapproved V2 publication |
| 1 Semantic owner | atomic source journal and replay path, all requested aggregate tests, root/snapshot closure, tombstone/forgetting behavior |
| 2 Runtime | admitted key/device/scope checks, restart-safe sender/receiver/inbox/ACK, exact-byte retry, staged bootstrap promotion |
| 3 Claim/evidence | object fsync/rename ordering, ledger head continuity, object digest verification, trust degradation without recall outage |
| 4 Search | local search trace has no network path, stale/skipped response is incomplete, per-shard fault isolation, exact-tail before ANN catch-up |
| 5 Certification | real disposable topology, saved raw measurements, copied-data canary, 24h/72h evidence, paired-generation rollback drill |

## F. Merge and rollback

- [ ] Integrate only controller-accepted patches in a fresh integration worktree.
- [ ] Rerun affected tests after the final integration byte change. Earlier green output is invalid once files change.
- [ ] Keep original worker worktree and receipt until the next gate passes.
- [ ] Define disable/quarantine/revert path before enabling any feature flag or sender. Never use a live primary database to debug an integration failure.
