# Cheap-Model Implementation Packs — Controller Entry Point

> **Purpose:** Make the six memory-mesh lanes safe enough for lower-cost implementation models without granting architectural, release, deployment, or cross-lane authority.
> **Source authority:** `../00-lane-map.md` and `../../2026-07-30-mnemes-full-surface-memory-mesh.md`. This directory is an execution aid, not competing architecture.

## Non-negotiable controller model

- A worker receives **one lane**, one clean isolated worktree per repository, one pinned commit, one explicit allowlist, and one timebox.
- Workers may write only the allowlisted paths and only in their isolated worktrees. They must not commit, push, install, activate services, change global Hermes/MCP configuration, touch real device-primary data, or use a destructive cleanup command.
- Workers implement a finite task from the corresponding lane plan. They do not reinterpret the architecture, add transport/protocol features, or make public/reliability claims.
- A controller, not the worker, owns cross-lane integration, contract changes, test acceptance, conflict resolution, merge/cherry-pick, canary, and rollback decisions.

## What is ready now

- `00-worker-contract.md`: copy into every worker prompt.
- `worker-preflight.sh`: proves the worker is in the intended worktree at the pinned HEAD and prints scoped initial state.
- `worker-final-guard.sh`: rejects a moved HEAD, out-of-scope changes, whitespace errors, or an absent test receipt.
- `receipt-template.md`: exact handoff shape.
- `10-lane-0-source-anchors.md` through `15-lane-5-source-anchors.md`: source-verified entry points, tests, and stop conditions for every lane.
- `02-controller-acceptance.md`, `04-dispatch-order.md`, and `06-worker-prompt-template.md`: controller-only acceptance, model-tier assignment, and ready-to-dispatch task shape.

## Required controller sequence

1. Pick the **smallest task block** from one lane plan—not an entire lane.
2. Create clean worktrees at recorded commits. Do not delegate into either currently dirty canonical checkout.
3. Copy the lane plan, this contract, exact source-anchor pack, and an allowlist into the worker prompt.
4. Run `bash worker-preflight.sh <worktree> <expected-head> -- <allowed paths...>` before worker edits.
5. Require RED → minimal GREEN → targeted regression. A pre-existing green suite does not close a requested behavior.
6. On return, run `bash worker-final-guard.sh ...` and independently rerun the named tests from the same worktree.
7. Record every acceptance row as verified, failed, skipped, blocked, or not implemented. Only then integrate via a controller-owned worktree.

## Branching rule

Lane 0 must produce Gate 1 before workers can implement runtime integration. After Gate 1, Lanes 1, 2, and 3 can work in parallel on non-overlapping paths. Lane 4 cannot claim integrated behavior until it consumes published Lane 1/2 interfaces. Lane 5 is test/evidence-only until all functional gates pass.

## Stop conditions

Stop immediately and return a blocker instead of guessing if the worker discovers: a missing or incompatible public API; a required shared-file edit; a mutation path that bypasses the canonical owner transaction; a different signed-byte contract; a failing test environment; an unexpected HEAD; or a need to use a live server/device database.
