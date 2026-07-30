# Baseline Identity — Captured 2026-07-30

This is a planning baseline only. Before assigning any worker, recreate clean isolated worktrees and re-run the worker preflight against the actual selected commits.

| Repository | Canonical planning checkout | Observed HEAD | Observed porcelain entry count | Relevant workspace packages |
|---|---|---|---:|---|
| Libraries | `/home/sikmindz/Coding/Libraries` | `fd6bdb7a5ff9b1755ae223a3b325a866366211d2` | 184 | `semantic-memory`, `claim-ledger`, `semantic-memory-mcp`, `stack-ids`, `contract-schema-gen`, `agent-graph`, `job-queue` |
| Mnemes | `/home/sikmindz/Coding/mnemes-replication` | `118121260899028b8d367398d813643dcd03fdcb` | 17 | `mnemes` |

## Consequences

- The canonical planning checkouts are concurrently mutable and dirty. They are **not** valid worker targets.
- A copied but dirty worktree is not sufficient. Use `git worktree add --detach <new-path> <recorded-commit>` or an equivalent clean clone, then run preflight.
- `Cargo.lock`, selected path dependencies, Rust toolchain, feature closure, test databases, and baseline command output must be recorded with each worker receipt. A worker’s self-report does not certify its result.
- No gate may be cited after source bytes change. Re-run gates from the final worktree generation.

## Known planning constraints

- Existing V1 fact-create behavior is a compatibility/canary baseline; V2 must be additive and disabled by default.
- Mnemes needs canonical semantic-memory public APIs. It must not recreate semantic write semantics with raw SQL.
- Claim/evidence truth remains a separate ordered component. Search is a consumer and local search must not call replication/network paths.
- The 15-second criterion is **query visibility**, not accepted HTTP or sender ACK. It remains unproven until the certification soak gates pass.
