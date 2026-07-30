# Worker Receipt — `<lane> / <task>`

## Identity and scope

- Worktree: `<absolute path>`
- Repository HEAD, before: `<sha>`
- Repository HEAD, after: `<sha>`
- Allowed paths: `<list>`
- Actual changed paths: `<list>`
- Scope guard: `PASS | FAIL` with command output

## Behavioral contract

State the one behavior implemented, typed failures it must produce, and the exact invariant it preserves. Do not restate the entire lane plan.

## RED → GREEN evidence

| Stage | Command | Exit | Actual result | What it proves / does not prove |
|---|---|---:|---|---|
| RED | | | | |
| focused GREEN | | | | |
| regression | | | | |
| formatting/lint | | | | |

## Acceptance matrix

| Requirement | Status: verified/failed/blocked/skipped/not implemented | Test or durable readback evidence | Remaining gap |
|---|---|---|---|
| | | | |

## Changed-file rationale

| Path | Why this exact file is necessary | Contract owner |
|---|---|---|
| | | |

## Risks / unresolved / memory candidates

- Known unproven behavior:
- Test-environment blocker:
- `memory_candidates` (proposal only; no memory writes):
- Next smallest safe task:
