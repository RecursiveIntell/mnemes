# Supply-chain advisory record — 2026-11-26

This record extends the 2026-11-11 policy recorded in the Libraries repository
(`docs/supply-chain-deferral-2026-11-11.md`): vulnerabilities, unsound and
yanked advisories fail the `cargo deny` check; unmaintained advisories with no
patched release are deferred only with a recorded owner, dependency-graph inclusion paths,
and review date.

## Fixed

| Advisory | Crate | Change |
|---|---|---|
| RUSTSEC-2026-0592 (vulnerability: unbounded recursion in `Url::origin()` on nested `blob:` URLs) | url | 2.5.8 → 2.5.9 in `Cargo.lock` (with form_urlencoded 1.2.2 → 1.2.3) |

## Deferred (unmaintained, no maintained release)

| Advisory | Crate | Dependency-graph inclusion paths (not runtime reachability) | Alternatives |
|---|---|---|---|
| RUSTSEC-2026-0543 | chrono 0.4.45 | Direct dependency of mnemes; also Libraries semantic-memory, bitemporal-runtime, forge-memory-bridge, stack-ids, semantic-memory-forge; hf-hub's xet-data/xet-runtime | jiff, time |
| RUSTSEC-2026-0542 | humantime 2.4.0 | hf-hub → hf-xet → xet-runtime (and xet-client/xet-data) | — (follows the chrono migration upstream) |

Both are maintenance-status advisories, not known vulnerabilities. chrono is
used across the shared Libraries owner as well as here, so replacing it is a
cross-repository migration rather than a lockfile change in this repository.

## Exit criteria

Re-evaluate on any of: a maintained chrono successor release, a Libraries
migration away from chrono, an hf-hub/xet release that drops chrono or
humantime, or any new vulnerability advisory against either crate (immediate
action then).

Review owner: RecursiveIntell supply-chain lane. Review deadline: 2027-05-31. This is a review date, not an automatic expiry; reassess against the exit criteria above.
