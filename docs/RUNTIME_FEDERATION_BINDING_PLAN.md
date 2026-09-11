# Runtime Federation Binding — Detailed Implementation Plan

**Status:** Plan complete; runtime federation implementation intentionally not started; locally implemented control-plane contract at `0abac8446f6c2aa4630aa10088d58b17f22ab1ac`
**Plan owner:** Mnemes source owner (`RecursiveIntell/mnemes`)
**Evidence cutoff:** 2026-09-10T20:18:47Z
**Source snapshot:** implementation commit `0abac8446f6c2aa4630aa10088d58b17f22ab1ac`; the exact current publication candidate is recorded by the bounded publication receipt, not inferred from this evolving plan document.
**Plan artifact:** `docs/RUNTIME_FEDERATION_BINDING_PLAN.md`

> **Hard exclusion:** The actual `NousResearch/hermes-agent` repository and PR #94878 are not part of this plan's mutation scope. No source, branch, PR body, comment, merge, or other external state in that repository may be changed by this workstream.

## 1. Executive decision

Implement runtime federation as a **profile-subject → explicitly authorized store → canonical semantic-memory query** pipeline, with transport identity, authorization state, store ownership, routing, and replication evidence kept separate.

The first implementation should remain local/loopback and use the existing Mnemes device credential plus authenticated actor binding. Remote federation should be a later, separately admitted transport phase using audience-bound resource authorization and sender-constrained or workload identity credentials. Do not make a bearer token, a profile ID supplied by a caller, a routing score, a namespace discovery result, or a search receipt authoritative by itself.

The decisive invariant is:

```text
authenticated device + authenticated actor
    -> canonical active actor/profile binding
    -> control-plane authorization snapshot
    -> authorized store set
    -> lifecycle/freshness/schema mask
    -> bounded routing over that set
    -> read-only canonical semantic-memory search
    -> global receipt bound to the authorization snapshot and child receipts
```

A request that cannot establish every link fails closed. There is no fallback from a failed profile binding to a sibling profile, root profile, global store, or unrestricted device-shard search.

## 2. Current source truth

### 2.1 Existing Mnemes implementation

Observed in the isolated source snapshot:

- The candidate adds locally tested profile/store/grant control-plane types and persistence helpers. These are not yet consumed by runtime profile-bound routing or HTTP/MCP search.
- `src/profile_store.rs` now owns `MemoryProfileId`, `MemoryProfile`, `MemoryStoreIdentity`, `MemoryAccessGrant`, lifecycle states, bounded effects/time windows, and the pure deterministic authorizer.
- `src/store.rs` owns the `pooled.db` schema and currently persists `memory_profiles`, `memory_stores`, and `memory_access_grants`.
- Profile/store registration checks active device ownership and path safety.
- Grant issuance checks that the referenced persisted issuer is an active actor with the operator tool profile; authenticated request-context binding remains a later integration gate.
- Authorization checks requester profile, requester device, target store, target owner profile/device, namespace, effect, validity interval, and revocation.
- Behavioral coverage exists for explicit-grant denial, effect/namespace/time bounds, profile-subject mismatch, path traversal, revoked store/profile/device, and operator-only issuance.
- `src/server.rs:696-704` authenticates a bearer device credential and optional actor ID, returning a `ServerContext` containing device and actor. It does not resolve a memory profile.
- `src/server.rs:2099-2198` runs witnessed search. Its sharded branch chooses a requester device, calls device-based `routed_search`, and does not apply profile grants. Its legacy branch calls the synchronous `memory()` accessor.
- `src/server.rs:1757-1936` advertises device/actor/search/operator tools, but no profile/store/grant tools.
- `src/store.rs:2072-2114` exposes deterministic device shard paths and a legacy writable `memory()` accessor. The latter is not an admissible federated-replica query surface.
- `src/store.rs:2821-2940` routes over `DeviceShard` rows, selects by device/lifecycle/routing terms, searches selected shards, merges results, and writes a routing receipt. The route key is a device ID, not a memory store ID or profile authorization snapshot.

### 2.2 Existing architecture constraints

The current Mnemes documentation establishes these source-owner boundaries:

- `semantic-memory` owns canonical semantic content, mutation/replay semantics, authority, and local search semantics.
- Mnemes owns device/actor lifecycle, grants, synchronization evidence, routing projections, and global routing receipts.
- Server shards are replayable per-device replicas, not unrestricted second writers.
- A writable global `memory/memory.db` is forbidden in the active sharded tree.
- Routing relevance cannot grant access.
- Search receipts prove retrieval execution, not truth or permission to act.
- Replication requires typed canonical mutation payloads; a digest-only journal cannot be converted into invented replay data.

### 2.3 Current gap

The current implementation has a grant control plane but no runtime consumer. Specifically:

1. An authenticated actor has no canonical memory-profile subject binding.
2. A search request can identify a device but cannot establish which profile is acting.
3. A device shard is not canonically mapped to a profile-owned store.
4. `routed_search` ranks device shards before any profile/store grant filtering.
5. Search receipts do not bind an authorization snapshot, profile subject, store grant IDs/digest, or profile-owned store identity.
6. The current HTTP/MCP path has no profile/store/grant lifecycle API.
7. Remote trust-domain federation, peer key lifecycle, and sender-constrained transport identity are not implemented.

These are observed source facts. They are not a claim that the current Mnemes server is already profile-federated.

## 3. Research basis and decisions

All web research in this section was performed through K(e)enable on 2026-09-10. Search snippets are discovery evidence; the linked documents were fetched where material.

### 3.1 MCP authorization: transport authentication is separate from application authorization

Primary source: [MCP Authorization, 2026-07-28](https://modelcontextprotocol.io/specification/2026-07-28/basic/authorization).

Relevant source-reported requirements:

- An HTTP MCP server acts as an OAuth resource server; the authorization server issues tokens.
- Servers must use Protected Resource Metadata (RFC 9728) for authorization-server discovery.
- Clients must use Resource Indicators (RFC 8707) to identify the intended MCP resource.
- Bearer authorization is sent on every HTTP request, not in a query string.
- The resource server validates that the token was issued for its own audience/resource.
- Scope challenges are authoritative for the current operation.
- STDIO MCP implementations should not use the HTTP OAuth flow and instead obtain credentials from the environment.

**Decision for Mnemes:** keep transport authentication and memory-profile authorization as separate layers. If Mnemes later exposes remote MCP, add resource/audience validation and per-request token validation; do not encode a profile ID in a token and assume that token validity grants access to every store. The profile/store grant check remains authoritative for the exact request.

### 3.2 OAuth token exchange: preserve subject and actor distinction

Primary source: [RFC 8693 OAuth 2.0 Token Exchange](https://datatracker.ietf.org/doc/rfc8693).

The RFC distinguishes:

- impersonation: actor is effectively indistinguishable from the subject within the granted context;
- delegation: actor retains its own identity and acts on behalf of the subject;
- composite tokens can represent both subject and actor;
- resource, audience, and scope are distinct request dimensions;
- token syntax and deployment trust models remain deployment-specific.

**Decision for Mnemes:** model runtime requests as explicit delegation, not impersonation:

```text
subject_profile_id = the memory profile whose authority is being exercised
actor_id           = the authenticated Hermes/process/human actor
requester_device   = the device carrying the actor
resource_store_id  = the exact memory store being accessed
requested_effect   = search | read | write
```

Every receipt and permit must preserve both subject and actor. Never collapse the actor into the profile or let a profile ID supplied by a caller manufacture a delegation relationship.

### 3.3 Resource indicators and sender constraint

Primary sources:

- [RFC 8707 Resource Indicators for OAuth 2.0](https://rfc-editor.org/rfc/rfc8707.html)
- [RFC 9449 DPoP](https://rfc-editor.org/rfc/rfc9449)
- [RFC 9700 OAuth 2.0 Security BCP](https://www.rfc-editor.org/info/rfc9700/)

These sources support explicit resource/audience binding, protection against token replay through sender-constrained tokens, PKCE and modern OAuth security practices, and caution around dynamic trust relationships.

**Decision for Mnemes:**

- Phase 1: retain the existing local device credential path; bind it to an actor and profile in Mnemes control-plane state.
- Phase 2: for remote HTTP federation, choose one admitted transport profile: mTLS/SPIFFE or OAuth resource indicators plus DPoP. Do not implement two remote trust stacks simultaneously.
- Remote access tokens must be audience/resource bound to the Mnemes endpoint, short-lived, and revalidated for every request. A long-lived bearer credential is not an acceptable federation permit.
- DPoP/mTLS authenticates the sender; it does not replace store/profile grant evaluation.

### 3.4 SPIFFE federation: keep trust domains and bundles distinct

Primary sources:

- [SPIFFE Federation](https://spiffe.io/docs/latest/spiffe-specs/spiffe_federation/)
- [SPIFFE JWT-SVID](https://spiffe.io/docs/latest/spiffe-specs/jwt-svid)
- [SPIFFE Identity](https://spiffe.io/docs/latest/spiffe-specs/spiffe-id)

SPIFFE defines separate trust domains, explicit bundle endpoints, stable endpoint configuration, periodic bundle refresh, key lifecycle, and a requirement that foreign bundles remain associated with their originating trust domain and not be merged into one undifferentiated bundle. JWT-SVID validators require a subject and audience.

**Decision for Mnemes:** if SPIFFE is selected for remote workload identity:

- add explicit `FederationTrustDomain` and `PeerBundle` records;
- preserve `<trust_domain, bundle>` association;
- validate SVID subject and audience before actor admission;
- refresh bundles with sequence/freshness evidence;
- rotate and retire keys deliberately;
- never treat possession of a foreign bundle as a memory grant;
- never merge foreign bundles into a single global trust set.

### 3.5 Zanzibar/ReBAC and OpenFGA: relationship graphs need consistency witnesses

Primary sources:

- [Zanzibar: Google’s Consistent, Global Authorization System](https://zanzibar.tech/)
- [OpenFGA Contextual Tuples](https://openfga.dev/docs/interacting/contextual-tuples)

Zanzibar uses object-relation-subject tuples and emphasizes causal ordering, snapshot consistency, and avoiding stale authorization decisions relative to content changes. OpenFGA contextual tuples are request-local, validated against the authorization model, and not persisted; token claims used as contextual tuples can remain effective until token expiry even after the underlying relationship changes.

**Decision for Mnemes:** use a small typed relationship model, not a generic authorization product or a free-form graph:

```text
actor --bound_to--> profile
profile --owns--> store
profile --granted--> store#effect@namespace
store --represents--> semantic-memory replica generation
```

Persist durable ownership and grants in Mnemes. Treat request-local query constraints as ephemeral context only. Every route must capture an authorization snapshot ID/digest and compare it against lifecycle/grant epoch at admission. Cached route summaries cannot grant access and must be invalidated when profile, grant, store, device, schema, epoch, or redaction state changes.

### 3.6 UCAN and Macaroons: attenuation is useful, but do not add a second authority language casually

Primary sources:

- [UCAN Delegation Specification](https://ucan.xyz/delegation/)
- [Macaroons: Cookies with Contextual Caveats](https://research.google.com/pubs/pub41892.html)

UCAN defines cryptographically verifiable delegation envelopes with issuer, audience, subject, command, policy, validity, and proof-chain semantics. Its subject-null “powerline” is intentionally powerful and must not be used as a root delegation. Macaroons demonstrate chained caveats that attenuate authority by purpose, time, location, and context.

**Decision for Mnemes:** do not import UCAN or Macaroon syntax into the first implementation. The existing typed `MemoryAccessGrant` is the canonical policy owner. If remote delegated permits are later required, define one versioned `MemoryAccessPermitV1` that is either:

- issued and verified by Mnemes against the canonical grant snapshot; or
- an explicitly adopted standard capability envelope with a one-way adapter.

The adapter may translate; it may not become a second policy owner. Delegation can only narrow authority: store, namespace, effect, time, actor, audience, and budget must be no broader than the parent grant. No wildcard/powerline root is admitted.

### 3.7 A2A: authenticate at the HTTP boundary; authorize data and actions separately

Primary source: [A2A Enterprise Features](https://a2a-protocol.org/latest/topics/enterprise-ready/).

A2A describes opaque agents that do not share internal memory/tools, establishes identity at HTTP/transport rather than in JSON-RPC payloads, requires server-side authentication on every request, and calls for granular skill/data/action authorization and least privilege.

**Decision for Mnemes:** treat a remote memory peer as a resource server/client, not as a trusted peer that receives unrestricted database access. The remote protocol may carry typed actor/profile/store references for correlation, but the server must derive and validate the effective subject from authenticated identity and control-plane binding. Do not expose semantic-memory internals or arbitrary SQL to peers.

### 3.8 TUF: key compromise and stale metadata are operational problems

Primary source: [The Update Framework specification](https://theupdateframework.github.io/specification/latest/).

TUF separates roles, supports delegated/threshold trust, addresses key compromise, freeze/rollback/mix-and-match attacks, and explicitly keeps security decisions separate from file transport.

**Decision for Mnemes:** borrow the operational pattern for federation key/bundle distribution only if remote federation is admitted: versioned metadata, freshness/rollback checks, role separation, revocation, and a recovery path. Do not use TUF metadata as semantic-memory authority or as a replacement for Mnemes grants.

### 3.9 Local-first synchronization: use typed semantic operations, not generic CRDT authority

Primary sources:

- [Automerge documentation](https://automerge.org/docs)
- current Mnemes `docs/DEVICE_OWNED_REPLICATED_MEMORY.md`
- current semantic-memory public documentation and local source dependency

Automerge demonstrates transport-agnostic synchronization and deterministic conflict handling for its document model. That does not make it the authority model for semantic-memory facts, supersession, redaction, forgetting, provenance, or claim adjudication.

**Decision for Mnemes:** retain the current device-primary/replica model. Replicate typed canonical semantic-memory operations through the existing `operation_journal`/fact-create path and future owner APIs. Never use generic row convergence, relevance rank, or replica freshness as truth promotion.

## 4. Frozen contract

### 4.1 New canonical identity relations

Add a versioned control-plane binding relation:

```text
ActorProfileBindingV1
- binding_id
- actor_id
- profile_id
- owner_device_id
- valid_from
- expires_at
- revoked_at
- issued_by_actor_id
- binding_epoch
- binding_digest
- recorded_at
```

Rules:

1. `actor_id` must exist and be active.
2. `profile_id` must exist and be active.
3. `owner_device_id` must equal the profile owner device for phase 1.
4. The actor's authenticated device must equal `owner_device_id` for phase 1.
5. A binding is valid only inside `[valid_from, expires_at)` and when not revoked.
6. Binding replacement is append-plus-revocation or epoch advancement; no destructive overwrite.
7. Resolution is deterministic: one active binding for `(actor_id, at)` or typed ambiguity denial.
8. A caller cannot select a profile by sending an arbitrary profile ID.
9. Profile switching uses a new actor or an explicit operator-issued binding transition; it does not mutate historical requests.
10. The binding digest participates in the authorization snapshot digest.

The binding table is the canonical owner of actor-to-profile subject selection. Do not add a `profile_id` field to multiple unrelated tables as competing truth.

### 4.2 Store ownership and physical mapping

The current `memory_stores` table becomes the canonical logical store registry. Add or enforce:

```text
MemoryStoreIdentityV2
- store_id
- profile_id
- owner_device_id
- namespace
- canonical_relative_path
- store_epoch
- replica_generation
- schema_generation
- status
- created_at
```

Rules:

1. The physical path is derived by one canonical builder from validated `store_id`/profile identity; callers do not choose arbitrary active paths.
2. The path must remain relative, traversal-safe, and inside the admitted Mnemes store root.
3. The mapping `(store_id, canonical_relative_path)` is unique.
4. A store belongs to exactly one profile and owner device per generation.
5. A profile may not silently acquire a second active store for the same namespace/generation.
6. A new restore/promotion creates a new `store_epoch` or explicit generation transition; it does not silently replace the old authority.
7. Existing device-shard rows are treated as a rebuildable compatibility projection until each row has a canonical `store_id` mapping.
8. No federated route may open a store that lacks a canonical mapping.

Phase 1 should either enforce one profile-owned store per device or explicitly migrate the shard cache key from `DeviceId` to `StoreId`. Do not pretend a device-level shard is profile-isolated when two profiles can share it.

### 4.3 Grant and permit separation

Retain `MemoryAccessGrant` as the durable policy record. Add a request-time, non-authoritative derived permit:

```text
MemoryAccessPermitV1
- permit_id
- subject_profile_id
- actor_id
- actor_device_id
- store_id
- namespace
- effect
- grant_ids
- authorization_snapshot_id
- authorization_snapshot_digest
- issued_at
- expires_at
- query_budget
- source_policy_version
- permit_digest
```

The permit is a projection of canonical grants and current lifecycle state. It is not allowed to grant more than the grants that produced it. It is single-request or short-lived; no indefinite cache.

A permit must be rejected when:

- actor/profile binding is missing, expired, revoked, or ambiguous;
- requester device is inactive;
- subject profile is inactive;
- target owner profile/device is inactive;
- store is quarantined/revoked or generation/schema is inadmissible;
- namespace/effect/time is outside the grant;
- authorization snapshot is stale or digest-mismatched;
- requested budget exceeds policy;
- a peer/audience/key binding does not match the authenticated transport.

### 4.4 Federated search request

Introduce a new API rather than widening the existing device-only method invisibly:

```text
routed_search_for_profile(
    actor_id,
    request: ProfileRoutingSearchRequest,
) -> ProfileRoutedSearchResponse
```

`ProfileRoutingSearchRequest` contains query, namespace filters, source filters, result limit, shard/store budget, completeness mode, freshness bound, and request identity. It does **not** contain an authoritative profile selector.

Resolution order:

1. authenticate transport/device;
2. resolve actor and active `ActorProfileBindingV1`;
3. create authorization snapshot over grants and lifecycle state;
4. enumerate only stores authorized for `search` and requested namespaces;
5. mask inactive/quarantined/stale/schema-incompatible stores;
6. route only within the authorized set;
7. open only read-only replica/query handles;
8. delegate search to canonical semantic-memory APIs;
9. merge results with full conflict scanning before top-K truncation;
10. persist a global routing receipt with authorization and child evidence;
11. return results plus explicit completeness/failed-store state.

No-result behavior must distinguish:

- `not_found_in_searched_authorized_stores`;
- `incomplete_authorized_store_set`;
- `no_authorized_stores`;
- `authorization_denied`;
- `store_unavailable`.

The server must never answer “not found” when required authorized stores were not searched.

### 4.5 Receipt contract

Extend the global receipt with:

```text
- requester_actor_id
- subject_profile_id
- binding_id / binding_digest
- authorization_snapshot_id / digest
- authorized_store_ids or privacy-preserving store-set digest
- grant_ids or grant-set digest
- requested_effect
- requested_namespace_set_digest
- store generations/epochs/schema generations
- selected/skipped/failed store outcomes
- child semantic-memory receipt IDs
- completeness and freshness outcome
- final result IDs
- merge digest
- receipt digest/authenticator
```

The raw query remains excluded unless explicit replay mode is selected. A receipt is retrieval evidence, not claim truth or action permission.

### 4.6 Runtime API exposure

Operator-only control-plane APIs:

- register memory profile;
- bind/unbind actor to profile;
- register/revoke/quarantine store;
- issue/revoke grant;
- inspect current authorization snapshot;
- inspect store-to-replica mapping;
- rotate/revoke federation peer keys.

Grant issuance checks an active operator actor. The current public lifecycle helper methods do not themselves accept an authenticated actor context and are not exposed by the current HTTP/MCP surface; operator-authenticated lifecycle APIs remain a future integration gate.

Agent/read APIs:

- profile-bound witnessed search;
- own profile/store metadata subject to redaction policy;
- authorization-denied and incomplete evidence without leaking ungranted store contents.

Do not expose raw SQL, arbitrary relative paths, or a generic “grant any profile” tool.

## 5. Implementation phases

### Phase 0 — Contract and schema freeze

**Owner:** Mnemes control-plane source (`src/profile_store.rs`, `src/store.rs`)

Actions:

1. Add `ActorProfileBindingV1`, binding status/epoch/digest types, and strict validation.
2. Add canonical store-path builder and store epoch/generation fields.
3. Add schema migration with append-preserving lifecycle records.
4. Add `AuthorizationSnapshotV1` derivation over binding, grants, profiles, stores, devices, policy version, and evaluation time.
5. Define typed errors for missing/ambiguous binding, stale snapshot, unauthorized store, incomplete store set, and inadmissible replica.
6. Populate schema/owner maps and document the one-authority model.

RED tests:

- unbound actor cannot resolve a profile;
- actor bound to another device fails;
- duplicate active bindings fail closed;
- revoked/expired binding fails;
- arbitrary profile ID never changes the resolved subject;
- path builder rejects traversal/absolute paths and produces one deterministic path.

Acceptance:

- migration reopens cleanly;
- old rows remain recoverable;
- exact binding/snapshot digests are stable;
- no server route consumes profile grants yet.

Rollback:

- disable only the new binding feature flag/route;
- retain new rows and receipts;
- do not delete or rewrite existing device/actor/grant records.

### Phase 1 — Profile-bound local authorization

**Owner:** `src/store.rs`, `src/profile_store.rs`, control-plane tests

Actions:

1. Implement `bind_actor_profile` and `resolve_actor_profile`.
2. Require active authenticated actor/device for binding issuance.
3. Enforce operator authority for binding transitions.
4. Implement `build_authorization_snapshot` and `issue_memory_access_permit`.
5. Add `list_authorized_stores` with deterministic ordering and no ungranted-content leakage.
6. Reuse the existing grant authorizer; do not duplicate grant semantics in HTTP handlers.

Acceptance:

- same-profile own-store access works;
- other-profile access requires exact grant;
- search/read/write effects remain distinct;
- namespace/time/revocation/device/profile/store lifecycle checks are enforced;
- authorization snapshot is immutable for the request and has a verifiable digest;
- grant revocation affects new requests immediately;
- prior receipts remain readable and historical.

### Phase 2 — Store-to-replica ownership and read-only query surface

**Owner:** `src/store.rs`, `src/shards.rs`, canonical semantic-memory integration boundary

Actions:

1. Add `store_id` as the runtime routing key.
2. Build one canonical `store_memory(store_id)` resolver from `memory_stores`, not caller paths.
3. Replace federated uses of writable `memory()` with a read-only query handle or an explicit semantic-memory API that enforces query-only mode.
4. Map or migrate existing device shards to canonical profile stores.
5. Refuse federated routing for unmapped/legacy global stores.
6. Bind store epoch, replica generation, schema generation, and lifecycle to the route admission check.
7. Keep device-shard compatibility projections rebuildable and non-authoritative.

Acceptance:

- a profile-owned store opens only through its canonical store ID;
- two stores cannot alias one physical path;
- revoked/quarantined stores are never opened;
- a query cannot mutate a replica or trigger schema migration;
- stale schema/generation/epoch fails closed;
- reopened stores preserve identity and lifecycle.

Rollback:

- keep profile-bound route disabled;
- quarantine new store mappings, not old replicas;
- restore pooled DB and replica tree as one generation if a migration is attempted.

### Phase 3 — Profile-filtered routed search

**Owner:** `src/shards.rs`, `src/store.rs`, `src/server.rs`

Actions:

1. Add `routed_search_for_profile`.
2. Resolve profile from authenticated actor binding, never from a free request field.
3. Obtain authorization snapshot and permitted store set before ranking.
4. Filter lifecycle/freshness/schema before score/rank/selection.
5. Route only authorized stores; use store ID as the stable routing identity.
6. Preserve bounded sparse selection, exhaustive mode, explicit fallback, and full conflict scanning.
7. Extend routing receipts with subject/binding/authorization/store evidence.
8. Make incomplete authorized-store coverage explicit.
9. Keep old device-only `routed_search` as a compatibility/test path only; do not advertise it as federated authorization.

Acceptance tests:

- unauthorized store never appears in selected/opened/outcome sets;
- a routing-term hit cannot overcome missing grant;
- grant revocation between two requests changes the selected set;
- profile switch changes the subject only through a new valid binding;
- same actor cannot request another profile by payload substitution;
- sparse and exhaustive modes operate on the same authorized set;
- same item ID with different content fails before top-K truncation;
- partial store failure returns incomplete evidence, never silent absence;
- receipt read-back verifies authorization snapshot and store-set coverage.

### Phase 4 — HTTP/MCP integration

**Owner:** `src/server.rs`, server integration tests

Actions:

1. Extend `ServerContext` with resolved profile binding and authorization subject.
2. Change profile-bound MCP search to require an authenticated actor; no actor means typed denial for the governed route.
3. For REST search, require an actor identity through the existing explicit request/authentication mechanism, validate it against the bearer device, and derive the profile server-side. Do not trust a `profile_id` request field.
4. Add operator-only profile/binding/store/grant lifecycle tools and schemas.
5. Add read-only authorization/store inspection with leakage-safe responses.
6. Keep HTTP status semantics distinct: 401 transport failure, 403 valid identity but denied effect, 409 conflict/stale snapshot, 422 invalid scope, 503 unavailable/incomplete dependency where appropriate.
7. Add per-request audit events with actor, subject, store-set digest, outcome, and receipt ID; never log credentials or raw query by default.
8. If remote MCP is later enabled, add RFC 9728 protected-resource metadata, RFC 8707 resource audience, scope challenges, and exact issuer/resource validation.

Acceptance:

- tools/list exposes only tools appropriate to actor/operator profile;
- tools/call cannot bypass profile binding by changing arguments;
- REST and MCP use the same store-owned authorization path;
- every successful federated search has a global receipt;
- denied requests have no semantic-memory side effects;
- session reauthorization observes revocation and binding epoch changes.

### Phase 5 — Typed remote federation transport

**Owner:** new topical federation module plus server transport owner; no semantic-memory shadow owner

This phase is not admitted until Phases 0–4 pass.

Actions:

1. Choose one remote transport identity profile: SPIFFE/mTLS or OAuth resource indicators + DPoP.
2. Add explicit peer/trust-domain registry, bundle/key version, refresh, expiry, and revocation records.
3. Validate remote workload subject, audience/resource, peer status, key version, and transport binding before actor admission.
4. Exchange only typed search/replication requests; never raw SQL or writable database handles.
5. Bind remote requests to subject profile, actor, store set, grant snapshot, deadline, and query/result budget.
6. For replication, require canonical semantic-memory owner-produced payloads, signatures, sequence/predecessor, store epoch, writer epoch, fencing token, and durable ACKs.
7. Keep foreign trust bundles separate by trust domain.
8. Add replay/duplicate/fork/gap/revocation/rollback handling.

Acceptance:

- forged audience, sender, peer key, profile, store, namespace, grant, or epoch is denied;
- token/key revocation blocks new requests and persistent sessions at the declared boundary;
- expired/stale bundle or permit fails closed;
- peer cannot use a search response as write authority;
- a replica cannot be promoted without explicit operator promotion/fencing;
- all remote operations have durable receipts and provenance references;
- no trust-domain bundle merge occurs.

### Phase 6 — Adversarial, recovery, and operational acceptance

**Owner:** Mnemes + independent security/federation reviewer before remote admission

Actions:

1. Run a hostile read-only audit across HTTP auth, binding, grants, store mapping, router, receipts, key lifecycle, and replication.
2. Attack confused deputy paths: caller-supplied profile/store, stale permit, stale route cache, namespace alias, path traversal, actor/device mismatch, revoked peer, replayed token, old epoch, stale bundle, incomplete store set.
3. Test crashes at each cross-database boundary.
4. Test restart, WAL recovery, key rotation, grant revocation, device quarantine, profile revocation, and replica quarantine.
5. Verify no live service or profile home is modified during offline tests.
6. Produce exact source/build/runtime receipts.

Final admission requires every required negative witness and every recovery drill to pass. A green unit suite alone is not sufficient.

## 6. Validation matrix

| Gate | Required evidence | Owner | Failure policy |
|---|---|---|---|
| V0 source/owner freeze | exact HEAD, dirty state, owner map, schema diff | controller | stop; no implementation |
| V1 binding correctness | unit + reopened SQLite tests | Mnemes control plane | fail closed |
| V2 grant semantics | own/foreign/effect/time/revoke/device/profile/store matrix | profile-store module | no route activation |
| V3 snapshot integrity | deterministic digest, stale/replay/epoch tests | control plane | deny stale snapshot |
| V4 store mapping | canonical path, alias/traversal, read-only handle tests | store/shard owner | quarantine unmapped stores |
| V5 route authorization | unauthorized stores never opened/selected | router owner | disable profile route |
| V6 receipt completeness | global + child receipts, store outcomes, authorization digest | receipt owner | no successful response |
| V7 HTTP/MCP parity | same authorization decisions across transports | server owner | disable affected route |
| V8 remote identity | audience/subject/key/bundle/peer lifecycle | federation owner | no remote admission |
| V9 replication semantics | canonical payload, signature, sequence/gap/fork/idempotency/reopen | semantic-memory + Mnemes | block replication |
| V10 recovery | revoke, restart, quarantine, rollback, promotion fencing | operations owner | retain quarantine |
| V11 performance/recall | sparse vs exhaustive over authorized set, after correctness | evaluation owner | no performance claim |

Use `scripts/run_tests.sh`/project-native test commands as applicable. For Rust, the minimum local baseline remains:

```bash
cargo fmt --all -- --check
cargo check --locked --all-targets
cargo test --locked --all-features
cargo clippy --locked --all-targets --all-features -- -D warnings
```

Add targeted tests per phase before broader matrices. Do not use source-text tests to prove wiring.

## 7. Rollback and quarantine

### Code rollback

- Keep implementation in an isolated Mnemes feature branch/worktree.
- Each phase is separately revertible.
- Do not reset or clean unrelated worktrees.
- Revert the phase commit, then rerun the prior phase's matrix.

### Data rollback

- Never mutate live profile homes or live memory stores during this plan.
- Schema changes use additive migrations and preserve prior rows.
- Store/path migration stages outside the active tree.
- Restore pooled control-plane DB and replica tree as one generation.
- Do not restore only `pooled.db` or only `memory/`.
- Preserve failed receipts, stale bindings, revoked grants, and quarantine records.

### Remote rollback

- Disable remote federation admission before deleting peer/key metadata.
- Revoke peer credentials and permits first.
- Quarantine affected peer/store generations.
- Reopen the last admitted source/replica generation and verify health, integrity, and a witnessed local query.
- Promotion requires a new writer epoch and fencing of the old generation.

## 8. Non-goals and forbidden shortcuts

- Do not modify the actual Hermes repository or PR #94878.
- Do not change Ares-specific behavior to match a generic Hermes PR.
- Do not make profile selection a caller-supplied authority field.
- Do not use namespace names, routing terms, relevance scores, or search results as grants.
- Do not retain a writable global semantic-memory fallback in the federated route.
- Do not copy live SQLite/WAL files as synchronization.
- Do not create a second semantic mutation journal in Mnemes.
- Do not invent replay payloads from digests or projections.
- Do not merge trust-domain bundles.
- Do not use an unrestricted wildcard/powerline delegation.
- Do not add a second authorization DSL without an explicit owner decision.
- Do not advertise remote federation before transport, grant, replica, receipt, and recovery gates pass.
- Do not run benchmarks until the authorized-store correctness gate passes; benchmark results are not authority proof.
- Do not activate live services, credentials, profile homes, or memory stores as part of this plan.

## 9. Specialist/profile routing

No specialist was needed to produce this plan: current Mnemes source, its existing architecture documents, and the requested K(e)enable primary-source research were sufficient to decide the ownership and sequencing questions.

Before Phase 5 remote federation implementation, route a narrow independent security/federation review. If the current profile set has no specialist with cryptographic federation and distributed authorization expertise, create a dedicated profile rather than asking a generalist to cover that lane superficially. The new profile should review only:

- transport identity and audience binding;
- trust-domain/bundle/key lifecycle;
- attenuation and replay semantics;
- confused-deputy and stale-authorization attacks;
- required negative witnesses.

That review is a gate challenge, not an authority grant and not a replacement for controller validation.

## 10. Cheapest falsifying experiment

Before implementing remote federation, implement only Phases 0–3 against two disposable local stores and two disposable profiles:

1. Bind actor A to profile A.
2. Register store A and store B under separate profiles/devices.
3. Issue search grant A→B for one namespace and a short interval.
4. Run profile-bound sparse and exhaustive search.
5. Assert store B is selected only inside the grant window.
6. Revoke the grant and repeat without restarting the server.
7. Revoke/quarantine the target device/store and repeat.
8. Attempt caller-supplied profile substitution, namespace widening, stale receipt replay, and route-cache reuse.
9. Reopen the control-plane DB and verify binding/grant/store lifecycle and receipt digests.

**Falsifier:** any unauthorized store is opened, selected, or represented as searched; any revocation is ignored within the declared boundary; any request can choose a profile without a valid binding; any receipt cannot explain the authorized set; or any test requires a global writable store.

This experiment is cheap, local, reversible, and directly exercises the current missing seam. It should precede remote OAuth/SPIFFE work and all benchmark work.

## 11. Revisit conditions

Revisit this plan when any of the following changes:

- semantic-memory exposes an admitted canonical replica/query API;
- Mnemes changes the store identity from device-level to profile/store-level;
- the MCP HTTP authorization contract changes;
- a remote peer/trust-domain transport is selected;
- profile sharing across multiple devices becomes a requirement;
- grant consistency requires a distributed authorization service rather than the current local control plane;
- a production or external-publication claim is requested.

Until then, the strongest supportable state is:

```text
Mnemes profile/store/grant control plane: implemented on an isolated local candidate; exact publication-head validation remains required.
Local actor-bound, grant-filtered profile routing: implemented on the isolated candidate with negative authorization, revocation, partial-store, and receipt read-back tests.
HTTP/MCP profile-bound route: not implemented or active.
Remote federation: not admitted.
Production readiness: not claimed.
```

## 12. Research source ledger

| ID | Source | Type/date | Plan use | Evidence state |
|---|---|---|---|---|
| R1 | MCP Authorization, modelcontextprotocol.io/specification/2026-07-28/basic/authorization | primary spec, 2026-07-28 | HTTP resource-server, audience, scopes, metadata, per-request bearer rules | source-reported |
| R2 | RFC 8693 OAuth Token Exchange, datatracker.ietf.org/doc/rfc8693 | IETF standard, 2020; page updated 2026-05-20 | subject/actor delegation vs impersonation; resource/audience/scope | source-reported |
| R3 | RFC 8707 Resource Indicators, rfc-editor.org/rfc/rfc8707.html | IETF standard, 2020 | explicit target resource/audience | source-reported |
| R4 | RFC 9449 DPoP, rfc-editor.org/rfc/rfc9449 | IETF standard, 2023 | sender-constrained tokens and replay reduction | source-reported |
| R5 | RFC 9700 OAuth Security BCP, rfc-editor.org/info/rfc9700 | IETF BCP, 2025 | current OAuth threat mitigations and dynamic trust caution | source-reported |
| R6 | SPIFFE Federation, spiffe.io/docs/latest/spiffe-specs/spiffe_federation | stable primary spec; retrieved 2026-09-10 | distinct trust domains/bundles, refresh, key lifecycle | source-reported |
| R7 | SPIFFE JWT-SVID, spiffe.io/docs/latest/spiffe-specs/jwt-svid | primary spec; retrieved 2026-09-10 | subject/audience validation | source-reported |
| R8 | Zanzibar, zanzibar.tech | original paper, 2019 | relationship authorization and causal consistency | source-reported |
| R9 | OpenFGA Contextual Tuples, openfga.dev/docs/interacting/contextual-tuples | current product documentation; retrieved 2026-09-10 | ephemeral request context and stale-token caveat | source-reported |
| R10 | UCAN Delegation Specification, ucan.xyz/delegation | current spec; retrieved 2026-09-10 | attenuated delegation/proof-chain design and powerline warning | source-reported |
| R11 | Macaroons paper, research.google.com/pubs/pub41892.html | original research publication, 2014 | chained caveat/attenuation reference | source-reported |
| R12 | TUF Specification, theupdateframework.github.io/specification/latest | current primary spec, 2026-01-22 | key compromise, threshold/delegated roles, rollback/freeze handling | source-reported |
| R13 | A2A Enterprise Features, a2a-protocol.org/latest/topics/enterprise-ready | current protocol documentation; retrieved 2026-09-10 | HTTP identity boundary, granular data/action auth, least privilege | source-reported |
| R14 | Automerge docs, automerge.org/docs | current project documentation; retrieved 2026-09-10 | sync/conflict reference, not adopted semantic authority | source-reported |
| R15 | Mnemes local `docs/DEVICE_OWNED_REPLICATED_MEMORY.md` | source snapshot @ `0abac844...` | current authority map, replica, journal, receipt invariants | locally observed |
| R16 | Mnemes local `docs/DEVICE_SHARDED_MEMORY.md` | source snapshot @ `0abac844...` | current device-shard router and production-blocked status | locally observed |
| R17 | semantic-memory public README/source dependency | current source-facing documentation; retrieved 2026-09-10 | canonical SQLite/authority/retrieval/receipt boundary | source-reported + locally observed |

## 13. Plan acceptance

The plan is complete when:

- exact current source and owners are named;
- the actor/profile/store/grant binding is explicit;
- transport identity is separated from application authorization;
- profile filtering occurs before routing;
- store identity and replica generation are canonical;
- receipt and snapshot requirements are explicit;
- phases have owners, RED/GREEN acceptance, rollback, and stop conditions;
- remote federation is gated behind local proof;
- cheapest falsifying experiment is executable;
- remaining blockers and forbidden shortcuts are visible;
- the Hermes repository/PR exclusion is explicit.

**Current plan state:** complete.
**Implementation state:** not started from this plan.
