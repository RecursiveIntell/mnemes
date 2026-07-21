# Mnemes Completion Plan — Shard Routing Wire-Up + MSI Parity + Verification

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Wire the existing `routed_search` path into the public witnessed-search endpoint, prove it returns data through the live MSI service, and verify the full mnemes ecosystem is in parity and clean.

**Architecture:** `MnemesStore::routed_search()` in `src/store.rs` is the canonical routing owner (typed `ShardRoutingReceipt`, bounded expansion, conflict scanning). `src/shard.rs::ShardRouter` is a duplicate implementation that should be removed or consolidated. The HTTP/MCP `run_witnessed_search()` in `src/server.rs` currently bypasses routing and calls `state.store.memory()` (legacy global path). The fix is to make `run_witnessed_search()` call `routed_search()` when shards are registered, falling back to legacy only for single-device/test mode.

**Tech Stack:** Rust 2021, Axum 0.7, semantic-memory 0.5, rusqlite, tokio, Candle embedder, nomic-embed-text 768d

---

## Current State (observed 2026-07-21)

- **Local repo:** `/home/sikmindz/Coding/mnemes`, `main` at `9f39b49`, clean except uncommitted `src/lib.rs` (added `pub mod shard`) and `src/shard.rs` (compile fixes: borrow scope, `Projection` arm, selection lifetime)
- **MSI repo:** `~/Coding/mnemes` at `9f39b49`, `mnemes.service` active
- **MSI shard catalog:** 2 shards — `bb18a9fd...` (active, 1009 facts, 35 namespaces) + `c8501f21...` (active, 0 facts)
- **MSI env:** `MNEMES_PORT`, `MNEMES_DATA_DIR` in server.env; `MNEMES_URL=http://127.0.0.1:1738` in client.env
- **Stale references:** Zero `pooled-memory`/`pooled_memory` references in active source or config
- **Legacy dirs:** `pooled-memory.legacy-20260721` (archived), `mnemes-shard-candidate` (renamed)
- **Compile:** `cargo check --all-targets` passes; `cargo fmt --check` has one formatting diff in `mnemes-admin.rs`
- **Tests:** Last full run passed with `--test-threads=1`; not rerun after shard.rs edits
- **Key defect:** `run_witnessed_search()` calls `state.store.memory().search_with_context()` — the legacy global path. It does NOT call `routed_search()`. This is the "coded but not wired" gap.
- **Duplicate:** `src/shard.rs` (`ShardRouter`) duplicates `src/shards.rs` + `store.rs::routed_search()`. Must consolidate.

## Constraints

- `semantic-memory` is canonical semantic authority; mnemes is control plane only
- Candle is local default; Ollama selectable; any embedder injectable via `open_with_embedder`
- Matryoshka is opt-in feature; nomic-embed-text 768d → 256d truncation for UNO Q
- Peer-first embedding routing is a future phase, not this plan
- Tests must run with `--test-threads=1` (port conflicts in parallel)
- No live SQLite rsync — use `.backup` API
- `src/shard.rs` should be removed after confirming nothing depends on it

---

## Phase 0: Pre-flight and Cleanup

### Task 0.1: Preserve current state as receipt

**Objective:** Capture exact dirty state before any changes.

**Files:** None (read-only)

**Step 1:** Capture state
```bash
cd /home/sikmindz/Coding/mnemes
git status --short --branch > /tmp/mnemes-preflight-status.txt
git diff --stat >> /tmp/mnemes-preflight-status.txt
git rev-parse HEAD >> /tmp/mnemes-preflight-status.txt
```

**Step 2:** Verify current compile passes
```bash
cargo check --all-targets 2>&1 | tail -5
```
Expected: `Finished` with only the `dead_code` warning on `ShardCache::len`

### Task 0.2: Fix formatting

**Objective:** Clear the `cargo fmt --check` failure.

**Files:**
- Modify: `src/bin/mnemes-admin.rs` (formatting)

**Step 1:** Run formatter
```bash
cargo fmt
```

**Step 2:** Verify
```bash
cargo fmt --check
```
Expected: exit 0, no output

### Task 0.3: Remove duplicate `src/shard.rs` module

**Objective:** Eliminate the duplicate `ShardRouter` implementation. The canonical routing lives in `store.rs::routed_search()` + `shards.rs` types.

**Files:**
- Modify: `src/lib.rs` — remove `pub mod shard;` line
- Delete: `src/shard.rs` — entire file (903 lines of duplicate code)
- Modify: `src/store.rs` — remove any imports from `crate::shard` if present

**Step 1:** Check for any imports of `crate::shard`
```bash
grep -rn 'use crate::shard' src/ tests/
```
Expected: no matches (store.rs uses `crate::shards`, not `crate::shard`)

**Step 2:** Remove the module declaration
```rust
// src/lib.rs — remove this line:
pub mod shard;
```

**Step 3:** Delete the file
```bash
rm src/shard.rs
```

**Step 4:** Verify compile
```bash
cargo check --all-targets
```
Expected: `Finished`, no errors. The `ShardCache::len` dead_code warning should also be gone.

**Step 5:** Commit
```bash
git add src/lib.rs src/shard.rs
git commit -m "refactor: remove duplicate shard.rs module — canonical routing is store::routed_search"
```

---

## Phase 1: Wire Routed Search into Witnessed Search

### Task 1.1: Add `has_shards()` helper to MnemesStore

**Objective:** Provide a fast check for whether any active shards are registered, so `run_witnessed_search` can decide whether to route or fall back.

**Files:**
- Modify: `src/store.rs` (add method after `aggregate_shard_stats` ~line 1648)

**Step 1:** Write failing test

Add to `tests/device_shards.rs`:
```rust
#[tokio::test]
async fn has_shards_returns_false_for_empty_store() {
    let tmp = tempfile::tempdir().unwrap();
    let store = MnemesStore::open(tmp.path()).await.unwrap();
    assert!(!store.has_shards().await.unwrap());
}
```

**Step 2:** Run test to verify failure
```bash
cargo test has_shards_returns_false -- --test-threads=1
```
Expected: FAIL — `method not found`

**Step 3:** Implement
```rust
/// Quick check whether any active shards are registered.
pub async fn has_shards(&self) -> Result<bool, MnemesError> {
    let conn = self.pool_conn.lock().await;
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM device_shards WHERE state = 'active'",
        [],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}
```

**Step 4:** Run test to verify pass
```bash
cargo test has_shards_returns_false -- --test-threads=1
```
Expected: PASS

### Task 1.2: Wire `run_witnessed_search` to use routed path

**Objective:** When shards are registered, call `routed_search()`. When no shards (test/single-device), fall back to the legacy `memory()` path.

**Files:**
- Modify: `src/server.rs` — rewrite `run_witnessed_search()` (lines 1799-1852)

**Step 1:** Write failing test

Add to `tests/server.rs`:
```rust
#[tokio::test]
async fn witnessed_search_uses_routed_path_when_shards_exist() {
    // This test verifies that when shards are registered,
    // the search response includes routing metadata.
    // We test the behavior through the store API directly.
    let tmp = tempfile::tempdir().unwrap();
    let store = MnemesStore::open(tmp.path()).await.unwrap();
    
    // Register a device + add facts to its shard
    let device_id = DeviceId::new();
    store.register_device(device_id, "test", "linux", "localhost").await.unwrap();
    store.ensure_shard(&device_id).await.unwrap();
    
    // Add a fact to the shard
    let device_mem = store.device_memory(&device_id).await.unwrap();
    device_mem.add_fact("test content", "general").await.unwrap();
    store.refresh_shard_metadata(&device_id).await.unwrap();
    
    // Routed search should return results
    let request = RoutingSearchRequest::new("test", 5);
    let response = store.routed_search(&device_id, request).await.unwrap();
    assert!(!response.results.is_empty());
    assert!(!response.routing_receipt.selected_shards.is_empty());
}
```

**Step 2:** Run test to verify failure
```bash
cargo test witnessed_search_uses_routed -- --test-threads=1
```
Expected: May pass already if `routed_search` works. The key change is in server.rs.

**Step 3:** Implement the server.rs change

Replace `run_witnessed_search` (lines 1799-1852) with:

```rust
#[cfg(feature = "server")]
async fn run_witnessed_search(
    state: &ServerState,
    request: McpSearchRequest,
) -> Result<WitnessedSearchResponse, MnemesError> {
    let namespaces = request
        .namespaces
        .as_ref()
        .map(|value| value.iter().map(String::as_str).collect::<Vec<_>>());
    let source_types = request
        .source_types
        .as_ref()
        .map(|values| parse_operation_source_types(values))
        .transpose()?;
    
    // Check if sharded mode is active
    let has_shards = state.store.has_shards().await?;
    
    if has_shards {
        // Routed path: use the authenticated device as requester.
        // For MCP/HTTP without device context, use a zero device.
        // The routing layer handles shard selection, parallel search,
        // merge, and receipt persistence.
        let routing_request = crate::shards::RoutingSearchRequest {
            query: request.query.clone(),
            top_k: request.limit,
            namespaces: request.namespaces.clone(),
            source_types: source_types.as_ref().map(|st| st.to_vec()),
            shard_budget: None,
            exhaustive: false,
        };
        
        // Use the first registered device as requester for now.
        // TODO: bind to authenticated device from authorize() context.
        let devices = state.store.list_devices().await?;
        let requester = devices.first()
            .map(|d| d.device_id.clone())
            .unwrap_or_else(DeviceId::new);
        
        let routed = state.store.routed_search(&requester, routing_request).await?;
        
        let results = routed
            .results
            .into_iter()
            .map(|r| result_from_operation_source(
                r.result.source,
                r.result.content,
                r.result.score,
            ))
            .collect::<Vec<_>>();
        
        return Ok(WitnessedSearchResponse {
            results,
            receipt: None, // Routed search has its own receipt type
            receipt_stored: false, // Routing receipt is persisted by routed_search
        });
    }
    
    // Legacy fallback: no shards registered (test/single-device mode)
    let mut context = SearchContext::default_now();
    context.receipt_mode = ReceiptMode::ReturnReceipt;
    context.exactness_profile = ExactnessProfile::PreferExact;

    let search_response = state
        .store
        .memory()
        .search_with_context(
            &request.query,
            request.limit,
            namespaces.as_deref(),
            source_types.as_deref(),
            context,
        )
        .await?;

    let results = search_response
        .results
        .into_iter()
        .map(|value| result_from_operation_source(value.source, value.content, value.score))
        .collect::<Vec<_>>();
    let receipt = search_response.receipt.clone();
    let receipt_stored = if let Some(receipt) = &receipt {
        state
            .store
            .memory()
            .get_search_receipt(&receipt.receipt_id)
            .await
            .ok()
            .flatten()
            .is_some()
    } else {
        false
    };

    Ok(WitnessedSearchResponse {
        results,
        receipt,
        receipt_stored,
    })
}
```

**Step 4:** Add necessary imports to server.rs
```rust
use crate::shards::RoutingSearchRequest;
use crate::types::DeviceId;
```
(Check which are already imported; only add missing ones.)

**Step 5:** Verify compile
```bash
cargo check --all-targets
```
Expected: `Finished`

### Task 1.3: Run full test suite

**Objective:** Verify no regressions from the routing wire-up.

**Step 1:** Run tests
```bash
cargo test -- --test-threads=1
```
Expected: All tests pass (same count as before)

**Step 2:** Run clippy
```bash
cargo clippy --all-targets -- -D warnings 2>&1 | tail -20
```
Expected: May have warnings to fix. Fix any that are new.

**Step 3:** Run fmt
```bash
cargo fmt --check
```
Expected: exit 0

### Task 1.4: Commit routing wire-up

```bash
git add src/server.rs src/store.rs tests/
git commit -m "feat: wire routed_search into witnessed search endpoint

When shards are registered, run_witnessed_search now calls
MnemesStore::routed_search() instead of the legacy global memory()
path. Falls back to legacy only for single-device/test mode."
git push origin main
```

---

## Phase 2: MSI Deployment and Live Verification

### Task 2.1: Build release binary locally

**Objective:** Produce a release binary for MSI deployment.

**Step 1:** Build
```bash
cd /home/sikmindz/Coding/mnemes
cargo build --release --bin mnemes-server
```
Expected: `Finished` with release binary at `target/release/mnemes-server`

**Step 2:** Verify binary exists and is executable
```bash
ls -la target/release/mnemes-server
file target/release/mnemes-server
```

### Task 2.2: Deploy to MSI

**Objective:** Update the MSI service with the new binary.

**Step 1:** SCP binary to MSI
```bash
scp target/release/mnemes-server msi:/tmp/mnemes-server-new
```

**Step 2:** Write deployment script
```bash
# Deploy script: stop service, backup old binary, swap, restart
ssh msi 'cat > /tmp/mnemes-deploy.sh' << 'SCRIPT'
#!/usr/bin/env bash
set -euo pipefail
OLD_BIN="$HOME/.local/share/mnemes/mnemes-server"
NEW_BIN="/tmp/mnemes-server-new"
systemctl --user stop mnemes.service
if [ -f "$OLD_BIN" ]; then
    cp "$OLD_BIN" "$OLD_BIN.old"
fi
cp "$NEW_BIN" "$OLD_BIN"
chmod +x "$OLD_BIN"
systemctl --user start mnemes.service
sleep 2
systemctl --user is-active mnemes.service
SCRIPT
```

**Step 3:** Execute deployment
```bash
ssh msi 'bash /tmp/mnemes-deploy.sh'
```
Expected: `active`

**Step 4:** Verify service is running with new binary
```bash
ssh msi 'systemctl --user show mnemes.service -p MainPID'
ssh msi 'ls -la ~/.local/share/mnemes/mnemes-server'
```

### Task 2.3: Refresh shard metadata on MSI

**Objective:** Ensure the shard catalog reflects current DB state.

**Step 1:** Call refresh endpoint (if available) or use admin CLI
```bash
ssh msi 'curl -s -X POST http://127.0.0.1:1738/v1/shards/refresh 2>/dev/null || true'
```

**Step 2:** Verify shard metadata
```bash
ssh msi 'sqlite3 ~/.local/share/mnemes/pooled.db "SELECT device_id, fact_count, namespaces_json FROM device_shards;"'
```
Expected: `bb18a9fd...` with `fact_count=1009` and populated namespaces

**Step 3:** Run a live authenticated search
```bash
ssh msi 'cd ~/Coding/mnemes && python3 scripts/mneme-client.py witnessed-search --query "semantic memory" --limit 5'
```
Expected: Non-empty results array

**Step 4:** Verify routing receipt was persisted
```bash
ssh msi 'sqlite3 ~/.local/share/mnemes/pooled.db "SELECT receipt_id, actual_selected_shard_count, exhaustive, final_result_ids_json FROM shard_routing_receipts ORDER BY recorded_at DESC LIMIT 1;"'
```
Expected: A receipt row with `actual_selected_shard_count >= 1` and non-empty `final_result_ids_json`

---

## Phase 3: Final Verification and Commit

### Task 3.1: Full local verification gauntlet

**Objective:** Prove the entire codebase is clean.

**Step 1:** Run full gauntlet
```bash
cd /home/sikmindz/Coding/mnemes
cargo fmt --check
cargo check --all-targets
cargo clippy --all-targets -- -D warnings
cargo test -- --test-threads=1
```
Expected: All pass

**Step 2:** Verify no stale references
```bash
grep -RIn 'pooled_memory\|pooled-memory\|PooledMemory' src/ tests/ scripts/ docs/ Cargo.toml README.md 2>/dev/null | grep -v 'legacy\|archived\|\.old' | head -20
```
Expected: no matches (or only historical doc references that are explicitly archival)

### Task 3.2: Verify MSI parity

**Objective:** Confirm local and MSI are at the same commit and both functional.

**Step 1:** Compare HEADs
```bash
LOCAL=$(git -C /home/sikmindz/Coding/mnemes rev-parse HEAD)
REMOTE=$(ssh msi 'git -C ~/Coding/mnemes rev-parse HEAD')
echo "local=$local remote=$remote"
[ "$local" = "$remote" ] && echo "PARITY OK" || echo "DRIFT"
```
Expected: `PARITY OK`

**Step 2:** Verify MSI service health
```bash
ssh msi 'systemctl --user is-active mnemes.service'
ssh msi 'curl -s http://127.0.0.1:1738/v1/health | head -100'
```
Expected: `active` + health JSON with embedding dimensions

**Step 3:** Verify MSI shard integrity
```bash
ssh msi 'sqlite3 ~/.local/share/mnemes/memory/shards/bb18a9fd-f73b-4e6e-935c-ce147706c18b/memory.db "PRAGMA integrity_check; SELECT COUNT(*) FROM facts;"'
```
Expected: `ok` + `1009`

### Task 3.3: Final commit and push

**Objective:** Ensure all changes are committed and pushed.

**Step 1:** Check status
```bash
git status --short
```
Expected: clean working tree

**Step 2:** Push
```bash
git push origin main
```

### Task 3.4: Record semantic-memory fact

**Objective:** Persist a durable record of this completion for future sessions.

**Step 1:** Add fact to semantic memory
```
sm_add_fact(
  content="2026-07-21: mnemes shard routing wired into witnessed search. 
    run_witnessed_search() now calls routed_search() when shards exist, 
    falling back to legacy memory() only for single-device/test mode. 
    Duplicate src/shard.rs removed; canonical routing is store.rs::routed_search 
    + shards.rs types. MSI deployed with new binary, 1009-fact shard verified, 
    routing receipt persisted. Local+MSI at commit 9f39b49+ (post-routing-wire-up). 
    All tests pass with --test-threads=1.",
  namespace="semantic-memory",
  memory_kind="episode_summary"
)
```

---

## Verification Gauntlet Summary

After all phases:
- `cargo fmt --check` — exit 0
- `cargo check --all-targets` — no errors
- `cargo clippy --all-targets -- -D warnings` — no warnings
- `cargo test -- --test-threads=1` — all pass
- MSI service active with new binary
- MSI authenticated search returns non-empty results
- MSI routing receipt persisted
- Local + MSI at same git HEAD
- Zero stale `pooled-memory` references in active source

## Claim Boundary

After this plan:
- **Safe to claim:** shard routing is wired into the public API, MSI is deployed and serving routed searches, naming is canonical, local+MSI parity verified
- **NOT safe to claim:** peer-first embedding routing is implemented (future work), Matryoshka is live-verified end-to-end (feature exposed but not exercised), hermes-infer C engine is complete (separate plan)

## Hard No List

- Do NOT create a second routing implementation — `store.rs::routed_search` is canonical
- Do NOT use `top_k` in MCP tool calls — use `limit`
- Do NOT rsync live SQLite files — use `.backup` API
- Do NOT parallelize same-crate implementation tasks
- Do NOT claim completion without a live authenticated search returning data
- Do NOT target `turbo-semantic` — canonical is `Libraries/semantic-memory`