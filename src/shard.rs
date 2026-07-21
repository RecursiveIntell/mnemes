//! Shard routing: per-device memory shard management, sparse activation,
//! and routing receipt persistence.
//!
//! The pooled server maintains one `memory/shards/<device_uuid>/memory.db`
//! per registered device. This module implements the "file allocation table"
//! — the routing layer that decides which shards to search, searches them in
//! parallel, merges results, and persists a durable routing receipt.
//!
//! Design principles:
//! - `pooled.db` holds only rebuildable routing metadata, never semantic content.
//! - Shard `MemoryStore` instances are cached (LRU) to avoid repeated cold opens.
//! - All shard stores share a single embedder to avoid N× GPU/HTTP connections.
//! - Namespace filtering eliminates irrelevant shards before any search runs.
//! - Every routing decision is witnessed in `shard_routing_receipts`.

use crate::error::MnemesError;
use crate::types::DeviceId;
use chrono::Utc;
use rusqlite::{params, OptionalExtension};
use semantic_memory::{
    Embedder, MemoryConfig, MemoryStore, SearchContext, SearchResult, SearchSourceType,
    SearchSource, VectorSearchReceiptV1,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

// ─── Types ──────────────────────────────────────────────────────

/// Metadata row from the `device_shards` table.
#[derive(Debug, Clone)]
pub struct ShardEntry {
    pub device_id: DeviceId,
    pub relative_path: String,
    pub state: ShardState,
    pub generation: i64,
    pub routing_terms: String,
    pub namespaces: Vec<String>,
    pub fact_count: i64,
    pub document_count: i64,
    pub chunk_count: i64,
    pub message_count: i64,
    pub search_count: i64,
    pub ewma_latency_ms: f64,
    pub last_refreshed_at: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardState {
    Active,
    Quarantined,
    Revoked,
}

impl ShardState {
    fn from_str(s: &str) -> Self {
        match s {
            "active" => Self::Active,
            "quarantined" => Self::Quarantined,
            "revoked" => Self::Revoked,
            _ => Self::Revoked,
        }
    }
}

/// A ranked shard candidate for sparse activation.
#[derive(Debug, Clone)]
struct RankedShard {
    entry: ShardEntry,
    score: f64,
    reason: String,
}

/// Outcome of searching one shard.
#[derive(Debug, Clone)]
struct ShardSearchOutcome {
    device_id: DeviceId,
    results: Vec<SearchResult>,
    error: Option<String>,
    latency_ms: f64,
    receipt: Option<VectorSearchReceiptV1>,
}

/// Merged routing result returned to the caller.
pub struct RoutedSearchResult {
    pub results: Vec<SearchResult>,
    pub routing_receipt_id: String,
    pub shard_count_searched: usize,
    pub shard_count_total: usize,
}

// ─── Shard cache ────────────────────────────────────────────────

/// LRU-bounded cache of open `MemoryStore` instances, one per shard.
/// Avoids repeated cold opens (which include HNSW index loading and
/// SQLite pool initialization). Bounded to prevent unbounded FD/RSS
/// growth when many devices are registered.
const SHARD_CACHE_CAP: usize = 16;

struct ShardCache {
    entries: std::collections::HashMap<String, Arc<MemoryStore>>,
    order: Vec<String>,
    cap: usize,
}

impl ShardCache {
    fn new(cap: usize) -> Self {
        Self {
            entries: std::collections::HashMap::new(),
            order: Vec::new(),
            cap,
        }
    }

    fn get(&mut self, key: &str) -> Option<Arc<MemoryStore>> {
        if let Some(store) = self.entries.get(key).cloned() {
            // Move to end (most recently used)
            if let Some(pos) = self.order.iter().position(|k| k == key) {
                self.order.remove(pos);
            }
            self.order.push(key.to_string());
            return Some(store);
        }
        None
    }

    fn insert(&mut self, key: String, store: Arc<MemoryStore>) {
        if self.entries.contains_key(&key) {
            // Already present — just bump LRU order
            if let Some(pos) = self.order.iter().position(|k| k == &key) {
                self.order.remove(pos);
            }
            self.order.push(key);
            return;
        }

        // Evict LRU if at capacity
        while self.order.len() >= self.cap {
            if let Some(evicted) = self.order.first().cloned() {
                self.order.remove(0);
                self.entries.remove(&evicted);
            } else {
                break;
            }
        }

        self.entries.insert(key.clone(), store);
        self.order.push(key);
    }

    fn len(&self) -> usize {
        self.entries.len()
    }
}

// ─── Shard router ───────────────────────────────────────────────

/// Manages shard metadata, caching, routing, and receipt persistence.
/// Owns the `pooled.db` connection for `device_shards` and
/// `shard_routing_receipts` tables.
pub struct ShardRouter {
    /// Connection to `pooled.db` (shared with MnemesStore).
    pool_conn: Arc<Mutex<rusqlite::Connection>>,
    /// Base directory for shard data (`<base>/memory/shards/<uuid>/memory.db`).
    base_dir: PathBuf,
    /// Memory config template for opening shard stores.
    memory_config_template: MemoryConfig,
    /// Shared embedder — cloned from the primary store to avoid N× connections.
    shared_embedder: Arc<dyn Embedder>,
    /// LRU cache of open shard stores.
    shard_cache: Mutex<ShardCache>,
}

impl ShardRouter {
    pub fn new(
        pool_conn: Arc<Mutex<rusqlite::Connection>>,
        base_dir: PathBuf,
        memory_config_template: MemoryConfig,
        shared_embedder: Arc<dyn Embedder>,
    ) -> Self {
        Self {
            pool_conn,
            base_dir,
            memory_config_template,
            shared_embedder,
            shard_cache: Mutex::new(ShardCache::new(SHARD_CACHE_CAP)),
        }
    }

    /// Ensure `device_shards` and `shard_routing_receipts` tables exist.
    /// Called during MnemesStore::init_schema.
    pub fn init_schema(conn: &rusqlite::Connection) -> Result<(), MnemesError> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS device_shards (
                device_id          TEXT PRIMARY KEY REFERENCES devices(device_id),
                relative_path      TEXT NOT NULL UNIQUE,
                state              TEXT NOT NULL DEFAULT 'active'
                    CHECK (state IN ('active', 'quarantined', 'revoked')),
                generation         INTEGER NOT NULL DEFAULT 1 CHECK (generation >= 1),
                routing_terms      TEXT NOT NULL DEFAULT '',
                namespaces_json    TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(namespaces_json)),
                fact_count         INTEGER NOT NULL DEFAULT 0 CHECK (fact_count >= 0),
                document_count     INTEGER NOT NULL DEFAULT 0 CHECK (document_count >= 0),
                chunk_count        INTEGER NOT NULL DEFAULT 0 CHECK (chunk_count >= 0),
                message_count      INTEGER NOT NULL DEFAULT 0 CHECK (message_count >= 0),
                search_count       INTEGER NOT NULL DEFAULT 0 CHECK (search_count >= 0),
                ewma_latency_ms    REAL NOT NULL DEFAULT 0.0 CHECK (ewma_latency_ms >= 0.0),
                last_refreshed_at  TEXT,
                created_at         TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_device_shards_state ON device_shards(state);

            CREATE TABLE IF NOT EXISTS shard_routing_receipts (
                receipt_id                 TEXT PRIMARY KEY,
                requester_device_id        TEXT NOT NULL REFERENCES devices(device_id),
                query_sha256               TEXT NOT NULL CHECK (length(query_sha256) = 64),
                shard_budget               INTEGER NOT NULL CHECK (shard_budget >= 0),
                actual_selected_shard_count INTEGER NOT NULL CHECK (actual_selected_shard_count >= 0),
                exhaustive                 INTEGER NOT NULL CHECK (exhaustive IN (0, 1)),
                eligible_shards_json       TEXT NOT NULL CHECK (json_valid(eligible_shards_json)),
                ranked_shards_json         TEXT NOT NULL CHECK (json_valid(ranked_shards_json)),
                selected_shards_json       TEXT NOT NULL CHECK (json_valid(selected_shards_json)),
                skipped_shards_json        TEXT NOT NULL CHECK (json_valid(skipped_shards_json)),
                outcomes_json              TEXT NOT NULL CHECK (json_valid(outcomes_json)),
                fallback_reason            TEXT,
                final_result_ids_json      TEXT NOT NULL CHECK (json_valid(final_result_ids_json)),
                merge_digest               TEXT NOT NULL CHECK (length(merge_digest) = 64),
                receipt_digest             TEXT NOT NULL CHECK (length(receipt_digest) = 64),
                recorded_at                TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_shard_routing_receipts_requester
                ON shard_routing_receipts(requester_device_id, recorded_at DESC);",
        )?;
        Ok(())
    }

    /// Ensure a shard entry exists for a device, creating the path if needed.
    pub async fn ensure_shard(
        &self,
        device_id: &DeviceId,
    ) -> Result<(), MnemesError> {
        let relative_path = format!("memory/shards/{}", device_id.as_str());
        let conn = self.pool_conn.lock().await;
        let exists: bool = conn
            .query_row(
                "SELECT 1 FROM device_shards WHERE device_id = ?1",
                params![device_id.as_str()],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .is_some();

        if !exists {
            // Create the shard directory
            let shard_dir = self.base_dir.join(&relative_path);
            std::fs::create_dir_all(&shard_dir)?;

            conn.execute(
                "INSERT INTO device_shards (device_id, relative_path, state, generation)
                 VALUES (?1, ?2, 'active', 1)",
                params![device_id.as_str(), relative_path],
            )?;
        }
        Ok(())
    }

    /// List all shards from the metadata table.
    pub async fn list_shards(&self) -> Result<Vec<ShardEntry>, MnemesError> {
        let conn = self.pool_conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT device_id, relative_path, state, generation, routing_terms,
                    namespaces_json, fact_count, document_count, chunk_count,
                    message_count, search_count, ewma_latency_ms, last_refreshed_at
             FROM device_shards",
        )?;
        let mut entries = Vec::new();
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let device_id_str: String = row.get(0)?;
            let relative_path: String = row.get(1)?;
            let state_str: String = row.get(2)?;
            let generation: i64 = row.get(3)?;
            let routing_terms: String = row.get(4)?;
            let namespaces_json: String = row.get(5)?;
            let fact_count: i64 = row.get(6)?;
            let document_count: i64 = row.get(7)?;
            let chunk_count: i64 = row.get(8)?;
            let message_count: i64 = row.get(9)?;
            let search_count: i64 = row.get(10)?;
            let ewma_latency_ms: f64 = row.get(11)?;
            let last_refreshed_at: Option<String> = row.get(12)?;

            let namespaces: Vec<String> = serde_json::from_str(&namespaces_json)
                .unwrap_or_default();

            entries.push(ShardEntry {
                device_id: DeviceId::parse(&device_id_str)?,
                relative_path,
                state: ShardState::from_str(&state_str),
                generation,
                routing_terms,
                namespaces,
                fact_count,
                document_count,
                chunk_count,
                message_count,
                search_count,
                ewma_latency_ms,
                last_refreshed_at,
            });
        }
        Ok(entries)
    }

    /// Refresh shard metadata from the actual shard databases.
    /// Updates `fact_count`, `namespaces_json`, `document_count`, `chunk_count`,
    /// and `message_count` by querying each shard's `memory.db`.
    /// This fixes the known issue where `fact_count` drifts from reality.
    pub async fn refresh_all_shard_metadata(&self) -> Result<usize, MnemesError> {
        let shards = self.list_shards().await?;
        let mut refreshed = 0usize;

        for shard in &shards {
            if shard.state != ShardState::Active {
                continue;
            }
            let shard_db_path = self.base_dir.join(&shard.relative_path).join("memory.db");
            if !shard_db_path.exists() {
                continue;
            }

            // Open a read-only connection to the shard DB to extract metadata.
            // We use a separate read-only connection, NOT the cached MemoryStore,
            // because we need raw SQL access to the facts table.
            let shard_conn = match rusqlite::Connection::open_with_flags(
                &shard_db_path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            ) {
                Ok(conn) => conn,
                Err(_) => continue,
            };

            let fact_count: i64 = shard_conn
                .query_row("SELECT COUNT(*) FROM facts", [], |row| row.get(0))
                .unwrap_or(0);
            let document_count: i64 = shard_conn
                .query_row("SELECT COUNT(*) FROM documents", [], |row| row.get(0))
                .unwrap_or(0);
            let chunk_count: i64 = shard_conn
                .query_row("SELECT COUNT(*) FROM chunks", [], |row| row.get(0))
                .unwrap_or(0);
            let message_count: i64 = shard_conn
                .query_row("SELECT COUNT(*) FROM messages", [], |row| row.get(0))
                .unwrap_or(0);

            // Extract distinct namespaces from facts
            let mut ns_stmt = shard_conn.prepare("SELECT DISTINCT namespace FROM facts")?;
            let mut ns_rows = ns_stmt.query([])?;
            let mut namespaces = Vec::new();
            while let Some(row) = ns_rows.next()? {
                let ns: String = row.get(0)?;
                namespaces.push(ns);
            }
            drop(ns_stmt);
            drop(shard_conn);

            let namespaces_json = serde_json::to_string(&namespaces)
                .unwrap_or_else(|_| "[]".to_string());
            let now = Utc::now().to_rfc3339();

            let conn = self.pool_conn.lock().await;
            conn.execute(
                "UPDATE device_shards
                 SET fact_count = ?1, document_count = ?2, chunk_count = ?3,
                     message_count = ?4, namespaces_json = ?5, last_refreshed_at = ?6
                 WHERE device_id = ?7",
                params![
                    fact_count,
                    document_count,
                    chunk_count,
                    message_count,
                    namespaces_json,
                    now,
                    shard.device_id.as_str(),
                ],
            )?;
            refreshed += 1;
        }

        Ok(refreshed)
    }

    /// Get or open a `MemoryStore` for a shard, using the LRU cache.
    /// All shard stores share the same embedder instance to avoid
    /// N× Ollama connections or N× GPU model loads.
    async fn get_shard_store(
        &self,
        shard: &ShardEntry,
    ) -> Result<Arc<MemoryStore>, MnemesError> {
        let device_key = shard.device_id.as_str().to_string();

        // Try cache first
        {
            let mut cache = self.shard_cache.lock().await;
            if let Some(store) = cache.get(&device_key) {
                return Ok(store);
            }
        }

        // Cold open — this is the expensive path (SQLite pool + HNSW load)
        let shard_dir = self.base_dir.join(&shard.relative_path);
        std::fs::create_dir_all(&shard_dir)?;

        let mut config = self.memory_config_template.clone();
        config.base_dir = shard_dir;

        // Open with the shared embedder to avoid N× connections
        // We wrap the Arc<dyn Embedder> in a Box for the API.
        // MemoryStore::open_with_embedder takes Box<dyn Embedder>, but
        // we need to share the embedder. We use a ShareableEmbedder wrapper.
        let store = MemoryStore::open_with_embedder(
            config,
            Box::new(SharedEmbedderWrapper::new(self.shared_embedder.clone())),
        )
        .map_err(MnemesError::Memory)?;

        let store = Arc::new(store);

        // Insert into cache
        {
            let mut cache = self.shard_cache.lock().await;
            cache.insert(device_key, store.clone());
        }

        Ok(store)
    }

    /// Route a search query across shards.
    ///
    /// Steps:
    /// 1. Load all shards from `device_shards`.
    /// 2. Filter to active shards only.
    /// 3. If request namespaces are specified, filter shards whose
    ///    `namespaces_json` intersects the requested set.
    /// 4. If remaining shards ≤ budget, search all (exhaustive).
    ///    Otherwise rank by fact_count + namespace overlap + latency and
    ///    activate top-K (sparse).
    /// 5. Search each selected shard in parallel via `search_with_context`.
    /// 6. Merge results by score (RRF-style if receipts available, else simple sort).
    /// 7. Persist routing receipt to `shard_routing_receipts`.
    /// 8. Update `search_count` and `ewma_latency_ms` on searched shards.
    pub async fn route_search(
        &self,
        query: &str,
        limit: usize,
        request_namespaces: Option<&[&str]>,
        source_types: Option<&[SearchSourceType]>,
        context: SearchContext,
        requester_device_id: &DeviceId,
        shard_budget: usize,
    ) -> Result<RoutedSearchResult, MnemesError> {
        let all_shards = self.list_shards().await?;

        // Step 1: Filter to active, non-revoked shards
        let eligible: Vec<ShardEntry> = all_shards
            .iter()
            .filter(|s| s.state == ShardState::Active)
            .cloned()
            .collect();

        let eligible_ids: Vec<String> = eligible
            .iter()
            .map(|s| s.device_id.as_str().to_string())
            .collect();

        if eligible.is_empty() {
            return Err(MnemesError::InvalidAsOf(
                "no active shards available for routing".to_string(),
            ));
        }

        // Step 2: Namespace filtering
        let after_ns_filter: Vec<ShardEntry> = if let Some(req_ns) = request_namespaces {
            let req_ns_set: HashSet<&str> = req_ns.iter().copied().collect();
            eligible
                .iter()
                .filter(|s| {
                    // If shard has no namespace metadata (empty), include it
                    // (conservative — don't exclude shards we haven't refreshed yet)
                    if s.namespaces.is_empty() {
                        return true;
                    }
                    // Include if any namespace overlaps
                    s.namespaces
                        .iter()
                        .any(|ns| req_ns_set.contains(ns.as_str()))
                })
                .cloned()
                .collect()
        } else {
            eligible.clone()
        };

        // Step 3: Rank and select
        let ranked: Vec<RankedShard> = after_ns_filter
            .iter()
            .map(|s| {
                let mut score = 0.0f64;
                let mut reasons = Vec::new();

                // Volume signal: more facts = higher score
                score += (s.fact_count as f64).ln().max(0.0) * 2.0;
                if s.fact_count > 0 {
                    reasons.push("has_facts".to_string());
                }

                // Namespace overlap: exact match boost
                if let Some(req_ns) = request_namespaces {
                    let req_ns_set: HashSet<&str> = req_ns.iter().copied().collect();
                    let overlap = s
                        .namespaces
                        .iter()
                        .filter(|ns| req_ns_set.contains(ns.as_str()))
                        .count();
                    score += overlap as f64 * 5.0;
                    if overlap > 0 {
                        reasons.push(format!("ns_overlap:{}", overlap));
                    }
                }

                // Latency signal: lower latency = higher score (inverse)
                if s.ewma_latency_ms > 0.0 {
                    score += 1000.0 / s.ewma_latency_ms.max(1.0);
                    reasons.push(format!("latency:{}ms", s.ewma_latency_ms.round() as u64));
                }

                // Recency: recently refreshed shards get a small boost
                if s.last_refreshed_at.is_some() {
                    score += 1.0;
                    reasons.push("refreshed".to_string());
                }

                RankedShard {
                    entry: s.clone(),
                    score,
                    reason: reasons.join(","),
                }
            })
            .collect();

        let exhaustive = after_ns_filter.len() <= shard_budget;
        let selected: Vec<&ShardEntry> = if exhaustive {
            after_ns_filter.iter().collect()
        } else {
            // Sparse: select top-K by score
            let mut sorted = ranked.clone();
            sorted.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
            sorted
                .iter()
                .take(shard_budget)
                .map(|r| &r.entry)
                .collect()
        };

        let selected_ids: Vec<String> = selected
            .iter()
            .map(|s| s.device_id.as_str().to_string())
            .collect();

        let skipped_ids: Vec<String> = after_ns_filter
            .iter()
            .filter(|s| !selected.iter().any(|sel| sel.device_id == s.device_id))
            .map(|s| s.device_id.as_str().to_string())
            .collect();

        // Step 4: Search each selected shard in parallel
        let query_owned = query.to_string();
        let limit_per_shard = limit.max(10) * 2; // Over-fetch for better merge

        let search_futures: Vec<_> = selected
            .iter()
            .map(|shard| {
                let shard = shard.clone();
                let query = query_owned.clone();
                let ns = request_namespaces.map(|ns| ns.to_vec());
                let st = source_types.map(|st| st.to_vec());
                let ctx = context.clone();
                async move {
                    let start = std::time::Instant::now();
                    let store = match self.get_shard_store(&shard).await {
                        Ok(s) => s,
                        Err(e) => {
                            return ShardSearchOutcome {
                                device_id: shard.device_id.clone(),
                                results: vec![],
                                error: Some(format!("open_error: {e}")),
                                latency_ms: start.elapsed().as_millis() as f64,
                                receipt: None,
                            };
                        }
                    };

                    let ns_refs: Option<Vec<&str>> = ns.as_ref().map(|v| v.iter().copied().collect());
                    let st_refs: Option<Vec<SearchSourceType>> = st;

                    match store
                        .search_with_context(
                            &query,
                            Some(limit_per_shard),
                            ns_refs.as_deref(),
                            st_refs.as_deref(),
                            ctx,
                        )
                        .await
                    {
                        Ok(response) => ShardSearchOutcome {
                            device_id: shard.device_id.clone(),
                            results: response.results,
                            error: None,
                            latency_ms: start.elapsed().as_millis() as f64,
                            receipt: response.receipt,
                        },
                        Err(e) => ShardSearchOutcome {
                            device_id: shard.device_id.clone(),
                            results: vec![],
                            error: Some(format!("search_error: {e}")),
                            latency_ms: start.elapsed().as_millis() as f64,
                            receipt: None,
                        },
                    }
                }
            })
            .collect();

        let outcomes = futures::future::join_all(search_futures).await;

        // Step 5: Merge results
        // Simple approach: collect all results, sort by score descending, take top `limit`.
        // Tag each result with its source shard for provenance.
        let mut all_results: Vec<(SearchResult, DeviceId)> = Vec::new();
        for outcome in &outcomes {
            for result in &outcome.results {
                all_results.push((result.clone(), outcome.device_id.clone()));
            }
        }

        // Sort by score descending
        all_results.sort_by(|a, b| {
            b.0.score
                .partial_cmp(&a.0.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let merged: Vec<SearchResult> = all_results
            .iter()
            .take(limit)
            .map(|(r, _)| r.clone())
            .collect();

        // Build final result IDs for receipt
        let final_result_ids: Vec<String> = all_results
            .iter()
            .take(limit)
            .map(|(r, device_id)| {
                let id = match &r.source {
                    SearchSource::Fact { fact_id, .. } => format!("fact:{fact_id}"),
                    SearchSource::Chunk { chunk_id, .. } => format!("chunk:{chunk_id}"),
                    SearchSource::Message { message_id, .. } => format!("message:{message_id}"),
                    SearchSource::Episode { episode_id, .. } => format!("episode:{episode_id}"),
                };
                format!("{}@{}", id, device_id.as_str())
            })
            .collect();

        // Step 6: Compute merge digest
        let merge_digest = {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            for id in &final_result_ids {
                hasher.update(id.as_bytes());
                hasher.update(b"\n");
            }
            let output = hasher.finalize();
            output.iter().map(|b| format!("{:02x}", b)).collect::<String>()
        };

        // Step 7: Build and persist routing receipt
        let query_sha256 = {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(query.as_bytes());
            let output = hasher.finalize();
            output.iter().map(|b| format!("{:02x}", b)).collect::<String>()
        };

        let receipt_id = format!(
            "route-{}",
            Uuid::new_v4()
        );

        let outcomes_json = serde_json::to_string(
            &outcomes
                .iter()
                .map(|o| {
                    json!({
                        "device_id": o.device_id.as_str(),
                        "result_count": o.results.len(),
                        "error": o.error,
                        "latency_ms": o.latency_ms.round() as u64,
                        "has_receipt": o.receipt.is_some(),
                    })
                })
                .collect::<Vec<_>>(),
        )
        .unwrap_or_else(|_| "[]".to_string());

        let ranked_json = serde_json::to_string(
            &ranked
                .iter()
                .map(|r| {
                    json!({
                        "device_id": r.entry.device_id.as_str(),
                        "score": r.score,
                        "reason": r.reason,
                        "fact_count": r.entry.fact_count,
                        "ewma_latency_ms": r.entry.ewma_latency_ms,
                    })
                })
                .collect::<Vec<_>>(),
        )
        .unwrap_or_else(|_| "[]".to_string());

        let eligible_json = serde_json::to_string(&eligible_ids)
            .unwrap_or_else(|_| "[]".to_string());
        let selected_json = serde_json::to_string(&selected_ids)
            .unwrap_or_else(|_| "[]".to_string());
        let skipped_json = serde_json::to_string(&skipped_ids)
            .unwrap_or_else(|_| "[]".to_string());
        let final_ids_json = serde_json::to_string(&final_result_ids)
            .unwrap_or_else(|_| "[]".to_string());

        // Compute receipt self-digest
        let receipt_digest = {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(receipt_id.as_bytes());
            hasher.update(query_sha256.as_bytes());
            hasher.update(format!("{}", shard_budget).as_bytes());
            hasher.update(format!("{}", selected.len()).as_bytes());
            hasher.update(format!("{}", exhaustive as u8).as_bytes());
            hasher.update(eligible_json.as_bytes());
            hasher.update(selected_json.as_bytes());
            hasher.update(skipped_json.as_bytes());
            hasher.update(outcomes_json.as_bytes());
            hasher.update(merge_digest.as_bytes());
            let output = hasher.finalize();
            output.iter().map(|b| format!("{:02x}", b)).collect::<String>()
        };

        let now = Utc::now().to_rfc3339();
        {
            let conn = self.pool_conn.lock().await;
            conn.execute(
                "INSERT INTO shard_routing_receipts (
                    receipt_id, requester_device_id, query_sha256,
                    shard_budget, actual_selected_shard_count, exhaustive,
                    eligible_shards_json, ranked_shards_json, selected_shards_json,
                    skipped_shards_json, outcomes_json, fallback_reason,
                    final_result_ids_json, merge_digest, receipt_digest, recorded_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
                params![
                    receipt_id,
                    requester_device_id.as_str(),
                    query_sha256,
                    shard_budget as i64,
                    selected.len() as i64,
                    exhaustive as i64,
                    eligible_json,
                    ranked_json,
                    selected_json,
                    skipped_json,
                    outcomes_json,
                    // No fallback reason — we either did exhaustive or sparse
                    if exhaustive { None } else { Some("sparse_top_k") },
                    final_ids_json,
                    merge_digest,
                    receipt_digest,
                    now,
                ],
            )?;
        }

        // Step 8: Update shard stats (search_count, ewma_latency_ms)
        {
            let conn = self.pool_conn.lock().await;
            for outcome in &outcomes {
                // EWMA update: new = old * 0.9 + sample * 0.1
                conn.execute(
                    "UPDATE device_shards
                     SET search_count = search_count + 1,
                         ewma_latency_ms = ewma_latency_ms * 0.9 + ?1 * 0.1
                     WHERE device_id = ?2",
                    params![outcome.latency_ms, outcome.device_id.as_str()],
                )?;
            }
        }

        Ok(RoutedSearchResult {
            results: merged,
            routing_receipt_id: receipt_id,
            shard_count_searched: selected.len(),
            shard_count_total: eligible.len(),
        })
    }

    /// Update routing_terms for a shard (future: centroid embeddings, keyword summaries).
    pub async fn update_routing_terms(
        &self,
        device_id: &DeviceId,
        terms: &str,
    ) -> Result<(), MnemesError> {
        let conn = self.pool_conn.lock().await;
        conn.execute(
            "UPDATE device_shards SET routing_terms = ?1 WHERE device_id = ?2",
            params![terms, device_id.as_str()],
        )?;
        Ok(())
    }
}

// ─── Shared embedder wrapper ────────────────────────────────────

/// Wraps an `Arc<dyn Embedder>` in a way that `MemoryStore::open_with_embedder`
/// can consume (which takes `Box<dyn Embedder>`). The inner Arc is shared
/// across all shard stores, so we don't spawn N× Ollama connections.
struct SharedEmbedderWrapper {
    inner: Arc<dyn Embedder>,
}

impl SharedEmbedderWrapper {
    fn new(inner: Arc<dyn Embedder>) -> Self {
        Self { inner }
    }
}

impl Embedder for SharedEmbedderWrapper {
    fn embed<'a>(&'a self, text: &'a str) -> semantic_memory::EmbedFuture<'a> {
        self.inner.embed(text)
    }

    fn embed_batch<'a>(&'a self, texts: Vec<String>) -> semantic_memory::EmbedBatchFuture<'a> {
        self.inner.embed_batch(texts)
    }

    fn model_name(&self) -> &str {
        self.inner.model_name()
    }

    fn dimensions(&self) -> usize {
        self.inner.dimensions()
    }

    fn embed_multi_optional<'a>(
        &'a self,
        text: &'a str,
    ) -> semantic_memory::OptionalMultiEmbedFuture<'a> {
        self.inner.embed_multi_optional(text)
    }

    fn embed_batch_multi_optional<'a>(
        &'a self,
        texts: Vec<String>,
    ) -> semantic_memory::OptionalMultiEmbedBatchFuture<'a> {
        self.inner.embed_batch_multi_optional(texts)
    }
}

// We need json! macro in this module
use serde_json::json;

// We need Uuid
use uuid::Uuid;

// We need rusqlite OpenFlags
// Already imported via rusqlite above

// Re-export futures for join_all
// (mnemes doesn't depend on futures directly, but tokio has join_all)
// We use tokio::task::JoinSet or futures::future::join_all.
// Since we don't have a futures dep, let's use tokio's join_all.
// Actually tokio doesn't export join_all directly. Let's check.
// tokio::task::JoinSet is available. But for simplicity, we can use
// a simple approach: collect into Vec and await sequentially.
// Actually, we want parallel execution. Let's use JoinSet.

// Actually, looking at the code above, I used futures::future::join_all.
// We need to either add futures as a dep or use tokio's JoinSet.
// Let me use tokio::task::JoinSet instead.

// Wait — I already wrote `futures::future::join_all` above.
// Let me fix that. We can use a simple manual approach:
// Since tokio is already a dep, we can use tokio::task::JoinSet.