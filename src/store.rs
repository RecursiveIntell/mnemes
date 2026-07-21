//! Pooled memory store: device/actor/operation registry with its own SQLite
//! database and lazily opened device-owned `semantic_memory::MemoryStore` shards.

use crate::error::MnemesError;
use crate::shards::*;
use crate::types::*;
use chrono::{DateTime, Utc};
use futures::future::join_all;
use hmac::{Hmac, Mac};
use rand::distributions::{Alphanumeric, DistString};
use rand::RngCore;
use rusqlite::{params, OptionalExtension};
use semantic_memory::GraphDirection;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

const DEFAULT_SHARD_CACHE_CAPACITY: usize = 4;
const POOLED_SCHEMA_GENERATION: i64 = 1;
const RECEIPT_AUTH_KEY_FILE: &str = ".routing-receipt-hmac.key";

/// Construct the process-wide embedder selected for the default server path.
///
/// `candle` is the default because it is local, in-process, and avoids a
/// second service hop. Shared-pool operators can select `ollama` (or inject
/// any implementation through `open_with_embedder`) without changing the
/// store or index contracts.
fn configured_provider_name(value: Option<&str>) -> String {
    value
        .unwrap_or("candle")
        .trim()
        .to_ascii_lowercase()
}

fn configured_embedder(
    memory_config: &semantic_memory::MemoryConfig,
) -> Result<Box<dyn semantic_memory::Embedder>, MnemesError> {
    let provider = configured_provider_name(std::env::var("MNEMES_EMBEDDER").ok().as_deref());

    match provider.as_str() {
        "candle" | "local" => {
            #[cfg(feature = "candle-local")]
            {
                Ok(Box::new(semantic_memory::CandleEmbedder::try_new(
                    &memory_config.embedding,
                )?))
            }
            #[cfg(not(feature = "candle-local"))]
            {
                Err(MnemesError::InvalidShardCatalog(
                    "MNEMES_EMBEDDER=candle requires the candle-local feature".to_string(),
                ))
            }
        }
        "ollama" | "http" => Ok(Box::new(semantic_memory::OllamaEmbedder::try_new(
            &memory_config.embedding,
        )?)),
        other => Err(MnemesError::InvalidShardCatalog(format!(
            "unsupported MNEMES_EMBEDDER provider `{other}`; use candle, ollama, or open_with_embedder"
        ))),
    }
}

#[derive(Clone)]
struct SharedEmbedder {
    inner: Arc<dyn semantic_memory::Embedder>,
}

impl semantic_memory::Embedder for SharedEmbedder {
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

struct ShardStoreCache {
    capacity: usize,
    stores: HashMap<String, Arc<semantic_memory::MemoryStore>>,
    lru: VecDeque<String>,
    total_opens: u64,
}

struct ShardSearchExecution {
    outcome: ShardSearchOutcome,
    results: Vec<RoutedSearchResult>,
}

impl ShardStoreCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            stores: HashMap::new(),
            lru: VecDeque::new(),
            total_opens: 0,
        }
    }

    fn get(&mut self, device_id: &DeviceId) -> Option<Arc<semantic_memory::MemoryStore>> {
        let key = device_id.as_str();
        let store = self.stores.get(key)?.clone();
        self.lru.retain(|value| value != key);
        self.lru.push_back(key.to_string());
        Some(store)
    }

    fn insert(&mut self, device_id: &DeviceId, store: Arc<semantic_memory::MemoryStore>) {
        let key = device_id.as_str().to_string();
        self.lru.retain(|value| value != &key);
        self.stores.insert(key.clone(), store);
        self.lru.push_back(key);
        self.total_opens = self.total_opens.saturating_add(1);
        while self.stores.len() > self.capacity {
            if let Some(evicted) = self.lru.pop_front() {
                self.stores.remove(&evicted);
            }
        }
    }
}

/// Credentials for a registered device are a random token (`device_id:secret`).
/// Only `secret`-derived digest is persisted.
#[derive(Debug, Clone)]
struct DeviceCredential {
    token: String,
    digest: String,
}

fn sha256_hex(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    let output = hasher.finalize();
    output
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<String>()
}

fn routing_receipt_digest(
    auth_key: &[u8; 32],
    receipt: &ShardRoutingReceipt,
) -> Result<String, MnemesError> {
    let material = serde_json::to_string(&(
        "pooled-routing-receipt-hmac-v1",
        &receipt.receipt_id,
        &receipt.requester_device_id,
        &receipt.query_sha256,
        receipt.shard_budget,
        receipt.actual_selected_shard_count,
        receipt.exhaustive,
        &receipt.eligible_shards,
        &receipt.ranked_shards,
        &receipt.selected_shards,
        &receipt.skipped_shards,
        &receipt.outcomes,
        &receipt.fallback_reason,
        &receipt.final_result_ids,
        &receipt.merge_digest,
        &receipt.recorded_at,
    ))
    .map_err(|error| MnemesError::InvalidShardCatalog(error.to_string()))?;
    let mut mac = Hmac::<Sha256>::new_from_slice(auth_key)
        .map_err(|error| MnemesError::InvalidShardCatalog(error.to_string()))?;
    mac.update(material.as_bytes());
    Ok(mac
        .finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn validate_routing_receipt(
    auth_key: &[u8; 32],
    receipt: &ShardRoutingReceipt,
) -> Result<(), MnemesError> {
    let invalid = |reason: &str| MnemesError::InvalidShardCatalog(reason.to_string());
    let is_digest =
        |value: &str| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit());
    if !is_digest(&receipt.query_sha256)
        || !is_digest(&receipt.merge_digest)
        || !is_digest(&receipt.receipt_digest)
    {
        return Err(invalid("routing receipt contains an invalid digest"));
    }
    if receipt.actual_selected_shard_count != receipt.selected_shards.len() {
        return Err(invalid("actual selected shard count is inconsistent"));
    }
    let eligible = receipt.eligible_shards.iter().collect::<HashSet<_>>();
    let selected = receipt.selected_shards.iter().collect::<HashSet<_>>();
    let skipped = receipt.skipped_shards.iter().collect::<HashSet<_>>();
    let ranked = receipt
        .ranked_shards
        .iter()
        .map(|shard| &shard.device_id)
        .collect::<HashSet<_>>();
    let outcomes = receipt
        .outcomes
        .iter()
        .map(|outcome| &outcome.device_id)
        .collect::<HashSet<_>>();
    if eligible.len() != receipt.eligible_shards.len()
        || selected.len() != receipt.selected_shards.len()
        || skipped.len() != receipt.skipped_shards.len()
        || ranked.len() != receipt.ranked_shards.len()
        || outcomes.len() != receipt.outcomes.len()
        || ranked != eligible
        || !selected.is_subset(&eligible)
        || !skipped.is_subset(&eligible)
        || !selected.is_disjoint(&skipped)
        || selected.union(&skipped).count() != eligible.len()
        || outcomes != selected
    {
        return Err(invalid("routing receipt shard sets are inconsistent"));
    }
    let final_ids = receipt.final_result_ids.iter().collect::<HashSet<_>>();
    if final_ids.len() != receipt.final_result_ids.len() {
        return Err(invalid(
            "routing receipt contains duplicate final result IDs",
        ));
    }
    if routing_receipt_digest(auth_key, receipt)? != receipt.receipt_digest {
        return Err(invalid("routing receipt authentication mismatch"));
    }
    Ok(())
}

fn read_receipt_auth_key(path: &std::path::Path) -> Result<[u8; 32], MnemesError> {
    // TODO(B3): this metadata check and the subsequent open are a TOCTOU race;
    // validate the opened file descriptor (or otherwise eliminate the gap).
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(MnemesError::InvalidShardCatalog(format!(
            "routing receipt authentication key is not a regular file: {}",
            path.display()
        )));
    }
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(MnemesError::InvalidShardCatalog(format!(
            "routing receipt authentication key permissions are too broad: {}",
            path.display()
        )));
    }
    let mut file = OpenOptions::new().read(true).open(path)?;
    let mut key = [0_u8; 32];
    file.read_exact(&mut key)?;
    let mut extra = [0_u8; 1];
    if file.read(&mut extra)? != 0 {
        return Err(MnemesError::InvalidShardCatalog(format!(
            "routing receipt authentication key has invalid length: {}",
            path.display()
        )));
    }
    Ok(key)
}

fn load_or_create_receipt_auth_key(
    base_dir: &std::path::Path,
) -> Result<[u8; 32], MnemesError> {
    let path = base_dir.join(RECEIPT_AUTH_KEY_FILE);
    if path.exists() {
        return read_receipt_auth_key(&path);
    }

    let temporary = base_dir.join(format!(
        "{RECEIPT_AUTH_KEY_FILE}.tmp-{}",
        uuid::Uuid::new_v4()
    ));
    let mut key = [0_u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut key);
    let write_result = (|| -> Result<(), MnemesError> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)?;
        file.write_all(&key)?;
        file.sync_all()?;
        match std::fs::hard_link(&temporary, &path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        std::fs::remove_file(&temporary)?;
        OpenOptions::new().read(true).open(base_dir)?.sync_all()?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = std::fs::remove_file(&temporary);
        write_result?;
    }
    read_receipt_auth_key(&path)
}

impl DeviceCredential {
    fn new(device_id: &DeviceId) -> Self {
        let secret = Alphanumeric.sample_string(&mut rand::thread_rng(), 64);
        let token = format!("{}:{}", device_id.as_str(), secret);
        let digest = format!("sha256:{}", sha256_hex(&token));
        Self { token, digest }
    }

    fn verify(stored: &Option<String>, token: &str) -> bool {
        // SHA-256 is intentionally unsalted here: credentials contain 64
        // cryptographically random alphanumeric characters, so offline
        // guessing is infeasible for the current credential format. TODO:
        // upgrade persisted credential fingerprints to a memory-hard KDF.
        let candidate = format!("sha256:{}", sha256_hex(token));
        let Some(stored_val) = stored.as_ref() else {
            return false;
        };
        subtle::ConstantTimeEq::ct_eq(stored_val.as_bytes(), candidate.as_bytes()).into()
    }
}

/// Combined mnemes memory store.
///
/// Owns a device/actor/operation SQLite database alongside a
/// lazily opened `semantic_memory::MemoryStore` shards. The databases are separate:
/// - `pooled.db` — device registry, actors, operation envelopes
/// - `memory/shards/<device_uuid>/memory.db` — semantic truth owned by semantic-memory
pub struct MnemesStore {
    pool_conn: tokio::sync::Mutex<rusqlite::Connection>,
    base_dir: PathBuf,
    memory_config: semantic_memory::MemoryConfig,
    embedder: Arc<dyn semantic_memory::Embedder>,
    shard_cache: tokio::sync::Mutex<ShardStoreCache>,
    legacy_memory: std::sync::OnceLock<semantic_memory::MemoryStore>,
    receipt_auth_key: [u8; 32],
}

impl MnemesStore {
    /// Open a mnemes memory store at the given base directory.
    /// Creates `pooled.db` for device/actor/operation data (legacy filename, stable wire identifier) and delegates
    /// memory content to `semantic_memory::MemoryStore` in a subdirectory.
    pub fn open(
        base_dir: PathBuf,
        memory_config: semantic_memory::MemoryConfig,
    ) -> Result<Self, MnemesError> {
        let embedder = configured_embedder(&memory_config)?;
        Self::open_with_shared_embedder(
            base_dir,
            memory_config,
            Arc::from(embedder),
            DEFAULT_SHARD_CACHE_CAPACITY,
        )
    }

    /// Open with a custom embedder (for testing).
    pub fn open_with_embedder(
        base_dir: PathBuf,
        memory_config: semantic_memory::MemoryConfig,
        embedder: Box<dyn semantic_memory::Embedder>,
    ) -> Result<Self, MnemesError> {
        Self::open_with_embedder_and_cache_capacity(
            base_dir,
            memory_config,
            embedder,
            DEFAULT_SHARD_CACHE_CAPACITY,
        )
    }

    /// Open with one shared custom embedder and an explicit bounded cache capacity.
    pub fn open_with_embedder_and_cache_capacity(
        base_dir: PathBuf,
        memory_config: semantic_memory::MemoryConfig,
        embedder: Box<dyn semantic_memory::Embedder>,
        cache_capacity: usize,
    ) -> Result<Self, MnemesError> {
        Self::open_with_shared_embedder(
            base_dir,
            memory_config,
            Arc::from(embedder),
            cache_capacity,
        )
    }

    fn open_with_shared_embedder(
        base_dir: PathBuf,
        memory_config: semantic_memory::MemoryConfig,
        embedder: Arc<dyn semantic_memory::Embedder>,
        cache_capacity: usize,
    ) -> Result<Self, MnemesError> {
        // Reject a pre-existing legacy global memory.db in the active tree.
        // The shard architecture requires all semantic content to live under
        // memory/shards/<device_uuid>/memory.db. A memory/memory.db at the
        // top level indicates an old non-sharded layout that must be migrated
        // before the store can be opened.
        let legacy_global = base_dir.join("memory").join("memory.db");
        if legacy_global.exists() {
            return Err(MnemesError::LegacyGlobalStorePresent(
                legacy_global.display().to_string(),
            ));
        }
        std::fs::create_dir_all(&base_dir)?;
        // Create the shard root before schema initialization. The schema
        // transaction records catalog rows, while these directories are the
        // filesystem side of that catalog and are reconciled below; keeping
        // creation outside the transaction avoids pretending SQLite can roll
        // back filesystem mutations.
        std::fs::create_dir_all(base_dir.join("memory").join("shards"))?;
        let conn = rusqlite::Connection::open(base_dir.join("pooled.db"))?;
        Self::init_schema(&conn)?;
        let receipt_auth_key = load_or_create_receipt_auth_key(&base_dir)?;
        Self::ensure_existing_shard_directories(&base_dir, &conn)?;
        Ok(Self {
            pool_conn: tokio::sync::Mutex::new(conn),
            base_dir,
            memory_config,
            embedder,
            shard_cache: tokio::sync::Mutex::new(ShardStoreCache::new(cache_capacity)),
            legacy_memory: std::sync::OnceLock::new(),
            receipt_auth_key,
        })
    }

    fn init_schema(conn: &rusqlite::Connection) -> Result<(), MnemesError> {
        let version_table_exists = conn.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM sqlite_master
                WHERE type = 'table' AND name = '_pooled_schema_version'
            )",
            [],
            |row| row.get::<_, bool>(0),
        )?;
        if version_table_exists {
            let mut statement =
                conn.prepare("SELECT version FROM _pooled_schema_version ORDER BY version ASC")?;
            let versions = statement
                .query_map([], |row| row.get::<_, i64>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            if versions != [POOLED_SCHEMA_GENERATION] {
                return Err(MnemesError::InvalidShardCatalog(format!(
                    "unsupported pooled schema generations {versions:?}; expected only {POOLED_SCHEMA_GENERATION}"
                )));
            }
        }

        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA foreign_keys = ON;
             PRAGMA busy_timeout = 5000;
             PRAGMA synchronous = NORMAL;",
        )?;

        conn.execute_batch(
            "BEGIN IMMEDIATE;
             CREATE TABLE IF NOT EXISTS _pooled_schema_version (
                 version INTEGER PRIMARY KEY,
                 applied_at TEXT NOT NULL
             );
             INSERT OR IGNORE INTO _pooled_schema_version(version, applied_at)
                 VALUES (1, datetime('now'));

             CREATE TABLE IF NOT EXISTS devices (
                device_id               TEXT PRIMARY KEY,
                label                   TEXT NOT NULL,
                platform                TEXT NOT NULL,
                hostname                TEXT NOT NULL,
                credential_fingerprint  TEXT,
                first_seen_at           TEXT NOT NULL,
                last_seen_at            TEXT NOT NULL,
                status                  TEXT NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active', 'revoked', 'quarantined')),
                created_at              TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS actors (
                actor_id        TEXT PRIMARY KEY,
                device_id       TEXT NOT NULL REFERENCES devices(device_id),
                actor_kind      TEXT NOT NULL,
                tool_profile    TEXT NOT NULL DEFAULT 'agent'
                    CHECK (tool_profile IN ('agent', 'operator')),
                provider_model  TEXT,
                recorded_at     TEXT NOT NULL,
                created_at      TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_actors_device ON actors(device_id);

            CREATE TABLE IF NOT EXISTS audit_events (
                event_id TEXT PRIMARY KEY,
                device_id TEXT,
                actor_id TEXT,
                endpoint TEXT NOT NULL,
                method TEXT NOT NULL,
                outcome TEXT NOT NULL CHECK (outcome IN ('ok', 'denied', 'error')),
                detail TEXT,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_audit_events_created_at
                ON audit_events(created_at);
            CREATE INDEX IF NOT EXISTS idx_audit_events_endpoint
                ON audit_events(endpoint, method);

            -- TODO(B1): add a UNIQUE constraint on idempotency_key (and a
            -- migration for existing stores) so concurrent retries cannot race
            -- the application-level check in submit_operation.
            CREATE TABLE IF NOT EXISTS operation_envelopes (
                operation_id            TEXT PRIMARY KEY,
                idempotency_key         TEXT NOT NULL,
                requesting_device_id    TEXT NOT NULL REFERENCES devices(device_id),
                requesting_actor_id     TEXT NOT NULL REFERENCES actors(actor_id),
                recording_device_id     TEXT NOT NULL,
                recording_server_id     TEXT NOT NULL,
                operation_kind          TEXT NOT NULL
                    CHECK (operation_kind IN ('observe', 'assert', 'supersede', 'revoke', 'redact', 'adjudicate')),
                target_kind             TEXT NOT NULL,
                target_id               TEXT NOT NULL,
                content_digest          TEXT NOT NULL,
                observed_at             TEXT,
                valid_time              TEXT,
                recorded_at             TEXT NOT NULL,
                receipt_id              TEXT,
                created_at              TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_operation_envelopes_idempotency
                ON operation_envelopes(idempotency_key);
            CREATE INDEX IF NOT EXISTS idx_operation_envelopes_device
                ON operation_envelopes(requesting_device_id);
            CREATE INDEX IF NOT EXISTS idx_operation_envelopes_recorded
                ON operation_envelopes(recorded_at DESC);

            CREATE TABLE IF NOT EXISTS provenance_edges (
                edge_id TEXT PRIMARY KEY,
                edge_type TEXT NOT NULL CHECK (
                    edge_type IN ('observed_by', 'recorded_by', 'derived_from', 'supports',
                                  'contradicts', 'supersedes', 'retrieved_from')
                ),
                source_kind TEXT NOT NULL,
                source_id TEXT NOT NULL,
                target_kind TEXT NOT NULL,
                target_id TEXT NOT NULL,
                operation_id TEXT REFERENCES operation_envelopes(operation_id),
                actor_id TEXT REFERENCES actors(actor_id),
                device_id TEXT REFERENCES devices(device_id),
                valid_from TEXT,
                valid_to TEXT,
                observed_at TEXT,
                recorded_at TEXT NOT NULL,
                content_digest TEXT,
                metadata TEXT,
                supersedes_edge_id TEXT REFERENCES provenance_edges(edge_id),
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                CHECK (length(source_kind) > 0 AND length(source_id) > 0),
                CHECK (length(target_kind) > 0 AND length(target_id) > 0),
                CHECK (valid_to IS NULL OR valid_from IS NULL OR valid_to >= valid_from),
                CHECK (metadata IS NULL OR json_valid(metadata)),
                CHECK (source_kind || ':' || source_id <> target_kind || ':' || target_id)
            );
            CREATE INDEX IF NOT EXISTS idx_edges_source ON provenance_edges(source_kind, source_id, recorded_at DESC);
            CREATE INDEX IF NOT EXISTS idx_edges_target ON provenance_edges(target_kind, target_id, recorded_at DESC);
            CREATE INDEX IF NOT EXISTS idx_edges_type ON provenance_edges(edge_type, recorded_at DESC);
            CREATE INDEX IF NOT EXISTS idx_edges_operation ON provenance_edges(operation_id, recorded_at DESC);
            CREATE INDEX IF NOT EXISTS idx_edges_valid ON provenance_edges(valid_from, valid_to);
            CREATE INDEX IF NOT EXISTS idx_edges_recorded ON provenance_edges(recorded_at DESC);
            CREATE INDEX IF NOT EXISTS idx_edges_supersedes ON provenance_edges(supersedes_edge_id);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_edges_idempotency ON provenance_edges(
                operation_id, edge_type, source_kind, source_id, target_kind, target_id, content_digest
            );

            CREATE TABLE IF NOT EXISTS device_shards (
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
                created_at         TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_device_shards_state ON device_shards(state);

            CREATE TABLE IF NOT EXISTS shard_routing_receipts (
                receipt_id                 TEXT PRIMARY KEY,
                requester_device_id        TEXT NOT NULL REFERENCES devices(device_id),
                query_sha256                TEXT NOT NULL CHECK (length(query_sha256) = 64),
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

        let schema_generation = conn.query_row(
            "SELECT MAX(version) FROM _pooled_schema_version",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        if schema_generation != POOLED_SCHEMA_GENERATION {
            return Err(MnemesError::InvalidShardCatalog(format!(
                "unsupported pooled schema generation {schema_generation}; expected {POOLED_SCHEMA_GENERATION}"
            )));
        }

        if !Self::has_table_column(conn, "actors", "tool_profile")? {
            conn.execute("ALTER TABLE actors ADD COLUMN tool_profile TEXT NOT NULL DEFAULT 'agent' CHECK (tool_profile IN ('agent', 'operator'))", [])?;
            conn.execute(
                "UPDATE actors SET tool_profile = 'agent' WHERE tool_profile IS NULL",
                [],
            )?;
        }

        if !Self::has_table_column(conn, "audit_events", "detail")? {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS audit_events (
                    event_id TEXT PRIMARY KEY,
                    device_id TEXT,
                    actor_id TEXT,
                    endpoint TEXT NOT NULL,
                    method TEXT NOT NULL,
                    outcome TEXT NOT NULL CHECK (outcome IN ('ok', 'denied', 'error')),
                    detail TEXT,
                    created_at TEXT NOT NULL
                );",
                [],
            )?;
            conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_events_created_at ON audit_events(created_at)", [])?;
            conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_events_endpoint ON audit_events(endpoint, method)", [])?;
        }

        for column in [
            "device_id",
            "relative_path",
            "state",
            "generation",
            "routing_terms",
            "namespaces_json",
            "fact_count",
            "document_count",
            "chunk_count",
            "message_count",
            "search_count",
            "ewma_latency_ms",
            "last_refreshed_at",
            "created_at",
        ] {
            if !Self::has_table_column(conn, "device_shards", column)? {
                return Err(MnemesError::InvalidShardCatalog(format!(
                    "device_shards is missing required column {column}"
                )));
            }
        }
        for column in [
            "receipt_id",
            "requester_device_id",
            "query_sha256",
            "shard_budget",
            "actual_selected_shard_count",
            "exhaustive",
            "eligible_shards_json",
            "ranked_shards_json",
            "selected_shards_json",
            "skipped_shards_json",
            "outcomes_json",
            "fallback_reason",
            "final_result_ids_json",
            "merge_digest",
            "receipt_digest",
            "recorded_at",
        ] {
            if !Self::has_table_column(conn, "shard_routing_receipts", column)? {
                return Err(MnemesError::InvalidShardCatalog(format!(
                    "shard_routing_receipts is missing required column {column}"
                )));
            }
        }

        Self::backfill_device_shards(conn)?;
        conn.execute_batch("COMMIT;")?;
        Ok(())
    }

    fn shard_relative_path(device_id: &DeviceId) -> PathBuf {
        PathBuf::from("memory")
            .join("shards")
            .join(device_id.as_str())
    }

    fn insert_device_shard_row(
        conn: &rusqlite::Connection,
        device_id: &DeviceId,
        status: DeviceStatus,
        created_at: &str,
    ) -> Result<(), MnemesError> {
        let relative_path = Self::shard_relative_path(device_id);
        conn.execute(
            "INSERT OR IGNORE INTO device_shards
             (device_id, relative_path, state, generation, routing_terms, namespaces_json,
              fact_count, document_count, chunk_count, message_count, search_count,
              ewma_latency_ms, last_refreshed_at, created_at)
             VALUES (?1, ?2, ?3, 1, '', '[]', 0, 0, 0, 0, 0, 0.0, NULL, ?4)",
            params![
                device_id.as_str(),
                relative_path.to_string_lossy(),
                ShardState::from(status).as_str(),
                created_at,
            ],
        )?;
        Ok(())
    }

    fn backfill_device_shards(conn: &rusqlite::Connection) -> Result<(), MnemesError> {
        let mut statement = conn.prepare(
            "SELECT device_id, status, created_at FROM devices
             WHERE device_id NOT IN (SELECT device_id FROM device_shards)
             ORDER BY device_id ASC",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let pending = rows.collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        for (raw_device_id, raw_status, created_at) in pending {
            let device_id = DeviceId::parse(&raw_device_id)?;
            let status = DeviceStatus::parse(&raw_status, &raw_device_id)?;
            Self::insert_device_shard_row(conn, &device_id, status, &created_at)?;
        }
        Ok(())
    }

    fn ensure_existing_shard_directories(
        base_dir: &std::path::Path,
        conn: &rusqlite::Connection,
    ) -> Result<(), MnemesError> {
        let mut statement =
            conn.prepare("SELECT device_id FROM device_shards ORDER BY device_id")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        for row in rows {
            let device_id = DeviceId::parse(row?)?;
            std::fs::create_dir_all(base_dir.join(Self::shard_relative_path(&device_id)))?;
        }
        Ok(())
    }

    fn has_table_column(
        conn: &rusqlite::Connection,
        table: &str,
        column: &str,
    ) -> Result<bool, MnemesError> {
        let pragma = format!("PRAGMA table_info({table})");
        let mut statement = conn.prepare(&pragma)?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let name: String = row.get(1)?;
            if name == column {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn generate_device_credentials(device_id: &DeviceId) -> DeviceCredential {
        DeviceCredential::new(device_id)
    }

    fn parse_authorization_token(raw: &str) -> Result<(DeviceId, String), MnemesError> {
        let token = raw.trim();
        let (device_id_value, secret) = token
            .split_once(':')
            .ok_or(MnemesError::InvalidCredential)?;
        if secret.is_empty() {
            return Err(MnemesError::InvalidCredential);
        }

        let device_id = DeviceId::parse(device_id_value)?;
        Ok((device_id, secret.to_string()))
    }

    async fn token_device_id(
        &self,
        token: &str,
    ) -> Result<(Device, Option<String>), MnemesError> {
        let (device_id, secret) = Self::parse_authorization_token(token)?;
        let mut row = {
            let conn = self.pool_conn.lock().await;
            conn.query_row(
                "SELECT credential_fingerprint, status FROM devices WHERE device_id = ?1",
                params![device_id.as_str()],
                |row| Ok((row.get::<_, Option<String>>(0)?, row.get::<_, String>(1)?)),
            )
            .ok()
        };

        let (stored_fingerprint, status) = match row.take() {
            Some(value) => value,
            None => return Err(MnemesError::InvalidCredential),
        };

        if status != DeviceStatus::Active.as_str() {
            return Err(MnemesError::DeviceNotActive(format!(
                "{device_id} (status: {status})"
            )));
        }

        let full_token = format!("{}:{}", device_id.as_str(), secret);
        if !DeviceCredential::verify(&stored_fingerprint, &full_token) {
            return Err(MnemesError::InvalidCredential);
        }

        let device = self
            .get_device(&device_id)
            .await?
            .ok_or_else(|| MnemesError::DeviceNotFound(device_id.to_string()))?;
        Ok((device, stored_fingerprint))
    }

    fn bootstrap_with_tx(
        tx: &rusqlite::Transaction<'_>,
        mut device: Device,
        mut actor: Actor,
        created_at: String,
        credential: DeviceCredential,
    ) -> Result<(DeviceId, ActorId, String), MnemesError> {
        device.first_seen_at = created_at.clone();
        device.last_seen_at = created_at.clone();
        device.credential_fingerprint = Some(credential.digest);
        actor.recorded_at = created_at.clone();
        actor.device_id = device.device_id.clone();

        tx.execute(
            "INSERT INTO devices (device_id, label, platform, hostname, \
             credential_fingerprint, first_seen_at, last_seen_at, status) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                device.device_id.as_str(),
                device.label,
                device.platform,
                device.hostname,
                device.credential_fingerprint,
                device.first_seen_at,
                device.last_seen_at,
                device.status.as_str(),
            ],
        )?;

        tx.execute(
            "INSERT INTO actors (actor_id, device_id, actor_kind, tool_profile, provider_model, recorded_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                actor.actor_id.as_str(),
                actor.device_id.as_str(),
                actor.actor_kind.as_str(),
                actor.tool_profile.as_str(),
                actor.provider_model,
                actor.recorded_at,
            ],
        )?;

        Self::insert_device_shard_row(tx, &device.device_id, device.status, &created_at)?;

        Ok((
            device.device_id.clone(),
            actor.actor_id.clone(),
            credential.token,
        ))
    }

    #[cfg(test)]
    async fn bootstrap_with_actor(
        &self,
        device: Device,
        actor: Actor,
    ) -> Result<(DeviceId, ActorId, String, String), MnemesError> {
        let created_at = Utc::now().to_rfc3339();
        let credentials = Self::generate_device_credentials(&device.device_id);

        let mut conn = self.pool_conn.lock().await;
        let tx = conn.transaction()?;
        let (device_id, actor_id, credential) =
            Self::bootstrap_with_tx(&tx, device, actor, created_at.clone(), credentials)?;
        tx.commit()?;
        Ok((device_id, actor_id, credential, created_at))
    }

    pub async fn authenticate_request(
        &self,
        token: &str,
        actor_id: Option<&ActorId>,
    ) -> Result<(Device, Option<Actor>), MnemesError> {
        let (device, _fingerprint) = self.token_device_id(token).await?;
        let actor = if let Some(actor_id) = actor_id {
            let actor = self
                .get_actor(actor_id)
                .await?
                .ok_or_else(|| MnemesError::ActorNotFound(actor_id.to_string()))?;

            if actor.device_id != device.device_id {
                return Err(MnemesError::AuthorizationDenied(
                    "actor does not belong to device".to_string(),
                ));
            }

            Some(actor)
        } else {
            None
        };

        Ok((device, actor))
    }

    pub async fn ensure_actor_profile(
        &self,
        actor: &Option<Actor>,
        requires_full: bool,
    ) -> Result<(), MnemesError> {
        if requires_full {
            let actor = actor.as_ref().ok_or_else(|| {
                MnemesError::AuthorizationDenied("actor required".to_string())
            })?;
            if !actor.tool_profile.is_full() {
                return Err(MnemesError::AuthorizationDenied(
                    "actor lacks operator profile".to_string(),
                ));
            }
        }

        Ok(())
    }

    // ─── Device registry ──────────────────────────────────────────────

    pub async fn bootstrap(
        &self,
        device: Device,
        actor_kind: ActorKind,
    ) -> Result<(DeviceId, ActorId, String, String), MnemesError> {
        std::fs::create_dir_all(self.device_shard_path(&device.device_id))?;
        let mut conn = self.pool_conn.lock().await;
        let tx = conn.transaction()?;
        let existing_devices: i64 =
            tx.query_row("SELECT COUNT(*) FROM devices", [], |row| row.get(0))?;
        if existing_devices > 0 {
            return Err(MnemesError::BootstrapRejected(
                "bootstrap requires an empty device registry".to_string(),
            ));
        }

        let actor = Actor {
            actor_id: ActorId::new(),
            device_id: device.device_id.clone(),
            tool_profile: ToolProfile::Operator,
            actor_kind,
            provider_model: None,
            recorded_at: String::new(),
        };

        let now = Utc::now().to_rfc3339();
        let credentials = Self::generate_device_credentials(&device.device_id);
        let (device_id, actor_id, credential) =
            Self::bootstrap_with_tx(&tx, device, actor, now.clone(), credentials)?;
        tx.commit()?;
        Ok((device_id, actor_id, credential, now))
    }

    pub async fn register_device_with_generated_credential(
        &self,
        mut device: Device,
    ) -> Result<(DeviceId, String), MnemesError> {
        let credentials = Self::generate_device_credentials(&device.device_id);
        device.credential_fingerprint = Some(credentials.digest);
        let device_id = self.register_device(device).await?;
        Ok((device_id, credentials.token))
    }

    pub async fn register_device(&self, mut device: Device) -> Result<DeviceId, MnemesError> {
        let now = Utc::now().to_rfc3339();
        device.first_seen_at = now.clone();
        device.last_seen_at = now.clone();
        let device_id_return = device.device_id.clone();

        std::fs::create_dir_all(self.device_shard_path(&device.device_id))?;

        let mut conn = self.pool_conn.lock().await;
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO devices (device_id, label, platform, hostname, \
             credential_fingerprint, first_seen_at, last_seen_at, status) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                device.device_id.as_str(),
                device.label,
                device.platform,
                device.hostname,
                device.credential_fingerprint,
                device.first_seen_at,
                device.last_seen_at,
                device.status.as_str(),
            ],
        )?;
        Self::insert_device_shard_row(&tx, &device_id_return, device.status, &now)?;
        tx.commit()?;
        Ok(device_id_return)
    }

    pub async fn rotate_device_credential(
        &self,
        device_id: &DeviceId,
    ) -> Result<String, MnemesError> {
        let credentials = Self::generate_device_credentials(device_id);
        let now = Utc::now().to_rfc3339();
        let conn = self.pool_conn.lock().await;
        let affected = conn.execute(
            "UPDATE devices SET credential_fingerprint = ?1, last_seen_at = ?2 WHERE device_id = ?3",
            params![credentials.digest, now, device_id.as_str()],
        )?;
        if affected == 0 {
            return Err(MnemesError::DeviceNotFound(device_id.to_string()));
        }
        Ok(credentials.token)
    }

    pub async fn get_device(
        &self,
        device_id: &DeviceId,
    ) -> Result<Option<Device>, MnemesError> {
        let conn = self.pool_conn.lock().await;
        let result = conn
            .query_row(
                "SELECT device_id, label, platform, hostname, credential_fingerprint, \
                 first_seen_at, last_seen_at, status \
                 FROM devices WHERE device_id = ?1",
                params![device_id.as_str()],
                |row| {
                    let status_str: String = row.get(7)?;
                    let device_id_str: String = row.get(0)?;
                    Ok((
                        device_id_str,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        status_str,
                    ))
                },
            )
            .ok();

        if let Some((did, label, platform, hostname, cred, first, last, status)) = result {
            Ok(Some(Device {
                device_id: DeviceId::parse(&did)?,
                label,
                platform,
                hostname,
                credential_fingerprint: cred,
                first_seen_at: first,
                last_seen_at: last,
                status: DeviceStatus::parse(&status, &did)?,
            }))
        } else {
            Ok(None)
        }
    }

    pub async fn set_device_status(
        &self,
        device_id: &DeviceId,
        status: DeviceStatus,
    ) -> Result<(), MnemesError> {
        let now = Utc::now().to_rfc3339();
        let mut conn = self.pool_conn.lock().await;
        let tx = conn.transaction()?;
        let affected = tx.execute(
            "UPDATE devices SET status = ?1, last_seen_at = ?2 WHERE device_id = ?3",
            params![status.as_str(), now, device_id.as_str()],
        )?;
        if affected == 0 {
            return Err(MnemesError::DeviceNotFound(device_id.to_string()));
        }
        tx.execute(
            "UPDATE device_shards SET state = ?1 WHERE device_id = ?2",
            params![ShardState::from(status).as_str(), device_id.as_str()],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub async fn list_devices_for_actor(
        &self,
        actor: &Actor,
    ) -> Result<Vec<Device>, MnemesError> {
        self.list_devices_for_device(&actor.device_id).await
    }

    async fn list_devices_for_device(
        &self,
        device_id: &DeviceId,
    ) -> Result<Vec<Device>, MnemesError> {
        let conn = self.pool_conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT device_id, label, platform, hostname, credential_fingerprint, \
             first_seen_at, last_seen_at, status \
             FROM devices WHERE device_id = ?1 ORDER BY first_seen_at ASC",
        )?;
        let rows = stmt.query_map(params![device_id.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
            ))
        })?;

        let mut devices = Vec::new();
        for row in rows {
            let (did, label, platform, hostname, cred, first, last, status) = row?;
            devices.push(Device {
                device_id: DeviceId::parse(&did)?,
                label,
                platform,
                hostname,
                credential_fingerprint: cred,
                first_seen_at: first,
                last_seen_at: last,
                status: DeviceStatus::parse(&status, &did)?,
            });
        }

        Ok(devices)
    }

    pub async fn list_devices(&self) -> Result<Vec<Device>, MnemesError> {
        let conn = self.pool_conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT device_id, label, platform, hostname, credential_fingerprint, \
             first_seen_at, last_seen_at, status \
             FROM devices ORDER BY first_seen_at ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
            ))
        })?;

        let mut devices = Vec::new();
        for row in rows {
            let (did, label, platform, hostname, cred, first, last, status) = row?;
            devices.push(Device {
                device_id: DeviceId::parse(&did)?,
                label,
                platform,
                hostname,
                credential_fingerprint: cred,
                first_seen_at: first,
                last_seen_at: last,
                status: DeviceStatus::parse(&status, &did)?,
            });
        }
        Ok(devices)
    }

    pub async fn revoke_device(&self, device_id: &DeviceId) -> Result<(), MnemesError> {
        self.set_device_status(device_id, DeviceStatus::Revoked)
            .await
    }

    pub async fn heartbeat_device(&self, device_id: &DeviceId) -> Result<(), MnemesError> {
        let now = Utc::now().to_rfc3339();
        let conn = self.pool_conn.lock().await;
        let updated = conn.execute(
            "UPDATE devices SET last_seen_at = ?1
             WHERE device_id = ?2 AND status = 'active'",
            params![now, device_id.as_str()],
        )?;
        if updated == 0 {
            return Err(MnemesError::DeviceNotFound(device_id.to_string()));
        }
        Ok(())
    }

    // ─── Actor registry ───────────────────────────────────────────────

    pub async fn register_actor(&self, mut actor: Actor) -> Result<ActorId, MnemesError> {
        let now = Utc::now().to_rfc3339();
        actor.recorded_at = now;
        let actor_id_return = actor.actor_id.clone();

        let conn = self.pool_conn.lock().await;
        conn.execute(
            "INSERT INTO actors (actor_id, device_id, actor_kind, tool_profile, provider_model, recorded_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                actor.actor_id.as_str(),
                actor.device_id.as_str(),
                actor.actor_kind.as_str(),
                actor.tool_profile.as_str(),
                actor.provider_model,
                actor.recorded_at,
            ],
        )?;
        Ok(actor_id_return)
    }

    pub async fn list_actors_for_device(
        &self,
        device_id: &DeviceId,
    ) -> Result<Vec<Actor>, MnemesError> {
        let conn = self.pool_conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT actor_id, device_id, actor_kind, tool_profile, provider_model, recorded_at \
             FROM actors WHERE device_id = ?1 ORDER BY recorded_at ASC",
        )?;
        let rows = stmt.query_map(params![device_id.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, String>(5)?,
            ))
        })?;

        let mut actors = Vec::new();
        for row in rows {
            let (aid, did, kind_str, tool_profile_str, pm, recorded_at) = row?;
            actors.push(Actor {
                actor_id: ActorId::parse(&aid)?,
                device_id: DeviceId::parse(&did)?,
                actor_kind: ActorKind::parse(kind_str),
                tool_profile: ToolProfile::parse(&tool_profile_str).unwrap_or_default(),
                provider_model: pm,
                recorded_at,
            });
        }

        Ok(actors)
    }

    pub async fn list_actors(&self) -> Result<Vec<Actor>, MnemesError> {
        let conn = self.pool_conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT actor_id, device_id, actor_kind, tool_profile, provider_model, recorded_at \
             FROM actors ORDER BY recorded_at ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, String>(5)?,
            ))
        })?;

        let mut actors = Vec::new();
        for row in rows {
            let (aid, did, kind_str, tool_profile_str, pm, recorded_at) = row?;
            actors.push(Actor {
                actor_id: ActorId::parse(&aid)?,
                device_id: DeviceId::parse(&did)?,
                actor_kind: ActorKind::parse(kind_str),
                tool_profile: ToolProfile::parse(&tool_profile_str).unwrap_or_default(),
                provider_model: pm,
                recorded_at,
            });
        }

        Ok(actors)
    }

    pub async fn get_actor(&self, actor_id: &ActorId) -> Result<Option<Actor>, MnemesError> {
        let conn = self.pool_conn.lock().await;
        let result = conn
            .query_row(
                "SELECT actor_id, device_id, actor_kind, tool_profile, provider_model, recorded_at \
                 FROM actors WHERE actor_id = ?1",
                params![actor_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, String>(5)?,
                    ))
                },
            )
            .ok();

        if let Some((aid, did, kind_str, profile_str, pm, rat)) = result {
            Ok(Some(Actor {
                actor_id: ActorId::parse(&aid)?,
                device_id: DeviceId::parse(&did)?,
                actor_kind: ActorKind::parse(kind_str),
                tool_profile: ToolProfile::parse(&profile_str).unwrap_or_default(),
                provider_model: pm,
                recorded_at: rat,
            }))
        } else {
            Ok(None)
        }
    }

    // ─── Operation envelopes ──────────────────────────────────────────

    pub async fn submit_operation(
        &self,
        mut envelope: OperationEnvelope,
    ) -> Result<String, MnemesError> {
        // Check idempotency first. TODO(B1): this check races concurrent
        // submissions until idempotency_key has a database UNIQUE constraint.
        let idempotency_key = envelope.idempotency_key.clone();
        if let Some(existing) = self
            .get_operation_by_idempotency_key(&idempotency_key)
            .await?
        {
            if existing.content_digest == envelope.content_digest {
                return existing
                    .receipt_id
                    .ok_or(MnemesError::IdempotencyConflict(format!(
                        "operation with key {idempotency_key} has no persistent receipt"
                    )));
            }

            return Err(MnemesError::IdempotencyConflict(format!(
                "operation with key {idempotency_key} already exists"
            )));
        }

        let now = Utc::now().to_rfc3339();
        envelope.recorded_at = now;
        let receipt_id = format!("op-receipt:{}", envelope.operation_id.as_str());
        envelope.receipt_id = Some(receipt_id.clone());

        let conn = self.pool_conn.lock().await;
        conn.execute(
            "INSERT INTO operation_envelopes \
             (operation_id, idempotency_key, requesting_device_id, requesting_actor_id, \
             recording_device_id, recording_server_id, operation_kind, target_kind, \
             target_id, content_digest, observed_at, valid_time, recorded_at, receipt_id) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            params![
                envelope.operation_id.as_str(),
                envelope.idempotency_key,
                envelope.requesting_device_id.as_str(),
                envelope.requesting_actor_id.as_str(),
                envelope.recording_device_id.as_str(),
                envelope.recording_server_id.as_str(),
                envelope.operation_kind.as_str(),
                envelope.target_kind,
                envelope.target_id,
                envelope.content_digest,
                envelope.observed_at,
                envelope.valid_time,
                envelope.recorded_at,
                envelope.receipt_id,
            ],
        )?;
        Ok(receipt_id)
    }

    /// Deterministic absolute shard directory derived only from a validated device ID.
    pub fn device_shard_path(&self, device_id: &DeviceId) -> PathBuf {
        self.base_dir.join(Self::shard_relative_path(device_id))
    }

    /// Embedding model shared by all lazily opened shards.
    pub fn embedding_model(&self) -> &str {
        self.embedder.model_name()
    }

    /// Embedding dimensions shared by all lazily opened shards.
    pub fn embedding_dimensions(&self) -> usize {
        self.embedder.dimensions()
    }

    /// Base directory for mnemes data.
    pub fn base_dir(&self) -> &std::path::Path {
        &self.base_dir
    }

    /// Legacy synchronous accessor for handlers that predate the shard architecture.
    /// Lazily opens legacy memory/memory.db on first access.
    pub fn memory(&self) -> &semantic_memory::MemoryStore {
        self.legacy_memory.get_or_init(|| {
            let legacy = self.base_dir.join("memory").join("memory.db");
            let config = semantic_memory::MemoryConfig {
                base_dir: legacy.parent().unwrap_or(&legacy).to_path_buf(),
                ..self.memory_config.clone()
            };
            semantic_memory::MemoryStore::open(config)
                .unwrap_or_else(|e| panic!("legacy memory store: {e:?}"))
        })
    }

    /// Open a device's semantic-memory owner store lazily through the bounded cache.
    pub async fn device_memory(
        &self,
        device_id: &DeviceId,
    ) -> Result<Arc<semantic_memory::MemoryStore>, MnemesError> {
        let expected_relative_path = Self::shard_relative_path(device_id);
        let catalog_path = {
            let conn = self.pool_conn.lock().await;
            conn.query_row(
                "SELECT relative_path FROM device_shards WHERE device_id = ?1",
                params![device_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        }
        .ok_or_else(|| MnemesError::DeviceNotFound(device_id.to_string()))?;
        if std::path::Path::new(&catalog_path) != expected_relative_path {
            return Err(MnemesError::InvalidShardCatalog(format!(
                "device {} has non-deterministic path {catalog_path}",
                device_id.as_str()
            )));
        }

        let mut cache = self.shard_cache.lock().await;
        if let Some(store) = cache.get(device_id) {
            return Ok(store);
        }

        let shard_path = self.device_shard_path(device_id);
        std::fs::create_dir_all(&shard_path)?;
        let mut config = self.memory_config.clone();
        config.base_dir = shard_path;
        let store = semantic_memory::MemoryStore::open_with_embedder(
            config,
            Box::new(SharedEmbedder {
                inner: self.embedder.clone(),
            }),
        )?;
        let store = Arc::new(store);
        cache.insert(device_id, store.clone());
        Ok(store)
    }

    /// Current cache metrics; does not open a shard.
    pub async fn shard_cache_metrics(&self) -> ShardCacheMetrics {
        let cache = self.shard_cache.lock().await;
        ShardCacheMetrics {
            len: cache.stores.len(),
            capacity: cache.capacity,
            total_opens: cache.total_opens,
        }
    }

    /// Drop cached handles while retaining the cumulative open counter.
    pub async fn clear_shard_cache(&self) {
        let mut cache = self.shard_cache.lock().await;
        cache.stores.clear();
        cache.lru.clear();
    }

    /// List the derived shard catalog without opening semantic-memory databases.
    pub async fn list_shards(&self) -> Result<Vec<DeviceShard>, MnemesError> {
        let conn = self.pool_conn.lock().await;
        let mut statement = conn.prepare(
            "SELECT s.device_id, s.relative_path, s.state, s.generation,
                    s.routing_terms, s.namespaces_json, s.fact_count, s.document_count,
                    s.chunk_count, s.message_count, s.search_count, s.ewma_latency_ms,
                    s.last_refreshed_at, s.created_at, d.status
             FROM device_shards s JOIN devices d ON d.device_id = s.device_id
             ORDER BY s.device_id ASC",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, u64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, u64>(6)?,
                row.get::<_, u64>(7)?,
                row.get::<_, u64>(8)?,
                row.get::<_, u64>(9)?,
                row.get::<_, u64>(10)?,
                row.get::<_, f64>(11)?,
                row.get::<_, Option<String>>(12)?,
                row.get::<_, String>(13)?,
                row.get::<_, String>(14)?,
            ))
        })?;
        let mut shards = Vec::new();
        for row in rows {
            let (
                raw_device_id,
                relative_path,
                raw_state,
                generation,
                routing_terms,
                namespaces_json,
                fact_count,
                document_count,
                chunk_count,
                message_count,
                search_count,
                ewma_latency_ms,
                last_refreshed_at,
                created_at,
                raw_device_status,
            ) = row?;
            let namespaces: Vec<String> =
                serde_json::from_str(&namespaces_json).map_err(|error| {
                    MnemesError::InvalidShardCatalog(format!(
                        "invalid namespaces for {raw_device_id}: {error}"
                    ))
                })?;
            shards.push(DeviceShard {
                device_id: DeviceId::parse(&raw_device_id)?,
                relative_path: PathBuf::from(relative_path),
                state: ShardState::parse(&raw_state)?,
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
                created_at,
                device_status: DeviceStatus::parse(&raw_device_status, &raw_device_id)?,
            });
        }
        Ok(shards)
    }

    /// Refresh one derived summary from public semantic-memory owner statistics.
    pub async fn refresh_shard_summary(
        &self,
        device_id: &DeviceId,
        routing_terms: &str,
        namespaces: &[String],
    ) -> Result<DeviceShard, MnemesError> {
        let memory = self.device_memory(device_id).await?;
        let stats = memory.stats().await?;
        let mut normalized_namespaces = namespaces.to_vec();
        normalized_namespaces.sort();
        normalized_namespaces.dedup();
        let normalized_terms = routing_tokens(routing_terms).join(" ");
        let namespaces_json = serde_json::to_string(&normalized_namespaces).map_err(|error| {
            MnemesError::InvalidShardCatalog(format!(
                "failed to serialize namespaces: {error}"
            ))
        })?;
        let refreshed_at = Utc::now().to_rfc3339();
        let conn = self.pool_conn.lock().await;
        let affected = conn.execute(
            "UPDATE device_shards
             SET routing_terms = ?1, namespaces_json = ?2,
                 fact_count = ?3, document_count = ?4, chunk_count = ?5,
                 message_count = ?6, generation = generation + 1,
                 last_refreshed_at = ?7
             WHERE device_id = ?8",
            params![
                normalized_terms,
                namespaces_json,
                stats.total_facts,
                stats.total_documents,
                stats.total_chunks,
                stats.total_messages,
                refreshed_at,
                device_id.as_str(),
            ],
        )?;
        drop(conn);
        if affected == 0 {
            return Err(MnemesError::DeviceNotFound(device_id.to_string()));
        }
        self.list_shards()
            .await?
            .into_iter()
            .find(|shard| &shard.device_id == device_id)
            .ok_or_else(|| MnemesError::DeviceNotFound(device_id.to_string()))
    }

    /// Aggregate derived catalog counts without opening any shard.
    pub async fn aggregate_shard_stats(&self) -> Result<ShardAggregateStats, MnemesError> {
        let conn = self.pool_conn.lock().await;
        conn.query_row(
            "SELECT COUNT(*), COALESCE(SUM(fact_count), 0),
                    COALESCE(SUM(document_count), 0), COALESCE(SUM(chunk_count), 0),
                    COALESCE(SUM(message_count), 0), COALESCE(SUM(search_count), 0)
             FROM device_shards",
            [],
            |row| {
                Ok(ShardAggregateStats {
                    shards: row.get(0)?,
                    facts: row.get(1)?,
                    documents: row.get(2)?,
                    chunks: row.get(3)?,
                    messages: row.get(4)?,
                    searches: row.get(5)?,
                })
            },
        )
        .map_err(Into::into)
    }

    async fn record_shard_search_observation(
        &self,
        device_id: &DeviceId,
        latency_ms: u64,
    ) -> Result<(), MnemesError> {
        let conn = self.pool_conn.lock().await;
        conn.execute(
            "UPDATE device_shards
             SET ewma_latency_ms = CASE WHEN search_count = 0 THEN ?1
                                        ELSE (ewma_latency_ms * 0.8) + (?1 * 0.2) END,
                 search_count = search_count + 1
             WHERE device_id = ?2",
            params![latency_ms as f64, device_id.as_str()],
        )?;
        Ok(())
    }

    async fn search_one_shard(
        &self,
        shard: &DeviceShard,
        request: &RoutingSearchRequest,
        route_receipt_id: &str,
        query_sha256: &str,
    ) -> ShardSearchExecution {
        let started = Instant::now();
        let memory = self.device_memory(&shard.device_id).await;
        let result = match memory {
            Ok(memory) => {
                let namespace_storage = request.namespaces.clone();
                let namespace_refs = namespace_storage
                    .as_ref()
                    .map(|values| values.iter().map(String::as_str).collect::<Vec<_>>());
                let mut context = semantic_memory::SearchContext::default_now();
                context.receipt_mode = semantic_memory::ReceiptMode::ReturnReceipt;
                context.exactness_profile = semantic_memory::ExactnessProfile::PreferExact;
                context.request_id =
                    Some(format!("{route_receipt_id}:{}", shard.device_id.as_str()));
                context.query_text_digest = Some(query_sha256.to_string());
                memory
                    .search_with_context(
                        &request.query,
                        Some(request.top_k),
                        namespace_refs.as_deref(),
                        request.source_types.as_deref(),
                        context,
                    )
                    .await
            }
            Err(error) => {
                let latency_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
                let _ = self
                    .record_shard_search_observation(&shard.device_id, latency_ms)
                    .await;
                return ShardSearchExecution {
                    outcome: ShardSearchOutcome {
                        device_id: shard.device_id.clone(),
                        shard_generation: shard.generation,
                        latency_ms,
                        result_count: 0,
                        child_search_receipt_id: None,
                        error: Some(error.to_string()),
                    },
                    results: Vec::new(),
                };
            }
        };
        let latency_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        let _ = self
            .record_shard_search_observation(&shard.device_id, latency_ms)
            .await;
        match result {
            Ok(response) => {
                let child_receipt_id = response
                    .receipt
                    .as_ref()
                    .map(|receipt| receipt.receipt_id.clone());
                let result_count = response.results.len();
                let results = response
                    .results
                    .into_iter()
                    .map(|result| RoutedSearchResult {
                        result,
                        device_id: shard.device_id.clone(),
                        shard_generation: shard.generation,
                        child_receipt_id: child_receipt_id.clone(),
                    })
                    .collect();
                ShardSearchExecution {
                    outcome: ShardSearchOutcome {
                        device_id: shard.device_id.clone(),
                        shard_generation: shard.generation,
                        latency_ms,
                        result_count,
                        child_search_receipt_id: child_receipt_id,
                        error: None,
                    },
                    results,
                }
            }
            Err(error) => ShardSearchExecution {
                outcome: ShardSearchOutcome {
                    device_id: shard.device_id.clone(),
                    shard_generation: shard.generation,
                    latency_ms,
                    result_count: 0,
                    child_search_receipt_id: None,
                    error: Some(error.to_string()),
                },
                results: Vec::new(),
            },
        }
    }

    async fn search_shard_batch(
        &self,
        shards: &[DeviceShard],
        request: &RoutingSearchRequest,
        route_receipt_id: &str,
        query_sha256: &str,
    ) -> Vec<ShardSearchExecution> {
        join_all(
            shards
                .iter()
                .map(|shard| self.search_one_shard(shard, request, route_receipt_id, query_sha256)),
        )
        .await
    }

    /// Sparse routed retrieval across selected device shards with bounded expansion.
    pub async fn routed_search(
        &self,
        requester_device_id: &DeviceId,
        request: RoutingSearchRequest,
    ) -> Result<RoutedSearchResponse, MnemesError> {
        if self.get_device(requester_device_id).await?.is_none() {
            return Err(MnemesError::DeviceNotFound(
                requester_device_id.to_string(),
            ));
        }
        let catalog = self.list_shards().await?;
        let ranked = rank_shards(&request.query, requester_device_id, &catalog);
        let eligible_shards = ranked
            .iter()
            .map(|value| value.device_id.clone())
            .collect::<Vec<_>>();
        let initial_budget = if request.exhaustive {
            ranked.len()
        } else {
            request
                .shard_budget
                .unwrap_or_else(|| ranked.len().min(2))
                .min(ranked.len())
        };
        let route_receipt_id = uuid::Uuid::new_v4().to_string();
        let query_sha256 = sha256_hex(&request.query);
        let by_device = catalog
            .into_iter()
            .map(|shard| (shard.device_id.clone(), shard))
            .collect::<HashMap<_, _>>();
        let initial = ranked
            .iter()
            .take(initial_budget)
            .filter_map(|ranked| by_device.get(&ranked.device_id).cloned())
            .collect::<Vec<_>>();

        let mut executions = self
            .search_shard_batch(&initial, &request, &route_receipt_id, &query_sha256)
            .await;
        let mut all_results = executions
            .iter()
            .flat_map(|execution| execution.results.clone())
            .collect::<Vec<_>>();
        let mut selected_count = initial.len();
        let mut fallback_reason = None;
        if !request.exhaustive && selected_count < ranked.len() {
            let merged = merge_routed_results(all_results.clone(), request.top_k)?;
            if merged.len() < request.top_k {
                fallback_reason = Some("insufficient_results_expand".to_string());
                for next in ranked.iter().skip(selected_count) {
                    let Some(shard) = by_device.get(&next.device_id) else {
                        continue;
                    };
                    let mut expanded = self
                        .search_shard_batch(
                            std::slice::from_ref(shard),
                            &request,
                            &route_receipt_id,
                            &query_sha256,
                        )
                        .await;
                    let execution = expanded.remove(0);
                    all_results.extend(execution.results.clone());
                    executions.push(execution);
                    selected_count += 1;
                    if merge_routed_results(all_results.clone(), request.top_k)?.len()
                        >= request.top_k
                    {
                        break;
                    }
                }
            }
        }

        let results = merge_routed_results(all_results, request.top_k)?;
        let selected_shards = ranked
            .iter()
            .take(selected_count)
            .map(|value| value.device_id.clone())
            .collect::<Vec<_>>();
        let skipped_shards = ranked
            .iter()
            .skip(selected_count)
            .map(|value| value.device_id.clone())
            .collect::<Vec<_>>();
        let final_result_ids = results
            .iter()
            .map(|result| result.result.source.result_id())
            .collect::<Vec<_>>();
        let merge_material = results
            .iter()
            .map(|result| {
                format!(
                    "{}:{}:{}",
                    result.result.source.result_id(),
                    result.device_id.as_str(),
                    sha256_hex(&result.result.content)
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        let actual_selected_shard_count = selected_shards.len();
        let mut receipt = ShardRoutingReceipt {
            receipt_id: route_receipt_id,
            requester_device_id: requester_device_id.clone(),
            query_sha256,
            shard_budget: initial_budget,
            actual_selected_shard_count,
            exhaustive: request.exhaustive,
            eligible_shards,
            ranked_shards: ranked,
            selected_shards,
            skipped_shards,
            outcomes: executions
                .into_iter()
                .map(|execution| execution.outcome)
                .collect(),
            fallback_reason,
            final_result_ids,
            merge_digest: sha256_hex(&merge_material),
            receipt_digest: String::new(),
            recorded_at: Utc::now().to_rfc3339(),
        };
        receipt.receipt_digest = routing_receipt_digest(&self.receipt_auth_key, &receipt)?;
        validate_routing_receipt(&self.receipt_auth_key, &receipt)?;
        self.persist_routing_receipt(&receipt).await?;
        Ok(RoutedSearchResponse {
            results,
            routing_receipt: receipt,
        })
    }

    async fn persist_routing_receipt(
        &self,
        receipt: &ShardRoutingReceipt,
    ) -> Result<(), MnemesError> {
        validate_routing_receipt(&self.receipt_auth_key, receipt)?;
        let eligible = serde_json::to_string(&receipt.eligible_shards)
            .map_err(|error| MnemesError::InvalidShardCatalog(error.to_string()))?;
        let ranked = serde_json::to_string(&receipt.ranked_shards)
            .map_err(|error| MnemesError::InvalidShardCatalog(error.to_string()))?;
        let selected = serde_json::to_string(&receipt.selected_shards)
            .map_err(|error| MnemesError::InvalidShardCatalog(error.to_string()))?;
        let skipped = serde_json::to_string(&receipt.skipped_shards)
            .map_err(|error| MnemesError::InvalidShardCatalog(error.to_string()))?;
        let outcomes = serde_json::to_string(&receipt.outcomes)
            .map_err(|error| MnemesError::InvalidShardCatalog(error.to_string()))?;
        let final_ids = serde_json::to_string(&receipt.final_result_ids)
            .map_err(|error| MnemesError::InvalidShardCatalog(error.to_string()))?;
        let conn = self.pool_conn.lock().await;
        conn.execute(
            "INSERT INTO shard_routing_receipts
             (receipt_id, requester_device_id, query_sha256, shard_budget,
              actual_selected_shard_count, exhaustive, eligible_shards_json,
              ranked_shards_json, selected_shards_json, skipped_shards_json,
              outcomes_json, fallback_reason, final_result_ids_json, merge_digest,
              receipt_digest, recorded_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            params![
                receipt.receipt_id,
                receipt.requester_device_id.as_str(),
                receipt.query_sha256,
                receipt.shard_budget as u64,
                receipt.actual_selected_shard_count as u64,
                i64::from(receipt.exhaustive),
                eligible,
                ranked,
                selected,
                skipped,
                outcomes,
                receipt.fallback_reason,
                final_ids,
                receipt.merge_digest,
                receipt.receipt_digest,
                receipt.recorded_at,
            ],
        )?;
        Ok(())
    }

    /// Read a durable typed routing receipt by ID.
    pub async fn get_routing_receipt(
        &self,
        receipt_id: &str,
    ) -> Result<Option<ShardRoutingReceipt>, MnemesError> {
        let conn = self.pool_conn.lock().await;
        let row = conn
            .query_row(
                "SELECT receipt_id, requester_device_id, query_sha256, shard_budget,
                        actual_selected_shard_count, exhaustive, eligible_shards_json,
                        ranked_shards_json, selected_shards_json, skipped_shards_json,
                        outcomes_json, fallback_reason, final_result_ids_json, merge_digest,
                        receipt_digest, recorded_at
                 FROM shard_routing_receipts WHERE receipt_id = ?1",
                params![receipt_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, usize>(3)?,
                        row.get::<_, usize>(4)?,
                        row.get::<_, bool>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, String>(8)?,
                        row.get::<_, String>(9)?,
                        row.get::<_, String>(10)?,
                        row.get::<_, Option<String>>(11)?,
                        row.get::<_, String>(12)?,
                        row.get::<_, String>(13)?,
                        row.get::<_, String>(14)?,
                        row.get::<_, String>(15)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            receipt_id,
            requester_device_id,
            query_sha256,
            shard_budget,
            actual_selected_shard_count,
            exhaustive,
            eligible,
            ranked,
            selected,
            skipped,
            outcomes,
            fallback_reason,
            final_ids,
            merge_digest,
            receipt_digest,
            recorded_at,
        )) = row
        else {
            return Ok(None);
        };
        let receipt = ShardRoutingReceipt {
            receipt_id,
            requester_device_id: DeviceId::parse(requester_device_id)?,
            query_sha256,
            shard_budget,
            actual_selected_shard_count,
            exhaustive,
            eligible_shards: serde_json::from_str::<Vec<DeviceId>>(&eligible)
                .map_err(|error| MnemesError::InvalidShardCatalog(error.to_string()))?,
            ranked_shards: serde_json::from_str::<Vec<RankedShard>>(&ranked)
                .map_err(|error| MnemesError::InvalidShardCatalog(error.to_string()))?,
            selected_shards: serde_json::from_str::<Vec<DeviceId>>(&selected)
                .map_err(|error| MnemesError::InvalidShardCatalog(error.to_string()))?,
            skipped_shards: serde_json::from_str::<Vec<DeviceId>>(&skipped)
                .map_err(|error| MnemesError::InvalidShardCatalog(error.to_string()))?,
            outcomes: serde_json::from_str::<Vec<ShardSearchOutcome>>(&outcomes)
                .map_err(|error| MnemesError::InvalidShardCatalog(error.to_string()))?,
            fallback_reason,
            final_result_ids: serde_json::from_str::<Vec<String>>(&final_ids)
                .map_err(|error| MnemesError::InvalidShardCatalog(error.to_string()))?,
            merge_digest,
            receipt_digest,
            recorded_at,
        };
        validate_routing_receipt(&self.receipt_auth_key, &receipt)?;
        Ok(Some(receipt))
    }

    /// Verify mnemes SQLite and every cataloged semantic-memory shard explicitly.
    pub async fn verify_all_shards(&self) -> Result<Vec<ShardIntegrityStatus>, MnemesError> {
        let shards = self.list_shards().await?;
        let mut statuses = Vec::with_capacity(shards.len());
        for shard in shards {
            let (status, detail) = match self.device_memory(&shard.device_id).await {
                Ok(memory) => match memory
                    .verify_integrity(semantic_memory::VerifyMode::Quick)
                    .await
                {
                    Ok(report) if report.ok => ("ok".to_string(), format!("{report:?}")),
                    Ok(report) => (
                        "degraded".to_string(),
                        format!("issues: {:?}", report.issues),
                    ),
                    Err(error) => ("failed".to_string(), error.to_string()),
                },
                Err(error) => ("failed".to_string(), error.to_string()),
            };
            statuses.push(ShardIntegrityStatus {
                device_id: shard.device_id,
                relative_path: shard.relative_path,
                status,
                detail,
            });
        }
        Ok(statuses)
    }

    /// Run `PRAGMA quick_check` against the mnemes SQLite database.
    pub async fn quick_check(&self) -> Result<String, MnemesError> {
        let conn = self.pool_conn.lock().await;
        let check = conn
            .query_row("PRAGMA quick_check", [], |row| row.get::<_, String>(0))
            .unwrap_or_else(|_| "error".to_string());
        Ok(check)
    }

    /// Total number of registered devices.
    pub async fn count_devices(&self) -> Result<u64, MnemesError> {
        let conn = self.pool_conn.lock().await;
        let total = conn.query_row("SELECT COUNT(*) FROM devices", [], |row| {
            row.get::<_, u64>(0)
        })?;
        Ok(total)
    }

    /// Total number of registered actors.
    pub async fn count_actors(&self) -> Result<u64, MnemesError> {
        let conn = self.pool_conn.lock().await;
        let total = conn.query_row("SELECT COUNT(*) FROM actors", [], |row| {
            row.get::<_, u64>(0)
        })?;
        Ok(total)
    }

    /// Total number of operation envelopes persisted in the pool.
    pub async fn count_operations(&self) -> Result<u64, MnemesError> {
        let conn = self.pool_conn.lock().await;
        let total = conn.query_row("SELECT COUNT(*) FROM operation_envelopes", [], |row| {
            row.get::<_, u64>(0)
        })?;
        Ok(total)
    }

    pub async fn get_operation(
        &self,
        operation_id: &OperationId,
    ) -> Result<Option<OperationEnvelope>, MnemesError> {
        let conn = self.pool_conn.lock().await;
        let result = conn
            .query_row(
                "SELECT operation_id, idempotency_key, requesting_device_id, \
                 requesting_actor_id, recording_device_id, recording_server_id, \
                 operation_kind, target_kind, target_id, content_digest, \
                 observed_at, valid_time, recorded_at, receipt_id \
                 FROM operation_envelopes WHERE operation_id = ?1",
                params![operation_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, String>(8)?,
                        row.get::<_, String>(9)?,
                        row.get::<_, Option<String>>(10)?,
                        row.get::<_, Option<String>>(11)?,
                        row.get::<_, String>(12)?,
                        row.get::<_, Option<String>>(13)?,
                    ))
                },
            )
            .ok();

        if let Some((
            oid,
            idem,
            req_dev,
            req_act,
            rec_dev,
            rec_srv,
            kind_str,
            tgt_kind,
            tgt_id,
            digest,
            obs_at,
            val_at,
            rec_at,
            rcpt_id,
        )) = result
        {
            Ok(Some(OperationEnvelope {
                operation_id: OperationId::parse(&oid)?,
                idempotency_key: idem,
                requesting_device_id: DeviceId::parse(&req_dev)?,
                requesting_actor_id: ActorId::parse(&req_act)?,
                recording_device_id: DeviceId::parse(&rec_dev)?,
                recording_server_id: DeviceId::parse(&rec_srv)?,
                operation_kind: OperationKind::parse(&kind_str, &oid)?,
                target_kind: tgt_kind,
                target_id: tgt_id,
                content_digest: digest,
                observed_at: obs_at,
                valid_time: val_at,
                recorded_at: rec_at,
                receipt_id: rcpt_id,
            }))
        } else {
            Ok(None)
        }
    }

    pub async fn get_operation_by_idempotency_key(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<OperationEnvelope>, MnemesError> {
        let operation_id = {
            let conn = self.pool_conn.lock().await;
            conn.query_row(
                "SELECT operation_id FROM operation_envelopes WHERE idempotency_key = ?1 LIMIT 1",
                params![idempotency_key],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        };

        if let Some(operation_id) = operation_id {
            self.get_operation(&OperationId::parse(&operation_id)?)
                .await
        } else {
            Ok(None)
        }
    }

    pub async fn get_operation_by_receipt(
        &self,
        receipt_id: &str,
    ) -> Result<Option<OperationEnvelope>, MnemesError> {
        let result = {
            let conn = self.pool_conn.lock().await;
            conn.query_row(
                "SELECT operation_id FROM operation_envelopes WHERE receipt_id = ?1 LIMIT 1",
                params![receipt_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        };

        if let Some(oid) = result {
            self.get_operation(&OperationId::parse(&oid)?).await
        } else {
            Ok(None)
        }
    }

    pub async fn list_operations(
        &self,
        device_id: Option<&DeviceId>,
        actor_id: Option<&ActorId>,
        limit: usize,
    ) -> Result<Vec<OperationEnvelope>, MnemesError> {
        let conn = self.pool_conn.lock().await;

        let mut sql = String::from(
            "SELECT operation_id, idempotency_key, requesting_device_id, \
             requesting_actor_id, recording_device_id, recording_server_id, \
             operation_kind, target_kind, target_id, content_digest, \
             observed_at, valid_time, recorded_at, receipt_id \
             FROM operation_envelopes WHERE 1=1",
        );
        let mut params_values: Vec<String> = Vec::new();

        if let Some(device_id) = device_id {
            sql.push_str(" AND requesting_device_id = ?");
            params_values.push(device_id.as_str().to_string());
        }

        if let Some(actor_id) = actor_id {
            sql.push_str(" AND requesting_actor_id = ?");
            params_values.push(actor_id.as_str().to_string());
        }

        sql.push_str(" ORDER BY recorded_at DESC LIMIT ?");
        let effective_limit = if limit == 0 { 100 } else { limit };
        params_values.push(effective_limit.to_string());

        let mut statement = conn.prepare(&sql)?;
        let mut rows = {
            let raw_params: Vec<rusqlite::types::Value> = params_values
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    if index + 1 == params_values.len() {
                        rusqlite::types::Value::Integer(value.parse::<i64>().unwrap_or(100))
                    } else {
                        rusqlite::types::Value::Text(value.clone())
                    }
                })
                .collect();
            statement.query(rusqlite::params_from_iter(raw_params))?
        };

        let mut operations = Vec::new();
        while let Some(row) = rows.next()? {
            let value = self.map_row_to_operation(row)?;
            operations.push(value);
        }

        Ok(operations)
    }

    fn map_row_to_operation(
        &self,
        row: &rusqlite::Row<'_>,
    ) -> Result<OperationEnvelope, rusqlite::Error> {
        Ok(OperationEnvelope {
            operation_id: OperationId::parse(&row.get::<_, String>(0)?).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?,
            idempotency_key: row.get(1)?,
            requesting_device_id: DeviceId::parse(&row.get::<_, String>(2)?).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    2,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?,
            requesting_actor_id: ActorId::parse(&row.get::<_, String>(3)?).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    3,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?,
            recording_device_id: DeviceId::parse(&row.get::<_, String>(4)?).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    4,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?,
            recording_server_id: DeviceId::parse(&row.get::<_, String>(5)?).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    5,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?,
            operation_kind: OperationKind::parse(&row.get::<_, String>(6)?, "operation").map_err(
                |error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        6,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                },
            )?,
            target_kind: row.get(7)?,
            target_id: row.get(8)?,
            content_digest: row.get(9)?,
            observed_at: row.get(10)?,
            valid_time: row.get(11)?,
            recorded_at: row.get(12)?,
            receipt_id: row.get(13)?,
        })
    }

    pub async fn check_idempotency(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<String>, MnemesError> {
        let conn = self.pool_conn.lock().await;
        let result = conn
            .query_row(
                "SELECT receipt_id FROM operation_envelopes WHERE idempotency_key = ?1 LIMIT 1",
                params![idempotency_key],
                |row| row.get::<_, Option<String>>(0),
            )
            .ok()
            .flatten();
        Ok(result)
    }

    pub async fn log_audit_event(
        &self,
        device: Option<&DeviceId>,
        actor: Option<&ActorId>,
        endpoint: &str,
        method: &str,
        outcome: &str,
        detail: Option<&str>,
    ) -> Result<(), MnemesError> {
        let conn = self.pool_conn.lock().await;
        let now = Utc::now().to_rfc3339();
        let event_id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO audit_events (event_id, device_id, actor_id, endpoint, method, outcome, detail, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                event_id,
                device.map(DeviceId::as_str),
                actor.map(ActorId::as_str),
                endpoint,
                method,
                outcome,
                detail,
                now,
            ],
        )?;
        Ok(())
    }

    pub async fn list_audit_events(
        &self,
        limit: usize,
    ) -> Result<Vec<AuditEvent>, MnemesError> {
        let conn = self.pool_conn.lock().await;
        let effective_limit = if limit == 0 { 100 } else { limit };
        let mut stmt = conn.prepare(
            "SELECT event_id, device_id, actor_id, endpoint, method, outcome, detail, created_at \
             FROM audit_events ORDER BY created_at DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![effective_limit.to_string()], |row| {
            Ok(AuditEvent {
                event_id: row.get::<_, String>(0)?,
                device_id: row.get::<_, Option<String>>(1)?,
                actor_id: row.get::<_, Option<String>>(2)?,
                endpoint: row.get::<_, String>(3)?,
                method: row.get::<_, String>(4)?,
                outcome: row.get::<_, String>(5)?,
                detail: row.get::<_, Option<String>>(6)?,
                created_at: row.get::<_, String>(7)?,
            })
        })?;

        let mut events = Vec::new();
        for row in rows {
            events.push(row?);
        }

        Ok(events)
    }

    // ─── Provenance edge helpers ─────────────────────────────────────

    fn parse_rfc3339(value: &str, field: &str) -> Result<DateTime<Utc>, MnemesError> {
        DateTime::parse_from_rfc3339(value)
            .map_err(|error| {
                MnemesError::InvalidProvenance(format!("invalid {field}: {value} ({error})"))
            })
            .map(|value| value.with_timezone(&Utc))
    }

    fn parse_optional_rfc3339(
        value: Option<String>,
        field: &str,
    ) -> Result<Option<DateTime<Utc>>, MnemesError> {
        value
            .map(|value| Self::parse_rfc3339(&value, field))
            .transpose()
    }

    fn as_of_time_params(as_of: AsOf) -> (String, Option<String>) {
        (
            as_of
                .recorded_at_or_before
                .unwrap_or_else(|| Utc::now().to_rfc3339()),
            as_of.valid_at.clone(),
        )
    }

    fn validate_item_ref(item: &MemoryItemRef) -> Result<(), MnemesError> {
        if item.kind.trim().is_empty() || item.id.trim().is_empty() {
            return Err(MnemesError::InvalidProvenance(
                "memory item references require non-empty kind and id".to_string(),
            ));
        }
        Ok(())
    }

    fn normalize_metadata(raw: &Option<String>) -> Result<Option<String>, MnemesError> {
        raw.as_ref()
            .map(|raw| {
                let parsed = serde_json::from_str::<Value>(raw).map_err(|error| {
                    MnemesError::InvalidProvenance(format!("invalid metadata JSON: {error}"))
                })?;
                serde_json::to_string(&parsed).map_err(|error| {
                    MnemesError::InvalidProvenance(format!(
                        "failed to canonicalize metadata JSON: {error}"
                    ))
                })
            })
            .transpose()
    }

    fn parse_metadata_for_result(raw: Option<String>) -> Result<Option<Value>, MnemesError> {
        raw.map(|value| serde_json::from_str::<Value>(&value))
            .transpose()
            .map_err(|error| {
                MnemesError::InvalidProvenance(format!(
                    "invalid metadata JSON in stored edge: {error}"
                ))
            })
    }

    fn map_row_to_provenance_edge(
        row: &rusqlite::Row<'_>,
    ) -> Result<ProvenanceEdge, rusqlite::Error> {
        let source_kind: String = row.get(1)?;
        let source_id: String = row.get(2)?;
        let target_kind: String = row.get(3)?;
        let target_id: String = row.get(4)?;
        let op_id_str: Option<String> = row.get(5)?;
        let edge_type: String = row.get(6)?;
        let actor_id_str: Option<String> = row.get(7)?;
        let device_id_str: Option<String> = row.get(8)?;
        let metadata_str: Option<String> = row.get(14)?;

        Ok(ProvenanceEdge {
            edge_id: ProvenanceEdgeId::parse(row.get::<_, String>(0)?).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?,
            edge_type: ProvenanceEdgeType::parse(&edge_type, "provenance edge").map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?,
            source: MemoryItemRef::new(source_kind, source_id).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?,
            target: MemoryItemRef::new(target_kind, target_id).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?,
            operation_id: op_id_str
                .map(|value| OperationId::parse(&value))
                .transpose()
                .map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?,
            actor_id: actor_id_str
                .map(|value| ActorId::parse(&value))
                .transpose()
                .map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?,
            device_id: device_id_str
                .map(|value| DeviceId::parse(&value))
                .transpose()
                .map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?,
            valid_from: Self::parse_optional_rfc3339(
                row.get::<_, Option<String>>(9)?,
                "valid_from",
            )
            .map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?,
            valid_to: Self::parse_optional_rfc3339(row.get::<_, Option<String>>(10)?, "valid_to")
                .map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?,
            observed_at: Self::parse_optional_rfc3339(
                row.get::<_, Option<String>>(11)?,
                "observed_at",
            )
            .map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?,
            recorded_at: Self::parse_rfc3339(&row.get::<_, String>(12)?, "recorded_at").map_err(
                |e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                },
            )?,
            content_digest: row.get::<_, Option<String>>(13)?,
            metadata: Self::parse_metadata_for_result(metadata_str).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?,
            supersedes_edge_id: row
                .get::<_, Option<String>>(15)?
                .map(|value| ProvenanceEdgeId::parse(&value))
                .transpose()
                .map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?,
        })
    }

    fn operation_matches(
        conn: &rusqlite::Connection,
        request: &ProvenanceEdgeRequest,
    ) -> Result<(), MnemesError> {
        if request.operation_id.is_none() {
            return Err(MnemesError::InvalidProvenance(
                "operation_id is required for provenance edge mutation".to_string(),
            ));
        }

        let operation_id = request
            .operation_id
            .as_ref()
            .expect("operation_id is required for provenance edges");
        let operation = Self::fetch_operation_conn(conn, operation_id)?.ok_or_else(|| {
            MnemesError::InvalidProvenance(format!("operation {operation_id} not found"))
        })?;

        if request.edge_type == ProvenanceEdgeType::ObservedBy
            && operation.operation_kind != OperationKind::Observe
        {
            return Err(MnemesError::InvalidProvenance(
                "observed_by requires an observe operation".to_string(),
            ));
        }

        if let Some(actor_id) = &request.actor_id {
            if actor_id != &operation.requesting_actor_id {
                return Err(MnemesError::InvalidProvenance(format!(
                    "actor {actor_id} does not match requesting actor for operation {operation_id}"
                )));
            }
        }

        if let Some(device_id) = &request.device_id {
            let matched = *device_id == operation.requesting_device_id
                || *device_id == operation.recording_device_id
                || *device_id == operation.recording_server_id;
            if !matched {
                return Err(MnemesError::InvalidProvenance(format!(
                    "device {device_id} does not match operation {operation_id} context"
                )));
            }
        }

        Ok(())
    }

    fn fetch_operation_conn(
        conn: &rusqlite::Connection,
        operation_id: &OperationId,
    ) -> Result<Option<OperationEnvelope>, MnemesError> {
        let operation_id = operation_id.as_str().to_string();
        let result = conn
            .query_row(
                "SELECT operation_id, idempotency_key, requesting_device_id, \
                 requesting_actor_id, recording_device_id, recording_server_id, \
                 operation_kind, target_kind, target_id, content_digest, \
                 observed_at, valid_time, recorded_at, receipt_id \
                 FROM operation_envelopes WHERE operation_id = ?1",
                params![operation_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, String>(8)?,
                        row.get::<_, String>(9)?,
                        row.get::<_, Option<String>>(10)?,
                        row.get::<_, Option<String>>(11)?,
                        row.get::<_, String>(12)?,
                        row.get::<_, Option<String>>(13)?,
                    ))
                },
            )
            .ok();

        if let Some((
            oid,
            idem,
            req_dev,
            req_act,
            rec_dev,
            rec_srv,
            kind_str,
            tgt_kind,
            tgt_id,
            digest,
            obs_at,
            val_at,
            rec_at,
            rcpt_id,
        )) = result
        {
            Ok(Some(OperationEnvelope {
                operation_id: OperationId::parse(&oid)?,
                idempotency_key: idem,
                requesting_device_id: DeviceId::parse(&req_dev)?,
                requesting_actor_id: ActorId::parse(&req_act)?,
                recording_device_id: DeviceId::parse(&rec_dev)?,
                recording_server_id: DeviceId::parse(&rec_srv)?,
                operation_kind: OperationKind::parse(&kind_str, &oid)?,
                target_kind: tgt_kind,
                target_id: tgt_id,
                content_digest: digest,
                observed_at: obs_at,
                valid_time: val_at,
                recorded_at: rec_at,
                receipt_id: rcpt_id,
            }))
        } else {
            Ok(None)
        }
    }

    fn existing_edges(
        conn: &rusqlite::Connection,
        request: &ProvenanceEdgeRequest,
    ) -> Result<Vec<ProvenanceEdge>, MnemesError> {
        let mut stmt = conn.prepare(
            "SELECT edge_id, source_kind, source_id, target_kind, target_id, operation_id, \
             edge_type, actor_id, device_id, valid_from, valid_to, observed_at, recorded_at, \
             content_digest, metadata, supersedes_edge_id \
             FROM provenance_edges \
             WHERE operation_id = ?1 AND edge_type = ?2 AND source_kind = ?3 \
               AND source_id = ?4 AND target_kind = ?5 AND target_id = ?6",
        )?;

        let rows = stmt.query_map(
            params![
                request.operation_id.as_ref().map(|id| id.as_str()),
                request.edge_type.as_str(),
                request.source.kind,
                request.source.id,
                request.target.kind,
                request.target.id,
            ],
            Self::map_row_to_provenance_edge,
        )?;

        let mut values = Vec::new();
        for row in rows {
            values.push(row?);
        }
        Ok(values)
    }

    fn edge_matches_request(
        edge: &ProvenanceEdge,
        request: &ProvenanceEdgeRequest,
        canonical_metadata: &Option<Value>,
    ) -> bool {
        edge.edge_type == request.edge_type
            && edge.source == request.source
            && edge.target == request.target
            && edge.operation_id == request.operation_id
            && edge.actor_id == request.actor_id
            && edge.device_id == request.device_id
            && edge.valid_from == request.valid_from
            && edge.valid_to == request.valid_to
            && edge.observed_at == request.observed_at
            && edge.content_digest == request.content_digest
            && edge.supersedes_edge_id == request.supersedes_edge_id
            && edge.metadata == *canonical_metadata
    }

    fn validate_supersedes_reference(
        conn: &rusqlite::Connection,
        request: &ProvenanceEdgeRequest,
    ) -> Result<(), MnemesError> {
        if let Some(supersedes_edge_id) = &request.supersedes_edge_id {
            let exists: bool = conn
                .query_row(
                    "SELECT 1 FROM provenance_edges WHERE edge_id = ?1 LIMIT 1",
                    params![supersedes_edge_id.as_str()],
                    |row| row.get::<_, i64>(0),
                )
                .optional()?
                .is_some();
            if !exists {
                return Err(MnemesError::InvalidProvenance(format!(
                    "supersedes_edge_id {supersedes_edge_id} does not exist"
                )));
            }
        }
        Ok(())
    }

    fn record_provenance_edge_conn(
        conn: &rusqlite::Connection,
        request: ProvenanceEdgeRequest,
    ) -> Result<ProvenanceEdge, MnemesError> {
        Self::validate_item_ref(&request.source)?;
        Self::validate_item_ref(&request.target)?;

        if request.source == request.target {
            return Err(MnemesError::InvalidProvenance(
                "self-referential provenance edge is forbidden".to_string(),
            ));
        }

        if let (Some(from), Some(to)) = (request.valid_from, request.valid_to) {
            if to < from {
                return Err(MnemesError::InvalidAsOf(
                    "valid_to cannot be earlier than valid_from".to_string(),
                ));
            }
        }

        Self::operation_matches(conn, &request)?;
        Self::validate_supersedes_reference(conn, &request)?;

        let canonical_metadata = Self::normalize_metadata(&request.metadata)?;
        let canonical_metadata_value: Option<Value> = canonical_metadata
            .as_ref()
            .map(|value| serde_json::from_str(value))
            .transpose()
            .map_err(|error| {
                MnemesError::InvalidProvenance(format!(
                    "invalid normalized metadata JSON: {error}"
                ))
            })?;

        let existing = Self::existing_edges(conn, &request)?;
        if !existing.is_empty() {
            for edge in existing {
                if Self::edge_matches_request(&edge, &request, &canonical_metadata_value) {
                    return Ok(edge);
                }
            }
            return Err(MnemesError::IdempotencyConflict(format!(
                "conflicting provenance edge for operation {}",
                request.operation_id.as_ref().unwrap()
            )));
        }

        let edge_id = ProvenanceEdgeId::new();
        let recorded_at = request.recorded_at.unwrap_or_else(Utc::now);
        let valid_from = request.valid_from.map(|dt| dt.to_rfc3339());
        let valid_to = request.valid_to.map(|dt| dt.to_rfc3339());
        let observed_at = request.observed_at.map(|dt| dt.to_rfc3339());

        conn.execute(
            "INSERT INTO provenance_edges (
                edge_id, edge_type, source_kind, source_id, target_kind, target_id,
                operation_id, actor_id, device_id, valid_from, valid_to, observed_at,
                recorded_at, content_digest, metadata, supersedes_edge_id
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            params![
                edge_id.as_str(),
                request.edge_type.as_str(),
                request.source.kind.as_str(),
                request.source.id.as_str(),
                request.target.kind.as_str(),
                request.target.id.as_str(),
                request.operation_id.as_ref().map(OperationId::as_str),
                request.actor_id.as_ref().map(ActorId::as_str),
                request.device_id.as_ref().map(DeviceId::as_str),
                valid_from,
                valid_to,
                observed_at,
                recorded_at.to_rfc3339(),
                request.content_digest,
                canonical_metadata,
                request
                    .supersedes_edge_id
                    .as_ref()
                    .map(ProvenanceEdgeId::as_str),
            ],
        )?;

        Ok(ProvenanceEdge {
            edge_id,
            edge_type: request.edge_type,
            source: request.source,
            target: request.target,
            operation_id: request.operation_id,
            actor_id: request.actor_id,
            device_id: request.device_id,
            valid_from: request.valid_from,
            valid_to: request.valid_to,
            observed_at: request.observed_at,
            recorded_at,
            content_digest: request.content_digest,
            metadata: canonical_metadata_value,
            supersedes_edge_id: request.supersedes_edge_id,
        })
    }

    pub async fn record_provenance_edge(
        &self,
        request: ProvenanceEdgeRequest,
    ) -> Result<ProvenanceEdge, MnemesError> {
        let mut conn = self.pool_conn.lock().await;
        let tx = conn.transaction()?;
        let edge = Self::record_provenance_edge_conn(&tx, request)?;
        tx.commit()?;
        Ok(edge)
    }

    pub async fn record_provenance_edges(
        &self,
        requests: &[ProvenanceEdgeRequest],
    ) -> Result<Vec<ProvenanceEdge>, MnemesError> {
        let mut conn = self.pool_conn.lock().await;
        let tx = conn.transaction()?;
        let mut edges = Vec::with_capacity(requests.len());

        for request in requests {
            let edge = Self::record_provenance_edge_conn(&tx, request.clone())?;
            edges.push(edge);
        }

        tx.commit()?;
        Ok(edges)
    }

    pub async fn get_provenance_edge(
        &self,
        edge_id: &ProvenanceEdgeId,
    ) -> Result<Option<ProvenanceEdge>, MnemesError> {
        let conn = self.pool_conn.lock().await;
        let row = conn
            .query_row(
                "SELECT edge_id, source_kind, source_id, target_kind, target_id, operation_id,
                        edge_type, actor_id, device_id, valid_from, valid_to, observed_at, recorded_at,
                        content_digest, metadata, supersedes_edge_id
                 FROM provenance_edges WHERE edge_id = ?1",
                params![edge_id.as_str()],
                Self::map_row_to_provenance_edge,
            )
            .optional()?;
        Ok(row)
    }

    pub async fn query_provenance_edges(
        &self,
        query: ProvenanceQuery,
    ) -> Result<Vec<ProvenanceEdge>, MnemesError> {
        let (as_of_recorded, as_of_valid) = Self::as_of_time_params(query.as_of);
        let mut sql = String::from(
            "SELECT edge_id, source_kind, source_id, target_kind, target_id, operation_id,
                    edge_type, actor_id, device_id, valid_from, valid_to, observed_at, recorded_at,
                    content_digest, metadata, supersedes_edge_id
             FROM provenance_edges
             WHERE recorded_at <= ?1
               AND (?2 IS NULL OR
                    ((valid_from IS NULL OR valid_from <= ?2)
                     AND (valid_to IS NULL OR ?2 < valid_to)))",
        );

        let mut args: Vec<rusqlite::types::Value> = vec![
            rusqlite::types::Value::Text(as_of_recorded),
            as_of_valid
                .map(rusqlite::types::Value::Text)
                .unwrap_or(rusqlite::types::Value::Null),
        ];

        if let Some(source) = query.source {
            sql.push_str(" AND source_kind = ? AND source_id = ?");
            args.push(rusqlite::types::Value::Text(source.kind));
            args.push(rusqlite::types::Value::Text(source.id));
        }

        if let Some(target) = query.target {
            sql.push_str(" AND target_kind = ? AND target_id = ?");
            args.push(rusqlite::types::Value::Text(target.kind));
            args.push(rusqlite::types::Value::Text(target.id));
        }

        if let Some(operation_id) = query.operation_id {
            sql.push_str(" AND operation_id = ?");
            args.push(rusqlite::types::Value::Text(
                operation_id.as_str().to_string(),
            ));
        }

        if !query.edge_types.is_empty() {
            sql.push_str(" AND edge_type IN (");
            for (index, edge_type) in query.edge_types.iter().enumerate() {
                if index > 0 {
                    sql.push(',');
                }
                sql.push('?');
                args.push(rusqlite::types::Value::Text(edge_type.as_str().to_string()));
            }
            sql.push(')');
        }

        if !query.include_superseded {
            sql.push_str(
                " AND NOT EXISTS (
                    SELECT 1
                    FROM provenance_edges AS supersedes
                    WHERE supersedes.edge_type = 'supersedes'
                      AND supersedes.recorded_at <= ?1
                      AND (?2 IS NULL OR
                           ((supersedes.valid_from IS NULL OR supersedes.valid_from <= ?2)
                             AND (supersedes.valid_to IS NULL OR ?2 < supersedes.valid_to)))
                      AND supersedes.target_kind = provenance_edges.target_kind
                      AND supersedes.target_id = provenance_edges.target_id
                      AND supersedes.edge_id <> provenance_edges.edge_id
                )",
            );
        }

        let limit = if query.limit == 0 { 100 } else { query.limit };
        sql.push_str(" ORDER BY rowid ASC LIMIT ");
        sql.push_str(&limit.to_string());

        let conn = self.pool_conn.lock().await;
        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query(rusqlite::params_from_iter(args))?;
        let mut edges = Vec::new();
        while let Some(row) = rows.next()? {
            edges.push(Self::map_row_to_provenance_edge(row)?);
        }
        Ok(edges)
    }

    pub async fn lineage(
        &self,
        root: MemoryItemRef,
        direction: GraphDirection,
        max_depth: usize,
        as_of: AsOf,
    ) -> Result<LineageResult, MnemesError> {
        let mut visited_nodes: HashSet<String> = HashSet::new();
        let mut queue: VecDeque<(MemoryItemRef, usize)> = VecDeque::new();
        let mut edges = Vec::new();
        let mut seen_edges: HashSet<String> = HashSet::new();
        let mut operation_ids: HashSet<String> = HashSet::new();
        let mut truncated = false;

        visited_nodes.insert(root.canonical_key());
        queue.push_back((root.clone(), 0));

        if root.kind == "operation" {
            operation_ids.insert(root.id.clone());
        }

        while let Some((current, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }

            let discovered = match direction {
                GraphDirection::Outgoing => {
                    self.query_provenance_edges(ProvenanceQuery {
                        source: Some(current.clone()),
                        as_of: as_of.clone(),
                        include_superseded: true,
                        limit: 10000,
                        ..Default::default()
                    })
                    .await?
                }
                GraphDirection::Incoming => {
                    self.query_provenance_edges(ProvenanceQuery {
                        target: Some(current.clone()),
                        as_of: as_of.clone(),
                        include_superseded: true,
                        limit: 10000,
                        ..Default::default()
                    })
                    .await?
                }
                GraphDirection::Both => {
                    let mut outgoing = self
                        .query_provenance_edges(ProvenanceQuery {
                            source: Some(current.clone()),
                            as_of: as_of.clone(),
                            include_superseded: true,
                            limit: 10000,
                            ..Default::default()
                        })
                        .await?;
                    let mut incoming = self
                        .query_provenance_edges(ProvenanceQuery {
                            target: Some(current.clone()),
                            as_of: as_of.clone(),
                            include_superseded: true,
                            limit: 10000,
                            ..Default::default()
                        })
                        .await?;
                    outgoing.append(&mut incoming);
                    outgoing
                }
            };

            for edge in discovered {
                if !seen_edges.insert(edge.edge_id.to_string()) {
                    continue;
                }

                edges.push(edge.clone());

                if edge.source.kind == "operation" {
                    operation_ids.insert(edge.source.id.clone());
                }
                if edge.target.kind == "operation" {
                    operation_ids.insert(edge.target.id.clone());
                }

                let next = match direction {
                    GraphDirection::Outgoing => {
                        if edge.source == current {
                            Some(edge.target)
                        } else {
                            None
                        }
                    }
                    GraphDirection::Incoming => {
                        if edge.target == current {
                            Some(edge.source)
                        } else {
                            None
                        }
                    }
                    GraphDirection::Both => {
                        if edge.source == current {
                            Some(edge.target)
                        } else if edge.target == current {
                            Some(edge.source)
                        } else {
                            None
                        }
                    }
                };

                if let Some(next_node) = next {
                    if visited_nodes.insert(next_node.canonical_key()) {
                        if depth + 1 < max_depth {
                            queue.push_back((next_node, depth + 1));
                        } else {
                            truncated = true;
                        }
                    }
                }
            }
        }

        let mut items: Vec<MemoryItemRef> = visited_nodes
            .into_iter()
            .filter_map(|key| MemoryItemRef::parse_key(&key).ok())
            .filter(|item| item.kind != "operation")
            .collect();
        items.sort_by_key(|item| item.canonical_key());

        let as_of_recorded_str = as_of
            .recorded_at_or_before
            .clone()
            .unwrap_or_else(|| Utc::now().to_rfc3339());
        let as_of_recorded = Self::parse_rfc3339(&as_of_recorded_str, "as_of_recorded")?;
        let mut operations = Vec::new();
        let conn = self.pool_conn.lock().await;
        for operation_id in operation_ids {
            let operation_id = OperationId::parse(&operation_id)?;
            let Some(operation) = Self::fetch_operation_conn(&conn, &operation_id)? else {
                continue;
            };
            let operation_recorded =
                Self::parse_rfc3339(&operation.recorded_at, "operation recorded_at")?;
            if operation_recorded <= as_of_recorded {
                operations.push(operation);
            }
        }

        operations.sort_by(|a, b| a.recorded_at.cmp(&b.recorded_at));

        Ok(LineageResult {
            root,
            edges,
            items,
            operations,
            truncated,
            as_of: as_of.clone(),
        })
    }

    pub async fn operation_provenance(
        &self,
        operation_id: &OperationId,
        as_of: AsOf,
    ) -> Result<(OperationEnvelope, Vec<ProvenanceEdge>), MnemesError> {
        let operation = self
            .get_operation(operation_id)
            .await?
            .ok_or_else(|| MnemesError::ProvenanceEdgeNotFound(operation_id.to_string()))?;

        let as_of_recorded_str = as_of
            .recorded_at_or_before
            .clone()
            .unwrap_or_else(|| Utc::now().to_rfc3339());
        let as_of_recorded = Self::parse_rfc3339(&as_of_recorded_str, "as_of_recorded")?;
        let operation_recorded =
            Self::parse_rfc3339(&operation.recorded_at, "operation recorded_at")?;
        if operation_recorded > as_of_recorded {
            return Err(MnemesError::ProvenanceEdgeNotFound(format!(
                "operation {operation_id} not visible at requested as_of"
            )));
        }

        let edges = self
            .query_provenance_edges(ProvenanceQuery {
                operation_id: Some(operation_id.clone()),
                as_of: as_of.clone(),
                limit: 10000,
                include_superseded: true,
                ..Default::default()
            })
            .await?;

        Ok((operation, edges))
    }

    pub async fn supersede(
        &self,
        newer: MemoryItemRef,
        prior: MemoryItemRef,
        operation_id: OperationId,
        valid_from: Option<DateTime<Utc>>,
        valid_to: Option<DateTime<Utc>>,
        metadata: Option<String>,
    ) -> Result<ProvenanceEdge, MnemesError> {
        self.record_provenance_edge(ProvenanceEdgeRequest {
            edge_type: ProvenanceEdgeType::Supersedes,
            source: newer,
            target: prior,
            operation_id: Some(operation_id),
            actor_id: None,
            device_id: None,
            valid_from,
            valid_to,
            observed_at: None,
            recorded_at: None,
            content_digest: None,
            metadata,
            supersedes_edge_id: None,
        })
        .await
    }

    pub async fn contradict(
        &self,
        evidence: MemoryItemRef,
        target: MemoryItemRef,
        operation_id: OperationId,
        valid_from: Option<DateTime<Utc>>,
        valid_to: Option<DateTime<Utc>>,
        metadata: Option<String>,
    ) -> Result<ProvenanceEdge, MnemesError> {
        self.record_provenance_edge(ProvenanceEdgeRequest {
            edge_type: ProvenanceEdgeType::Contradicts,
            source: evidence,
            target,
            operation_id: Some(operation_id),
            actor_id: None,
            device_id: None,
            valid_from,
            valid_to,
            observed_at: None,
            recorded_at: None,
            content_digest: None,
            metadata,
            supersedes_edge_id: None,
        })
        .await
    }

    pub async fn as_of_item_lineage(
        &self,
        item: MemoryItemRef,
        as_of: AsOf,
    ) -> Result<LineageResult, MnemesError> {
        self.lineage(item, GraphDirection::Both, usize::MAX / 2, as_of)
            .await
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use semantic_memory::{EmbeddingConfig, MemoryConfig, MockEmbedder};
    use tempfile::TempDir;

    fn open_test_store() -> (MnemesStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let config = MemoryConfig {
            base_dir: dir.path().to_path_buf(),
            embedding: EmbeddingConfig {
                dimensions: 768,
                ..Default::default()
            },
            ..Default::default()
        };
        let store = MnemesStore::open_with_embedder(
            dir.path().to_path_buf(),
            config,
            Box::new(MockEmbedder::new(768)),
        )
        .unwrap();
        (store, dir)
    }

    #[tokio::test]
    async fn register_and_get_device() {
        let (store, _dir) = open_test_store();
        let dev_id = DeviceId::new();
        let device = Device::new(dev_id.clone(), "laptop", "linux", "nobara-pc");
        let returned = store.register_device(device).await.unwrap();
        assert_eq!(returned, dev_id);

        let fetched = store.get_device(&dev_id).await.unwrap().unwrap();
        assert_eq!(fetched.device_id, dev_id);
        assert_eq!(fetched.label, "laptop");
        assert_eq!(fetched.status, DeviceStatus::Active);
    }

    #[tokio::test]
    async fn revoke_device_blocks_heartbeat() {
        let (store, _dir) = open_test_store();
        let dev_id = DeviceId::new();
        store
            .register_device(Device::new(dev_id.clone(), "server", "linux", "msi"))
            .await
            .unwrap();
        store.revoke_device(&dev_id).await.unwrap();
        assert!(store.heartbeat_device(&dev_id).await.is_err());
    }

    #[tokio::test]
    async fn list_devices_returns_all() {
        let (store, _dir) = open_test_store();
        store
            .register_device(Device::new(DeviceId::new(), "d1", "linux", "h1"))
            .await
            .unwrap();
        store
            .register_device(Device::new(DeviceId::new(), "d2", "linux", "h2"))
            .await
            .unwrap();
        let devices = store.list_devices().await.unwrap();
        assert_eq!(devices.len(), 2);
    }

    #[tokio::test]
    async fn register_and_get_actor() {
        let (store, _dir) = open_test_store();
        let dev_id = DeviceId::new();
        store
            .register_device(Device::new(dev_id.clone(), "test", "linux", "host"))
            .await
            .unwrap();
        let actor_id = ActorId::new();
        store
            .register_actor(Actor::new(
                actor_id.clone(),
                dev_id.clone(),
                ActorKind::Hermes,
            ))
            .await
            .unwrap();
        let fetched = store.get_actor(&actor_id).await.unwrap().unwrap();
        assert_eq!(fetched.actor_id, actor_id);
        assert_eq!(fetched.device_id, dev_id);
        assert_eq!(fetched.actor_kind, ActorKind::Hermes);
    }

    #[tokio::test]
    async fn submit_operation_is_idempotent() {
        let (store, _dir) = open_test_store();
        let dev_id = DeviceId::new();
        store
            .register_device(Device::new(dev_id.clone(), "test", "linux", "host"))
            .await
            .unwrap();
        let actor_id = ActorId::new();
        store
            .register_actor(Actor::new(
                actor_id.clone(),
                dev_id.clone(),
                ActorKind::Codex,
            ))
            .await
            .unwrap();

        let op_id = OperationId::new();
        let envelope = OperationEnvelope {
            operation_id: op_id,
            idempotency_key: "key-1".to_string(),
            requesting_device_id: dev_id.clone(),
            requesting_actor_id: actor_id.clone(),
            recording_device_id: dev_id.clone(),
            recording_server_id: dev_id.clone(),
            operation_kind: OperationKind::Observe,
            target_kind: "fact".to_string(),
            target_id: "f1".to_string(),
            content_digest: "sha256:abc".to_string(),
            observed_at: Some("2026-07-19T12:00:00Z".to_string()),
            valid_time: None,
            recorded_at: String::new(),
            receipt_id: None,
        };

        let r1 = store.submit_operation(envelope.clone()).await.unwrap();
        assert!(r1.starts_with("op-receipt:"));

        let mut env2 = envelope.clone();
        env2.operation_id = OperationId::new();
        let r2 = store.submit_operation(env2).await.unwrap();
        assert_eq!(r1, r2);
    }

    #[tokio::test]
    async fn get_operation_returns_full_envelope() {
        let (store, _dir) = open_test_store();
        let dev_id = DeviceId::new();
        store
            .register_device(Device::new(dev_id.clone(), "test", "linux", "host"))
            .await
            .unwrap();
        let actor_id = ActorId::new();
        store
            .register_actor(Actor::new(
                actor_id.clone(),
                dev_id.clone(),
                ActorKind::Hermes,
            ))
            .await
            .unwrap();

        let op_id = OperationId::new();
        let envelope = OperationEnvelope {
            operation_id: op_id.clone(),
            idempotency_key: "key-2".to_string(),
            requesting_device_id: dev_id.clone(),
            requesting_actor_id: actor_id.clone(),
            recording_device_id: dev_id.clone(),
            recording_server_id: dev_id.clone(),
            operation_kind: OperationKind::Assert,
            target_kind: "fact".to_string(),
            target_id: "f2".to_string(),
            content_digest: "sha256:def".to_string(),
            observed_at: None,
            valid_time: Some("2026-07-19T12:00:00Z".to_string()),
            recorded_at: String::new(),
            receipt_id: None,
        };

        store.submit_operation(envelope).await.unwrap();
        let fetched = store.get_operation(&op_id).await.unwrap().unwrap();
        assert_eq!(fetched.operation_kind, OperationKind::Assert);
        assert_eq!(fetched.target_id, "f2");
        assert!(!fetched.recorded_at.is_empty());
        assert!(fetched.receipt_id.is_some());
    }

    #[tokio::test]
    async fn memory_store_is_accessible() {
        let (store, _dir) = open_test_store();
        let device_id = DeviceId::new();
        store
            .register_device(Device::new(
                device_id.clone(),
                "memory-owner",
                "linux",
                "localhost",
            ))
            .await
            .unwrap();
        // Verify the device-owned semantic-memory store is accessible.
        let stats = store
            .device_memory(&device_id)
            .await
            .unwrap()
            .stats()
            .await
            .unwrap();
        assert_eq!(stats.total_facts, 0);
    }

    #[tokio::test]
    async fn bootstrap_creates_operator_and_is_authenticatable() {
        let (store, _dir) = open_test_store();
        let device = Device::new(DeviceId::new(), "bootstrap", "linux", "localhost");
        let (device_id, actor_id, credential, created_at) = store
            .bootstrap(device.clone(), ActorKind::Human)
            .await
            .unwrap();

        assert_eq!(device_id, device.device_id);
        assert!(!actor_id.to_string().is_empty());
        assert!(!credential.is_empty());
        assert!(!created_at.is_empty());

        let created_device = store.get_device(&device_id).await.unwrap().unwrap();
        assert_eq!(created_device.device_id, device_id);
        assert_eq!(created_device.label, device.label);
        assert_eq!(created_device.platform, device.platform);
        assert_eq!(created_device.hostname, device.hostname);

        let created_actor = store.get_actor(&actor_id).await.unwrap().unwrap();
        assert_eq!(created_actor.actor_id, actor_id);
        assert_eq!(created_actor.device_id, device_id);
        assert_eq!(created_actor.tool_profile, ToolProfile::Operator);
        assert_eq!(created_actor.actor_kind, ActorKind::Human);

        let (authed_device, authed_actor) = store
            .authenticate_request(&credential, Some(&actor_id))
            .await
            .unwrap();
        assert_eq!(authed_device.device_id, device_id);
        assert_eq!(authed_actor.unwrap().actor_id, actor_id);
    }

    #[tokio::test]
    async fn bootstrap_persists_only_credential_digest() {
        let (store, _dir) = open_test_store();
        let (device_id, actor_id, credential, _created_at) = store
            .bootstrap(
                Device::new(DeviceId::new(), "bootstrap", "linux", "localhost"),
                ActorKind::Service,
            )
            .await
            .unwrap();

        let device = store.get_device(&device_id).await.unwrap().unwrap();
        let secret = credential
            .strip_prefix(&format!("{device_id}:"))
            .expect("credential format should include device id");

        let fingerprint = device.credential_fingerprint.unwrap();
        assert!(!fingerprint.contains(secret));
        assert!(fingerprint.starts_with("sha256:"));
        assert!(store
            .authenticate_request(&credential, Some(&actor_id))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn bootstrap_rejects_if_not_first_device() {
        let (store, _dir) = open_test_store();
        let _ = store
            .bootstrap(
                Device::new(DeviceId::new(), "bootstrap", "linux", "localhost"),
                ActorKind::Human,
            )
            .await
            .unwrap();
        let second_attempt = store
            .bootstrap(
                Device::new(DeviceId::new(), "bootstrap", "linux", "localhost"),
                ActorKind::Human,
            )
            .await;

        assert!(matches!(
            second_attempt,
            Err(MnemesError::BootstrapRejected(_))
        ));

        let devices = store.list_devices().await.unwrap();
        let actors = store.list_actors().await.unwrap();
        assert_eq!(devices.len(), 1);
        assert_eq!(actors.len(), 1);
    }

    #[tokio::test]
    async fn bootstrap_is_atomic_if_actor_insert_fails() {
        let (store, _dir) = open_test_store();

        let preexisting_device_id = DeviceId::new();
        store
            .register_device(Device::new(
                preexisting_device_id.clone(),
                "existing",
                "linux",
                "host",
            ))
            .await
            .unwrap();
        let duplicate_actor_id = ActorId::new();
        store
            .register_actor(Actor::new(
                duplicate_actor_id.clone(),
                preexisting_device_id,
                ActorKind::Hermes,
            ))
            .await
            .unwrap();

        let actor = Actor {
            actor_id: duplicate_actor_id,
            device_id: DeviceId::new(),
            tool_profile: ToolProfile::Operator,
            actor_kind: ActorKind::Hermes,
            provider_model: None,
            recorded_at: String::new(),
        };

        let failed = store
            .bootstrap_with_actor(
                Device::new(DeviceId::new(), "new-device", "linux", "new-host"),
                actor,
            )
            .await;
        assert!(failed.is_err());

        let devices = store.list_devices().await.unwrap();
        let actors = store.list_actors().await.unwrap();
        assert_eq!(devices.len(), 1);
        assert_eq!(actors.len(), 1);
    }

    #[test]
    fn provider_name_defaults_to_candle_and_normalizes_aliases() {
        assert_eq!(configured_provider_name(None), "candle");
        assert_eq!(configured_provider_name(Some("  LOCAL ")), "local");
        assert_eq!(configured_provider_name(Some("OLLAMA")), "ollama");
    }

    #[test]
    fn provider_name_does_not_silently_fallback_unknown_values() {
        assert_eq!(configured_provider_name(Some("custom-provider")), "custom-provider");
    }
}
