//! Pooled memory store: device/actor/operation registry with its own SQLite
//! database and lazily opened device-owned `semantic_memory::MemoryStore` shards.

use crate::error::MnemesError;
use crate::profile_store::*;
use crate::replication::{SignedFactCreateBatchV1, SignedFactSupersedeBatchV1};
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
const POOLED_SCHEMA_GENERATION: i64 = 3;
const RECEIPT_AUTH_KEY_FILE: &str = ".routing-receipt-hmac.key";

/// Raw Mnemes-owned supersession admission projection from the control plane.
/// It deliberately excludes the owner semantic payload and stream state.
type FactSupersedeAdmissionRow = (Vec<u8>, i64, i64, i64, i64, i64, String);

/// Construct the process-wide embedder selected for the default server path.
///
/// `candle` is the default because it is local, in-process, and avoids a
/// second service hop. Shared-pool operators can select `ollama` (or inject
/// any implementation through `open_with_embedder`) without changing the
/// store or index contracts.
fn configured_provider_name(value: Option<&str>) -> String {
    value.unwrap_or("candle").trim().to_ascii_lowercase()
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
        // Testing only: a deterministic embedder that never touches the
        // network. The candle default eagerly downloads ~547 MB from
        // HuggingFace on first use, which CI runners cannot do reliably —
        // HF rate-limits the shared runner IP pool and six parallel CLI
        // tests fail on 'failed to download config.json: Rate limited'.
        "mock" => Ok(Box::new(semantic_memory::MockEmbedder::new(
            memory_config.embedding.dimensions,
        ))),
        other => Err(MnemesError::InvalidShardCatalog(format!(
            "unsupported MNEMES_EMBEDDER provider `{other}`; use candle, ollama, mock, or open_with_embedder"
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

/// Result of accepting one source fact into a device-owned shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FactSyncOutcome {
    /// The server re-embedded and persisted a new local fact.
    Synced { server_fact_id: String },
    /// This source fact was already accepted for this device.
    Skipped,
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
        self.get_key(device_id.as_str())
    }

    fn get_key(&mut self, key: &str) -> Option<Arc<semantic_memory::MemoryStore>> {
        let store = self.stores.get(key)?.clone();
        self.lru.retain(|value| value != key);
        self.lru.push_back(key.to_string());
        Some(store)
    }

    fn insert(&mut self, device_id: &DeviceId, store: Arc<semantic_memory::MemoryStore>) {
        self.insert_key(device_id.as_str(), store);
    }

    fn insert_key(&mut self, key: &str, store: Arc<semantic_memory::MemoryStore>) {
        let key = key.to_string();
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

fn profile_routing_receipt_digest(
    auth_key: &[u8; 32],
    receipt: &ProfileRoutingReceipt,
) -> Result<String, MnemesError> {
    let material = serde_json::to_string(&(
        "profile-routing-receipt-hmac-v1",
        &receipt.receipt_id,
        &receipt.actor_id,
        &receipt.subject_profile_id,
        &receipt.authorization_snapshot_digest,
        &receipt.authorized_stores,
        &receipt.selected_stores,
        &receipt.skipped_stores,
        &receipt.outcomes,
        receipt.complete,
        &receipt.final_result_ids,
        &receipt.query_sha256,
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

fn validate_profile_routing_receipt(
    auth_key: &[u8; 32],
    receipt: &ProfileRoutingReceipt,
) -> Result<(), MnemesError> {
    let invalid = |reason: &str| MnemesError::InvalidShardCatalog(reason.to_string());
    let is_digest =
        |value: &str| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit());
    if !is_digest(&receipt.authorization_snapshot_digest)
        || !is_digest(&receipt.query_sha256)
        || !is_digest(&receipt.receipt_digest)
    {
        return Err(invalid(
            "profile routing receipt contains an invalid digest",
        ));
    }
    let authorized = receipt
        .authorized_stores
        .iter()
        .map(|store| store.store_id.as_str())
        .collect::<HashSet<_>>();
    let selected = receipt
        .selected_stores
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let skipped = receipt
        .skipped_stores
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let outcomes = receipt
        .outcomes
        .iter()
        .map(|outcome| outcome.store_id.as_str())
        .collect::<HashSet<_>>();
    if authorized.len() != receipt.authorized_stores.len()
        || selected.len() != receipt.selected_stores.len()
        || skipped.len() != receipt.skipped_stores.len()
        || outcomes.len() != receipt.outcomes.len()
        || !selected.is_subset(&authorized)
        || !skipped.is_subset(&authorized)
        || !selected.is_disjoint(&skipped)
        || selected.union(&skipped).count() != authorized.len()
        || outcomes != selected
        || receipt.complete
            != receipt
                .outcomes
                .iter()
                .all(|outcome| outcome.error.is_none())
    {
        return Err(invalid(
            "profile routing receipt store sets are inconsistent",
        ));
    }
    let final_ids = receipt.final_result_ids.iter().collect::<HashSet<_>>();
    if final_ids.len() != receipt.final_result_ids.len() {
        return Err(invalid(
            "profile routing receipt contains duplicate final result IDs",
        ));
    }
    if profile_routing_receipt_digest(auth_key, receipt)? != receipt.receipt_digest {
        return Err(invalid("profile routing receipt authentication mismatch"));
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

fn load_or_create_receipt_auth_key(base_dir: &std::path::Path) -> Result<[u8; 32], MnemesError> {
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

/// One operator-provisioned fact-create signing-key admission record.
///
/// This record lives in Mnemes' control-plane SQLite database. It is never
/// accepted from the HTTP transport and must be provisioned by a trusted local
/// operator path before a device can submit signed fact-create batches.
#[derive(Debug, Clone)]
pub struct FactCreateAdmission {
    pub device_id: DeviceId,
    pub store_id: String,
    pub namespace: String,
    pub principal_id: String,
    pub key_version: u64,
    pub public_key: [u8; 32],
    pub activated_at: u64,
    pub cutoff_at: u64,
    pub stream_epoch: u64,
    pub fencing_token: String,
}

/// Combined Mnemes control-plane store.
///
/// Owns a device/actor/operation SQLite database alongside lazily opened
/// `semantic_memory::MemoryStore` shards. The databases are separate:
/// - `pooled.db` — device registry, actors, operation envelopes
/// - `memory/shards/<device_uuid>/memory.db` — semantic truth owned by semantic-memory
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FactCreateAckRecord {
    pub batch_id: String,
    pub request_digest: String,
    pub home_device_id: String,
    pub store_id: String,
    pub stream_epoch: u64,
    pub accepted_head: i64,
    pub disposition: String,
}

/// Operator-provisioned admission for one replacement namespace in the
/// supersession transport family.  This is deliberately independent of
/// fact-create admissions and of the owner stream epoch.
#[derive(Debug, Clone)]
pub struct FactSupersedeAdmission {
    pub device_id: DeviceId,
    pub store_id: String,
    pub replacement_namespace: String,
    pub principal_id: String,
    pub key_version: u64,
    pub public_key: [u8; 32],
    pub activated_at: u64,
    pub cutoff_at: u64,
    pub store_epoch: u64,
    pub writer_epoch: u64,
    pub fencing_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FactSupersedeAckRecord {
    pub batch_id: String,
    pub request_digest: String,
    pub home_device_id: String,
    pub store_id: String,
    pub owner_stream_epoch: u64,
    pub accepted_head: i64,
    pub disposition: String,
}

pub struct MnemesStore {
    pool_conn: tokio::sync::Mutex<rusqlite::Connection>,
    fact_create_gate: tokio::sync::Mutex<()>,
    fact_supersede_gate: tokio::sync::Mutex<()>,
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
            fact_create_gate: tokio::sync::Mutex::new(()),
            fact_supersede_gate: tokio::sync::Mutex::new(()),
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
            if !matches!(versions.as_slice(), [1] | [2] | [3] | [1, 2] | [1, 2, 3]) {
                return Err(MnemesError::InvalidShardCatalog(format!(
                    "unsupported pooled schema generations {versions:?}; expected a supported generation marker"
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

            CREATE TABLE IF NOT EXISTS memory_profiles (
                profile_id      TEXT PRIMARY KEY,
                owner_device_id TEXT NOT NULL REFERENCES devices(device_id),
                label           TEXT NOT NULL,
                status          TEXT NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active', 'revoked')),
                created_at      TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS memory_stores (
                store_id        TEXT PRIMARY KEY,
                profile_id      TEXT NOT NULL REFERENCES memory_profiles(profile_id),
                owner_device_id TEXT NOT NULL REFERENCES devices(device_id),
                namespace       TEXT NOT NULL,
                relative_path   TEXT NOT NULL UNIQUE,
                status          TEXT NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active', 'quarantined', 'revoked')),
                created_at      TEXT NOT NULL,
                CHECK (length(store_id) > 0),
                CHECK (length(namespace) > 0),
                CHECK (length(relative_path) > 0)
            );
            CREATE INDEX IF NOT EXISTS idx_memory_stores_profile
                ON memory_stores(profile_id, status);

            CREATE TABLE IF NOT EXISTS memory_access_grants (
                grant_id          TEXT PRIMARY KEY,
                grantee_profile_id TEXT NOT NULL REFERENCES memory_profiles(profile_id),
                store_id          TEXT NOT NULL REFERENCES memory_stores(store_id),
                namespace         TEXT NOT NULL,
                effect            TEXT NOT NULL CHECK (effect IN ('search', 'read', 'write')),
                issued_by_actor_id TEXT NOT NULL REFERENCES actors(actor_id),
                valid_from        INTEGER NOT NULL CHECK (valid_from >= 0),
                expires_at        INTEGER NOT NULL CHECK (expires_at > valid_from),
                revoked_at        INTEGER,
                created_at        TEXT NOT NULL,
                CHECK (revoked_at IS NULL OR revoked_at >= valid_from),
                UNIQUE(grantee_profile_id, store_id, namespace, effect, valid_from)
            );
            CREATE INDEX IF NOT EXISTS idx_memory_access_grants_lookup
                ON memory_access_grants(grantee_profile_id, store_id, namespace, effect, revoked_at);

            CREATE TABLE IF NOT EXISTS actor_profile_bindings (
                binding_id          TEXT PRIMARY KEY,
                actor_id            TEXT NOT NULL REFERENCES actors(actor_id),
                profile_id          TEXT NOT NULL REFERENCES memory_profiles(profile_id),
                owner_device_id     TEXT NOT NULL REFERENCES devices(device_id),
                issued_by_actor_id  TEXT NOT NULL REFERENCES actors(actor_id),
                valid_from          INTEGER NOT NULL CHECK (valid_from >= 0),
                expires_at          INTEGER NOT NULL CHECK (expires_at > valid_from),
                revoked_at          INTEGER,
                binding_epoch       INTEGER NOT NULL CHECK (binding_epoch > 0),
                binding_digest      TEXT NOT NULL CHECK (length(binding_digest) = 64),
                recorded_at         TEXT NOT NULL,
                CHECK (revoked_at IS NULL OR revoked_at >= valid_from),
                UNIQUE(actor_id, binding_epoch)
            );
            CREATE INDEX IF NOT EXISTS idx_actor_profile_bindings_resolve
                ON actor_profile_bindings(actor_id, valid_from, expires_at, revoked_at);

            CREATE TABLE IF NOT EXISTS profile_routing_receipts (
                receipt_id                    TEXT PRIMARY KEY,
                requester_actor_id            TEXT NOT NULL REFERENCES actors(actor_id),
                subject_profile_id            TEXT NOT NULL REFERENCES memory_profiles(profile_id),
                authorization_snapshot_digest TEXT NOT NULL CHECK (length(authorization_snapshot_digest) = 64),
                receipt_json                  TEXT NOT NULL CHECK (json_valid(receipt_json)),
                receipt_digest                TEXT NOT NULL CHECK (length(receipt_digest) = 64),
                recorded_at                   TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_profile_routing_receipts_actor
                ON profile_routing_receipts(requester_actor_id, recorded_at DESC);

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
                ON shard_routing_receipts(requester_device_id, recorded_at DESC);
            CREATE TABLE IF NOT EXISTS fact_create_admissions (
                device_id TEXT NOT NULL REFERENCES devices(device_id),
                store_id TEXT NOT NULL,
                namespace TEXT NOT NULL,
                principal_id TEXT NOT NULL,
                key_version INTEGER NOT NULL CHECK(key_version > 0),
                public_key BLOB NOT NULL CHECK(length(public_key)=32),
                activated_at INTEGER NOT NULL CHECK(activated_at >= 0),
                cutoff_at INTEGER NOT NULL CHECK(cutoff_at >= activated_at),
                revoked INTEGER NOT NULL DEFAULT 0 CHECK(revoked IN (0, 1)),
                stream_epoch INTEGER NOT NULL CHECK(stream_epoch > 0),
                fencing_token TEXT NOT NULL,
                PRIMARY KEY(device_id, store_id, namespace, principal_id, key_version)
            );
            CREATE INDEX IF NOT EXISTS idx_fact_create_admission_scope
                ON fact_create_admissions(device_id, store_id, namespace);
            CREATE TABLE IF NOT EXISTS fact_create_acks (
                batch_id TEXT PRIMARY KEY,
                request_digest TEXT NOT NULL,
                home_device_id TEXT NOT NULL,
                store_id TEXT NOT NULL,
                stream_epoch INTEGER NOT NULL,
                accepted_head INTEGER NOT NULL,
                disposition TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS fact_supersede_admissions (
                device_id TEXT NOT NULL REFERENCES devices(device_id),
                store_id TEXT NOT NULL,
                replacement_namespace TEXT NOT NULL,
                principal_id TEXT NOT NULL,
                key_version INTEGER NOT NULL CHECK(key_version > 0),
                public_key BLOB NOT NULL CHECK(length(public_key)=32),
                activated_at INTEGER NOT NULL CHECK(activated_at >= 0),
                cutoff_at INTEGER NOT NULL CHECK(cutoff_at >= activated_at),
                revoked INTEGER NOT NULL DEFAULT 0 CHECK(revoked IN (0, 1)),
                store_epoch INTEGER NOT NULL CHECK(store_epoch > 0),
                writer_epoch INTEGER NOT NULL CHECK(writer_epoch > 0),
                fencing_token TEXT NOT NULL,
                PRIMARY KEY(device_id, store_id, replacement_namespace, principal_id, key_version)
            );
            CREATE INDEX IF NOT EXISTS idx_fact_supersede_admission_scope
                ON fact_supersede_admissions(device_id, store_id, replacement_namespace);
            CREATE TABLE IF NOT EXISTS fact_supersede_acks (
                batch_id TEXT PRIMARY KEY,
                request_digest TEXT NOT NULL,
                home_device_id TEXT NOT NULL,
                store_id TEXT NOT NULL,
                owner_stream_epoch INTEGER NOT NULL,
                accepted_head INTEGER NOT NULL,
                disposition TEXT NOT NULL
            );",
        )?;
        for (table, columns) in [
            (
                "fact_create_admissions",
                &[
                    "device_id",
                    "store_id",
                    "namespace",
                    "principal_id",
                    "key_version",
                    "public_key",
                    "activated_at",
                    "cutoff_at",
                    "revoked",
                    "stream_epoch",
                    "fencing_token",
                ] as &[&str],
            ),
            (
                "fact_create_acks",
                &[
                    "batch_id",
                    "request_digest",
                    "home_device_id",
                    "store_id",
                    "stream_epoch",
                    "accepted_head",
                    "disposition",
                ],
            ),
            (
                "fact_supersede_admissions",
                &[
                    "device_id",
                    "store_id",
                    "replacement_namespace",
                    "principal_id",
                    "key_version",
                    "public_key",
                    "activated_at",
                    "cutoff_at",
                    "revoked",
                    "store_epoch",
                    "writer_epoch",
                    "fencing_token",
                ],
            ),
            (
                "fact_supersede_acks",
                &[
                    "batch_id",
                    "request_digest",
                    "home_device_id",
                    "store_id",
                    "owner_stream_epoch",
                    "accepted_head",
                    "disposition",
                ],
            ),
        ] {
            for column in columns {
                if !Self::has_table_column(conn, table, column)? {
                    return Err(MnemesError::InvalidShardCatalog(format!(
                        "{table} is missing required column {column}"
                    )));
                }
            }
        }

        conn.execute("DELETE FROM _pooled_schema_version", [])?;
        conn.execute(
            "INSERT INTO _pooled_schema_version(version, applied_at) VALUES (?1, datetime('now'))",
            [POOLED_SCHEMA_GENERATION],
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

    pub(crate) async fn token_device_id(
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
            let actor = actor
                .as_ref()
                .ok_or_else(|| MnemesError::AuthorizationDenied("actor required".to_string()))?;
            if !actor.tool_profile.is_full() {
                return Err(MnemesError::AuthorizationDenied(
                    "actor lacks operator profile".to_string(),
                ));
            }
        }

        Ok(())
    }

    // ─── Profile/store/grant registry ─────────────────────────────────

    /// Register one profile owned by an active device.
    pub async fn register_memory_profile(
        &self,
        mut profile: MemoryProfile,
    ) -> Result<MemoryProfileId, MnemesError> {
        profile.validate()?;
        if profile.status != MemoryProfileStatus::Active {
            return Err(MnemesError::InvalidMemoryScope(
                "new memory profiles must be active".to_string(),
            ));
        }
        let now = Utc::now().to_rfc3339();
        let conn = self.pool_conn.lock().await;
        let active: Option<String> = conn
            .query_row(
                "SELECT status FROM devices WHERE device_id = ?1",
                params![profile.owner_device_id.as_str()],
                |row| row.get(0),
            )
            .optional()?;
        if active.as_deref() != Some(DeviceStatus::Active.as_str()) {
            return Err(MnemesError::DeviceNotActive(
                profile.owner_device_id.to_string(),
            ));
        }
        profile.created_at = now;
        conn.execute(
            "INSERT INTO memory_profiles(profile_id, owner_device_id, label, status, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                profile.profile_id.as_str(),
                profile.owner_device_id.as_str(),
                profile.label,
                profile.status.as_str(),
                profile.created_at,
            ],
        )?;
        Ok(profile.profile_id)
    }

    /// Register one independently addressable store for an active profile.
    pub async fn register_memory_store(
        &self,
        mut store: MemoryStoreIdentity,
    ) -> Result<String, MnemesError> {
        store.validate()?;
        if store.status != MemoryStoreStatus::Active {
            return Err(MnemesError::InvalidMemoryScope(
                "new memory stores must be active".to_string(),
            ));
        }
        let now = Utc::now().to_rfc3339();
        let conn = self.pool_conn.lock().await;
        let owner: Option<(String, String)> = conn
            .query_row(
                "SELECT p.owner_device_id, d.status
                 FROM memory_profiles p
                 JOIN devices d ON d.device_id = p.owner_device_id
                 WHERE p.profile_id = ?1 AND p.status = 'active'",
                params![store.profile_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((owner_device, device_status)) = owner else {
            return Err(MnemesError::MemoryGrantDenied(
                "store profile is not active or does not exist".to_string(),
            ));
        };
        if device_status != DeviceStatus::Active.as_str() {
            return Err(MnemesError::DeviceNotActive(owner_device));
        }
        if owner_device != store.owner_device_id.as_str() {
            return Err(MnemesError::MemoryGrantDenied(
                "store owner device does not match the active profile owner".to_string(),
            ));
        }
        store.created_at = now;
        conn.execute(
            "INSERT INTO memory_stores(
                store_id, profile_id, owner_device_id, namespace, relative_path, status, created_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                store.store_id,
                store.profile_id.as_str(),
                store.owner_device_id.as_str(),
                store.namespace,
                store.relative_path,
                store.status.as_str(),
                store.created_at,
            ],
        )?;
        Ok(store.store_id)
    }

    /// Issue a grant through an operator actor; HTTP callers cannot self-grant.
    pub async fn grant_memory_access(
        &self,
        mut grant: MemoryAccessGrant,
    ) -> Result<MemoryGrantId, MnemesError> {
        grant.validate()?;
        let now = Utc::now().to_rfc3339();
        let conn = self.pool_conn.lock().await;
        let issuer_is_operator: Option<bool> = conn
            .query_row(
                "SELECT a.tool_profile = 'operator' AND d.status = 'active'
                 FROM actors a JOIN devices d ON d.device_id = a.device_id
                 WHERE a.actor_id = ?1",
                params![grant.issued_by_actor_id.as_str()],
                |row| row.get(0),
            )
            .optional()?;
        if issuer_is_operator != Some(true) {
            return Err(MnemesError::AuthorizationDenied(
                "memory grants require an active operator actor".to_string(),
            ));
        }
        let target_store: Option<(String, String, String, String)> = conn
            .query_row(
                "SELECT s.namespace, s.owner_device_id, p.status, d.status
                 FROM memory_stores s
                 JOIN memory_profiles p ON p.profile_id = s.profile_id
                 JOIN devices d ON d.device_id = s.owner_device_id
                 WHERE s.store_id = ?1 AND s.status = 'active'",
                params![grant.store_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        let Some((store_namespace, owner_device_id, owner_profile_status, owner_device_status)) =
            target_store
        else {
            return Err(MnemesError::MemoryGrantDenied(
                "grant target store is not active or does not exist".to_string(),
            ));
        };
        if store_namespace != grant.namespace {
            return Err(MnemesError::MemoryGrantDenied(
                "grant namespace does not match the target store".to_string(),
            ));
        }
        if owner_profile_status != MemoryProfileStatus::Active.as_str() {
            return Err(MnemesError::MemoryGrantDenied(
                "grant target profile is not active".to_string(),
            ));
        }
        if owner_device_status != DeviceStatus::Active.as_str() {
            return Err(MnemesError::DeviceNotActive(owner_device_id));
        }
        let grantee_owner: Option<(String, String)> = conn
            .query_row(
                "SELECT p.status, d.status
                 FROM memory_profiles p
                 JOIN devices d ON d.device_id = p.owner_device_id
                 WHERE p.profile_id = ?1",
                params![grant.grantee_profile_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((grantee_status, grantee_device_status)) = grantee_owner else {
            return Err(MnemesError::MemoryGrantDenied(
                "grant grantee profile does not exist".to_string(),
            ));
        };
        if grantee_status != MemoryProfileStatus::Active.as_str() {
            return Err(MnemesError::MemoryGrantDenied(
                "grant grantee profile is not active".to_string(),
            ));
        }
        if grantee_device_status != DeviceStatus::Active.as_str() {
            return Err(MnemesError::MemoryGrantDenied(
                "grant grantee device is not active".to_string(),
            ));
        }
        grant.created_at = now;
        conn.execute(
            "INSERT INTO memory_access_grants(
                grant_id, grantee_profile_id, store_id, namespace, effect,
                issued_by_actor_id, valid_from, expires_at, revoked_at, created_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                grant.grant_id.as_str(),
                grant.grantee_profile_id.as_str(),
                grant.store_id,
                grant.namespace,
                grant.effect.as_str(),
                grant.issued_by_actor_id.as_str(),
                grant.valid_from as i64,
                grant.expires_at as i64,
                grant.revoked_at.map(|value| value as i64),
                grant.created_at,
            ],
        )?;
        Ok(grant.grant_id)
    }

    /// Revoke one grant without deleting its append-only control-plane record.
    pub async fn revoke_memory_access(
        &self,
        grant_id: &MemoryGrantId,
        revoked_at: u64,
    ) -> Result<(), MnemesError> {
        let conn = self.pool_conn.lock().await;
        let affected = conn.execute(
            "UPDATE memory_access_grants SET revoked_at = ?1
             WHERE grant_id = ?2 AND revoked_at IS NULL",
            params![revoked_at as i64, grant_id.as_str()],
        )?;
        if affected == 0 {
            return Err(MnemesError::MemoryGrantDenied(
                "grant does not exist or is already revoked".to_string(),
            ));
        }
        Ok(())
    }

    /// Authorize one exact profile/store/namespace/effect request at a timestamp.
    pub async fn authorize_memory_access(
        &self,
        requester_profile_id: &MemoryProfileId,
        store_id: &str,
        namespace: &str,
        effect: MemoryAccessEffect,
        at: u64,
    ) -> Result<(), MnemesError> {
        let conn = self.pool_conn.lock().await;
        let profile = conn
            .query_row(
                "SELECT profile_id, owner_device_id, label, status, created_at
                 FROM memory_profiles WHERE profile_id = ?1",
                params![requester_profile_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| {
                MnemesError::MemoryGrantDenied("requesting profile not found".to_string())
            })?;
        let profile = MemoryProfile {
            profile_id: MemoryProfileId::parse(profile.0)?,
            owner_device_id: DeviceId::parse(profile.1)?,
            label: profile.2,
            status: MemoryProfileStatus::parse(&profile.3)?,
            created_at: profile.4,
        };
        let requester_device_status: String = conn.query_row(
            "SELECT status FROM devices WHERE device_id = ?1",
            params![profile.owner_device_id.as_str()],
            |row| row.get(0),
        )?;
        if requester_device_status != DeviceStatus::Active.as_str() {
            return Err(MnemesError::DeviceNotActive(
                profile.owner_device_id.to_string(),
            ));
        }
        let store_row = conn
            .query_row(
                "SELECT store_id, profile_id, owner_device_id, namespace, relative_path, status, created_at
                 FROM memory_stores WHERE store_id = ?1",
                params![store_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| MnemesError::MemoryGrantDenied("target store not found".to_string()))?;
        let mut store = MemoryStoreIdentity::new(
            store_row.0,
            MemoryProfileId::parse(store_row.1)?,
            DeviceId::parse(store_row.2)?,
            store_row.3,
            store_row.4,
        )?;
        store.status = MemoryStoreStatus::parse(&store_row.5)?;
        store.created_at = store_row.6;
        let owner_device_status: String = conn.query_row(
            "SELECT status FROM devices WHERE device_id = ?1",
            params![store.owner_device_id.as_str()],
            |row| row.get(0),
        )?;
        if owner_device_status != DeviceStatus::Active.as_str() {
            return Err(MnemesError::DeviceNotActive(
                store.owner_device_id.to_string(),
            ));
        }
        let owner_profile_status: String = conn.query_row(
            "SELECT status FROM memory_profiles WHERE profile_id = ?1",
            params![store.profile_id.as_str()],
            |row| row.get(0),
        )?;
        if owner_profile_status != MemoryProfileStatus::Active.as_str() {
            return Err(MnemesError::MemoryGrantDenied(
                "target store owner profile is not active".to_string(),
            ));
        }
        let mut statement = conn.prepare(
            "SELECT grant_id, grantee_profile_id, store_id, namespace, effect,
                    issued_by_actor_id, valid_from, expires_at, revoked_at, created_at
             FROM memory_access_grants
             WHERE grantee_profile_id = ?1 AND store_id = ?2
             ORDER BY valid_from ASC, grant_id ASC",
        )?;
        let rows =
            statement.query_map(params![requester_profile_id.as_str(), store_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, Option<i64>>(8)?,
                    row.get::<_, String>(9)?,
                ))
            })?;
        let mut grants = Vec::new();
        for row in rows {
            let (
                grant_id,
                grantee_profile_id,
                grant_store_id,
                grant_namespace,
                grant_effect,
                issued_by_actor_id,
                valid_from,
                expires_at,
                revoked_at,
                created_at,
            ) = row?;
            grants.push(MemoryAccessGrant {
                grant_id: MemoryGrantId::parse(grant_id)?,
                grantee_profile_id: MemoryProfileId::parse(grantee_profile_id)?,
                store_id: grant_store_id,
                namespace: grant_namespace,
                effect: MemoryAccessEffect::parse(&grant_effect)?,
                issued_by_actor_id: ActorId::parse(issued_by_actor_id)?,
                valid_from: u64::try_from(valid_from).map_err(|_| {
                    MnemesError::InvalidMemoryScope("negative grant valid_from".to_string())
                })?,
                expires_at: u64::try_from(expires_at).map_err(|_| {
                    MnemesError::InvalidMemoryScope("negative grant expires_at".to_string())
                })?,
                revoked_at: revoked_at
                    .map(|value| {
                        u64::try_from(value).map_err(|_| {
                            MnemesError::InvalidMemoryScope("negative grant revoked_at".to_string())
                        })
                    })
                    .transpose()?,
                created_at,
            });
        }
        authorize_memory_access(
            requester_profile_id,
            &profile,
            &store,
            &grants,
            effect,
            namespace,
            at,
        )
    }

    /// Persist one operator-issued actor-to-profile subject transition.
    pub async fn bind_actor_profile(
        &self,
        mut binding: ActorProfileBinding,
    ) -> Result<ActorProfileBindingId, MnemesError> {
        binding.validate()?;
        let valid_from = i64::try_from(binding.valid_from).map_err(|_| {
            MnemesError::ActorProfileBindingDenied(
                "binding valid_from exceeds SQLite range".to_string(),
            )
        })?;
        let expires_at = i64::try_from(binding.expires_at).map_err(|_| {
            MnemesError::ActorProfileBindingDenied(
                "binding expires_at exceeds SQLite range".to_string(),
            )
        })?;
        let binding_epoch = i64::try_from(binding.binding_epoch).map_err(|_| {
            MnemesError::ActorProfileBindingDenied("binding epoch exceeds SQLite range".to_string())
        })?;
        let now = Utc::now().to_rfc3339();
        let conn = self.pool_conn.lock().await;
        let issuer_is_operator: Option<bool> = conn
            .query_row(
                "SELECT a.tool_profile = 'operator' AND d.status = 'active'
                 FROM actors a JOIN devices d ON d.device_id = a.device_id
                 WHERE a.actor_id = ?1",
                params![binding.issued_by_actor_id.as_str()],
                |row| row.get(0),
            )
            .optional()?;
        if issuer_is_operator != Some(true) {
            return Err(MnemesError::ActorProfileBindingDenied(
                "binding transitions require an active operator actor".to_string(),
            ));
        }
        let actor_device: Option<(String, String)> = conn
            .query_row(
                "SELECT a.device_id, d.status FROM actors a
                 JOIN devices d ON d.device_id = a.device_id
                 WHERE a.actor_id = ?1",
                params![binding.actor_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((actor_device_id, actor_device_status)) = actor_device else {
            return Err(MnemesError::ActorProfileBindingDenied(
                "binding actor does not exist".to_string(),
            ));
        };
        if actor_device_status != DeviceStatus::Active.as_str()
            || actor_device_id != binding.owner_device_id.as_str()
        {
            return Err(MnemesError::ActorProfileBindingDenied(
                "binding actor must belong to the active profile owner device".to_string(),
            ));
        }
        let profile_owner: Option<(String, String)> = conn
            .query_row(
                "SELECT p.owner_device_id, d.status FROM memory_profiles p
                 JOIN devices d ON d.device_id = p.owner_device_id
                 WHERE p.profile_id = ?1 AND p.status = 'active'",
                params![binding.profile_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((profile_owner_device, profile_owner_status)) = profile_owner else {
            return Err(MnemesError::ActorProfileBindingDenied(
                "binding profile is not active or does not exist".to_string(),
            ));
        };
        if profile_owner_status != DeviceStatus::Active.as_str()
            || profile_owner_device != binding.owner_device_id.as_str()
        {
            return Err(MnemesError::ActorProfileBindingDenied(
                "binding owner device must equal the active profile owner".to_string(),
            ));
        }
        let overlap_exists: bool = conn.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM actor_profile_bindings
                 WHERE actor_id = ?1 AND revoked_at IS NULL
                   AND valid_from < ?2 AND expires_at > ?3
             )",
            params![binding.actor_id.as_str(), expires_at, valid_from],
            |row| row.get(0),
        )?;
        if overlap_exists {
            return Err(MnemesError::ActorProfileBindingDenied(
                "an overlapping active binding already exists for this actor".to_string(),
            ));
        }
        binding.recorded_at = now;
        conn.execute(
            "INSERT INTO actor_profile_bindings(
                binding_id, actor_id, profile_id, owner_device_id, issued_by_actor_id,
                valid_from, expires_at, revoked_at, binding_epoch, binding_digest, recorded_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, ?8, ?9, ?10)",
            params![
                binding.binding_id.as_str(),
                binding.actor_id.as_str(),
                binding.profile_id.as_str(),
                binding.owner_device_id.as_str(),
                binding.issued_by_actor_id.as_str(),
                valid_from,
                expires_at,
                binding_epoch,
                binding.binding_digest,
                binding.recorded_at,
            ],
        )?;
        Ok(binding.binding_id)
    }

    /// Revoke one binding without deleting its issuance record.
    pub async fn revoke_actor_profile_binding(
        &self,
        binding_id: &ActorProfileBindingId,
        revoked_at: u64,
    ) -> Result<(), MnemesError> {
        let revoked_at = i64::try_from(revoked_at).map_err(|_| {
            MnemesError::ActorProfileBindingDenied(
                "binding revoked_at exceeds SQLite range".to_string(),
            )
        })?;
        let conn = self.pool_conn.lock().await;
        let affected = conn.execute(
            "UPDATE actor_profile_bindings SET revoked_at = ?1
             WHERE binding_id = ?2 AND revoked_at IS NULL",
            params![revoked_at, binding_id.as_str()],
        )?;
        if affected == 0 {
            return Err(MnemesError::ActorProfileBindingDenied(
                "binding does not exist or is already revoked".to_string(),
            ));
        }
        Ok(())
    }

    /// Resolve exactly one currently valid memory-profile subject for an actor.
    pub async fn resolve_actor_profile(
        &self,
        actor_id: &ActorId,
        at: u64,
    ) -> Result<ActorProfileBinding, MnemesError> {
        let at = i64::try_from(at).map_err(|_| {
            MnemesError::ActorProfileBindingDenied(
                "authorization time exceeds SQLite range".to_string(),
            )
        })?;
        let conn = self.pool_conn.lock().await;
        let mut statement = conn.prepare(
            "SELECT b.binding_id, b.actor_id, b.profile_id, b.owner_device_id,
                    b.issued_by_actor_id, b.valid_from, b.expires_at, b.revoked_at,
                    b.binding_epoch, b.binding_digest, b.recorded_at,
                    a.device_id, ad.status, p.owner_device_id, p.status, pd.status
             FROM actor_profile_bindings b
             JOIN actors a ON a.actor_id = b.actor_id
             JOIN devices ad ON ad.device_id = a.device_id
             JOIN memory_profiles p ON p.profile_id = b.profile_id
             JOIN devices pd ON pd.device_id = p.owner_device_id
             WHERE b.actor_id = ?1 AND b.revoked_at IS NULL
               AND b.valid_from <= ?2 AND b.expires_at > ?2
             ORDER BY b.binding_epoch ASC, b.binding_id ASC",
        )?;
        let rows = statement
            .query_map(params![actor_id.as_str(), at], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, Option<i64>>(7)?,
                    row.get::<_, i64>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, String>(10)?,
                    row.get::<_, String>(11)?,
                    row.get::<_, String>(12)?,
                    row.get::<_, String>(13)?,
                    row.get::<_, String>(14)?,
                    row.get::<_, String>(15)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        if rows.len() != 1 {
            return Err(MnemesError::ActorProfileBindingDenied(if rows.is_empty() {
                "no active binding exists for this actor".to_string()
            } else {
                "multiple active bindings exist for this actor".to_string()
            }));
        }
        let row = rows.into_iter().next().expect("exactly one checked above");
        let binding = ActorProfileBinding {
            binding_id: ActorProfileBindingId::parse(row.0)?,
            actor_id: ActorId::parse(row.1)?,
            profile_id: MemoryProfileId::parse(row.2)?,
            owner_device_id: DeviceId::parse(row.3)?,
            issued_by_actor_id: ActorId::parse(row.4)?,
            valid_from: u64::try_from(row.5).map_err(|_| {
                MnemesError::ActorProfileBindingDenied("negative binding valid_from".to_string())
            })?,
            expires_at: u64::try_from(row.6).map_err(|_| {
                MnemesError::ActorProfileBindingDenied("negative binding expires_at".to_string())
            })?,
            revoked_at: row
                .7
                .map(|value| {
                    u64::try_from(value).map_err(|_| {
                        MnemesError::ActorProfileBindingDenied(
                            "negative binding revoked_at".to_string(),
                        )
                    })
                })
                .transpose()?,
            binding_epoch: u64::try_from(row.8).map_err(|_| {
                MnemesError::ActorProfileBindingDenied("negative binding epoch".to_string())
            })?,
            binding_digest: row.9,
            recorded_at: row.10,
        };
        binding.validate()?;
        if row.11 != binding.owner_device_id.as_str()
            || row.13 != binding.owner_device_id.as_str()
            || row.12 != DeviceStatus::Active.as_str()
            || row.14 != MemoryProfileStatus::Active.as_str()
            || row.15 != DeviceStatus::Active.as_str()
        {
            return Err(MnemesError::ActorProfileBindingDenied(
                "binding actor, profile, and owner-device lifecycle relation is invalid"
                    .to_string(),
            ));
        }
        Ok(binding)
    }

    /// Derive a request-local snapshot before any store ranking or opening.
    pub async fn build_authorization_snapshot(
        &self,
        actor_id: &ActorId,
        effect: MemoryAccessEffect,
        namespaces: Option<&[String]>,
        at: u64,
    ) -> Result<AuthorizationSnapshot, MnemesError> {
        let binding = self.resolve_actor_profile(actor_id, at).await?;
        let requested_namespaces = namespaces.map_or_else(Vec::new, ToOwned::to_owned);
        let profile = MemoryProfile {
            profile_id: binding.profile_id.clone(),
            owner_device_id: binding.owner_device_id.clone(),
            label: "authorization subject".to_string(),
            status: MemoryProfileStatus::Active,
            created_at: String::new(),
        };
        let stores = {
            let conn = self.pool_conn.lock().await;
            let mut statement = conn.prepare(
                "SELECT s.store_id, s.profile_id, s.owner_device_id, s.namespace, s.relative_path,
                        s.status, s.created_at
                 FROM memory_stores s
                 JOIN memory_profiles p ON p.profile_id = s.profile_id
                 JOIN devices d ON d.device_id = s.owner_device_id
                 WHERE s.status = 'active' AND p.status = 'active' AND d.status = 'active'
                 ORDER BY s.store_id ASC",
            )?;
            let rows = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            rows
        };
        let mut authorized_stores = Vec::new();
        for row in stores {
            let store = MemoryStoreIdentity {
                store_id: row.0,
                profile_id: MemoryProfileId::parse(row.1)?,
                owner_device_id: DeviceId::parse(row.2)?,
                namespace: row.3,
                relative_path: row.4,
                status: MemoryStoreStatus::parse(&row.5)?,
                created_at: row.6,
            };
            if !requested_namespaces.is_empty()
                && !requested_namespaces
                    .iter()
                    .any(|namespace| namespace == &store.namespace)
            {
                continue;
            }
            let grants = self
                .memory_grants_for_profile_store(&binding.profile_id, &store.store_id)
                .await?;
            if authorize_memory_access(
                &binding.profile_id,
                &profile,
                &store,
                &grants,
                effect,
                &store.namespace,
                at,
            )
            .is_err()
            {
                continue;
            }
            let mut grant_ids = grants
                .iter()
                .filter(|grant| grant.allows(effect, &store.namespace, at))
                .map(|grant| grant.grant_id.clone())
                .collect::<Vec<_>>();
            grant_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
            let authorization_expires_at = grants
                .iter()
                .filter(|grant| grant.allows(effect, &store.namespace, at))
                .map(|grant| grant.expires_at)
                .min()
                .unwrap_or(binding.expires_at)
                .min(binding.expires_at);
            authorized_stores.push(AuthorizedMemoryStore {
                store_id: store.store_id,
                profile_id: store.profile_id,
                owner_device_id: store.owner_device_id,
                namespace: store.namespace,
                relative_path: store.relative_path,
                grant_ids,
                authorization_expires_at,
            });
        }
        Ok(AuthorizationSnapshot::new(
            &binding,
            effect,
            requested_namespaces,
            authorized_stores,
            at,
        ))
    }

    /// Validate a snapshot against current control-plane state before use.
    pub async fn validate_authorization_snapshot(
        &self,
        snapshot: &AuthorizationSnapshot,
        at: u64,
    ) -> Result<(), MnemesError> {
        snapshot.validate()?;
        if snapshot.evaluated_at != at {
            return Err(MnemesError::AuthorizationSnapshotInvalid(
                "snapshot evaluation time does not match request authorization time".to_string(),
            ));
        }
        let namespaces =
            (!snapshot.namespaces.is_empty()).then_some(snapshot.namespaces.as_slice());
        let current = self
            .build_authorization_snapshot(&snapshot.actor_id, snapshot.effect, namespaces, at)
            .await?;
        if current.snapshot_digest != snapshot.snapshot_digest {
            return Err(MnemesError::AuthorizationSnapshotInvalid(
                "snapshot no longer matches current binding, lifecycle, or grants".to_string(),
            ));
        }
        Ok(())
    }

    /// Return only stores admitted by a current validated snapshot.
    pub async fn list_authorized_stores(
        &self,
        snapshot: &AuthorizationSnapshot,
        at: u64,
    ) -> Result<Vec<AuthorizedMemoryStore>, MnemesError> {
        self.validate_authorization_snapshot(snapshot, at).await?;
        Ok(snapshot.authorized_stores.clone())
    }

    /// Issue a bounded request permit from a current snapshot; it is not durable authority.
    pub async fn issue_memory_access_permit(
        &self,
        snapshot: &AuthorizationSnapshot,
        store_id: &str,
        namespace: &str,
        at: u64,
        query_budget: usize,
    ) -> Result<MemoryAccessPermit, MnemesError> {
        self.validate_authorization_snapshot(snapshot, at).await?;
        let store = snapshot
            .authorized_stores
            .iter()
            .find(|store| store.store_id == store_id && store.namespace == namespace)
            .ok_or_else(|| {
                MnemesError::MemoryGrantDenied(
                    "store is not admitted by the authorization snapshot".to_string(),
                )
            })?;
        let expires_at = store.authorization_expires_at.min(at.saturating_add(60));
        MemoryAccessPermit::new(snapshot, store, at, expires_at, query_budget)
    }

    async fn memory_grants_for_profile_store(
        &self,
        requester_profile_id: &MemoryProfileId,
        store_id: &str,
    ) -> Result<Vec<MemoryAccessGrant>, MnemesError> {
        let conn = self.pool_conn.lock().await;
        let mut statement = conn.prepare(
            "SELECT grant_id, grantee_profile_id, store_id, namespace, effect,
                    issued_by_actor_id, valid_from, expires_at, revoked_at, created_at
             FROM memory_access_grants
             WHERE grantee_profile_id = ?1 AND store_id = ?2
             ORDER BY valid_from ASC, grant_id ASC",
        )?;
        let rows = statement
            .query_map(params![requester_profile_id.as_str(), store_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, Option<i64>>(8)?,
                    row.get::<_, String>(9)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(|row| {
                Ok(MemoryAccessGrant {
                    grant_id: MemoryGrantId::parse(row.0)?,
                    grantee_profile_id: MemoryProfileId::parse(row.1)?,
                    store_id: row.2,
                    namespace: row.3,
                    effect: MemoryAccessEffect::parse(&row.4)?,
                    issued_by_actor_id: ActorId::parse(row.5)?,
                    valid_from: u64::try_from(row.6).map_err(|_| {
                        MnemesError::InvalidMemoryScope("negative grant valid_from".to_string())
                    })?,
                    expires_at: u64::try_from(row.7).map_err(|_| {
                        MnemesError::InvalidMemoryScope("negative grant expires_at".to_string())
                    })?,
                    revoked_at: row
                        .8
                        .map(|value| {
                            u64::try_from(value).map_err(|_| {
                                MnemesError::InvalidMemoryScope(
                                    "negative grant revoked_at".to_string(),
                                )
                            })
                        })
                        .transpose()?,
                    created_at: row.9,
                })
            })
            .collect()
    }

    /// Change a profile lifecycle state without deleting its history.
    pub async fn set_memory_profile_status(
        &self,
        profile_id: &MemoryProfileId,
        status: MemoryProfileStatus,
    ) -> Result<(), MnemesError> {
        let conn = self.pool_conn.lock().await;
        let affected = conn.execute(
            "UPDATE memory_profiles SET status = ?1 WHERE profile_id = ?2",
            params![status.as_str(), profile_id.as_str()],
        )?;
        if affected == 0 {
            return Err(MnemesError::MemoryGrantDenied(
                "memory profile does not exist".to_string(),
            ));
        }
        Ok(())
    }

    /// Change a store lifecycle state without deleting its history.
    pub async fn set_memory_store_status(
        &self,
        store_id: &str,
        status: MemoryStoreStatus,
    ) -> Result<(), MnemesError> {
        let conn = self.pool_conn.lock().await;
        let affected = conn.execute(
            "UPDATE memory_stores SET status = ?1 WHERE store_id = ?2",
            params![status.as_str(), store_id],
        )?;
        if affected == 0 {
            return Err(MnemesError::MemoryGrantDenied(
                "memory store does not exist".to_string(),
            ));
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

    pub async fn get_device(&self, device_id: &DeviceId) -> Result<Option<Device>, MnemesError> {
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

    pub async fn list_devices_for_actor(&self, actor: &Actor) -> Result<Vec<Device>, MnemesError> {
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

    /// Embedding configuration used for newly opened shard stores.
    pub fn memory_config(&self) -> &semantic_memory::MemoryConfig {
        &self.memory_config
    }

    /// Return whether the legacy accessor has already been initialized.
    /// This is observational and never opens or creates the legacy store.
    pub fn has_legacy_memory(&self) -> bool {
        self.legacy_memory.get().is_some()
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
            semantic_memory::MemoryStore::open_with_embedder(
                config,
                Box::new(SharedEmbedder {
                    inner: self.embedder.clone(),
                }),
            )
            .unwrap_or_else(|e| panic!("legacy memory store: {e:?}"))
        })
    }

    /// Local bootstrap-only admission API; no HTTP route exposes this.
    pub async fn admit_fact_create_key(
        &self,
        admission: FactCreateAdmission,
    ) -> Result<(), MnemesError> {
        let _gate = self.fact_create_gate.lock().await;
        if admission.store_id.is_empty()
            || admission.namespace.is_empty()
            || admission.principal_id.is_empty()
            || admission.fencing_token.is_empty()
            || admission.key_version == 0
            || admission.stream_epoch == 0
            || admission.activated_at > admission.cutoff_at
        {
            return Err(MnemesError::FactCreateRejected(
                "invalid fact-create admission record".into(),
            ));
        }
        let conn = self.pool_conn.lock().await;
        conn.execute(
            "INSERT INTO fact_create_admissions \
             (device_id,store_id,namespace,principal_id,key_version,public_key,activated_at,cutoff_at,revoked,stream_epoch,fencing_token) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,0,?9,?10) \
             ON CONFLICT(device_id,store_id,namespace,principal_id,key_version) DO UPDATE SET \
             public_key=excluded.public_key,activated_at=excluded.activated_at,cutoff_at=excluded.cutoff_at,revoked=0,stream_epoch=excluded.stream_epoch,fencing_token=excluded.fencing_token",
            rusqlite::params![
                admission.device_id.as_str(),
                admission.store_id,
                admission.namespace,
                admission.principal_id,
                i64::try_from(admission.key_version).map_err(|_| MnemesError::FactCreateRejected("key version does not fit SQLite INTEGER".into()))?,
                admission.public_key.as_slice(),
                i64::try_from(admission.activated_at).map_err(|_| MnemesError::FactCreateRejected("activation time does not fit SQLite INTEGER".into()))?,
                i64::try_from(admission.cutoff_at).map_err(|_| MnemesError::FactCreateRejected("cutoff time does not fit SQLite INTEGER".into()))?,
                i64::try_from(admission.stream_epoch).map_err(|_| MnemesError::FactCreateRejected("stream epoch does not fit SQLite INTEGER".into()))?,
                admission.fencing_token,
            ],
        )?;
        Ok(())
    }

    pub async fn revoke_fact_create_key(
        &self,
        device_id: &DeviceId,
        store_id: &str,
        namespace: &str,
        principal_id: &str,
        key_version: u64,
    ) -> Result<(), MnemesError> {
        let _gate = self.fact_create_gate.lock().await;
        let conn = self.pool_conn.lock().await;
        conn.execute(
            "UPDATE fact_create_admissions SET revoked=1 WHERE device_id=?1 AND store_id=?2 AND namespace=?3 AND principal_id=?4 AND key_version=?5",
            rusqlite::params![device_id.as_str(), store_id, namespace, principal_id, key_version as i64],
        )?;
        Ok(())
    }

    /// Atomically admit, apply, and acknowledge one V1 fact-create request.
    ///
    /// The gate serializes mutations within this process only. The Mnemes
    /// control database must not be written by any other process; it does not
    /// provide cross-process serialization with semantic-memory's database.
    pub async fn apply_fact_create_request(
        &self,
        batch: &SignedFactCreateBatchV1,
        request_digest: String,
    ) -> Result<FactCreateAckRecord, MnemesError> {
        let _gate = self.fact_create_gate.lock().await;
        let existing = self.get_fact_create_ack_locked(&batch.batch_id).await?;
        if let Some(existing) = existing {
            if existing.request_digest != request_digest {
                return Err(MnemesError::FactCreateRejected(
                    "batch id digest collision".into(),
                ));
            }
            return Ok(existing);
        }
        if batch.entries.len() != 1 {
            return Err(MnemesError::FactCreateRejected(
                "V1 accepts exactly one entry".into(),
            ));
        }
        let envelopes = batch
            .semantic_envelopes()
            .map_err(|e| MnemesError::Replication(e.to_string()))?;
        let payload =
            semantic_memory::journal::validate_fact_create_replica_envelope(&envelopes[0])
                .map_err(|e| MnemesError::FactCreateRejected(e.to_string()))?;
        self.check_fact_create_admission_locked(batch, &payload.namespace)
            .await?;
        let memory = self
            .device_memory(&DeviceId::parse(&batch.home_device_id)?)
            .await?;
        let decision = memory
            .apply_verified_fact_create(envelopes[0].clone())
            .await?;
        let accepted_head = match decision {
            semantic_memory::journal::ReplicaApplyOutcome::Applied { sequence, .. }
            | semantic_memory::journal::ReplicaApplyOutcome::Duplicate { sequence } => sequence,
            semantic_memory::journal::ReplicaApplyOutcome::Fork { .. }
            | semantic_memory::journal::ReplicaApplyOutcome::Gap { .. }
            | semantic_memory::journal::ReplicaApplyOutcome::EpochConflict { .. }
            | semantic_memory::journal::ReplicaApplyOutcome::StalePredecessor { .. } => {
                return Err(MnemesError::FactCreateRejected("semantic conflict".into()));
            }
        };
        let ack = FactCreateAckRecord {
            batch_id: batch.batch_id.clone(),
            request_digest,
            home_device_id: batch.home_device_id.clone(),
            store_id: batch.store_id.clone(),
            stream_epoch: batch.stream_epoch,
            accepted_head,
            disposition: "accepted".into(),
        };
        self.persist_fact_create_ack_locked(&ack).await?;
        Ok(ack)
    }

    async fn check_fact_create_admission_locked(
        &self,
        batch: &SignedFactCreateBatchV1,
        namespace: &str,
    ) -> Result<(), MnemesError> {
        self.check_fact_create_admission_inner(batch, namespace)
            .await
    }

    async fn check_fact_create_admission_inner(
        &self,
        batch: &SignedFactCreateBatchV1,
        namespace: &str,
    ) -> Result<(), MnemesError> {
        let device_id = DeviceId::parse(&batch.home_device_id)?;
        let conn = self.pool_conn.lock().await;
        let row: Option<(Vec<u8>, i64, i64, i64, i64, String)> = conn.query_row(
            "SELECT public_key,activated_at,cutoff_at,revoked,stream_epoch,fencing_token \
             FROM fact_create_admissions WHERE device_id=?1 AND store_id=?2 AND namespace=?3 AND principal_id=?4 AND key_version=?5",
            rusqlite::params![device_id.as_str(), batch.store_id, namespace, batch.signer_principal_id, batch.signer_key_version as i64],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?)),
        ).optional()?;
        match row {
            Some((pk, activated, cutoff, revoked, epoch, fence))
                if pk.as_slice() == batch.signer_public_key
                    && revoked == 0
                    && activated >= 0
                    && cutoff >= activated
                    && batch.observed_at >= activated as u64
                    && batch.observed_at <= cutoff as u64
                    && epoch == batch.stream_epoch as i64
                    && fence == batch.fencing_token =>
            {
                Ok(())
            }
            _ => Err(MnemesError::FactCreateRejected(
                "key admission, lifecycle, scope, epoch, or fencing token rejected".into(),
            )),
        }
    }

    async fn get_fact_create_ack_locked(
        &self,
        batch_id: &str,
    ) -> Result<Option<FactCreateAckRecord>, MnemesError> {
        let conn = self.pool_conn.lock().await;
        Ok(conn.query_row("SELECT batch_id,request_digest,home_device_id,store_id,stream_epoch,accepted_head,disposition FROM fact_create_acks WHERE batch_id=?1", [batch_id], |r| Ok(FactCreateAckRecord { batch_id:r.get(0)?, request_digest:r.get(1)?, home_device_id:r.get(2)?, store_id:r.get(3)?, stream_epoch:u64::try_from(r.get::<_, i64>(4)?).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(4, 0))?, accepted_head:r.get(5)?, disposition:r.get(6)? })).optional()?)
    }

    async fn persist_fact_create_ack_locked(
        &self,
        ack: &FactCreateAckRecord,
    ) -> Result<(), MnemesError> {
        let conn = self.pool_conn.lock().await;
        conn.execute("INSERT INTO fact_create_acks (batch_id,request_digest,home_device_id,store_id,stream_epoch,accepted_head,disposition) VALUES (?1,?2,?3,?4,?5,?6,?7)", rusqlite::params![ack.batch_id,ack.request_digest,ack.home_device_id,ack.store_id,i64::try_from(ack.stream_epoch).map_err(|_| MnemesError::FactCreateRejected("stream epoch does not fit SQLite INTEGER".into()))?,ack.accepted_head,ack.disposition])?;
        Ok(())
    }

    /// Local bootstrap-only admission API for the supersession family.
    pub async fn admit_fact_supersede_key(
        &self,
        admission: FactSupersedeAdmission,
    ) -> Result<(), MnemesError> {
        let _gate = self.fact_supersede_gate.lock().await;
        if admission.store_id.is_empty()
            || admission.replacement_namespace.is_empty()
            || admission.principal_id.is_empty()
            || admission.fencing_token.is_empty()
            || admission.key_version == 0
            || admission.store_epoch == 0
            || admission.writer_epoch == 0
            || admission.activated_at > admission.cutoff_at
        {
            return Err(MnemesError::FactSupersedeRejected(
                "invalid fact-supersede admission record".into(),
            ));
        }
        let conn = self.pool_conn.lock().await;
        conn.execute(
            "INSERT INTO fact_supersede_admissions \
             (device_id,store_id,replacement_namespace,principal_id,key_version,public_key,activated_at,cutoff_at,revoked,store_epoch,writer_epoch,fencing_token) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,0,?9,?10,?11) \
             ON CONFLICT(device_id,store_id,replacement_namespace,principal_id,key_version) DO UPDATE SET \
             public_key=excluded.public_key,activated_at=excluded.activated_at,cutoff_at=excluded.cutoff_at,revoked=0,store_epoch=excluded.store_epoch,writer_epoch=excluded.writer_epoch,fencing_token=excluded.fencing_token",
            rusqlite::params![
                admission.device_id.as_str(), admission.store_id, admission.replacement_namespace,
                admission.principal_id,
                i64::try_from(admission.key_version).map_err(|_| MnemesError::FactSupersedeRejected("key version does not fit SQLite INTEGER".into()))?,
                admission.public_key.as_slice(),
                i64::try_from(admission.activated_at).map_err(|_| MnemesError::FactSupersedeRejected("activation time does not fit SQLite INTEGER".into()))?,
                i64::try_from(admission.cutoff_at).map_err(|_| MnemesError::FactSupersedeRejected("cutoff time does not fit SQLite INTEGER".into()))?,
                i64::try_from(admission.store_epoch).map_err(|_| MnemesError::FactSupersedeRejected("store epoch does not fit SQLite INTEGER".into()))?,
                i64::try_from(admission.writer_epoch).map_err(|_| MnemesError::FactSupersedeRejected("writer epoch does not fit SQLite INTEGER".into()))?,
                admission.fencing_token,
            ],
        )?;
        Ok(())
    }

    /// Revoke one exact supersession writer admission.
    pub async fn revoke_fact_supersede_key(
        &self,
        device_id: &DeviceId,
        store_id: &str,
        replacement_namespace: &str,
        principal_id: &str,
        key_version: u64,
    ) -> Result<(), MnemesError> {
        let _gate = self.fact_supersede_gate.lock().await;
        let conn = self.pool_conn.lock().await;
        conn.execute(
            "UPDATE fact_supersede_admissions SET revoked=1 WHERE device_id=?1 AND store_id=?2 AND replacement_namespace=?3 AND principal_id=?4 AND key_version=?5",
            rusqlite::params![device_id.as_str(), store_id, replacement_namespace, principal_id, key_version as i64],
        )?;
        Ok(())
    }

    /// Atomically admit, apply, and acknowledge one owner-produced V1
    /// supersession.  The gate is process-local only.
    pub async fn apply_fact_supersede_request(
        &self,
        batch: &SignedFactSupersedeBatchV1,
        request_digest: String,
    ) -> Result<FactSupersedeAckRecord, MnemesError> {
        let _gate = self.fact_supersede_gate.lock().await;
        if let Some(existing) = self.get_fact_supersede_ack_locked(&batch.batch_id).await? {
            if existing.request_digest != request_digest {
                return Err(MnemesError::FactSupersedeRejected(
                    "batch id digest collision".into(),
                ));
            }
            return Ok(existing);
        }
        let envelope = batch
            .semantic_envelope()
            .map_err(|error| MnemesError::Replication(error.to_string()))?;
        let replacement_namespace = batch
            .replacement_namespace()
            .map_err(|error| MnemesError::FactSupersedeRejected(error.to_string()))?;
        self.check_fact_supersede_admission_locked(batch, &replacement_namespace)
            .await?;
        let memory = self
            .device_memory(&DeviceId::parse(&batch.home_device_id)?)
            .await?;
        let decision = match memory.apply_verified_fact_supersede(envelope).await {
            Ok(decision) => decision,
            Err(semantic_memory::MemoryError::CorruptData { detail, .. })
                if detail == "fact-supersede semantic lineage digest mismatch" =>
            {
                return Err(MnemesError::FactSupersedeSemanticConflict);
            }
            Err(error) => return Err(error.into()),
        };
        let accepted_head = match decision {
            semantic_memory::journal::ReplicaApplyOutcome::Applied { sequence, .. }
            | semantic_memory::journal::ReplicaApplyOutcome::Duplicate { sequence } => sequence,
            semantic_memory::journal::ReplicaApplyOutcome::Fork { .. }
            | semantic_memory::journal::ReplicaApplyOutcome::Gap { .. }
            | semantic_memory::journal::ReplicaApplyOutcome::EpochConflict { .. }
            | semantic_memory::journal::ReplicaApplyOutcome::StalePredecessor { .. } => {
                return Err(MnemesError::FactSupersedeSemanticConflict);
            }
        };
        let ack = FactSupersedeAckRecord {
            batch_id: batch.batch_id.clone(),
            request_digest,
            home_device_id: batch.home_device_id.clone(),
            store_id: batch.store_id.clone(),
            owner_stream_epoch: batch.owner_stream_epoch,
            accepted_head,
            disposition: "accepted".into(),
        };
        self.persist_fact_supersede_ack_locked(&ack).await?;
        Ok(ack)
    }

    async fn check_fact_supersede_admission_locked(
        &self,
        batch: &SignedFactSupersedeBatchV1,
        replacement_namespace: &str,
    ) -> Result<(), MnemesError> {
        let device_id = DeviceId::parse(&batch.home_device_id)?;
        let conn = self.pool_conn.lock().await;
        let row: Option<FactSupersedeAdmissionRow> = conn.query_row(
            "SELECT public_key,activated_at,cutoff_at,revoked,store_epoch,writer_epoch,fencing_token \
             FROM fact_supersede_admissions WHERE device_id=?1 AND store_id=?2 AND replacement_namespace=?3 AND principal_id=?4 AND key_version=?5",
            rusqlite::params![device_id.as_str(), batch.store_id, replacement_namespace, batch.signer_principal_id, batch.signer_key_version as i64],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?)),
        ).optional()?;
        match row {
            Some((pk, activated, cutoff, revoked, store_epoch, writer_epoch, fence))
                if pk.as_slice() == batch.signer_public_key
                    && revoked == 0
                    && activated >= 0
                    && cutoff >= activated
                    && batch.observed_at >= activated as u64
                    && batch.observed_at <= cutoff as u64
                    && store_epoch == batch.store_epoch as i64
                    && writer_epoch == batch.writer_epoch as i64
                    && fence == batch.fencing_token =>
            {
                Ok(())
            }
            _ => Err(MnemesError::FactSupersedeRejected(
                "key admission, lifecycle, replacement namespace, epoch, or fencing token rejected"
                    .into(),
            )),
        }
    }

    async fn get_fact_supersede_ack_locked(
        &self,
        batch_id: &str,
    ) -> Result<Option<FactSupersedeAckRecord>, MnemesError> {
        let conn = self.pool_conn.lock().await;
        Ok(conn.query_row(
            "SELECT batch_id,request_digest,home_device_id,store_id,owner_stream_epoch,accepted_head,disposition FROM fact_supersede_acks WHERE batch_id=?1",
            [batch_id],
            |r| Ok(FactSupersedeAckRecord {
                batch_id: r.get(0)?, request_digest: r.get(1)?, home_device_id: r.get(2)?, store_id: r.get(3)?,
                owner_stream_epoch: u64::try_from(r.get::<_, i64>(4)?).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(4, 0))?,
                accepted_head: r.get(5)?, disposition: r.get(6)?,
            }),
        ).optional()?)
    }

    async fn persist_fact_supersede_ack_locked(
        &self,
        ack: &FactSupersedeAckRecord,
    ) -> Result<(), MnemesError> {
        let conn = self.pool_conn.lock().await;
        conn.execute(
            "INSERT INTO fact_supersede_acks (batch_id,request_digest,home_device_id,store_id,owner_stream_epoch,accepted_head,disposition) VALUES (?1,?2,?3,?4,?5,?6,?7)",
            rusqlite::params![ack.batch_id, ack.request_digest, ack.home_device_id, ack.store_id,
                i64::try_from(ack.owner_stream_epoch).map_err(|_| MnemesError::FactSupersedeRejected("owner stream epoch does not fit SQLite INTEGER".into()))?,
                ack.accepted_head, ack.disposition],
        )?;
        Ok(())
    }

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

    /// Open one active profile store through its canonical store identity only.
    ///
    /// Unlike the legacy device-shard accessor this never creates a directory,
    /// runs migrations, or returns a write-capable semantic-memory handle.
    pub async fn profile_store_memory(
        &self,
        store_id: &str,
    ) -> Result<Arc<semantic_memory::MemoryStore>, MnemesError> {
        let (profile_id, relative_path): (String, String) = {
            let conn = self.pool_conn.lock().await;
            conn.query_row(
                "SELECT s.profile_id, s.relative_path
                 FROM memory_stores s
                 JOIN memory_profiles p ON p.profile_id = s.profile_id
                 JOIN devices d ON d.device_id = s.owner_device_id
                 WHERE s.store_id = ?1 AND s.status = 'active'
                   AND p.status = 'active' AND d.status = 'active'",
                params![store_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?
        }
        .ok_or_else(|| {
            MnemesError::MemoryGrantDenied(
                "profile store is not active or does not exist".to_string(),
            )
        })?;
        let profile_id = MemoryProfileId::parse(profile_id)?;
        let expected = canonical_memory_store_relative_path(&profile_id, store_id)?;
        if relative_path != expected {
            return Err(MnemesError::InvalidMemoryScope(
                "profile store path is not canonical".to_string(),
            ));
        }
        let cache_key = format!("profile:{store_id}");
        let mut cache = self.shard_cache.lock().await;
        if let Some(store) = cache.get_key(&cache_key) {
            return Ok(store);
        }
        let mut config = self.memory_config.clone();
        config.base_dir = self.base_dir.join(relative_path);
        let store = semantic_memory::MemoryStore::open_existing_read_only_with_embedder(
            config,
            Box::new(SharedEmbedder {
                inner: self.embedder.clone(),
            }),
        )?;
        let store = Arc::new(store);
        cache.insert_key(&cache_key, store.clone());
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

    /// Return aggregate semantic counts from the canonical shard catalog.
    ///
    /// This is intentionally catalog-only: operator/read paths must not open
    /// or recreate the rejected legacy `memory/memory.db` store merely to
    /// report statistics.
    pub async fn shard_stats(&self) -> Result<semantic_memory::MemoryStats, MnemesError> {
        let shards = self.list_shards().await?;
        let mut stats = semantic_memory::MemoryStats {
            total_facts: 0,
            total_documents: 0,
            total_chunks: 0,
            total_sessions: 0,
            total_messages: 0,
            database_size_bytes: 0,
            embedding_model: Some(self.embedder.model_name().to_string()),
            embedding_dimensions: Some(self.embedder.dimensions()),
        };
        for shard in shards {
            stats.total_facts = stats.total_facts.saturating_add(shard.fact_count);
            stats.total_documents = stats.total_documents.saturating_add(shard.document_count);
            stats.total_chunks = stats.total_chunks.saturating_add(shard.chunk_count);
            stats.total_messages = stats.total_messages.saturating_add(shard.message_count);
            let path = self.device_shard_path(&shard.device_id).join("memory.db");
            if let Ok(metadata) = std::fs::metadata(path) {
                stats.database_size_bytes =
                    stats.database_size_bytes.saturating_add(metadata.len());
            }
        }
        Ok(stats)
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
            MnemesError::InvalidShardCatalog(format!("failed to serialize namespaces: {error}"))
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

    /// Quick check whether any active shards with facts are registered.
    pub async fn has_shards(&self) -> Result<bool, MnemesError> {
        let conn = self.pool_conn.lock().await;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM device_shards WHERE state = 'active' AND fact_count > 0",
            [],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Return whether the canonical catalog contains any active semantic shard.
    /// This does not open or create a shard database.
    pub async fn has_registered_shards(&self) -> Result<bool, MnemesError> {
        let conn = self.pool_conn.lock().await;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM device_shards WHERE state = 'active'",
            [],
            |row| row.get(0),
        )?;
        Ok(count > 0)
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
            return Err(MnemesError::DeviceNotFound(requester_device_id.to_string()));
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

    /// Search only the stores admitted by the actor's current profile snapshot.
    /// The request has no profile selector: subject resolution is server-side.
    pub async fn routed_search_for_profile(
        &self,
        actor_id: &ActorId,
        request: RoutingSearchRequest,
        authorization_time: u64,
    ) -> Result<ProfileRoutedSearchResponse, MnemesError> {
        let snapshot = self
            .build_authorization_snapshot(
                actor_id,
                MemoryAccessEffect::Search,
                request.namespaces.as_deref(),
                authorization_time,
            )
            .await?;
        let authorized_stores = snapshot.authorized_stores.clone();
        let selected_stores = authorized_stores
            .iter()
            .map(|store| store.store_id.clone())
            .collect::<Vec<_>>();
        let query_sha256 = sha256_hex(&request.query);
        let mut outcomes = Vec::with_capacity(authorized_stores.len());
        let mut results = Vec::new();
        for authorized in &authorized_stores {
            let started = Instant::now();
            match self.profile_store_memory(&authorized.store_id).await {
                Ok(memory) => {
                    let namespaces = request
                        .namespaces
                        .as_ref()
                        .map(|values| values.iter().map(String::as_str).collect::<Vec<_>>());
                    match memory
                        .search(
                            &request.query,
                            Some(request.top_k),
                            namespaces.as_deref(),
                            request.source_types.as_deref(),
                        )
                        .await
                    {
                        Ok(store_results) => {
                            let result_count = store_results.len();
                            results.extend(store_results.into_iter().map(|result| {
                                ProfileRoutedSearchResult {
                                    result,
                                    store_id: authorized.store_id.clone(),
                                    profile_id: authorized.profile_id.clone(),
                                    owner_device_id: authorized.owner_device_id.clone(),
                                    namespace: authorized.namespace.clone(),
                                    child_search_receipt_id: None,
                                }
                            }));
                            outcomes.push(ProfileStoreSearchOutcome {
                                store_id: authorized.store_id.clone(),
                                profile_id: authorized.profile_id.clone(),
                                latency_ms: started.elapsed().as_millis().min(u128::from(u64::MAX))
                                    as u64,
                                result_count,
                                child_search_receipt_id: None,
                                error: None,
                            });
                        }
                        Err(error) => outcomes.push(ProfileStoreSearchOutcome {
                            store_id: authorized.store_id.clone(),
                            profile_id: authorized.profile_id.clone(),
                            latency_ms: started.elapsed().as_millis().min(u128::from(u64::MAX))
                                as u64,
                            result_count: 0,
                            child_search_receipt_id: None,
                            error: Some(error.to_string()),
                        }),
                    }
                }
                Err(error) => outcomes.push(ProfileStoreSearchOutcome {
                    store_id: authorized.store_id.clone(),
                    profile_id: authorized.profile_id.clone(),
                    latency_ms: started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                    result_count: 0,
                    child_search_receipt_id: None,
                    error: Some(error.to_string()),
                }),
            }
        }
        results.sort_by(|left, right| {
            right
                .result
                .score
                .total_cmp(&left.result.score)
                .then_with(|| {
                    left.result
                        .source
                        .result_id()
                        .cmp(&right.result.source.result_id())
                })
                .then_with(|| left.store_id.cmp(&right.store_id))
        });
        let mut content_by_id = HashMap::<String, String>::new();
        let mut emitted = HashSet::<String>::new();
        let mut merged = Vec::new();
        for result in results {
            let result_id = result.result.source.result_id();
            if let Some(existing) = content_by_id.get(&result_id) {
                if existing != &result.result.content {
                    return Err(MnemesError::ConflictingShardItem { item_id: result_id });
                }
            } else {
                content_by_id.insert(result_id.clone(), result.result.content.clone());
            }
            if emitted.insert(result_id) {
                merged.push(result);
            }
        }
        merged.truncate(request.top_k);
        let final_result_ids = merged
            .iter()
            .map(|result| result.result.source.result_id())
            .collect::<Vec<_>>();
        let mut receipt = ProfileRoutingReceipt {
            receipt_id: uuid::Uuid::new_v4().to_string(),
            actor_id: actor_id.clone(),
            subject_profile_id: snapshot.subject_profile_id.clone(),
            authorization_snapshot_digest: snapshot.snapshot_digest.clone(),
            authorized_stores,
            selected_stores,
            skipped_stores: Vec::new(),
            complete: outcomes.iter().all(|outcome| outcome.error.is_none()),
            outcomes,
            final_result_ids,
            query_sha256,
            receipt_digest: String::new(),
            recorded_at: Utc::now().to_rfc3339(),
        };
        receipt.receipt_digest = profile_routing_receipt_digest(&self.receipt_auth_key, &receipt)?;
        validate_profile_routing_receipt(&self.receipt_auth_key, &receipt)?;
        self.persist_profile_routing_receipt(&receipt).await?;
        Ok(ProfileRoutedSearchResponse {
            results: merged,
            routing_receipt: receipt,
        })
    }

    async fn persist_profile_routing_receipt(
        &self,
        receipt: &ProfileRoutingReceipt,
    ) -> Result<(), MnemesError> {
        validate_profile_routing_receipt(&self.receipt_auth_key, receipt)?;
        let payload = serde_json::to_string(receipt)
            .map_err(|error| MnemesError::InvalidShardCatalog(error.to_string()))?;
        let conn = self.pool_conn.lock().await;
        conn.execute(
            "INSERT INTO profile_routing_receipts(
                receipt_id, requester_actor_id, subject_profile_id,
                authorization_snapshot_digest, receipt_json, receipt_digest, recorded_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                receipt.receipt_id,
                receipt.actor_id.as_str(),
                receipt.subject_profile_id.as_str(),
                receipt.authorization_snapshot_digest,
                payload,
                receipt.receipt_digest,
                receipt.recorded_at,
            ],
        )?;
        Ok(())
    }

    /// Read and authenticate one durable profile routing receipt.
    pub async fn get_profile_routing_receipt(
        &self,
        receipt_id: &str,
    ) -> Result<Option<ProfileRoutingReceipt>, MnemesError> {
        let conn = self.pool_conn.lock().await;
        let payload = conn
            .query_row(
                "SELECT receipt_json FROM profile_routing_receipts WHERE receipt_id = ?1",
                params![receipt_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let Some(payload) = payload else {
            return Ok(None);
        };
        let receipt = serde_json::from_str::<ProfileRoutingReceipt>(&payload)
            .map_err(|error| MnemesError::InvalidShardCatalog(error.to_string()))?;
        validate_profile_routing_receipt(&self.receipt_auth_key, &receipt)?;
        Ok(Some(receipt))
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

    /// Accept a source fact into the authenticated device's shard.
    ///
    /// The source ID is stored only in pooled control-plane state for durable
    /// idempotency. Content follows semantic-memory's normal `add_fact` path,
    /// so the server owns embedding generation and all schema-specific writes.
    pub async fn sync_fact_to_shard(
        &self,
        device_id: &DeviceId,
        source_fact_id: &str,
        namespace: &str,
        content: &str,
        source: Option<&str>,
        metadata: Option<Value>,
    ) -> Result<FactSyncOutcome, MnemesError> {
        let device = self
            .get_device(device_id)
            .await?
            .ok_or_else(|| MnemesError::DeviceNotFound(device_id.to_string()))?;
        if device.status != DeviceStatus::Active {
            return Err(MnemesError::DeviceNotActive(device_id.to_string()));
        }

        {
            let conn = self.pool_conn.lock().await;
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS synced_facts (
                    source_fact_id TEXT NOT NULL,
                    device_id TEXT NOT NULL,
                    server_fact_id TEXT NOT NULL,
                    synced_at TEXT NOT NULL,
                    PRIMARY KEY (source_fact_id, device_id)
                )",
            )?;
            let already: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM synced_facts
                 WHERE source_fact_id = ?1 AND device_id = ?2)",
                params![source_fact_id, device_id.as_str()],
                |row| row.get(0),
            )?;
            if already {
                return Ok(FactSyncOutcome::Skipped);
            }
        }

        // Reconcile a pre-daemon migration by exact namespace+content match.
        // Existing shards predate source-ID tracking, so this maps matching
        // facts without blindly duplicating the already-migrated baseline.
        let shard_db = self.device_shard_path(device_id).join("memory.db");
        let existing_fact_id = if shard_db.exists() {
            let conn = rusqlite::Connection::open_with_flags(
                &shard_db,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                    | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )?;
            conn.query_row(
                "SELECT id FROM facts WHERE namespace = ?1 AND content = ?2 LIMIT 1",
                params![namespace, content],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        } else {
            None
        };

        let (server_fact_id, outcome) = if let Some(existing) = existing_fact_id {
            (existing, FactSyncOutcome::Skipped)
        } else {
            // Re-embedding is done entirely at the authority's configured provider.
            let shard = self.device_memory(device_id).await?;
            let id = shard.add_fact(namespace, content, source, metadata).await?;
            (id.clone(), FactSyncOutcome::Synced { server_fact_id: id })
        };

        // Commit the source-ID acknowledgement only after the fact is durable.
        // If this acknowledgement write fails, a retry is safer than silently
        // advancing the client watermark.
        let conn = self.pool_conn.lock().await;
        conn.execute(
            "INSERT INTO synced_facts (source_fact_id, device_id, server_fact_id, synced_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                source_fact_id,
                device_id.as_str(),
                &server_fact_id,
                chrono::Utc::now().to_rfc3339(),
            ],
        )?;

        Ok(outcome)
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

    pub async fn list_audit_events(&self, limit: usize) -> Result<Vec<AuditEvent>, MnemesError> {
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
                MnemesError::InvalidProvenance(format!("invalid normalized metadata JSON: {error}"))
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

    #[tokio::test]
    async fn profile_store_access_requires_explicit_grant_and_honors_revocation() {
        let (store, _dir) = open_test_store();
        let owner_device = DeviceId::new();
        let requester_device = DeviceId::new();
        store
            .register_device(Device::new(
                owner_device.clone(),
                "owner",
                "linux",
                "owner-host",
            ))
            .await
            .unwrap();
        store
            .register_device(Device::new(
                requester_device.clone(),
                "requester",
                "linux",
                "requester-host",
            ))
            .await
            .unwrap();

        let operator_id = ActorId::new();
        let mut operator = Actor::new(operator_id.clone(), owner_device.clone(), ActorKind::Human);
        operator.tool_profile = ToolProfile::Operator;
        store.register_actor(operator).await.unwrap();

        let owner_profile = MemoryProfile::new(
            MemoryProfileId::new("owner-profile").unwrap(),
            owner_device.clone(),
            "Owner profile",
        )
        .unwrap();
        let requester_profile = MemoryProfile::new(
            MemoryProfileId::new("requester-profile").unwrap(),
            requester_device.clone(),
            "Requester profile",
        )
        .unwrap();
        store
            .register_memory_profile(owner_profile.clone())
            .await
            .unwrap();
        store
            .register_memory_profile(requester_profile.clone())
            .await
            .unwrap();

        let memory_store = MemoryStoreIdentity::new(
            "owner-store",
            owner_profile.profile_id.clone(),
            owner_device.clone(),
            "private",
            "memory/profiles/owner-profile/owner-store",
        )
        .unwrap();
        store
            .register_memory_store(memory_store.clone())
            .await
            .unwrap();

        assert!(store
            .authorize_memory_access(
                &requester_profile.profile_id,
                &memory_store.store_id,
                "private",
                MemoryAccessEffect::Search,
                10,
            )
            .await
            .is_err());

        let grant = MemoryAccessGrant {
            grant_id: MemoryGrantId::new(),
            grantee_profile_id: requester_profile.profile_id.clone(),
            store_id: memory_store.store_id.clone(),
            namespace: "private".to_string(),
            effect: MemoryAccessEffect::Search,
            issued_by_actor_id: operator_id,
            valid_from: 10,
            expires_at: 20,
            revoked_at: None,
            created_at: String::new(),
        };
        let grant_id = grant.grant_id.clone();
        store.grant_memory_access(grant).await.unwrap();
        assert!(store
            .authorize_memory_access(
                &requester_profile.profile_id,
                &memory_store.store_id,
                "private",
                MemoryAccessEffect::Search,
                10,
            )
            .await
            .is_ok());

        store
            .set_memory_store_status(&memory_store.store_id, MemoryStoreStatus::Revoked)
            .await
            .unwrap();
        assert!(store
            .authorize_memory_access(
                &requester_profile.profile_id,
                &memory_store.store_id,
                "private",
                MemoryAccessEffect::Search,
                10,
            )
            .await
            .is_err());

        store
            .set_memory_store_status(&memory_store.store_id, MemoryStoreStatus::Active)
            .await
            .unwrap();
        store
            .set_memory_profile_status(&owner_profile.profile_id, MemoryProfileStatus::Revoked)
            .await
            .unwrap();
        assert!(store
            .authorize_memory_access(
                &requester_profile.profile_id,
                &memory_store.store_id,
                "private",
                MemoryAccessEffect::Search,
                10,
            )
            .await
            .is_err());
        store
            .set_memory_profile_status(&owner_profile.profile_id, MemoryProfileStatus::Active)
            .await
            .unwrap();
        store.revoke_memory_access(&grant_id, 15).await.unwrap();
        assert!(store
            .authorize_memory_access(
                &requester_profile.profile_id,
                &memory_store.store_id,
                "private",
                MemoryAccessEffect::Search,
                10,
            )
            .await
            .is_err());

        store.revoke_device(&owner_device).await.unwrap();
        assert!(store
            .authorize_memory_access(
                &requester_profile.profile_id,
                &memory_store.store_id,
                "private",
                MemoryAccessEffect::Search,
                10,
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn actor_profile_binding_drives_a_deterministic_revocable_authorization_snapshot() {
        let (store, _dir) = open_test_store();
        let owner_device = DeviceId::new();
        let requester_device = DeviceId::new();
        store
            .register_device(Device::new(
                owner_device.clone(),
                "owner",
                "linux",
                "owner-host",
            ))
            .await
            .unwrap();
        store
            .register_device(Device::new(
                requester_device.clone(),
                "requester",
                "linux",
                "requester-host",
            ))
            .await
            .unwrap();

        let operator_id = ActorId::new();
        let mut operator = Actor::new(operator_id.clone(), owner_device, ActorKind::Human);
        operator.tool_profile = ToolProfile::Operator;
        store.register_actor(operator).await.unwrap();

        let requester_actor_id = ActorId::new();
        store
            .register_actor(Actor::new(
                requester_actor_id.clone(),
                requester_device.clone(),
                ActorKind::Hermes,
            ))
            .await
            .unwrap();

        let requester_profile = MemoryProfile::new(
            MemoryProfileId::new("requester-profile").unwrap(),
            requester_device.clone(),
            "Requester profile",
        )
        .unwrap();
        store
            .register_memory_profile(requester_profile.clone())
            .await
            .unwrap();
        let own_store = MemoryStoreIdentity::new(
            "requester-store",
            requester_profile.profile_id.clone(),
            requester_device.clone(),
            "private",
            "memory/profiles/requester-profile/requester-store",
        )
        .unwrap();
        store
            .register_memory_store(own_store.clone())
            .await
            .unwrap();

        let binding = ActorProfileBinding::new(
            requester_actor_id.clone(),
            requester_profile.profile_id.clone(),
            requester_device,
            operator_id,
            10,
            20,
            1,
        )
        .unwrap();
        let binding_id = binding.binding_id.clone();
        store.bind_actor_profile(binding).await.unwrap();

        let snapshot = store
            .build_authorization_snapshot(&requester_actor_id, MemoryAccessEffect::Search, None, 11)
            .await
            .unwrap();
        assert_eq!(snapshot.subject_profile_id, requester_profile.profile_id);
        assert_eq!(snapshot.binding_id, binding_id);
        assert_eq!(snapshot.authorized_stores.len(), 1);
        assert_eq!(snapshot.authorized_stores[0].store_id, own_store.store_id);
        assert!(store
            .validate_authorization_snapshot(&snapshot, 11)
            .await
            .is_ok());

        let permit = store
            .issue_memory_access_permit(&snapshot, &own_store.store_id, "private", 11, 5)
            .await
            .unwrap();
        assert_eq!(permit.subject_profile_id, requester_profile.profile_id);
        assert_eq!(permit.store_id, own_store.store_id);

        store
            .revoke_actor_profile_binding(&binding_id, 12)
            .await
            .unwrap();
        assert!(store
            .validate_authorization_snapshot(&snapshot, 12)
            .await
            .is_err());
        assert!(
            store
                .build_authorization_snapshot(
                    &requester_actor_id,
                    MemoryAccessEffect::Search,
                    None,
                    12,
                )
                .await
                .is_err()
        );
    }

    #[test]
    fn provider_name_defaults_to_candle_and_normalizes_aliases() {
        assert_eq!(configured_provider_name(None), "candle");
        assert_eq!(configured_provider_name(Some("  LOCAL ")), "local");
        assert_eq!(configured_provider_name(Some("OLLAMA")), "ollama");
    }

    #[test]
    fn provider_name_does_not_silently_fallback_unknown_values() {
        assert_eq!(
            configured_provider_name(Some("custom-provider")),
            "custom-provider"
        );
    }
}
