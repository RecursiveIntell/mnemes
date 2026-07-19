//! Pooled memory store: device/actor/operation registry with its own SQLite
//! database, wrapping a `semantic_memory::MemoryStore` for the actual memory
//! content.

use crate::error::PooledMemoryError;
use crate::types::*;
use chrono::Utc;
use rusqlite::params;
use std::path::PathBuf;
use std::sync::Arc;

/// Combined pooled memory store.
///
/// Owns a device/actor/operation SQLite database alongside a
/// `semantic_memory::MemoryStore`. The two databases are separate:
/// - `pooled.db` — device registry, actors, operation envelopes
/// - `memory.db` — facts, documents, episodes, embeddings (owned by semantic-memory)
pub struct PooledMemoryStore {
    pool_conn: tokio::sync::Mutex<rusqlite::Connection>,
    memory: Arc<semantic_memory::MemoryStore>,
}

impl PooledMemoryStore {
    /// Open a pooled memory store at the given base directory.
    /// Creates `pooled.db` for device/actor/operation data and delegates
    /// memory content to `semantic_memory::MemoryStore` in a subdirectory.
    pub fn open(
        base_dir: PathBuf,
        memory_config: semantic_memory::MemoryConfig,
    ) -> Result<Self, PooledMemoryError> {
        std::fs::create_dir_all(&base_dir)?;

        let pool_db_path = base_dir.join("pooled.db");
        let conn = rusqlite::Connection::open(&pool_db_path)?;
        Self::init_schema(&conn)?;

        // Adjust memory config to use a subdirectory
        let mut mem_config = memory_config;
        mem_config.base_dir = base_dir.join("memory");
        std::fs::create_dir_all(&mem_config.base_dir)?;

        let memory = semantic_memory::MemoryStore::open(mem_config)
            .map_err(PooledMemoryError::Memory)?;

        Ok(Self {
            pool_conn: tokio::sync::Mutex::new(conn),
            memory: Arc::new(memory),
        })
    }

    /// Open with a custom embedder (for testing).
    pub fn open_with_embedder(
        base_dir: PathBuf,
        memory_config: semantic_memory::MemoryConfig,
        embedder: Box<dyn semantic_memory::Embedder>,
    ) -> Result<Self, PooledMemoryError> {
        std::fs::create_dir_all(&base_dir)?;

        let pool_db_path = base_dir.join("pooled.db");
        let conn = rusqlite::Connection::open(&pool_db_path)?;
        Self::init_schema(&conn)?;

        let mut mem_config = memory_config;
        mem_config.base_dir = base_dir.join("memory");
        std::fs::create_dir_all(&mem_config.base_dir)?;

        let memory = semantic_memory::MemoryStore::open_with_embedder(mem_config, embedder)
            .map_err(PooledMemoryError::Memory)?;

        Ok(Self {
            pool_conn: tokio::sync::Mutex::new(conn),
            memory: Arc::new(memory),
        })
    }

    /// Access the underlying semantic-memory store.
    pub fn memory(&self) -> &semantic_memory::MemoryStore {
        &self.memory
    }

    fn init_schema(conn: &rusqlite::Connection) -> Result<(), PooledMemoryError> {
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA foreign_keys = ON;
             PRAGMA busy_timeout = 5000;
             PRAGMA synchronous = NORMAL;",
        )?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS devices (
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
                provider_model  TEXT,
                recorded_at     TEXT NOT NULL,
                created_at      TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_actors_device ON actors(device_id);

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
                ON operation_envelopes(recorded_at DESC);",
        )?;

        Ok(())
    }

    // ─── Device registry ──────────────────────────────────────────────

    pub async fn register_device(&self, mut device: Device) -> Result<DeviceId, PooledMemoryError> {
        let now = Utc::now().to_rfc3339();
        device.first_seen_at = now.clone();
        device.last_seen_at = now;
        let device_id_return = device.device_id.clone();

        let conn = self.pool_conn.lock().await;
        conn.execute(
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
        Ok(device_id_return)
    }

    pub async fn get_device(&self, device_id: &DeviceId) -> Result<Option<Device>, PooledMemoryError> {
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

    pub async fn list_devices(&self) -> Result<Vec<Device>, PooledMemoryError> {
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

    pub async fn revoke_device(&self, device_id: &DeviceId) -> Result<(), PooledMemoryError> {
        let now = Utc::now().to_rfc3339();
        let conn = self.pool_conn.lock().await;
        let affected = conn.execute(
            "UPDATE devices SET status = 'revoked', last_seen_at = ?1 WHERE device_id = ?2",
            params![now, device_id.as_str()],
        )?;
        if affected == 0 {
            return Err(PooledMemoryError::DeviceNotFound(device_id.to_string()));
        }
        Ok(())
    }

    pub async fn heartbeat_device(&self, device_id: &DeviceId) -> Result<(), PooledMemoryError> {
        let now = Utc::now().to_rfc3339();
        let conn = self.pool_conn.lock().await;
        let status: String = conn
            .query_row(
                "SELECT status FROM devices WHERE device_id = ?1",
                params![device_id.as_str()],
                |row| row.get(0),
            )
            .map_err(|_| PooledMemoryError::DeviceNotFound(device_id.to_string()))?;

        if status != "active" {
            return Err(PooledMemoryError::DeviceNotActive(format!(
                "{device_id} (status: {status})"
            )));
        }
        conn.execute(
            "UPDATE devices SET last_seen_at = ?1 WHERE device_id = ?2",
            params![now, device_id.as_str()],
        )?;
        Ok(())
    }

    // ─── Actor registry ───────────────────────────────────────────────

    pub async fn register_actor(&self, mut actor: Actor) -> Result<ActorId, PooledMemoryError> {
        let now = Utc::now().to_rfc3339();
        actor.recorded_at = now;
        let actor_id_return = actor.actor_id.clone();

        let conn = self.pool_conn.lock().await;
        conn.execute(
            "INSERT INTO actors (actor_id, device_id, actor_kind, provider_model, recorded_at) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                actor.actor_id.as_str(),
                actor.device_id.as_str(),
                actor.actor_kind.as_str(),
                actor.provider_model,
                actor.recorded_at,
            ],
        )?;
        Ok(actor_id_return)
    }

    pub async fn get_actor(&self, actor_id: &ActorId) -> Result<Option<Actor>, PooledMemoryError> {
        let conn = self.pool_conn.lock().await;
        let result = conn
            .query_row(
                "SELECT actor_id, device_id, actor_kind, provider_model, recorded_at \
                 FROM actors WHERE actor_id = ?1",
                params![actor_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )
            .ok();

        if let Some((aid, did, kind_str, pm, rat)) = result {
            Ok(Some(Actor {
                actor_id: ActorId::parse(&aid)?,
                device_id: DeviceId::parse(&did)?,
                actor_kind: ActorKind::parse(kind_str),
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
    ) -> Result<String, PooledMemoryError> {
        // Check idempotency first
        let idempotency_key = envelope.idempotency_key.clone();
        if let Some(existing) = self.check_idempotency(&idempotency_key).await? {
            return Ok(existing);
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

    pub async fn get_operation(
        &self,
        operation_id: &OperationId,
    ) -> Result<Option<OperationEnvelope>, PooledMemoryError> {
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
            oid, idem, req_dev, req_act, rec_dev, rec_srv, kind_str, tgt_kind, tgt_id,
            digest, obs_at, val_at, rec_at, rcpt_id,
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

    pub async fn check_idempotency(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<String>, PooledMemoryError> {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;
    use semantic_memory::{EmbeddingConfig, MemoryConfig, MockEmbedder};
    use tempfile::TempDir;

    fn open_test_store() -> (PooledMemoryStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let config = MemoryConfig {
            base_dir: dir.path().to_path_buf(),
            embedding: EmbeddingConfig {
                dimensions: 768,
                ..Default::default()
            },
            ..Default::default()
        };
        let store = PooledMemoryStore::open_with_embedder(
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
            .register_actor(Actor::new(actor_id.clone(), dev_id.clone(), ActorKind::Hermes))
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
            .register_actor(Actor::new(actor_id.clone(), dev_id.clone(), ActorKind::Codex))
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
            .register_actor(Actor::new(actor_id.clone(), dev_id.clone(), ActorKind::Hermes))
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
        // Verify the underlying semantic-memory store is accessible
        let stats = store.memory().stats().await.unwrap();
        assert_eq!(stats.total_facts, 0);
    }
}
