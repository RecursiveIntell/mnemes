//! Pooled memory store: device/actor/operation registry with its own SQLite
//! database, wrapping a `semantic_memory::MemoryStore` for the actual memory
//! content.

use crate::error::PooledMemoryError;
use crate::types::*;
use chrono::{DateTime, Utc};
use rusqlite::{params, OptionalExtension};
use semantic_memory::GraphDirection;
use serde_json::Value;
use std::collections::{HashSet, VecDeque};
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

        let memory =
            semantic_memory::MemoryStore::open(mem_config).map_err(PooledMemoryError::Memory)?;

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
            );",
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

    pub async fn get_device(
        &self,
        device_id: &DeviceId,
    ) -> Result<Option<Device>, PooledMemoryError> {
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

    // ─── Provenance edge helpers ─────────────────────────────────────

    fn parse_rfc3339(value: &str, field: &str) -> Result<DateTime<Utc>, PooledMemoryError> {
        DateTime::parse_from_rfc3339(value)
            .map_err(|error| {
                PooledMemoryError::InvalidProvenance(format!("invalid {field}: {value} ({error})"))
            })
            .map(|value| value.with_timezone(&Utc))
    }

    fn parse_optional_rfc3339(
        value: Option<String>,
        field: &str,
    ) -> Result<Option<DateTime<Utc>>, PooledMemoryError> {
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

    fn validate_item_ref(item: &MemoryItemRef) -> Result<(), PooledMemoryError> {
        if item.kind.trim().is_empty() || item.id.trim().is_empty() {
            return Err(PooledMemoryError::InvalidProvenance(
                "memory item references require non-empty kind and id".to_string(),
            ));
        }
        Ok(())
    }

    fn normalize_metadata(raw: &Option<String>) -> Result<Option<String>, PooledMemoryError> {
        raw.as_ref()
            .map(|raw| {
                let parsed = serde_json::from_str::<Value>(raw).map_err(|error| {
                    PooledMemoryError::InvalidProvenance(format!("invalid metadata JSON: {error}"))
                })?;
                serde_json::to_string(&parsed).map_err(|error| {
                    PooledMemoryError::InvalidProvenance(format!(
                        "failed to canonicalize metadata JSON: {error}"
                    ))
                })
            })
            .transpose()
    }

    fn parse_metadata_for_result(raw: Option<String>) -> Result<Option<Value>, PooledMemoryError> {
        raw.map(|value| serde_json::from_str::<Value>(&value))
            .transpose()
            .map_err(|error| {
                PooledMemoryError::InvalidProvenance(format!(
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
    ) -> Result<(), PooledMemoryError> {
        if request.operation_id.is_none() {
            return Err(PooledMemoryError::InvalidProvenance(
                "operation_id is required for provenance edge mutation".to_string(),
            ));
        }

        let operation_id = request
            .operation_id
            .as_ref()
            .expect("operation_id is required for provenance edges");
        let operation = Self::fetch_operation_conn(conn, operation_id)?.ok_or_else(|| {
            PooledMemoryError::InvalidProvenance(format!("operation {operation_id} not found"))
        })?;

        if request.edge_type == ProvenanceEdgeType::ObservedBy
            && operation.operation_kind != OperationKind::Observe
        {
            return Err(PooledMemoryError::InvalidProvenance(
                "observed_by requires an observe operation".to_string(),
            ));
        }

        if let Some(actor_id) = &request.actor_id {
            if actor_id != &operation.requesting_actor_id {
                return Err(PooledMemoryError::InvalidProvenance(format!(
                    "actor {actor_id} does not match requesting actor for operation {operation_id}"
                )));
            }
        }

        if let Some(device_id) = &request.device_id {
            let matched = *device_id == operation.requesting_device_id
                || *device_id == operation.recording_device_id
                || *device_id == operation.recording_server_id;
            if !matched {
                return Err(PooledMemoryError::InvalidProvenance(format!(
                    "device {device_id} does not match operation {operation_id} context"
                )));
            }
        }

        Ok(())
    }

    fn fetch_operation_conn(
        conn: &rusqlite::Connection,
        operation_id: &OperationId,
    ) -> Result<Option<OperationEnvelope>, PooledMemoryError> {
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
    ) -> Result<Vec<ProvenanceEdge>, PooledMemoryError> {
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
    ) -> Result<(), PooledMemoryError> {
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
                return Err(PooledMemoryError::InvalidProvenance(format!(
                    "supersedes_edge_id {supersedes_edge_id} does not exist"
                )));
            }
        }
        Ok(())
    }

    fn record_provenance_edge_conn(
        conn: &rusqlite::Connection,
        request: ProvenanceEdgeRequest,
    ) -> Result<ProvenanceEdge, PooledMemoryError> {
        Self::validate_item_ref(&request.source)?;
        Self::validate_item_ref(&request.target)?;

        if request.source == request.target {
            return Err(PooledMemoryError::InvalidProvenance(
                "self-referential provenance edge is forbidden".to_string(),
            ));
        }

        if let (Some(from), Some(to)) = (request.valid_from, request.valid_to) {
            if to < from {
                return Err(PooledMemoryError::InvalidAsOf(
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
                PooledMemoryError::InvalidProvenance(format!(
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
            return Err(PooledMemoryError::IdempotencyConflict(format!(
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
    ) -> Result<ProvenanceEdge, PooledMemoryError> {
        let mut conn = self.pool_conn.lock().await;
        let tx = conn.transaction()?;
        let edge = Self::record_provenance_edge_conn(&tx, request)?;
        tx.commit()?;
        Ok(edge)
    }

    pub async fn record_provenance_edges(
        &self,
        requests: &[ProvenanceEdgeRequest],
    ) -> Result<Vec<ProvenanceEdge>, PooledMemoryError> {
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
    ) -> Result<Option<ProvenanceEdge>, PooledMemoryError> {
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
    ) -> Result<Vec<ProvenanceEdge>, PooledMemoryError> {
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
        let mut rows = stmt.query(rusqlite::params_from_iter(args.into_iter()))?;
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
    ) -> Result<LineageResult, PooledMemoryError> {
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
    ) -> Result<(OperationEnvelope, Vec<ProvenanceEdge>), PooledMemoryError> {
        let operation = self
            .get_operation(operation_id)
            .await?
            .ok_or_else(|| PooledMemoryError::ProvenanceEdgeNotFound(operation_id.to_string()))?;

        let as_of_recorded_str = as_of
            .recorded_at_or_before
            .clone()
            .unwrap_or_else(|| Utc::now().to_rfc3339());
        let as_of_recorded = Self::parse_rfc3339(&as_of_recorded_str, "as_of_recorded")?;
        let operation_recorded =
            Self::parse_rfc3339(&operation.recorded_at, "operation recorded_at")?;
        if operation_recorded > as_of_recorded {
            return Err(PooledMemoryError::ProvenanceEdgeNotFound(format!(
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
    ) -> Result<ProvenanceEdge, PooledMemoryError> {
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
    ) -> Result<ProvenanceEdge, PooledMemoryError> {
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
    ) -> Result<LineageResult, PooledMemoryError> {
        self.lineage(item, GraphDirection::Both, usize::MAX / 2, as_of)
            .await
    }
}
#[cfg(test)]
mod tests {
    use super::*;
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
        // Verify the underlying semantic-memory store is accessible
        let stats = store.memory().stats().await.unwrap();
        assert_eq!(stats.total_facts, 0);
    }
}
