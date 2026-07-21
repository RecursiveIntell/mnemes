//! Replica store and apply layer for device-owned replication.
//!
//! `ReplicaStore` requests a read-only SQLite connection for a device
//! semantic-memory database, providing search and query capabilities without
//! an intended write path. The current `MemoryStore` API cannot adopt that
//! pre-opened connection, so the connection is closed before `MemoryStore::open`;
//! see [`ReplicaStore::open`] for this limitation.
//! `ReplicaApplier` is the single mechanically-enforced write path that
//! replays verified journal entries onto the replica.

use crate::error::MnemesError;
use rusqlite::Connection;
use semantic_memory::{MemoryConfig, MemoryStore};
use sha2::{Digest, Sha256};
use std::path::Path;
use std::sync::Arc;

/// A read-only handle to a device replica's semantic-memory database.
///
/// Opening requests read-only access at the SQLite connection level. Because
/// `MemoryStore` currently owns its connection pool and exposes no constructor
/// for adopting a pre-opened connection, that read-only connection is discarded
/// before the inner store is opened. Consequently, this is a documented API
/// limitation rather than a complete read-only guarantee for `inner`; mutation
/// admission remains with the replica apply boundary.
#[derive(Clone)]
pub struct ReplicaStore {
    inner: Arc<MemoryStore>,
    device_id: String,
    store_id: String,
}

impl ReplicaStore {
    /// Open an existing semantic-memory database as a read-only replica.
    ///
    /// `db_path` must point to a valid semantic-memory SQLite file (schema
    /// version 18+ for journal support, earlier versions for read-only search).
    ///
    /// Returns an error if the database cannot be opened or is corrupt.
    pub fn open(
        db_path: &Path,
        device_id: &str,
        store_id: &str,
    ) -> Result<Self, MnemesError> {
        // Force read-only at the SQLite level via URI parameter
        let path_str = db_path.to_string_lossy();
        let uri = format!("file:{}?mode=ro", path_str);

        let conn = Connection::open_with_flags(
            &uri,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                | rusqlite::OpenFlags::SQLITE_OPEN_URI
                | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|e| {
            MnemesError::Replication(format!(
                "cannot open replica {}:{} at {}: {e}",
                device_id,
                store_id,
                db_path.display()
            ))
        })?;

        conn.execute_batch("PRAGMA query_only = ON;").map_err(|e| {
            MnemesError::Replication(format!("cannot set query_only on replica: {e}"))
        })?;

        let mut config = MemoryConfig::default();
        config.base_dir = db_path.parent().unwrap_or(db_path).to_path_buf();

        // Limitation: MemoryStore currently owns its pool and has no public
        // constructor accepting this pre-opened read-only connection. Keep the
        // probe/documentation here until MemoryStore exposes that constructor.
        drop(conn);
        let store = MemoryStore::open(config).map_err(|e| {
            MnemesError::Replication(format!(
                "cannot open MemoryStore for replica {}:{}: {e:?}",
                device_id, store_id
            ))
        })?;

        Ok(Self {
            inner: Arc::new(store),
            device_id: device_id.to_string(),
            store_id: store_id.to_string(),
        })
    }

    /// Access the underlying MemoryStore for search operations (read-only).
    pub fn store(&self) -> &MemoryStore {
        &self.inner
    }

    pub fn device_id(&self) -> &str {
        &self.device_id
    }

    pub fn store_id(&self) -> &str {
        &self.store_id
    }
}

/// The single write path for applying replication journal entries to a replica.
///
/// Does NOT hold a long-lived write connection. Each apply opens a fresh
/// write transaction, replays the mutation, and commits atomically.
pub struct ReplicaApplier {
    db_path: Box<Path>,
    device_id: String,
    store_id: String,
}

impl ReplicaApplier {
    /// Create an applier that targets a specific replica database.
    pub fn new(db_path: &Path, device_id: &str, store_id: &str) -> Self {
        Self {
            db_path: db_path.to_path_buf().into_boxed_path(),
            device_id: device_id.to_string(),
            store_id: store_id.to_string(),
        }
    }

    /// Apply a single verified journal entry.
    ///
    /// Opens a fresh write connection, checks for previous application
    /// (idempotent), replays the mutation, records the journal sequence,
    /// and commits atomically. Returns `AlreadyApplied` if the sequence
    /// was previously committed.
    ///
    /// **Internal API, not an admission boundary.** Callers must validate the
    /// replication envelope and perform authorization before invoking this
    /// method; it only applies an already-admitted journal entry.
    pub fn apply_entry(
        &self,
        journal_sequence: i64,
        operation_kind: &str,
        payload: &[u8],
        replay_fn: &dyn Fn(&Connection) -> Result<(), MnemesError>,
    ) -> Result<ApplyOutcome, MnemesError> {
        let conn = Connection::open(&*self.db_path).map_err(|e| {
            MnemesError::Replication(format!("cannot open replica for apply: {e}"))
        })?;

        // Check if already applied
        let stored_payload: Option<Vec<u8>> = match conn.query_row(
            "SELECT payload FROM mutation_journal
             WHERE home_device_id = ?1 AND store_id = ?2 AND sequence = ?3",
            rusqlite::params![self.device_id, self.store_id, journal_sequence],
            |row| row.get(0),
        ) {
            Ok(payload) => Some(payload),
            Err(rusqlite::Error::QueryReturnedNoRows) => None,
            Err(error) => {
                return Err(MnemesError::Replication(format!(
                    "cannot check replay state for sequence {journal_sequence}: {error}"
                )))
            }
        };

        if let Some(stored_payload) = stored_payload {
            if Sha256::digest(&stored_payload) != Sha256::digest(payload) {
                return Err(MnemesError::Replication(format!(
                    "replay payload mismatch for sequence {journal_sequence}"
                )));
            }
            return Ok(ApplyOutcome::AlreadyApplied {
                sequence: journal_sequence,
            });
        }

        let tx = conn
            .unchecked_transaction()
            .map_err(|e| MnemesError::Replication(format!("begin apply tx: {e}")))?;

        // Replay and journal recording must share the same transaction.
        replay_fn(&tx)?;

        // Record the journal entry
        tx.execute(
            "INSERT INTO mutation_journal
             (home_device_id, store_id, sequence, operation_kind, payload)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                self.device_id,
                self.store_id,
                journal_sequence,
                operation_kind,
                payload
            ],
        )
        .map_err(|e| MnemesError::Replication(format!("journal record on apply: {e}")))?;

        tx.commit()
            .map_err(|e| MnemesError::Replication(format!("commit apply tx: {e}")))?;

        Ok(ApplyOutcome::Applied {
            sequence: journal_sequence,
        })
    }

    pub fn device_id(&self) -> &str {
        &self.device_id
    }

    pub fn store_id(&self) -> &str {
        &self.store_id
    }
}

/// Result of applying a journal entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyOutcome {
    Applied { sequence: i64 },
    AlreadyApplied { sequence: i64 },
}
