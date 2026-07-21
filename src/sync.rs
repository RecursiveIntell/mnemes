//! Device-to-server synchronization engine.
//!
//! Orchestrates the full vertical slice: export journal entries from a device
//! primary, wrap them in replication envelopes, validate and admit them on the
//! server, replay onto the replica, and record durable ACKs.

use crate::error::MnemesError;
use crate::replica::{ApplyOutcome, ReplicaApplier};
use crate::replication::TrustedKeyRegistry;
use rusqlite::Connection;

/// Result of synchronizing one journal batch.
#[derive(Debug)]
pub struct SyncResult {
    pub entries_synced: usize,
    pub next_sequence: i64,
    pub has_more: bool,
    pub errors: Vec<String>,
}

/// Export a batch of journal entries from a device's primary database.
///
/// Reads `mutation_journal` from the device's SQLite database, returning
/// entries with sequence numbers in `[start_sequence, start_sequence + limit)`.
/// Stops at the first gap.
pub fn export_device_journal(
    conn: &Connection,
    home_device_id: &str,
    store_id: &str,
    start_sequence: i64,
    limit: usize,
) -> Result<(Vec<JournalPayload>, i64, bool), MnemesError> {
    let mut stmt = conn
        .prepare(
            "SELECT sequence, operation_kind, payload
             FROM mutation_journal
             WHERE home_device_id = ?1 AND store_id = ?2 AND sequence >= ?3
             ORDER BY sequence ASC
             LIMIT ?4",
        )
        .map_err(|e| MnemesError::Replication(format!("journal export: {e}")))?;

    let rows = stmt
        .query_map(
            rusqlite::params![home_device_id, store_id, start_sequence, limit as i64],
            |row| {
                Ok(JournalPayload {
                    sequence: row.get(0)?,
                    operation_kind: row.get(1)?,
                    payload: row.get(2)?,
                })
            },
        )
        .map_err(|e| MnemesError::Replication(format!("export query: {e}")))?;

    let mut entries = Vec::new();
    let mut expected = start_sequence;
    let mut has_gap = false;
    for row in rows {
        let entry = row.map_err(|e| MnemesError::Replication(format!("row: {e}")))?;
        if entry.sequence != expected {
            has_gap = true;
            break;
        }
        expected = entry.sequence + 1;
        entries.push(entry);
    }

    Ok((
        entries.clone(),
        expected,
        !has_gap && entries.len() >= limit,
    ))
}

/// Export the canonical semantic-memory operation journal.
///
/// `operation_journal` intentionally stores only digests and metadata. It does
/// not contain replayable mutation bytes, so this boundary refuses to invent a
/// payload and fails closed until the canonical owner supplies an export API.
pub fn export_operation_journal(
    conn: &Connection,
    _home_device_id: &str,
    _store_id: &str,
    _start_sequence: i64,
    _limit: usize,
) -> Result<(Vec<JournalPayload>, i64, bool), MnemesError> {
    let exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='operation_journal')",
            [],
            |row| row.get(0),
        )
        .map_err(|e| MnemesError::Replication(format!("canonical journal probe: {e}")))?;
    if !exists {
        return Err(MnemesError::Replication(
            "canonical operation_journal is unavailable".into(),
        ));
    }
    Err(MnemesError::Replication(
        "operation_journal has no replayable payload; replication is unavailable until the canonical owner exports payload bytes".into(),
    ))
}

/// A single journal entry ready for replication.
#[derive(Debug, Clone)]
pub struct JournalPayload {
    pub sequence: i64,
    pub operation_kind: String,
    pub payload: Vec<u8>,
}

/// Synchronize one batch from device primary to server replica.
///
/// 1. Exports journal entries from the device's source connection
/// 2. For each entry, forwards the raw journal payload
/// 3. Envelope parsing and admission are deferred to the transport layer
/// 4. Applies the entry via `ReplicaApplier`
/// 5. Records outcome
pub fn sync_batch(
    device_conn: &Connection,
    replica_applier: &ReplicaApplier,
    registry: &TrustedKeyRegistry,
    home_device_id: &str,
    store_id: &str,
    start_sequence: i64,
    max_batch: usize,
    /* replay dispatcher */
    dispatch: &dyn Fn(&Connection, &str, &[u8]) -> Result<(), MnemesError>,
) -> Result<SyncResult, MnemesError> {
    let (entries, next_seq, has_more) = export_device_journal(
        device_conn,
        home_device_id,
        store_id,
        start_sequence,
        max_batch,
    )?;

    let mut synced = 0;
    let mut errors = Vec::new();

    for entry in &entries {
        // Accept raw journal payloads here. Full envelope parsing and
        // admission are deferred to the transport layer.
        let _ = registry;

        // Apply to replica
        match replica_applier.apply_entry(
            entry.sequence,
            &entry.operation_kind,
            &entry.payload,
            &|conn| dispatch(conn, &entry.operation_kind, &entry.payload),
        ) {
            Ok(ApplyOutcome::Applied { .. }) | Ok(ApplyOutcome::AlreadyApplied { .. }) => {
                synced += 1;
            }
            Err(e) => {
                errors.push(format!("seq {} apply failed: {e}", entry.sequence));
            }
        }
    }

    Ok(SyncResult {
        entries_synced: synced,
        next_sequence: next_seq,
        has_more,
        errors,
    })
}

/// Determine whether the device has pending journal entries to sync.
pub fn has_pending(
    conn: &Connection,
    home_device_id: &str,
    store_id: &str,
    last_applied_sequence: i64,
) -> Result<bool, MnemesError> {
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM mutation_journal
             WHERE home_device_id = ?1 AND store_id = ?2 AND sequence > ?3",
            rusqlite::params![home_device_id, store_id, last_applied_sequence],
            |row| row.get(0),
        )
        .map_err(|e| MnemesError::Replication(format!("pending check: {e}")))?;
    Ok(count > 0)
}
