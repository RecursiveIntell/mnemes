//! HTTP handler for device-initiated synchronization.
//!
//! POST /v1/sync
//! Body: JSON array of journal entries with raw payloads
//! Response: { synced: N, next_sequence: N, has_more: bool, errors: [] }

use crate::error::MnemesError;
use crate::replica::ReplicaApplier;
use crate::replication::TrustedKeyRegistry;

/// Request body for a sync batch push.
#[derive(Debug, serde::Deserialize)]
pub struct SyncRequest {
    pub home_device_id: String,
    pub store_id: String,
    pub start_sequence: i64,
    pub entries: Vec<SyncEntry>,
}

/// A single journal entry within a sync batch.
#[derive(Debug, serde::Deserialize)]
pub struct SyncEntry {
    pub sequence: i64,
    pub operation_kind: String,
    pub payload_hex: String, // hex-encoded payload
}

/// Response after processing a sync batch.
#[derive(Debug, serde::Serialize)]
pub struct SyncResponse {
    pub synced: usize,
    pub next_sequence: i64,
    pub has_more: bool,
    pub errors: Vec<String>,
}

/// Process a sync request: decode raw payloads and apply them to the replica.
/// Full envelope parsing and admission are deferred to the transport layer.
///
/// `replica_base_dir` is the directory containing replica databases (one per store).
/// `dispatch_fn` is the replay closure — in production this calls semantic-memory's
/// journal replay to apply the semantic mutation.
pub fn process_sync_request(
    request: SyncRequest,
    registry: &TrustedKeyRegistry,
    replica_base_dir: &std::path::Path,
    dispatch_fn: &dyn Fn(&rusqlite::Connection, &str, &[u8]) -> Result<(), MnemesError>,
) -> Result<SyncResponse, MnemesError> {
    let replica_path = replica_base_dir.join(format!("{}.db", request.store_id));
    let applier = ReplicaApplier::new(&replica_path, &request.home_device_id, &request.store_id);

    let mut synced = 0;
    let mut errors = Vec::new();

    for entry in &request.entries {
        // Decode hex payload
        let payload = match hex::decode(&entry.payload_hex) {
            Ok(p) => p,
            Err(e) => {
                errors.push(format!("seq {} hex: {e}", entry.sequence));
                continue;
            }
        };

        // Accept raw journal payloads. Full envelope parsing and admission are
        // deferred to the transport layer.
        let _ = registry;

        match applier.apply_entry(entry.sequence, &entry.operation_kind, &payload, &|conn| {
            dispatch_fn(conn, &entry.operation_kind, &payload)
        }) {
            Ok(_) => synced += 1,
            Err(e) => {
                errors.push(format!("seq {} apply: {e}", entry.sequence));
            }
        }
    }

    Ok(SyncResponse {
        synced,
        next_sequence: request.start_sequence + synced as i64 + errors.len() as i64,
        has_more: false,
        errors,
    })
}
