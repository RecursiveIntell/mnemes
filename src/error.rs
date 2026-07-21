use thiserror::Error;

/// Errors from the mnemes layer.
#[derive(Debug, Error)]
pub enum MnemesError {
    #[error("device not found: {0}")]
    DeviceNotFound(String),

    #[error("device is not active: {0}")]
    DeviceNotActive(String),

    #[error("invalid device credential")]
    InvalidCredential,

    #[error("authorization denied: {0}")]
    AuthorizationDenied(String),

    #[error("bootstrap rejected: {0}")]
    BootstrapRejected(String),

    #[error("actor not found: {0}")]
    ActorNotFound(String),

    #[error("idempotency conflict: operation with key {0} already exists")]
    IdempotencyConflict(String),

    #[error("database error: {0}")]
    Database(String),

    #[error("provenance edge not found: {0}")]
    ProvenanceEdgeNotFound(String),

    #[error("invalid provenance request: {0}")]
    InvalidProvenance(String),

    #[error("invalid as-of filter: {0}")]
    InvalidAsOf(String),

    #[error("invalid shard catalog: {0}")]
    InvalidShardCatalog(String),

    #[error("legacy global memory store present in active runtime tree: {0}")]
    LegacyGlobalStorePresent(String),

    #[error("conflicting content for canonical shard item {item_id}")]
    ConflictingShardItem { item_id: String },

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("replication error: {0}")]
    Replication(String),

    #[error("underlying memory error: {0}")]
    Memory(#[from] semantic_memory::MemoryError),
}

impl From<rusqlite::Error> for MnemesError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Database(e.to_string())
    }
}
