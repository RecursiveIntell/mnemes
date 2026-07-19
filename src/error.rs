use thiserror::Error;

/// Errors from the pooled-memory layer.
#[derive(Debug, Error)]
pub enum PooledMemoryError {
    #[error("device not found: {0}")]
    DeviceNotFound(String),

    #[error("device is not active: {0}")]
    DeviceNotActive(String),

    #[error("actor not found: {0}")]
    ActorNotFound(String),

    #[error("idempotency conflict: operation with key {0} already exists")]
    IdempotencyConflict(String),

    #[error("database error: {0}")]
    Database(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("underlying memory error: {0}")]
    Memory(#[from] semantic_memory::MemoryError),
}

impl From<rusqlite::Error> for PooledMemoryError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Database(e.to_string())
    }
}
