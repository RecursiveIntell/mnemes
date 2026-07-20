//! Pooled multi-device memory server.
//!
//! A standalone crate that adds device/actor identity, idempotent operation
//! envelopes, and cross-device provenance on top of `semantic-memory`.
//! Devices that prefer local-only memory can use `semantic-memory` directly
//! without this crate.

pub mod error;
#[cfg(feature = "server")]
pub mod server;
pub mod store;
pub mod types;

pub use error::PooledMemoryError;
pub use store::PooledMemoryStore;
pub use types::*;

pub use semantic_memory;
