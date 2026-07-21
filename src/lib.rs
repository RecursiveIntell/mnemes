//! Mnemes — multi-device memory control plane.
//!
//! A standalone crate that adds device/actor identity, idempotent operation
//! envelopes, and cross-device provenance on top of `semantic-memory`.
//! Devices that prefer local-only memory can use `semantic-memory` directly
//! without this crate.

pub mod error;
pub mod replica;
pub mod replication;
#[cfg(feature = "server")]
pub mod server;
pub mod shards;
pub mod store;
pub mod sync;
pub mod sync_handler;
pub mod types;

pub use error::MnemesError;
pub use shards::*;
pub use store::MnemesStore;
pub use types::*;

pub use semantic_memory;
