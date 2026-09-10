//! Mnemes — multi-device memory control plane.
//!
//! A standalone crate that adds device/actor identity, idempotent operation
//! envelopes, and cross-device provenance on top of `semantic-memory`.
//! Devices that prefer local-only memory can use `semantic-memory` directly
//! without this crate.

pub mod error;
pub mod profile_store;
pub mod replica;
pub mod replication;
pub mod run_pack;
#[cfg(feature = "server")]
pub mod server;
pub mod shards;
pub mod store;
pub mod sync;
pub mod sync_handler;
pub mod types;

pub use error::MnemesError;
pub use profile_store::{
    authorize_memory_access, MemoryAccessEffect, MemoryAccessGrant, MemoryGrantId, MemoryProfile,
    MemoryProfileId, MemoryProfileStatus, MemoryStoreIdentity, MemoryStoreStatus,
};
pub use run_pack::RunPackEvidenceProjectionV1;
pub use shards::*;
pub use store::{
    FactCreateAckRecord, FactCreateAdmission, FactSupersedeAckRecord, FactSupersedeAdmission,
    MnemesStore,
};
pub use types::*;

pub use semantic_memory;
