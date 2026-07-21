//! Versioned protocol primitives for device-owned replication.
mod canonical;
mod error;
mod state_machine;
mod trusted_key;
mod types;

pub use canonical::{canonical_digest, validate_envelope};
pub use error::ReplicationError;
pub use state_machine::{validate_identity_collision, ReplicaState, ReplicaWatermarkV1};
pub use trusted_key::{
    validate_admitted_envelope, validate_trusted_key, AllowedArtifacts, TrustedKeyRecord,
    TrustedKeyRegistry,
};
pub use types::{same_identity, ArtifactKind, MemoryMutationEnvelopeV1, SignerRole};
pub use types::{DIGEST_DOMAIN_TAG, SIGNATURE_DOMAIN_TAG};
