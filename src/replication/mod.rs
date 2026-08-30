//! Versioned protocol primitives for device-owned replication.
mod canonical;
mod error;
mod fact_create;
mod fact_supersede;
mod state_machine;
mod trusted_key;
mod types;

pub use canonical::{canonical_digest, validate_envelope};
pub use error::ReplicationError;
pub use fact_create::{
    FactCreateTransportEntryV1, SignedFactCreateBatchV1, FACT_CREATE_TRANSPORT_MAX_ENTRIES,
    FACT_CREATE_TRANSPORT_MAX_WIRE_BYTES, FACT_CREATE_TRANSPORT_PROTOCOL_VERSION,
    FACT_CREATE_TRANSPORT_SIGNATURE_DOMAIN,
};
pub use fact_supersede::{
    FactSupersedeTransportEntryV1, SignedFactSupersedeBatchV1,
    FACT_SUPERSEDE_TRANSPORT_MAX_ENTRIES, FACT_SUPERSEDE_TRANSPORT_MAX_WIRE_BYTES,
    FACT_SUPERSEDE_TRANSPORT_PROTOCOL_FAMILY, FACT_SUPERSEDE_TRANSPORT_PROTOCOL_VERSION,
    FACT_SUPERSEDE_TRANSPORT_SIGNATURE_DOMAIN,
};
pub use state_machine::{validate_identity_collision, ReplicaState, ReplicaWatermarkV1};
pub use trusted_key::{
    validate_admitted_envelope, validate_trusted_key, AllowedArtifacts, TrustedKeyRecord,
    TrustedKeyRegistry,
};
pub use types::{same_identity, ArtifactKind, MemoryMutationEnvelopeV1, SignerRole};
pub use types::{DIGEST_DOMAIN_TAG, SIGNATURE_DOMAIN_TAG};
