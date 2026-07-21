use thiserror::Error;

/// Typed replication protocol errors.  All variants are fail-closed:
/// unknown versions/roles/artifacts, malformed fields, or structural
/// violations produce a specific variant rather than a generic string.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ReplicationError {
    #[error("unsupported protocol version: {0}")]
    UnsupportedProtocolVersion(u16),

    #[error("unknown artifact kind: {0}")]
    UnknownArtifactKind(u8),

    #[error("unknown signer role: {0}")]
    UnknownSignerRole(u8),

    #[error("empty required field: {0}")]
    EmptyField(&'static str),

    #[error("wrong public key length: got {0}, expected 32")]
    WrongPublicKeyLength(usize),

    #[error("wrong signature length: got {0}, expected 64")]
    WrongSignatureLength(usize),

    #[error("signer role {role:?} may not sign artifact {artifact:?}")]
    RoleArtifactMismatch {
        role: crate::replication::SignerRole,
        artifact: crate::replication::ArtifactKind,
    },

    #[error("invalid Ed25519 public key: {0}")]
    InvalidPublicKey(String),

    #[error("signature verification failed: {0}")]
    SignatureVerification(String),

    #[error("field too long for u32 length prefix: {field} ({len} bytes)")]
    FieldTooLong { field: &'static str, len: usize },

    #[error("authorization_snapshot_id must be 16 bytes (UUID), got {0}")]
    WrongSnapshotIdLength(usize),

    #[error("canonical payload is empty")]
    EmptyPayload,

    #[error("canonical digest computation failed: {0}")]
    DigestComputation(String),

    #[error("envelope validation failed before collision check: {0}")]
    PreCollisionValidation(String),

    #[error("identity collision with different content")]
    IdentityCollision,

    #[error("payload length mismatch: declared {declared}, actual {actual}")]
    PayloadLengthMismatch { declared: u64, actual: u64 },

    #[error("payload digest mismatch: SHA-256(canonical_payload) does not match declared payload_digest")]
    PayloadDigestMismatch,

    // ---- Trusted-key admission errors ----
    #[error(
        "signer principal {principal_id} key version {key_version} not found in trusted registry"
    )]
    SignerNotAdmitted {
        principal_id: String,
        key_version: u64,
    },

    #[error("embedded public key for {principal_id} (v{key_version}) does not match admitted key")]
    KeyMismatch {
        principal_id: String,
        key_version: u64,
    },

    #[error("admitted key for signer role {admitted_role:?} does not match envelope role {envelope_role:?}")]
    RoleMismatch {
        admitted_role: crate::replication::SignerRole,
        envelope_role: crate::replication::SignerRole,
    },

    #[error("admitted key permission {admitted_artifact:?} does not match envelope artifact {envelope_artifact:?}")]
    ArtifactPermissionMismatch {
        admitted_artifact: crate::replication::ArtifactKind,
        envelope_artifact: crate::replication::ArtifactKind,
    },

    #[error("duplicate trusted key for principal {principal_id} key version {key_version}")]
    DuplicateTrustedKey {
        principal_id: String,
        key_version: u64,
    },

    #[error("admitted key for {principal_id} (v{key_version}) has been revoked")]
    KeyRevoked {
        principal_id: String,
        key_version: u64,
    },

    #[error("admitted key for {principal_id} (v{key_version}) observed_at {observed_at} outside lifecycle window [{activated_at}, {cutoff_at}]")]
    KeyLifecycleOutsideWindow {
        principal_id: String,
        key_version: u64,
        observed_at: u64,
        activated_at: u64,
        cutoff_at: u64,
    },

    #[error("admitted key for {principal_id} (v{key_version}) scope mismatch: envelope {field}={envelope_value}, key scope={key_scope}")]
    KeyScopeMismatch {
        principal_id: String,
        key_version: u64,
        field: &'static str,
        envelope_value: String,
        key_scope: String,
    },

    // ---- Strict structural validation errors ----
    #[error("control character (0x{byte:02x}) in field '{field}' at position {pos}")]
    ControlCharacterInField {
        field: &'static str,
        byte: u8,
        pos: usize,
    },

    #[error("invalid temporal interval: valid_from ({valid_from}) > valid_to ({valid_to})")]
    InvalidTemporalInterval { valid_from: u64, valid_to: u64 },

    #[error("field '{field}' exceeds maximum length: {len} > {max}")]
    FieldExceedsMaxLength {
        field: &'static str,
        len: usize,
        max: usize,
    },

    #[error("idempotency key contains characters that are not allowed: {key}")]
    InvalidIdempotencyKey { key: String },
}
