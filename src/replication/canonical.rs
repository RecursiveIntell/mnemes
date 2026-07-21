//! Canonical digest computation and envelope validation.
//!
//! # Serde / wire separation
//!
//! Serde (`Serialize`/`Deserialize`) on `MemoryMutationEnvelopeV1` is a
//! **projection-only** convenience — it provides JSON/MessagePack for API
//! presentation and debugging.  It does **not** constitute a transport
//! decoder.  Strict wire decoding with truncation, overflow, unknown-field,
//! and trailing-byte rejection is a separate decoder layer that is not
//! provided here (and must be independently specified and tested when
//! introduced).
//!
//! The canonical envelope format is the binary fixed-order encoding
//! produced by `signing_preimage()`.  All cryptographic operations
//! (signature, digest) operate on this binary encoding, never on serde
//! projections.

use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use sha2::{Digest, Sha256};

use super::error::ReplicationError;
use super::types::{
    ArtifactKind, MemoryMutationEnvelopeV1, DIGEST_DOMAIN_TAG, SIGNATURE_DOMAIN_TAG,
};

/// Compute the canonical digest for a mutation envelope.
///
/// The digest is `SHA256(DIGEST_DOMAIN_TAG || field_bytes)` where
/// `field_bytes` is the deterministic fixed-order encoding of all
/// ADR-required fields (the preimage with the **digest** domain tag,
/// distinct from the signature domain tag).
pub fn canonical_digest(value: &MemoryMutationEnvelopeV1) -> Result<[u8; 32], ReplicationError> {
    let preimage = value.signing_preimage()?;
    let mut hasher = Sha256::new();
    // Strip the signature domain tag from the preimage, replace with the
    // digest domain tag.
    if preimage.len() < SIGNATURE_DOMAIN_TAG.len() {
        return Err(ReplicationError::DigestComputation(
            "preimage shorter than signature domain tag".into(),
        ));
    }
    hasher.update(DIGEST_DOMAIN_TAG);
    hasher.update(&preimage[SIGNATURE_DOMAIN_TAG.len()..]);
    Ok(hasher.finalize().into())
}

/// Cryptographically validate a mutation envelope using its embedded
/// public key.
///
/// Checks (in order):
/// 1. Protocol version is supported (currently 1 only).
/// 2. Artifact kind is recognised.
/// 3. Signer role is recognised.
/// 4. All required string/hash fields are non-empty.
/// 5. Reject control characters in string fields.
/// 6. Enforce practical byte limits.
/// 7. Validate temporal interval (valid_from <= valid_to).
/// 8. Public key and signature are correct lengths.
/// 9. Role/artifact matrix permits the signature.
/// 10. Ed25519 signature verifies against the domain-separated preimage.
///
/// Unknown versions, roles, and artifact kinds fail closed.
///
/// # Key trust
///
/// This function verifies that the envelope carries a valid Ed25519
/// signature from the *embedded* public key.  It does **not** check
/// whether the embedded key is authorised — that is a separate trusted
/// key registry lookup that callers must perform upstream:
///
/// ```text
/// let registry = ... ;                // authoritative key store
/// let trusted = registry.lookup(      // resolve by principal + version
///     envelope.signer_principal_id,
///     envelope.signer_key_version,
/// )?;
/// if trusted.key != envelope.signer_public_key {
///     return Err("key mismatch: envelope claims key X, registry says Y");
/// }
/// validate_envelope(&envelope)?;      // cryptographic check only
/// ```
pub fn validate_envelope(value: &MemoryMutationEnvelopeV1) -> Result<(), ReplicationError> {
    // -- 1. Protocol version --
    if value.protocol_version != 1 {
        return Err(ReplicationError::UnsupportedProtocolVersion(
            value.protocol_version,
        ));
    }

    // -- 2. Recognised artifact kind (fail closed on unknown) --
    match ArtifactKind::from_u8(value.artifact_kind.to_u8()) {
        Some(_) => {}
        None => {
            return Err(ReplicationError::UnknownArtifactKind(
                value.artifact_kind.to_u8(),
            ));
        }
    }

    // -- 3. Recognised signer role (fail closed on unknown) --
    match crate::replication::SignerRole::from_u8(value.signer_role as u8) {
        Some(_) => {}
        None => {
            return Err(ReplicationError::UnknownSignerRole(value.signer_role as u8));
        }
    }

    // -- 4. Required string fields non-empty --
    for (name, field) in [
        ("operation_id", value.operation_id.as_str()),
        ("idempotency_key", value.idempotency_key.as_str()),
        ("home_device_id", value.home_device_id.as_str()),
        ("store_id", value.store_id.as_str()),
        ("actor_id", value.actor_id.as_str()),
        ("namespace", value.namespace.as_str()),
        ("fencing_token", value.fencing_token.as_str()),
        ("operation_kind", value.operation_kind.as_str()),
        ("signer_principal_id", value.signer_principal_id.as_str()),
    ] {
        if field.trim().is_empty() {
            return Err(ReplicationError::EmptyField(name));
        }
    }

    // Payload must not be empty
    if value.canonical_payload.is_empty() {
        return Err(ReplicationError::EmptyPayload);
    }

    // -- 5. Reject control characters in string fields --
    for (name, field) in [
        ("operation_id", value.operation_id.as_str()),
        ("idempotency_key", value.idempotency_key.as_str()),
        ("home_device_id", value.home_device_id.as_str()),
        ("store_id", value.store_id.as_str()),
        ("actor_id", value.actor_id.as_str()),
        ("namespace", value.namespace.as_str()),
        ("fencing_token", value.fencing_token.as_str()),
        ("operation_kind", value.operation_kind.as_str()),
        ("signer_principal_id", value.signer_principal_id.as_str()),
    ] {
        if let Some((pos, byte)) = field
            .bytes()
            .enumerate()
            .find(|(_, b)| *b < 0x20 || *b == 0x7f)
        {
            return Err(ReplicationError::ControlCharacterInField {
                field: name,
                byte,
                pos,
            });
        }
    }

    // -- 6. Enforce practical byte limits for string/payload fields --
    const MAX_OPERATION_ID_LEN: usize = 128;
    const MAX_IDEMPOTENCY_KEY_LEN: usize = 128;
    const MAX_DEVICE_STORE_ACTOR_LEN: usize = 64;
    const MAX_NAMESPACE_LEN: usize = 256;
    const MAX_FENCING_TOKEN_LEN: usize = 128;
    const MAX_OPERATION_KIND_LEN: usize = 64;
    const MAX_PRINCIPAL_LEN: usize = 128;
    const MAX_PAYLOAD_LEN: usize = 1_048_576; // 1 MiB

    let byte_checks: [(&str, &str, usize); 9] = [
        ("operation_id", &value.operation_id, MAX_OPERATION_ID_LEN),
        (
            "idempotency_key",
            &value.idempotency_key,
            MAX_IDEMPOTENCY_KEY_LEN,
        ),
        (
            "home_device_id",
            &value.home_device_id,
            MAX_DEVICE_STORE_ACTOR_LEN,
        ),
        ("store_id", &value.store_id, MAX_DEVICE_STORE_ACTOR_LEN),
        ("actor_id", &value.actor_id, MAX_DEVICE_STORE_ACTOR_LEN),
        ("namespace", &value.namespace, MAX_NAMESPACE_LEN),
        ("fencing_token", &value.fencing_token, MAX_FENCING_TOKEN_LEN),
        (
            "operation_kind",
            &value.operation_kind,
            MAX_OPERATION_KIND_LEN,
        ),
        (
            "signer_principal_id",
            &value.signer_principal_id,
            MAX_PRINCIPAL_LEN,
        ),
    ];
    for (name, field, max) in &byte_checks {
        if field.len() > *max {
            return Err(ReplicationError::FieldExceedsMaxLength {
                field: name,
                len: field.len(),
                max: *max,
            });
        }
    }
    if value.canonical_payload.len() > MAX_PAYLOAD_LEN {
        return Err(ReplicationError::FieldExceedsMaxLength {
            field: "canonical_payload",
            len: value.canonical_payload.len(),
            max: MAX_PAYLOAD_LEN,
        });
    }

    // -- 7. Validate temporal interval (valid_from > valid_to is rejected)
    if value.valid_from > value.valid_to {
        return Err(ReplicationError::InvalidTemporalInterval {
            valid_from: value.valid_from,
            valid_to: value.valid_to,
        });
    }

    // -- Payload length must match declared payload_length --
    let actual_len = value.canonical_payload.len() as u64;
    if value.payload_length != actual_len {
        return Err(ReplicationError::PayloadLengthMismatch {
            declared: value.payload_length,
            actual: actual_len,
        });
    }

    // -- Payload digest must match SHA-256 of canonical payload bytes --
    let computed_digest = Sha256::digest(&value.canonical_payload);
    if computed_digest.as_slice() != value.payload_digest.as_slice() {
        return Err(ReplicationError::PayloadDigestMismatch);
    }

    // authorization_snapshot_id must be exactly 16 bytes (UUID)
    if value.authorization_snapshot_id.len() != 16 {
        return Err(ReplicationError::WrongSnapshotIdLength(
            value.authorization_snapshot_id.len(),
        ));
    }

    // -- 5. Key and signature length checks --
    if value.signer_public_key.len() != 32 {
        return Err(ReplicationError::WrongPublicKeyLength(
            value.signer_public_key.len(),
        ));
    }
    if value.signature.len() != 64 {
        return Err(ReplicationError::WrongSignatureLength(
            value.signature.len(),
        ));
    }

    // -- 6. Role / artifact matrix check --
    if !value.signer_role.may_sign(value.artifact_kind) {
        return Err(ReplicationError::RoleArtifactMismatch {
            role: value.signer_role,
            artifact: value.artifact_kind,
        });
    }

    // -- 7. Ed25519 signature verification --
    let key = VerifyingKey::from_bytes(&value.signer_public_key)
        .map_err(|e| ReplicationError::InvalidPublicKey(e.to_string()))?;
    let preimage = value.signing_preimage()?;
    key.verify(&preimage, &Signature::from_bytes(&value.signature))
        .map_err(|e| ReplicationError::SignatureVerification(e.to_string()))?;

    Ok(())
}
