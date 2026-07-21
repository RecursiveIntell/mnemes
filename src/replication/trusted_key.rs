use super::canonical::validate_envelope;
use super::error::ReplicationError;
use super::types::{ArtifactKind, MemoryMutationEnvelopeV1, SignerRole};

/// Closed, typed artifact permission carried by one admitted key record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllowedArtifacts {
    Single(ArtifactKind),
    RecoveryBootstrapAndPromotion,
}

impl AllowedArtifacts {
    pub const fn allows(self, artifact: ArtifactKind) -> bool {
        match self {
            Self::Single(allowed) => allowed as u8 == artifact as u8,
            Self::RecoveryBootstrapAndPromotion => {
                matches!(artifact, ArtifactKind::Bootstrap | ArtifactKind::Promotion)
            }
        }
    }
}

/// A non-self-authorizing in-memory trusted-key record binding the
/// signer principal, key version, public key, role, artifact permission,
/// lifecycle timestamps, revocation flag, and device/store/namespace scope.
///
/// Keys are admitted *out of band* (operator action, provisioning).
/// An envelope that carries a self-generated key not found in the
/// registry MUST be rejected by `validate_trusted_key`.
#[derive(Debug, Clone)]
pub struct TrustedKeyRecord {
    /// Principal identifier bound to this key.
    pub signer_principal_id: String,
    /// Key version (monotonically increasing within the principal).
    pub signer_key_version: u64,
    /// Raw Ed25519 public key bytes (32 bytes).
    pub public_key: [u8; 32],
    /// The signer role this key is authorised for.
    pub signer_role: SignerRole,
    /// The artifact kind this key is permitted to sign.
    pub allowed_artifacts: AllowedArtifacts,
    /// Lifecycle: key is valid only when `observed_at >= activated_at`.
    pub activated_at: u64,
    /// Lifecycle: key is valid only when `observed_at <= cutoff_at`.
    pub cutoff_at: u64,
    /// Explicit revocation supersedes the lifecycle window.
    pub revoked: bool,
    /// Scope: home_device_id this key is bound to.
    pub home_device_id: String,
    /// Scope: store_id this key is bound to.
    pub store_id: String,
    /// Scope: namespace this key is bound to.
    pub namespace: String,
}

/// In-memory trusted-key registry.
///
/// Admitted-key records are added via `admit`. Lookup is by
/// `(signer_principal_id, signer_key_version)`.  This is a test-only
/// registry — production deployments MUST use a persistent store.
#[derive(Debug, Clone)]
pub struct TrustedKeyRegistry {
    keys: Vec<TrustedKeyRecord>,
}

impl TrustedKeyRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self { keys: Vec::new() }
    }

    /// Admit a key record.
    pub fn try_admit(&mut self, record: TrustedKeyRecord) -> Result<(), ReplicationError> {
        if self
            .lookup(&record.signer_principal_id, record.signer_key_version)
            .is_some()
        {
            return Err(ReplicationError::DuplicateTrustedKey {
                principal_id: record.signer_principal_id,
                key_version: record.signer_key_version,
            });
        }
        self.keys.push(record);
        Ok(())
    }

    /// Fallibly admit a record.
    ///
    /// Duplicate `(signer_principal_id, signer_key_version)` pairs are
    /// rejected as configuration errors and are returned to the caller rather
    /// than panicking on input or provisioning data.
    pub fn admit(&mut self, record: TrustedKeyRecord) -> Result<(), ReplicationError> {
        self.try_admit(record)
    }

    /// Look up a trusted key by principal id and key version.
    pub fn lookup(&self, principal_id: &str, key_version: u64) -> Option<&TrustedKeyRecord> {
        self.keys
            .iter()
            .find(|k| k.signer_principal_id == principal_id && k.signer_key_version == key_version)
    }
}

impl Default for TrustedKeyRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Validate that the envelope's signer is bound to an admitted trusted key.
///
/// Checks (in order):
/// 1. Key record exists for `(signer_principal_id, signer_key_version)`.
/// 2. Embedded public key matches the admitted key's public key.
/// 3. Envelope's signer role matches the admitted key's role.
/// 4. Envelope's artifact kind matches the admitted key's artifact permission.
/// 5. Admitted key is not revoked.
/// 6. Envelope's `observed_at` falls within `[activated_at, cutoff_at]`.
/// 7. Device/store/namespace scope matches between envelope and admitted key.
///
/// This function is an authorization-only predicate and MUST NOT be used as
/// an envelope admission boundary. Call [`validate_admitted_envelope`] for
/// untrusted input.
pub fn validate_trusted_key(
    envelope: &MemoryMutationEnvelopeV1,
    registry: &TrustedKeyRegistry,
) -> Result<(), ReplicationError> {
    // 1. Key record exists
    let record = registry
        .lookup(&envelope.signer_principal_id, envelope.signer_key_version)
        .ok_or_else(|| ReplicationError::SignerNotAdmitted {
            principal_id: envelope.signer_principal_id.clone(),
            key_version: envelope.signer_key_version,
        })?;

    // 2. Public key matches
    if record.public_key != envelope.signer_public_key {
        return Err(ReplicationError::KeyMismatch {
            principal_id: envelope.signer_principal_id.clone(),
            key_version: envelope.signer_key_version,
        });
    }

    // 3. Role matches
    if record.signer_role != envelope.signer_role {
        return Err(ReplicationError::RoleMismatch {
            admitted_role: record.signer_role,
            envelope_role: envelope.signer_role,
        });
    }

    // 4. Artifact permission matches
    if !record.allowed_artifacts.allows(envelope.artifact_kind) {
        return Err(ReplicationError::ArtifactPermissionMismatch {
            admitted_artifact: envelope.artifact_kind,
            envelope_artifact: envelope.artifact_kind,
        });
    }

    // 5. Not revoked
    if record.revoked {
        return Err(ReplicationError::KeyRevoked {
            principal_id: envelope.signer_principal_id.clone(),
            key_version: envelope.signer_key_version,
        });
    }

    // 6. Lifecycle window
    if envelope.observed_at < record.activated_at || envelope.observed_at > record.cutoff_at {
        return Err(ReplicationError::KeyLifecycleOutsideWindow {
            principal_id: envelope.signer_principal_id.clone(),
            key_version: envelope.signer_key_version,
            observed_at: envelope.observed_at,
            activated_at: record.activated_at,
            cutoff_at: record.cutoff_at,
        });
    }

    // 7. Scope checks
    if envelope.home_device_id != record.home_device_id {
        return Err(ReplicationError::KeyScopeMismatch {
            principal_id: envelope.signer_principal_id.clone(),
            key_version: envelope.signer_key_version,
            field: "home_device_id",
            envelope_value: envelope.home_device_id.clone(),
            key_scope: record.home_device_id.clone(),
        });
    }
    if envelope.store_id != record.store_id {
        return Err(ReplicationError::KeyScopeMismatch {
            principal_id: envelope.signer_principal_id.clone(),
            key_version: envelope.signer_key_version,
            field: "store_id",
            envelope_value: envelope.store_id.clone(),
            key_scope: record.store_id.clone(),
        });
    }
    if envelope.namespace != record.namespace {
        return Err(ReplicationError::KeyScopeMismatch {
            principal_id: envelope.signer_principal_id.clone(),
            key_version: envelope.signer_key_version,
            field: "namespace",
            envelope_value: envelope.namespace.clone(),
            key_scope: record.namespace.clone(),
        });
    }

    Ok(())
}

/// Safely admit an envelope from an untrusted boundary.
///
/// Structural, payload, and cryptographic validation always precede trusted
/// registry admission. Keeping this sequencing in one API prevents callers
/// from accidentally treating a valid registry binding as a valid envelope.
pub fn validate_admitted_envelope(
    envelope: &MemoryMutationEnvelopeV1,
    registry: &TrustedKeyRegistry,
) -> Result<(), ReplicationError> {
    validate_envelope(envelope)?;
    validate_trusted_key(envelope, registry)
}
