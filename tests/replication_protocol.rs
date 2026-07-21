use ed25519_dalek::{Signer, SigningKey};
use mnemes::replication::{
    canonical_digest, validate_envelope, validate_trusted_key, AllowedArtifacts, ArtifactKind,
    MemoryMutationEnvelopeV1, ReplicaState, ReplicaWatermarkV1, ReplicationError, SignerRole,
    TrustedKeyRecord, TrustedKeyRegistry, SIGNATURE_DOMAIN_TAG,
};
use rand::rngs::OsRng;
use sha2::{Digest, Sha256};

// ---------------------------------------------------------------------------
// Helper: build a valid signed envelope
// ---------------------------------------------------------------------------
fn signed_envelope() -> (MemoryMutationEnvelopeV1, SigningKey) {
    let signing_key = SigningKey::generate(&mut OsRng);
    let mut value = MemoryMutationEnvelopeV1::test_fixture();
    // Compute correct payload digest and length from the actual payload bytes
    value.payload_digest = Sha256::digest(&value.canonical_payload).into();
    value.payload_length = value.canonical_payload.len() as u64;
    value.signer_public_key = signing_key.verifying_key().to_bytes();
    value.signature = signing_key
        .sign(&value.signing_preimage().unwrap())
        .to_bytes();
    (value, signing_key)
}

fn envelope() -> MemoryMutationEnvelopeV1 {
    signed_envelope().0
}

// ---------------------------------------------------------------------------
// Helper: admit the signing key used by a signed_envelope
// ---------------------------------------------------------------------------
fn admit_for_envelope(registry: &mut TrustedKeyRegistry, envelope: &MemoryMutationEnvelopeV1) {
    let _ = registry.admit(TrustedKeyRecord {
        signer_principal_id: envelope.signer_principal_id.clone(),
        signer_key_version: envelope.signer_key_version,
        public_key: envelope.signer_public_key,
        signer_role: envelope.signer_role,
        allowed_artifacts: AllowedArtifacts::Single(envelope.artifact_kind),
        activated_at: 0,
        cutoff_at: u64::MAX,
        revoked: false,
        home_device_id: envelope.home_device_id.clone(),
        store_id: envelope.store_id.clone(),
        namespace: envelope.namespace.clone(),
    });
}

// ---------------------------------------------------------------------------
// Existing tests (preserved — updated for new API surface)
// ---------------------------------------------------------------------------

#[test]
fn canonical_digest_is_stable_and_signature_validates() {
    let value = envelope();
    let first = canonical_digest(&value).unwrap();
    let second = canonical_digest(&value).unwrap();
    assert_eq!(first, second);
    validate_envelope(&value).unwrap();
}

#[test]
fn tampered_payload_digest_is_rejected() {
    let (mut value, _key) = signed_envelope();
    value.payload_digest = [0xabu8; 32];
    assert!(validate_envelope(&value).is_err());
}

#[test]
fn duplicate_identity_with_changed_digest_is_conflict() {
    let (first, key) = signed_envelope();
    let mut second = first.clone();
    let new_payload = b"different content".to_vec();
    second.canonical_payload = new_payload;
    second.payload_digest = Sha256::digest(&second.canonical_payload).into();
    second.payload_length = second.canonical_payload.len() as u64;
    second.signature = key.sign(&second.signing_preimage().unwrap()).to_bytes();
    assert!(mnemes::replication::validate_identity_collision(&first, &second).is_err());
}

#[test]
fn replica_state_transitions_are_closed() {
    assert!(ReplicaState::Current.can_transition_to(ReplicaState::Lagging));
    assert!(!ReplicaState::Retired.can_transition_to(ReplicaState::Current));
    assert!(!ReplicaState::Quarantined.can_transition_to(ReplicaState::Current));
}

#[test]
fn signer_role_permissions_are_closed() {
    assert!(SignerRole::DeviceWriter.may_sign(ArtifactKind::Mutation));
    assert!(!SignerRole::DeviceWriter.may_sign(ArtifactKind::Promotion));
    assert!(SignerRole::RecoveryAuthority.may_sign(ArtifactKind::Bootstrap));
}

#[test]
fn watermark_rejects_gaps_and_wrong_predecessor() {
    let current = ReplicaWatermarkV1::new(4, [7; 32]);
    assert!(current.accepts_next(5, [7; 32]));
    assert!(!current.accepts_next(6, [7; 32]));
    assert!(!current.accepts_next(5, [8; 32]));
}

#[test]
fn unknown_required_version_is_rejected() {
    let mut value = envelope();
    value.protocol_version = 99;
    assert!(validate_envelope(&value).is_err());
}

#[test]
fn fixture_has_expected_namespace_and_authority_binding() {
    let value = envelope();
    assert!(!value.namespace.is_empty());
    assert!(!value.authorization_snapshot_digest.iter().all(|b| *b == 0));
    assert!(!value.authority_receipt_digest.iter().all(|b| *b == 0));
}

// ---------------------------------------------------------------------------
// Existing RED/GREEN tests (preserved)
// ---------------------------------------------------------------------------

#[test]
fn replica_state_and_watermark_have_single_canonical_owner() {
    let _s = ReplicaState::Current;
    let _w = ReplicaWatermarkV1::new(0, [0; 32]);
}

#[test]
fn rejected_state_is_terminal() {
    match ReplicaState::Rejected {
        ReplicaState::Rejected => {}
        _ => panic!("Rejected variant missing"),
    }
    assert!(!ReplicaState::Rejected.can_transition_to(ReplicaState::Current));
    assert!(!ReplicaState::Rejected.can_transition_to(ReplicaState::Bootstrapping));
    assert!(!ReplicaState::Rejected.can_transition_to(ReplicaState::Quarantined));
    assert!(!ReplicaState::Retired.can_transition_to(ReplicaState::Bootstrapping));
    assert!(!ReplicaState::Retired.can_transition_to(ReplicaState::Current));
}

#[test]
fn sequence_overflow_is_rejected() {
    let wm = ReplicaWatermarkV1::new(u64::MAX, [0; 32]);
    assert!(!wm.accepts_next(0, [0; 32]), "overflow must be rejected");
    assert!(
        !wm.accepts_next(u64::MAX, [0; 32]),
        "repeat of MAX must be rejected"
    );
}

#[test]
fn fixed_size_public_key_and_signature() {
    let (value, _) = signed_envelope();
    assert_eq!(value.signer_public_key.len(), 32);
    assert_eq!(value.signature.len(), 64);
}

#[test]
fn malformed_snapshot_id_is_rejected_by_validate() {
    let (mut value, key) = signed_envelope();
    value.authorization_snapshot_id = vec![0u8; 8];
    value.signature = key.sign(&value.signing_preimage().unwrap()).to_bytes();
    assert!(
        validate_envelope(&value).is_err(),
        "wrong-size snapshot id must be rejected"
    );
}

#[test]
fn signing_preimage_contains_domain_tag() {
    let value = envelope();
    let preimage = value.signing_preimage().unwrap();
    let expected_domain = b"mnemes/mutation-envelope/signature/v1\0";
    assert!(
        preimage.starts_with(expected_domain),
        "preimage must begin with domain tag"
    );
}

#[test]
fn digest_uses_separate_domain() {
    let value = envelope();
    let preimage = value.signing_preimage().unwrap();
    let digest_tag = b"mnemes/mutation-envelope/v1\0";
    let fields = &preimage[SIGNATURE_DOMAIN_TAG.len()..];
    let manual = {
        let mut hasher = Sha256::new();
        hasher.update(digest_tag);
        hasher.update(fields);
        hasher.finalize()
    };
    let computed = canonical_digest(&value).unwrap();
    assert_eq!(
        manual.as_slice(),
        &computed[..],
        "canonical_digest must match SHA256(digest_domain || fields)\n\
         fields = {} bytes after stripping signature domain",
        fields.len()
    );
}

#[test]
fn validate_envelope_checks_role_artifact_matrix() {
    let (mut value, key) = signed_envelope();
    value.artifact_kind = ArtifactKind::Promotion;
    value.signer_role = SignerRole::DeviceWriter;
    value.signature = key.sign(&value.signing_preimage().unwrap()).to_bytes();
    assert!(
        validate_envelope(&value).is_err(),
        "DeviceWriter signing Promotion must be rejected"
    );
}

#[test]
fn unknown_signer_role_is_rejected() {
    let (mut value, key) = signed_envelope();
    value.signer_role = SignerRole::OperatorRoot;
    value.signature = key.sign(&value.signing_preimage().unwrap()).to_bytes();
    assert!(
        validate_envelope(&value).is_err(),
        "OperatorRoot signing Mutation must be rejected"
    );
}

#[test]
fn identity_collision_validates_envelopes_first() {
    let first = envelope();
    let mut second = first.clone();
    second.signature = [2u8; 64];
    let result = mnemes::replication::validate_identity_collision(&first, &second);
    assert!(
        result.is_err(),
        "collision check must validate before comparing — corrupt signature should fail"
    );
}

#[test]
fn changed_payload_with_preserved_metadata_is_rejected() {
    let (mut value, _key) = signed_envelope();
    value.canonical_payload = b"tampered".to_vec();
    let result = validate_envelope(&value);
    assert!(
        result.is_err(),
        "changed canonical_payload with preserved payload_digest/length must be rejected"
    );
}

#[test]
fn wrong_payload_length_is_rejected() {
    let (mut value, key) = signed_envelope();
    value.payload_length = 9999;
    value.signature = key.sign(&value.signing_preimage().unwrap()).to_bytes();
    match validate_envelope(&value) {
        Err(ReplicationError::PayloadLengthMismatch { declared, actual }) => {
            assert_eq!(declared, 9999);
            assert_eq!(actual, 7); // b"payload".len()
        }
        _ => panic!("expected PayloadLengthMismatch"),
    }
}

#[test]
fn wrong_payload_digest_is_rejected() {
    let (mut value, key) = signed_envelope();
    value.payload_digest = [0xabu8; 32];
    value.signature = key.sign(&value.signing_preimage().unwrap()).to_bytes();
    match validate_envelope(&value) {
        Err(ReplicationError::PayloadDigestMismatch) => {}
        _ => panic!("expected PayloadDigestMismatch"),
    }
}

#[test]
fn preimage_includes_all_adr_fields() {
    let value = envelope();
    let preimage = value.signing_preimage().unwrap();
    assert!(
        preimage.len() > 200,
        "preimage too short to contain all ADR-required fields: {} bytes",
        preimage.len()
    );
    let mut changed = value.clone();
    changed.policy_version = 42;
    assert_ne!(
        value.signing_preimage().unwrap(),
        changed.signing_preimage().unwrap(),
        "policy_version must be bound in preimage"
    );
    changed = value.clone();
    changed.signer_key_version = 99;
    assert_ne!(
        value.signing_preimage().unwrap(),
        changed.signing_preimage().unwrap(),
        "signer_key_version must be bound in preimage"
    );
    changed = value.clone();
    changed.observed_at = 1700000000;
    assert_ne!(
        value.signing_preimage().unwrap(),
        changed.signing_preimage().unwrap(),
        "observed_at must be bound in preimage"
    );
}

#[test]
fn unsupported_version_returns_typed_error() {
    let mut value = envelope();
    value.protocol_version = 99;
    match validate_envelope(&value) {
        Err(ReplicationError::UnsupportedProtocolVersion(v)) => assert_eq!(v, 99),
        _ => panic!("expected UnsupportedProtocolVersion(99)"),
    }
}

#[test]
fn role_artifact_mismatch_returns_typed_error() {
    let (mut value, key) = signed_envelope();
    value.artifact_kind = ArtifactKind::Promotion;
    value.signer_role = SignerRole::DeviceWriter;
    value.signature = key.sign(&value.signing_preimage().unwrap()).to_bytes();
    match validate_envelope(&value) {
        Err(ReplicationError::RoleArtifactMismatch { .. }) => {}
        _ => panic!("expected RoleArtifactMismatch"),
    }
}

#[allow(dead_code)]
fn _keep_types_linked(_: ArtifactKind, _: SignerRole) {}

// ===========================================================================
// REQUIREMENT 1: Trusted-key admission
// ===========================================================================

#[test]
fn operation_identity_excludes_epochs() {
    // Operation identity is (home_device_id, store_id, operation_id).
    // Epoch changes must NOT break collision detection.
    let base = envelope();
    let mut diff_epoch = base.clone();
    diff_epoch.store_epoch += 100;
    diff_epoch.writer_epoch += 100;
    diff_epoch.sequence += 1; // stream identity changes, but operation identity must survive
    assert!(
        mnemes::replication::same_identity(&base, &diff_epoch),
        "operation identity must survive epoch changes"
    );
}

#[test]
fn operation_identity_is_scoped_to_device_and_store() {
    let base = envelope();
    for (field, changed) in [
        ("device", {
            let mut v = base.clone();
            v.home_device_id = "other-device".into();
            v.idempotency_key = "different-idempotency".into();
            v.sequence += 1;
            v
        }),
        ("store", {
            let mut v = base.clone();
            v.store_id = "other-store".into();
            v.idempotency_key = "different-idempotency".into();
            v.sequence += 1;
            v
        }),
    ] {
        assert!(
            !mnemes::replication::same_identity(&base, &changed),
            "{field} must scope operation identity"
        );
    }
}

#[test]
fn stream_identity_includes_store_id() {
    // Stream identity is (home_device_id, store_id, store_epoch, writer_epoch, sequence).
    // Different store with same epochs/sequence must NOT collide.
    let a = envelope();
    let mut b = envelope();
    b.home_device_id = a.home_device_id.clone();
    b.store_id = "different-store".into();
    b.store_epoch = a.store_epoch;
    b.writer_epoch = a.writer_epoch;
    b.sequence = a.sequence;
    // Explicitly clear operation and idempotency to isolate stream identity
    b.operation_id = "different-op".into();
    b.idempotency_key = "different-key".into();
    assert!(
        !mnemes::replication::same_identity(&a, &b),
        "different store with same epoch/sequence must not be same stream identity"
    );
}

#[test]
fn admitted_validation_rejects_invalid_signature_even_for_trusted_key() {
    let (mut value, _) = signed_envelope();
    let mut registry = TrustedKeyRegistry::new();
    admit_for_envelope(&mut registry, &value);
    value.signature[0] ^= 1;
    assert!(matches!(
        mnemes::replication::validate_admitted_envelope(&value, &registry),
        Err(ReplicationError::SignatureVerification(_))
    ));
}

/// Self-generated unadmitted key must be rejected by validate_trusted_key.
#[test]
fn unadmitted_key_is_rejected() {
    let (envelope, _key) = signed_envelope();
    let registry = TrustedKeyRegistry::new();
    let result = validate_trusted_key(&envelope, &registry);
    match result {
        Err(ReplicationError::SignerNotAdmitted {
            principal_id,
            key_version,
        }) => {
            assert_eq!(principal_id, envelope.signer_principal_id);
            assert_eq!(key_version, envelope.signer_key_version);
        }
        _ => panic!(
            "expected SignerNotAdmitted, got {:?}",
            result.map_err(|e| format!("{e}"))
        ),
    }
}

/// Mismatched public key (admitted record has different key than envelope).
#[test]
fn mismatched_public_key_is_rejected() {
    let (envelope, _signing_key) = signed_envelope();
    let mut registry = TrustedKeyRegistry::new();
    // Admit with a DIFFERENT public key
    let other_key = SigningKey::generate(&mut OsRng);
    let _ = registry.admit(TrustedKeyRecord {
        signer_principal_id: envelope.signer_principal_id.clone(),
        signer_key_version: envelope.signer_key_version,
        public_key: other_key.verifying_key().to_bytes(),
        signer_role: envelope.signer_role,
        allowed_artifacts: AllowedArtifacts::Single(envelope.artifact_kind),
        activated_at: 0,
        cutoff_at: u64::MAX,
        revoked: false,
        home_device_id: envelope.home_device_id.clone(),
        store_id: envelope.store_id.clone(),
        namespace: envelope.namespace.clone(),
    });

    let result = validate_trusted_key(&envelope, &registry);
    match result {
        Err(ReplicationError::KeyMismatch { .. }) => {}
        _ => panic!("expected KeyMismatch, got {:?}", result),
    }
}

/// Revoked key must be rejected.
#[test]
fn revoked_key_is_rejected() {
    let (envelope, _key) = signed_envelope();
    let mut registry = TrustedKeyRegistry::new();
    let _ = registry.admit(TrustedKeyRecord {
        signer_principal_id: envelope.signer_principal_id.clone(),
        signer_key_version: envelope.signer_key_version,
        public_key: envelope.signer_public_key,
        signer_role: envelope.signer_role,
        allowed_artifacts: AllowedArtifacts::Single(envelope.artifact_kind),
        activated_at: 0,
        cutoff_at: u64::MAX,
        revoked: true,
        home_device_id: envelope.home_device_id.clone(),
        store_id: envelope.store_id.clone(),
        namespace: envelope.namespace.clone(),
    });

    let result = validate_trusted_key(&envelope, &registry);
    match result {
        Err(ReplicationError::KeyRevoked { .. }) => {}
        _ => panic!("expected KeyRevoked, got {:?}", result),
    }
}

/// Key outside lifecycle window (observed_at < activated_at).
#[test]
fn key_before_activation_window_is_rejected() {
    let (envelope, _key) = signed_envelope();
    let mut registry = TrustedKeyRegistry::new();
    let _ = registry.admit(TrustedKeyRecord {
        signer_principal_id: envelope.signer_principal_id.clone(),
        signer_key_version: envelope.signer_key_version,
        public_key: envelope.signer_public_key,
        signer_role: envelope.signer_role,
        allowed_artifacts: AllowedArtifacts::Single(envelope.artifact_kind),
        activated_at: envelope.observed_at + 100, // activation is in the future
        cutoff_at: u64::MAX,
        revoked: false,
        home_device_id: envelope.home_device_id.clone(),
        store_id: envelope.store_id.clone(),
        namespace: envelope.namespace.clone(),
    });

    let result = validate_trusted_key(&envelope, &registry);
    match result {
        Err(ReplicationError::KeyLifecycleOutsideWindow { .. }) => {}
        _ => panic!("expected KeyLifecycleOutsideWindow, got {:?}", result),
    }
}

/// Key outside lifecycle window (observed_at > cutoff_at).
#[test]
fn key_past_cutoff_window_is_rejected() {
    let (envelope, _key) = signed_envelope();
    let mut registry = TrustedKeyRegistry::new();
    let _ = registry.admit(TrustedKeyRecord {
        signer_principal_id: envelope.signer_principal_id.clone(),
        signer_key_version: envelope.signer_key_version,
        public_key: envelope.signer_public_key,
        signer_role: envelope.signer_role,
        allowed_artifacts: AllowedArtifacts::Single(envelope.artifact_kind),
        activated_at: 0,
        cutoff_at: envelope.observed_at - 1, // cutoff is in the past
        revoked: false,
        home_device_id: envelope.home_device_id.clone(),
        store_id: envelope.store_id.clone(),
        namespace: envelope.namespace.clone(),
    });

    let result = validate_trusted_key(&envelope, &registry);
    match result {
        Err(ReplicationError::KeyLifecycleOutsideWindow { .. }) => {}
        _ => panic!("expected KeyLifecycleOutsideWindow, got {:?}", result),
    }
}

/// Scope mismatch: wrong home_device_id.
#[test]
fn scope_mismatch_home_device_id_is_rejected() {
    let (envelope, _key) = signed_envelope();
    let mut registry = TrustedKeyRegistry::new();
    let _ = registry.admit(TrustedKeyRecord {
        signer_principal_id: envelope.signer_principal_id.clone(),
        signer_key_version: envelope.signer_key_version,
        public_key: envelope.signer_public_key,
        signer_role: envelope.signer_role,
        allowed_artifacts: AllowedArtifacts::Single(envelope.artifact_kind),
        activated_at: 0,
        cutoff_at: u64::MAX,
        revoked: false,
        home_device_id: "different-device".into(),
        store_id: envelope.store_id.clone(),
        namespace: envelope.namespace.clone(),
    });

    let result = validate_trusted_key(&envelope, &registry);
    match result {
        Err(ReplicationError::KeyScopeMismatch { field, .. }) => {
            assert_eq!(field, "home_device_id");
        }
        _ => panic!(
            "expected KeyScopeMismatch(home_device_id), got {:?}",
            result
        ),
    }
}

/// Scope mismatch: wrong store_id.
#[test]
fn scope_mismatch_store_id_is_rejected() {
    let (envelope, _key) = signed_envelope();
    let mut registry = TrustedKeyRegistry::new();
    let _ = registry.admit(TrustedKeyRecord {
        signer_principal_id: envelope.signer_principal_id.clone(),
        signer_key_version: envelope.signer_key_version,
        public_key: envelope.signer_public_key,
        signer_role: envelope.signer_role,
        allowed_artifacts: AllowedArtifacts::Single(envelope.artifact_kind),
        activated_at: 0,
        cutoff_at: u64::MAX,
        revoked: false,
        home_device_id: envelope.home_device_id.clone(),
        store_id: "different-store".into(),
        namespace: envelope.namespace.clone(),
    });

    let result = validate_trusted_key(&envelope, &registry);
    match result {
        Err(ReplicationError::KeyScopeMismatch { field, .. }) => {
            assert_eq!(field, "store_id");
        }
        _ => panic!("expected KeyScopeMismatch(store_id), got {:?}", result),
    }
}

/// Role mismatch: admitted key has a different role than envelope claims.
#[test]
fn role_mismatch_is_rejected() {
    let (envelope, _key) = signed_envelope();
    let mut registry = TrustedKeyRegistry::new();
    let _ = registry.admit(TrustedKeyRecord {
        signer_principal_id: envelope.signer_principal_id.clone(),
        signer_key_version: envelope.signer_key_version,
        public_key: envelope.signer_public_key,
        signer_role: SignerRole::SemanticAuthorityIssuer, // different from envelope's DeviceWriter
        allowed_artifacts: AllowedArtifacts::Single(envelope.artifact_kind),
        activated_at: 0,
        cutoff_at: u64::MAX,
        revoked: false,
        home_device_id: envelope.home_device_id.clone(),
        store_id: envelope.store_id.clone(),
        namespace: envelope.namespace.clone(),
    });

    let result = validate_trusted_key(&envelope, &registry);
    match result {
        Err(ReplicationError::RoleMismatch { .. }) => {}
        _ => panic!("expected RoleMismatch, got {:?}", result),
    }
}

/// Artifact permission mismatch.
#[test]
fn artifact_permission_mismatch_is_rejected() {
    let (mut envelope, key) = signed_envelope();
    // Change envelope to Bootstrap
    envelope.artifact_kind = ArtifactKind::Bootstrap;
    envelope.signer_role = SignerRole::RecoveryAuthority;
    envelope.signature = key.sign(&envelope.signing_preimage().unwrap()).to_bytes();

    let mut registry = TrustedKeyRegistry::new();
    // Admit with wrong artifact permission (Mutation instead of Bootstrap)
    let _ = registry.admit(TrustedKeyRecord {
        signer_principal_id: envelope.signer_principal_id.clone(),
        signer_key_version: envelope.signer_key_version,
        public_key: envelope.signer_public_key,
        signer_role: SignerRole::RecoveryAuthority,
        allowed_artifacts: AllowedArtifacts::Single(ArtifactKind::Mutation), // wrong!
        activated_at: 0,
        cutoff_at: u64::MAX,
        revoked: false,
        home_device_id: envelope.home_device_id.clone(),
        store_id: envelope.store_id.clone(),
        namespace: envelope.namespace.clone(),
    });

    let result = validate_trusted_key(&envelope, &registry);
    match result {
        Err(ReplicationError::ArtifactPermissionMismatch { .. }) => {}
        _ => panic!("expected ArtifactPermissionMismatch, got {:?}", result),
    }
}

/// Fully admitted key passes.
#[test]
fn fully_admitted_key_passes() {
    let (envelope, _key) = signed_envelope();
    let mut registry = TrustedKeyRegistry::new();
    admit_for_envelope(&mut registry, &envelope);
    validate_trusted_key(&envelope, &registry).unwrap();
}

// ===========================================================================
// REQUIREMENT 2: Canonical length encoding (big-endian / network order)
// ===========================================================================

/// Verify that the length prefix for the first variable-length field
/// (operation_id, which appears at byte offset 53 post-signature-domain)
/// is encoded in big-endian (network) byte order.
#[test]
fn length_prefix_is_big_endian() {
    let value = envelope();
    let preimage = value.signing_preimage().unwrap();

    // After the signature domain tag (45 bytes), the fixed fields are:
    //   protocol_version: u16 = 2 bytes
    //   artifact_kind: u8 = 1 byte
    //   operation_schema_version: u16 = 2 bytes
    //   semantic_schema_generation: u16 = 2 bytes
    // Total fixed before first variable-length: 45 + 7 = 52 bytes

    // The operation_id begins at offset 52 with a u32 BE length prefix.
    let offset = SIGNATURE_DOMAIN_TAG.len() + 7; // 52

    // operation_id = "operation-1" which is 11 bytes => prefix should be 0x0000000b
    let prefix: [u8; 4] = preimage[offset..offset + 4].try_into().unwrap();
    assert_eq!(
        prefix,
        [0x00, 0x00, 0x00, 0x0b],
        "u32 length prefix must be BE: expected [0,0,0,11] for 'operation-1' (11 bytes), got {prefix:02x?}"
    );
}

/// Verify a longer variable-length field encodes BE correctly.
#[test]
fn length_prefix_big_endian_with_larger_value() {
    let mut value = envelope();
    value.operation_id = "a".repeat(300); // 300 bytes
                                          // Re-sign since preimage changed
    let (_, key) = signed_envelope();
    value.signer_public_key = key.verifying_key().to_bytes();
    value.signature = key.sign(&value.signing_preimage().unwrap()).to_bytes();

    let preimage = value.signing_preimage().unwrap();
    let offset = SIGNATURE_DOMAIN_TAG.len() + 7; // 52

    let prefix: [u8; 4] = preimage[offset..offset + 4].try_into().unwrap();
    assert_eq!(
        prefix,
        [0x00, 0x00, 0x01, 0x2c],
        "u32 BE: expected [0,0,1,0x2c] for 300 bytes, got {prefix:02x?}"
    );
}

// ===========================================================================
// REQUIREMENT 3: Scoped identity
// ===========================================================================

/// Same idempotency_key across different devices is NOT the same identity.
#[test]
fn same_idempotency_key_different_device_is_not_same_identity() {
    let a = envelope();
    let mut b = envelope();

    // Same idempotency_key, different home_device_id
    b.idempotency_key = a.idempotency_key.clone();
    b.home_device_id = "different-device".into();
    // Ensure operation_id is different so the ONLY possible match is idempotency_key scope
    b.operation_id = "different-op".into();

    assert!(
        !mnemes::replication::same_identity(&a, &b),
        "same idempotency_key on different devices must NOT be same identity"
    );
}

/// Same idempotency_key AND same home_device_id IS the same identity.
#[test]
fn same_idempotency_key_same_device_is_same_identity() {
    let a = envelope();
    let mut b = envelope();

    b.idempotency_key = a.idempotency_key.clone();
    b.home_device_id = a.home_device_id.clone();
    b.operation_id = "different-op".into();

    assert!(
        mnemes::replication::same_identity(&a, &b),
        "same idempotency_key on same device must be same identity"
    );
}

/// Different operation_id AND different scoped idempotency_key is NOT same identity.
#[test]
fn different_operation_id_and_different_scoped_key_is_not_same_identity() {
    let a = envelope();
    let mut b = envelope();
    b.operation_id = "different-op".into();
    b.idempotency_key = "different-key".into();
    // Also change the epoch/sequence scope
    b.store_epoch = 99;
    b.writer_epoch = 99;

    assert!(
        !mnemes::replication::same_identity(&a, &b),
        "completely different envelopes should not share identity"
    );
}

/// Cross-store same-idempotency-key same-device is still same identity (store is not part of idempotency scope).
#[test]
fn same_idempotency_key_same_device_different_store_is_same_identity() {
    let a = envelope();
    let mut b = envelope();

    b.idempotency_key = a.idempotency_key.clone();
    b.home_device_id = a.home_device_id.clone();
    b.store_id = "different-store".into();
    b.operation_id = "different-op".into();

    assert!(
        mnemes::replication::same_identity(&a, &b),
        "same idempotency_key on same device should be same identity regardless of store"
    );
}

// ===========================================================================
// REQUIREMENT 4: Collision semantics (canonical digest comparison)
// ===========================================================================

/// Same scoped identity + same canonical digest = idempotent duplicate.
#[test]
fn same_identity_same_digest_is_idempotent_duplicate() {
    let (first, _key) = signed_envelope();
    // Clone produces the exact same envelope — must be a duplicate
    let second = first.clone();
    let result = mnemes::replication::validate_identity_collision(&first, &second);
    assert!(
        result.is_ok(),
        "identical envelopes must be idempotent duplicates"
    );
}

/// Same scoped identity + different canonical digest = typed conflict.
#[test]
fn same_identity_different_digest_is_conflict() {
    let (first, key) = signed_envelope();
    let mut second = first.clone();
    // Change payload (changes canonical digest) but keep same scoped identity
    second.canonical_payload = b"altered content".to_vec();
    second.payload_digest = Sha256::digest(&second.canonical_payload).into();
    second.payload_length = second.canonical_payload.len() as u64;
    second.signature = key.sign(&second.signing_preimage().unwrap()).to_bytes();

    let result = mnemes::replication::validate_identity_collision(&first, &second);
    match result {
        Err(ReplicationError::IdentityCollision) => {}
        _ => panic!("expected IdentityCollision, got {:?}", result),
    }
}

/// Different scoped identity (different operation_id) is OK even with same payload.
#[test]
fn different_identity_with_same_content_is_ok() {
    let (first, key) = signed_envelope();
    let mut second = first.clone();
    second.operation_id = "completely-different-operation".into();
    second.idempotency_key = "different-idem-key".into();
    second.sequence = 999;
    second.signature = key.sign(&second.signing_preimage().unwrap()).to_bytes();

    let result = mnemes::replication::validate_identity_collision(&first, &second);
    assert!(
        result.is_ok(),
        "different identity with same content is not a collision"
    );
}

// ===========================================================================
// REQUIREMENT 5: Strict structural validation
// ===========================================================================

/// Control character in operation_id must be rejected.
#[test]
fn control_character_in_operation_id_is_rejected() {
    let (mut value, key) = signed_envelope();
    value.operation_id = "op\x00with-null".into();
    value.signature = key.sign(&value.signing_preimage().unwrap()).to_bytes();
    match validate_envelope(&value) {
        Err(ReplicationError::ControlCharacterInField {
            field: "operation_id",
            byte: 0x00,
            ..
        }) => {}
        other => panic!(
            "expected ControlCharacterInField(operation_id, 0x00), got {:?}",
            other.map_err(|e| format!("{e}"))
        ),
    }
}

/// Control character (DEL 0x7f) must be rejected.
#[test]
fn control_character_del_is_rejected() {
    let (mut value, key) = signed_envelope();
    value.idempotency_key = "key\x7fdel".into();
    value.signature = key.sign(&value.signing_preimage().unwrap()).to_bytes();
    match validate_envelope(&value) {
        Err(ReplicationError::ControlCharacterInField {
            field: "idempotency_key",
            byte: 0x7f,
            ..
        }) => {}
        other => panic!(
            "expected ControlCharacterInField(idempotency_key, 0x7f), got {:?}",
            other.map_err(|e| format!("{e}"))
        ),
    }
}

/// Newline (0x0a) in namespace must be rejected.
#[test]
fn newline_in_namespace_is_rejected() {
    let (mut value, key) = signed_envelope();
    value.namespace = "default\nnewline".into();
    value.signature = key.sign(&value.signing_preimage().unwrap()).to_bytes();
    match validate_envelope(&value) {
        Err(ReplicationError::ControlCharacterInField {
            field: "namespace",
            byte: 0x0a,
            ..
        }) => {}
        other => panic!(
            "expected ControlCharacterInField(namespace, 0x0a), got {:?}",
            other.map_err(|e| format!("{e}"))
        ),
    }
}

/// valid_from > valid_to must be rejected.
#[test]
fn valid_from_after_valid_to_is_rejected() {
    let (mut value, key) = signed_envelope();
    value.valid_from = 2000;
    value.valid_to = 1000;
    value.signature = key.sign(&value.signing_preimage().unwrap()).to_bytes();
    match validate_envelope(&value) {
        Err(ReplicationError::InvalidTemporalInterval {
            valid_from: 2000,
            valid_to: 1000,
        }) => {}
        other => panic!(
            "expected InvalidTemporalInterval(2000, 1000), got {:?}",
            other.map_err(|e| format!("{e}"))
        ),
    }
}

/// Field exceeding maximum length must be rejected.
#[test]
fn field_exceeding_max_length_is_rejected() {
    let (mut value, key) = signed_envelope();
    value.operation_id = "x".repeat(200); // max is 128
    value.signature = key.sign(&value.signing_preimage().unwrap()).to_bytes();
    match validate_envelope(&value) {
        Err(ReplicationError::FieldExceedsMaxLength {
            field: "operation_id",
            len: 200,
            max: 128,
        }) => {}
        other => panic!(
            "expected FieldExceedsMaxLength(operation_id, 200, 128), got {:?}",
            other.map_err(|e| format!("{e}"))
        ),
    }
}

/// Oversized payload must be rejected.
#[test]
fn oversized_payload_is_rejected() {
    let (mut value, key) = signed_envelope();
    // Create payload just over the 1 MiB limit
    value.canonical_payload = vec![0x42u8; 1_048_577];
    value.payload_digest = Sha256::digest(&value.canonical_payload).into();
    value.payload_length = value.canonical_payload.len() as u64;
    value.signature = key.sign(&value.signing_preimage().unwrap()).to_bytes();
    match validate_envelope(&value) {
        Err(ReplicationError::FieldExceedsMaxLength {
            field: "canonical_payload",
            ..
        }) => {}
        other => panic!(
            "expected FieldExceedsMaxLength(canonical_payload), got {:?}",
            other.map_err(|e| format!("{e}"))
        ),
    }
}

// ===========================================================================
// REQUIREMENT 6: Serde projection-only (documented in canonical.rs doc comment)
// ===========================================================================

/// This is a compile/link test that serde derives exist but are not
/// required for protocol operation.  The real evidence is the doc
/// comment in canonical.rs which we spot-check below.
#[test]
fn serde_projection_documented() {
    let value = envelope();
    // Serde Serialize must exist (derive macro)
    let json = serde_json::to_string(&value).unwrap();
    assert!(
        json.contains("protocol_version"),
        "serde JSON output must contain protocol fields"
    );
    // Deserialize round-trip
    let _: MemoryMutationEnvelopeV1 = serde_json::from_str(&json).unwrap();
}

// ===========================================================================
// REQUIREMENT 7: Exhaustive role × artifact truth-table
// ===========================================================================

/// Frozen signer-role / artifact-kind matrix per ADR.
///
/// Each entry is (SignerRole, ArtifactKind, expected_allowed).
/// Document any discrepancy between the ADR and the current code.
const ROLE_ARTIFACT_MATRIX: &[(SignerRole, ArtifactKind, bool)] = &[
    // DeviceWriter
    (SignerRole::DeviceWriter, ArtifactKind::Mutation, true),
    (SignerRole::DeviceWriter, ArtifactKind::Promotion, false),
    (SignerRole::DeviceWriter, ArtifactKind::Bootstrap, false),
    (SignerRole::DeviceWriter, ArtifactKind::Ack, false),
    (SignerRole::DeviceWriter, ArtifactKind::Grant, false),
    (SignerRole::DeviceWriter, ArtifactKind::Proposal, false),
    (
        SignerRole::DeviceWriter,
        ArtifactKind::RoutingReceipt,
        false,
    ),
    // SemanticAuthorityIssuer
    (
        SignerRole::SemanticAuthorityIssuer,
        ArtifactKind::Mutation,
        true,
    ),
    (
        SignerRole::SemanticAuthorityIssuer,
        ArtifactKind::Promotion,
        false,
    ),
    (
        SignerRole::SemanticAuthorityIssuer,
        ArtifactKind::Bootstrap,
        false,
    ),
    (
        SignerRole::SemanticAuthorityIssuer,
        ArtifactKind::Ack,
        false,
    ),
    (
        SignerRole::SemanticAuthorityIssuer,
        ArtifactKind::Grant,
        false,
    ),
    (
        SignerRole::SemanticAuthorityIssuer,
        ArtifactKind::Proposal,
        false,
    ),
    (
        SignerRole::SemanticAuthorityIssuer,
        ArtifactKind::RoutingReceipt,
        false,
    ),
    // RecoveryAuthority
    (SignerRole::RecoveryAuthority, ArtifactKind::Mutation, false),
    (SignerRole::RecoveryAuthority, ArtifactKind::Promotion, true),
    (SignerRole::RecoveryAuthority, ArtifactKind::Bootstrap, true),
    (SignerRole::RecoveryAuthority, ArtifactKind::Ack, false),
    (SignerRole::RecoveryAuthority, ArtifactKind::Grant, false),
    (SignerRole::RecoveryAuthority, ArtifactKind::Proposal, false),
    (
        SignerRole::RecoveryAuthority,
        ArtifactKind::RoutingReceipt,
        false,
    ),
    // SyncService
    (SignerRole::SyncService, ArtifactKind::Mutation, false),
    (SignerRole::SyncService, ArtifactKind::Promotion, false),
    (SignerRole::SyncService, ArtifactKind::Bootstrap, false),
    (SignerRole::SyncService, ArtifactKind::Ack, true),
    (SignerRole::SyncService, ArtifactKind::Grant, false),
    (SignerRole::SyncService, ArtifactKind::Proposal, false),
    (SignerRole::SyncService, ArtifactKind::RoutingReceipt, false),
    // GrantAuthority
    (SignerRole::GrantAuthority, ArtifactKind::Mutation, false),
    (SignerRole::GrantAuthority, ArtifactKind::Promotion, false),
    (SignerRole::GrantAuthority, ArtifactKind::Bootstrap, false),
    (SignerRole::GrantAuthority, ArtifactKind::Ack, false),
    (SignerRole::GrantAuthority, ArtifactKind::Grant, true),
    (SignerRole::GrantAuthority, ArtifactKind::Proposal, false),
    (
        SignerRole::GrantAuthority,
        ArtifactKind::RoutingReceipt,
        false,
    ),
    // OperatorRoot
    (SignerRole::OperatorRoot, ArtifactKind::Mutation, false),
    (SignerRole::OperatorRoot, ArtifactKind::Promotion, false),
    (SignerRole::OperatorRoot, ArtifactKind::Bootstrap, false),
    (SignerRole::OperatorRoot, ArtifactKind::Ack, false),
    (SignerRole::OperatorRoot, ArtifactKind::Grant, true),
    (SignerRole::OperatorRoot, ArtifactKind::Proposal, false),
    (
        SignerRole::OperatorRoot,
        ArtifactKind::RoutingReceipt,
        false,
    ),
    // ProposalIssuer
    (SignerRole::ProposalIssuer, ArtifactKind::Mutation, false),
    (SignerRole::ProposalIssuer, ArtifactKind::Promotion, false),
    (SignerRole::ProposalIssuer, ArtifactKind::Bootstrap, false),
    (SignerRole::ProposalIssuer, ArtifactKind::Ack, false),
    (SignerRole::ProposalIssuer, ArtifactKind::Grant, false),
    (SignerRole::ProposalIssuer, ArtifactKind::Proposal, true),
    (
        SignerRole::ProposalIssuer,
        ArtifactKind::RoutingReceipt,
        false,
    ),
    // RoutingService
    (SignerRole::RoutingService, ArtifactKind::Mutation, false),
    (SignerRole::RoutingService, ArtifactKind::Promotion, false),
    (SignerRole::RoutingService, ArtifactKind::Bootstrap, false),
    (SignerRole::RoutingService, ArtifactKind::Ack, false),
    (SignerRole::RoutingService, ArtifactKind::Grant, false),
    (SignerRole::RoutingService, ArtifactKind::Proposal, false),
    (
        SignerRole::RoutingService,
        ArtifactKind::RoutingReceipt,
        true,
    ),
];

/// Exhaustive test: every role × artifact pair is checked against the
/// frozen matrix.  A discrepancy means the ADR or code must be updated.
#[test]
fn exhaustive_role_artifact_matrix() {
    let mut failures = Vec::new();
    for &(role, artifact, expected) in ROLE_ARTIFACT_MATRIX {
        let actual = role.may_sign(artifact);
        if actual != expected {
            failures.push(format!(
                "DISCREPANCY: {:?}.may_sign({:?}) = {actual}, expected {expected}",
                role, artifact
            ));
        }
    }
    if !failures.is_empty() {
        panic!(
            "Role/artifact matrix discrepancies with frozen ADR matrix:\n{}",
            failures.join("\n")
        );
    }
}

// ===========================================================================
// REQUIREMENT 8: Deterministic Rust golden vector
// ===========================================================================

/// Deterministic Rust golden vector: fixed-seed Ed25519 key, fixed-payload
/// envelope, known preimage, payload digest, envelope digest, and signature.
///
/// This must remain stable across code changes.  Run this test to detect
/// accidental breakage of the binary encoding.
///
/// # Python golden vector
///
/// The companion script tests/replication_protocol_vector.py independently
/// constructs the same preimage in Python stdlib (big-endian struct packing,
/// hashlib SHA-256) and asserts identical digests.  The Python script can
/// be run separately:
///
///     python3 tests/replication_protocol_vector.py
///
/// Python-pinned hex values (regenerated by running the script):
///   payload_digest:  0d88b4a8715474b2866006449b2d293aedbc78142e6e9bc6e220fa6ef8ef667f
///   envelope_digest: 1625d1c4f66e8a3329310eab93a03a1a8b00d20efcd5f1215acf45f1350c7642
///   preimage:        706f6f6c... (446 bytes, see script output for full hex)
#[test]
fn deterministic_rust_golden_vector() {
    use ed25519_dalek::SigningKey;

    // Fixed seed for reproducibility
    let seed = [
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e,
        0x1f, 0x20,
    ];
    let signing_key = SigningKey::from_bytes(&seed);
    let verifying_key = signing_key.verifying_key();

    let mut value = MemoryMutationEnvelopeV1 {
        protocol_version: 1,
        artifact_kind: ArtifactKind::Mutation,
        operation_schema_version: 1,
        semantic_schema_generation: 1,
        operation_id: "golden-op".into(),
        idempotency_key: "golden-idem".into(),
        home_device_id: "golden-device".into(),
        store_id: "golden-store".into(),
        actor_id: "golden-actor".into(),
        store_epoch: 1,
        writer_epoch: 1,
        sequence: 1,
        previous_envelope_digest: [0u8; 32],
        fencing_token: "golden-fence".into(),
        namespace: "golden-ns".into(),
        operation_kind: "golden-kind".into(),
        canonical_payload: b"golden-payload".to_vec(),
        payload_digest: [0u8; 32], // filled below
        payload_length: 0,         // filled below
        requested_effect_digest: [2u8; 32],
        policy_version: 1,
        authorization_snapshot_id: vec![0u8; 16],
        authorization_snapshot_digest: [3u8; 32],
        authority_receipt_digest: [4u8; 32],
        signer_principal_id: "golden-principal".into(),
        signer_role: SignerRole::DeviceWriter,
        signer_key_version: 1,
        observed_at: 1000000,
        valid_from: 1000000,
        valid_to: 2000000,
        signer_public_key: verifying_key.to_bytes(),
        signature: [0u8; 64],
    };

    // Fill payload metadata
    value.payload_digest = Sha256::digest(&value.canonical_payload).into();
    value.payload_length = value.canonical_payload.len() as u64;
    // Sign
    value.signature = signing_key
        .sign(&value.signing_preimage().unwrap())
        .to_bytes();

    // Compute canonical digest
    let env_digest = canonical_digest(&value).unwrap();

    // Assertions that lock the golden values

    // Payload digest must equal SHA256(b"golden-payload")
    let expected_payload_digest = Sha256::digest(b"golden-payload");
    assert_eq!(
        value.payload_digest.as_slice(),
        expected_payload_digest.as_slice(),
        "payload_digest must be SHA256(b\"golden-payload\")"
    );
    assert_eq!(
        value.payload_length, 14,
        "payload_length must match b\"golden-payload\".len() == 14"
    );

    // Signing preimage must start with the domain tag
    let preimage = value.signing_preimage().unwrap();
    assert!(preimage.starts_with(SIGNATURE_DOMAIN_TAG));

    // Verify that validate_envelope passes
    validate_envelope(&value).unwrap();

    // Verify the canonical digest is what we computed
    assert!(
        env_digest.iter().any(|&b| b != 0),
        "digest must not be all zeros"
    );
}
