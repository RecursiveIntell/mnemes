use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

use super::error::ReplicationError;

/// Domain tag for mutation envelope signing preimage.
pub const SIGNATURE_DOMAIN_TAG: &[u8; 38] = b"mnemes/mutation-envelope/signature/v1\0";

/// Domain tag for mutation envelope canonical digest.
pub const DIGEST_DOMAIN_TAG: &[u8; 28] = b"mnemes/mutation-envelope/v1\0";

// ---------------------------------------------------------------------------
// Custom serde for [u8; 64] — not natively supported by serde 1.x for N > 32
// ---------------------------------------------------------------------------

mod sig64_serde {
    use super::*;

    pub fn serialize<S>(arr: &[u8; 64], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(arr)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<[u8; 64], D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(Sig64Visitor)
    }

    struct Sig64Visitor;

    impl<'de> serde::de::Visitor<'de> for Sig64Visitor {
        type Value = [u8; 64];

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("64 bytes")
        }

        fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<[u8; 64], E> {
            if v.len() != 64 {
                return Err(E::custom(format!("expected 64 bytes, got {}", v.len())));
            }
            let mut arr = [0u8; 64];
            arr.copy_from_slice(v);
            Ok(arr)
        }

        fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<[u8; 64], A::Error> {
            let mut arr = [0u8; 64];
            for (i, elem) in arr.iter_mut().enumerate() {
                match seq.next_element::<u8>()? {
                    Some(v) => *elem = v,
                    None => {
                        return Err(serde::de::Error::custom(format!(
                            "expected 64 bytes, got {}",
                            i
                        )));
                    }
                }
            }
            Ok(arr)
        }
    }
}

// ---------------------------------------------------------------------------
// SignerRole — closed versioned enum
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum SignerRole {
    OperatorRoot = 1,
    DeviceWriter = 2,
    SemanticAuthorityIssuer = 3,
    SyncService = 4,
    GrantAuthority = 5,
    ProposalIssuer = 6,
    RecoveryAuthority = 7,
    RoutingService = 8,
}

impl SignerRole {
    /// Attempt to decode from a raw byte.  Returns None for unknown values.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::OperatorRoot),
            2 => Some(Self::DeviceWriter),
            3 => Some(Self::SemanticAuthorityIssuer),
            4 => Some(Self::SyncService),
            5 => Some(Self::GrantAuthority),
            6 => Some(Self::ProposalIssuer),
            7 => Some(Self::RecoveryAuthority),
            8 => Some(Self::RoutingService),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// ArtifactKind — closed versioned enum
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArtifactKind {
    Mutation,
    Promotion,
    Bootstrap,
    Ack,
    Grant,
    Proposal,
    RoutingReceipt,
}

impl ArtifactKind {
    /// Encode as a single byte for the wire format.
    pub fn to_u8(self) -> u8 {
        match self {
            Self::Mutation => 1,
            Self::Promotion => 2,
            Self::Bootstrap => 3,
            Self::Ack => 4,
            Self::Grant => 5,
            Self::Proposal => 6,
            Self::RoutingReceipt => 7,
        }
    }

    /// Attempt to decode from a raw byte.  Returns None for unknown values.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Mutation),
            2 => Some(Self::Promotion),
            3 => Some(Self::Bootstrap),
            4 => Some(Self::Ack),
            5 => Some(Self::Grant),
            6 => Some(Self::Proposal),
            7 => Some(Self::RoutingReceipt),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Closed signer-role / artifact matrix
// ---------------------------------------------------------------------------

impl SignerRole {
    /// Whether this role is permitted to sign the given artifact kind.
    /// This is the closed versioned matrix per the adopted contract.
    pub fn may_sign(self, artifact: ArtifactKind) -> bool {
        matches!(
            (self, artifact),
            (Self::DeviceWriter, ArtifactKind::Mutation)
                | (Self::SemanticAuthorityIssuer, ArtifactKind::Mutation)
                | (
                    Self::RecoveryAuthority,
                    ArtifactKind::Promotion | ArtifactKind::Bootstrap
                )
                | (Self::SyncService, ArtifactKind::Ack)
                | (Self::GrantAuthority, ArtifactKind::Grant)
                | (Self::OperatorRoot, ArtifactKind::Grant)
                | (Self::ProposalIssuer, ArtifactKind::Proposal)
                | (Self::RoutingService, ArtifactKind::RoutingReceipt)
        )
    }
}

// ---------------------------------------------------------------------------
// MemoryMutationEnvelopeV1 — ADR-complete versioned envelope
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryMutationEnvelopeV1 {
    // --- versioning ---
    pub protocol_version: u16,
    pub artifact_kind: ArtifactKind,
    pub operation_schema_version: u16,
    pub semantic_schema_generation: u16,

    // --- operation identity ---
    pub operation_id: String,
    pub idempotency_key: String,

    // --- lineage & authorization ---
    pub home_device_id: String,
    pub store_id: String,
    pub actor_id: String,
    pub store_epoch: u64,
    pub writer_epoch: u64,
    pub sequence: u64,
    pub previous_envelope_digest: [u8; 32],
    pub fencing_token: String,

    // --- operation scope ---
    pub namespace: String,
    pub operation_kind: String,

    // --- payload contract ---
    pub canonical_payload: Vec<u8>,
    pub payload_digest: [u8; 32],
    pub payload_length: u64,

    // --- governance ---
    pub requested_effect_digest: [u8; 32],
    pub policy_version: u64,
    pub authorization_snapshot_id: Vec<u8>, // 16-byte UUID
    pub authorization_snapshot_digest: [u8; 32],
    pub authority_receipt_digest: [u8; 32],

    // --- signer identity ---
    pub signer_principal_id: String,
    pub signer_role: SignerRole,
    pub signer_key_version: u64,

    // --- temporal ---
    pub observed_at: u64,
    pub valid_from: u64,
    pub valid_to: u64,

    // --- cryptographic ---
    pub signer_public_key: [u8; 32],
    #[serde(with = "sig64_serde")]
    pub signature: [u8; 64],
}

impl MemoryMutationEnvelopeV1 {
    /// Fixed-order deterministic binary encoding of all ADR-required fields
    /// (excluding the signature itself).  The preimage starts with the
    /// signature domain tag so that the signature is domain-separated.
    ///
    /// Encoding rules:
    /// - u8: 1 raw byte
    /// - u16: 2 bytes big-endian
    /// - u64: 8 bytes big-endian
    /// - `[u8; N]`: N raw bytes
    /// - String / Vec<u8>: u32 BE (big-endian) length prefix + bytes
    ///
    /// Returns `Err(ReplicationError::FieldTooLong)` if any variable-length
    /// field exceeds `u32::MAX`.
    pub fn signing_preimage(&self) -> Result<Vec<u8>, ReplicationError> {
        let mut out = Vec::with_capacity(512);

        // Domain tag (signature domain) — part of the fixed preimage
        out.extend_from_slice(SIGNATURE_DOMAIN_TAG);

        // Encode each field in ADR-specified order
        out.extend_from_slice(&self.protocol_version.to_be_bytes()); // 1
        out.push(self.artifact_kind.to_u8()); // 2
        out.extend_from_slice(&self.operation_schema_version.to_be_bytes()); // 3
        out.extend_from_slice(&self.semantic_schema_generation.to_be_bytes()); // 4
        Self::encode_bytes(&mut out, self.operation_id.as_bytes(), "operation_id")?; // 5
        Self::encode_bytes(&mut out, self.idempotency_key.as_bytes(), "idempotency_key")?; // 6
        Self::encode_bytes(&mut out, self.home_device_id.as_bytes(), "home_device_id")?; // 7
        Self::encode_bytes(&mut out, self.store_id.as_bytes(), "store_id")?; // 8
        Self::encode_bytes(&mut out, self.actor_id.as_bytes(), "actor_id")?; // 9
        out.extend_from_slice(&self.store_epoch.to_be_bytes()); // 10
        out.extend_from_slice(&self.writer_epoch.to_be_bytes()); // 11
        out.extend_from_slice(&self.sequence.to_be_bytes()); // 12
        out.extend_from_slice(&self.previous_envelope_digest); // 13
        Self::encode_bytes(&mut out, self.fencing_token.as_bytes(), "fencing_token")?; // 14
        Self::encode_bytes(&mut out, self.namespace.as_bytes(), "namespace")?; // 15
        Self::encode_bytes(&mut out, self.operation_kind.as_bytes(), "operation_kind")?; // 16
        out.extend_from_slice(&self.payload_digest); // 17
        out.extend_from_slice(&self.payload_length.to_be_bytes()); // 18
        out.extend_from_slice(&self.requested_effect_digest); // 19
        out.extend_from_slice(&self.policy_version.to_be_bytes()); // 20
        Self::encode_bytes(
            &mut out,
            &self.authorization_snapshot_id,
            "authorization_snapshot_id",
        )?; // 21
        out.extend_from_slice(&self.authorization_snapshot_digest); // 22
        out.extend_from_slice(&self.authority_receipt_digest); // 23
        Self::encode_bytes(
            &mut out,
            self.signer_principal_id.as_bytes(),
            "signer_principal_id",
        )?; // 24
        out.push(self.signer_role as u8); // 25
        out.extend_from_slice(&self.signer_key_version.to_be_bytes()); // 26
        out.extend_from_slice(&self.observed_at.to_be_bytes()); // 27
        out.extend_from_slice(&self.valid_from.to_be_bytes()); // 28
        out.extend_from_slice(&self.valid_to.to_be_bytes()); // 29
        Ok(out)
    }

    /// Encode bytes with a u32 BE (big-endian / network order) length prefix.
    ///
    /// Returns `FieldTooLong` if `bytes` exceeds `u32::MAX` (fail-closed,
    /// not assert).
    fn encode_bytes(
        out: &mut Vec<u8>,
        bytes: &[u8],
        field: &'static str,
    ) -> Result<(), ReplicationError> {
        let len = bytes.len();
        if len > u32::MAX as usize {
            return Err(ReplicationError::FieldTooLong { field, len });
        }
        out.extend_from_slice(&(len as u32).to_be_bytes());
        out.extend_from_slice(bytes);
        Ok(())
    }

    /// Convenience test fixture with sensible defaults.
    /// Keys and digests are zeroed — tests must set real signing keys.
    pub fn test_fixture() -> Self {
        Self {
            protocol_version: 1,
            artifact_kind: ArtifactKind::Mutation,
            operation_schema_version: 1,
            semantic_schema_generation: 1,
            operation_id: "operation-1".into(),
            idempotency_key: "idem-1".into(),
            home_device_id: "device-1".into(),
            store_id: "store-1".into(),
            actor_id: "actor-1".into(),
            store_epoch: 1,
            writer_epoch: 1,
            sequence: 1,
            previous_envelope_digest: [0u8; 32],
            fencing_token: "fence-1".into(),
            namespace: "default".into(),
            operation_kind: "fact_append".into(),
            canonical_payload: b"payload".to_vec(),
            payload_digest: [1u8; 32],
            payload_length: 7,
            requested_effect_digest: [2u8; 32],
            policy_version: 1,
            authorization_snapshot_id: vec![0u8; 16],
            authorization_snapshot_digest: [3u8; 32],
            authority_receipt_digest: [4u8; 32],
            signer_principal_id: "operator-1".into(),
            signer_role: SignerRole::DeviceWriter,
            signer_key_version: 1,
            observed_at: 1000000,
            valid_from: 1000000,
            valid_to: 2000000,
            signer_public_key: [0u8; 32],
            signature: [0u8; 64],
        }
    }
}

// ---------------------------------------------------------------------------
// Identity collision detection
// ---------------------------------------------------------------------------

/// Returns `true` if the two envelopes share any required identity scope.
///
/// Identity predicates (per ADR):
/// - `(home_device_id, store_id, operation_id)` — operation identity,
///   intentionally excludes epochs so promotion does not break collision.
/// - `(home_device_id, idempotency_key)` — scoped, never bare.
/// - `(home_device_id, store_id, store_epoch, writer_epoch, sequence)` — stream.
pub fn same_identity(a: &MemoryMutationEnvelopeV1, b: &MemoryMutationEnvelopeV1) -> bool {
    (a.operation_id == b.operation_id
        && a.home_device_id == b.home_device_id
        && a.store_id == b.store_id)
        || (a.home_device_id == b.home_device_id && a.idempotency_key == b.idempotency_key)
        || (a.home_device_id == b.home_device_id
            && a.store_id == b.store_id
            && a.store_epoch == b.store_epoch
            && a.writer_epoch == b.writer_epoch
            && a.sequence == b.sequence)
}
