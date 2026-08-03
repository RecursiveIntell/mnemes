//! Typed, signed transport profile for semantic-memory fact-create journal entries.
//!
//! This is deliberately separate from `MemoryMutationEnvelopeV1`. The latter
//! carries governed operator mutations; this profile carries the closed
//! semantic-memory V38 fact-create journal contract and must not invent
//! authority fields that the journal does not own.

use super::{ReplicationError, SignerRole};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use semantic_memory::journal::{
    validate_fact_create_replica_envelope, FactCreateReplicaEnvelopeV1, JournalEntry,
    FACT_CREATE_OPERATION, FACT_CREATE_PAYLOAD_SCHEMA, VERIFIED_RECORD_STATE,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

pub const FACT_CREATE_TRANSPORT_PROTOCOL_VERSION: u16 = 1;
pub const FACT_CREATE_TRANSPORT_MAX_WIRE_BYTES: usize = 4 * 1024 * 1024;
/// Version 1 deliberately carries exactly one record per request. The canonical
/// semantic-memory receiver commits one record, its inbox decision, and its
/// stream head in a single transaction. Multi-entry batching is deferred until
/// an authority-owned all-or-nothing batch transaction exists.
pub const FACT_CREATE_TRANSPORT_MAX_ENTRIES: usize = 1;
pub const FACT_CREATE_TRANSPORT_SIGNATURE_DOMAIN: &[u8] =
    b"mnemes/fact-create-batch/signature/v1\0";

const MAX_ID_BYTES: usize = 128;
const MAX_FENCING_TOKEN_BYTES: usize = 256;

mod sig64_serde {
    use super::*;

    pub fn serialize<S>(value: &[u8; 64], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(value)
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

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("64 bytes")
        }

        fn visit_bytes<E: serde::de::Error>(self, value: &[u8]) -> Result<[u8; 64], E> {
            if value.len() != 64 {
                return Err(E::custom(format!("expected 64 bytes, got {}", value.len())));
            }
            let mut result = [0u8; 64];
            result.copy_from_slice(value);
            Ok(result)
        }

        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut sequence: A,
        ) -> Result<[u8; 64], A::Error> {
            let mut result = [0u8; 64];
            for (index, byte) in result.iter_mut().enumerate() {
                *byte = sequence.next_element()?.ok_or_else(|| {
                    serde::de::Error::custom(format!("expected 64 bytes, got {index}"))
                })?;
            }
            if sequence.next_element::<u8>()?.is_some() {
                return Err(serde::de::Error::custom("expected exactly 64 bytes"));
            }
            Ok(result)
        }
    }
}

/// One exact semantic-memory V38 journal record as transported over HTTP.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FactCreateTransportEntryV1 {
    pub sequence: i64,
    pub payload: Vec<u8>,
    pub payload_digest: [u8; 32],
    pub predecessor_digest: [u8; 32],
    pub journal_envelope_digest: [u8; 32],
}

impl FactCreateTransportEntryV1 {
    /// Copy a verified journal row without reserializing or recomputing its
    /// canonical payload bytes.
    pub fn from_journal_entry(entry: &JournalEntry) -> Result<Self, ReplicationError> {
        if entry.operation_kind != FACT_CREATE_OPERATION
            || entry.payload_schema != FACT_CREATE_PAYLOAD_SCHEMA
            || entry.record_state != VERIFIED_RECORD_STATE
        {
            return Err(ReplicationError::PreCollisionValidation(
                "journal entry is not an admitted verified fact-create record".to_string(),
            ));
        }
        Ok(Self {
            sequence: entry.sequence,
            payload: entry.payload.clone(),
            payload_digest: entry.payload_digest,
            predecessor_digest: entry.predecessor_digest,
            journal_envelope_digest: entry.envelope_digest,
        })
    }

    fn to_semantic_envelope(
        &self,
        home_device_id: &str,
        store_id: &str,
        stream_epoch: u64,
    ) -> FactCreateReplicaEnvelopeV1 {
        FactCreateReplicaEnvelopeV1 {
            home_device_id: home_device_id.to_string(),
            store_id: store_id.to_string(),
            stream_epoch,
            sequence: self.sequence,
            operation_kind: FACT_CREATE_OPERATION.to_string(),
            payload_schema: FACT_CREATE_PAYLOAD_SCHEMA.to_string(),
            payload: self.payload.clone(),
            payload_digest: self.payload_digest,
            predecessor_digest: self.predecessor_digest,
            envelope_digest: self.journal_envelope_digest,
        }
    }
}

/// One signed batch of contiguous fact-create records.
///
/// The signer is fixed to `DeviceWriter` for this profile. The server still
/// performs persistent trusted-key admission and scope checks after this local
/// structural/signature validation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedFactCreateBatchV1 {
    pub protocol_version: u16,
    pub batch_id: String,
    pub home_device_id: String,
    pub store_id: String,
    pub stream_epoch: u64,
    pub start_sequence: i64,
    pub entries: Vec<FactCreateTransportEntryV1>,
    pub signer_principal_id: String,
    pub signer_role: SignerRole,
    pub signer_key_version: u64,
    pub observed_at: u64,
    pub fencing_token: String,
    pub signer_public_key: [u8; 32],
    #[serde(with = "sig64_serde")]
    pub signature: [u8; 64],
}

impl SignedFactCreateBatchV1 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        batch_id: impl Into<String>,
        home_device_id: impl Into<String>,
        store_id: impl Into<String>,
        stream_epoch: u64,
        start_sequence: i64,
        entries: Vec<FactCreateTransportEntryV1>,
        signer_principal_id: impl Into<String>,
        signer_key_version: u64,
        observed_at: u64,
        fencing_token: impl Into<String>,
    ) -> Result<Self, ReplicationError> {
        let batch = Self {
            protocol_version: FACT_CREATE_TRANSPORT_PROTOCOL_VERSION,
            batch_id: batch_id.into(),
            home_device_id: home_device_id.into(),
            store_id: store_id.into(),
            stream_epoch,
            start_sequence,
            entries,
            signer_principal_id: signer_principal_id.into(),
            signer_role: SignerRole::DeviceWriter,
            signer_key_version,
            observed_at,
            fencing_token: fencing_token.into(),
            signer_public_key: [0u8; 32],
            signature: [0u8; 64],
        };
        batch.validate_structure()?;
        Ok(batch)
    }

    /// Sign the exact fixed-order preimage. The key is never serialized.
    pub fn sign(&mut self, signing_key: &SigningKey) -> Result<(), ReplicationError> {
        self.validate_structure()?;
        self.signer_public_key = signing_key.verifying_key().to_bytes();
        self.signature = signing_key.sign(&self.signing_preimage()?).to_bytes();
        Ok(())
    }

    /// Decode one bounded JSON projection and reject unknown/trailing bytes.
    /// JSON is a projection only; the signature covers `signing_preimage`, not
    /// serde's field ordering or formatting.
    pub fn decode_json(bytes: &[u8]) -> Result<Self, ReplicationError> {
        if bytes.len() > FACT_CREATE_TRANSPORT_MAX_WIRE_BYTES {
            return Err(ReplicationError::FieldExceedsMaxLength {
                field: "wire_body",
                len: bytes.len(),
                max: FACT_CREATE_TRANSPORT_MAX_WIRE_BYTES,
            });
        }
        let batch: Self = serde_json::from_slice(bytes).map_err(|error| {
            ReplicationError::PreCollisionValidation(format!(
                "fact-create transport JSON decode failed: {error}"
            ))
        })?;
        batch.validate()?;
        Ok(batch)
    }

    /// Validate structure, semantic-memory's canonical journal record, and
    /// the Ed25519 signature.
    pub fn validate(&self) -> Result<(), ReplicationError> {
        self.validate_structure()?;
        for entry in &self.entries {
            let envelope =
                entry.to_semantic_envelope(&self.home_device_id, &self.store_id, self.stream_epoch);
            validate_fact_create_replica_envelope(&envelope).map_err(|error| {
                ReplicationError::PreCollisionValidation(format!(
                    "semantic-memory fact-create validation failed at sequence {}: {error}",
                    entry.sequence
                ))
            })?;
        }

        let verifying_key = VerifyingKey::from_bytes(&self.signer_public_key)
            .map_err(|error| ReplicationError::InvalidPublicKey(error.to_string()))?;
        let signature = Signature::from_bytes(&self.signature);
        verifying_key
            .verify(&self.signing_preimage()?, &signature)
            .map_err(|error| ReplicationError::SignatureVerification(error.to_string()))
    }

    /// Convert validated records to the semantic-memory receiver contract.
    pub fn semantic_envelopes(&self) -> Result<Vec<FactCreateReplicaEnvelopeV1>, ReplicationError> {
        self.validate()?;
        Ok(self
            .entries
            .iter()
            .map(|entry| {
                entry.to_semantic_envelope(&self.home_device_id, &self.store_id, self.stream_epoch)
            })
            .collect())
    }

    /// Fixed-order, length-prefixed signing preimage. The exact journal bytes
    /// are included; no semantic payload reserialization occurs here.
    pub fn signing_preimage(&self) -> Result<Vec<u8>, ReplicationError> {
        let mut output = Vec::with_capacity(512 + self.entries.len() * 160);
        output.extend_from_slice(FACT_CREATE_TRANSPORT_SIGNATURE_DOMAIN);
        output.extend_from_slice(&self.protocol_version.to_be_bytes());
        encode_string(&mut output, &self.batch_id, "batch_id")?;
        encode_string(&mut output, &self.home_device_id, "home_device_id")?;
        encode_string(&mut output, &self.store_id, "store_id")?;
        output.extend_from_slice(&self.stream_epoch.to_be_bytes());
        output.extend_from_slice(&self.start_sequence.to_be_bytes());
        output.extend_from_slice(&(self.entries.len() as u32).to_be_bytes());
        for entry in &self.entries {
            output.extend_from_slice(&entry.sequence.to_be_bytes());
            encode_bytes(&mut output, &entry.payload, "payload")?;
            output.extend_from_slice(&entry.payload_digest);
            output.extend_from_slice(&entry.predecessor_digest);
            output.extend_from_slice(&entry.journal_envelope_digest);
        }
        encode_string(
            &mut output,
            &self.signer_principal_id,
            "signer_principal_id",
        )?;
        output.push(self.signer_role as u8);
        output.extend_from_slice(&self.signer_key_version.to_be_bytes());
        output.extend_from_slice(&self.observed_at.to_be_bytes());
        encode_string(&mut output, &self.fencing_token, "fencing_token")?;
        Ok(output)
    }

    fn validate_structure(&self) -> Result<(), ReplicationError> {
        if self.protocol_version != FACT_CREATE_TRANSPORT_PROTOCOL_VERSION {
            return Err(ReplicationError::UnsupportedProtocolVersion(
                self.protocol_version,
            ));
        }
        if self.signer_role != SignerRole::DeviceWriter {
            return Err(ReplicationError::RoleArtifactMismatch {
                role: self.signer_role,
                artifact: super::ArtifactKind::Mutation,
            });
        }
        for (field, value, max) in [
            ("batch_id", self.batch_id.as_str(), MAX_ID_BYTES),
            ("home_device_id", self.home_device_id.as_str(), MAX_ID_BYTES),
            ("store_id", self.store_id.as_str(), MAX_ID_BYTES),
            (
                "signer_principal_id",
                self.signer_principal_id.as_str(),
                MAX_ID_BYTES,
            ),
            (
                "fencing_token",
                self.fencing_token.as_str(),
                MAX_FENCING_TOKEN_BYTES,
            ),
        ] {
            if value.is_empty() {
                return Err(ReplicationError::EmptyField(field));
            }
            if value.trim() != value || value.chars().any(char::is_control) {
                return Err(ReplicationError::PreCollisionValidation(format!(
                    "fact-create batch field '{field}' must be trimmed and contain no control characters"
                )));
            }
            if value.len() > max {
                return Err(ReplicationError::FieldExceedsMaxLength {
                    field,
                    len: value.len(),
                    max,
                });
            }
        }
        if self.stream_epoch == 0 {
            return Err(ReplicationError::PreCollisionValidation(
                "fact-create stream_epoch must be positive".to_string(),
            ));
        }
        if self.start_sequence < 1 {
            return Err(ReplicationError::PreCollisionValidation(
                "fact-create start_sequence must be positive".to_string(),
            ));
        }
        if self.entries.is_empty() {
            return Err(ReplicationError::EmptyField("entries"));
        }
        if self.entries.len() > FACT_CREATE_TRANSPORT_MAX_ENTRIES {
            return Err(ReplicationError::FieldExceedsMaxLength {
                field: "entries",
                len: self.entries.len(),
                max: FACT_CREATE_TRANSPORT_MAX_ENTRIES,
            });
        }
        for (offset, entry) in self.entries.iter().enumerate() {
            let expected = self.start_sequence + offset as i64;
            if entry.sequence != expected {
                return Err(ReplicationError::PreCollisionValidation(format!(
                    "fact-create batch sequence mismatch: expected {expected}, got {}",
                    entry.sequence
                )));
            }
        }
        Ok(())
    }
}

fn encode_string(
    output: &mut Vec<u8>,
    value: &str,
    field: &'static str,
) -> Result<(), ReplicationError> {
    encode_bytes(output, value.as_bytes(), field)
}

fn encode_bytes(
    output: &mut Vec<u8>,
    value: &[u8],
    field: &'static str,
) -> Result<(), ReplicationError> {
    let length = value.len();
    let length_u32 =
        u32::try_from(length).map_err(|_| ReplicationError::FieldTooLong { field, len: length })?;
    output.extend_from_slice(&length_u32.to_be_bytes());
    output.extend_from_slice(value);
    Ok(())
}
