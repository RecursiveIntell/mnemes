//! Typed, signed transport profile for owner-produced fact-supersede records.
//!
//! Mnemes owns only the signed admission wrapper.  The enclosed semantic
//! envelope is copied verbatim into the closed semantic-memory apply API.

use super::{ArtifactKind, ReplicationError, SignerRole};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use semantic_memory::journal::{
    validate_fact_supersede_replica_envelope, FactCreatePayloadV1, FactSupersedeReplicaEnvelopeV1,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

pub const FACT_SUPERSEDE_TRANSPORT_PROTOCOL_FAMILY: &str = "mnemes.fact-supersede.v1";
pub const FACT_SUPERSEDE_TRANSPORT_PROTOCOL_VERSION: u16 = 1;
pub const FACT_SUPERSEDE_TRANSPORT_MAX_WIRE_BYTES: usize = 4 * 1024 * 1024;
pub const FACT_SUPERSEDE_TRANSPORT_MAX_ENTRIES: usize = 1;
pub const FACT_SUPERSEDE_TRANSPORT_SIGNATURE_DOMAIN: &[u8] =
    b"mnemes/fact-supersede-batch/signature/v1\0";

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

/// A verbatim owner envelope.  Mnemes never reconstructs its payload or
/// envelope fields; this wrapper is solely the transport batch entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FactSupersedeTransportEntryV1 {
    pub owner_envelope: FactSupersedeReplicaEnvelopeV1,
}

impl FactSupersedeTransportEntryV1 {
    pub fn from_owner_envelope(owner_envelope: FactSupersedeReplicaEnvelopeV1) -> Self {
        Self { owner_envelope }
    }
}

/// One signed Mnemes admission request for one owner-produced supersession.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedFactSupersedeBatchV1 {
    pub protocol_family: String,
    pub protocol_version: u16,
    pub batch_id: String,
    pub home_device_id: String,
    pub store_id: String,
    /// Mnemes control-plane promotion generation; it is not an owner epoch.
    pub store_epoch: u64,
    /// Mnemes writer fencing generation; it is not an owner epoch.
    pub writer_epoch: u64,
    /// Copied owner stream epoch, with no Mnemes semantic interpretation.
    pub owner_stream_epoch: u64,
    pub start_sequence: i64,
    pub entries: Vec<FactSupersedeTransportEntryV1>,
    pub signer_principal_id: String,
    pub signer_role: SignerRole,
    pub signer_key_version: u64,
    pub observed_at: u64,
    pub fencing_token: String,
    pub signer_public_key: [u8; 32],
    #[serde(with = "sig64_serde")]
    pub signature: [u8; 64],
}

impl SignedFactSupersedeBatchV1 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        batch_id: impl Into<String>,
        home_device_id: impl Into<String>,
        store_id: impl Into<String>,
        store_epoch: u64,
        writer_epoch: u64,
        owner_stream_epoch: u64,
        start_sequence: i64,
        entries: Vec<FactSupersedeTransportEntryV1>,
        signer_principal_id: impl Into<String>,
        signer_key_version: u64,
        observed_at: u64,
        fencing_token: impl Into<String>,
    ) -> Result<Self, ReplicationError> {
        let batch = Self {
            protocol_family: FACT_SUPERSEDE_TRANSPORT_PROTOCOL_FAMILY.to_string(),
            protocol_version: FACT_SUPERSEDE_TRANSPORT_PROTOCOL_VERSION,
            batch_id: batch_id.into(),
            home_device_id: home_device_id.into(),
            store_id: store_id.into(),
            store_epoch,
            writer_epoch,
            owner_stream_epoch,
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

    pub fn sign(&mut self, signing_key: &SigningKey) -> Result<(), ReplicationError> {
        self.validate_structure()?;
        self.signer_public_key = signing_key.verifying_key().to_bytes();
        self.signature = signing_key.sign(&self.signing_preimage()?).to_bytes();
        Ok(())
    }

    pub fn decode_json(bytes: &[u8]) -> Result<Self, ReplicationError> {
        if bytes.len() > FACT_SUPERSEDE_TRANSPORT_MAX_WIRE_BYTES {
            return Err(ReplicationError::FieldExceedsMaxLength {
                field: "wire_body",
                len: bytes.len(),
                max: FACT_SUPERSEDE_TRANSPORT_MAX_WIRE_BYTES,
            });
        }
        let batch: Self = serde_json::from_slice(bytes).map_err(|error| {
            ReplicationError::PreCollisionValidation(format!(
                "fact-supersede transport JSON decode failed: {error}"
            ))
        })?;
        batch.validate()?;
        Ok(batch)
    }

    /// Validate only protocol, owner-envelope, and signature contracts.  The
    /// returned owner envelope remains byte-for-byte the transported one.
    pub fn validate(&self) -> Result<(), ReplicationError> {
        self.validate_structure()?;
        validate_fact_supersede_replica_envelope(&self.entries[0].owner_envelope).map_err(
            |error| {
                ReplicationError::PreCollisionValidation(format!(
                    "semantic-memory fact-supersede validation failed: {error}"
                ))
            },
        )?;
        let verifying_key = VerifyingKey::from_bytes(&self.signer_public_key)
            .map_err(|error| ReplicationError::InvalidPublicKey(error.to_string()))?;
        verifying_key
            .verify(
                &self.signing_preimage()?,
                &Signature::from_bytes(&self.signature),
            )
            .map_err(|error| ReplicationError::SignatureVerification(error.to_string()))
    }

    pub fn semantic_envelope(&self) -> Result<FactSupersedeReplicaEnvelopeV1, ReplicationError> {
        self.validate()?;
        Ok(self.entries[0].owner_envelope.clone())
    }

    /// The only payload projection Mnemes uses: the already owner-validated
    /// replacement namespace required by its independent admission row.
    pub fn replacement_namespace(&self) -> Result<String, ReplicationError> {
        let payload = validate_fact_supersede_replica_envelope(&self.entries[0].owner_envelope)
            .map_err(|error| ReplicationError::PreCollisionValidation(error.to_string()))?;
        let replacement: FactCreatePayloadV1 = serde_json::from_slice(&payload.replacement_payload)
            .map_err(|error| {
                ReplicationError::PreCollisionValidation(format!(
                    "owner-validated replacement payload cannot project namespace: {error}"
                ))
            })?;
        Ok(replacement.namespace)
    }

    pub fn signing_preimage(&self) -> Result<Vec<u8>, ReplicationError> {
        let mut output = Vec::with_capacity(768);
        output.extend_from_slice(FACT_SUPERSEDE_TRANSPORT_SIGNATURE_DOMAIN);
        encode_string(&mut output, &self.protocol_family, "protocol_family")?;
        output.extend_from_slice(&self.protocol_version.to_be_bytes());
        encode_string(&mut output, &self.batch_id, "batch_id")?;
        encode_string(&mut output, &self.home_device_id, "home_device_id")?;
        encode_string(&mut output, &self.store_id, "store_id")?;
        output.extend_from_slice(&self.store_epoch.to_be_bytes());
        output.extend_from_slice(&self.writer_epoch.to_be_bytes());
        output.extend_from_slice(&self.owner_stream_epoch.to_be_bytes());
        output.extend_from_slice(&self.start_sequence.to_be_bytes());
        output.extend_from_slice(&(self.entries.len() as u32).to_be_bytes());
        for entry in &self.entries {
            let envelope = &entry.owner_envelope;
            encode_string(
                &mut output,
                &envelope.home_device_id,
                "owner_home_device_id",
            )?;
            encode_string(&mut output, &envelope.store_id, "owner_store_id")?;
            output.extend_from_slice(&envelope.stream_epoch.to_be_bytes());
            output.extend_from_slice(&envelope.sequence.to_be_bytes());
            encode_string(
                &mut output,
                &envelope.operation_kind,
                "owner_operation_kind",
            )?;
            encode_string(
                &mut output,
                &envelope.payload_schema,
                "owner_payload_schema",
            )?;
            encode_bytes(&mut output, &envelope.payload, "owner_payload")?;
            output.extend_from_slice(&envelope.payload_digest);
            output.extend_from_slice(&envelope.predecessor_digest);
            output.extend_from_slice(&envelope.envelope_digest);
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
        if self.protocol_family != FACT_SUPERSEDE_TRANSPORT_PROTOCOL_FAMILY {
            return Err(ReplicationError::PreCollisionValidation(
                "fact-supersede protocol family mismatch".to_string(),
            ));
        }
        if self.protocol_version != FACT_SUPERSEDE_TRANSPORT_PROTOCOL_VERSION {
            return Err(ReplicationError::UnsupportedProtocolVersion(
                self.protocol_version,
            ));
        }
        if self.signer_role != SignerRole::DeviceWriter {
            return Err(ReplicationError::RoleArtifactMismatch {
                role: self.signer_role,
                artifact: ArtifactKind::Mutation,
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
                    "fact-supersede batch field '{field}' must be trimmed and contain no control characters"
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
        if self.store_epoch == 0 || self.writer_epoch == 0 || self.owner_stream_epoch == 0 {
            return Err(ReplicationError::PreCollisionValidation(
                "fact-supersede transport generation fields must be positive".to_string(),
            ));
        }
        if self.start_sequence < 1 {
            return Err(ReplicationError::PreCollisionValidation(
                "fact-supersede start_sequence must be positive".to_string(),
            ));
        }
        if self.entries.len() != FACT_SUPERSEDE_TRANSPORT_MAX_ENTRIES {
            return Err(ReplicationError::FieldExceedsMaxLength {
                field: "entries",
                len: self.entries.len(),
                max: FACT_SUPERSEDE_TRANSPORT_MAX_ENTRIES,
            });
        }
        let envelope = &self.entries[0].owner_envelope;
        if envelope.home_device_id != self.home_device_id
            || envelope.store_id != self.store_id
            || envelope.stream_epoch != self.owner_stream_epoch
            || envelope.sequence != self.start_sequence
        {
            return Err(ReplicationError::PreCollisionValidation(
                "fact-supersede transport does not exactly bind owner envelope identity"
                    .to_string(),
            ));
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
