//! Strict, compact admission of Recursive Agent run-pack observations.

use crate::error::MnemesError;
use crate::store::MnemesStore;
use crate::types::{
    MemoryItemRef, OperationEnvelope, OperationId, OperationKind, ProvenanceEdgeRequest,
    ProvenanceEdgeType,
};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunPackAdmissionEnvelope {
    projection: RunPackEvidenceProjectionV1,
    admission_witness: RunPackAdmissionWitnessV1,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct RunPackAdmissionWitnessV1 {
    format: String,
    canonical_projection_digest: String,
    pack_manifest_digest: String,
    pack_content_digest: String,
    verification_receipt_digest: String,
    verified: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    signature: String,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunPackEvidenceProjectionV1 {
    pub schema: String,
    pub projection_id: String,
    pub run_id: String,
    pub pack_manifest_digest: String,
    pub pack_content_digest: String,
    pub verification: Verification,
    pub vault: Vault,
    pub origin: Origin,
    pub event_summary: EventSummary,
}
#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Verification {
    pub verifier_contract_version: String,
    pub verified_at: String,
    pub verification_receipt_digest: String,
    pub outcome: String,
}
#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Vault {
    pub object_id: String,
    pub relative_ref: String,
    pub retention_state: String,
}
#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Origin {
    pub operator_adapter: String,
    pub source_device_ref: Option<String>,
    pub observed_at: Option<String>,
    pub recorded_at: String,
}
#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct EventSummary {
    pub terminal_state: String,
    pub receipt_chain_digest: String,
    pub artifact_digests: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunPackObservationReceipt {
    pub receipt_id: String,
    pub operation_id: OperationId,
    pub recorded_at: String,
}

impl MnemesStore {
    /// Authenticate the submitting device, admit only the frozen projection, and
    /// record it through the existing operation and provenance owners.
    pub async fn import_run_pack_observation(
        &self,
        token: &str,
        bytes: &[u8],
        attestation_key: Option<&[u8]>,
    ) -> Result<RunPackObservationReceipt, MnemesError> {
        let (device, _) = self.token_device_id(token).await?;
        let envelope: RunPackAdmissionEnvelope = serde_json::from_slice(bytes).map_err(|e| {
            MnemesError::InvalidProvenance(format!("invalid run-pack admission envelope: {e}"))
        })?;
        let projection = envelope.projection;
        let witness = envelope.admission_witness;
        let key = attestation_key.ok_or_else(|| {
            MnemesError::InvalidProvenance(
                "run-pack admission attestation is not configured".into(),
            )
        })?;
        let canonical = serde_json::to_vec(&projection).map_err(|e| {
            MnemesError::InvalidProvenance(format!("cannot canonicalize run-pack projection: {e}"))
        })?;
        let canonical_digest = format!("sha256:{}", hex::encode(Sha256::digest(&canonical)));
        let mut mac = HmacSha256::new_from_slice(key).map_err(|_| {
            MnemesError::InvalidProvenance("invalid run-pack attestation key".into())
        })?;
        let mut signed_value = serde_json::to_value(&witness).map_err(|e| {
            MnemesError::InvalidProvenance(format!("cannot canonicalize admission witness: {e}"))
        })?;
        signed_value
            .as_object_mut()
            .map(|object| object.remove("signature"));
        let signed = serde_json::to_vec(&signed_value).map_err(|e| {
            MnemesError::InvalidProvenance(format!("cannot canonicalize admission witness: {e}"))
        })?;
        mac.update(&signed);
        let expected = mac.finalize().into_bytes();
        let supplied = hex::decode(&witness.signature).unwrap_or_default();
        if supplied.len() != expected.len()
            || supplied.as_slice().ct_eq(expected.as_slice()).unwrap_u8() != 1
        {
            return Err(MnemesError::InvalidProvenance(
                "invalid run-pack admission witness signature".into(),
            ));
        }
        if witness.format != "mnemes.run-pack-admission-witness/v1"
            || !witness.verified
            || witness.canonical_projection_digest != canonical_digest
            || witness.pack_manifest_digest != projection.pack_manifest_digest
            || witness.pack_content_digest != projection.pack_content_digest
            || witness.verification_receipt_digest
                != projection.verification.verification_receipt_digest
        {
            return Err(MnemesError::InvalidProvenance(
                "missing or mismatched server-attested run-pack admission witness".into(),
            ));
        }
        if projection.schema != "RunPackEvidenceProjectionV1"
            || projection.verification.outcome != "verified"
        {
            return Err(MnemesError::InvalidProvenance(
                "unsupported or unverified run-pack projection".into(),
            ));
        }
        if !valid_digest(&projection.projection_id)
            || !valid_digest(&projection.pack_manifest_digest)
            || !valid_digest(&projection.pack_content_digest)
            || !valid_digest(&projection.verification.verification_receipt_digest)
            || !valid_digest(&projection.event_summary.receipt_chain_digest)
            || projection
                .event_summary
                .artifact_digests
                .iter()
                .any(|digest| !valid_digest(digest))
        {
            return Err(MnemesError::InvalidProvenance(
                "run-pack projection contains an invalid content digest".into(),
            ));
        }
        if !matches!(
            projection.vault.retention_state.as_str(),
            "available" | "quarantined" | "pack_unavailable" | "tampered" | "superseded"
        ) {
            return Err(MnemesError::InvalidProvenance(
                "unsupported run-pack retention state".into(),
            ));
        }
        if projection.vault.relative_ref.starts_with('/')
            || projection.vault.relative_ref.split('/').any(|p| p == "..")
        {
            return Err(MnemesError::InvalidProvenance(
                "run-pack vault reference escapes its root".into(),
            ));
        }
        // Validate the only caller-supplied temporal fact before persisting an
        // operation. A malformed observed timestamp must not leave a durable
        // operation that a retry merely discovers later.
        let observed_at = projection
            .origin
            .observed_at
            .as_deref()
            .map(parse_time)
            .transpose()?;
        let digest = format!("sha256:{}", hex::encode(Sha256::digest(bytes)));
        let actor = self
            .list_actors_for_device(&device.device_id)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| MnemesError::ActorNotFound(device.device_id.to_string()))?;
        let proposed_operation_id = OperationId::parse(uuid::Uuid::new_v4().to_string())?;
        let idem = format!(
            "mnemes-run-pack-import:{}:{}",
            projection.pack_manifest_digest, projection.schema
        );
        let envelope = OperationEnvelope {
            operation_id: proposed_operation_id,
            idempotency_key: idem,
            requesting_device_id: device.device_id.clone(),
            requesting_actor_id: actor.actor_id.clone(),
            recording_device_id: device.device_id.clone(),
            recording_server_id: device.device_id.clone(),
            operation_kind: OperationKind::Observe,
            target_kind: "run_pack_projection".into(),
            target_id: projection.projection_id.clone(),
            content_digest: digest.clone(),
            observed_at: projection.origin.observed_at.clone(),
            valid_time: projection.origin.observed_at.clone(),
            recorded_at: String::new(),
            receipt_id: None,
        };
        let receipt_id = self.submit_operation(envelope).await?;
        // `submit_operation` returns the old receipt for an exact retry. Fetch
        // its persisted envelope so the provenance edge always references the
        // durable operation, never this call's discarded candidate ID.
        let operation = self
            .get_operation_by_receipt(&receipt_id)
            .await?
            .ok_or_else(|| MnemesError::Database("operation receipt missing".into()))?;
        let recorded_at = operation.recorded_at.clone();
        self.record_provenance_edge(ProvenanceEdgeRequest {
            edge_type: ProvenanceEdgeType::DerivedFrom,
            source: MemoryItemRef::new("run_pack_projection", projection.projection_id)?,
            target: MemoryItemRef::new("pack_manifest", projection.pack_manifest_digest)?,
            operation_id: Some(operation.operation_id.clone()),
            actor_id: Some(actor.actor_id),
            device_id: Some(device.device_id),
            valid_from: None,
            valid_to: None,
            observed_at,
            recorded_at: None,
            content_digest: Some(digest),
            metadata: None,
            supersedes_edge_id: None,
        })
        .await?;
        Ok(RunPackObservationReceipt {
            receipt_id,
            operation_id: operation.operation_id,
            recorded_at,
        })
    }
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .chars()
            .all(|character| character.is_ascii_hexdigit() && !character.is_ascii_uppercase())
}

fn parse_time(value: &str) -> Result<DateTime<Utc>, MnemesError> {
    DateTime::parse_from_rfc3339(value)
        .map(|v| v.with_timezone(&Utc))
        .map_err(|e| MnemesError::InvalidProvenance(format!("invalid observed_at: {e}")))
}
