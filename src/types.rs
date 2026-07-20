//! Device identity management and observation provenance.
//!
//! Feature: `device-mgmt`.

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::fmt;
use std::str::FromStr;
use uuid::{Uuid, Variant, Version};

use crate::error::PooledMemoryError;
use semantic_memory::MemoryError;

macro_rules! opaque_uuid_v4 {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, JsonSchema)]
        #[schemars(transparent)]
        pub struct $name(String);

        impl $name {
            /// Generates a new random UUID v4 identity.
            pub fn new() -> Self {
                Self(Uuid::new_v4().to_string())
            }

            /// Parses and validates a UUID v4 identity.
            pub fn parse(value: impl AsRef<str>) -> Result<Self, MemoryError> {
                let value = value.as_ref();
                let parsed = Uuid::parse_str(value).map_err(|error| {
                    MemoryError::InvalidKey(format!(
                        "invalid {} '{value}': {error}",
                        stringify!($name)
                    ))
                })?;
                if parsed.get_version() != Some(Version::Random)
                    || parsed.get_variant() != Variant::RFC4122
                {
                    return Err(MemoryError::InvalidKey(format!(
                        "invalid {} '{value}': expected an RFC 4122 UUID v4",
                        stringify!($name)
                    )));
                }
                Ok(Self(parsed.to_string()))
            }

            /// Returns the canonical hyphenated UUID string.
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Consumes this identity and returns its canonical UUID string.
            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = MemoryError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::parse(value)
            }
        }

        impl TryFrom<String> for $name {
            type Error = MemoryError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::parse(value)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::parse(value).map_err(serde::de::Error::custom)
            }
        }
    };
}

opaque_uuid_v4!(DeviceId, "Opaque, validated UUID v4 identifying a device.");
opaque_uuid_v4!(ActorId, "Opaque, validated UUID v4 identifying an actor.");
opaque_uuid_v4!(
    OperationId,
    "Opaque, validated UUID v4 identifying an idempotent operation."
);
opaque_uuid_v4!(
    ProvenanceEdgeId,
    "Opaque, validated UUID v4 identifying a provenance edge event."
);

/// Stable typed reference for another subsystem identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub struct MemoryItemRef {
    /// Node domain kind.
    pub kind: String,
    /// Node identity.
    pub id: String,
}

impl MemoryItemRef {
    /// Construct and validate a typed item reference.
    pub fn new(kind: impl Into<String>, id: impl Into<String>) -> Result<Self, PooledMemoryError> {
        let kind = kind.into();
        let id = id.into();
        if kind.trim().is_empty() {
            return Err(PooledMemoryError::InvalidProvenance(
                "memory item kind cannot be empty".to_string(),
            ));
        }
        if id.trim().is_empty() {
            return Err(PooledMemoryError::InvalidProvenance(
                "memory item id cannot be empty".to_string(),
            ));
        }
        Ok(Self { kind, id })
    }

    /// Parse from canonical `kind:id`.
    pub fn parse_key(value: impl AsRef<str>) -> Result<Self, PooledMemoryError> {
        let value = value.as_ref();
        let mut parts = value.splitn(2, ':');
        let kind = parts.next().unwrap_or_default();
        let id = parts.next().unwrap_or_default();
        if kind.is_empty() || id.is_empty() || parts.next().is_some() {
            return Err(PooledMemoryError::InvalidProvenance(format!(
                "invalid memory item key '{value}'"
            )));
        }
        Self::new(kind.to_string(), id.to_string())
    }

    /// Canonical key for map/dictionary operations.
    pub fn canonical_key(&self) -> String {
        format!("{}:{}", self.kind, self.id)
    }
}

impl fmt::Display for MemoryItemRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.canonical_key())
    }
}

/// Provenance edge types for append-only lineage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProvenanceEdgeType {
    /// observed_by: memory item -> operation/device/actor observation node.
    ObservedBy,
    /// recorded_by: memory item -> operation/device/actor recorder node.
    RecordedBy,
    /// derived_from: source evidence -> target derived node.
    DerivedFrom,
    /// supports: evidence -> claim/item evidence relation.
    Supports,
    /// contradicts: evidence -> claim/item contradiction relation.
    Contradicts,
    /// supersedes: newer -> prior relation.
    Supersedes,
    /// retrieved_from: node from retrieval/import/search operation.
    RetrievedFrom,
}

impl ProvenanceEdgeType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ObservedBy => "observed_by",
            Self::RecordedBy => "recorded_by",
            Self::DerivedFrom => "derived_from",
            Self::Supports => "supports",
            Self::Contradicts => "contradicts",
            Self::Supersedes => "supersedes",
            Self::RetrievedFrom => "retrieved_from",
        }
    }

    pub(crate) fn parse(value: &str, row_id: &str) -> Result<Self, MemoryError> {
        match value {
            "observed_by" => Ok(Self::ObservedBy),
            "recorded_by" => Ok(Self::RecordedBy),
            "derived_from" => Ok(Self::DerivedFrom),
            "supports" => Ok(Self::Supports),
            "contradicts" => Ok(Self::Contradicts),
            "supersedes" => Ok(Self::Supersedes),
            "retrieved_from" => Ok(Self::RetrievedFrom),
            other => Err(MemoryError::CorruptData {
                table: "provenance_edges",
                row_id: row_id.to_string(),
                detail: format!("invalid provenance edge type '{other}'"),
            }),
        }
    }
}

/// A registered client or server device.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Device {
    /// Stable device identity.
    pub device_id: DeviceId,
    /// Operator-visible device label.
    pub label: String,
    /// Platform name, such as `linux`, `macos`, or `windows`.
    pub platform: String,
    /// Device hostname at registration time.
    pub hostname: String,
    /// Optional fingerprint of the credential presented by the device.
    pub credential_fingerprint: Option<String>,
    /// First registration time, always replaced with server time on registration.
    pub first_seen_at: String,
    /// Most recent server-observed activity time.
    pub last_seen_at: String,
    /// Current management status.
    pub status: DeviceStatus,
}

impl Device {
    /// Creates an active registration request; the store supplies both timestamps.
    pub fn new(
        device_id: DeviceId,
        label: impl Into<String>,
        platform: impl Into<String>,
        hostname: impl Into<String>,
    ) -> Self {
        Self {
            device_id,
            label: label.into(),
            platform: platform.into(),
            hostname: hostname.into(),
            credential_fingerprint: None,
            first_seen_at: String::new(),
            last_seen_at: String::new(),
            status: DeviceStatus::Active,
        }
    }
}

/// Lifecycle status of a registered device.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeviceStatus {
    /// Device may submit operations.
    Active,
    /// Device credentials have been revoked.
    Revoked,
    /// Device has been isolated pending operator review.
    Quarantined,
}

impl DeviceStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Revoked => "revoked",
            Self::Quarantined => "quarantined",
        }
    }

    pub(crate) fn parse(value: &str, row_id: &str) -> Result<Self, MemoryError> {
        match value {
            "active" => Ok(Self::Active),
            "revoked" => Ok(Self::Revoked),
            "quarantined" => Ok(Self::Quarantined),
            other => Err(MemoryError::CorruptData {
                table: "devices",
                row_id: row_id.to_string(),
                detail: format!("invalid status '{other}'"),
            }),
        }
    }
}

/// An actor executing through a registered device.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Actor {
    /// Stable actor identity.
    pub actor_id: ActorId,
    /// Device through which this actor operates.
    pub device_id: DeviceId,
    /// Actor implementation class.
    pub actor_kind: ActorKind,
    /// Optional provider and model identifier.
    pub provider_model: Option<String>,
    /// Registration time, always replaced with server time on registration.
    pub recorded_at: String,
}

impl Actor {
    /// Creates an actor registration request; the store supplies `recorded_at`.
    pub fn new(actor_id: ActorId, device_id: DeviceId, actor_kind: ActorKind) -> Self {
        Self {
            actor_id,
            device_id,
            actor_kind,
            provider_model: None,
            recorded_at: String::new(),
        }
    }
}

/// Kind of principal or process represented by an actor.
#[derive(Debug, Clone, PartialEq, Eq, JsonSchema)]
pub enum ActorKind {
    /// Human operator.
    Human,
    /// Hermes agent.
    Hermes,
    /// Codex agent.
    Codex,
    /// Ollama-backed agent.
    Ollama,
    /// Long-running service.
    Service,
    /// Plugin process.
    Plugin,
    /// Generic operating-system process.
    Process,
    /// Forward-compatible actor kind not recognized by this build.
    Unknown(String),
}

impl ActorKind {
    /// Returns the stable database/wire representation.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Human => "human",
            Self::Hermes => "hermes",
            Self::Codex => "codex",
            Self::Ollama => "ollama",
            Self::Service => "service",
            Self::Plugin => "plugin",
            Self::Process => "process",
            Self::Unknown(value) => value,
        }
    }

    /// Parses a stable actor kind, retaining unknown values losslessly.
    pub fn parse(value: impl Into<String>) -> Self {
        let value = value.into();
        match value.as_str() {
            "human" => Self::Human,
            "hermes" => Self::Hermes,
            "codex" => Self::Codex,
            "ollama" => Self::Ollama,
            "service" => Self::Service,
            "plugin" => Self::Plugin,
            "process" => Self::Process,
            _ => Self::Unknown(value),
        }
    }
}

impl Serialize for ActorKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ActorKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self::parse(String::deserialize(deserializer)?))
    }
}

/// Durable lineage and idempotency metadata for one accepted operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OperationEnvelope {
    /// Stable operation identity.
    pub operation_id: OperationId,
    /// Caller-selected key used to collapse retries.
    pub idempotency_key: String,
    /// Device requesting the operation.
    pub requesting_device_id: DeviceId,
    /// Actor requesting the operation.
    pub requesting_actor_id: ActorId,
    /// Device that recorded the observation.
    pub recording_device_id: DeviceId,
    /// Server device that accepted the operation.
    pub recording_server_id: DeviceId,
    /// Semantic operation class.
    pub operation_kind: OperationKind,
    /// Target resource class.
    pub target_kind: String,
    /// Stable target resource identity.
    pub target_id: String,
    /// Digest of the operation content.
    pub content_digest: String,
    /// Optional client observation time.
    pub observed_at: Option<String>,
    /// Optional domain-valid time.
    pub valid_time: Option<String>,
    /// Acceptance time, always replaced with server time on submission.
    pub recorded_at: String,
    /// Receipt assigned by the accepting server.
    pub receipt_id: Option<String>,
}

/// Supported durable operation classes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OperationKind {
    /// Record an observation without promoting it to an assertion.
    Observe,
    /// Assert a target value or fact.
    Assert,
    /// Supersede a prior target.
    Supersede,
    /// Revoke a target.
    Revoke,
    /// Redact target content.
    Redact,
    /// Record an adjudication result.
    Adjudicate,
}

impl OperationKind {
    /// Returns the stable database/wire representation.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Observe => "observe",
            Self::Assert => "assert",
            Self::Supersede => "supersede",
            Self::Revoke => "revoke",
            Self::Redact => "redact",
            Self::Adjudicate => "adjudicate",
        }
    }

    pub(crate) fn parse(value: &str, row_id: &str) -> Result<Self, MemoryError> {
        match value {
            "observe" => Ok(Self::Observe),
            "assert" => Ok(Self::Assert),
            "supersede" => Ok(Self::Supersede),
            "revoke" => Ok(Self::Revoke),
            "redact" => Ok(Self::Redact),
            "adjudicate" => Ok(Self::Adjudicate),
            other => Err(MemoryError::CorruptData {
                table: "operation_envelopes",
                row_id: row_id.to_string(),
                detail: format!("invalid operation_kind '{other}'"),
            }),
        }
    }
}

/// Bitemporal filter for provenance and lineage queries.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AsOf {
    /// Domain-valid cutoff.
    pub valid_at: Option<String>,
    /// Historical recording cutoff.
    pub recorded_at_or_before: Option<String>,
}

impl AsOf {
    pub fn at(valid_at: Option<String>, recorded_at_or_before: Option<String>) -> Self {
        Self {
            valid_at,
            recorded_at_or_before,
        }
    }

    pub fn now() -> Self {
        Self {
            valid_at: None,
            recorded_at_or_before: None,
        }
    }
}

/// Request to append one provenance edge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProvenanceEdgeRequest {
    pub edge_type: ProvenanceEdgeType,
    pub source: MemoryItemRef,
    pub target: MemoryItemRef,
    pub operation_id: Option<OperationId>,
    pub actor_id: Option<ActorId>,
    pub device_id: Option<DeviceId>,
    pub valid_from: Option<DateTime<Utc>>,
    pub valid_to: Option<DateTime<Utc>>,
    pub observed_at: Option<DateTime<Utc>>,
    /// Caller-provided recorded time for migration/import mode only.
    pub recorded_at: Option<DateTime<Utc>>,
    pub content_digest: Option<String>,
    /// Optional serialized JSON metadata payload.
    pub metadata: Option<String>,
    pub supersedes_edge_id: Option<ProvenanceEdgeId>,
}

/// Persisted provenance edge row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProvenanceEdge {
    pub edge_id: ProvenanceEdgeId,
    pub edge_type: ProvenanceEdgeType,
    pub source: MemoryItemRef,
    pub target: MemoryItemRef,
    pub operation_id: Option<OperationId>,
    pub actor_id: Option<ActorId>,
    pub device_id: Option<DeviceId>,
    pub valid_from: Option<DateTime<Utc>>,
    pub valid_to: Option<DateTime<Utc>>,
    pub observed_at: Option<DateTime<Utc>>,
    pub recorded_at: DateTime<Utc>,
    pub content_digest: Option<String>,
    pub metadata: Option<Value>,
    pub supersedes_edge_id: Option<ProvenanceEdgeId>,
}

/// Filter request for provenance edge queries.
#[derive(Debug, Clone, Default)]
pub struct ProvenanceQuery {
    pub source: Option<MemoryItemRef>,
    pub target: Option<MemoryItemRef>,
    pub edge_types: Vec<ProvenanceEdgeType>,
    pub operation_id: Option<OperationId>,
    pub as_of: AsOf,
    pub include_superseded: bool,
    pub limit: usize,
}

/// Lineage traversal result root type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LineageNode {
    MemoryItem(MemoryItemRef),
    Operation(Box<OperationEnvelope>),
}

/// Result for provenance lineage traversal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageResult {
    pub root: MemoryItemRef,
    pub edges: Vec<ProvenanceEdge>,
    pub items: Vec<MemoryItemRef>,
    pub operations: Vec<OperationEnvelope>,
    pub truncated: bool,
    pub as_of: AsOf,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};

    #[test]
    fn device_ids_require_uuid_v4_and_round_trip_as_strings() {
        let id = DeviceId::new();
        let encoded = serde_json::to_string(&id).unwrap();
        assert_eq!(serde_json::from_str::<DeviceId>(&encoded).unwrap(), id);
        assert!(DeviceId::parse("00000000-0000-1000-8000-000000000000").is_err());
    }

    #[test]
    fn actor_kind_preserves_unknown_wire_values() {
        let kind: ActorKind = serde_json::from_str("\"future-agent\"").unwrap();
        assert_eq!(kind, ActorKind::Unknown("future-agent".to_string()));
        assert_eq!(serde_json::to_string(&kind).unwrap(), "\"future-agent\"");
    }

    #[test]
    fn memory_item_ref_is_strict_and_serializes_as_object() {
        let reference = MemoryItemRef::new("fact", "f-1").unwrap();
        let encoded = serde_json::to_string(&reference).unwrap();
        let decoded: MemoryItemRef = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, reference);
        assert_eq!(reference.canonical_key(), "fact:f-1");

        assert!(MemoryItemRef::new("", "f-1").is_err());
        assert!(MemoryItemRef::new("fact", "").is_err());
        assert!(MemoryItemRef::parse_key("no-colon").is_err());
    }

    #[test]
    fn provenance_edge_type_round_trip_and_unknown_edge_rejected() {
        let edge_type = ProvenanceEdgeType::Contradicts;
        let encoded = serde_json::to_string(&edge_type).unwrap();
        assert_eq!(encoded, "\"contradicts\"");
        let decoded: ProvenanceEdgeType = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, edge_type);
        assert!(serde_json::from_str::<ProvenanceEdgeType>("\"bogus\"").is_err());
    }

    #[test]
    fn provenance_edge_id_is_valid_uuid_v4() {
        assert!(ProvenanceEdgeId::parse("00000000-0000-0000-0000-000000000000").is_err());
        let id = ProvenanceEdgeId::new();
        let encoded = serde_json::to_string(&id).unwrap();
        let decoded = serde_json::from_str::<ProvenanceEdgeId>(&encoded).unwrap();
        assert_eq!(decoded, id);
    }

    #[test]
    fn as_of_uses_rfc3339_timestamps() {
        let at = DateTime::parse_from_rfc3339("2026-07-19T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let value = AsOf::at(Some(at.to_rfc3339()), Some(at.to_rfc3339()));
        assert_eq!(value.valid_at.unwrap(), at.to_rfc3339());
        assert_eq!(value.recorded_at_or_before.unwrap(), at.to_rfc3339());
    }
}
