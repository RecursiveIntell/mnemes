//! Device identity management and observation provenance.
//!
//! Feature: `device-mgmt`.

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;
use uuid::{Uuid, Variant, Version};

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
                    MemoryError::InvalidKey(format!("invalid {} '{value}': {error}", stringify!($name)))
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
