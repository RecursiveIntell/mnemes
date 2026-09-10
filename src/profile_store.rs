//! Canonical memory-profile, store-identity, and access-grant contracts.
//!
//! A `ToolProfile` describes the MCP surface exposed to an actor. The types in
//! this module describe a different boundary: which independently addressable
//! memory store belongs to which memory profile, and which other profiles may
//! access an exact namespace for a bounded purpose and time window.

use crate::{ActorId, DeviceId, MnemesError};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;
use uuid::{Uuid, Variant, Version};

const MAX_ID_BYTES: usize = 128;
const MAX_LABEL_BYTES: usize = 256;
const MAX_PATH_BYTES: usize = 512;
pub const AUTHORIZATION_POLICY_VERSION: &str = "mnemes.profile-authz.v1";
pub const MAX_PERMIT_QUERY_BUDGET: usize = 100;

fn digest_fields(domain: &str, fields: impl IntoIterator<Item = String>) -> String {
    let mut hasher = Sha256::new();
    hasher.update((domain.len() as u64).to_be_bytes());
    hasher.update(domain.as_bytes());
    for field in fields {
        hasher.update((field.len() as u64).to_be_bytes());
        hasher.update(field.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

fn validate_text(value: &str, field: &str, max_bytes: usize) -> Result<(), MnemesError> {
    if value.is_empty() {
        return Err(MnemesError::InvalidMemoryScope(format!(
            "{field} cannot be empty"
        )));
    }
    if value.len() > max_bytes {
        return Err(MnemesError::InvalidMemoryScope(format!(
            "{field} exceeds {max_bytes} bytes"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(MnemesError::InvalidMemoryScope(format!(
            "{field} contains a control character"
        )));
    }
    Ok(())
}

/// Canonical relative location for a profile-owned semantic-memory store.
/// Callers name the store; they do not choose an arbitrary runtime path.
pub fn canonical_memory_store_relative_path(
    profile_id: &MemoryProfileId,
    store_id: &str,
) -> Result<String, MnemesError> {
    validate_text(store_id, "store_id", MAX_ID_BYTES)?;
    if !store_id
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(MnemesError::InvalidMemoryScope(
            "store_id must contain only ASCII letters, digits, '.', '_', or '-'".to_string(),
        ));
    }
    Ok(format!("memory/profiles/{profile_id}/{store_id}"))
}

/// Stable identity of a Hermes/Ares memory profile.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[schemars(transparent)]
pub struct MemoryProfileId(String);

impl MemoryProfileId {
    pub fn parse(value: impl AsRef<str>) -> Result<Self, MnemesError> {
        let value = value.as_ref().trim();
        validate_text(value, "profile_id", MAX_ID_BYTES)?;
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            return Err(MnemesError::InvalidMemoryScope(
                "profile_id must contain only ASCII letters, digits, '.', '_', or '-'".to_string(),
            ));
        }
        Ok(Self(value.to_string()))
    }

    pub fn new(value: impl Into<String>) -> Result<Self, MnemesError> {
        Self::parse(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MemoryProfileId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Lifecycle state of a memory profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MemoryProfileStatus {
    Active,
    Revoked,
}

impl MemoryProfileStatus {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Revoked => "revoked",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, MnemesError> {
        match value {
            "active" => Ok(Self::Active),
            "revoked" => Ok(Self::Revoked),
            other => Err(MnemesError::InvalidMemoryScope(format!(
                "invalid memory profile status '{other}'"
            ))),
        }
    }
}

/// Durable identity and owner of one memory profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MemoryProfile {
    pub profile_id: MemoryProfileId,
    pub owner_device_id: DeviceId,
    pub label: String,
    pub status: MemoryProfileStatus,
    pub created_at: String,
}

impl MemoryProfile {
    pub fn new(
        profile_id: MemoryProfileId,
        owner_device_id: DeviceId,
        label: impl Into<String>,
    ) -> Result<Self, MnemesError> {
        let label = label.into();
        validate_text(&label, "profile label", MAX_LABEL_BYTES)?;
        Ok(Self {
            profile_id,
            owner_device_id,
            label,
            status: MemoryProfileStatus::Active,
            created_at: String::new(),
        })
    }

    pub fn validate(&self) -> Result<(), MnemesError> {
        validate_text(&self.label, "profile label", MAX_LABEL_BYTES)
    }
}

/// Lifecycle state of one independently addressable memory store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MemoryStoreStatus {
    Active,
    Quarantined,
    Revoked,
}

impl MemoryStoreStatus {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Quarantined => "quarantined",
            Self::Revoked => "revoked",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, MnemesError> {
        match value {
            "active" => Ok(Self::Active),
            "quarantined" => Ok(Self::Quarantined),
            "revoked" => Ok(Self::Revoked),
            other => Err(MnemesError::InvalidMemoryScope(format!(
                "invalid memory store status '{other}'"
            ))),
        }
    }
}

/// Stable control-plane identity for a profile-owned semantic-memory store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MemoryStoreIdentity {
    pub store_id: String,
    pub profile_id: MemoryProfileId,
    pub owner_device_id: DeviceId,
    pub namespace: String,
    pub relative_path: String,
    pub status: MemoryStoreStatus,
    pub created_at: String,
}

impl MemoryStoreIdentity {
    pub fn new(
        store_id: impl Into<String>,
        profile_id: MemoryProfileId,
        owner_device_id: DeviceId,
        namespace: impl Into<String>,
        relative_path: impl Into<String>,
    ) -> Result<Self, MnemesError> {
        let store_id = store_id.into();
        let namespace = namespace.into();
        let relative_path = relative_path.into();
        validate_text(&store_id, "store_id", MAX_ID_BYTES)?;
        validate_text(&namespace, "namespace", MAX_ID_BYTES)?;
        validate_text(&relative_path, "relative_path", MAX_PATH_BYTES)?;
        let path = std::path::Path::new(&relative_path);
        if path.is_absolute()
            || path
                .components()
                .any(|component| matches!(component, std::path::Component::ParentDir))
        {
            return Err(MnemesError::InvalidMemoryScope(
                "relative_path must be relative and may not contain '..'".to_string(),
            ));
        }
        let expected_path = canonical_memory_store_relative_path(&profile_id, &store_id)?;
        if relative_path != expected_path {
            return Err(MnemesError::InvalidMemoryScope(format!(
                "relative_path must equal the canonical profile store path {expected_path}"
            )));
        }
        Ok(Self {
            store_id,
            profile_id,
            owner_device_id,
            namespace,
            relative_path,
            status: MemoryStoreStatus::Active,
            created_at: String::new(),
        })
    }

    pub fn validate(&self) -> Result<(), MnemesError> {
        validate_text(&self.store_id, "store_id", MAX_ID_BYTES)?;
        validate_text(&self.namespace, "namespace", MAX_ID_BYTES)?;
        validate_text(&self.relative_path, "relative_path", MAX_PATH_BYTES)?;
        let path = std::path::Path::new(&self.relative_path);
        if path.is_absolute()
            || path
                .components()
                .any(|component| matches!(component, std::path::Component::ParentDir))
        {
            return Err(MnemesError::InvalidMemoryScope(
                "relative_path must be relative and may not contain '..'".to_string(),
            ));
        }
        if self.relative_path
            != canonical_memory_store_relative_path(&self.profile_id, &self.store_id)?
        {
            return Err(MnemesError::InvalidMemoryScope(
                "relative_path does not match the canonical profile store path".to_string(),
            ));
        }
        Ok(())
    }
}

/// Purpose granted to a profile for one exact store namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MemoryAccessEffect {
    Search,
    Read,
    Write,
}

impl MemoryAccessEffect {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Search => "search",
            Self::Read => "read",
            Self::Write => "write",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, MnemesError> {
        match value {
            "search" => Ok(Self::Search),
            "read" => Ok(Self::Read),
            "write" => Ok(Self::Write),
            other => Err(MnemesError::InvalidMemoryScope(format!(
                "invalid memory access effect '{other}'"
            ))),
        }
    }
}

/// UUID identity of one grant lifecycle record.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[schemars(transparent)]
pub struct MemoryGrantId(String);

impl MemoryGrantId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn parse(value: impl AsRef<str>) -> Result<Self, MnemesError> {
        let value = value.as_ref();
        let parsed = Uuid::parse_str(value).map_err(|error| {
            MnemesError::InvalidMemoryScope(format!("invalid grant_id: {error}"))
        })?;
        if parsed.get_version() != Some(Version::Random) || parsed.get_variant() != Variant::RFC4122
        {
            return Err(MnemesError::InvalidMemoryScope(
                "grant_id must be an RFC 4122 UUID v4".to_string(),
            ));
        }
        Ok(Self(parsed.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for MemoryGrantId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for MemoryGrantId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// UUID identity for one append-preserving actor-to-memory-profile binding.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[schemars(transparent)]
pub struct ActorProfileBindingId(String);

impl ActorProfileBindingId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn parse(value: impl AsRef<str>) -> Result<Self, MnemesError> {
        let parsed = Uuid::parse_str(value.as_ref()).map_err(|error| {
            MnemesError::InvalidMemoryScope(format!("invalid actor profile binding id: {error}"))
        })?;
        if parsed.get_version() != Some(Version::Random) || parsed.get_variant() != Variant::RFC4122
        {
            return Err(MnemesError::InvalidMemoryScope(
                "actor profile binding id must be an RFC 4122 UUID v4".to_string(),
            ));
        }
        Ok(Self(parsed.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for ActorProfileBindingId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ActorProfileBindingId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Canonical authorization subject selection for one actor.
///
/// The binding digest covers immutable issuance fields. Revocation is a later
/// lifecycle event and is evaluated into each request-time snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ActorProfileBinding {
    pub binding_id: ActorProfileBindingId,
    pub actor_id: ActorId,
    pub profile_id: MemoryProfileId,
    pub owner_device_id: DeviceId,
    pub issued_by_actor_id: ActorId,
    pub valid_from: u64,
    pub expires_at: u64,
    pub revoked_at: Option<u64>,
    pub binding_epoch: u64,
    pub binding_digest: String,
    pub recorded_at: String,
}

impl ActorProfileBinding {
    pub fn new(
        actor_id: ActorId,
        profile_id: MemoryProfileId,
        owner_device_id: DeviceId,
        issued_by_actor_id: ActorId,
        valid_from: u64,
        expires_at: u64,
        binding_epoch: u64,
    ) -> Result<Self, MnemesError> {
        let mut binding = Self {
            binding_id: ActorProfileBindingId::new(),
            actor_id,
            profile_id,
            owner_device_id,
            issued_by_actor_id,
            valid_from,
            expires_at,
            revoked_at: None,
            binding_epoch,
            binding_digest: String::new(),
            recorded_at: String::new(),
        };
        binding.binding_digest = binding.expected_digest();
        binding.validate()?;
        Ok(binding)
    }

    pub fn validate(&self) -> Result<(), MnemesError> {
        if self.expires_at <= self.valid_from {
            return Err(MnemesError::ActorProfileBindingDenied(
                "binding expires_at must be after valid_from".to_string(),
            ));
        }
        if self.binding_epoch == 0 {
            return Err(MnemesError::ActorProfileBindingDenied(
                "binding_epoch must be positive".to_string(),
            ));
        }
        if self.revoked_at.is_some_and(|value| value < self.valid_from) {
            return Err(MnemesError::ActorProfileBindingDenied(
                "binding revoked_at cannot precede valid_from".to_string(),
            ));
        }
        if self.binding_digest.len() != 64 || self.binding_digest != self.expected_digest() {
            return Err(MnemesError::ActorProfileBindingDenied(
                "binding digest does not match immutable issuance fields".to_string(),
            ));
        }
        Ok(())
    }

    pub fn is_active_at(&self, at: u64) -> bool {
        self.revoked_at.is_none() && at >= self.valid_from && at < self.expires_at
    }

    pub fn expected_digest(&self) -> String {
        digest_fields(
            "mnemes.actor-profile-binding.v1",
            [
                self.binding_id.to_string(),
                self.actor_id.to_string(),
                self.profile_id.to_string(),
                self.owner_device_id.to_string(),
                self.issued_by_actor_id.to_string(),
                self.valid_from.to_string(),
                self.expires_at.to_string(),
                self.binding_epoch.to_string(),
            ],
        )
    }
}

/// One store admitted to a request-time authorization snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AuthorizedMemoryStore {
    pub store_id: String,
    pub profile_id: MemoryProfileId,
    pub owner_device_id: DeviceId,
    pub namespace: String,
    pub relative_path: String,
    pub grant_ids: Vec<MemoryGrantId>,
    pub authorization_expires_at: u64,
}

/// Immutable request-time projection of the current binding, lifecycle, and grants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AuthorizationSnapshot {
    pub authorization_snapshot_id: String,
    pub actor_id: ActorId,
    pub actor_device_id: DeviceId,
    pub subject_profile_id: MemoryProfileId,
    pub binding_id: ActorProfileBindingId,
    pub binding_digest: String,
    pub effect: MemoryAccessEffect,
    pub namespaces: Vec<String>,
    pub authorized_stores: Vec<AuthorizedMemoryStore>,
    pub evaluated_at: u64,
    pub policy_version: String,
    pub snapshot_digest: String,
}

impl AuthorizationSnapshot {
    pub fn new(
        actor_id: ActorId,
        actor_device_id: DeviceId,
        subject_profile_id: MemoryProfileId,
        binding: &ActorProfileBinding,
        effect: MemoryAccessEffect,
        mut namespaces: Vec<String>,
        mut authorized_stores: Vec<AuthorizedMemoryStore>,
        evaluated_at: u64,
    ) -> Self {
        namespaces.sort();
        namespaces.dedup();
        authorized_stores.sort_by(|left, right| left.store_id.cmp(&right.store_id));
        for store in &mut authorized_stores {
            store
                .grant_ids
                .sort_by(|left, right| left.as_str().cmp(right.as_str()));
            store.grant_ids.dedup();
        }
        let mut snapshot = Self {
            authorization_snapshot_id: Uuid::new_v4().to_string(),
            actor_id,
            actor_device_id,
            subject_profile_id,
            binding_id: binding.binding_id.clone(),
            binding_digest: binding.binding_digest.clone(),
            effect,
            namespaces,
            authorized_stores,
            evaluated_at,
            policy_version: AUTHORIZATION_POLICY_VERSION.to_string(),
            snapshot_digest: String::new(),
        };
        snapshot.snapshot_digest = snapshot.expected_digest();
        snapshot
    }

    pub fn expected_digest(&self) -> String {
        let stores = self
            .authorized_stores
            .iter()
            .map(|store| {
                format!(
                    "{}|{}|{}|{}|{}|{}|{}",
                    store.store_id,
                    store.profile_id,
                    store.owner_device_id,
                    store.namespace,
                    store.relative_path,
                    store
                        .grant_ids
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(","),
                    store.authorization_expires_at
                )
            })
            .collect::<Vec<_>>();
        digest_fields(
            "mnemes.authorization-snapshot.v1",
            [
                self.actor_id.to_string(),
                self.actor_device_id.to_string(),
                self.subject_profile_id.to_string(),
                self.binding_id.to_string(),
                self.binding_digest.clone(),
                self.effect.as_str().to_string(),
                self.namespaces.join(","),
                stores.join("\n"),
                self.evaluated_at.to_string(),
                self.policy_version.clone(),
            ],
        )
    }

    pub fn validate(&self) -> Result<(), MnemesError> {
        if self.policy_version != AUTHORIZATION_POLICY_VERSION {
            return Err(MnemesError::AuthorizationSnapshotInvalid(
                "unsupported authorization policy version".to_string(),
            ));
        }
        if self.snapshot_digest != self.expected_digest() {
            return Err(MnemesError::AuthorizationSnapshotInvalid(
                "snapshot digest does not match its authorization material".to_string(),
            ));
        }
        Ok(())
    }
}

/// Short-lived derived permit; it cannot exceed the snapshot that produced it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MemoryAccessPermit {
    pub permit_id: String,
    pub subject_profile_id: MemoryProfileId,
    pub actor_id: ActorId,
    pub actor_device_id: DeviceId,
    pub store_id: String,
    pub namespace: String,
    pub effect: MemoryAccessEffect,
    pub grant_ids: Vec<MemoryGrantId>,
    pub authorization_snapshot_id: String,
    pub authorization_snapshot_digest: String,
    pub issued_at: u64,
    pub expires_at: u64,
    pub query_budget: usize,
    pub policy_version: String,
    pub permit_digest: String,
}

impl MemoryAccessPermit {
    pub fn new(
        snapshot: &AuthorizationSnapshot,
        store: &AuthorizedMemoryStore,
        issued_at: u64,
        expires_at: u64,
        query_budget: usize,
    ) -> Result<Self, MnemesError> {
        if expires_at <= issued_at {
            return Err(MnemesError::AuthorizationSnapshotInvalid(
                "permit expires_at must be after issued_at".to_string(),
            ));
        }
        if query_budget == 0 || query_budget > MAX_PERMIT_QUERY_BUDGET {
            return Err(MnemesError::AuthorizationSnapshotInvalid(format!(
                "query budget must be within 1..={MAX_PERMIT_QUERY_BUDGET}"
            )));
        }
        let mut permit = Self {
            permit_id: Uuid::new_v4().to_string(),
            subject_profile_id: snapshot.subject_profile_id.clone(),
            actor_id: snapshot.actor_id.clone(),
            actor_device_id: snapshot.actor_device_id.clone(),
            store_id: store.store_id.clone(),
            namespace: store.namespace.clone(),
            effect: snapshot.effect,
            grant_ids: store.grant_ids.clone(),
            authorization_snapshot_id: snapshot.authorization_snapshot_id.clone(),
            authorization_snapshot_digest: snapshot.snapshot_digest.clone(),
            issued_at,
            expires_at,
            query_budget,
            policy_version: snapshot.policy_version.clone(),
            permit_digest: String::new(),
        };
        permit
            .grant_ids
            .sort_by(|left, right| left.as_str().cmp(right.as_str()));
        permit.permit_digest = digest_fields(
            "mnemes.memory-access-permit.v1",
            [
                permit.subject_profile_id.to_string(),
                permit.actor_id.to_string(),
                permit.actor_device_id.to_string(),
                permit.store_id.clone(),
                permit.namespace.clone(),
                permit.effect.as_str().to_string(),
                permit
                    .grant_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(","),
                permit.authorization_snapshot_digest.clone(),
                permit.issued_at.to_string(),
                permit.expires_at.to_string(),
                permit.query_budget.to_string(),
                permit.policy_version.clone(),
            ],
        );
        Ok(permit)
    }
}

/// Explicit, bounded authorization from one profile to another profile's store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MemoryAccessGrant {
    pub grant_id: MemoryGrantId,
    pub grantee_profile_id: MemoryProfileId,
    pub store_id: String,
    pub namespace: String,
    pub effect: MemoryAccessEffect,
    pub issued_by_actor_id: ActorId,
    pub valid_from: u64,
    pub expires_at: u64,
    pub revoked_at: Option<u64>,
    pub created_at: String,
}

impl MemoryAccessGrant {
    pub fn validate(&self) -> Result<(), MnemesError> {
        validate_text(&self.store_id, "store_id", MAX_ID_BYTES)?;
        validate_text(&self.namespace, "namespace", MAX_ID_BYTES)?;
        if self.expires_at <= self.valid_from {
            return Err(MnemesError::InvalidMemoryScope(
                "grant expires_at must be after valid_from".to_string(),
            ));
        }
        if self.revoked_at.is_some_and(|value| value < self.valid_from) {
            return Err(MnemesError::InvalidMemoryScope(
                "grant revoked_at cannot precede valid_from".to_string(),
            ));
        }
        Ok(())
    }

    pub fn allows(&self, effect: MemoryAccessEffect, namespace: &str, at: u64) -> bool {
        self.revoked_at.is_none()
            && self.effect == effect
            && self.namespace == namespace
            && at >= self.valid_from
            && at < self.expires_at
    }
}

/// Deterministic access decision; ranking or namespace discovery never grants access.
pub fn authorize_memory_access(
    requester_profile_id: &MemoryProfileId,
    profile: &MemoryProfile,
    store: &MemoryStoreIdentity,
    grants: &[MemoryAccessGrant],
    effect: MemoryAccessEffect,
    namespace: &str,
    at: u64,
) -> Result<(), MnemesError> {
    if profile.profile_id != *requester_profile_id {
        return Err(MnemesError::MemoryGrantDenied(
            "requesting profile identity does not match the authorization subject".to_string(),
        ));
    }
    if profile.status != MemoryProfileStatus::Active {
        return Err(MnemesError::MemoryGrantDenied(
            "requesting profile is not active".to_string(),
        ));
    }
    if store.status != MemoryStoreStatus::Active {
        return Err(MnemesError::MemoryGrantDenied(
            "target store is not active".to_string(),
        ));
    }
    if store.namespace != namespace {
        return Err(MnemesError::MemoryGrantDenied(
            "requested namespace is not the store namespace".to_string(),
        ));
    }
    if store.profile_id == *requester_profile_id {
        return Ok(());
    }
    if grants.iter().any(|grant| {
        grant.grantee_profile_id == *requester_profile_id
            && grant.store_id == store.store_id
            && grant.allows(effect, namespace, at)
    }) {
        return Ok(());
    }
    Err(MnemesError::MemoryGrantDenied(
        "no active grant covers the requested profile, store, namespace, effect, and time"
            .to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (MemoryProfile, MemoryProfile, MemoryStoreIdentity) {
        let owner = DeviceId::new();
        let requester = MemoryProfile::new(
            MemoryProfileId::new("requester").unwrap(),
            owner.clone(),
            "Requester",
        )
        .unwrap();
        let owner_profile =
            MemoryProfile::new(MemoryProfileId::new("owner").unwrap(), owner, "Owner").unwrap();
        let store = MemoryStoreIdentity::new(
            "store-owner",
            owner_profile.profile_id.clone(),
            owner_profile.owner_device_id.clone(),
            "private",
            "memory/profiles/owner/store-owner",
        )
        .unwrap();
        (requester, owner_profile, store)
    }

    #[test]
    fn own_store_is_authorized_but_other_profile_requires_a_grant() {
        let (requester, owner_profile, store) = fixture();
        assert!(authorize_memory_access(
            &owner_profile.profile_id,
            &owner_profile,
            &store,
            &[],
            MemoryAccessEffect::Search,
            "private",
            10,
        )
        .is_ok());
        assert!(matches!(
            authorize_memory_access(
                &requester.profile_id,
                &requester,
                &store,
                &[],
                MemoryAccessEffect::Search,
                "private",
                10,
            ),
            Err(MnemesError::MemoryGrantDenied(_))
        ));
    }

    #[test]
    fn grants_are_bounded_by_effect_namespace_and_time() {
        let (requester, _owner_profile, store) = fixture();
        let grant = MemoryAccessGrant {
            grant_id: MemoryGrantId::new(),
            grantee_profile_id: requester.profile_id.clone(),
            store_id: store.store_id.clone(),
            namespace: "private".to_string(),
            effect: MemoryAccessEffect::Search,
            issued_by_actor_id: ActorId::new(),
            valid_from: 10,
            expires_at: 20,
            revoked_at: None,
            created_at: String::new(),
        };

        assert!(authorize_memory_access(
            &requester.profile_id,
            &requester,
            &store,
            std::slice::from_ref(&grant),
            MemoryAccessEffect::Search,
            "private",
            10,
        )
        .is_ok());
        assert!(authorize_memory_access(
            &requester.profile_id,
            &requester,
            &store,
            std::slice::from_ref(&grant),
            MemoryAccessEffect::Read,
            "private",
            10,
        )
        .is_err());
        assert!(authorize_memory_access(
            &requester.profile_id,
            &requester,
            &store,
            std::slice::from_ref(&grant),
            MemoryAccessEffect::Search,
            "private",
            20,
        )
        .is_err());
        assert!(authorize_memory_access(
            &requester.profile_id,
            &requester,
            &store,
            std::slice::from_ref(&grant),
            MemoryAccessEffect::Search,
            "other",
            10,
        )
        .is_err());

        let mut revoked = grant;
        revoked.revoked_at = Some(15);
        assert!(authorize_memory_access(
            &requester.profile_id,
            &requester,
            &store,
            &[revoked],
            MemoryAccessEffect::Search,
            "private",
            10,
        )
        .is_err());
    }

    #[test]
    fn mismatched_profile_subject_is_rejected() {
        let (requester, owner_profile, store) = fixture();
        assert!(matches!(
            authorize_memory_access(
                &requester.profile_id,
                &owner_profile,
                &store,
                &[],
                MemoryAccessEffect::Search,
                "private",
                10,
            ),
            Err(MnemesError::MemoryGrantDenied(_))
        ));
    }

    #[test]
    fn store_paths_cannot_escape_the_store_root() {
        let result = MemoryStoreIdentity::new(
            "store",
            MemoryProfileId::new("profile").unwrap(),
            DeviceId::new(),
            "private",
            "memory/shards/../outside",
        );
        assert!(matches!(result, Err(MnemesError::InvalidMemoryScope(_))));
    }
}
