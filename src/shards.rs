//! Device-shard catalog types plus deterministic routing and merge helpers.

use crate::{DeviceId, DeviceStatus, MnemesError};
use semantic_memory::{SearchResult, SearchSourceType};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

/// Lifecycle state of one cataloged semantic-memory shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShardState {
    Active,
    Quarantined,
    Revoked,
}

impl ShardState {
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
            other => Err(MnemesError::InvalidShardCatalog(format!(
                "invalid shard state '{other}'"
            ))),
        }
    }
}

impl From<DeviceStatus> for ShardState {
    fn from(value: DeviceStatus) -> Self {
        match value {
            DeviceStatus::Active => Self::Active,
            DeviceStatus::Quarantined => Self::Quarantined,
            DeviceStatus::Revoked => Self::Revoked,
        }
    }
}

/// Derived routing catalog row for a device-owned shard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceShard {
    pub device_id: DeviceId,
    pub relative_path: PathBuf,
    pub state: ShardState,
    pub generation: u64,
    pub routing_terms: String,
    pub namespaces: Vec<String>,
    pub fact_count: u64,
    pub document_count: u64,
    pub chunk_count: u64,
    pub message_count: u64,
    pub search_count: u64,
    pub ewma_latency_ms: f64,
    pub last_refreshed_at: Option<String>,
    pub created_at: String,
    /// Joined device state used by the policy mask; it is not duplicated in the catalog table.
    pub device_status: DeviceStatus,
}

impl DeviceShard {
    /// Minimal active row useful for pure routing tests.
    pub fn active(device_id: DeviceId, relative_path: impl Into<PathBuf>) -> Self {
        Self {
            device_id,
            relative_path: relative_path.into(),
            state: ShardState::Active,
            generation: 1,
            routing_terms: String::new(),
            namespaces: Vec::new(),
            fact_count: 0,
            document_count: 0,
            chunk_count: 0,
            message_count: 0,
            search_count: 0,
            ewma_latency_ms: 0.0,
            last_refreshed_at: None,
            created_at: String::new(),
            device_status: DeviceStatus::Active,
        }
    }

    pub(crate) fn eligible(&self) -> bool {
        self.state == ShardState::Active && self.device_status == DeviceStatus::Active
    }
}

/// One deterministic catalog ranking decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RankedShard {
    pub device_id: DeviceId,
    pub score: u64,
}

/// Lowercase ASCII-alphanumeric tokenizer used only for advisory shard routing.
pub fn routing_tokens(value: &str) -> Vec<String> {
    let mut tokens = value
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>();
    tokens.sort();
    tokens.dedup();
    tokens
}

/// Apply the active policy mask, score sparse overlap plus locality, and sort stably.
pub fn rank_shards(
    query: &str,
    requester_device_id: &DeviceId,
    shards: &[DeviceShard],
) -> Vec<RankedShard> {
    let query_terms = routing_tokens(query).into_iter().collect::<HashSet<_>>();
    let mut ranked = shards
        .iter()
        .filter(|shard| shard.eligible())
        .map(|shard| {
            let mut catalog_terms = routing_tokens(&shard.routing_terms)
                .into_iter()
                .collect::<HashSet<_>>();
            for namespace in &shard.namespaces {
                catalog_terms.extend(routing_tokens(namespace));
            }
            let overlap = query_terms.intersection(&catalog_terms).count() as u64;
            let locality = u64::from(&shard.device_id == requester_device_id);
            RankedShard {
                device_id: shard.device_id.clone(),
                score: overlap.saturating_mul(2).saturating_add(locality),
            }
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.device_id.as_str().cmp(right.device_id.as_str()))
    });
    ranked
}

/// Routed search request with safe sparse defaults.
#[derive(Debug, Clone)]
pub struct RoutingSearchRequest {
    pub query: String,
    pub top_k: usize,
    pub namespaces: Option<Vec<String>>,
    pub source_types: Option<Vec<SearchSourceType>>,
    pub shard_budget: Option<usize>,
    pub exhaustive: bool,
}

impl RoutingSearchRequest {
    pub fn new(query: impl Into<String>, top_k: usize) -> Self {
        Self {
            query: query.into(),
            top_k: top_k.max(1),
            namespaces: None,
            source_types: None,
            shard_budget: None,
            exhaustive: false,
        }
    }

    pub fn with_shard_budget(mut self, shard_budget: usize) -> Self {
        self.shard_budget = Some(shard_budget.max(1));
        self
    }

    pub fn exhaustive(mut self) -> Self {
        self.exhaustive = true;
        self
    }
}

/// One global result annotated with its owning shard and child receipt.
#[derive(Debug, Clone, Serialize)]
pub struct RoutedSearchResult {
    pub result: SearchResult,
    pub device_id: DeviceId,
    pub shard_generation: u64,
    pub child_receipt_id: Option<String>,
}

/// Merge shard results by score and canonical owner ID, failing on ID/content conflict.
pub fn merge_routed_results(
    mut results: Vec<RoutedSearchResult>,
    top_k: usize,
) -> Result<Vec<RoutedSearchResult>, MnemesError> {
    results.sort_by(|left, right| {
        right
            .result
            .score
            .total_cmp(&left.result.score)
            .then_with(|| {
                left.result
                    .source
                    .result_id()
                    .cmp(&right.result.source.result_id())
            })
    });

    let mut seen = HashMap::<String, String>::new();
    for result in &results {
        let item_id = result.result.source.result_id();
        if let Some(existing_content) = seen.get(&item_id) {
            if existing_content != &result.result.content {
                return Err(MnemesError::ConflictingShardItem { item_id });
            }
        } else {
            seen.insert(item_id, result.result.content.clone());
        }
    }

    let mut emitted = HashSet::<String>::new();
    let mut merged = Vec::with_capacity(results.len().min(top_k));
    for result in results {
        let item_id = result.result.source.result_id();
        if !emitted.insert(item_id) {
            continue;
        }
        merged.push(result);
        if merged.len() == top_k {
            break;
        }
    }
    Ok(merged)
}

/// Per-shard execution evidence stored in the mnemes routing receipt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardSearchOutcome {
    pub device_id: DeviceId,
    pub shard_generation: u64,
    pub latency_ms: u64,
    pub result_count: usize,
    pub child_search_receipt_id: Option<String>,
    pub error: Option<String>,
}

/// Typed durable receipt for one sparse or exhaustive routed search.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardRoutingReceipt {
    pub receipt_id: String,
    pub requester_device_id: DeviceId,
    pub query_sha256: String,
    pub shard_budget: usize,
    pub actual_selected_shard_count: usize,
    pub exhaustive: bool,
    pub eligible_shards: Vec<DeviceId>,
    pub ranked_shards: Vec<RankedShard>,
    pub selected_shards: Vec<DeviceId>,
    pub skipped_shards: Vec<DeviceId>,
    pub outcomes: Vec<ShardSearchOutcome>,
    pub fallback_reason: Option<String>,
    pub final_result_ids: Vec<String>,
    pub merge_digest: String,
    pub receipt_digest: String,
    pub recorded_at: String,
}

/// Global routed response. Child receipts remain attached to individual results/outcomes.
#[derive(Debug, Clone, Serialize)]
pub struct RoutedSearchResponse {
    pub results: Vec<RoutedSearchResult>,
    pub routing_receipt: ShardRoutingReceipt,
}

/// Observable bounded-cache state without opening any shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ShardCacheMetrics {
    pub len: usize,
    pub capacity: usize,
    pub total_opens: u64,
}

/// Catalog-only aggregate counts; reading it never opens shard databases.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ShardAggregateStats {
    pub shards: u64,
    pub facts: u64,
    pub documents: u64,
    pub chunks: u64,
    pub messages: u64,
    pub searches: u64,
}

/// Explicit integrity status for one semantic-memory shard.
#[derive(Debug, Clone, Serialize)]
pub struct ShardIntegrityStatus {
    pub device_id: DeviceId,
    pub relative_path: PathBuf,
    pub status: String,
    pub detail: String,
}
