use mnemes::{
    merge_routed_results, rank_shards, Device, DeviceId, DeviceShard, DeviceStatus,
    MnemesError, MnemesStore, RoutedSearchResult, RoutingSearchRequest, ShardState,
};
use semantic_memory::{MockEmbedder, SearchResult, SearchSource};
use sha2::{Digest, Sha256};
use std::path::{Component, Path, PathBuf};
use tempfile::TempDir;

fn open_store(cache_capacity: usize) -> (TempDir, MnemesStore) {
    let temp = TempDir::new().unwrap();
    let base = temp.path().join("pooled-store");
    let store = MnemesStore::open_with_embedder_and_cache_capacity(
        base,
        semantic_memory::MemoryConfig {
            base_dir: temp.path().join("ignored-template-path"),
            ..Default::default()
        },
        Box::new(MockEmbedder::new(768)),
        cache_capacity,
    )
    .unwrap();
    (temp, store)
}

async fn register(store: &MnemesStore, label: &str) -> DeviceId {
    let id = DeviceId::new();
    store
        .register_device(Device::new(id.clone(), label, "linux", label))
        .await
        .unwrap();
    id
}

async fn seed(store: &MnemesStore, device_id: &DeviceId, namespace: &str, content: &str) {
    store
        .device_memory(device_id)
        .await
        .unwrap()
        .add_fact(namespace, content, None, None)
        .await
        .unwrap();
}

async fn refresh(
    store: &MnemesStore,
    device_id: &DeviceId,
    routing_terms: &str,
    namespaces: &[&str],
) {
    store
        .refresh_shard_summary(
            device_id,
            routing_terms,
            &namespaces
                .iter()
                .map(|value| (*value).to_string())
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn registration_creates_distinct_deterministic_safe_shard_paths() {
    let (temp, store) = open_store(4);
    let first = register(&store, "first").await;
    let second = register(&store, "second").await;

    let shards = store.list_shards().await.unwrap();
    assert_eq!(shards.len(), 2);
    assert_ne!(shards[0].relative_path, shards[1].relative_path);

    for device_id in [&first, &second] {
        let expected = PathBuf::from("memory")
            .join("shards")
            .join(device_id.as_str());
        let shard = shards
            .iter()
            .find(|shard| &shard.device_id == device_id)
            .unwrap();
        assert_eq!(shard.relative_path, expected);
        assert_eq!(
            store.device_shard_path(device_id),
            temp.path().join("pooled-store").join(expected)
        );
        assert!(store.device_shard_path(device_id).is_dir());
        assert!(shard.relative_path.components().all(|part| !matches!(
            part,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )));
        assert!(shard.relative_path.starts_with(Path::new("memory/shards")));
    }
}

#[tokio::test]
async fn top_one_routing_opens_and_searches_only_one_of_three_shards() {
    let (_temp, store) = open_store(2);
    let requester = register(&store, "requester").await;
    let target = register(&store, "target").await;
    let other = register(&store, "other").await;

    seed(&store, &target, "rust", "borrow checker ownership").await;
    seed(&store, &requester, "local", "unrelated local note").await;
    seed(&store, &other, "other", "unrelated remote note").await;
    refresh(&store, &requester, "local", &["local"]).await;
    refresh(&store, &target, "borrow checker", &["rust"]).await;
    refresh(&store, &other, "remote", &["other"]).await;
    store.clear_shard_cache().await;
    let before = store.shard_cache_metrics().await;

    let response = store
        .routed_search(
            &requester,
            RoutingSearchRequest::new("borrow checker", 1).with_shard_budget(1),
        )
        .await
        .unwrap();

    let after = store.shard_cache_metrics().await;
    assert_eq!(after.total_opens - before.total_opens, 1);
    assert_eq!(response.routing_receipt.selected_shards, vec![target]);
    assert_eq!(response.routing_receipt.outcomes.len(), 1);
    assert_eq!(response.results.len(), 1);
}

#[tokio::test]
async fn sparse_top_k_uses_summary_scores_and_persists_exact_skipped_set() {
    let (_temp, store) = open_store(4);
    let requester = register(&store, "requester").await;
    let strong = register(&store, "strong").await;
    let weak = register(&store, "weak").await;

    seed(&store, &requester, "local", "unrelated local note").await;
    seed(
        &store,
        &strong,
        "rust ownership",
        "borrow checker ownership",
    )
    .await;
    seed(&store, &weak, "rust", "rust note").await;
    refresh(&store, &requester, "local", &["local"]).await;
    refresh(
        &store,
        &strong,
        "borrow checker ownership",
        &["rust", "ownership"],
    )
    .await;
    refresh(&store, &weak, "rust", &["rust"]).await;
    store.clear_shard_cache().await;

    let before = store.shard_cache_metrics().await;
    let response = store
        .routed_search(
            &requester,
            RoutingSearchRequest::new("borrow checker ownership", 1).with_shard_budget(1),
        )
        .await
        .unwrap();
    let after = store.shard_cache_metrics().await;

    assert_eq!(after.total_opens - before.total_opens, 1);
    assert_eq!(response.results.len(), 1);
    assert_eq!(
        response.routing_receipt.selected_shards,
        vec![strong.clone()]
    );
    let mut expected_skipped = vec![weak.clone(), requester.clone()];
    expected_skipped.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    let mut actual_skipped = response.routing_receipt.skipped_shards.clone();
    actual_skipped.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    assert_eq!(actual_skipped, expected_skipped);
    assert_eq!(response.routing_receipt.actual_selected_shard_count, 1);
    assert_eq!(response.routing_receipt.outcomes.len(), 1);
    assert_eq!(response.routing_receipt.fallback_reason, None);
    assert!(
        response.routing_receipt.ranked_shards[0].score
            > response.routing_receipt.ranked_shards[1].score
    );

    let persisted = store
        .get_routing_receipt(&response.routing_receipt.receipt_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(persisted.selected_shards, vec![strong]);
    let mut persisted_skipped = persisted.skipped_shards.clone();
    persisted_skipped.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    assert_eq!(persisted_skipped, expected_skipped);
    assert_eq!(
        persisted.receipt_digest,
        response.routing_receipt.receipt_digest
    );
}

#[tokio::test]
async fn insufficient_results_expand_with_a_durable_fallback_record() {
    let (_temp, store) = open_store(4);
    let requester = register(&store, "requester").await;
    let second = register(&store, "second").await;
    let third = register(&store, "third").await;

    seed(&store, &requester, "alpha", "alpha first").await;
    seed(&store, &second, "alpha", "alpha second").await;
    seed(
        &store,
        &third,
        "other",
        "not selected before enough results",
    )
    .await;
    refresh(&store, &requester, "alpha", &["alpha"]).await;
    refresh(&store, &second, "alpha", &["alpha"]).await;
    refresh(&store, &third, "other", &["other"]).await;
    store.clear_shard_cache().await;

    let response = store
        .routed_search(
            &requester,
            RoutingSearchRequest::new("alpha", 2).with_shard_budget(1),
        )
        .await
        .unwrap();

    assert_eq!(response.results.len(), 2);
    assert_eq!(response.routing_receipt.outcomes.len(), 2);
    assert_eq!(
        response.routing_receipt.fallback_reason.as_deref(),
        Some("insufficient_results_expand")
    );
    assert!(response
        .routing_receipt
        .selected_shards
        .contains(&requester));
    assert!(response.routing_receipt.selected_shards.contains(&second));
    assert!(!response.routing_receipt.selected_shards.contains(&third));
    assert_eq!(response.routing_receipt.actual_selected_shard_count, 2);
}

#[tokio::test]
async fn exhaustive_mode_selects_every_eligible_shard() {
    let (_temp, store) = open_store(4);
    let requester = register(&store, "requester").await;
    let second = register(&store, "second").await;
    let third = register(&store, "third").await;

    let response = store
        .routed_search(
            &requester,
            RoutingSearchRequest::new("anything", 1).exhaustive(),
        )
        .await
        .unwrap();

    assert!(response.routing_receipt.exhaustive);
    assert_eq!(response.routing_receipt.eligible_shards.len(), 3);
    assert_eq!(response.routing_receipt.selected_shards.len(), 3);
    assert_eq!(response.routing_receipt.outcomes.len(), 3);
    assert!(response
        .routing_receipt
        .selected_shards
        .contains(&requester));
    assert!(response.routing_receipt.selected_shards.contains(&second));
    assert!(response.routing_receipt.selected_shards.contains(&third));
}

#[tokio::test]
async fn revoked_and_quarantined_shards_are_masked_before_scoring_or_opening() {
    let (_temp, store) = open_store(2);
    let requester = register(&store, "requester").await;
    let revoked = register(&store, "revoked").await;
    let quarantined = register(&store, "quarantined").await;
    let active = register(&store, "active").await;
    refresh(&store, &revoked, "needle needle needle", &["needle"]).await;
    refresh(&store, &quarantined, "needle needle needle", &["needle"]).await;
    refresh(&store, &active, "needle", &["needle"]).await;
    store
        .device_memory(&active)
        .await
        .unwrap()
        .add_fact("needle", "needle evidence", None, None)
        .await
        .unwrap();
    store.revoke_device(&revoked).await.unwrap();
    store
        .set_device_status(&quarantined, DeviceStatus::Quarantined)
        .await
        .unwrap();
    store.clear_shard_cache().await;
    let before = store.shard_cache_metrics().await;

    let response = store
        .routed_search(
            &requester,
            RoutingSearchRequest::new("needle", 1).with_shard_budget(1),
        )
        .await
        .unwrap();
    let after = store.shard_cache_metrics().await;

    assert_eq!(after.total_opens - before.total_opens, 1);
    assert!(!response.routing_receipt.eligible_shards.contains(&revoked));
    assert!(!response
        .routing_receipt
        .eligible_shards
        .contains(&quarantined));
    assert_eq!(response.routing_receipt.selected_shards, vec![active]);
}

#[test]
fn stable_tie_breaking_uses_device_uuid_ascending() {
    let requester = DeviceId::parse("00000000-0000-4000-8000-000000000003").unwrap();
    let low = DeviceId::parse("00000000-0000-4000-8000-000000000001").unwrap();
    let high = DeviceId::parse("00000000-0000-4000-8000-000000000002").unwrap();
    let shards = vec![
        DeviceShard::active(
            high.clone(),
            "memory/shards/00000000-0000-4000-8000-000000000002",
        ),
        DeviceShard::active(
            low.clone(),
            "memory/shards/00000000-0000-4000-8000-000000000001",
        ),
    ];

    let ranked = rank_shards("same", &requester, &shards);
    assert_eq!(ranked[0].device_id, low);
    assert_eq!(ranked[1].device_id, high);
}

#[tokio::test]
async fn routing_receipt_is_persisted_without_the_raw_query() {
    let (temp, store) = open_store(2);
    let requester = register(&store, "requester").await;
    let secret_query = "private raw query 9f5f31";

    let response = store
        .routed_search(&requester, RoutingSearchRequest::new(secret_query, 1))
        .await
        .unwrap();
    let persisted = store
        .get_routing_receipt(&response.routing_receipt.receipt_id)
        .await
        .unwrap()
        .unwrap();

    let expected_hash = format!("{:x}", Sha256::digest(secret_query.as_bytes()));
    assert_eq!(persisted.query_sha256, expected_hash);
    assert!(!serde_json::to_string(&persisted)
        .unwrap()
        .contains(secret_query));

    drop(store);
    let conn = rusqlite::Connection::open(temp.path().join("pooled-store/pooled.db")).unwrap();
    let raw: String = conn
        .query_row(
            "SELECT query_sha256 || eligible_shards_json || ranked_shards_json || selected_shards_json || skipped_shards_json || outcomes_json || COALESCE(fallback_reason, '') || final_result_ids_json || merge_digest FROM shard_routing_receipts WHERE receipt_id = ?1",
            [response.routing_receipt.receipt_id],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!raw.contains(secret_query));
}

#[test]
fn conflicting_content_for_the_same_canonical_item_id_fails_closed() {
    let first_device = DeviceId::new();
    let second_device = DeviceId::new();
    let make = |device_id: DeviceId, content: &str| RoutedSearchResult {
        result: SearchResult {
            content: content.to_string(),
            source: SearchSource::Fact {
                fact_id: "same-id".to_string(),
                namespace: "n".to_string(),
            },
            score: 1.0,
            bm25_rank: Some(1),
            vector_rank: None,
            cosine_similarity: None,
        },
        device_id,
        shard_generation: 1,
        child_receipt_id: Some("child".to_string()),
    };

    let error = merge_routed_results(
        vec![make(first_device, "one"), make(second_device, "two")],
        10,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        MnemesError::ConflictingShardItem { .. }
    ));
}

#[test]
fn conflicting_content_after_top_k_still_fails_closed() {
    let make = |fact_id: &str, content: &str, score: f64| RoutedSearchResult {
        result: SearchResult {
            content: content.to_string(),
            source: SearchSource::Fact {
                fact_id: fact_id.to_string(),
                namespace: "n".to_string(),
            },
            score,
            bm25_rank: None,
            vector_rank: None,
            cosine_similarity: None,
        },
        device_id: DeviceId::new(),
        shard_generation: 1,
        child_receipt_id: Some("child".to_string()),
    };
    let error = merge_routed_results(
        vec![
            make("same-id", "one", 3.0),
            make("other-id", "other", 2.0),
            make("same-id", "two", 1.0),
        ],
        1,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        MnemesError::ConflictingShardItem { .. }
    ));
}

#[tokio::test]
async fn tampered_routing_receipt_is_rejected_on_read() {
    let (temp, store) = open_store(2);
    let requester = register(&store, "requester").await;
    let response = store
        .routed_search(&requester, RoutingSearchRequest::new("query", 1))
        .await
        .unwrap();
    let mut tampered = response.routing_receipt.clone();
    tampered.merge_digest = "f".repeat(64);
    let material = serde_json::to_string(&(
        "pooled-routing-receipt-v1",
        &tampered.receipt_id,
        &tampered.requester_device_id,
        &tampered.query_sha256,
        tampered.shard_budget,
        tampered.actual_selected_shard_count,
        tampered.exhaustive,
        &tampered.eligible_shards,
        &tampered.ranked_shards,
        &tampered.selected_shards,
        &tampered.skipped_shards,
        &tampered.outcomes,
        &tampered.fallback_reason,
        &tampered.final_result_ids,
        &tampered.merge_digest,
        &tampered.recorded_at,
    ))
    .unwrap();
    tampered.receipt_digest = format!("{:x}", Sha256::digest(material.as_bytes()));
    let conn = rusqlite::Connection::open(temp.path().join("pooled-store/pooled.db")).unwrap();
    conn.execute(
        "UPDATE shard_routing_receipts
         SET merge_digest = ?1, receipt_digest = ?2
         WHERE receipt_id = ?3",
        rusqlite::params![
            tampered.merge_digest,
            tampered.receipt_digest,
            response.routing_receipt.receipt_id
        ],
    )
    .unwrap();
    drop(conn);
    assert!(store
        .get_routing_receipt(&response.routing_receipt.receipt_id)
        .await
        .is_err());
}

#[test]
fn legacy_global_memory_db_in_active_tree_is_rejected() {
    let temp = tempfile::TempDir::new().unwrap();
    let base = temp.path().join("pooled-store");
    std::fs::create_dir_all(base.join("memory")).unwrap();
    std::fs::write(base.join("memory/memory.db"), b"legacy").unwrap();
    let result = MnemesStore::open_with_embedder(
        base,
        semantic_memory::MemoryConfig::default(),
        Box::new(semantic_memory::MockEmbedder::new(768)),
    );
    let error = match result {
        Ok(_) => panic!("legacy global store must be rejected"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("legacy global"));
}

#[test]
fn unsupported_pooled_schema_generation_is_rejected() {
    let temp = tempfile::TempDir::new().unwrap();
    let base = temp.path().join("pooled-store");
    let store = MnemesStore::open_with_embedder(
        base.clone(),
        semantic_memory::MemoryConfig::default(),
        Box::new(semantic_memory::MockEmbedder::new(768)),
    )
    .unwrap();
    drop(store);
    let conn = rusqlite::Connection::open(base.join("pooled.db")).unwrap();
    conn.execute("UPDATE _pooled_schema_version SET version = 99", [])
        .unwrap();
    conn.execute("CREATE TABLE future_generation_sentinel(value TEXT)", [])
        .unwrap();
    drop(conn);
    let result = MnemesStore::open_with_embedder(
        base,
        semantic_memory::MemoryConfig::default(),
        Box::new(semantic_memory::MockEmbedder::new(768)),
    );
    assert!(result.is_err());
    let conn = rusqlite::Connection::open(temp.path().join("pooled-store/pooled.db")).unwrap();
    let versions = conn
        .prepare("SELECT version FROM _pooled_schema_version ORDER BY version")
        .unwrap()
        .query_map([], |row| row.get::<_, i64>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(versions, vec![99]);
    let sentinel_exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='future_generation_sentinel'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(sentinel_exists, 1);
}

#[tokio::test]
async fn open_store_cache_never_exceeds_its_configured_bound() {
    let (_temp, store) = open_store(2);
    let one = register(&store, "one").await;
    let two = register(&store, "two").await;
    let three = register(&store, "three").await;

    store.device_memory(&one).await.unwrap();
    store.device_memory(&two).await.unwrap();
    store.device_memory(&three).await.unwrap();

    let metrics = store.shard_cache_metrics().await;
    assert_eq!(metrics.capacity, 2);
    assert_eq!(metrics.len, 2);
    assert!(metrics.len <= metrics.capacity);
    assert_eq!(metrics.total_opens, 3);
}

#[tokio::test]
async fn device_status_changes_are_mirrored_to_shard_state() {
    let (_temp, store) = open_store(2);
    let device = register(&store, "device").await;
    store
        .set_device_status(&device, DeviceStatus::Quarantined)
        .await
        .unwrap();
    let shard = store
        .list_shards()
        .await
        .unwrap()
        .into_iter()
        .find(|shard| shard.device_id == device)
        .unwrap();
    assert_eq!(shard.state, ShardState::Quarantined);
}
