use mnemes::semantic_memory::{MemoryConfig, MemoryStore, MockEmbedder};
use mnemes::{
    canonical_memory_store_relative_path, Actor, ActorId, ActorKind, ActorProfileBinding, Device,
    DeviceId, MemoryAccessEffect, MemoryAccessGrant, MemoryGrantId, MemoryProfile, MemoryProfileId,
    MemoryStoreIdentity, MnemesError, MnemesStore, RoutingSearchRequest, ToolProfile,
};
use tempfile::TempDir;

fn config(base_dir: std::path::PathBuf) -> MemoryConfig {
    MemoryConfig {
        base_dir,
        ..Default::default()
    }
}

fn open_store(base: std::path::PathBuf) -> MnemesStore {
    MnemesStore::open_with_embedder(base.clone(), config(base), Box::new(MockEmbedder::new(768)))
        .unwrap()
}

async fn register_operator(store: &MnemesStore, device_id: DeviceId) -> ActorId {
    let actor_id = ActorId::new();
    let mut actor = Actor::new(actor_id.clone(), device_id, ActorKind::Human);
    actor.tool_profile = ToolProfile::Operator;
    store.register_actor(actor).await.unwrap();
    actor_id
}

async fn register_profile_store(
    store: &MnemesStore,
    base: &std::path::Path,
    profile: &MemoryProfile,
    store_id: &str,
    content: Option<&str>,
) {
    let relative_path =
        canonical_memory_store_relative_path(&profile.profile_id, store_id).unwrap();
    if let Some(content) = content {
        let physical = MemoryStore::open_with_embedder(
            config(base.join(&relative_path)),
            Box::new(MockEmbedder::new(768)),
        )
        .unwrap();
        physical
            .add_fact("private", content, None, None)
            .await
            .unwrap();
    }
    store
        .register_memory_store(
            MemoryStoreIdentity::new(
                store_id,
                profile.profile_id.clone(),
                profile.owner_device_id.clone(),
                "private",
                relative_path,
            )
            .unwrap(),
        )
        .await
        .unwrap();
}

struct Fixture {
    _temp: TempDir,
    base: std::path::PathBuf,
    store: MnemesStore,
    requester_actor: ActorId,
    requester_profile: MemoryProfile,
    operator: ActorId,
}

async fn fixture() -> Fixture {
    let temp = TempDir::new().unwrap();
    let base = temp.path().join("pooled-store");
    let store = open_store(base.clone());
    let requester_device = DeviceId::new();
    store
        .register_device(Device::new(
            requester_device.clone(),
            "requester",
            "linux",
            "requester-host",
        ))
        .await
        .unwrap();
    let operator = register_operator(&store, requester_device.clone()).await;
    let requester_actor = ActorId::new();
    store
        .register_actor(Actor::new(
            requester_actor.clone(),
            requester_device.clone(),
            ActorKind::Hermes,
        ))
        .await
        .unwrap();
    let requester_profile = MemoryProfile::new(
        MemoryProfileId::new("requester-profile").unwrap(),
        requester_device.clone(),
        "Requester",
    )
    .unwrap();
    store
        .register_memory_profile(requester_profile.clone())
        .await
        .unwrap();
    store
        .bind_actor_profile(
            ActorProfileBinding::new(
                requester_actor.clone(),
                requester_profile.profile_id.clone(),
                requester_device,
                operator.clone(),
                1,
                1_000,
                1,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    Fixture {
        _temp: temp,
        base,
        store,
        requester_actor,
        requester_profile,
        operator,
    }
}

#[tokio::test]
async fn unbound_actor_is_denied_before_profile_store_opening() {
    let fixture = fixture().await;
    register_profile_store(
        &fixture.store,
        &fixture.base,
        &fixture.requester_profile,
        "own-store",
        Some("OWN_SECRET"),
    )
    .await;
    let unbound = ActorId::new();
    fixture
        .store
        .register_actor(Actor::new(
            unbound.clone(),
            fixture.requester_profile.owner_device_id.clone(),
            ActorKind::Hermes,
        ))
        .await
        .unwrap();
    let before = fixture.store.shard_cache_metrics().await.total_opens;
    let error = fixture
        .store
        .routed_search_for_profile(&unbound, RoutingSearchRequest::new("OWN_SECRET", 1), 10)
        .await
        .unwrap_err();
    assert!(matches!(error, MnemesError::ActorProfileBindingDenied(_)));
    assert_eq!(
        fixture.store.shard_cache_metrics().await.total_opens,
        before
    );
}

#[tokio::test]
async fn foreign_store_is_not_opened_without_grant_then_grant_and_revocation_apply_per_request() {
    let fixture = fixture().await;
    let foreign_device = DeviceId::new();
    fixture
        .store
        .register_device(Device::new(
            foreign_device.clone(),
            "foreign",
            "linux",
            "foreign-host",
        ))
        .await
        .unwrap();
    let foreign_profile = MemoryProfile::new(
        MemoryProfileId::new("foreign-profile").unwrap(),
        foreign_device,
        "Foreign",
    )
    .unwrap();
    fixture
        .store
        .register_memory_profile(foreign_profile.clone())
        .await
        .unwrap();
    register_profile_store(
        &fixture.store,
        &fixture.base,
        &foreign_profile,
        "foreign-store",
        Some("FOREIGN_SECRET"),
    )
    .await;

    let denied = fixture
        .store
        .routed_search_for_profile(
            &fixture.requester_actor,
            RoutingSearchRequest::new("FOREIGN_SECRET", 1),
            10,
        )
        .await
        .unwrap();
    assert!(denied.results.is_empty());
    assert!(denied.routing_receipt.authorized_stores.is_empty());
    assert_eq!(fixture.store.shard_cache_metrics().await.total_opens, 0);

    let grant = MemoryAccessGrant {
        grant_id: MemoryGrantId::new(),
        grantee_profile_id: fixture.requester_profile.profile_id.clone(),
        store_id: "foreign-store".to_string(),
        namespace: "private".to_string(),
        effect: MemoryAccessEffect::Search,
        issued_by_actor_id: fixture.operator.clone(),
        valid_from: 1,
        expires_at: 1_000,
        revoked_at: None,
        created_at: String::new(),
    };
    let grant_id = grant.grant_id.clone();
    fixture.store.grant_memory_access(grant).await.unwrap();
    let granted = fixture
        .store
        .routed_search_for_profile(
            &fixture.requester_actor,
            RoutingSearchRequest::new("FOREIGN_SECRET", 1),
            10,
        )
        .await
        .unwrap();
    assert_eq!(granted.results.len(), 1);
    assert_eq!(granted.results[0].store_id, "foreign-store");
    fixture
        .store
        .revoke_memory_access(&grant_id, 11)
        .await
        .unwrap();
    let revoked = fixture
        .store
        .routed_search_for_profile(
            &fixture.requester_actor,
            RoutingSearchRequest::new("FOREIGN_SECRET", 1),
            12,
        )
        .await
        .unwrap();
    assert!(revoked.results.is_empty());
    assert!(revoked.routing_receipt.authorized_stores.is_empty());
}

#[tokio::test]
async fn partial_failure_is_explicit_and_global_receipt_reads_back_with_validation() {
    let fixture = fixture().await;
    register_profile_store(
        &fixture.store,
        &fixture.base,
        &fixture.requester_profile,
        "good-store",
        Some("GOOD_SECRET"),
    )
    .await;
    register_profile_store(
        &fixture.store,
        &fixture.base,
        &fixture.requester_profile,
        "missing-store",
        None,
    )
    .await;
    let response = fixture
        .store
        .routed_search_for_profile(
            &fixture.requester_actor,
            RoutingSearchRequest::new("GOOD_SECRET", 5),
            10,
        )
        .await
        .unwrap();
    assert_eq!(response.results.len(), 1);
    assert!(!response.routing_receipt.complete);
    assert!(response
        .routing_receipt
        .outcomes
        .iter()
        .any(|outcome| outcome.store_id == "missing-store" && outcome.error.is_some()));
    let receipt = fixture
        .store
        .get_profile_routing_receipt(&response.routing_receipt.receipt_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        receipt.receipt_digest,
        response.routing_receipt.receipt_digest
    );
    assert_eq!(
        receipt.final_result_ids,
        response.routing_receipt.final_result_ids
    );
}
