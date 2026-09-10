use mnemes::semantic_memory::{MemoryConfig, MockEmbedder};
use mnemes::{
    Actor, ActorId, ActorKind, ActorProfileBinding, Device, DeviceId, MemoryAccessEffect,
    MemoryProfile, MemoryProfileId, MnemesStore, ToolProfile,
};
use tempfile::TempDir;

fn open_store(base: std::path::PathBuf) -> MnemesStore {
    MnemesStore::open_with_embedder(
        base.clone(),
        MemoryConfig {
            base_dir: base,
            ..Default::default()
        },
        Box::new(MockEmbedder::new(768)),
    )
    .unwrap()
}

async fn register_operator(store: &MnemesStore, device_id: DeviceId) -> ActorId {
    store
        .register_device(Device::new(
            device_id.clone(),
            "owner",
            "linux",
            "owner-host",
        ))
        .await
        .unwrap();
    let actor_id = ActorId::new();
    let mut actor = Actor::new(actor_id.clone(), device_id, ActorKind::Human);
    actor.tool_profile = ToolProfile::Operator;
    store.register_actor(actor).await.unwrap();
    actor_id
}

#[tokio::test]
async fn unbound_actor_and_cross_device_binding_fail_closed() {
    let temp = TempDir::new().unwrap();
    let store = open_store(temp.path().join("pooled-store"));
    let owner_device = DeviceId::new();
    let operator_id = register_operator(&store, owner_device.clone()).await;
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
    let requester_actor_id = ActorId::new();
    store
        .register_actor(Actor::new(
            requester_actor_id.clone(),
            requester_device.clone(),
            ActorKind::Hermes,
        ))
        .await
        .unwrap();
    let profile = MemoryProfile::new(
        MemoryProfileId::new("owner-profile").unwrap(),
        owner_device,
        "Owner profile",
    )
    .unwrap();
    store
        .register_memory_profile(profile.clone())
        .await
        .unwrap();

    assert!(store
        .build_authorization_snapshot(&requester_actor_id, MemoryAccessEffect::Search, None, 10,)
        .await
        .is_err());

    let cross_device = ActorProfileBinding::new(
        requester_actor_id,
        profile.profile_id,
        requester_device,
        operator_id,
        10,
        20,
        1,
    )
    .unwrap();
    assert!(store.bind_actor_profile(cross_device).await.is_err());
}

#[tokio::test]
async fn overlapping_bindings_fail_and_valid_binding_survives_reopen() {
    let temp = TempDir::new().unwrap();
    let base = temp.path().join("pooled-store");
    let store = open_store(base.clone());
    let owner_device = DeviceId::new();
    let operator_id = register_operator(&store, owner_device.clone()).await;
    let actor_id = ActorId::new();
    store
        .register_actor(Actor::new(
            actor_id.clone(),
            owner_device.clone(),
            ActorKind::Hermes,
        ))
        .await
        .unwrap();
    let profile = MemoryProfile::new(
        MemoryProfileId::new("bound-profile").unwrap(),
        owner_device.clone(),
        "Bound profile",
    )
    .unwrap();
    store
        .register_memory_profile(profile.clone())
        .await
        .unwrap();

    let first = ActorProfileBinding::new(
        actor_id.clone(),
        profile.profile_id.clone(),
        owner_device.clone(),
        operator_id.clone(),
        10,
        30,
        1,
    )
    .unwrap();
    let first_id = first.binding_id.clone();
    store.bind_actor_profile(first).await.unwrap();

    let overlap = ActorProfileBinding::new(
        actor_id.clone(),
        profile.profile_id,
        owner_device,
        operator_id,
        20,
        40,
        2,
    )
    .unwrap();
    assert!(store.bind_actor_profile(overlap).await.is_err());
    drop(store);

    let reopened = open_store(base);
    let binding = reopened.resolve_actor_profile(&actor_id, 15).await.unwrap();
    assert_eq!(binding.binding_id, first_id);
    assert_eq!(binding.binding_digest.len(), 64);
}
