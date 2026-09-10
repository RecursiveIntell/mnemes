use mnemes::semantic_memory::{MemoryConfig, MemoryStore, MockEmbedder};
use mnemes::{
    canonical_memory_store_relative_path, Device, DeviceId, MemoryProfile, MemoryProfileId,
    MemoryStoreIdentity, MnemesStore,
};
use tempfile::TempDir;

fn config(base_dir: std::path::PathBuf) -> MemoryConfig {
    MemoryConfig {
        base_dir,
        ..Default::default()
    }
}

#[tokio::test]
async fn profile_store_opens_only_at_canonical_path_and_is_query_only() {
    let temp = TempDir::new().unwrap();
    let base = temp.path().join("pooled-store");
    let profile_store = MnemesStore::open_with_embedder(
        base.clone(),
        config(base.clone()),
        Box::new(MockEmbedder::new(768)),
    )
    .unwrap();
    let device_id = DeviceId::new();
    profile_store
        .register_device(Device::new(
            device_id.clone(),
            "owner",
            "linux",
            "owner-host",
        ))
        .await
        .unwrap();
    let profile = MemoryProfile::new(
        MemoryProfileId::new("owner-profile").unwrap(),
        device_id.clone(),
        "Owner profile",
    )
    .unwrap();
    profile_store
        .register_memory_profile(profile.clone())
        .await
        .unwrap();
    let store_id = "owner-store";
    let relative_path =
        canonical_memory_store_relative_path(&profile.profile_id, store_id).unwrap();
    let physical = MemoryStore::open_with_embedder(
        config(base.join(&relative_path)),
        Box::new(MockEmbedder::new(768)),
    )
    .unwrap();
    physical
        .add_fact("private", "profile-owned evidence", None, None)
        .await
        .unwrap();
    drop(physical);

    let identity = MemoryStoreIdentity::new(
        store_id,
        profile.profile_id,
        device_id,
        "private",
        relative_path,
    )
    .unwrap();
    profile_store.register_memory_store(identity).await.unwrap();

    let query_store = profile_store.profile_store_memory(store_id).await.unwrap();
    let results = query_store
        .search("profile-owned evidence", Some(1), None, None)
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert!(query_store
        .add_fact("private", "must not persist", None, None)
        .await
        .is_err());
}
