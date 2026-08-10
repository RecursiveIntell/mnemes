use hmac::Mac;
use mnemes::{
    Actor, ActorId, ActorKind, AsOf, Device, DeviceId, MnemesError, MnemesStore,
    RunPackEvidenceProjectionV1,
};
use semantic_memory::{EmbeddingConfig, MemoryConfig, MockEmbedder};
use serde_json::{json, Value};
use sha2::Digest;
use tempfile::TempDir;

async fn open_authenticated_store() -> (MnemesStore, String, TempDir) {
    let directory = TempDir::new().expect("temporary directory");
    let config = MemoryConfig {
        base_dir: directory.path().to_path_buf(),
        embedding: EmbeddingConfig {
            dimensions: 768,
            ..Default::default()
        },
        ..Default::default()
    };
    let store = MnemesStore::open_with_embedder(
        directory.path().to_path_buf(),
        config,
        Box::new(MockEmbedder::new(768)),
    )
    .expect("store");
    let device_id = DeviceId::new();
    let (_, credential) = store
        .register_device_with_generated_credential(Device::new(
            device_id.clone(),
            "run-pack-importer-test",
            "linux",
            "localhost",
        ))
        .await
        .expect("device registration");
    store
        .register_actor(Actor::new(ActorId::new(), device_id, ActorKind::Hermes))
        .await
        .expect("actor registration");
    (store, credential, directory)
}

fn valid_projection() -> Value {
    serde_json::from_slice(include_bytes!(
        "../fixtures/witnessed-workbench/run-pack-evidence-projection-v1.json"
    ))
    .expect("frozen Recursive Agent projection fixture")
}

fn bytes(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).expect("fixture serialization")
}

fn witnessed_projection() -> Vec<u8> {
    let projection = valid_projection();
    let typed: RunPackEvidenceProjectionV1 = serde_json::from_value(projection.clone()).unwrap();
    let canonical = serde_json::to_vec(&typed).unwrap();
    let mut witness = json!({"format":"mnemes.run-pack-admission-witness/v1","canonical_projection_digest":format!("sha256:{}", hex::encode(sha2::Sha256::digest(&canonical))),"pack_manifest_digest":"9f64acec41bde3a3f6d6a5b25c4928aa9b39eb23fdbbc7a1ce3bfe7538cafe9b","pack_content_digest":"ec192a928263b72f747fc3c40dfa0d11621171fb40d429a3d5ffb3a760102729","verification_receipt_digest":"eb0895ac3ebb64973a1844b314721c43921dab55b9eb8471c03467b1656894b2","verified":true});
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(b"test-key").unwrap();
    mac.update(&serde_json::to_vec(&witness).unwrap());
    witness["signature"] = json!(hex::encode(mac.finalize().into_bytes()));
    bytes(&json!({"projection": projection, "admission_witness": witness}))
}

#[tokio::test]
async fn exact_duplicate_is_one_stable_operation_and_edge_set() {
    let (store, credential, _directory) = open_authenticated_store().await;
    let projection = witnessed_projection();

    let first = store
        .import_run_pack_observation(&credential, &projection, Some(b"test-key"))
        .await
        .expect("first import");
    let second = store
        .import_run_pack_observation(&credential, &projection, Some(b"test-key"))
        .await
        .expect("exact retry");

    assert_eq!(first, second);
    assert_eq!(store.count_operations().await.expect("operation count"), 1);
    let (_, edges) = store
        .operation_provenance(&first.operation_id, AsOf::now())
        .await
        .expect("operation provenance");
    assert_eq!(edges.len(), 1);
}

#[tokio::test]
async fn changed_projection_under_same_manifest_key_is_rejected_without_overwrite() {
    let (store, credential, _directory) = open_authenticated_store().await;
    let original = witnessed_projection();
    let receipt = store
        .import_run_pack_observation(&credential, &original, Some(b"test-key"))
        .await
        .expect("first import");
    let mut changed = serde_json::from_slice::<Value>(&original).unwrap();
    changed["projection"]["event_summary"]["terminal_state"] = json!("failed");

    let result = store
        .import_run_pack_observation(&credential, &bytes(&changed), Some(b"test-key"))
        .await;
    assert!(matches!(result, Err(MnemesError::InvalidProvenance(_))));
    assert_eq!(store.count_operations().await.expect("operation count"), 1);
    assert_eq!(
        store
            .get_operation(&receipt.operation_id)
            .await
            .expect("operation query")
            .expect("original operation")
            .content_digest,
        format!("sha256:{}", hex::encode(sha2::Sha256::digest(&original)))
    );
}

#[tokio::test]
async fn malformed_or_tampered_projection_creates_no_operation() {
    let (store, credential, _directory) = open_authenticated_store().await;
    let mut tampered = valid_projection();
    tampered["unexpected_client_fact"] = json!(true);
    assert!(matches!(
        store
            .import_run_pack_observation(&credential, &bytes(&tampered), Some(b"test-key"))
            .await,
        Err(MnemesError::InvalidProvenance(_))
    ));

    let mut escaped = valid_projection();
    escaped["vault"]["relative_ref"] = json!("../outside");
    assert!(matches!(
        store
            .import_run_pack_observation(&credential, &bytes(&escaped), Some(b"test-key"))
            .await,
        Err(MnemesError::InvalidProvenance(_))
    ));

    let mut malformed_digest = valid_projection();
    malformed_digest["pack_manifest_digest"] = json!("not-a-digest");
    assert!(matches!(
        store
            .import_run_pack_observation(&credential, &bytes(&malformed_digest), Some(b"test-key"))
            .await,
        Err(MnemesError::InvalidProvenance(_))
    ));

    let mut unknown_retention = valid_projection();
    unknown_retention["vault"]["retention_state"] = json!("client_claimed_green");
    assert!(matches!(
        store
            .import_run_pack_observation(&credential, &bytes(&unknown_retention), Some(b"test-key"))
            .await,
        Err(MnemesError::InvalidProvenance(_))
    ));
    assert_eq!(store.count_operations().await.expect("operation count"), 0);
}

#[tokio::test]
async fn accepting_server_time_dominates_client_recorded_time() {
    let (store, credential, _directory) = open_authenticated_store().await;
    let receipt = store
        .import_run_pack_observation(&credential, &witnessed_projection(), Some(b"test-key"))
        .await
        .expect("import");

    assert_ne!(
        receipt.recorded_at,
        valid_projection()["origin"]["recorded_at"]
            .as_str()
            .expect("fixture recorded_at")
    );
    let operation = store
        .get_operation(&receipt.operation_id)
        .await
        .expect("operation query")
        .expect("operation");
    assert_eq!(operation.recorded_at, receipt.recorded_at);
    assert_eq!(
        operation.observed_at.as_deref(),
        Some("2026-08-10T00:00:00Z")
    );
}
