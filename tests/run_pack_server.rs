use hmac::Mac;
use mnemes::server::build_router_with_run_pack_attestation_key;
use mnemes::{
    Actor, ActorId, ActorKind, Device, DeviceId, MnemesStore, RunPackEvidenceProjectionV1,
};
use reqwest::{Client, StatusCode};
use semantic_memory::{EmbeddingConfig, MemoryConfig, MockEmbedder};
use serde_json::Value;
use sha2::Digest;
use tempfile::TempDir;

async fn spawn_server() -> (String, String, tokio::task::JoinHandle<()>, TempDir) {
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
            "run-pack-server-test",
            "linux",
            "localhost",
        ))
        .await
        .expect("device registration");
    store
        .register_actor(Actor::new(ActorId::new(), device_id, ActorKind::Hermes))
        .await
        .expect("actor registration");
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("listener");
    let address = listener.local_addr().expect("listener address");
    let handle = tokio::spawn(async move {
        axum::serve(
            listener,
            build_router_with_run_pack_attestation_key(store, b"test-key".to_vec()),
        )
        .await
        .expect("server run")
    });
    (format!("http://{address}"), credential, handle, directory)
}

fn projection() -> Value {
    serde_json::from_slice(include_bytes!(
        "../fixtures/witnessed-workbench/run-pack-evidence-projection-v1.json"
    ))
    .expect("frozen Recursive Agent projection fixture")
}

fn witnessed_projection() -> Vec<u8> {
    let projection = projection();
    let typed: RunPackEvidenceProjectionV1 = serde_json::from_value(projection.clone()).unwrap();
    let canonical = serde_json::to_vec(&typed).unwrap();
    let mut witness = serde_json::json!({"format":"mnemes.run-pack-admission-witness/v1","canonical_projection_digest":format!("sha256:{}", hex::encode(sha2::Sha256::digest(&canonical))),"pack_manifest_digest":"9f64acec41bde3a3f6d6a5b25c4928aa9b39eb23fdbbc7a1ce3bfe7538cafe9b","pack_content_digest":"ec192a928263b72f747fc3c40dfa0d11621171fb40d429a3d5ffb3a760102729","verification_receipt_digest":"eb0895ac3ebb64973a1844b314721c43921dab55b9eb8471c03467b1656894b2","verified":true});
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(b"test-key").unwrap();
    mac.update(&serde_json::to_vec(&witness).unwrap());
    witness["signature"] = serde_json::json!(hex::encode(mac.finalize().into_bytes()));
    serde_json::to_vec(&serde_json::json!({"projection": projection, "admission_witness": witness}))
        .unwrap()
}

#[tokio::test]
async fn run_pack_observation_http_surface_authenticates_and_is_idempotent() {
    let (base_url, credential, handle, _directory) = spawn_server().await;
    let client = Client::new();
    let body = witnessed_projection();

    let unauthenticated = client
        .post(format!("{base_url}/v1/run-pack-observations"))
        .body(body.clone())
        .send()
        .await
        .expect("unauthenticated request");
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);

    let first = client
        .post(format!("{base_url}/v1/run-pack-observations"))
        .bearer_auth(&credential)
        .body(body.clone())
        .send()
        .await
        .expect("first request");
    assert_eq!(first.status(), StatusCode::CREATED);
    let first: Value = first.json().await.expect("first response");

    let second = client
        .post(format!("{base_url}/v1/run-pack-observations"))
        .bearer_auth(&credential)
        .body(body)
        .send()
        .await
        .expect("retry request");
    assert_eq!(second.status(), StatusCode::CREATED);
    let second: Value = second.json().await.expect("second response");
    assert_eq!(first, second);

    handle.abort();
    let _ = handle.await;
}
