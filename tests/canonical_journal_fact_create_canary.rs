use ed25519_dalek::SigningKey;
use mnemes::replication::{FactCreateTransportEntryV1, SignedFactCreateBatchV1};
use mnemes::server::build_router;
use mnemes::{Device, DeviceId, FactCreateAckRecord, FactCreateAdmission, MnemesStore};
use reqwest::StatusCode;
use rusqlite::Connection;
use semantic_memory::journal::{export_verified_contiguous, ExportStatus};
use semantic_memory::{MemoryConfig, MemoryStore, MockEmbedder, ReplicationMode};
use tempfile::TempDir;

fn read_fact_count(base: &std::path::Path, device: &DeviceId) -> i64 {
    let conn = Connection::open(
        base.join("memory/shards")
            .join(device.as_str())
            .join("memory.db"),
    )
    .unwrap();
    conn.query_row("SELECT COUNT(*) FROM facts", [], |row| row.get(0))
        .unwrap()
}

async fn post(base: &str, credential: &str, body: &str) -> (StatusCode, String) {
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{base}/v1/replication/fact-create/v1"))
        .bearer_auth(credential)
        .header("content-type", "application/json")
        .body(body.to_owned())
        .send()
        .await
        .unwrap();
    let status = response.status();
    (status, response.text().await.unwrap())
}

#[tokio::test]
async fn canonical_journal_fact_create_http_canary() {
    let source_temp = TempDir::new().unwrap();
    let device_id = DeviceId::new();
    let device = device_id.to_string();
    let store_id = "canary-source";
    let source_config = MemoryConfig {
        base_dir: source_temp.path().to_path_buf(),
        journal_device_id: Some(device.clone()),
        journal_store_id: Some(store_id.into()),
        replication_mode: ReplicationMode::FactCreateRequired,
        replication_stream_epoch: 7,
        ..Default::default()
    };
    let source =
        MemoryStore::open_with_embedder(source_config, Box::new(MockEmbedder::new(768))).unwrap();
    source
        .add_fact("canary", "canonical canary fact", Some("test"), None)
        .await
        .unwrap();
    drop(source);

    let source_conn = Connection::open(source_temp.path().join("memory.db")).unwrap();
    let exported = export_verified_contiguous(&source_conn, &device, store_id, 7, 1, 1).unwrap();
    assert_eq!(exported.status, ExportStatus::End);
    assert_eq!(exported.entries.len(), 1);
    let journal = &exported.entries[0];
    let entry = FactCreateTransportEntryV1::from_journal_entry(journal).unwrap();
    assert_eq!(entry.payload, journal.payload);
    assert_eq!(entry.payload_digest, journal.payload_digest);
    assert_eq!(entry.journal_envelope_digest, journal.envelope_digest);

    let receiver_temp = TempDir::new().unwrap();
    let receiver = MnemesStore::open_with_embedder(
        receiver_temp.path().to_path_buf(),
        MemoryConfig {
            base_dir: receiver_temp.path().to_path_buf(),
            ..Default::default()
        },
        Box::new(MockEmbedder::new(768)),
    )
    .unwrap();
    let (registered, credential) = receiver
        .register_device_with_generated_credential(Device::new(
            device_id.clone(),
            "canary",
            "linux",
            "localhost",
        ))
        .await
        .unwrap();
    let signing_key = SigningKey::from_bytes(&[7; 32]);
    receiver
        .admit_fact_create_key(FactCreateAdmission {
            device_id: registered.clone(),
            store_id: store_id.into(),
            namespace: "canary".into(),
            principal_id: "canary-principal".into(),
            key_version: 1,
            public_key: signing_key.verifying_key().to_bytes(),
            activated_at: 0,
            cutoff_at: i64::MAX as u64,
            stream_epoch: 7,
            fencing_token: "canary-fence".into(),
        })
        .await
        .unwrap();
    let mut batch = SignedFactCreateBatchV1::new(
        "canary-batch",
        &device,
        store_id,
        7,
        1,
        vec![entry],
        "canary-principal",
        1,
        0,
        "canary-fence",
    )
    .unwrap();
    batch.sign(&signing_key).unwrap();
    let body = serde_json::to_string(&batch).unwrap();

    let app = build_router(receiver);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let base = format!("http://{addr}");
    let (status, ack) = post(&base, &credential, &body).await;
    assert_eq!(status, StatusCode::OK);
    let ack_record: FactCreateAckRecord = serde_json::from_str(&ack).unwrap();
    assert_eq!(ack_record.batch_id, batch.batch_id);
    assert!(!ack_record.request_digest.is_empty());
    assert_eq!(ack_record.accepted_head, 1);
    assert_eq!(ack_record.disposition, "accepted");
    let (replay_status, replay_ack) = post(&base, &credential, &body).await;
    assert_eq!(replay_status, status);
    assert_eq!(replay_ack, ack);
    task.abort();
    let _ = task.await;
    assert_eq!(read_fact_count(receiver_temp.path(), &device_id), 1);

    let receiver = MnemesStore::open_with_embedder(
        receiver_temp.path().to_path_buf(),
        MemoryConfig {
            base_dir: receiver_temp.path().to_path_buf(),
            ..Default::default()
        },
        Box::new(MockEmbedder::new(768)),
    )
    .unwrap();
    let app = build_router(receiver);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let base = format!("http://{addr}");
    let (restart_status, restart_ack) = post(&base, &credential, &body).await;
    assert_eq!(restart_status, status);
    assert_eq!(restart_ack, ack);
    assert_eq!(read_fact_count(receiver_temp.path(), &device_id), 1);
    task.abort();
}
