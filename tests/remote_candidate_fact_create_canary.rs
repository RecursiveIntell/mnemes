use ed25519_dalek::SigningKey;
use mnemes::replication::{FactCreateTransportEntryV1, SignedFactCreateBatchV1};
use mnemes::FactCreateAckRecord;
use reqwest::StatusCode;
use rusqlite::Connection;
use semantic_memory::journal::{export_verified_contiguous, ExportStatus};
use semantic_memory::{MemoryConfig, MemoryStore, MockEmbedder, ReplicationMode};
use std::env;
use tempfile::TempDir;

fn required_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("missing required environment variable: {name}"))
}

#[tokio::test]
#[ignore = "requires an explicitly staged non-production Mnemes candidate"]
async fn remote_candidate_applies_one_canonical_journal_fact_and_replays_exact_ack() {
    let base_url = required_env("MNEMES_REMOTE_CANARY_URL");
    let credential = required_env("MNEMES_REMOTE_CANARY_CREDENTIAL");
    let device_id = required_env("MNEMES_REMOTE_CANARY_DEVICE_ID");
    let batch_id = required_env("MNEMES_REMOTE_CANARY_BATCH_ID");
    let body_path = required_env("MNEMES_REMOTE_CANARY_BODY_PATH");
    let ack_path = required_env("MNEMES_REMOTE_CANARY_ACK_PATH");
    let store_id = "remote-candidate-canary-v1";
    let namespace = "canary";
    let principal = "remote-candidate-canary";
    let epoch = 7;
    let fence = "uno-q-candidate-20260730T013600Z";

    let source_temp = TempDir::new().unwrap();
    let source = MemoryStore::open_with_embedder(
        MemoryConfig {
            base_dir: source_temp.path().to_path_buf(),
            journal_device_id: Some(device_id.clone()),
            journal_store_id: Some(store_id.into()),
            replication_mode: ReplicationMode::FactCreateRequired,
            replication_stream_epoch: epoch,
            ..Default::default()
        },
        Box::new(MockEmbedder::new(768)),
    )
    .unwrap();
    source
        .add_fact(
            namespace,
            "test-only typed Mnemes candidate canary",
            Some("remote-candidate-canary"),
            None,
        )
        .await
        .unwrap();
    drop(source);

    let source_conn = Connection::open(source_temp.path().join("memory.db")).unwrap();
    let exported =
        export_verified_contiguous(&source_conn, &device_id, store_id, epoch, 1, 1).unwrap();
    assert_eq!(exported.status, ExportStatus::End);
    assert_eq!(exported.entries.len(), 1);
    let journal = &exported.entries[0];
    let entry = FactCreateTransportEntryV1::from_journal_entry(journal).unwrap();
    assert_eq!(entry.payload, journal.payload);
    assert_eq!(entry.payload_digest, journal.payload_digest);
    assert_eq!(entry.journal_envelope_digest, journal.envelope_digest);

    let signing_key = SigningKey::from_bytes(&[7; 32]);
    let mut batch = SignedFactCreateBatchV1::new(
        batch_id.clone(),
        &device_id,
        store_id,
        epoch,
        1,
        vec![entry],
        principal,
        1,
        0,
        fence,
    )
    .unwrap();
    batch.sign(&signing_key).unwrap();
    let body = serde_json::to_string(&batch).unwrap();
    std::fs::write(&body_path, &body).unwrap();

    let client = reqwest::Client::new();
    let response = client
        .post(format!(
            "{}/v1/replication/fact-create/v1",
            base_url.trim_end_matches('/')
        ))
        .bearer_auth(credential)
        .header("content-type", "application/json")
        .body(body.clone())
        .send()
        .await
        .unwrap();
    let status = response.status();
    let ack = response.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{ack}");
    let ack_record: FactCreateAckRecord = serde_json::from_str(&ack).unwrap();
    assert_eq!(ack_record.batch_id, batch_id);
    assert_eq!(ack_record.accepted_head, 1);
    assert_eq!(ack_record.disposition, "accepted");
    assert!(!ack_record.request_digest.is_empty());

    let replay = client
        .post(format!(
            "{}/v1/replication/fact-create/v1",
            base_url.trim_end_matches('/')
        ))
        .bearer_auth(required_env("MNEMES_REMOTE_CANARY_CREDENTIAL"))
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(replay.text().await.unwrap(), ack);
    std::fs::write(ack_path, ack).unwrap();
}
