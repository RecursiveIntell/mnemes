use ed25519_dalek::SigningKey;
use mnemes::replication::{
    FactCreateTransportEntryV1, FactSupersedeTransportEntryV1, SignedFactCreateBatchV1,
    SignedFactSupersedeBatchV1,
};
use mnemes::server::{build_memory_store, build_router, build_staged_fact_supersede_router};
use mnemes::{Device, DeviceId, FactCreateAdmission, FactSupersedeAdmission, MnemesStore};
use reqwest::{Client, StatusCode};
use semantic_memory::journal::{
    encode_fact_create_payload, envelope_digest, export_verified_contiguous, payload_digest,
    FactCreatePayloadV1, FactCreateReplicaEnvelopeV1, FactSupersedePayloadV1,
    FactSupersedeReplicaEnvelopeV1, JournalEntry, FACT_CREATE_OPERATION,
    FACT_CREATE_PAYLOAD_SCHEMA, GENESIS_PREDECESSOR, VERIFIED_RECORD_STATE,
};
use semantic_memory::{
    AssertionDraftV1, AuthorityIssuer, AuthorityPermit, MemoryConfig, MemoryStore,
    MemoryTransitionCandidateV1, MemoryTransitionOutcomeV1, MockEmbedder, ReplicationMode,
    SourceArtifactV1, SourceSpanRefV1, SupersessionDraftV1, TransitionOperation,
};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::process::Command;
use tempfile::TempDir;
use tokio::task::JoinHandle;

struct DeviceIdentity {
    device_id: String,
    credential: String,
}

struct ActorIdentity {
    actor_id: String,
}

struct RunningServer {
    base_url: String,
    _temp_dir: TempDir,
    _handle: JoinHandle<()>,
}

impl RunningServer {
    async fn stop(self) {
        let _ = self.stop_keep_temp().await;
    }

    async fn stop_keep_temp(self) -> TempDir {
        self._handle.abort();
        let _ = self._handle.await;
        self._temp_dir
    }
}

async fn open_store() -> (TempDir, MnemesStore) {
    let temp = TempDir::new().unwrap();
    let base = PathBuf::from(temp.path());
    let store = MnemesStore::open_with_embedder(
        base.join("pooled-store"),
        semantic_memory::MemoryConfig {
            base_dir: base.clone(),
            ..Default::default()
        },
        Box::new(semantic_memory::MockEmbedder::new(768)),
    )
    .unwrap();

    (temp, store)
}

async fn spawn_server() -> RunningServer {
    let (temp, store) = open_store().await;
    let app = build_router(store);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");
    let handle = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .unwrap_or_else(|error| panic!("server stopped: {error}"));
    });

    RunningServer {
        base_url,
        _temp_dir: temp,
        _handle: handle,
    }
}

#[cfg(feature = "server")]
async fn spawn_server_with_store(_temp: TempDir, store: MnemesStore) -> RunningServer {
    let app = build_router(store);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");
    let handle = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .unwrap_or_else(|error| panic!("server stopped: {error}"));
    });

    RunningServer {
        base_url,
        _temp_dir: _temp,
        _handle: handle,
    }
}

#[cfg(feature = "server")]
async fn spawn_staged_fact_supersede_server_with_store(
    _temp: TempDir,
    store: MnemesStore,
) -> RunningServer {
    let app = build_staged_fact_supersede_router(store);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");
    let handle = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .unwrap_or_else(|error| panic!("server stopped: {error}"));
    });

    RunningServer {
        base_url,
        _temp_dir: _temp,
        _handle: handle,
    }
}

async fn register_device(server: &RunningServer, client: &Client) -> DeviceIdentity {
    let response = client
        .post(format!("{}/v1/devices/register", server.base_url))
        .json(&json!({
            "label": "ci-device",
            "platform": "linux",
            "hostname": "localhost",
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);
    let body: Value = response.json().await.unwrap();

    DeviceIdentity {
        device_id: body["device_id"].as_str().unwrap().to_string(),
        credential: body["credential"].as_str().unwrap().to_string(),
    }
}

async fn register_actor(
    server: &RunningServer,
    client: &Client,
    device: &DeviceIdentity,
    profile: &str,
) -> ActorIdentity {
    let response = client
        .post(format!("{}/v1/actors", server.base_url))
        .bearer_auth(&device.credential)
        .json(&json!({
            "device_id": device.device_id,
            "actor_kind": "hermes",
            "tool_profile": profile,
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);
    let body: Value = response.json().await.unwrap();

    ActorIdentity {
        actor_id: body["actor_id"].as_str().unwrap().to_string(),
    }
}

fn fact_batch(
    home: &str,
    store_id: &str,
    namespace: &str,
    fence: &str,
) -> (SignedFactCreateBatchV1, SigningKey) {
    let payload = encode_fact_create_payload(&FactCreatePayloadV1 {
        fact_id: "00000000-0000-0000-0000-000000000001".into(),
        namespace: namespace.into(),
        content: "typed HTTP replication test fact".into(),
        source: Some("test".into()),
        metadata: None,
    })
    .unwrap();
    let pd = payload_digest(&payload);
    let entry = JournalEntry {
        journal_id: 1,
        home_device_id: home.into(),
        store_id: store_id.into(),
        stream_epoch: 7,
        sequence: 1,
        operation_kind: FACT_CREATE_OPERATION.into(),
        payload_schema: FACT_CREATE_PAYLOAD_SCHEMA.into(),
        payload,
        payload_digest: pd,
        predecessor_digest: GENESIS_PREDECESSOR,
        envelope_digest: envelope_digest(
            home,
            store_id,
            7,
            1,
            FACT_CREATE_OPERATION,
            FACT_CREATE_PAYLOAD_SCHEMA,
            &GENESIS_PREDECESSOR,
            &pd,
        ),
        record_state: VERIFIED_RECORD_STATE.into(),
        created_at: "2026-07-29T00:00:00Z".into(),
    };
    let entry = FactCreateTransportEntryV1::from_journal_entry(&entry).unwrap();
    let key = SigningKey::from_bytes(&[7u8; 32]);
    let mut batch = SignedFactCreateBatchV1::new(
        "batch-http-1",
        home,
        store_id,
        7,
        1,
        vec![entry],
        "http-replication-key",
        1,
        1_000,
        fence,
    )
    .unwrap();
    batch.sign(&key).unwrap();
    (batch, key)
}

fn owner_candidate(
    candidate_id: &str,
    evidence: &str,
    assertion_id: &str,
    assertion: &str,
    operation: TransitionOperation,
) -> MemoryTransitionCandidateV1 {
    let artifact_id = format!("artifact:{candidate_id}");
    let span = SourceSpanRefV1::new(&artifact_id, 0, evidence.len()).unwrap();
    MemoryTransitionCandidateV1::new(
        candidate_id,
        vec![SourceArtifactV1::new(&artifact_id, evidence).unwrap()],
        vec![span.clone()],
        vec![
            AssertionDraftV1::new(assertion_id, "general", assertion, vec![span], vec![]).unwrap(),
        ],
        operation,
        vec![],
    )
    .unwrap()
}

async fn owner_created_supersede_envelopes(
    home_device_id: &str,
) -> (FactCreateReplicaEnvelopeV1, FactSupersedeReplicaEnvelopeV1) {
    let temp = TempDir::new().unwrap();
    let primary = MemoryStore::open_with_embedder(
        MemoryConfig {
            base_dir: temp.path().to_path_buf(),
            journal_device_id: Some(home_device_id.into()),
            journal_store_id: Some("primary".into()),
            replication_mode: ReplicationMode::FactCreateRequired,
            replication_stream_epoch: 7,
            ..Default::default()
        },
        Box::new(MockEmbedder::new(768)),
    )
    .unwrap();
    let authority = primary.authority();
    let old = authority
        .verify_and_commit(
            AuthorityIssuer::from_operator_token("server-test-operator-token")
                .unwrap()
                .mint_operator_system(
                    "principal:test",
                    "server-test",
                    AuthorityPermit::APPEND_CAPABILITY,
                ),
            "server-owner-create".into(),
            owner_candidate(
                "server-owner-create-candidate",
                "owner original",
                "old",
                "owner original",
                TransitionOperation::Append {
                    assertion_id: "old".into(),
                },
            ),
        )
        .await
        .unwrap();
    let old_id = match old {
        MemoryTransitionOutcomeV1::Committed {
            authority_receipt, ..
        } => authority_receipt.affected_ids[0].clone(),
        _ => panic!("governed owner create must commit"),
    };
    authority
        .verify_and_commit(
            AuthorityIssuer::from_operator_token("server-test-operator-token")
                .unwrap()
                .mint_operator_system(
                    "principal:test",
                    "server-test",
                    AuthorityPermit::SUPERSEDE_CAPABILITY,
                ),
            "server-owner-supersede".into(),
            owner_candidate(
                "server-owner-supersede-candidate",
                "owner replacement",
                "new",
                "owner replacement",
                TransitionOperation::Supersede {
                    draft: SupersessionDraftV1::new(&old_id, "new").unwrap(),
                },
            ),
        )
        .await
        .unwrap();
    let conn = rusqlite::Connection::open(temp.path().join("memory.db")).unwrap();
    let entries = export_verified_contiguous(&conn, home_device_id, "primary", 7, 1, 2)
        .unwrap()
        .entries;
    assert_eq!(entries.len(), 2);
    (
        FactCreateReplicaEnvelopeV1::from(entries[0].clone()),
        FactSupersedeReplicaEnvelopeV1::from(entries[1].clone()),
    )
}

fn supersede_batch(
    batch_id: &str,
    owner_envelope: FactSupersedeReplicaEnvelopeV1,
    writer_epoch: u64,
    fence: &str,
) -> (SignedFactSupersedeBatchV1, SigningKey) {
    let key = SigningKey::from_bytes(&[8u8; 32]);
    let mut batch = SignedFactSupersedeBatchV1::new(
        batch_id,
        owner_envelope.home_device_id.clone(),
        owner_envelope.store_id.clone(),
        3,
        writer_epoch,
        owner_envelope.stream_epoch,
        owner_envelope.sequence,
        vec![FactSupersedeTransportEntryV1::from_owner_envelope(
            owner_envelope,
        )],
        "http-supersede-key",
        1,
        1_000,
        fence,
    )
    .unwrap();
    batch.sign(&key).unwrap();
    (batch, key)
}

async fn post_supersede(
    server: &RunningServer,
    client: &Client,
    device: &DeviceIdentity,
    batch: &SignedFactSupersedeBatchV1,
) -> (StatusCode, Value) {
    let response = client
        .post(format!(
            "{}/v1/replication/fact-supersede/v1",
            server.base_url
        ))
        .bearer_auth(&device.credential)
        .json(batch)
        .send()
        .await
        .unwrap();
    let status = response.status();
    (status, response.json().await.unwrap())
}

async fn admit_supersede(store: &MnemesStore, device_id: &str) {
    store
        .admit_fact_supersede_key(FactSupersedeAdmission {
            device_id: DeviceId::parse(device_id).unwrap(),
            store_id: "primary".into(),
            replacement_namespace: "general".into(),
            principal_id: "http-supersede-key".into(),
            key_version: 1,
            public_key: SigningKey::from_bytes(&[8u8; 32])
                .verifying_key()
                .to_bytes(),
            activated_at: 0,
            cutoff_at: i64::MAX as u64,
            store_epoch: 3,
            writer_epoch: 4,
            fencing_token: "supersede-fence-4".into(),
        })
        .await
        .unwrap();
}

async fn fact_count(store: &MnemesStore, device_id: &str) -> u64 {
    store
        .device_memory(&DeviceId::parse(device_id).unwrap())
        .await
        .unwrap()
        .stats()
        .await
        .unwrap()
        .total_facts as u64
}

#[cfg(feature = "server")]
fn supersede_ack_count(temp: &TempDir, batch_id: &str) -> i64 {
    let conn =
        rusqlite::Connection::open(temp.path().join("pooled-store").join("pooled.db")).unwrap();
    conn.query_row(
        "SELECT COUNT(*) FROM fact_supersede_acks WHERE batch_id=?1",
        [batch_id],
        |row| row.get(0),
    )
    .unwrap()
}

#[cfg(feature = "server")]
fn supersede_inbox_next_sequence(temp: &TempDir, device_id: &str) -> i64 {
    let conn = rusqlite::Connection::open(
        temp.path()
            .join("pooled-store")
            .join("memory")
            .join("shards")
            .join(device_id)
            .join("memory.db"),
    )
    .unwrap();
    conn.query_row(
        "SELECT next_sequence FROM replication_inbox_streams \
         WHERE home_device_id=?1 AND store_id='primary'",
        [device_id],
        |row| row.get(0),
    )
    .unwrap()
}

#[cfg(feature = "server")]
fn supersede_inbox_stream_count(temp: &TempDir, device_id: &str) -> i64 {
    let conn = rusqlite::Connection::open(
        temp.path()
            .join("pooled-store")
            .join("memory")
            .join("shards")
            .join(device_id)
            .join("memory.db"),
    )
    .unwrap();
    conn.query_row(
        "SELECT COUNT(*) FROM replication_inbox_streams \
         WHERE home_device_id=?1 AND store_id='primary'",
        [device_id],
        |row| row.get(0),
    )
    .unwrap()
}

async fn post_fact(
    server: &RunningServer,
    client: &Client,
    device: &DeviceIdentity,
    batch: &SignedFactCreateBatchV1,
) -> (StatusCode, Value) {
    let response = client
        .post(format!("{}/v1/replication/fact-create/v1", server.base_url))
        .bearer_auth(&device.credential)
        .json(batch)
        .send()
        .await
        .unwrap();
    let status = response.status();
    (status, response.json().await.unwrap())
}

#[cfg(feature = "server")]
async fn apply_owner_create_for_supersede(
    server: &RunningServer,
    client: &Client,
    device: &DeviceIdentity,
    create: &FactCreateReplicaEnvelopeV1,
    batch_id: &str,
) {
    let mut batch = SignedFactCreateBatchV1::new(
        batch_id,
        device.device_id.clone(),
        "primary",
        7,
        1,
        vec![FactCreateTransportEntryV1 {
            sequence: create.sequence,
            payload: create.payload.clone(),
            payload_digest: create.payload_digest,
            predecessor_digest: create.predecessor_digest,
            journal_envelope_digest: create.envelope_digest,
        }],
        "http-replication-key",
        1,
        1_000,
        "fence-7",
    )
    .unwrap();
    batch.sign(&SigningKey::from_bytes(&[7u8; 32])).unwrap();
    assert_eq!(
        post_fact(server, client, device, &batch).await.0,
        StatusCode::OK
    );
}

async fn spawn_admitted_server() -> (RunningServer, DeviceIdentity, MnemesStore) {
    let (temp, store) = open_store().await;
    let device_id = DeviceId::new();
    let (registered, credential) = store
        .register_device_with_generated_credential(Device::new(
            device_id.clone(),
            "ci-device",
            "linux",
            "localhost",
        ))
        .await
        .unwrap();
    store
        .admit_fact_create_key(FactCreateAdmission {
            device_id: registered.clone(),
            store_id: "primary".into(),
            namespace: "general".into(),
            principal_id: "http-replication-key".into(),
            key_version: 1,
            public_key: SigningKey::from_bytes(&[7u8; 32])
                .verifying_key()
                .to_bytes(),
            activated_at: 0,
            cutoff_at: i64::MAX as u64,
            stream_epoch: 7,
            fencing_token: "fence-7".into(),
        })
        .await
        .unwrap();
    let count_store = MnemesStore::open_with_embedder(
        temp.path().join("pooled-store"),
        semantic_memory::MemoryConfig {
            base_dir: temp.path().to_path_buf(),
            ..Default::default()
        },
        Box::new(semantic_memory::MockEmbedder::new(768)),
    )
    .unwrap();
    let server = spawn_server_with_store(temp, store).await;
    (
        server,
        DeviceIdentity {
            device_id: registered.to_string(),
            credential,
        },
        count_store,
    )
}

#[cfg(feature = "server")]
async fn spawn_admitted_staged_fact_supersede_server(
) -> (RunningServer, DeviceIdentity, MnemesStore) {
    let (temp, store) = open_store().await;
    let device_id = DeviceId::new();
    let (registered, credential) = store
        .register_device_with_generated_credential(Device::new(
            device_id.clone(),
            "ci-device",
            "linux",
            "localhost",
        ))
        .await
        .unwrap();
    store
        .admit_fact_create_key(FactCreateAdmission {
            device_id: registered.clone(),
            store_id: "primary".into(),
            namespace: "general".into(),
            principal_id: "http-replication-key".into(),
            key_version: 1,
            public_key: SigningKey::from_bytes(&[7u8; 32])
                .verifying_key()
                .to_bytes(),
            activated_at: 0,
            cutoff_at: i64::MAX as u64,
            stream_epoch: 7,
            fencing_token: "fence-7".into(),
        })
        .await
        .unwrap();
    let count_store = MnemesStore::open_with_embedder(
        temp.path().join("pooled-store"),
        semantic_memory::MemoryConfig {
            base_dir: temp.path().to_path_buf(),
            ..Default::default()
        },
        Box::new(semantic_memory::MockEmbedder::new(768)),
    )
    .unwrap();
    let server = spawn_staged_fact_supersede_server_with_store(temp, store).await;
    (
        server,
        DeviceIdentity {
            device_id: registered.to_string(),
            credential,
        },
        count_store,
    )
}

#[cfg(feature = "server")]
#[tokio::test]
async fn fact_create_http_applies_then_duplicates_without_second_fact() {
    let (server, device, count_store) = spawn_admitted_server().await;
    let client = Client::new();
    let (batch, _) = fact_batch(&device.device_id, "primary", "general", "fence-7");
    let (status, body) = post_fact(&server, &client, &device, &batch).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["disposition"], "accepted");
    assert_eq!(body["batch_id"], batch.batch_id);
    assert_eq!(body["accepted_head"], 1);
    assert_eq!(fact_count(&count_store, &device.device_id).await, 1);
    let (status, body2) = post_fact(&server, &client, &device, &batch).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body2, body);
    assert_eq!(fact_count(&count_store, &device.device_id).await, 1);
}

#[cfg(feature = "server")]
#[tokio::test]
async fn fact_create_http_rejects_admission_scope_and_signature_before_mutation() {
    let (server, device, count_store) = spawn_admitted_server().await;
    let client = Client::new();
    for (batch, expected) in [
        (
            fact_batch(&device.device_id, "primary", "other", "fence-7").0,
            StatusCode::FORBIDDEN,
        ),
        (
            fact_batch(&device.device_id, "primary", "general", "wrong-fence").0,
            StatusCode::FORBIDDEN,
        ),
    ] {
        let (status, _) = post_fact(&server, &client, &device, &batch).await;
        assert_eq!(status, expected);
    }
    let (mut bad, _) = fact_batch(&device.device_id, "primary", "general", "fence-7");
    bad.signature[0] ^= 1;
    let (status, _) = post_fact(&server, &client, &device, &bad).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(fact_count(&count_store, &device.device_id).await, 0);
}

#[cfg(feature = "server")]
#[tokio::test]
async fn fact_create_http_rejects_unadmitted_and_revoked_keys() {
    let (server, device, count_store) = spawn_admitted_server().await;
    let client = Client::new();
    let (mut unadmitted, key) = fact_batch(&device.device_id, "primary", "general", "fence-7");
    unadmitted.signer_principal_id = "unadmitted".into();
    unadmitted.sign(&key).unwrap();
    let (status, _) = post_fact(&server, &client, &device, &unadmitted).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let device_id = DeviceId::parse(&device.device_id).unwrap();
    let _ = count_store
        .revoke_fact_create_key(&device_id, "primary", "general", "http-replication-key", 1)
        .await;
    let (batch, _) = fact_batch(&device.device_id, "primary", "general", "fence-7");
    let (status, _) = post_fact(&server, &client, &device, &batch).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[cfg(feature = "server")]
#[tokio::test]
async fn fact_create_http_duplicate_survives_store_reopen() {
    let (server, device, count_store) = spawn_admitted_server().await;
    let client = Client::new();
    let (batch, _) = fact_batch(&device.device_id, "primary", "general", "fence-7");
    assert_eq!(
        post_fact(&server, &client, &device, &batch).await.0,
        StatusCode::OK
    );
    assert_eq!(fact_count(&count_store, &device.device_id).await, 1);
    drop(server);
}

#[cfg(feature = "server")]
async fn mcp_call(
    client: &Client,
    server: &RunningServer,
    token: &str,
    actor_id: &str,
    name: &str,
    arguments: Value,
) -> Value {
    let response = client
        .post(format!("{}/v1/mcp", server.base_url))
        .bearer_auth(token)
        .json(&json!({
            "method": "tools/call",
            "params": {
                "actor_id": actor_id,
                "name": name,
                "arguments": arguments,
            },
            "id": "tool-call",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = response.json().await.unwrap();
    body["result"].to_owned()
}

#[cfg(feature = "server")]
async fn seed_witnessed_fact(store: &MnemesStore, namespace: &str, content: &str) {
    store
        .memory()
        .add_fact(namespace, content, None, None)
        .await
        .unwrap();
}

#[tokio::test]
async fn auth_and_revoke_enforce_device_state_and_profiles() {
    let server = spawn_server().await;
    let client = Client::new();
    let device = register_device(&server, &client).await;
    let actor = register_actor(&server, &client, &device, "agent").await;

    let response = client
        .get(format!("{}/v1/health", server.base_url))
        .bearer_auth(&device.credential)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let no_auth = client
        .get(format!("{}/v1/health", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(no_auth.status(), StatusCode::UNAUTHORIZED);

    let revoked = client
        .post(format!(
            "{}/v1/devices/{}/revoke",
            server.base_url, device.device_id
        ))
        .bearer_auth(&device.credential)
        .send()
        .await
        .unwrap();
    assert_eq!(revoked.status(), StatusCode::OK);

    let denied = client
        .get(format!("{}/v1/health", server.base_url))
        .bearer_auth(&device.credential)
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);

    let submit = client
        .post(format!("{}/v1/operations", server.base_url))
        .bearer_auth(&device.credential)
        .json(&json!({
            "idempotency_key": "idempotent-1",
            "requesting_device_id": device.device_id,
            "requesting_actor_id": actor.actor_id,
            "operation_kind": "assert",
            "target_kind": "fact",
            "target_id": "node-1",
            "content_digest": "sha256:demo",
            "recording_device_id": device.device_id,
            "recording_server_id": device.device_id,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(submit.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn mcp_tools_list_and_call_are_profile_scoped() {
    let server = spawn_server().await;
    let client = Client::new();
    let device = register_device(&server, &client).await;
    let agent = register_actor(&server, &client, &device, "human").await;
    let operator = register_actor(&server, &client, &device, "operator").await;
    let agent_tools = [
        "sm_get_device",
        "sm_list_devices",
        "sm_get_actor",
        "sm_get_operation",
        "sm_search_witnessed",
        "sm_stats",
        "sm_health",
        "sm_heartbeat",
    ];
    let operator_tools = [
        "sm_register_device",
        "sm_revoke_device",
        "sm_rotate_device_key",
        "sm_register_actor",
        "sm_submit_operation",
        "sm_verify_integrity",
    ];
    let legacy = [
        "devices.list",
        "actors.list",
        "operations.list",
        "health.check",
        "operations.submit",
    ];

    let list_agent = client
        .post(format!("{}/v1/mcp", server.base_url))
        .bearer_auth(&device.credential)
        .json(&json!({
            "method": "tools/list",
            "params": {"actor_id": agent.actor_id},
            "id": 1,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(list_agent.status(), StatusCode::OK);

    let list_agent_json: Value = list_agent.json().await.unwrap();
    let names_agent = list_agent_json["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value["name"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    for name in &agent_tools {
        assert!(names_agent.contains(&name.to_string()));
    }
    for name in &operator_tools {
        assert!(!names_agent.contains(&name.to_string()));
    }
    for name in &legacy {
        assert!(!names_agent.contains(&name.to_string()));
    }

    let denied_write_call = client
        .post(format!("{}/v1/mcp", server.base_url))
        .bearer_auth(&device.credential)
        .json(&json!({
            "method": "tools/call",
            "params": {
                "actor_id": agent.actor_id,
                "name": "sm_submit_operation",
            },
            "id": 2,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(denied_write_call.status(), StatusCode::FORBIDDEN);

    let denied_hidden_call = client
        .post(format!("{}/v1/mcp", server.base_url))
        .bearer_auth(&device.credential)
        .json(&json!({
            "method": "tools/call",
            "params": {
                "actor_id": operator.actor_id,
                "name": "operations.submit",
            },
            "id": "legacy",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(denied_hidden_call.status(), StatusCode::FORBIDDEN);

    let list_operator = client
        .post(format!("{}/v1/mcp", server.base_url))
        .bearer_auth(&device.credential)
        .json(&json!({
            "method": "tools/list",
            "params": {"actor_id": operator.actor_id},
            "id": 4,
        }))
        .send()
        .await
        .unwrap();
    let list_operator_json: Value = list_operator.json().await.unwrap();
    let names_operator = list_operator_json["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value["name"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    for name in &agent_tools {
        assert!(names_operator.contains(&name.to_string()));
    }
    for name in &operator_tools {
        assert!(names_operator.contains(&name.to_string()));
    }
    for name in &legacy {
        assert!(!names_operator.contains(&name.to_string()));
    }
}

#[tokio::test]
async fn mcp_read_and_operator_tools_are_store_backed() {
    let server = spawn_server().await;
    let client = Client::new();
    let device = register_device(&server, &client).await;
    let operator = register_actor(&server, &client, &device, "operator").await;

    let read_device = mcp_call(
        &client,
        &server,
        &device.credential,
        &operator.actor_id,
        "sm_get_device",
        json!({ "device_id": device.device_id }),
    )
    .await;
    assert_eq!(read_device["device_id"].as_str().unwrap(), device.device_id);

    let read_devices = mcp_call(
        &client,
        &server,
        &device.credential,
        &operator.actor_id,
        "sm_list_devices",
        json!({}),
    )
    .await;
    assert!(!read_devices.as_array().unwrap().is_empty());

    let read_actor = mcp_call(
        &client,
        &server,
        &device.credential,
        &operator.actor_id,
        "sm_get_actor",
        json!({ "actor_id": operator.actor_id }),
    )
    .await;
    assert_eq!(read_actor["actor_id"].as_str().unwrap(), operator.actor_id);

    let submitted = mcp_call(
        &client,
        &server,
        &device.credential,
        &operator.actor_id,
        "sm_submit_operation",
        json!({
            "idempotency_key": "e2e-tool-submit",
            "requesting_device_id": device.device_id,
            "requesting_actor_id": operator.actor_id,
            "operation_kind": "assert",
            "target_kind": "fact",
            "target_id": "node-42",
            "content_digest": "sha256:cat",
            "recording_device_id": device.device_id,
            "recording_server_id": device.device_id,
        }),
    )
    .await;
    assert_eq!(
        submitted["idempotency_key"].as_str().unwrap(),
        "e2e-tool-submit"
    );

    let read_operation = mcp_call(
        &client,
        &server,
        &device.credential,
        &operator.actor_id,
        "sm_get_operation",
        json!({ "operation_id": submitted["operation_id"] }),
    )
    .await;
    assert_eq!(
        read_operation["idempotency_key"].as_str().unwrap(),
        "e2e-tool-submit"
    );

    let _search = mcp_call(
        &client,
        &server,
        &device.credential,
        &operator.actor_id,
        "sm_search_witnessed",
        json!({ "query": "nothing", "source_types": ["facts"] }),
    )
    .await;

    let stats = mcp_call(
        &client,
        &server,
        &device.credential,
        &operator.actor_id,
        "sm_stats",
        json!({}),
    )
    .await;
    assert_eq!(stats["pooled"]["operations"].as_u64().unwrap(), 1);

    let health = mcp_call(
        &client,
        &server,
        &device.credential,
        &operator.actor_id,
        "sm_health",
        json!({}),
    )
    .await;
    assert!(health["service_id"].is_string());

    let heartbeat = mcp_call(
        &client,
        &server,
        &device.credential,
        &operator.actor_id,
        "sm_heartbeat",
        json!({ "device_id": device.device_id }),
    )
    .await;
    assert_eq!(heartbeat["device_id"].as_str().unwrap(), device.device_id);

    let created_actor = mcp_call(
        &client,
        &server,
        &device.credential,
        &operator.actor_id,
        "sm_register_actor",
        json!({
            "device_id": device.device_id,
            "actor_kind": "hermes",
            "tool_profile": "agent",
        }),
    )
    .await;
    assert!(created_actor["actor_id"].as_str().is_some());

    let created_device = mcp_call(
        &client,
        &server,
        &device.credential,
        &operator.actor_id,
        "sm_register_device",
        json!({
            "label": "child",
            "platform": "linux",
            "hostname": "child-host",
        }),
    )
    .await;
    assert!(created_device["device_id"].as_str().is_some());
    assert!(created_device["credential"].as_str().is_some());
    let child_device_id = created_device["device_id"].as_str().unwrap();

    let child_actor = mcp_call(
        &client,
        &server,
        &device.credential,
        &operator.actor_id,
        "sm_register_actor",
        json!({"device_id": child_device_id, "actor_kind": "codex", "tool_profile": "agent"}),
    )
    .await;
    assert!(child_actor["actor_id"].as_str().is_some());

    let child_rotated = mcp_call(
        &client,
        &server,
        &device.credential,
        &operator.actor_id,
        "sm_rotate_device_key",
        json!({"device_id": child_device_id}),
    )
    .await;
    assert!(child_rotated["credential"].as_str().is_some());

    let child_revoked = mcp_call(
        &client,
        &server,
        &device.credential,
        &operator.actor_id,
        "sm_revoke_device",
        json!({"device_id": child_device_id}),
    )
    .await;
    assert_eq!(child_revoked["status"], "revoked");

    let rotated = mcp_call(
        &client,
        &server,
        &device.credential,
        &operator.actor_id,
        "sm_rotate_device_key",
        json!({ "device_id": device.device_id }),
    )
    .await;
    assert_ne!(rotated["credential"].as_str().unwrap(), "");

    let rotated_credential = rotated["credential"].as_str().unwrap().to_string();
    let integrity = mcp_call(
        &client,
        &server,
        &rotated_credential,
        &operator.actor_id,
        "sm_verify_integrity",
        json!({}),
    )
    .await;
    assert_eq!(integrity["pooled_sqlite"]["status"].as_str().unwrap(), "ok");

    let revoked = mcp_call(
        &client,
        &server,
        &rotated_credential,
        &operator.actor_id,
        "sm_revoke_device",
        json!({ "device_id": device.device_id }),
    )
    .await;
    assert_eq!(revoked["status"].as_str().unwrap(), "revoked");

    let denied = client
        .get(format!("{}/v1/health", server.base_url))
        .bearer_auth(&rotated_credential)
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn mcp_submit_operation_is_persistent_and_idempotent() {
    let server = spawn_server().await;
    let client = Client::new();
    let device = register_device(&server, &client).await;
    let operator = register_actor(&server, &client, &device, "operator").await;

    let base_request = json!({
        "idempotency_key": "idem-ops",
        "requesting_device_id": device.device_id,
        "requesting_actor_id": operator.actor_id,
        "operation_kind": "assert",
        "target_kind": "fact",
        "target_id": "node-1",
        "content_digest": "sha256:demo",
        "recording_device_id": device.device_id,
        "recording_server_id": device.device_id,
        "observed_at": "2026-07-19T00:00:00Z"
    });

    let submit_once = client
        .post(format!("{}/v1/mcp", server.base_url))
        .bearer_auth(&device.credential)
        .json(&json!({
            "method": "tools/call",
            "params": {
                "actor_id": operator.actor_id,
                "name": "sm_submit_operation",
                "arguments": base_request,
            },
            "id": 1,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(submit_once.status(), StatusCode::OK);
    let submit_once_body: Value = submit_once.json().await.unwrap();
    let operation_id = submit_once_body["result"]["operation_id"]
        .as_str()
        .unwrap()
        .to_string();
    let receipt_id = submit_once_body["result"]["receipt_id"]
        .as_str()
        .unwrap()
        .to_string();

    let submit_twice = client
        .post(format!("{}/v1/mcp", server.base_url))
        .bearer_auth(&device.credential)
        .json(&json!({
            "method": "tools/call",
            "params": {
                "actor_id": operator.actor_id,
                "name": "sm_submit_operation",
                "arguments": {
                    "idempotency_key": "idem-ops",
                    "requesting_device_id": device.device_id,
                    "requesting_actor_id": operator.actor_id,
                    "operation_kind": "assert",
                    "target_kind": "fact",
                    "target_id": "node-1",
                    "content_digest": "sha256:demo",
                    "recording_device_id": device.device_id,
                    "recording_server_id": device.device_id,
                    "observed_at": "2026-07-19T00:00:00Z"
                },
            },
            "id": 2,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(submit_twice.status(), StatusCode::OK);
    let submit_twice_body: Value = submit_twice.json().await.unwrap();
    assert_eq!(
        submit_twice_body["result"]["operation_id"]
            .as_str()
            .unwrap(),
        operation_id
    );
    assert_eq!(
        submit_twice_body["result"]["receipt_id"].as_str().unwrap(),
        receipt_id
    );

    let conflict = client
        .post(format!("{}/v1/mcp", server.base_url))
        .bearer_auth(&device.credential)
        .json(&json!({
            "method": "tools/call",
            "params": {
                "actor_id": operator.actor_id,
                "name": "sm_submit_operation",
                "arguments": {
                    "idempotency_key": "idem-ops",
                    "requesting_device_id": device.device_id,
                    "requesting_actor_id": operator.actor_id,
                    "operation_kind": "assert",
                    "target_kind": "fact",
                    "target_id": "node-1",
                    "content_digest": "sha256:changed",
                    "recording_device_id": device.device_id,
                    "recording_server_id": device.device_id,
                    "observed_at": "2026-07-19T00:00:00Z"
                },
            },
            "id": 3,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::BAD_REQUEST);
    let conflict_body: Value = conflict.json().await.unwrap();
    assert_eq!(
        conflict_body["error"]["code"].as_i64().unwrap_or(-1),
        -32600
    );

    let operations = client
        .get(format!(
            "{}/v1/operations?actor_id={}&device_id={}",
            server.base_url, operator.actor_id, device.device_id
        ))
        .bearer_auth(&device.credential)
        .send()
        .await
        .unwrap();
    assert_eq!(operations.status(), StatusCode::OK);
    let operation_list: Vec<Value> = operations.json().await.unwrap();
    assert_eq!(operation_list.len(), 1);
    assert_eq!(
        operation_list[0]["operation_id"].as_str().unwrap(),
        operation_id
    );
    assert_eq!(
        operation_list[0]["receipt_id"].as_str().unwrap(),
        receipt_id
    );
}

#[tokio::test]
async fn mcp_verify_integrity_is_operator_only() {
    let server = spawn_server().await;
    let client = Client::new();
    let device = register_device(&server, &client).await;
    let operator = register_actor(&server, &client, &device, "operator").await;
    let agent = register_actor(&server, &client, &device, "agent").await;

    let denied = client
        .post(format!("{}/v1/mcp", server.base_url))
        .bearer_auth(&device.credential)
        .json(&json!({
            "method": "tools/call",
            "params": {
                "actor_id": agent.actor_id,
                "name": "sm_verify_integrity",
            },
            "id": "verify-1",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);

    let allowed = client
        .post(format!("{}/v1/mcp", server.base_url))
        .bearer_auth(&device.credential)
        .json(&json!({
            "method": "tools/call",
            "params": {
                "actor_id": operator.actor_id,
                "name": "sm_verify_integrity",
            },
            "id": "verify-2",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(allowed.status(), StatusCode::OK);
    let allowed_body: Value = allowed.json().await.unwrap();
    assert_eq!(allowed_body["result"]["pooled_sqlite"]["status"], "ok");
    assert!(allowed_body["result"]["semantic_memory"]["status"].is_string());
}

#[tokio::test]
async fn mcp_and_http_witnessed_search_has_durable_receipt() {
    let (temp, store) = open_store().await;
    seed_witnessed_fact(&store, "facts", "The witness saw the red fox.").await;

    let server = spawn_server_with_store(temp, store).await;
    let client = Client::new();
    let device = register_device(&server, &client).await;
    let actor = register_actor(&server, &client, &device, "agent").await;

    let mcp = client
        .post(format!("{}/v1/mcp", server.base_url))
        .bearer_auth(&device.credential)
        .json(&json!({
            "method": "tools/call",
            "params": {
                "actor_id": actor.actor_id,
                "name": "sm_search_witnessed",
                "arguments": {
                    "query": "witness",
                    "source_types": ["facts"],
                },
            },
            "id": 1,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(mcp.status(), StatusCode::OK);
    let mcp_body: Value = mcp.json().await.unwrap();
    assert!(mcp_body["result"]["receipt"].is_object());
    assert_eq!(mcp_body["result"]["receipt_stored"].as_bool(), Some(true));

    let http = client
        .post(format!("{}/v1/search/witnessed", server.base_url))
        .bearer_auth(&device.credential)
        .json(&json!({
            "query": "witness",
            "source_types": ["facts"],
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(http.status(), StatusCode::OK);
    let http_body: Value = http.json().await.unwrap();
    assert!(http_body["receipt"].is_object());
    assert_eq!(http_body["receipt_stored"].as_bool(), Some(true));
    assert!(http_body["receipt"]["receipt_id"].is_string());
    assert_eq!(
        mcp_body["result"]["results"][0]["item_id"].as_str(),
        http_body["results"][0]["item_id"].as_str(),
    );
}

#[tokio::test]
async fn audit_events_are_available() {
    let server = spawn_server().await;
    let client = Client::new();
    let device = register_device(&server, &client).await;
    let _ = register_actor(&server, &client, &device, "agent").await;

    let response = client
        .get(format!("{}/v1/health", server.base_url))
        .bearer_auth(&device.credential)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let events = client
        .get(format!("{}/v1/audit/events", server.base_url))
        .bearer_auth(&device.credential)
        .send()
        .await
        .unwrap();
    assert_eq!(events.status(), StatusCode::OK);
    let body: Vec<Value> = events.json().await.unwrap();
    assert!(!body.is_empty());
}

#[tokio::test]
async fn route_aliases_cover_root_and_versioned_endpoints() {
    let server = spawn_server().await;
    let client = Client::new();
    let device = register_device(&server, &client).await;

    let livez_v1 = client
        .get(format!("{}/v1/livez", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(livez_v1.status(), StatusCode::OK);
    let livez_root = client
        .get(format!("{}/livez", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(livez_root.status(), StatusCode::OK);
    assert_eq!(livez_v1.json::<Value>().await.unwrap()["service"], "up");
    assert_eq!(livez_root.json::<Value>().await.unwrap()["service"], "up");

    assert_eq!(
        client
            .get(format!("{}/healthz", server.base_url))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .get(format!("{}/v1/health", server.base_url))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );

    assert_eq!(
        client
            .get(format!("{}/integrity", server.base_url))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .get(format!("{}/v1/integrity", server.base_url))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );

    assert_eq!(
        client
            .post(format!("{}/mcp", server.base_url))
            .json(&json!({
                "method": "tools/list",
                "id": 1,
            }))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .post(format!("{}/v1/mcp", server.base_url))
            .json(&json!({
                "method": "tools/list",
                "id": 1,
            }))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );

    let health = client
        .get(format!("{}/healthz", server.base_url))
        .bearer_auth(&device.credential)
        .send()
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);

    let mcp_root = client
        .post(format!("{}/mcp", server.base_url))
        .bearer_auth(&device.credential)
        .json(&json!({
            "method": "tools/list",
            "id": 2,
            "params": {
                "actor_id": "not-a-uuid",
            },
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(mcp_root.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn mcp_authenticates_before_actor_validation() {
    let server = spawn_server().await;
    let client = Client::new();
    let device = register_device(&server, &client).await;

    let unauthorized = client
        .post(format!("{}/v1/mcp", server.base_url))
        .json(&json!({
            "method": "tools/list",
            "id": 1,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let revoked = client
        .post(format!(
            "{}/v1/devices/{}/revoke",
            server.base_url, device.device_id
        ))
        .bearer_auth(&device.credential)
        .send()
        .await
        .unwrap();
    assert_eq!(revoked.status(), StatusCode::OK);

    let denied = client
        .post(format!("{}/v1/mcp", server.base_url))
        .bearer_auth(&device.credential)
        .json(&json!({
            "method": "tools/list",
            "params": {"actor_id": "not-a-uuid"},
            "id": 2,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
}

#[test]
fn server_data_directory_matches_admin_store_layout() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().to_str().unwrap();
    let store = build_memory_store(data_dir).unwrap();
    assert!(temp.path().join("pooled.db").is_file());
    drop(store);
    let reopened = build_memory_store(data_dir).unwrap();
    drop(reopened);
    assert!(!temp.path().join("pooled.db").join("pooled.db").exists());
}

#[tokio::test]
async fn legacy_sync_routes_fail_closed_before_auth_or_body_processing() {
    let server = spawn_server().await;
    let client = Client::new();

    for (route, body) in [
        (
            "/v1/sync",
            json!({
                "home_device_id": "caller-controlled",
                "store_id": "../../escape",
                "start_sequence": 1,
                "entries": [{
                    "sequence": 1,
                    "operation_kind": "raw-sql",
                    "payload_hex": "44524f50205441424c452066616374733b"
                }]
            }),
        ),
        (
            "/v1/sync/facts",
            json!({
                "facts": [{
                    "fact_id": "caller-controlled",
                    "namespace": "sync-test",
                    "content": "must not be admitted"
                }]
            }),
        ),
    ] {
        let response = client
            .post(format!("{}{route}", server.base_url))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED, "{route}");
        let response_body: Value = response.json().await.unwrap();
        assert_eq!(response_body["error"], "SYNC_DISABLED", "{route}");
    }
}

#[cfg(feature = "server")]
#[tokio::test]
async fn fact_create_http_rejects_batches_outside_admission_window_before_and_after_cutoff() {
    let (server, device, count_store) = spawn_admitted_server().await;
    let client = Client::new();
    let (mut batch, key) = fact_batch(&device.device_id, "primary", "general", "fence-7");
    count_store
        .admit_fact_create_key(FactCreateAdmission {
            device_id: DeviceId::parse(&device.device_id).unwrap(),
            store_id: "primary".into(),
            namespace: "general".into(),
            principal_id: "http-replication-key".into(),
            key_version: 1,
            public_key: key.verifying_key().to_bytes(),
            activated_at: 2_000,
            cutoff_at: 3_000,
            stream_epoch: 7,
            fencing_token: "fence-7".into(),
        })
        .await
        .unwrap();
    batch.batch_id = "batch-before-activation".into();
    batch.sign(&key).unwrap();
    assert_eq!(
        post_fact(&server, &client, &device, &batch).await.0,
        StatusCode::FORBIDDEN
    );
    let (mut after, _) = fact_batch(&device.device_id, "primary", "general", "fence-7");
    after.batch_id = "batch-after-cutoff".into();
    after.observed_at = 4_000;
    after.sign(&key).unwrap();
    assert_eq!(
        post_fact(&server, &client, &device, &after).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(fact_count(&count_store, &device.device_id).await, 0);
}

#[cfg(feature = "server")]
#[tokio::test]
async fn fact_create_http_rejects_same_batch_id_with_changed_signed_body() {
    let (server, device, count_store) = spawn_admitted_server().await;
    let client = Client::new();
    let (batch, key) = fact_batch(&device.device_id, "primary", "general", "fence-7");
    assert_eq!(
        post_fact(&server, &client, &device, &batch).await.0,
        StatusCode::OK
    );
    let mut altered = batch.clone();
    altered.observed_at += 1;
    altered.sign(&key).unwrap();
    assert_eq!(
        post_fact(&server, &client, &device, &altered).await.0,
        StatusCode::CONFLICT
    );
    assert_eq!(fact_count(&count_store, &device.device_id).await, 1);
}

#[cfg(feature = "server")]
#[tokio::test]
async fn fact_create_http_recovers_when_semantic_fact_preexists_without_ack() {
    let (server, device, count_store) = spawn_admitted_server().await;
    let client = Client::new();
    let (batch, _) = fact_batch(&device.device_id, "primary", "general", "fence-7");
    let envelope = batch.semantic_envelopes().unwrap().remove(0);
    count_store
        .device_memory(&DeviceId::parse(&device.device_id).unwrap())
        .await
        .unwrap()
        .apply_verified_fact_create(envelope)
        .await
        .unwrap();
    let (status, ack) = post_fact(&server, &client, &device, &batch).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ack["disposition"], "accepted");
    assert_eq!(post_fact(&server, &client, &device, &batch).await.1, ack);
    assert_eq!(fact_count(&count_store, &device.device_id).await, 1);
}

#[cfg(feature = "server")]
#[tokio::test]
async fn fact_create_http_exact_retry_survives_real_store_reopen() {
    let (server, device, count_store) = spawn_admitted_server().await;
    let client = Client::new();
    let (batch, _) = fact_batch(&device.device_id, "primary", "general", "fence-7");
    let (status, ack) = post_fact(&server, &client, &device, &batch).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fact_count(&count_store, &device.device_id).await, 1);
    let temp = server.stop_keep_temp().await;
    drop(count_store);
    let base = temp.path().to_path_buf();
    let reopened = MnemesStore::open_with_embedder(
        base.join("pooled-store"),
        semantic_memory::MemoryConfig {
            base_dir: base,
            ..Default::default()
        },
        Box::new(semantic_memory::MockEmbedder::new(768)),
    )
    .unwrap();
    let reopened_count = MnemesStore::open_with_embedder(
        temp.path().join("pooled-store"),
        semantic_memory::MemoryConfig {
            base_dir: temp.path().to_path_buf(),
            ..Default::default()
        },
        Box::new(semantic_memory::MockEmbedder::new(768)),
    )
    .unwrap();
    let server = spawn_server_with_store(temp, reopened).await;
    let (retry_status, retry_ack) = post_fact(&server, &client, &device, &batch).await;
    assert_eq!(retry_status, StatusCode::OK);
    assert_eq!(retry_ack, ack);
    assert_eq!(fact_count(&reopened_count, &device.device_id).await, 1);
    server.stop().await;
}

#[cfg(feature = "server")]
#[tokio::test]
async fn default_router_quarantines_fact_supersede_before_semantic_mutation() {
    let (server, device, count_store) = spawn_admitted_server().await;
    let response = Client::new()
        .post(format!(
            "{}/v1/replication/fact-supersede/v1",
            server.base_url
        ))
        .bearer_auth(&device.credential)
        .body("{}")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(fact_count(&count_store, &device.device_id).await, 0);
    server.stop().await;
}

#[cfg(feature = "server")]
#[tokio::test]
async fn fact_supersede_http_applies_owner_export_then_returns_same_ack_on_retry() {
    let (server, device, receiver) = spawn_admitted_staged_fact_supersede_server().await;
    admit_supersede(&receiver, &device.device_id).await;
    let client = Client::new();
    let (create, supersede) = owner_created_supersede_envelopes(&device.device_id).await;
    let create_entry = FactCreateTransportEntryV1 {
        sequence: create.sequence,
        payload: create.payload,
        payload_digest: create.payload_digest,
        predecessor_digest: create.predecessor_digest,
        journal_envelope_digest: create.envelope_digest,
    };
    let mut create_batch = SignedFactCreateBatchV1::new(
        "owner-create-for-supersede",
        device.device_id.clone(),
        "primary",
        7,
        1,
        vec![create_entry],
        "http-replication-key",
        1,
        1_000,
        "fence-7",
    )
    .unwrap();
    create_batch
        .sign(&SigningKey::from_bytes(&[7u8; 32]))
        .unwrap();
    assert_eq!(
        post_fact(&server, &client, &device, &create_batch).await.0,
        StatusCode::OK
    );
    let (batch, _) = supersede_batch("owner-supersede-1", supersede, 4, "supersede-fence-4");
    let (status, ack) = post_supersede(&server, &client, &device, &batch).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ack["disposition"], "accepted");
    assert_eq!(fact_count(&receiver, &device.device_id).await, 2);
    let (status, duplicate_ack) = post_supersede(&server, &client, &device, &batch).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(duplicate_ack, ack);
    assert_eq!(fact_count(&receiver, &device.device_id).await, 2);
    server.stop().await;
}

#[cfg(feature = "server")]
#[tokio::test]
async fn fact_supersede_http_rejects_key_revoked_via_admin_cli_without_semantic_change() {
    let (temp, store) = open_store().await;
    let data_dir = temp.path().join("pooled-store");
    let device_id = DeviceId::new();
    let (registered, credential) = store
        .register_device_with_generated_credential(Device::new(
            device_id,
            "ci-device",
            "linux",
            "localhost",
        ))
        .await
        .unwrap();
    store
        .admit_fact_supersede_key(FactSupersedeAdmission {
            device_id: registered.clone(),
            store_id: "primary".into(),
            replacement_namespace: "general".into(),
            principal_id: "http-supersede-key".into(),
            key_version: 1,
            public_key: SigningKey::from_bytes(&[8u8; 32])
                .verifying_key()
                .to_bytes(),
            activated_at: 0,
            cutoff_at: i64::MAX as u64,
            store_epoch: 3,
            writer_epoch: 4,
            fencing_token: "supersede-fence-4".into(),
        })
        .await
        .unwrap();
    let revoked = Command::new(env!("CARGO_BIN_EXE_mnemes-admin"))
        .args([
            "fact-supersede-revoke",
            data_dir.to_str().unwrap(),
            registered.as_str(),
            "primary",
            "general",
            "http-supersede-key",
            "1",
        ])
        .output()
        .unwrap();
    assert!(
        revoked.status.success(),
        "{}",
        String::from_utf8_lossy(&revoked.stderr)
    );
    let count_store = MnemesStore::open_with_embedder(
        data_dir,
        semantic_memory::MemoryConfig {
            base_dir: temp.path().to_path_buf(),
            ..Default::default()
        },
        Box::new(semantic_memory::MockEmbedder::new(768)),
    )
    .unwrap();
    let server = spawn_staged_fact_supersede_server_with_store(temp, store).await;
    let device = DeviceIdentity {
        device_id: registered.to_string(),
        credential,
    };
    let (_, supersede) = owner_created_supersede_envelopes(&device.device_id).await;
    let (batch, _) = supersede_batch("supersede-revoked", supersede, 4, "supersede-fence-4");
    assert_eq!(
        post_supersede(&server, &Client::new(), &device, &batch)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(fact_count(&count_store, &device.device_id).await, 0);
    let temp = server.stop_keep_temp().await;
    assert_eq!(supersede_ack_count(&temp, "supersede-revoked"), 0);
    assert_eq!(supersede_inbox_stream_count(&temp, &device.device_id), 0);
}

#[cfg(feature = "server")]
#[tokio::test]
async fn fact_supersede_http_refuses_collision_stale_predecessor_and_stale_writer_epoch() {
    let (server, device, receiver) = spawn_admitted_staged_fact_supersede_server().await;
    admit_supersede(&receiver, &device.device_id).await;
    let client = Client::new();
    let (create, supersede) = owner_created_supersede_envelopes(&device.device_id).await;
    let create_entry = FactCreateTransportEntryV1 {
        sequence: create.sequence,
        payload: create.payload,
        payload_digest: create.payload_digest,
        predecessor_digest: create.predecessor_digest,
        journal_envelope_digest: create.envelope_digest,
    };
    let mut create_batch = SignedFactCreateBatchV1::new(
        "owner-create-for-refusal",
        device.device_id.clone(),
        "primary",
        7,
        1,
        vec![create_entry],
        "http-replication-key",
        1,
        1_000,
        "fence-7",
    )
    .unwrap();
    create_batch
        .sign(&SigningKey::from_bytes(&[7u8; 32]))
        .unwrap();
    assert_eq!(
        post_fact(&server, &client, &device, &create_batch).await.0,
        StatusCode::OK
    );

    let (mut stale_writer, _) =
        supersede_batch("writer-stale", supersede.clone(), 3, "supersede-fence-4");
    stale_writer
        .sign(&SigningKey::from_bytes(&[8u8; 32]))
        .unwrap();
    assert_eq!(
        post_supersede(&server, &client, &device, &stale_writer)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(fact_count(&receiver, &device.device_id).await, 1);

    let mut stale_envelope = supersede.clone();
    let mut stale_payload: FactSupersedePayloadV1 =
        serde_json::from_slice(&stale_envelope.payload).unwrap();
    stale_payload.semantic_predecessor_digest = "0".repeat(64);
    stale_envelope.payload = serde_json::to_vec(&stale_payload).unwrap();
    stale_envelope.reseal();
    let (stale, _) = supersede_batch("semantic-stale", stale_envelope, 4, "supersede-fence-4");
    let (stale_status, stale_body) = post_supersede(&server, &client, &device, &stale).await;
    assert_eq!(stale_status, StatusCode::CONFLICT);
    assert_eq!(
        stale_body["error"],
        Value::String("fact-supersede conflict".into())
    );
    assert_eq!(fact_count(&receiver, &device.device_id).await, 1);

    let mut terminal_stale = supersede.clone();
    let accepted_owner_head = supersede.envelope_digest;
    let (valid, key) = supersede_batch(
        "owner-supersede-collision",
        supersede,
        4,
        "supersede-fence-4",
    );
    assert_eq!(
        post_supersede(&server, &client, &device, &valid).await.0,
        StatusCode::OK
    );
    terminal_stale.sequence = 3;
    terminal_stale.predecessor_digest = accepted_owner_head;
    terminal_stale.reseal();
    let (terminal_stale, _) = supersede_batch(
        "semantic-stale-replay",
        terminal_stale,
        4,
        "supersede-fence-4",
    );
    for _ in 0..2 {
        let (status, body) = post_supersede(&server, &client, &device, &terminal_stale).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(
            body["error"],
            Value::String("fact-supersede conflict".into()),
            "an exact stale replay must remain a conflict"
        );
    }
    let mut collision = valid.clone();
    collision.store_epoch = 9;
    collision.sign(&key).unwrap();
    assert_eq!(
        post_supersede(&server, &client, &device, &collision)
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert_eq!(fact_count(&receiver, &device.device_id).await, 2);
    let temp = server.stop_keep_temp().await;
    assert_eq!(supersede_ack_count(&temp, "writer-stale"), 0);
    assert_eq!(supersede_ack_count(&temp, "semantic-stale"), 0);
    assert_eq!(supersede_ack_count(&temp, "semantic-stale-replay"), 0);
    assert_eq!(supersede_inbox_next_sequence(&temp, &device.device_id), 3);
}

#[cfg(feature = "server")]
#[tokio::test]
async fn fact_supersede_http_reports_owner_stream_gap_as_conflict_without_ack() {
    let (server, device, receiver) = spawn_admitted_staged_fact_supersede_server().await;
    admit_supersede(&receiver, &device.device_id).await;
    let client = Client::new();
    let (create, mut supersede) = owner_created_supersede_envelopes(&device.device_id).await;
    apply_owner_create_for_supersede(&server, &client, &device, &create, "owner-create-for-gap")
        .await;
    supersede.sequence = 3;
    supersede.reseal();
    let (batch, _) = supersede_batch("owner-supersede-gap", supersede, 4, "supersede-fence-4");
    assert_eq!(
        post_supersede(&server, &client, &device, &batch).await.0,
        StatusCode::CONFLICT
    );
    assert_eq!(fact_count(&receiver, &device.device_id).await, 1);
    let temp = server.stop_keep_temp().await;
    assert_eq!(supersede_ack_count(&temp, "owner-supersede-gap"), 0);
    assert_eq!(supersede_inbox_next_sequence(&temp, &device.device_id), 2);
}

#[cfg(feature = "server")]
#[tokio::test]
async fn fact_supersede_http_reports_owner_epoch_conflict_without_ack() {
    let (server, device, receiver) = spawn_admitted_staged_fact_supersede_server().await;
    admit_supersede(&receiver, &device.device_id).await;
    let client = Client::new();
    let (create, mut supersede) = owner_created_supersede_envelopes(&device.device_id).await;
    apply_owner_create_for_supersede(
        &server,
        &client,
        &device,
        &create,
        "owner-create-for-epoch-conflict",
    )
    .await;
    supersede.stream_epoch = 8;
    supersede.reseal();
    let (batch, _) = supersede_batch(
        "owner-supersede-epoch-conflict",
        supersede,
        4,
        "supersede-fence-4",
    );
    assert_eq!(
        post_supersede(&server, &client, &device, &batch).await.0,
        StatusCode::CONFLICT
    );
    assert_eq!(fact_count(&receiver, &device.device_id).await, 1);
    let temp = server.stop_keep_temp().await;
    assert_eq!(
        supersede_ack_count(&temp, "owner-supersede-epoch-conflict"),
        0
    );
    assert_eq!(supersede_inbox_next_sequence(&temp, &device.device_id), 2);
}

#[cfg(feature = "server")]
#[tokio::test]
async fn fact_supersede_http_reports_changed_same_sequence_owner_envelope_as_conflict_without_ack()
{
    let (server, device, receiver) = spawn_admitted_staged_fact_supersede_server().await;
    admit_supersede(&receiver, &device.device_id).await;
    let client = Client::new();
    let (create, supersede) = owner_created_supersede_envelopes(&device.device_id).await;
    apply_owner_create_for_supersede(&server, &client, &device, &create, "owner-create-for-fork")
        .await;
    let (valid, _) = supersede_batch(
        "owner-supersede-for-fork",
        supersede.clone(),
        4,
        "supersede-fence-4",
    );
    assert_eq!(
        post_supersede(&server, &client, &device, &valid).await.0,
        StatusCode::OK
    );
    let mut changed = supersede;
    changed.payload.push(b' ');
    changed.reseal();
    let (fork, _) = supersede_batch("owner-supersede-fork", changed, 4, "supersede-fence-4");
    assert_eq!(
        post_supersede(&server, &client, &device, &fork).await.0,
        StatusCode::CONFLICT
    );
    assert_eq!(fact_count(&receiver, &device.device_id).await, 2);
    let temp = server.stop_keep_temp().await;
    assert_eq!(supersede_ack_count(&temp, "owner-supersede-fork"), 0);
    assert_eq!(supersede_inbox_next_sequence(&temp, &device.device_id), 3);
}
