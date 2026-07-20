use pooled_memory::server::{build_memory_store, build_router};
use pooled_memory::PooledMemoryStore;
use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use std::path::PathBuf;
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

async fn open_store() -> (TempDir, PooledMemoryStore) {
    let temp = TempDir::new().unwrap();
    let base = PathBuf::from(temp.path());
    let store = PooledMemoryStore::open_with_embedder(
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
async fn spawn_server_with_store(_temp: TempDir, store: PooledMemoryStore) -> RunningServer {
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
async fn seed_witnessed_fact(store: &PooledMemoryStore, namespace: &str, content: &str) {
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
