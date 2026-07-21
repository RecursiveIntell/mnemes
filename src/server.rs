#[cfg(feature = "server")]
use crate::{
    Actor, ActorId, Device, DeviceId, MnemesError, MnemesStore, OperationEnvelope, OperationId,
    OperationKind, ToolProfile,
};
#[cfg(feature = "server")]
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
#[cfg(feature = "server")]
use chrono::Utc;
#[cfg(feature = "server")]
use semantic_memory::{
    ExactnessProfile, MemoryConfig, ReceiptMode, SearchContext, SearchSource, SearchSourceType,
    VectorSearchReceiptV1, VerifyMode,
};
#[cfg(feature = "server")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "server")]
use serde_json::{json, Value};
#[cfg(feature = "server")]
use std::time::Duration;
#[cfg(feature = "server")]
use std::{collections::HashMap, path::PathBuf, sync::Arc};
#[cfg(feature = "server")]
use tower::limit::ConcurrencyLimitLayer;
#[cfg(feature = "server")]
use tower_http::limit::RequestBodyLimitLayer;
#[cfg(feature = "server")]
use tower_http::timeout::TimeoutLayer;
#[cfg(feature = "server")]
use uuid::Uuid;

#[cfg(feature = "server")]
pub const SCHEMA_VERSION: &str = "mnemes.server.v1";

#[cfg(feature = "server")]
#[derive(Clone)]
pub struct ServerState {
    store: Arc<MnemesStore>,
    server_id: String,
    trusted_keys: Arc<crate::replication::TrustedKeyRegistry>,
}

#[cfg(feature = "server")]
#[derive(Debug)]
struct ServerContext {
    device: Device,
    actor: Option<Actor>,
}

#[cfg(feature = "server")]
#[derive(Debug, Clone)]
struct AuditContext {
    device: Option<Device>,
    actor: Option<Actor>,
}

#[cfg(feature = "server")]
#[derive(Serialize)]
struct ErrorResponse {
    error: &'static str,
}

#[cfg(feature = "server")]
#[derive(Serialize)]
struct HealthResponse {
    service_id: String,
    schema_version: String,
    server_time: String,
    ready: bool,
    embedding: HashMap<&'static str, String>,
}

#[cfg(feature = "server")]
#[derive(Deserialize)]
struct RegisterDeviceRequest {
    label: String,
    platform: String,
    hostname: String,
}

#[cfg(feature = "server")]
#[derive(Serialize)]
struct RegisterDeviceResponse {
    device_id: String,
    credential: String,
    first_seen_at: String,
    last_seen_at: String,
    status: String,
}

#[cfg(feature = "server")]
#[derive(Deserialize)]
struct RegisterActorRequest {
    device_id: String,
    actor_kind: String,
    #[serde(default)]
    provider_model: Option<String>,
    #[serde(default)]
    tool_profile: Option<String>,
}

#[cfg(feature = "server")]
#[derive(Serialize)]
struct RegisterActorResponse {
    actor_id: String,
}

#[cfg(feature = "server")]
#[derive(Deserialize)]
struct ActorFilterQuery {
    #[serde(default)]
    device_id: Option<String>,
}

#[cfg(feature = "server")]
#[derive(Deserialize)]
struct OperationFilterQuery {
    #[serde(default)]
    device_id: Option<String>,
    #[serde(default)]
    actor_id: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[cfg(feature = "server")]
#[derive(Deserialize)]
struct OperationSubmitRequest {
    idempotency_key: String,
    requesting_device_id: String,
    requesting_actor_id: String,
    operation_kind: String,
    target_kind: String,
    target_id: String,
    content_digest: String,
    #[serde(default)]
    recording_device_id: Option<String>,
    #[serde(default)]
    recording_server_id: Option<String>,
    #[serde(default)]
    observed_at: Option<String>,
    #[serde(default)]
    valid_time: Option<String>,
}

#[cfg(feature = "server")]
#[derive(Serialize)]
struct OperationEnvelopeResponse {
    operation_id: String,
    receipt_id: String,
    idempotency_key: String,
    recorded_at: String,
}

#[cfg(feature = "server")]
#[derive(Serialize)]
struct OperationListItem {
    operation_id: String,
    requesting_device_id: String,
    requesting_actor_id: String,
    operation_kind: String,
    target_kind: String,
    target_id: String,
    content_digest: String,
    observed_at: Option<String>,
    valid_time: Option<String>,
    recorded_at: String,
    idempotency_key: String,
    receipt_id: Option<String>,
}

#[cfg(feature = "server")]
#[derive(Deserialize)]
struct AuditQuery {
    #[serde(default)]
    limit: Option<usize>,
}

#[cfg(feature = "server")]
#[derive(Deserialize)]
struct JsonRpcRequest {
    #[serde(default)]
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Option<Value>,
}

#[cfg(feature = "server")]
#[derive(Serialize)]
struct JsonRpcResponse {
    jsonrpc: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

#[cfg(feature = "server")]
#[derive(Serialize)]
struct JsonRpcError {
    code: i32,
    message: String,
}

#[cfg(feature = "server")]
#[derive(Serialize)]
struct McpTool {
    name: String,
    description: String,
    #[serde(rename = "inputSchema")]
    input_schema: Value,
}

#[cfg(feature = "server")]
#[derive(Serialize)]
struct WitnessedSearchItem {
    item_id: String,
    namespace: String,
    content: String,
    score: f64,
    source: String,
    proof: String,
    source_provenance: String,
}

#[cfg(feature = "server")]
#[derive(Serialize)]
struct WitnessedSearchResponse {
    results: Vec<WitnessedSearchItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    receipt: Option<VectorSearchReceiptV1>,
    receipt_stored: bool,
}

#[cfg(feature = "server")]
#[derive(Deserialize)]
struct WitnessedSearchRequest {
    query: String,
    #[serde(default)]
    namespaces: Option<Vec<String>>,
    #[serde(default)]
    source_types: Option<Vec<String>>,
    #[serde(default)]
    limit: Option<usize>,
}

#[cfg(feature = "server")]
#[derive(Serialize)]
struct IntegrityCheckReport {
    status: &'static str,
    detail: String,
}

#[cfg(feature = "server")]
#[derive(Serialize)]
struct VerifyIntegrityResponse {
    pooled_sqlite: IntegrityCheckReport,
    semantic_memory: IntegrityCheckReport,
}

#[cfg(feature = "server")]
#[derive(Deserialize)]
struct McpDeviceRequest {
    device_id: String,
}

#[cfg(feature = "server")]
#[derive(Deserialize)]
struct McpActorRequest {
    actor_id: String,
}

#[cfg(feature = "server")]
#[derive(Deserialize)]
struct McpOperationRequest {
    operation_id: String,
}

#[cfg(feature = "server")]
#[derive(Deserialize)]
struct McpRegisterActorRequest {
    device_id: String,
    actor_kind: String,
    #[serde(default)]
    provider_model: Option<String>,
    #[serde(default)]
    tool_profile: Option<String>,
}

#[cfg(feature = "server")]
#[derive(Deserialize)]
struct McpSearchRequest {
    query: String,
    #[serde(default)]
    namespaces: Option<Vec<String>>,
    #[serde(default)]
    source_types: Option<Vec<String>>,
    #[serde(default)]
    limit: Option<usize>,
}

#[cfg(feature = "server")]
#[derive(Serialize)]
struct McpStatsResponse {
    pooled: PoolStats,
    semantic: serde_json::Value,
}

#[cfg(feature = "server")]
#[derive(Serialize)]
struct PoolStats {
    devices: u64,
    actors: u64,
    operations: u64,
}

#[cfg(feature = "server")]
#[derive(Serialize)]
struct DeviceInfo {
    device_id: String,
    label: String,
    platform: String,
    hostname: String,
    status: String,
    first_seen_at: String,
    last_seen_at: String,
}

#[cfg(feature = "server")]
#[derive(Serialize)]
struct ActorInfo {
    actor_id: String,
    device_id: String,
    actor_kind: String,
    tool_profile: String,
    provider_model: Option<String>,
    recorded_at: String,
}

#[cfg(feature = "server")]
#[derive(Serialize)]
struct OperationInfo {
    operation_id: String,
    requesting_device_id: String,
    requesting_actor_id: String,
    operation_kind: String,
    target_kind: String,
    target_id: String,
    content_digest: String,
    observed_at: Option<String>,
    valid_time: Option<String>,
    recorded_at: String,
    idempotency_key: String,
    receipt_id: Option<String>,
}

#[cfg(feature = "server")]
pub fn build_router(store: MnemesStore) -> Router {
    build_router_with_state(ServerState {
        store: Arc::new(store),
        server_id: Uuid::new_v4().to_string(),
        trusted_keys: Arc::new(crate::replication::TrustedKeyRegistry::new()),
    })
}

#[cfg(feature = "server")]
fn build_router_with_state(state: ServerState) -> Router {
    Router::new()
        .route("/livez", get(livez_handler))
        .route("/v1/health", get(health_handler))
        .route("/v1/livez", get(livez_handler))
        .route("/healthz", get(health_handler))
        .route("/v1/integrity", get(integrity_handler))
        .route("/integrity", get(integrity_handler))
        .route("/v1/devices/register", post(register_device_handler))
        .route("/v1/devices", get(list_devices_handler))
        .route("/v1/devices/:device_id/heartbeat", post(heartbeat_handler))
        .route("/v1/devices/:device_id/rotate", post(rotate_device_handler))
        .route("/v1/devices/:device_id/revoke", post(revoke_device_handler))
        .route(
            "/v1/devices/:device_id/quarantine",
            post(quarantine_device_handler),
        )
        .route(
            "/v1/actors",
            get(list_actors_handler).post(register_actor_handler),
        )
        .route(
            "/v1/operations",
            post(submit_operation_handler).get(list_operations_handler),
        )
        .route("/v1/operations/:operation_id", get(get_operation_handler))
        .route("/v1/search/witnessed", post(search_witnessed_handler))
        .route("/v1/sync", post(sync_endpoint))
        .route("/v1/receipts/:receipt_id", get(get_receipt_handler))
        .route("/v1/audit/events", get(list_audit_events_handler))
        .route("/mcp", post(mcp_handler))
        .route("/v1/mcp", post(mcp_handler))
        .with_state(state)
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(30),
        ))
        .layer(RequestBodyLimitLayer::new(64 * 1024))
        .layer(ConcurrencyLimitLayer::new(64))
}

#[cfg(feature = "server")]
fn bearer_token(headers: &HeaderMap) -> Result<String, MnemesError> {
    let value = headers
        .get("authorization")
        .ok_or(MnemesError::InvalidCredential)?;
    let raw = value.to_str().map_err(|_| MnemesError::InvalidCredential)?;
    if !raw.starts_with("Bearer ") {
        return Err(MnemesError::InvalidCredential);
    }
    let token = raw.trim_start_matches("Bearer ").trim();
    if token.is_empty() {
        return Err(MnemesError::InvalidCredential);
    }
    Ok(token.to_string())
}

#[cfg(feature = "server")]
fn parse_device_id(raw: &str) -> Result<DeviceId, MnemesError> {
    DeviceId::parse(raw).map_err(|_| MnemesError::DeviceNotFound(raw.to_string()))
}

#[cfg(feature = "server")]
fn parse_actor_id(raw: &str) -> Result<ActorId, MnemesError> {
    ActorId::parse(raw).map_err(|_| MnemesError::InvalidAsOf("invalid actor id".to_string()))
}

#[cfg(feature = "server")]
async fn authorize(
    state: &ServerState,
    headers: &HeaderMap,
    actor_id: Option<ActorId>,
) -> Result<ServerContext, MnemesError> {
    let token = bearer_token(headers)?;
    let actor_ref = actor_id.as_ref();
    let (device, actor) = state.store.authenticate_request(&token, actor_ref).await?;
    Ok(ServerContext { device, actor })
}

#[cfg(feature = "server")]
fn map_store_error(error: &MnemesError) -> (StatusCode, &'static str, &'static str) {
    match error {
        MnemesError::InvalidCredential => {
            (StatusCode::UNAUTHORIZED, "invalid credentials", "denied")
        }
        MnemesError::DeviceNotActive(_) | MnemesError::AuthorizationDenied(_) => {
            (StatusCode::FORBIDDEN, "access denied", "denied")
        }
        MnemesError::DeviceNotFound(_) | MnemesError::ActorNotFound(_) => {
            (StatusCode::NOT_FOUND, "not found", "error")
        }
        _ => (StatusCode::BAD_REQUEST, "invalid request", "error"),
    }
}

#[cfg(feature = "server")]
fn unauthorized_rpc(error: MnemesError, id: Option<Value>) -> Response {
    let (status, message, _) = map_store_error(&error);
    let code = match status {
        StatusCode::UNAUTHORIZED => -32000,
        StatusCode::FORBIDDEN => -32001,
        StatusCode::NOT_FOUND => -32002,
        _ => -32600,
    };
    let response = JsonRpcResponse {
        jsonrpc: "2.0",
        id,
        result: None,
        error: Some(JsonRpcError {
            code,
            message: message.to_string(),
        }),
    };
    (status, Json(response)).into_response()
}

#[cfg(feature = "server")]
async fn log_audit(
    state: &ServerState,
    context: &AuditContext,
    endpoint: &str,
    method: &str,
    outcome: &str,
    detail: Option<&str>,
) {
    let _ = state
        .store
        .log_audit_event(
            context.device.as_ref().map(|d| &d.device_id),
            context.actor.as_ref().map(|a| &a.actor_id),
            endpoint,
            method,
            outcome,
            detail,
        )
        .await;
}

#[cfg(feature = "server")]
fn error_response(error: &MnemesError) -> Response {
    let (status, message, _) = map_store_error(error);
    (status, Json(ErrorResponse { error: message })).into_response()
}

#[cfg(feature = "server")]
fn ensure_operator(actor: Option<&Actor>) -> Result<(), MnemesError> {
    let actor =
        actor.ok_or_else(|| MnemesError::AuthorizationDenied("actor required".to_string()))?;
    if actor.tool_profile != ToolProfile::Operator {
        return Err(MnemesError::AuthorizationDenied(
            "operator profile required".to_string(),
        ));
    }
    Ok(())
}

#[cfg(feature = "server")]
async fn health_handler(headers: HeaderMap, State(state): State<ServerState>) -> Response {
    let context = match authorize(&state, &headers, None).await {
        Ok(context) => context,
        Err(error) => return error_response(&error),
    };

    let mut embedding = HashMap::new();
    embedding.insert(
        "dimensions",
        state.store.memory_config().embedding.dimensions.to_string(),
    );
    embedding.insert("model", state.store.memory_config().embedding.model.clone());

    let response = HealthResponse {
        service_id: state.server_id.clone(),
        schema_version: SCHEMA_VERSION.to_string(),
        server_time: Utc::now().to_rfc3339(),
        ready: matches!(context.device.status, crate::DeviceStatus::Active),
        embedding,
    };

    log_audit(
        &state,
        &AuditContext {
            device: Some(context.device.clone()),
            actor: context.actor,
        },
        "/v1/health",
        "GET",
        "ok",
        Some("health read"),
    )
    .await;

    (StatusCode::OK, Json(response)).into_response()
}

#[cfg(feature = "server")]
async fn livez_handler() -> Response {
    let response = serde_json::json!({
        "service": "up",
    });

    (StatusCode::OK, Json(response)).into_response()
}

#[cfg(feature = "server")]
async fn integrity_handler(headers: HeaderMap, State(state): State<ServerState>) -> Response {
    let context = match authorize(&state, &headers, None).await {
        Ok(context) => context,
        Err(error) => return error_response(&error),
    };

    let report = match state
        .store
        .memory()
        .verify_integrity(VerifyMode::Quick)
        .await
    {
        Ok(report) => report,
        Err(error) => {
            let _ = log_audit(
                &state,
                &AuditContext {
                    device: Some(context.device.clone()),
                    actor: context.actor,
                },
                "/v1/integrity",
                "GET",
                "error",
                Some("verify failed"),
            )
            .await;
            return error_response(&MnemesError::Memory(error));
        }
    };

    let response = serde_json::json!({
        "service_id": state.server_id.clone(),
        "schema_version": SCHEMA_VERSION,
        "checked_at": Utc::now().to_rfc3339(),
        "report": format!("{:?}", report),
    });

    log_audit(
        &state,
        &AuditContext {
            device: Some(context.device),
            actor: context.actor,
        },
        "/v1/integrity",
        "GET",
        "ok",
        Some("integrity report read"),
    )
    .await;

    (StatusCode::OK, Json(response)).into_response()
}

#[cfg(feature = "server")]
async fn register_device_handler(
    headers: HeaderMap,
    State(state): State<ServerState>,
    Json(payload): Json<RegisterDeviceRequest>,
) -> Response {
    match std::env::var("BOOTSTRAP_SECRET") {
        Ok(secret) => match bearer_token(&headers) {
            Ok(token) if token == secret => {}
            _ => return error_response(&MnemesError::InvalidCredential),
        },
        Err(std::env::VarError::NotPresent) => match state.store.count_devices().await {
            Ok(0) => {}
            Ok(_) => {
                return error_response(&MnemesError::AuthorizationDenied(
                    "registration closed".to_string(),
                ))
            }
            Err(error) => return error_response(&error),
        },
        Err(_) => return error_response(&MnemesError::InvalidCredential),
    }
    let new_device = crate::types::Device::new(
        crate::types::DeviceId::new(),
        payload.label,
        payload.platform,
        payload.hostname,
    );

    let (device_id, credential) = match state
        .store
        .register_device_with_generated_credential(new_device)
        .await
    {
        Ok(value) => value,
        Err(error) => return error_response(&error),
    };

    let device = match state.store.get_device(&device_id).await {
        Ok(Some(device)) => device,
        Ok(None) => {
            return error_response(&MnemesError::DeviceNotFound(device_id.to_string()));
        }
        Err(error) => return error_response(&error),
    };

    let response = RegisterDeviceResponse {
        device_id: device.device_id.to_string(),
        credential,
        first_seen_at: device.first_seen_at.clone(),
        last_seen_at: device.last_seen_at.clone(),
        status: device.status.as_str().to_string(),
    };

    log_audit(
        &state,
        &AuditContext {
            device: Some(device),
            actor: None,
        },
        "/v1/devices/register",
        "POST",
        "ok",
        Some("device registered"),
    )
    .await;

    (StatusCode::CREATED, Json(response)).into_response()
}

#[cfg(feature = "server")]
async fn list_devices_handler(
    headers: HeaderMap,
    State(state): State<ServerState>,
    Query(_query): Query<AuditQuery>,
) -> Response {
    let context = match authorize(&state, &headers, None).await {
        Ok(context) => context,
        Err(error) => return error_response(&error),
    };

    let devices = match state.store.list_devices().await {
        Ok(values) => values,
        Err(error) => return error_response(&error),
    };

    let response: Vec<DeviceInfo> = devices
        .into_iter()
        .map(|value| DeviceInfo {
            device_id: value.device_id.to_string(),
            label: value.label,
            platform: value.platform,
            hostname: value.hostname,
            status: value.status.as_str().to_string(),
            first_seen_at: value.first_seen_at,
            last_seen_at: value.last_seen_at,
        })
        .collect();

    let _ = log_audit(
        &state,
        &AuditContext {
            device: Some(context.device),
            actor: context.actor,
        },
        "/v1/devices",
        "GET",
        "ok",
        Some("devices listed"),
    )
    .await;

    (StatusCode::OK, Json(response)).into_response()
}

#[cfg(feature = "server")]
async fn heartbeat_handler(
    headers: HeaderMap,
    State(state): State<ServerState>,
    Path(device_id): Path<String>,
) -> Response {
    let device_id = match parse_device_id(&device_id) {
        Ok(value) => value,
        Err(error) => return error_response(&error),
    };

    let context = match authorize(&state, &headers, None).await {
        Ok(context) => context,
        Err(error) => return error_response(&error),
    };

    if context.device.device_id != device_id {
        return error_response(&MnemesError::AuthorizationDenied(
            "device mismatch".to_string(),
        ));
    }

    if let Err(error) = state.store.heartbeat_device(&device_id).await {
        let _ = log_audit(
            &state,
            &AuditContext {
                device: Some(context.device),
                actor: context.actor,
            },
            "/v1/devices/{id}/heartbeat",
            "POST",
            "error",
            Some("heartbeat failed"),
        )
        .await;
        return error_response(&error);
    }

    let response = serde_json::json!({"ok": true});

    let _ = log_audit(
        &state,
        &AuditContext {
            device: Some(context.device),
            actor: context.actor,
        },
        "/v1/devices/{id}/heartbeat",
        "POST",
        "ok",
        Some("heartbeat accepted"),
    )
    .await;

    (StatusCode::OK, Json(response)).into_response()
}

#[cfg(feature = "server")]
async fn rotate_device_handler(
    headers: HeaderMap,
    State(state): State<ServerState>,
    Path(device_id): Path<String>,
) -> Response {
    let device_id = match parse_device_id(&device_id) {
        Ok(value) => value,
        Err(error) => return error_response(&error),
    };

    let context = match authorize(&state, &headers, None).await {
        Ok(context) => context,
        Err(error) => return error_response(&error),
    };

    if context.device.device_id != device_id {
        return error_response(&MnemesError::AuthorizationDenied(
            "device mismatch".to_string(),
        ));
    }

    let credential = match state.store.rotate_device_credential(&device_id).await {
        Ok(value) => value,
        Err(error) => {
            let _ = log_audit(
                &state,
                &AuditContext {
                    device: Some(context.device),
                    actor: context.actor,
                },
                "/v1/devices/{id}/rotate",
                "POST",
                "error",
                Some("rotate failed"),
            )
            .await;
            return error_response(&error);
        }
    };

    let response =
        serde_json::json!({"device_id": device_id.to_string(), "credential": credential});
    let _ = log_audit(
        &state,
        &AuditContext {
            device: Some(context.device),
            actor: context.actor,
        },
        "/v1/devices/{id}/rotate",
        "POST",
        "ok",
        Some("device credential rotated"),
    )
    .await;

    (StatusCode::OK, Json(response)).into_response()
}

#[cfg(feature = "server")]
async fn revoke_device_handler(
    headers: HeaderMap,
    State(state): State<ServerState>,
    Path(device_id): Path<String>,
) -> Response {
    let device_id = match parse_device_id(&device_id) {
        Ok(value) => value,
        Err(error) => return error_response(&error),
    };

    let context = match authorize(&state, &headers, None).await {
        Ok(context) => context,
        Err(error) => return error_response(&error),
    };

    if context.device.device_id != device_id {
        return error_response(&MnemesError::AuthorizationDenied(
            "device mismatch".to_string(),
        ));
    }

    if let Err(error) = state.store.revoke_device(&device_id).await {
        let _ = log_audit(
            &state,
            &AuditContext {
                device: Some(context.device),
                actor: context.actor,
            },
            "/v1/devices/{id}/revoke",
            "POST",
            "error",
            Some("revoke failed"),
        )
        .await;
        return error_response(&error);
    }

    let response = serde_json::json!({"status":"revoked","device_id":device_id.to_string()});

    let _ = log_audit(
        &state,
        &AuditContext {
            device: Some(context.device),
            actor: context.actor,
        },
        "/v1/devices/{id}/revoke",
        "POST",
        "ok",
        Some("device revoked"),
    )
    .await;

    (StatusCode::OK, Json(response)).into_response()
}

#[cfg(feature = "server")]
async fn quarantine_device_handler(
    headers: HeaderMap,
    State(state): State<ServerState>,
    Path(device_id): Path<String>,
) -> Response {
    let device_id = match parse_device_id(&device_id) {
        Ok(value) => value,
        Err(error) => return error_response(&error),
    };

    let context = match authorize(&state, &headers, None).await {
        Ok(context) => context,
        Err(error) => return error_response(&error),
    };

    if context.device.device_id != device_id {
        return error_response(&MnemesError::AuthorizationDenied(
            "device mismatch".to_string(),
        ));
    }

    if let Err(error) = state
        .store
        .set_device_status(&device_id, crate::DeviceStatus::Quarantined)
        .await
    {
        let _ = log_audit(
            &state,
            &AuditContext {
                device: Some(context.device),
                actor: context.actor,
            },
            "/v1/devices/{id}/quarantine",
            "POST",
            "error",
            Some("quarantine failed"),
        )
        .await;
        return error_response(&error);
    }

    let response = serde_json::json!({"status":"quarantined","device_id":device_id.to_string()});
    let _ = log_audit(
        &state,
        &AuditContext {
            device: Some(context.device),
            actor: context.actor,
        },
        "/v1/devices/{id}/quarantine",
        "POST",
        "ok",
        Some("device quarantined"),
    )
    .await;

    (StatusCode::OK, Json(response)).into_response()
}

#[cfg(feature = "server")]
async fn list_actors_handler(
    headers: HeaderMap,
    State(state): State<ServerState>,
    Query(query): Query<ActorFilterQuery>,
) -> Response {
    let context = match authorize(&state, &headers, None).await {
        Ok(context) => context,
        Err(error) => return error_response(&error),
    };

    let actors = if let Some(raw) = query.device_id {
        let device_id = match parse_device_id(&raw) {
            Ok(value) => value,
            Err(error) => return error_response(&error),
        };
        match state.store.list_actors_for_device(&device_id).await {
            Ok(values) => values,
            Err(error) => return error_response(&error),
        }
    } else {
        match state.store.list_actors().await {
            Ok(values) => values,
            Err(error) => return error_response(&error),
        }
    };

    let response: Vec<ActorInfo> = actors
        .into_iter()
        .map(|actor| ActorInfo {
            actor_id: actor.actor_id.to_string(),
            device_id: actor.device_id.to_string(),
            actor_kind: actor.actor_kind.as_str().to_string(),
            tool_profile: actor.tool_profile.as_str().to_string(),
            provider_model: actor.provider_model,
            recorded_at: actor.recorded_at,
        })
        .collect();

    let _ = log_audit(
        &state,
        &AuditContext {
            device: Some(context.device),
            actor: context.actor,
        },
        "/v1/actors",
        "GET",
        "ok",
        Some("actors listed"),
    )
    .await;

    (StatusCode::OK, Json(response)).into_response()
}

#[cfg(feature = "server")]
async fn register_actor_handler(
    headers: HeaderMap,
    State(state): State<ServerState>,
    Json(payload): Json<RegisterActorRequest>,
) -> Response {
    let device_id = match parse_device_id(&payload.device_id) {
        Ok(value) => value,
        Err(error) => return error_response(&error),
    };

    let context = match authorize(&state, &headers, None).await {
        Ok(context) => context,
        Err(error) => return error_response(&error),
    };

    if context.device.device_id != device_id {
        return error_response(&MnemesError::AuthorizationDenied(
            "device mismatch".to_string(),
        ));
    }

    let profile = payload
        .tool_profile
        .as_deref()
        .and_then(crate::types::ToolProfile::parse)
        .unwrap_or_default();

    let actor = crate::types::Actor {
        actor_id: crate::types::ActorId::new(),
        device_id,
        actor_kind: crate::types::ActorKind::parse(payload.actor_kind),
        tool_profile: profile,
        provider_model: payload.provider_model,
        recorded_at: String::new(),
    };

    let actor_id = match state.store.register_actor(actor).await {
        Ok(value) => value,
        Err(error) => return error_response(&error),
    };

    let _ = log_audit(
        &state,
        &AuditContext {
            device: Some(context.device),
            actor: context.actor,
        },
        "/v1/actors",
        "POST",
        "ok",
        Some("actor registered"),
    )
    .await;

    let response = RegisterActorResponse {
        actor_id: actor_id.to_string(),
    };

    (StatusCode::CREATED, Json(response)).into_response()
}

#[cfg(feature = "server")]
fn parse_operation_kind(value: &str) -> Result<OperationKind, MnemesError> {
    match value {
        "observe" => Ok(OperationKind::Observe),
        "assert" => Ok(OperationKind::Assert),
        "supersede" => Ok(OperationKind::Supersede),
        "revoke" => Ok(OperationKind::Revoke),
        "redact" => Ok(OperationKind::Redact),
        "adjudicate" => Ok(OperationKind::Adjudicate),
        _ => Err(MnemesError::InvalidAsOf(
            "invalid operation kind".to_string(),
        )),
    }
}

#[cfg(feature = "server")]
async fn submit_operation_handler(
    headers: HeaderMap,
    State(state): State<ServerState>,
    Json(payload): Json<OperationSubmitRequest>,
) -> Response {
    let device_id = match parse_device_id(&payload.requesting_device_id) {
        Ok(value) => value,
        Err(error) => return error_response(&error),
    };

    let actor_id = match parse_actor_id(&payload.requesting_actor_id) {
        Ok(value) => value,
        Err(error) => return error_response(&error),
    };

    let context = match authorize(&state, &headers, Some(actor_id.clone())).await {
        Ok(context) => context,
        Err(error) => return error_response(&error),
    };

    if context.device.device_id != device_id {
        return error_response(&MnemesError::AuthorizationDenied(
            "device mismatch".to_string(),
        ));
    }

    if let Err(error) = ensure_operator(context.actor.as_ref()) {
        return error_response(&error);
    }

    let operation = OperationEnvelope {
        operation_id: OperationId::new(),
        idempotency_key: payload.idempotency_key.clone(),
        requesting_device_id: device_id,
        requesting_actor_id: actor_id,
        recording_device_id: match payload.recording_device_id.as_deref() {
            Some(raw) => match parse_device_id(raw) {
                Ok(value) => value,
                Err(error) => return error_response(&error),
            },
            None => context.device.device_id.clone(),
        },
        recording_server_id: match payload.recording_server_id.as_deref() {
            Some(raw) => match parse_device_id(raw) {
                Ok(value) => value,
                Err(error) => return error_response(&error),
            },
            None => context.device.device_id.clone(),
        },
        operation_kind: match parse_operation_kind(&payload.operation_kind) {
            Ok(value) => value,
            Err(error) => return error_response(&error),
        },
        target_kind: payload.target_kind,
        target_id: payload.target_id,
        content_digest: payload.content_digest,
        observed_at: payload.observed_at,
        valid_time: payload.valid_time,
        recorded_at: String::new(),
        receipt_id: None,
    };

    let receipt_id = match state.store.submit_operation(operation).await {
        Ok(receipt_id) => receipt_id,
        Err(error) => {
            let _ = log_audit(
                &state,
                &AuditContext {
                    device: Some(context.device),
                    actor: context.actor,
                },
                "/v1/operations",
                "POST",
                "error",
                Some("submit failed"),
            )
            .await;
            return error_response(&error);
        }
    };

    let envelope = match state
        .store
        .get_operation_by_idempotency_key(&payload.idempotency_key)
        .await
    {
        Ok(Some(envelope)) => envelope,
        Ok(None) => {
            return error_response(&MnemesError::InvalidAsOf("operation not found".to_string()))
        }
        Err(error) => return error_response(&error),
    };

    let _ = log_audit(
        &state,
        &AuditContext {
            device: Some(context.device),
            actor: context.actor,
        },
        "/v1/operations",
        "POST",
        "ok",
        Some("operation submitted"),
    )
    .await;

    let response = OperationEnvelopeResponse {
        operation_id: envelope.operation_id.to_string(),
        receipt_id,
        idempotency_key: envelope.idempotency_key,
        recorded_at: envelope.recorded_at,
    };

    (StatusCode::CREATED, Json(response)).into_response()
}

#[cfg(feature = "server")]
async fn list_operations_handler(
    headers: HeaderMap,
    State(state): State<ServerState>,
    Query(query): Query<OperationFilterQuery>,
) -> Response {
    let actor_filter = if let Some(raw) = query.actor_id.as_deref() {
        Some(match parse_actor_id(raw) {
            Ok(value) => value,
            Err(error) => return error_response(&error),
        })
    } else {
        None
    };

    let context = match authorize(&state, &headers, actor_filter.clone()).await {
        Ok(context) => context,
        Err(error) => return error_response(&error),
    };

    let device_filter = if let Some(raw) = query.device_id.as_deref() {
        Some(match parse_device_id(raw) {
            Ok(value) => value,
            Err(error) => return error_response(&error),
        })
    } else {
        None
    };

    let ops = match state
        .store
        .list_operations(
            device_filter.as_ref(),
            actor_filter.as_ref(),
            query.limit.unwrap_or(100),
        )
        .await
    {
        Ok(values) => values,
        Err(error) => return error_response(&error),
    };

    let response: Vec<OperationListItem> = ops
        .into_iter()
        .map(|operation| OperationListItem {
            operation_id: operation.operation_id.to_string(),
            requesting_device_id: operation.requesting_device_id.to_string(),
            requesting_actor_id: operation.requesting_actor_id.to_string(),
            operation_kind: operation.operation_kind.as_str().to_string(),
            target_kind: operation.target_kind,
            target_id: operation.target_id,
            content_digest: operation.content_digest,
            observed_at: operation.observed_at,
            valid_time: operation.valid_time,
            recorded_at: operation.recorded_at,
            idempotency_key: operation.idempotency_key,
            receipt_id: operation.receipt_id,
        })
        .collect();

    let _ = log_audit(
        &state,
        &AuditContext {
            device: Some(context.device),
            actor: context.actor,
        },
        "/v1/operations",
        "GET",
        "ok",
        Some("operations listed"),
    )
    .await;

    (StatusCode::OK, Json(response)).into_response()
}

#[cfg(feature = "server")]
async fn get_operation_handler(
    headers: HeaderMap,
    State(state): State<ServerState>,
    Path(operation_id): Path<String>,
) -> Response {
    let operation_id = match parse_id(&operation_id) {
        Some(value) => value,
        None => {
            return error_response(&MnemesError::InvalidAsOf(
                "invalid operation id".to_string(),
            ))
        }
    };

    let context = match authorize(&state, &headers, None).await {
        Ok(context) => context,
        Err(error) => return error_response(&error),
    };

    let envelope = match state.store.get_operation(&operation_id).await {
        Ok(Some(value)) => value,
        Ok(None) => {
            return error_response(&MnemesError::InvalidAsOf("operation not found".to_string()))
        }
        Err(error) => return error_response(&error),
    };

    let response = OperationInfo {
        operation_id: envelope.operation_id.to_string(),
        requesting_device_id: envelope.requesting_device_id.to_string(),
        requesting_actor_id: envelope.requesting_actor_id.to_string(),
        operation_kind: envelope.operation_kind.as_str().to_string(),
        target_kind: envelope.target_kind,
        target_id: envelope.target_id,
        content_digest: envelope.content_digest,
        observed_at: envelope.observed_at,
        valid_time: envelope.valid_time,
        recorded_at: envelope.recorded_at,
        idempotency_key: envelope.idempotency_key,
        receipt_id: envelope.receipt_id,
    };

    let _ = log_audit(
        &state,
        &AuditContext {
            device: Some(context.device),
            actor: context.actor,
        },
        "/v1/operations/{operation_id}",
        "GET",
        "ok",
        Some("operation read"),
    )
    .await;

    (StatusCode::OK, Json(response)).into_response()
}

#[cfg(feature = "server")]
fn parse_id(raw: &str) -> Option<OperationId> {
    OperationId::parse(raw).ok()
}

#[cfg(feature = "server")]
async fn get_receipt_handler(
    headers: HeaderMap,
    State(state): State<ServerState>,
    Path(receipt_id): Path<String>,
) -> Response {
    let context = match authorize(&state, &headers, None).await {
        Ok(context) => context,
        Err(error) => return error_response(&error),
    };

    let envelope = match state.store.get_operation_by_receipt(&receipt_id).await {
        Ok(Some(envelope)) => envelope,
        Ok(None) => {
            return error_response(&MnemesError::InvalidAsOf("receipt not found".to_string()))
        }
        Err(error) => return error_response(&error),
    };

    let response = serde_json::json!({
        "receipt_id": receipt_id,
        "operation_id": envelope.operation_id.to_string(),
        "idempotency_key": envelope.idempotency_key,
        "requesting_device_id": envelope.requesting_device_id.to_string(),
        "requesting_actor_id": envelope.requesting_actor_id.to_string(),
        "recording_device_id": envelope.recording_device_id.to_string(),
        "recording_server_id": envelope.recording_server_id.to_string(),
        "operation_kind": envelope.operation_kind.as_str(),
        "target_kind": envelope.target_kind,
        "target_id": envelope.target_id,
        "content_digest": envelope.content_digest,
        "observed_at": envelope.observed_at,
        "valid_time": envelope.valid_time,
        "recorded_at": envelope.recorded_at,
        "receipt_id": envelope.receipt_id,
    });

    let _ = log_audit(
        &state,
        &AuditContext {
            device: Some(context.device),
            actor: context.actor,
        },
        "/v1/receipts/{receipt_id}",
        "GET",
        "ok",
        Some("receipt read"),
    )
    .await;

    (StatusCode::OK, Json(response)).into_response()
}

#[cfg(feature = "server")]
async fn list_audit_events_handler(
    headers: HeaderMap,
    State(state): State<ServerState>,
    Query(query): Query<AuditQuery>,
) -> Response {
    let context = match authorize(&state, &headers, None).await {
        Ok(context) => context,
        Err(error) => return error_response(&error),
    };

    let events = match state
        .store
        .list_audit_events(query.limit.unwrap_or(100))
        .await
    {
        Ok(values) => values,
        Err(error) => return error_response(&error),
    };

    let _ = log_audit(
        &state,
        &AuditContext {
            device: Some(context.device),
            actor: context.actor,
        },
        "/v1/audit/events",
        "GET",
        "ok",
        Some("audit events listed"),
    )
    .await;

    (StatusCode::OK, Json(events)).into_response()
}

#[cfg(feature = "server")]
fn tools_for_actor(actor: Option<&Actor>) -> Vec<McpTool> {
    let mut tools = vec![
        McpTool {
            name: "sm_get_device".to_string(),
            description: "Return a device by UUID".to_string(),
            input_schema: json!({
                "type":"object",
                "properties": {
                    "device_id": {"type":"string","format":"uuid"}
                },
                "required": ["device_id"],
                "additionalProperties": false
            }),
        },
        McpTool {
            name: "sm_list_devices".to_string(),
            description: "List registered devices".to_string(),
            input_schema: json!({"type":"object","additionalProperties": false}),
        },
        McpTool {
            name: "sm_get_actor".to_string(),
            description: "Return an actor by UUID".to_string(),
            input_schema: json!({
                "type":"object",
                "properties": {
                    "actor_id": {"type":"string","format":"uuid"}
                },
                "required": ["actor_id"],
                "additionalProperties": false
            }),
        },
        McpTool {
            name: "sm_get_operation".to_string(),
            description: "Return a persisted operation envelope by UUID".to_string(),
            input_schema: json!({
                "type":"object",
                "properties": {
                    "operation_id": {"type":"string","format":"uuid"}
                },
                "required": ["operation_id"],
                "additionalProperties": false
            }),
        },
        McpTool {
            name: "sm_search_witnessed".to_string(),
            description: "Search witnessed semantic-memory entries".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string" },
                    "namespaces": {
                        "type": "array",
                        "items": {"type":"string"}
                    },
                    "source_types": {
                        "type": "array",
                        "items": {"type":"string"}
                    },
                    "limit": {"type":"integer","minimum":1}
                },
                "required": ["query"],
                "additionalProperties": false
            }),
        },
        McpTool {
            name: "sm_stats".to_string(),
            description: "Read registry and semantic memory statistics".to_string(),
            input_schema: json!({"type":"object","additionalProperties": false}),
        },
        McpTool {
            name: "sm_health".to_string(),
            description: "Read service health".to_string(),
            input_schema: json!({"type":"object","additionalProperties": false}),
        },
        McpTool {
            name: "sm_heartbeat".to_string(),
            description: "Heartbeat for authenticated device".to_string(),
            input_schema: json!({
                "type":"object",
                "properties": {
                    "device_id": {"type":"string","format":"uuid"}
                },
                "additionalProperties": false
            }),
        },
    ];

    if actor.is_some_and(|value| value.tool_profile == ToolProfile::Operator) {
        tools.push(McpTool {
            name: "sm_register_device".to_string(),
            description: "Register a new device".to_string(),
            input_schema: json!({
                "type":"object",
                "properties": {
                    "label": {"type":"string"},
                    "platform": {"type":"string"},
                    "hostname": {"type":"string"},
                },
                "required": ["label", "platform", "hostname"],
                "additionalProperties": false
            }),
        });
        tools.push(McpTool {
            name: "sm_revoke_device".to_string(),
            description: "Revoke an existing device".to_string(),
            input_schema: json!({
                "type":"object",
                "properties": {
                    "device_id": {"type":"string","format":"uuid"}
                },
                "required": ["device_id"],
                "additionalProperties": false
            }),
        });
        tools.push(McpTool {
            name: "sm_rotate_device_key".to_string(),
            description: "Rotate a device credential".to_string(),
            input_schema: json!({
                "type":"object",
                "properties": {
                    "device_id": {"type":"string","format":"uuid"}
                },
                "required": ["device_id"],
                "additionalProperties": false
            }),
        });
        tools.push(McpTool {
            name: "sm_register_actor".to_string(),
            description: "Register an actor".to_string(),
            input_schema: json!({
                "type":"object",
                "properties": {
                    "device_id": {"type":"string","format":"uuid"},
                    "actor_kind": {"type":"string"},
                    "provider_model": {"type":"string"},
                    "tool_profile": {"type":"string"}
                },
                "required": ["device_id", "actor_kind"],
                "additionalProperties": false
            }),
        });
        tools.push(McpTool {
            name: "sm_submit_operation".to_string(),
            description: "Submit an operation envelope".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "idempotency_key": {"type":"string"},
                    "requesting_device_id": {"type":"string","format":"uuid"},
                    "requesting_actor_id": {"type":"string","format":"uuid"},
                    "operation_kind": {"type":"string"},
                    "target_kind": {"type":"string"},
                    "target_id": {"type":"string"},
                    "content_digest": {"type":"string"},
                    "recording_device_id": {"type":"string","format":"uuid"},
                    "recording_server_id": {"type":"string","format":"uuid"},
                    "observed_at": {"type":"string"},
                    "valid_time": {"type":"string"}
                },
                "required": [
                    "idempotency_key",
                    "requesting_device_id",
                    "requesting_actor_id",
                    "operation_kind",
                    "target_kind",
                    "target_id",
                    "content_digest"
                ],
                "additionalProperties": false
            }),
        });
        tools.push(McpTool {
            name: "sm_verify_integrity".to_string(),
            description: "Run integrity checks".to_string(),
            input_schema: json!({"type":"object","additionalProperties": false}),
        });
    }

    tools
}

#[cfg(feature = "server")]
fn read_tool(name: &str) -> bool {
    matches!(
        name,
        "sm_get_device"
            | "sm_list_devices"
            | "sm_get_actor"
            | "sm_get_operation"
            | "sm_search_witnessed"
            | "sm_stats"
            | "sm_health"
            | "sm_heartbeat"
    )
}

#[cfg(feature = "server")]
fn operator_tool(name: &str) -> bool {
    matches!(
        name,
        "sm_register_device"
            | "sm_revoke_device"
            | "sm_rotate_device_key"
            | "sm_register_actor"
            | "sm_submit_operation"
            | "sm_verify_integrity"
    )
}

#[cfg(feature = "server")]
fn hidden_tool(name: &str) -> bool {
    matches!(
        name,
        "devices.list" | "actors.list" | "operations.list" | "health.check" | "operations.submit"
    )
}

#[cfg(feature = "server")]
fn parse_tool_call(params: Option<&Value>) -> Result<(Option<String>, String, Value), MnemesError> {
    let params_obj = params
        .and_then(Value::as_object)
        .ok_or_else(|| MnemesError::InvalidAsOf("params must be object".to_string()))?;

    let name = params_obj
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| MnemesError::InvalidAsOf("missing tool name".to_string()))?
        .to_string();
    let actor_id = params_obj
        .get("actor_id")
        .and_then(Value::as_str)
        .map(str::to_string);

    let mut arguments = serde_json::Map::new();
    if let Some(raw) = params_obj.get("arguments").and_then(Value::as_object) {
        for (key, value) in raw {
            arguments.insert(key.clone(), value.clone());
        }
    }
    for (key, value) in params_obj {
        if !matches!(key.as_str(), "arguments" | "name" | "actor_id") {
            arguments.insert(key.clone(), value.clone());
        }
    }

    Ok((actor_id, name, Value::Object(arguments)))
}

#[cfg(feature = "server")]
fn parse_actor_id_param(params: Option<&Value>) -> Result<Option<ActorId>, MnemesError> {
    let Some(params) = params.and_then(Value::as_object) else {
        return Ok(None);
    };

    if let Some(raw) = params.get("actor_id").and_then(Value::as_str) {
        Ok(Some(parse_actor_id(raw)?))
    } else {
        Ok(None)
    }
}

#[cfg(feature = "server")]
fn parse_operation_source_types(raw: &[String]) -> Result<Vec<SearchSourceType>, MnemesError> {
    raw.iter()
        .map(|value| match value.to_lowercase().as_str() {
            "facts" | "fact" => Ok(SearchSourceType::Facts),
            "chunks" | "chunk" => Ok(SearchSourceType::Chunks),
            "messages" | "message" => Ok(SearchSourceType::Messages),
            "episodes" | "episode" => Ok(SearchSourceType::Episodes),
            _ => Err(MnemesError::InvalidAsOf(format!(
                "unsupported source_type {value}"
            ))),
        })
        .collect()
}

#[cfg(feature = "server")]
fn result_from_operation_source(
    source: SearchSource,
    content: String,
    score: f64,
) -> WitnessedSearchItem {
    let source_name = source.source_kind().to_string();

    match source {
        SearchSource::Fact { fact_id, namespace } => WitnessedSearchItem {
            item_id: format!("fact:{fact_id}"),
            namespace,
            content,
            score,
            source: source_name,
            proof: "unverified".to_string(),
            source_provenance: "unknown".to_string(),
        },
        SearchSource::Chunk {
            chunk_id,
            document_id,
            ..
        } => WitnessedSearchItem {
            item_id: format!("chunk:{chunk_id}"),
            namespace: document_id,
            content,
            score,
            source: source_name,
            proof: "unverified".to_string(),
            source_provenance: "unknown".to_string(),
        },
        SearchSource::Message { message_id, .. } => WitnessedSearchItem {
            item_id: format!("message:{message_id}"),
            namespace: message_id.to_string(),
            content,
            score,
            source: source_name,
            proof: "unverified".to_string(),
            source_provenance: "unknown".to_string(),
        },
        SearchSource::Episode { episode_id, .. } => WitnessedSearchItem {
            item_id: format!("episode:{episode_id}"),
            namespace: episode_id,
            content,
            score,
            source: source_name,
            proof: "unverified".to_string(),
            source_provenance: "unknown".to_string(),
        },
        SearchSource::Projection {
            projection_id,
            scope_key,
            ..
        } => WitnessedSearchItem {
            item_id: format!("projection:{projection_id}"),
            namespace: serde_json::to_string(&scope_key).unwrap_or_else(|_| "unknown".to_string()),
            content,
            score,
            source: source_name,
            proof: "unverified".to_string(),
            source_provenance: "unknown".to_string(),
        },
    }
}

#[cfg(feature = "server")]
async fn run_witnessed_search(
    state: &ServerState,
    request: McpSearchRequest,
) -> Result<WitnessedSearchResponse, MnemesError> {
    let namespaces = request
        .namespaces
        .as_ref()
        .map(|value| value.iter().map(String::as_str).collect::<Vec<_>>());
    let source_types = request
        .source_types
        .as_ref()
        .map(|values| parse_operation_source_types(values))
        .transpose()?;

    // Check if sharded mode is active
    let has_shards = state.store.has_shards().await?;

    if has_shards {
        // Routed path: delegate to MnemesStore::routed_search which handles
        // shard selection, parallel search, merge, conflict scanning, and
        // routing receipt persistence.
        let routing_request = crate::shards::RoutingSearchRequest {
            query: request.query.clone(),
            top_k: request.limit.unwrap_or(10),
            namespaces: request.namespaces.clone(),
            source_types: source_types.as_ref().map(|st| st.to_vec()),
            shard_budget: None,
            exhaustive: false,
        };

        // Use the first registered device as requester.
        // TODO: bind to the authenticated device from authorize() context.
        let devices = state.store.list_devices().await?;
        let requester = devices
            .first()
            .map(|d| d.device_id.clone())
            .unwrap_or_else(DeviceId::new);

        let routed = state
            .store
            .routed_search(&requester, routing_request)
            .await?;

        let results = routed
            .results
            .into_iter()
            .map(|r| {
                result_from_operation_source(r.result.source, r.result.content, r.result.score)
            })
            .collect::<Vec<_>>();

        return Ok(WitnessedSearchResponse {
            results,
            receipt: None,
            receipt_stored: false,
        });
    }

    // Legacy fallback: no shards registered (test/single-device mode)
    let mut context = SearchContext::default_now();
    context.receipt_mode = ReceiptMode::ReturnReceipt;
    context.exactness_profile = ExactnessProfile::PreferExact;

    let search_response = state
        .store
        .memory()
        .search_with_context(
            &request.query,
            request.limit,
            namespaces.as_deref(),
            source_types.as_deref(),
            context,
        )
        .await?;

    let results = search_response
        .results
        .into_iter()
        .map(|value| result_from_operation_source(value.source, value.content, value.score))
        .collect::<Vec<_>>();
    let receipt = search_response.receipt.clone();
    let receipt_stored = if let Some(receipt) = &receipt {
        state
            .store
            .memory()
            .get_search_receipt(&receipt.receipt_id)
            .await
            .ok()
            .flatten()
            .is_some()
    } else {
        false
    };

    Ok(WitnessedSearchResponse {
        results,
        receipt,
        receipt_stored,
    })
}

#[cfg(feature = "server")]
async fn search_witnessed_handler(
    headers: HeaderMap,
    State(state): State<ServerState>,
    Json(payload): Json<WitnessedSearchRequest>,
) -> Response {
    let _ = match authorize(&state, &headers, None).await {
        Ok(context) => context,
        Err(error) => return error_response(&error),
    };

    let request = McpSearchRequest {
        query: payload.query,
        namespaces: payload.namespaces,
        source_types: payload.source_types,
        limit: payload.limit,
    };
    match run_witnessed_search(&state, request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(error) => error_response(&error),
    }
}

#[cfg(feature = "server")]
async fn sync_endpoint(
    headers: HeaderMap,
    State(state): State<ServerState>,
    Json(request): Json<crate::sync_handler::SyncRequest>,
) -> Response {
    let _ = match authorize(&state, &headers, None).await {
        Ok(ctx) => ctx,
        Err(error) => return error_response(&error),
    };
    // Replicas live under base_dir/replicas/{store_id}.db
    let replica_base = state.store.base_dir().join("replicas");
    std::fs::create_dir_all(&replica_base).ok();
    // Dispatch: replay raw SQL payload against the replica connection.
    // The payload is a hex-encoded raw journal entry (SQL statements to
    // replay the semantic-memory mutation against the replica DB).
    let dispatch = |conn: &rusqlite::Connection, _kind: &str, payload: &[u8]| {
        // Decode payload as UTF-8 SQL batch and execute it
        let sql = std::str::from_utf8(payload)
            .map_err(|e| MnemesError::Replication(format!("payload not valid UTF-8 SQL: {e}")))?;
        conn.execute_batch(sql)
            .map_err(|e| MnemesError::Replication(format!("replay SQL batch failed: {e}")))
    };
    match crate::sync_handler::process_sync_request(
        request,
        &state.trusted_keys,
        &replica_base,
        &dispatch,
    ) {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(error) => error_response(&error),
    }
}

async fn build_health_payload(state: &ServerState) -> HealthResponse {
    let mut embedding = HashMap::new();
    embedding.insert(
        "dimensions",
        state.store.memory_config().embedding.dimensions.to_string(),
    );
    embedding.insert("model", state.store.memory_config().embedding.model.clone());
    let ready = matches!(state.store.quick_check().await, Ok(ref value) if value == "ok");

    HealthResponse {
        service_id: state.server_id.clone(),
        schema_version: SCHEMA_VERSION.to_string(),
        server_time: Utc::now().to_rfc3339(),
        ready,
        embedding,
    }
}

#[cfg(feature = "server")]
async fn mcp_handler(
    headers: HeaderMap,
    State(state): State<ServerState>,
    Json(request): Json<JsonRpcRequest>,
) -> Response {
    let rpc_id = request.id.clone();
    if let Err(error) = authorize(&state, &headers, None).await {
        return unauthorized_rpc(error, rpc_id);
    }
    let mut rpc_response = JsonRpcResponse {
        jsonrpc: "2.0",
        id: rpc_id.clone(),
        result: None,
        error: None,
    };

    let result = match request.method.as_str() {
        "initialize" => {
            let tools = tools_for_actor(None);
            Some(serde_json::json!({
                "name": "mnemes",
                "version": SCHEMA_VERSION,
                "tools": tools
            }))
        }
        "tools/list" => {
            let actor_id = match parse_actor_id_param(request.params.as_ref()) {
                Ok(Some(actor_id)) => actor_id,
                Ok(None) => {
                    return unauthorized_rpc(
                        MnemesError::InvalidAsOf("actor_id required".to_string()),
                        rpc_id,
                    );
                }
                Err(error) => return unauthorized_rpc(error, rpc_id),
            };
            let context = match authorize(&state, &headers, Some(actor_id)).await {
                Ok(context) => context,
                Err(error) => return unauthorized_rpc(error, rpc_id),
            };
            Some(serde_json::json!({
                "tools": tools_for_actor(context.actor.as_ref())
            }))
        }
        "tools/call" => {
            let tool_result: Result<Value, MnemesError> = (async {
                let (actor_id_raw, name, args) = parse_tool_call(request.params.as_ref())?;
                let actor_id = actor_id_raw
                    .ok_or_else(|| MnemesError::InvalidAsOf("actor_id required".to_string()))?;
                let actor_id = parse_actor_id(&actor_id)?;
                let context = authorize(&state, &headers, Some(actor_id)).await?;

                if hidden_tool(&name) {
                    return Err(MnemesError::AuthorizationDenied(
                        "tool is not available".to_string(),
                    ));
                }

                let is_read_tool = read_tool(&name);
                let is_operator_tool = operator_tool(&name);
                if is_operator_tool
                    && context
                        .actor
                        .as_ref()
                        .map_or(true, |value| value.tool_profile != ToolProfile::Operator)
                {
                    return Err(MnemesError::AuthorizationDenied(
                        "operator profile required".to_string(),
                    ));
                }

                if !is_read_tool && !is_operator_tool {
                    return Err(MnemesError::InvalidAsOf("unsupported tool".to_string()));
                }

                let response = match name.as_str() {
                    "sm_get_device" => {
                        let tool_request: McpDeviceRequest = serde_json::from_value(args.clone())
                            .map_err(|_| {
                            MnemesError::InvalidAsOf("invalid sm_get_device args".to_string())
                        })?;
                        let device_id = parse_device_id(&tool_request.device_id)?;
                        if device_id != context.device.device_id
                            && context
                                .actor
                                .as_ref()
                                .map_or(true, |actor| actor.tool_profile != ToolProfile::Operator)
                        {
                            return Err(MnemesError::AuthorizationDenied(
                                "device mismatch".to_string(),
                            ));
                        }
                        let device =
                            state.store.get_device(&device_id).await?.ok_or_else(|| {
                                MnemesError::DeviceNotFound(tool_request.device_id.clone())
                            })?;
                        serde_json::to_value(DeviceInfo {
                            device_id: device.device_id.to_string(),
                            label: device.label,
                            platform: device.platform,
                            hostname: device.hostname,
                            status: device.status.as_str().to_string(),
                            first_seen_at: device.first_seen_at,
                            last_seen_at: device.last_seen_at,
                        })
                        .map_err(|error| {
                            MnemesError::InvalidAsOf(format!("failed to serialize result: {error}"))
                        })?
                    }
                    "sm_list_devices" => {
                        let devices = state.store.list_devices().await?;
                        serde_json::to_value(
                            devices
                                .into_iter()
                                .map(|device| DeviceInfo {
                                    device_id: device.device_id.to_string(),
                                    label: device.label,
                                    platform: device.platform,
                                    hostname: device.hostname,
                                    status: device.status.as_str().to_string(),
                                    first_seen_at: device.first_seen_at,
                                    last_seen_at: device.last_seen_at,
                                })
                                .collect::<Vec<_>>(),
                        )
                        .map_err(|error| {
                            MnemesError::InvalidAsOf(format!("failed to serialize result: {error}"))
                        })?
                    }
                    "sm_get_actor" => {
                        let tool_request: McpActorRequest = serde_json::from_value(args.clone())
                            .map_err(|_| {
                                MnemesError::InvalidAsOf("invalid sm_get_actor args".to_string())
                            })?;
                        let actor_id = parse_actor_id(&tool_request.actor_id)?;
                        let actor = state.store.get_actor(&actor_id).await?.ok_or_else(|| {
                            MnemesError::ActorNotFound(tool_request.actor_id.clone())
                        })?;
                        if actor.device_id != context.device.device_id {
                            return Err(MnemesError::AuthorizationDenied(
                                "actor does not belong to device".to_string(),
                            ));
                        }
                        serde_json::to_value(ActorInfo {
                            actor_id: actor.actor_id.to_string(),
                            device_id: actor.device_id.to_string(),
                            actor_kind: actor.actor_kind.as_str().to_string(),
                            tool_profile: actor.tool_profile.as_str().to_string(),
                            provider_model: actor.provider_model,
                            recorded_at: actor.recorded_at,
                        })
                        .map_err(|error| {
                            MnemesError::InvalidAsOf(format!("failed to serialize result: {error}"))
                        })?
                    }
                    "sm_get_operation" => {
                        let tool_request: McpOperationRequest =
                            serde_json::from_value(args.clone()).map_err(|_| {
                                MnemesError::InvalidAsOf(
                                    "invalid sm_get_operation args".to_string(),
                                )
                            })?;
                        let operation_id =
                            parse_id(&tool_request.operation_id).ok_or_else(|| {
                                MnemesError::InvalidAsOf("invalid operation id".to_string())
                            })?;
                        let envelope =
                            state
                                .store
                                .get_operation(&operation_id)
                                .await?
                                .ok_or_else(|| {
                                    MnemesError::InvalidAsOf("operation not found".to_string())
                                })?;
                        if envelope.requesting_device_id != context.device.device_id {
                            return Err(MnemesError::AuthorizationDenied(
                                "operation does not belong to device".to_string(),
                            ));
                        }
                        if context
                            .actor
                            .as_ref()
                            .map_or(true, |value| value.actor_id != envelope.requesting_actor_id)
                        {
                            return Err(MnemesError::AuthorizationDenied(
                                "actor mismatch".to_string(),
                            ));
                        }
                        serde_json::to_value(OperationInfo {
                            operation_id: envelope.operation_id.to_string(),
                            requesting_device_id: envelope.requesting_device_id.to_string(),
                            requesting_actor_id: envelope.requesting_actor_id.to_string(),
                            operation_kind: envelope.operation_kind.as_str().to_string(),
                            target_kind: envelope.target_kind,
                            target_id: envelope.target_id,
                            content_digest: envelope.content_digest,
                            observed_at: envelope.observed_at,
                            valid_time: envelope.valid_time,
                            recorded_at: envelope.recorded_at,
                            idempotency_key: envelope.idempotency_key,
                            receipt_id: envelope.receipt_id,
                        })
                        .map_err(|error| {
                            MnemesError::InvalidAsOf(format!("failed to serialize result: {error}"))
                        })?
                    }
                    "sm_search_witnessed" => {
                        let tool_request: McpSearchRequest = serde_json::from_value(args.clone())
                            .map_err(|_| {
                            MnemesError::InvalidAsOf("invalid sm_search_witnessed args".to_string())
                        })?;
                        serde_json::to_value(run_witnessed_search(&state, tool_request).await?)
                            .map_err(|error| {
                                MnemesError::InvalidAsOf(format!(
                                    "failed to serialize result: {error}"
                                ))
                            })?
                    }
                    "sm_stats" => {
                        let pooled = PoolStats {
                            devices: state.store.count_devices().await?,
                            actors: state.store.count_actors().await?,
                            operations: state.store.count_operations().await?,
                        };
                        let semantic = state.store.memory().stats().await?;
                        serde_json::to_value(McpStatsResponse {
                            pooled,
                            semantic: serde_json::to_value(semantic).map_err(|error| {
                                MnemesError::InvalidAsOf(format!(
                                    "failed to serialize stats: {error}"
                                ))
                            })?,
                        })
                        .map_err(|error| {
                            MnemesError::InvalidAsOf(format!("failed to serialize result: {error}"))
                        })?
                    }
                    "sm_health" => serde_json::to_value(build_health_payload(&state).await)
                        .map_err(|error| {
                            MnemesError::InvalidAsOf(format!(
                                "failed to serialize health payload: {error}"
                            ))
                        })?,
                    "sm_heartbeat" => {
                        let device_id = match args.as_object() {
                            Some(values) if values.get("device_id").is_none() => {
                                context.device.device_id.clone()
                            }
                            Some(values) => {
                                let tool_request: McpDeviceRequest = serde_json::from_value(
                                    Value::Object(values.clone()),
                                )
                                .map_err(|_| {
                                    MnemesError::InvalidAsOf(
                                        "invalid sm_heartbeat args".to_string(),
                                    )
                                })?;
                                let device_id = parse_device_id(&tool_request.device_id)?;
                                if device_id != context.device.device_id {
                                    return Err(MnemesError::AuthorizationDenied(
                                        "device mismatch".to_string(),
                                    ));
                                }
                                device_id
                            }
                            None => context.device.device_id.clone(),
                        };
                        state.store.heartbeat_device(&device_id).await?;
                        json!({"ok": true, "device_id": device_id.to_string()})
                    }
                    "sm_register_device" => {
                        let tool_request: RegisterDeviceRequest = serde_json::from_value(args)
                            .map_err(|_| {
                                MnemesError::InvalidAsOf(
                                    "invalid sm_register_device args".to_string(),
                                )
                            })?;
                        let new_device = Device::new(
                            DeviceId::new(),
                            tool_request.label,
                            tool_request.platform,
                            tool_request.hostname,
                        );
                        let (device_id, credential) = state
                            .store
                            .register_device_with_generated_credential(new_device)
                            .await?;
                        let device =
                            state.store.get_device(&device_id).await?.ok_or_else(|| {
                                MnemesError::DeviceNotFound(device_id.to_string())
                            })?;
                        serde_json::to_value(RegisterDeviceResponse {
                            device_id: device.device_id.to_string(),
                            credential,
                            first_seen_at: device.first_seen_at,
                            last_seen_at: device.last_seen_at,
                            status: device.status.as_str().to_string(),
                        })
                        .map_err(|error| {
                            MnemesError::InvalidAsOf(format!("failed to serialize result: {error}"))
                        })?
                    }
                    "sm_revoke_device" => {
                        let tool_request: McpDeviceRequest = serde_json::from_value(args.clone())
                            .map_err(|_| {
                            MnemesError::InvalidAsOf("invalid sm_revoke_device args".to_string())
                        })?;
                        let device_id = parse_device_id(&tool_request.device_id)?;
                        state.store.revoke_device(&device_id).await?;
                        json!({"status":"revoked","device_id":device_id.to_string()})
                    }
                    "sm_rotate_device_key" => {
                        let tool_request: McpDeviceRequest = serde_json::from_value(args.clone())
                            .map_err(|_| {
                            MnemesError::InvalidAsOf(
                                "invalid sm_rotate_device_key args".to_string(),
                            )
                        })?;
                        let device_id = parse_device_id(&tool_request.device_id)?;
                        let credential = state.store.rotate_device_credential(&device_id).await?;
                        json!({"device_id":device_id.to_string(),"credential":credential})
                    }
                    "sm_register_actor" => {
                        let tool_request: McpRegisterActorRequest =
                            serde_json::from_value(args.clone()).map_err(|_| {
                                MnemesError::InvalidAsOf(
                                    "invalid sm_register_actor args".to_string(),
                                )
                            })?;
                        let device_id = parse_device_id(&tool_request.device_id)?;
                        let actor = crate::types::Actor {
                            actor_id: crate::types::ActorId::new(),
                            device_id,
                            actor_kind: crate::types::ActorKind::parse(tool_request.actor_kind),
                            tool_profile: tool_request
                                .tool_profile
                                .as_deref()
                                .and_then(crate::types::ToolProfile::parse)
                                .unwrap_or_default(),
                            provider_model: tool_request.provider_model,
                            recorded_at: String::new(),
                        };
                        let actor_id = state.store.register_actor(actor).await?;
                        json!({"actor_id": actor_id.to_string()})
                    }
                    "sm_submit_operation" => {
                        let tool_request: OperationSubmitRequest =
                            serde_json::from_value(args.clone()).map_err(|_| {
                                MnemesError::InvalidAsOf(
                                    "invalid sm_submit_operation args".to_string(),
                                )
                            })?;
                        let requesting_device_id =
                            parse_device_id(&tool_request.requesting_device_id)?;
                        let requesting_actor_id =
                            parse_actor_id(&tool_request.requesting_actor_id)?;
                        if requesting_device_id != context.device.device_id {
                            return Err(MnemesError::AuthorizationDenied(
                                "device mismatch".to_string(),
                            ));
                        }
                        if context
                            .actor
                            .as_ref()
                            .map_or(true, |value| value.actor_id != requesting_actor_id)
                        {
                            return Err(MnemesError::AuthorizationDenied(
                                "actor mismatch".to_string(),
                            ));
                        }
                        let recording_device_id = match tool_request.recording_device_id {
                            Some(raw) => parse_device_id(&raw)?,
                            None => context.device.device_id.clone(),
                        };
                        let recording_server_id = match tool_request.recording_server_id {
                            Some(raw) => parse_device_id(&raw)?,
                            None => context.device.device_id.clone(),
                        };
                        let operation = OperationEnvelope {
                            operation_id: OperationId::new(),
                            idempotency_key: tool_request.idempotency_key.clone(),
                            requesting_device_id,
                            requesting_actor_id,
                            recording_device_id,
                            recording_server_id,
                            operation_kind: parse_operation_kind(&tool_request.operation_kind)?,
                            target_kind: tool_request.target_kind,
                            target_id: tool_request.target_id,
                            content_digest: tool_request.content_digest,
                            observed_at: tool_request.observed_at,
                            valid_time: tool_request.valid_time,
                            recorded_at: String::new(),
                            receipt_id: None,
                        };
                        let receipt_id = state.store.submit_operation(operation).await?;
                        let envelope = state
                            .store
                            .get_operation_by_idempotency_key(&tool_request.idempotency_key)
                            .await?
                            .ok_or_else(|| {
                                MnemesError::InvalidAsOf("operation not found".to_string())
                            })?;
                        serde_json::to_value(OperationEnvelopeResponse {
                            operation_id: envelope.operation_id.to_string(),
                            receipt_id,
                            idempotency_key: envelope.idempotency_key,
                            recorded_at: envelope.recorded_at,
                        })
                        .map_err(|error| {
                            MnemesError::InvalidAsOf(format!("failed to serialize result: {error}"))
                        })?
                    }
                    "sm_verify_integrity" => {
                        let (pooled_status, pooled_detail) = match state.store.quick_check().await {
                            Ok(value) if value == "ok" => ("ok", value),
                            Ok(value) => ("degraded", value),
                            Err(error) => ("failed", error.to_string()),
                        };

                        let (semantic_status, semantic_detail) = match state
                            .store
                            .memory()
                            .verify_integrity(VerifyMode::Quick)
                            .await
                        {
                            Ok(report) => {
                                if report.ok {
                                    ("ok", format!("{report:?}"))
                                } else {
                                    ("degraded", format!("issues: {:?}", report.issues))
                                }
                            }
                            Err(error) => ("failed", format!("error: {error}")),
                        };
                        serde_json::to_value(VerifyIntegrityResponse {
                            pooled_sqlite: IntegrityCheckReport {
                                status: pooled_status,
                                detail: pooled_detail,
                            },
                            semantic_memory: IntegrityCheckReport {
                                status: semantic_status,
                                detail: semantic_detail,
                            },
                        })
                        .map_err(|error| {
                            MnemesError::InvalidAsOf(format!("failed to serialize result: {error}"))
                        })?
                    }
                    _ => return Err(MnemesError::InvalidAsOf("unsupported tool".to_string())),
                };
                Ok(response)
            })
            .await;

            match tool_result {
                Ok(response) => Some(response),
                Err(error) => return unauthorized_rpc(error, rpc_id),
            }
        }
        _ => None,
    };

    if let Some(result) = result {
        rpc_response.result = Some(result);
        (StatusCode::OK, Json(rpc_response)).into_response()
    } else {
        rpc_response.error = Some(JsonRpcError {
            code: -32601,
            message: "unsupported method".to_string(),
        });
        (StatusCode::BAD_REQUEST, Json(rpc_response)).into_response()
    }
}

#[cfg(feature = "server")]
pub fn build_memory_store(base_dir: &str) -> Result<MnemesStore, MnemesError> {
    let db_dir = PathBuf::from(base_dir);
    let mut memory_config = MemoryConfig {
        base_dir: db_dir.join("memory"),
        ..Default::default()
    };
    if let Ok(url) = std::env::var("MNEMES_OLLAMA_URL") {
        memory_config.embedding.ollama_url = url;
    }
    if let Ok(model) = std::env::var("MNEMES_EMBEDDING_MODEL") {
        memory_config.embedding.model = model;
    }
    if let Ok(dimensions) = std::env::var("MNEMES_EMBEDDING_DIMENSIONS") {
        memory_config.embedding.dimensions = dimensions.parse().map_err(|_| {
            MnemesError::InvalidShardCatalog(
                "MNEMES_EMBEDDING_DIMENSIONS must be a positive integer".to_string(),
            )
        })?;
    }
    MnemesStore::open(db_dir, memory_config)
}

#[cfg(feature = "server")]
pub fn build_default_store() -> Result<MnemesStore, MnemesError> {
    build_memory_store("./data/mnemes")
}
