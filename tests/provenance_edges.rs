use chrono::{DateTime, Utc};
use mnemes::{
    Actor, ActorId, ActorKind, AsOf, Device, DeviceId, MemoryItemRef, OperationEnvelope,
    OperationId, OperationKind, MnemesStore, ProvenanceEdgeRequest, ProvenanceEdgeType,
    ProvenanceQuery,
};
use semantic_memory::{EmbeddingConfig, MemoryConfig, MockEmbedder};
use tempfile::TempDir;

fn fixed_time(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .unwrap()
        .with_timezone(&Utc)
}

async fn open_store() -> (MnemesStore, TempDir) {
    let dir = TempDir::new().unwrap();
    let config = MemoryConfig {
        base_dir: dir.path().to_path_buf(),
        embedding: EmbeddingConfig {
            dimensions: 768,
            ..Default::default()
        },
        ..Default::default()
    };

    let store = MnemesStore::open_with_embedder(
        dir.path().to_path_buf(),
        config,
        Box::new(MockEmbedder::new(768)),
    )
    .unwrap();

    (store, dir)
}

async fn setup_subjects(store: &MnemesStore) -> (DeviceId, ActorId) {
    let device_id = DeviceId::new();
    store
        .register_device(Device::new(
            device_id.clone(),
            "integration-device",
            "linux",
            "integration-host",
        ))
        .await
        .unwrap();

    let actor_id = ActorId::new();
    store
        .register_actor(Actor::new(
            actor_id.clone(),
            device_id.clone(),
            ActorKind::Codex,
        ))
        .await
        .unwrap();

    (device_id, actor_id)
}

async fn submit_operation(
    store: &MnemesStore,
    device_id: DeviceId,
    actor_id: ActorId,
    operation_kind: OperationKind,
    target_kind: &str,
    target_id: &str,
    content_digest: &str,
) -> OperationId {
    let operation_id = OperationId::new();
    let envelope = OperationEnvelope {
        operation_id: operation_id.clone(),
        idempotency_key: operation_id.as_str().to_string(),
        requesting_device_id: device_id.clone(),
        requesting_actor_id: actor_id.clone(),
        recording_device_id: device_id.clone(),
        recording_server_id: device_id,
        operation_kind,
        target_kind: target_kind.to_string(),
        target_id: target_id.to_string(),
        content_digest: content_digest.to_string(),
        observed_at: None,
        valid_time: None,
        recorded_at: String::new(),
        receipt_id: None,
    };

    store.submit_operation(envelope).await.unwrap();
    operation_id
}

fn make_edge_request(
    edge_type: ProvenanceEdgeType,
    source: MemoryItemRef,
    target: MemoryItemRef,
    operation_id: OperationId,
    content_digest: Option<&str>,
    metadata: Option<&str>,
    recorded_at: Option<DateTime<Utc>>,
) -> ProvenanceEdgeRequest {
    ProvenanceEdgeRequest {
        edge_type,
        source,
        target,
        operation_id: Some(operation_id),
        actor_id: None,
        device_id: None,
        valid_from: None,
        valid_to: None,
        observed_at: None,
        recorded_at,
        content_digest: content_digest.map(str::to_string),
        metadata: metadata.map(str::to_string),
        supersedes_edge_id: None,
    }
}

#[tokio::test]
async fn schema_and_edge_storage_persist_across_reopen() {
    let (store, dir) = open_store().await;
    let (device_id, actor_id) = setup_subjects(&store).await;
    let operation_id = submit_operation(
        &store,
        device_id.clone(),
        actor_id,
        OperationKind::Observe,
        "fact",
        "fact-1",
        "sha256:record-1",
    )
    .await;

    let edge = store
        .record_provenance_edge(make_edge_request(
            ProvenanceEdgeType::ObservedBy,
            MemoryItemRef::new("fact", "fact-1").unwrap(),
            MemoryItemRef::new("operation", operation_id.as_str()).unwrap(),
            operation_id.clone(),
            Some("digest-observed"),
            Some("{\"source\":\"integration\"}"),
            None,
        ))
        .await
        .unwrap();

    drop(store);

    let reopened = MnemesStore::open_with_embedder(
        dir.path().to_path_buf(),
        MemoryConfig {
            base_dir: dir.path().to_path_buf(),
            embedding: EmbeddingConfig {
                dimensions: 768,
                ..Default::default()
            },
            ..Default::default()
        },
        Box::new(MockEmbedder::new(768)),
    )
    .unwrap();

    let loaded = reopened
        .get_provenance_edge(&edge.edge_id)
        .await
        .unwrap()
        .expect("edge persisted after reopen");

    assert_eq!(loaded.edge_id, edge.edge_id);
    assert_eq!(loaded.source, MemoryItemRef::new("fact", "fact-1").unwrap());
    assert_eq!(
        loaded.target,
        MemoryItemRef::new("operation", operation_id.as_str()).unwrap()
    );
}

#[tokio::test]
async fn record_each_edge_type_and_query_by_filters() {
    let (store, _dir) = open_store().await;
    let (device_id, actor_id) = setup_subjects(&store).await;

    let observe = submit_operation(
        &store,
        device_id.clone(),
        actor_id.clone(),
        OperationKind::Observe,
        "fact",
        "source",
        "sha256:op-observe",
    )
    .await;
    let assert = submit_operation(
        &store,
        device_id.clone(),
        actor_id.clone(),
        OperationKind::Assert,
        "fact",
        "target",
        "sha256:op-assert",
    )
    .await;
    let supersede = submit_operation(
        &store,
        device_id.clone(),
        actor_id.clone(),
        OperationKind::Supersede,
        "fact",
        "derived",
        "sha256:op-supersede",
    )
    .await;
    let adjudicate = submit_operation(
        &store,
        device_id.clone(),
        actor_id,
        OperationKind::Adjudicate,
        "fact",
        "claim",
        "sha256:op-adjudicate",
    )
    .await;

    let item_a = MemoryItemRef::new("fact", "a").unwrap();
    let item_b = MemoryItemRef::new("fact", "b").unwrap();
    let item_c = MemoryItemRef::new("fact", "c").unwrap();

    store
        .record_provenance_edge(make_edge_request(
            ProvenanceEdgeType::ObservedBy,
            item_a.clone(),
            MemoryItemRef::new("operation", observe.as_str()).unwrap(),
            observe.clone(),
            Some("d1"),
            None,
            None,
        ))
        .await
        .unwrap();

    store
        .record_provenance_edge(make_edge_request(
            ProvenanceEdgeType::RecordedBy,
            item_b.clone(),
            MemoryItemRef::new("operation", assert.as_str()).unwrap(),
            assert.clone(),
            Some("d2"),
            Some("{\"mode\":\"indexer\"}"),
            None,
        ))
        .await
        .unwrap();

    store
        .record_provenance_edge(make_edge_request(
            ProvenanceEdgeType::DerivedFrom,
            item_a.clone(),
            item_b.clone(),
            supersede.clone(),
            Some("d3"),
            None,
            None,
        ))
        .await
        .unwrap();

    store
        .record_provenance_edge(make_edge_request(
            ProvenanceEdgeType::Supports,
            item_c.clone(),
            item_b.clone(),
            assert.clone(),
            Some("d4"),
            None,
            None,
        ))
        .await
        .unwrap();

    store
        .record_provenance_edge(make_edge_request(
            ProvenanceEdgeType::Contradicts,
            item_c.clone(),
            item_a.clone(),
            adjudicate.clone(),
            Some("d5"),
            None,
            None,
        ))
        .await
        .unwrap();

    let sup_edge = store
        .supersede(
            MemoryItemRef::new("fact", "a-v2").unwrap(),
            item_a.clone(),
            adjudicate,
            None,
            None,
            Some("{\"reason\":\"update\"}".to_string()),
        )
        .await
        .unwrap();
    assert_eq!(sup_edge.edge_type, ProvenanceEdgeType::Supersedes);

    let retrieved = store
        .record_provenance_edge(make_edge_request(
            ProvenanceEdgeType::RetrievedFrom,
            MemoryItemRef::new("operation", assert.as_str()).unwrap(),
            item_b,
            assert,
            Some("d6"),
            Some("{\"retrieved\":true}"),
            None,
        ))
        .await
        .unwrap();

    let supports = store
        .query_provenance_edges(ProvenanceQuery {
            source: Some(item_c),
            target: None,
            edge_types: vec![
                ProvenanceEdgeType::Supports,
                ProvenanceEdgeType::Contradicts,
            ],
            as_of: AsOf::now(),
            limit: 10,
            include_superseded: true,
            ..Default::default()
        })
        .await
        .unwrap();

    let total = store
        .query_provenance_edges(ProvenanceQuery {
            as_of: AsOf::now(),
            include_superseded: false,
            limit: 50,
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(supports.len(), 2);
    assert_eq!(supports[0].edge_type, ProvenanceEdgeType::Supports);
    assert_eq!(supports[1].edge_type, ProvenanceEdgeType::Contradicts);
    assert!(total.len() >= 6);
    assert_eq!(retrieved.edge_type, ProvenanceEdgeType::RetrievedFrom);
}

#[tokio::test]
async fn batch_insertion_is_atomic() {
    let (store, _dir) = open_store().await;
    let (device_id, actor_id) = setup_subjects(&store).await;

    let op = submit_operation(
        &store,
        device_id,
        actor_id,
        OperationKind::Assert,
        "fact",
        "batch-target",
        "sha256:batch-op",
    )
    .await;

    let source_a = MemoryItemRef::new("fact", "batch-a").unwrap();
    let source_b = MemoryItemRef::new("fact", "batch-b").unwrap();
    let target = MemoryItemRef::new("operation", op.as_str()).unwrap();

    let valid_a_from = fixed_time("2026-07-19T12:00:00Z");
    let valid_a_to = fixed_time("2026-07-19T12:10:00Z");
    let invalid_from = fixed_time("2026-07-19T12:20:00Z");
    let invalid_to = fixed_time("2026-07-19T12:05:00Z");

    let request_a = ProvenanceEdgeRequest {
        edge_type: ProvenanceEdgeType::Supports,
        source: source_a,
        target: target.clone(),
        operation_id: Some(op.clone()),
        actor_id: None,
        device_id: None,
        valid_from: Some(valid_a_from),
        valid_to: Some(valid_a_to),
        observed_at: None,
        recorded_at: None,
        content_digest: Some("batch-one".to_string()),
        metadata: None,
        supersedes_edge_id: None,
    };

    let request_b = ProvenanceEdgeRequest {
        edge_type: ProvenanceEdgeType::Supports,
        source: source_b,
        target,
        operation_id: Some(op.clone()),
        actor_id: None,
        device_id: None,
        valid_from: Some(invalid_from),
        valid_to: Some(invalid_to),
        observed_at: None,
        recorded_at: None,
        content_digest: Some("batch-two".to_string()),
        metadata: None,
        supersedes_edge_id: None,
    };

    let batch = store
        .record_provenance_edges(&[request_a.clone(), request_b.clone()])
        .await;
    assert!(batch.is_err());

    let visible = store
        .query_provenance_edges(ProvenanceQuery {
            source: Some(request_a.source.clone()),
            operation_id: Some(op.clone()),
            as_of: AsOf::now(),
            target: None,
            edge_types: vec![ProvenanceEdgeType::Supports],
            include_superseded: false,
            limit: 20,
        })
        .await
        .unwrap();
    assert!(visible.is_empty());
}

#[tokio::test]
async fn duplicate_event_is_idempotent_and_different_digest_conflicts() {
    let (store, _dir) = open_store().await;
    let (device_id, actor_id) = setup_subjects(&store).await;
    let op = submit_operation(
        &store,
        device_id,
        actor_id,
        OperationKind::Adjudicate,
        "fact",
        "idempotent",
        "sha256:adjudicate",
    )
    .await;

    let request = ProvenanceEdgeRequest {
        edge_type: ProvenanceEdgeType::Supports,
        source: MemoryItemRef::new("fact", "idemp-source").unwrap(),
        target: MemoryItemRef::new("fact", "idemp-target").unwrap(),
        operation_id: Some(op.clone()),
        actor_id: None,
        device_id: None,
        valid_from: None,
        valid_to: None,
        observed_at: None,
        recorded_at: None,
        content_digest: Some("digest-same".to_string()),
        metadata: Some("{\"score\":0.5}".to_string()),
        supersedes_edge_id: None,
    };

    let first = store.record_provenance_edge(request.clone()).await.unwrap();
    let second = store.record_provenance_edge(request.clone()).await.unwrap();
    assert_eq!(first.edge_id, second.edge_id);

    let mut conflict = request.clone();
    conflict.content_digest = Some("digest-different".to_string());
    let mismatch = store.record_provenance_edge(conflict).await;
    assert!(mismatch.is_err());
}

#[tokio::test]
async fn invalid_references_and_payloads_are_rejected() {
    let (store, _dir) = open_store().await;
    let (device_id, actor_id) = setup_subjects(&store).await;
    let op = submit_operation(
        &store,
        device_id,
        actor_id,
        OperationKind::Assert,
        "fact",
        "bad-input",
        "sha256:bad",
    )
    .await;

    assert!(MemoryItemRef::new("", "fact").is_err());
    assert!(MemoryItemRef::new("fact", "").is_err());

    let invalid_self = store
        .record_provenance_edge(ProvenanceEdgeRequest {
            edge_type: ProvenanceEdgeType::Supports,
            source: MemoryItemRef::new("fact", "same").unwrap(),
            target: MemoryItemRef::new("fact", "same").unwrap(),
            operation_id: Some(op.clone()),
            actor_id: None,
            device_id: None,
            valid_from: None,
            valid_to: None,
            observed_at: None,
            recorded_at: None,
            content_digest: Some("invalid-self".to_string()),
            metadata: None,
            supersedes_edge_id: None,
        })
        .await;
    assert!(invalid_self.is_err());

    let invalid_json = store
        .record_provenance_edge(ProvenanceEdgeRequest {
            edge_type: ProvenanceEdgeType::Supports,
            source: MemoryItemRef::new("fact", "src").unwrap(),
            target: MemoryItemRef::new("fact", "dst").unwrap(),
            operation_id: Some(op),
            actor_id: None,
            device_id: None,
            valid_from: Some(fixed_time("2026-07-19T12:10:00Z")),
            valid_to: Some(fixed_time("2026-07-19T12:00:00Z")),
            observed_at: None,
            recorded_at: None,
            content_digest: Some("invalid-time".to_string()),
            metadata: Some("{invalid-json".to_string()),
            supersedes_edge_id: None,
        })
        .await;
    assert!(invalid_json.is_err());
}

#[tokio::test]
async fn operation_actor_device_foreign_keys_are_checked() {
    let (store, _dir) = open_store().await;
    let (device_id, actor_id) = setup_subjects(&store).await;
    let op = submit_operation(
        &store,
        device_id,
        actor_id.clone(),
        OperationKind::Observe,
        "fact",
        "fk-check",
        "sha256:fk",
    )
    .await;

    let ghost_actor = ActorId::new();
    let actor_check = store.record_provenance_edge(ProvenanceEdgeRequest {
        edge_type: ProvenanceEdgeType::ObservedBy,
        source: MemoryItemRef::new("fact", "fk-source").unwrap(),
        target: MemoryItemRef::new("operation", op.as_str()).unwrap(),
        operation_id: Some(op.clone()),
        actor_id: Some(ghost_actor),
        device_id: None,
        valid_from: None,
        valid_to: None,
        observed_at: None,
        recorded_at: None,
        content_digest: Some("fk-actor".to_string()),
        metadata: None,
        supersedes_edge_id: None,
    });
    assert!(actor_check.await.is_err());

    let ghost_device = DeviceId::new();
    let device_check = store.record_provenance_edge(ProvenanceEdgeRequest {
        edge_type: ProvenanceEdgeType::ObservedBy,
        source: MemoryItemRef::new("fact", "fk-source-2").unwrap(),
        target: MemoryItemRef::new("operation", op.as_str()).unwrap(),
        operation_id: Some(op),
        actor_id: None,
        device_id: Some(ghost_device),
        valid_from: None,
        valid_to: None,
        observed_at: None,
        recorded_at: None,
        content_digest: Some("fk-device".to_string()),
        metadata: None,
        supersedes_edge_id: None,
    });
    assert!(device_check.await.is_err());
}

#[tokio::test]
async fn observed_by_requires_observe_operation() {
    let (store, _dir) = open_store().await;
    let (device_id, actor_id) = setup_subjects(&store).await;
    let op_assert = submit_operation(
        &store,
        device_id,
        actor_id,
        OperationKind::Assert,
        "fact",
        "mismatch",
        "sha256:mismatch",
    )
    .await;

    let response = store
        .record_provenance_edge(ProvenanceEdgeRequest {
            edge_type: ProvenanceEdgeType::ObservedBy,
            source: MemoryItemRef::new("fact", "x").unwrap(),
            target: MemoryItemRef::new("operation", op_assert.as_str()).unwrap(),
            operation_id: Some(op_assert),
            actor_id: None,
            device_id: None,
            valid_from: None,
            valid_to: None,
            observed_at: None,
            recorded_at: None,
            content_digest: Some("mismatch".to_string()),
            metadata: None,
            supersedes_edge_id: None,
        })
        .await;

    assert!(response.is_err());
}

#[tokio::test]
async fn record_and_query_by_as_of_recorded_cuttoff() {
    let (store, _dir) = open_store().await;
    let (device_id, actor_id) = setup_subjects(&store).await;

    let op = submit_operation(
        &store,
        device_id,
        actor_id,
        OperationKind::Assert,
        "fact",
        "asof-item",
        "sha256:asof",
    )
    .await;

    let earlier_recorded = fixed_time("2026-07-19T10:00:00Z");
    let later_recorded = fixed_time("2026-07-19T11:00:00Z");

    let early = store
        .record_provenance_edge(ProvenanceEdgeRequest {
            edge_type: ProvenanceEdgeType::Supports,
            source: MemoryItemRef::new("fact", "early").unwrap(),
            target: MemoryItemRef::new("fact", "asof").unwrap(),
            operation_id: Some(op.clone()),
            actor_id: None,
            device_id: None,
            valid_from: None,
            valid_to: None,
            observed_at: None,
            recorded_at: Some(earlier_recorded),
            content_digest: Some("asof-early".to_string()),
            metadata: None,
            supersedes_edge_id: None,
        })
        .await
        .unwrap();

    let _late = store
        .record_provenance_edge(ProvenanceEdgeRequest {
            edge_type: ProvenanceEdgeType::Supports,
            source: MemoryItemRef::new("fact", "late").unwrap(),
            target: MemoryItemRef::new("fact", "asof").unwrap(),
            operation_id: Some(op),
            actor_id: None,
            device_id: None,
            valid_from: None,
            valid_to: None,
            observed_at: None,
            recorded_at: Some(later_recorded),
            content_digest: Some("asof-late".to_string()),
            metadata: None,
            supersedes_edge_id: None,
        })
        .await
        .unwrap();

    let before = store
        .query_provenance_edges(ProvenanceQuery {
            target: Some(MemoryItemRef::new("fact", "asof").unwrap()),
            as_of: AsOf::at(None, Some(fixed_time("2026-07-19T10:30:00Z").to_rfc3339())),
            limit: 10,
            include_superseded: false,
            ..Default::default()
        })
        .await
        .unwrap();

    let after = store
        .query_provenance_edges(ProvenanceQuery {
            target: Some(MemoryItemRef::new("fact", "asof").unwrap()),
            as_of: AsOf::at(None, Some(fixed_time("2026-07-19T11:30:00Z").to_rfc3339())),
            limit: 10,
            include_superseded: false,
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(before.len(), 1);
    assert_eq!(before[0].edge_id, early.edge_id);
    assert_eq!(after.len(), 2);
}
