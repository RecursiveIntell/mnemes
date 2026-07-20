use chrono::{DateTime, Utc};
use pooled_memory::{
    Actor, ActorId, ActorKind, AsOf, Device, DeviceId, MemoryItemRef, OperationEnvelope,
    OperationId, OperationKind, PooledMemoryStore, ProvenanceEdgeRequest, ProvenanceEdgeType,
    ProvenanceQuery,
};
use semantic_memory::{EmbeddingConfig, GraphDirection, MemoryConfig, MockEmbedder};
use std::sync::Arc;
use tempfile::TempDir;

fn fixed_time(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .unwrap()
        .with_timezone(&Utc)
}

async fn open_store() -> (PooledMemoryStore, TempDir) {
    let dir = TempDir::new().unwrap();
    let config = MemoryConfig {
        base_dir: dir.path().to_path_buf(),
        embedding: EmbeddingConfig {
            dimensions: 768,
            ..Default::default()
        },
        ..Default::default()
    };

    let store = PooledMemoryStore::open_with_embedder(
        dir.path().to_path_buf(),
        config,
        Box::new(MockEmbedder::new(768)),
    )
    .unwrap();

    (store, dir)
}

async fn setup_subjects(store: &PooledMemoryStore) -> (DeviceId, ActorId) {
    let device_id = DeviceId::new();
    store
        .register_device(Device::new(
            device_id.clone(),
            "bitemporal-device",
            "linux",
            "bitemporal-host",
        ))
        .await
        .unwrap();

    let actor_id = ActorId::new();
    store
        .register_actor(Actor::new(
            actor_id.clone(),
            device_id.clone(),
            ActorKind::Hermes,
        ))
        .await
        .unwrap();

    (device_id, actor_id)
}

async fn submit_operation(
    store: &PooledMemoryStore,
    device_id: DeviceId,
    actor_id: ActorId,
    operation_kind: OperationKind,
    target_kind: &str,
    target_id: &str,
    digest: &str,
) -> OperationId {
    let operation_id = OperationId::new();
    let envelope = OperationEnvelope {
        operation_id: operation_id.clone(),
        idempotency_key: operation_id.as_str().to_string(),
        requesting_device_id: device_id.clone(),
        requesting_actor_id: actor_id.clone(),
        recording_device_id: device_id.clone(),
        recording_server_id: device_id.clone(),
        operation_kind,
        target_kind: target_kind.to_string(),
        target_id: target_id.to_string(),
        content_digest: digest.to_string(),
        observed_at: None,
        valid_time: None,
        recorded_at: String::new(),
        receipt_id: None,
    };
    store.submit_operation(envelope).await.unwrap();
    operation_id
}

#[tokio::test]
async fn as_of_recorded_and_valid_axes_are_independent() {
    let (store, _dir) = open_store().await;
    let (device_id, actor_id) = setup_subjects(&store).await;

    let op = submit_operation(
        &store,
        device_id,
        actor_id,
        OperationKind::Assert,
        "fact",
        "domain",
        "sha256:domain-op",
    )
    .await;

    let valid_window = (
        fixed_time("2026-07-19T09:00:00Z"),
        fixed_time("2026-07-19T11:00:00Z"),
    );

    let record_early = fixed_time("2026-07-19T08:00:00Z");
    let record_late = fixed_time("2026-07-19T12:00:00Z");

    store
        .record_provenance_edge(ProvenanceEdgeRequest {
            edge_type: ProvenanceEdgeType::Supports,
            source: MemoryItemRef::new("fact", "x").unwrap(),
            target: MemoryItemRef::new("fact", "domain").unwrap(),
            operation_id: Some(op.clone()),
            actor_id: None,
            device_id: None,
            valid_from: Some(valid_window.0),
            valid_to: Some(valid_window.1),
            observed_at: None,
            recorded_at: Some(record_early),
            content_digest: Some("record-early".to_string()),
            metadata: None,
            supersedes_edge_id: None,
        })
        .await
        .unwrap();

    store
        .record_provenance_edge(ProvenanceEdgeRequest {
            edge_type: ProvenanceEdgeType::Contradicts,
            source: MemoryItemRef::new("fact", "y").unwrap(),
            target: MemoryItemRef::new("fact", "domain").unwrap(),
            operation_id: Some(op.clone()),
            actor_id: None,
            device_id: None,
            valid_from: Some(fixed_time("2026-07-19T10:00:00Z")),
            valid_to: Some(fixed_time("2026-07-19T13:00:00Z")),
            observed_at: None,
            recorded_at: Some(record_late),
            content_digest: Some("record-late".to_string()),
            metadata: None,
            supersedes_edge_id: None,
        })
        .await
        .unwrap();

    let by_recorded_before = store
        .query_provenance_edges(ProvenanceQuery {
            source: Some(MemoryItemRef::new("fact", "x").unwrap()),
            as_of: AsOf::at(None, Some(fixed_time("2026-07-19T09:30:00Z").to_rfc3339())),
            include_superseded: false,
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(by_recorded_before.len(), 1);

    let by_recorded_middle = store
        .query_provenance_edges(ProvenanceQuery {
            target: Some(MemoryItemRef::new("fact", "domain").unwrap()),
            as_of: AsOf::at(None, Some(fixed_time("2026-07-19T12:30:00Z").to_rfc3339())),
            include_superseded: false,
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(by_recorded_middle.len(), 2);

    let by_valid_after_first_expires = store
        .query_provenance_edges(ProvenanceQuery {
            target: Some(MemoryItemRef::new("fact", "domain").unwrap()),
            as_of: AsOf::at(
                Some(fixed_time("2026-07-19T12:30:00Z").to_rfc3339()),
                Some(fixed_time("2026-07-19T23:00:00Z").to_rfc3339()),
            ),
            include_superseded: false,
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(by_valid_after_first_expires.len(), 1);
}

#[tokio::test]
async fn valid_bounds_use_half_open_intervals() {
    let (store, _dir) = open_store().await;
    let (device_id, actor_id) = setup_subjects(&store).await;
    let op = submit_operation(
        &store,
        device_id,
        actor_id,
        OperationKind::Assert,
        "fact",
        "bounds",
        "sha256:bounds",
    )
    .await;

    let edge = store
        .record_provenance_edge(ProvenanceEdgeRequest {
            edge_type: ProvenanceEdgeType::Supports,
            source: MemoryItemRef::new("fact", "bounded").unwrap(),
            target: MemoryItemRef::new("fact", "bounds").unwrap(),
            operation_id: Some(op),
            actor_id: None,
            device_id: None,
            valid_from: Some(fixed_time("2026-07-19T10:00:00Z")),
            valid_to: Some(fixed_time("2026-07-19T12:00:00Z")),
            observed_at: None,
            recorded_at: None,
            content_digest: Some("bounded".to_string()),
            metadata: None,
            supersedes_edge_id: None,
        })
        .await
        .unwrap();

    let before = store
        .query_provenance_edges(ProvenanceQuery {
            source: Some(MemoryItemRef::new("fact", "bounded").unwrap()),
            as_of: AsOf::at(Some(fixed_time("2026-07-19T09:59:59Z").to_rfc3339()), None),
            include_superseded: false,
            limit: 20,
            ..Default::default()
        })
        .await
        .unwrap();

    let at_start = store
        .query_provenance_edges(ProvenanceQuery {
            source: Some(MemoryItemRef::new("fact", "bounded").unwrap()),
            as_of: AsOf::at(Some(fixed_time("2026-07-19T10:00:00Z").to_rfc3339()), None),
            include_superseded: false,
            limit: 20,
            ..Default::default()
        })
        .await
        .unwrap();

    let at_end_exclusive = store
        .query_provenance_edges(ProvenanceQuery {
            source: Some(MemoryItemRef::new("fact", "bounded").unwrap()),
            as_of: AsOf::at(Some(fixed_time("2026-07-19T12:00:00Z").to_rfc3339()), None),
            include_superseded: false,
            limit: 20,
            ..Default::default()
        })
        .await
        .unwrap();

    let after = store
        .query_provenance_edges(ProvenanceQuery {
            source: Some(MemoryItemRef::new("fact", "bounded").unwrap()),
            as_of: AsOf::at(Some(fixed_time("2026-07-19T12:00:01Z").to_rfc3339()), None),
            include_superseded: false,
            limit: 20,
            ..Default::default()
        })
        .await
        .unwrap();

    assert!(before.is_empty());
    assert_eq!(at_start.len(), 1);
    assert_eq!(at_start[0].edge_id, edge.edge_id);
    assert!(at_end_exclusive.is_empty());
    assert!(after.is_empty());
}

#[tokio::test]
async fn supersession_projection_hides_predecessor_when_excluded() {
    let (store, _dir) = open_store().await;
    let (device_id, actor_id) = setup_subjects(&store).await;

    let op_create = submit_operation(
        &store,
        device_id.clone(),
        actor_id.clone(),
        OperationKind::Assert,
        "fact",
        "old",
        "sha256:old-op",
    )
    .await;

    let op_supersede = submit_operation(
        &store,
        device_id,
        actor_id,
        OperationKind::Supersede,
        "fact",
        "sup",
        "sha256:sup-op",
    )
    .await;

    let old_claim = MemoryItemRef::new("fact", "claim-old").unwrap();
    let new_claim = MemoryItemRef::new("fact", "claim-new").unwrap();
    let evidence = MemoryItemRef::new("fact", "evidence").unwrap();

    let old_support = store
        .record_provenance_edge(ProvenanceEdgeRequest {
            edge_type: ProvenanceEdgeType::Supports,
            source: evidence.clone(),
            target: old_claim.clone(),
            operation_id: Some(op_create),
            actor_id: None,
            device_id: None,
            valid_from: None,
            valid_to: None,
            observed_at: None,
            recorded_at: None,
            content_digest: Some("evidence-old".to_string()),
            metadata: None,
            supersedes_edge_id: None,
        })
        .await
        .unwrap();

    let _supersede = store
        .supersede(
            new_claim,
            old_claim.clone(),
            op_supersede,
            Some(fixed_time("2026-07-19T12:00:00Z")),
            None,
            Some("{\"reason\":\"refresh\"}".to_string()),
        )
        .await
        .unwrap();

    let hidden = store
        .query_provenance_edges(ProvenanceQuery {
            target: Some(old_claim.clone()),
            as_of: AsOf::at(None, None),
            include_superseded: false,
            edge_types: vec![ProvenanceEdgeType::Supports],
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap();

    let all = store
        .query_provenance_edges(ProvenanceQuery {
            target: Some(old_claim),
            as_of: AsOf::at(None, None),
            include_superseded: true,
            edge_types: vec![ProvenanceEdgeType::Supports],
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(hidden.len(), 0);
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].edge_id, old_support.edge_id);
}

#[tokio::test]
async fn contradiction_is_append_only_and_retrievable() {
    let (store, _dir) = open_store().await;
    let (device_id, actor_id) = setup_subjects(&store).await;

    let op_support = submit_operation(
        &store,
        device_id.clone(),
        actor_id.clone(),
        OperationKind::Assert,
        "fact",
        "claim",
        "sha256:claim-support",
    )
    .await;

    let op_contradict = submit_operation(
        &store,
        device_id,
        actor_id,
        OperationKind::Adjudicate,
        "fact",
        "claim",
        "sha256:claim-contradict",
    )
    .await;

    let fact_a = MemoryItemRef::new("fact", "evidence-a").unwrap();
    let fact_b = MemoryItemRef::new("fact", "claim").unwrap();

    let support = store
        .record_provenance_edge(ProvenanceEdgeRequest {
            edge_type: ProvenanceEdgeType::Supports,
            source: fact_a,
            target: fact_b.clone(),
            operation_id: Some(op_support),
            actor_id: None,
            device_id: None,
            valid_from: None,
            valid_to: None,
            observed_at: None,
            recorded_at: None,
            content_digest: Some("support-evidence".to_string()),
            metadata: Some("{\"label\":\"support\"}".to_string()),
            supersedes_edge_id: None,
        })
        .await
        .unwrap();

    let contradict = store
        .contradict(
            MemoryItemRef::new("fact", "evidence-b").unwrap(),
            fact_b,
            op_contradict,
            None,
            None,
            Some("{\"label\":\"contradiction\"}".to_string()),
        )
        .await
        .unwrap();

    let all = store
        .query_provenance_edges(ProvenanceQuery {
            target: Some(MemoryItemRef::new("fact", "claim").unwrap()),
            as_of: AsOf::at(None, None),
            edge_types: vec![
                ProvenanceEdgeType::Supports,
                ProvenanceEdgeType::Contradicts,
            ],
            include_superseded: true,
            limit: 20,
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(all.len(), 2);

    let ids: Vec<_> = all.iter().map(|edge| edge.edge_id.to_string()).collect();
    assert!(ids.contains(&support.edge_id.to_string()));
    assert!(ids.contains(&contradict.edge_id.to_string()));
}

#[tokio::test]
async fn lineage_traversal_is_cycle_safe_and_respects_direction_and_depth() {
    let (store, _dir) = open_store().await;
    let (device_id, actor_id) = setup_subjects(&store).await;

    let op = submit_operation(
        &store,
        device_id,
        actor_id,
        OperationKind::Assert,
        "fact",
        "lineage",
        "sha256:lineage-op",
    )
    .await;

    store
        .record_provenance_edge(ProvenanceEdgeRequest {
            edge_type: ProvenanceEdgeType::Supports,
            source: MemoryItemRef::new("fact", "a").unwrap(),
            target: MemoryItemRef::new("fact", "b").unwrap(),
            operation_id: Some(op.clone()),
            actor_id: None,
            device_id: None,
            valid_from: None,
            valid_to: None,
            observed_at: None,
            recorded_at: None,
            content_digest: Some("a-b".to_string()),
            metadata: None,
            supersedes_edge_id: None,
        })
        .await
        .unwrap();

    store
        .record_provenance_edge(ProvenanceEdgeRequest {
            edge_type: ProvenanceEdgeType::Supports,
            source: MemoryItemRef::new("fact", "b").unwrap(),
            target: MemoryItemRef::new("fact", "c").unwrap(),
            operation_id: Some(op.clone()),
            actor_id: None,
            device_id: None,
            valid_from: None,
            valid_to: None,
            observed_at: None,
            recorded_at: None,
            content_digest: Some("b-c".to_string()),
            metadata: None,
            supersedes_edge_id: None,
        })
        .await
        .unwrap();

    store
        .record_provenance_edge(ProvenanceEdgeRequest {
            edge_type: ProvenanceEdgeType::Supports,
            source: MemoryItemRef::new("fact", "c").unwrap(),
            target: MemoryItemRef::new("fact", "a").unwrap(),
            operation_id: Some(op.clone()),
            actor_id: None,
            device_id: None,
            valid_from: None,
            valid_to: None,
            observed_at: None,
            recorded_at: None,
            content_digest: Some("c-a".to_string()),
            metadata: None,
            supersedes_edge_id: None,
        })
        .await
        .unwrap();

    let directed = store
        .lineage(
            MemoryItemRef::new("fact", "a").unwrap(),
            GraphDirection::Outgoing,
            4,
            AsOf::now(),
        )
        .await
        .unwrap();

    assert_eq!(directed.edges.len(), 3);
    assert!(!directed.truncated);

    let truncated = store
        .lineage(
            MemoryItemRef::new("fact", "a").unwrap(),
            GraphDirection::Outgoing,
            1,
            AsOf::now(),
        )
        .await
        .unwrap();

    assert_eq!(truncated.edges.len(), 1);
    assert!(truncated.truncated);

    let incoming = store
        .lineage(
            MemoryItemRef::new("fact", "c").unwrap(),
            GraphDirection::Incoming,
            4,
            AsOf::now(),
        )
        .await
        .unwrap();

    assert!(incoming
        .edges
        .iter()
        .any(|edge| edge.edge_type == ProvenanceEdgeType::Supports));
}

#[tokio::test]
async fn as_of_item_lineage_returns_projection_without_mutation() {
    let (store, _dir) = open_store().await;
    let (device_id, actor_id) = setup_subjects(&store).await;

    let op = submit_operation(
        &store,
        device_id,
        actor_id,
        OperationKind::Assert,
        "fact",
        "projection",
        "sha256:proj",
    )
    .await;

    store
        .record_provenance_edge(ProvenanceEdgeRequest {
            edge_type: ProvenanceEdgeType::Supports,
            source: MemoryItemRef::new("fact", "source").unwrap(),
            target: MemoryItemRef::new("fact", "projection").unwrap(),
            operation_id: Some(op),
            actor_id: None,
            device_id: None,
            valid_from: None,
            valid_to: None,
            observed_at: None,
            recorded_at: Some(fixed_time("2026-07-19T01:00:00Z")),
            content_digest: Some("proj-support".to_string()),
            metadata: None,
            supersedes_edge_id: None,
        })
        .await
        .unwrap();

    let view_now = store
        .as_of_item_lineage(
            MemoryItemRef::new("fact", "projection").unwrap(),
            AsOf::now(),
        )
        .await
        .unwrap();

    assert!(!view_now.edges.is_empty());
    let view_before = store
        .as_of_item_lineage(
            MemoryItemRef::new("fact", "projection").unwrap(),
            AsOf::at(None, Some(fixed_time("2026-07-19T00:59:00Z").to_rfc3339())),
        )
        .await
        .unwrap();

    assert!(view_before.edges.is_empty());
}

#[tokio::test]
async fn concurrent_transactions_do_not_corrupt_store() {
    let (store, _dir) = open_store().await;
    let (device_id, actor_id) = setup_subjects(&store).await;
    let store = Arc::new(store);

    let mut writers = Vec::new();
    for index in 0..16usize {
        let store = Arc::clone(&store);
        let device_id = device_id.clone();
        let actor_id = actor_id.clone();
        writers.push(tokio::spawn(async move {
            let op = submit_operation(
                &store,
                device_id,
                actor_id,
                if index % 2 == 0 {
                    OperationKind::Assert
                } else {
                    OperationKind::Observe
                },
                "fact",
                &format!("concurrent-{index}"),
                &format!("sha256:{index}"),
            )
            .await;

            store
                .record_provenance_edge(ProvenanceEdgeRequest {
                    edge_type: ProvenanceEdgeType::Supports,
                    source: MemoryItemRef::new("fact", format!("source-{index}")).unwrap(),
                    target: MemoryItemRef::new("operation", op.as_str()).unwrap(),
                    operation_id: Some(op),
                    actor_id: None,
                    device_id: None,
                    valid_from: None,
                    valid_to: None,
                    observed_at: None,
                    recorded_at: None,
                    content_digest: Some(format!("digest-{index}")),
                    metadata: Some(format!("{{\"index\":{index}}}")),
                    supersedes_edge_id: None,
                })
                .await
                .unwrap();

            let _ = store
                .query_provenance_edges(ProvenanceQuery {
                    as_of: AsOf::now(),
                    limit: 50,
                    include_superseded: false,
                    ..Default::default()
                })
                .await
                .unwrap();
        }));
    }

    for handle in writers {
        handle.await.unwrap();
    }

    let final_count = store
        .query_provenance_edges(ProvenanceQuery {
            as_of: AsOf::now(),
            edge_types: vec![ProvenanceEdgeType::Supports],
            include_superseded: false,
            limit: 100,
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(final_count.len(), 16);
}
