use hmac::Mac;
use mnemes::{
    Actor, ActorId, ActorKind, AsOf, Device, DeviceId, MnemesError, MnemesStore,
    RunPackEvidenceProjectionV1,
};
use semantic_memory::{EmbeddingConfig, MemoryConfig, MockEmbedder};
use serde_json::{json, Value};
use sha2::Digest;
use tempfile::TempDir;

type FixtureResult<T> = Result<T, Box<dyn std::error::Error>>;
type TestResult = FixtureResult<()>;
const ATTESTATION_KEY: &[u8] = b"witnessed-workbench-mnemes-test-only-v1";

async fn open_authenticated_store() -> Result<(MnemesStore, String, TempDir), MnemesError> {
    let directory = TempDir::new().map_err(MnemesError::Io)?;
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
    )?;
    let device_id = DeviceId::new();
    let (_, credential) = store
        .register_device_with_generated_credential(Device::new(
            device_id.clone(),
            "witnessed-workbench-fixture",
            "linux",
            "localhost",
        ))
        .await?;
    store
        .register_actor(Actor::new(ActorId::new(), device_id, ActorKind::Hermes))
        .await?;
    Ok((store, credential, directory))
}

fn signed_envelope(projection: Value) -> FixtureResult<Vec<u8>> {
    let typed: RunPackEvidenceProjectionV1 = serde_json::from_value(projection.clone())?;
    let canonical = serde_json::to_vec(&typed)?;
    let mut witness = json!({
        "format": "mnemes.run-pack-admission-witness/v1",
        "canonical_projection_digest": format!("sha256:{}", hex::encode(sha2::Sha256::digest(&canonical))),
        "pack_manifest_digest": projection["pack_manifest_digest"],
        "pack_content_digest": projection["pack_content_digest"],
        "verification_receipt_digest": projection["verification"]["verification_receipt_digest"],
        "verified": true,
    });
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(ATTESTATION_KEY)?;
    mac.update(&serde_json::to_vec(&witness)?);
    witness["signature"] = json!(hex::encode(mac.finalize().into_bytes()));
    Ok(serde_json::to_vec(&json!({
        "projection": projection,
        "admission_witness": witness,
    }))?)
}

#[tokio::test]
async fn imports_real_generated_projection_when_fixture_paths_are_explicit() -> TestResult {
    let projection_path = match std::env::var("MNEMES_PHASE5_PROJECTION") {
        Ok(value) => std::path::PathBuf::from(value),
        Err(_) => return Ok(()),
    };
    let receipt_path = std::path::PathBuf::from(std::env::var("MNEMES_PHASE5_RECEIPT_OUT")?);
    let projection: Value = serde_json::from_slice(&std::fs::read(&projection_path)?)?;
    let bytes = signed_envelope(projection.clone())?;
    let (store, credential, _directory) = open_authenticated_store().await?;

    let first = store
        .import_run_pack_observation(&credential, &bytes, Some(ATTESTATION_KEY))
        .await?;
    let second = store
        .import_run_pack_observation(&credential, &bytes, Some(ATTESTATION_KEY))
        .await?;
    if first != second || store.count_operations().await? != 1 {
        return Err("generated projection was not exact-idempotently observed".into());
    }
    let (_, edges) = store
        .operation_provenance(&first.operation_id, AsOf::now())
        .await?;
    if edges.len() != 1 {
        return Err("generated projection did not produce exactly one provenance edge".into());
    }

    let mut changed = projection;
    changed["event_summary"]["terminal_state"] = json!("failed");
    let changed_bytes = signed_envelope(changed)?;
    let conflict = store
        .import_run_pack_observation(&credential, &changed_bytes, Some(ATTESTATION_KEY))
        .await;
    if !matches!(conflict, Err(MnemesError::IdempotencyConflict { .. }))
        || store.count_operations().await? != 1
    {
        return Err(
            "different generated projection under the same manifest key did not conflict".into(),
        );
    }

    if let Some(parent) = receipt_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let output = json!({
        "schema": "witnessed-workbench.mnemes-fixture-result/v1",
        "projection_sha256": hex::encode(sha2::Sha256::digest(std::fs::read(&projection_path)?)),
        "receipt_id": first.receipt_id,
        "operation_id": first.operation_id.to_string(),
        "recorded_at": first.recorded_at,
        "idempotent": true,
        "different_bytes_same_key_rejected": true,
        "provenance_edge_count": edges.len(),
    });
    std::fs::write(receipt_path, serde_json::to_vec_pretty(&output)?)?;
    Ok(())
}
