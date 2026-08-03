use ed25519_dalek::SigningKey;
use mnemes::replication::{FactCreateTransportEntryV1, SignedFactCreateBatchV1};
use semantic_memory::journal::{
    encode_fact_create_payload, envelope_digest, payload_digest, FactCreatePayloadV1, JournalEntry,
    FACT_CREATE_OPERATION, FACT_CREATE_PAYLOAD_SCHEMA, GENESIS_PREDECESSOR, VERIFIED_RECORD_STATE,
};

fn journal_entry(sequence: i64, predecessor_digest: [u8; 32]) -> JournalEntry {
    let payload = encode_fact_create_payload(&FactCreatePayloadV1 {
        fact_id: "00000000-0000-0000-0000-000000000001".to_string(),
        namespace: "general".to_string(),
        content: "typed replication test fact".to_string(),
        source: Some("test".to_string()),
        metadata: None,
    })
    .expect("fixture payload must encode");
    let payload_digest = payload_digest(&payload);
    let envelope_digest = envelope_digest(
        "laptop",
        "primary",
        7,
        sequence,
        FACT_CREATE_OPERATION,
        FACT_CREATE_PAYLOAD_SCHEMA,
        &predecessor_digest,
        &payload_digest,
    );
    JournalEntry {
        journal_id: sequence,
        home_device_id: "laptop".to_string(),
        store_id: "primary".to_string(),
        stream_epoch: 7,
        sequence,
        operation_kind: FACT_CREATE_OPERATION.to_string(),
        payload_schema: FACT_CREATE_PAYLOAD_SCHEMA.to_string(),
        payload,
        payload_digest,
        predecessor_digest,
        envelope_digest,
        record_state: VERIFIED_RECORD_STATE.to_string(),
        created_at: "2026-07-29T00:00:00Z".to_string(),
    }
}

fn signed_batch() -> (SignedFactCreateBatchV1, SigningKey) {
    let entry =
        FactCreateTransportEntryV1::from_journal_entry(&journal_entry(1, GENESIS_PREDECESSOR))
            .expect("fixture journal entry must map");
    let signing_key = SigningKey::from_bytes(&[7u8; 32]);
    let mut batch = SignedFactCreateBatchV1::new(
        "batch-1",
        "laptop",
        "primary",
        7,
        1,
        vec![entry],
        "laptop-replication-key",
        1,
        1_000,
        "fence-7",
    )
    .expect("fixture batch must construct");
    batch.sign(&signing_key).expect("fixture batch must sign");
    (batch, signing_key)
}

#[test]
fn valid_batch_verifies_and_maps_to_semantic_envelopes_without_reencoding_payload() {
    let (batch, _signing_key) = signed_batch();
    batch.validate().expect("valid batch must validate");

    let envelopes = batch
        .semantic_envelopes()
        .expect("valid batch must map to semantic envelopes");
    assert_eq!(envelopes.len(), 1);
    assert_eq!(envelopes[0].home_device_id, "laptop");
    assert_eq!(envelopes[0].store_id, "primary");
    assert_eq!(envelopes[0].stream_epoch, 7);
    assert_eq!(envelopes[0].sequence, 1);
    assert_eq!(envelopes[0].payload, batch.entries[0].payload);
    assert_eq!(envelopes[0].payload_digest, batch.entries[0].payload_digest);
    assert_eq!(
        envelopes[0].envelope_digest,
        batch.entries[0].journal_envelope_digest
    );
}

#[test]
fn changed_payload_or_journal_digest_is_rejected() {
    let (mut batch, _signing_key) = signed_batch();
    batch.entries[0].payload[0] ^= 1;
    assert!(batch.validate().is_err());

    let (mut batch, _signing_key) = signed_batch();
    batch.entries[0].journal_envelope_digest[0] ^= 1;
    assert!(batch.validate().is_err());
}

#[test]
fn sequence_and_predecessor_binding_is_rejected() {
    let (mut batch, _signing_key) = signed_batch();
    batch.entries[0].sequence = 2;
    assert!(batch.validate().is_err());

    let (mut batch, _signing_key) = signed_batch();
    batch.entries[0].predecessor_digest[0] = 1;
    assert!(batch.validate().is_err());
}

#[test]
fn strict_json_decoder_rejects_unknown_fields_and_trailing_bytes() {
    let (batch, _signing_key) = signed_batch();
    let mut json = serde_json::to_vec(&batch).expect("batch must serialize as projection");

    let mut unknown = json.clone();
    unknown.pop();
    unknown.extend_from_slice(b",\"unknown\":true}");
    assert!(SignedFactCreateBatchV1::decode_json(&unknown).is_err());

    json.extend_from_slice(b"{}\n");
    assert!(SignedFactCreateBatchV1::decode_json(&json).is_err());
}

#[test]
fn two_entry_signed_batch_is_rejected_before_semantic_mapping() {
    let (mut batch, _signing_key) = signed_batch();
    batch.entries.push(batch.entries[0].clone());
    let error = batch
        .validate()
        .expect_err("V1 must reject a two-entry batch");
    assert!(error.to_string().contains("entries"));
}
