use ed25519_dalek::SigningKey;
use mnemes::replication::{
    FactSupersedeTransportEntryV1, SignedFactSupersedeBatchV1,
    FACT_SUPERSEDE_TRANSPORT_PROTOCOL_FAMILY,
};
use semantic_memory::journal::{
    encode_fact_create_payload, encode_fact_supersede_payload, envelope_digest, payload_digest,
    FactCreatePayloadV1, FactSupersedePayloadV1, FactSupersedeReplicaEnvelopeV1,
    FACT_SUPERSEDE_OPERATION, FACT_SUPERSEDE_PAYLOAD_SCHEMA, GENESIS_PREDECESSOR,
};

fn owner_envelope() -> FactSupersedeReplicaEnvelopeV1 {
    let replacement_payload = encode_fact_create_payload(&FactCreatePayloadV1 {
        fact_id: "00000000-0000-0000-0000-000000000002".into(),
        namespace: "general".into(),
        content: "owner-produced replacement".into(),
        source: Some("test".into()),
        metadata: None,
    })
    .unwrap();
    let payload = encode_fact_supersede_payload(&FactSupersedePayloadV1 {
        schema_version: FACT_SUPERSEDE_PAYLOAD_SCHEMA.into(),
        old_fact_id: "00000000-0000-0000-0000-000000000001".into(),
        new_fact_id: "00000000-0000-0000-0000-000000000002".into(),
        replacement_payload_digest: payload_digest(&replacement_payload),
        replacement_payload,
        semantic_predecessor_digest: "1".repeat(64),
        current_head_digest: "2".repeat(64),
        owner_valid_at: "2026-08-01T00:00:00Z".into(),
        owner_recorded_at: "2026-08-01T00:00:01Z".into(),
        authority_digest: format!("blake3:{}", "3".repeat(64)),
        authority_receipt_id: "00000000-0000-0000-0000-000000000003".into(),
        authority_receipt_digest: "4".repeat(64),
        transition_record_id: "00000000-0000-0000-0000-000000000004".into(),
        transition_digest: "5".repeat(64),
        receipt_id: "00000000-0000-0000-0000-000000000005".into(),
        receipt_digest: "6".repeat(64),
    })
    .unwrap();
    let pd = payload_digest(&payload);
    FactSupersedeReplicaEnvelopeV1 {
        home_device_id: "laptop".into(),
        store_id: "primary".into(),
        stream_epoch: 7,
        sequence: 1,
        operation_kind: FACT_SUPERSEDE_OPERATION.into(),
        payload_schema: FACT_SUPERSEDE_PAYLOAD_SCHEMA.into(),
        payload,
        payload_digest: pd,
        predecessor_digest: GENESIS_PREDECESSOR,
        envelope_digest: envelope_digest(
            "laptop",
            "primary",
            7,
            1,
            FACT_SUPERSEDE_OPERATION,
            FACT_SUPERSEDE_PAYLOAD_SCHEMA,
            &GENESIS_PREDECESSOR,
            &pd,
        ),
    }
}

fn signed_batch() -> (SignedFactSupersedeBatchV1, SigningKey) {
    let key = SigningKey::from_bytes(&[8u8; 32]);
    let mut batch = SignedFactSupersedeBatchV1::new(
        "supersede-batch-1",
        "laptop",
        "primary",
        3,
        4,
        7,
        1,
        vec![FactSupersedeTransportEntryV1::from_owner_envelope(
            owner_envelope(),
        )],
        "laptop-supersede-key",
        1,
        1_000,
        "fence-4",
    )
    .unwrap();
    batch.sign(&key).unwrap();
    (batch, key)
}

#[test]
fn valid_signed_owner_envelope_maps_without_payload_reencoding() {
    let (batch, _) = signed_batch();
    batch.validate().unwrap();
    let envelope = batch.semantic_envelope().unwrap();
    assert_eq!(envelope, batch.entries[0].owner_envelope);
    assert_eq!(envelope.payload, batch.entries[0].owner_envelope.payload);
    assert_eq!(batch.replacement_namespace().unwrap(), "general");
}

#[test]
fn strict_wire_rejects_unknown_trailing_family_schema_payload_signature_and_generations() {
    let (batch, key) = signed_batch();
    let mut json = serde_json::to_vec(&batch).unwrap();
    let mut unknown = json.clone();
    unknown.pop();
    unknown.extend_from_slice(b",\"unknown\":true}");
    assert!(SignedFactSupersedeBatchV1::decode_json(&unknown).is_err());
    json.extend_from_slice(b"{}\n");
    assert!(SignedFactSupersedeBatchV1::decode_json(&json).is_err());

    let (mut wrong_family, _) = signed_batch();
    wrong_family.protocol_family = "mnemes.fact-create.v1".into();
    wrong_family.sign(&key).unwrap_err();
    assert!(wrong_family.validate().is_err());

    let (mut wrong_schema, _) = signed_batch();
    wrong_schema.entries[0].owner_envelope.payload_schema = "semantic_memory.fact.create.v1".into();
    wrong_schema.sign(&key).unwrap();
    assert!(wrong_schema.validate().is_err());

    let (mut altered, _) = signed_batch();
    altered.entries[0].owner_envelope.payload[0] ^= 1;
    altered.sign(&key).unwrap();
    assert!(altered.validate().is_err());

    let (mut bad_signature, _) = signed_batch();
    bad_signature.signature[0] ^= 1;
    assert!(bad_signature.validate().is_err());

    let (mut predecessor, _) = signed_batch();
    predecessor.entries[0].owner_envelope.predecessor_digest[0] ^= 1;
    predecessor.sign(&key).unwrap();
    assert!(predecessor.validate().is_err());

    for zero in [0, 1, 2] {
        let (mut invalid, _) = signed_batch();
        match zero {
            0 => invalid.store_epoch = 0,
            1 => invalid.writer_epoch = 0,
            _ => invalid.owner_stream_epoch = 0,
        }
        assert!(invalid.validate().is_err());
    }
    assert_eq!(
        batch.protocol_family,
        FACT_SUPERSEDE_TRANSPORT_PROTOCOL_FAMILY
    );
}
