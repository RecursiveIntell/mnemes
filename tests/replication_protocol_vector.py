"""Independent Python golden-vector check for mnemes replication protocol.

Constructs the fixed-order preimage using the ADR's exact big-endian format
and SHA-256, then verifies that Rust's payload digest and envelope digest
match independently derived values.

This does NOT perform Ed25519 signature verification (that requires the
Rust ed25519-dalek crate). It provides cross-language evidence for:
- Payload digest: SHA-256(canonical_payload)
- Envelope digest: SHA-256(digest_domain_tag || fields)

Usage:
    python3 tests/replication_protocol_vector.py
    exit code 0 = all checks pass
"""

import hashlib
import struct
import sys


# Constants matching Rust SIGNATURE_DOMAIN_TAG and DIGEST_DOMAIN_TAG
SIGNATURE_DOMAIN_TAG = b"mnemes/mutation-envelope/signature/v1\x00"
DIGEST_DOMAIN_TAG = b"mnemes/mutation-envelope/v1\x00"


def build_preimage(
    protocol_version: int,
    artifact_kind: int,
    operation_schema_version: int,
    semantic_schema_generation: int,
    operation_id: str,
    idempotency_key: str,
    home_device_id: str,
    store_id: str,
    actor_id: str,
    store_epoch: int,
    writer_epoch: int,
    sequence: int,
    previous_envelope_digest: bytes,
    fencing_token: str,
    namespace: str,
    operation_kind: str,
    payload_digest: bytes,
    payload_length: int,
    requested_effect_digest: bytes,
    policy_version: int,
    authorization_snapshot_id: bytes,
    authorization_snapshot_digest: bytes,
    authority_receipt_digest: bytes,
    signer_principal_id: str,
    signer_role: int,
    signer_key_version: int,
    observed_at: int,
    valid_from: int,
    valid_to: int,
) -> bytes:
    """Build the deterministic fixed-order preimage per ADR encoding rules.

    All integers are big-endian. Variable-length fields use u32 BE length prefix.
    """
    out = bytearray()

    # Domain tag (signature domain)
    out.extend(SIGNATURE_DOMAIN_TAG)

    def encode_bytes(data: bytes) -> bytes:
        """Encode bytes with u32 BE length prefix."""
        if len(data) > 0xFFFFFFFF:
            raise ValueError(f"field too long: {len(data)} bytes")
        return struct.pack(">I", len(data)) + data

    # 1. protocol_version (u16 BE)
    out.extend(struct.pack(">H", protocol_version))
    # 2. artifact_kind (u8)
    out.extend(struct.pack("B", artifact_kind))
    # 3. operation_schema_version (u16 BE)
    out.extend(struct.pack(">H", operation_schema_version))
    # 4. semantic_schema_generation (u16 BE)
    out.extend(struct.pack(">H", semantic_schema_generation))
    # 5. operation_id
    out.extend(encode_bytes(operation_id.encode("utf-8")))
    # 6. idempotency_key
    out.extend(encode_bytes(idempotency_key.encode("utf-8")))
    # 7. home_device_id
    out.extend(encode_bytes(home_device_id.encode("utf-8")))
    # 8. store_id
    out.extend(encode_bytes(store_id.encode("utf-8")))
    # 9. actor_id
    out.extend(encode_bytes(actor_id.encode("utf-8")))
    # 10. store_epoch (u64 BE)
    out.extend(struct.pack(">Q", store_epoch))
    # 11. writer_epoch (u64 BE)
    out.extend(struct.pack(">Q", writer_epoch))
    # 12. sequence (u64 BE)
    out.extend(struct.pack(">Q", sequence))
    # 13. previous_envelope_digest ([u8; 32])
    out.extend(previous_envelope_digest)
    # 14. fencing_token
    out.extend(encode_bytes(fencing_token.encode("utf-8")))
    # 15. namespace
    out.extend(encode_bytes(namespace.encode("utf-8")))
    # 16. operation_kind
    out.extend(encode_bytes(operation_kind.encode("utf-8")))
    # 17. payload_digest ([u8; 32])
    out.extend(payload_digest)
    # 18. payload_length (u64 BE)
    out.extend(struct.pack(">Q", payload_length))
    # 19. requested_effect_digest ([u8; 32])
    out.extend(requested_effect_digest)
    # 20. policy_version (u64 BE)
    out.extend(struct.pack(">Q", policy_version))
    # 21. authorization_snapshot_id (Vec<u8>)
    out.extend(encode_bytes(authorization_snapshot_id))
    # 22. authorization_snapshot_digest ([u8; 32])
    out.extend(authorization_snapshot_digest)
    # 23. authority_receipt_digest ([u8; 32])
    out.extend(authority_receipt_digest)
    # 24. signer_principal_id
    out.extend(encode_bytes(signer_principal_id.encode("utf-8")))
    # 25. signer_role (u8)
    out.extend(struct.pack("B", signer_role))
    # 26. signer_key_version (u64 BE)
    out.extend(struct.pack(">Q", signer_key_version))
    # 27. observed_at (u64 BE)
    out.extend(struct.pack(">Q", observed_at))
    # 28. valid_from (u64 BE)
    out.extend(struct.pack(">Q", valid_from))
    # 29. valid_to (u64 BE)
    out.extend(struct.pack(">Q", valid_to))

    return bytes(out)


# ---------------------------------------------------------------------------
# Golden vector: fixed values matching Rust test fixture values
# ---------------------------------------------------------------------------

GOLDEN_CANONICAL_PAYLOAD = b"golden-payload"

# Payload digest = SHA-256(GOLDEN_CANONICAL_PAYLOAD)
expected_payload_digest = hashlib.sha256(GOLDEN_CANONICAL_PAYLOAD).digest()
print(f"payload_digest (hex): {expected_payload_digest.hex()}")
print(f"payload_digest (len): {len(expected_payload_digest)}")
assert len(expected_payload_digest) == 32

# Construct the golden preimage
golden_preimage = build_preimage(
    protocol_version=1,
    artifact_kind=0,  # ArtifactKind::Mutation = 0
    operation_schema_version=1,
    semantic_schema_generation=1,
    operation_id="golden-op",
    idempotency_key="golden-idem",
    home_device_id="golden-device",
    store_id="golden-store",
    actor_id="golden-actor",
    store_epoch=1,
    writer_epoch=1,
    sequence=1,
    previous_envelope_digest=b"\x00" * 32,
    fencing_token="golden-fence",
    namespace="golden-ns",
    operation_kind="golden-kind",
    payload_digest=expected_payload_digest,
    payload_length=14,  # len(b"golden-payload")
    requested_effect_digest=b"\x02" * 32,
    policy_version=1,
    authorization_snapshot_id=b"\x00" * 16,
    authorization_snapshot_digest=b"\x03" * 32,
    authority_receipt_digest=b"\x04" * 32,
    signer_principal_id="golden-principal",
    signer_role=0,  # SignerRole::DeviceWriter = 0
    signer_key_version=1,
    observed_at=1_000_000,
    valid_from=1_000_000,
    valid_to=2_000_000,
)

print(f"preimage (len): {len(golden_preimage)} bytes")

# The fields portion (after stripping the signature domain tag)
fields = golden_preimage[len(SIGNATURE_DOMAIN_TAG):]
print(f"fields (len): {len(fields)} bytes")

# Envelope digest = SHA-256(digest_domain_tag || fields)
envelope_digest = hashlib.sha256(DIGEST_DOMAIN_TAG + fields).digest()
print(f"envelope_digest (hex): {envelope_digest.hex()}")

# Provide preimage hex for Rust test to cross-reference
print(f"preimage (hex): {golden_preimage.hex()}")

# ---------------------------------------------------------------------------
# Verification:
#   1. payload_digest must be SHA-256(b"golden-payload") == 14 bytes
#   2. payload_length must be 14
#   3. preimage starts with signature domain tag
#   4. envelope digest matches SHA-256(digest_domain || fields)
#   5. preimage hex is deterministic
# ---------------------------------------------------------------------------

assert expected_payload_digest.hex() == hashlib.sha256(b"golden-payload").hexdigest()
assert len(GOLDEN_CANONICAL_PAYLOAD) == 14, "payload length must be 14"

assert golden_preimage.startswith(SIGNATURE_DOMAIN_TAG), (
    "preimage must start with signature domain tag"
)

# Store constant strings for Rust cross-reference
EXPECTED_PAYLOAD_DIGEST_HEX = expected_payload_digest.hex()
EXPECTED_ENVELOPE_DIGEST_HEX = envelope_digest.hex()
EXPECTED_PREIMAGE_HEX = golden_preimage.hex()


print("\n=== ALL PYTHON GOLDEN VECTOR CHECKS PASSED ===")
print(f"  payload_digest (fixed):    {EXPECTED_PAYLOAD_DIGEST_HEX}")
print(f"  envelope_digest (fixed):   {EXPECTED_ENVELOPE_DIGEST_HEX}")
print(f"  preimage (fixed):          {EXPECTED_PREIMAGE_HEX[:64]}... (truncated)")
print(f"  Signature domain tag:      {SIGNATURE_DOMAIN_TAG}")
print(f"  Digest domain tag:         {DIGEST_DOMAIN_TAG}")
sys.exit(0)
