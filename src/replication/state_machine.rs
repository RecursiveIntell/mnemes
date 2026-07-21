use super::canonical::{canonical_digest, validate_envelope};
use super::error::ReplicationError;
use super::types::{same_identity, MemoryMutationEnvelopeV1};

/// Canonical owner of the ReplicaState enum.
///
/// Only this module defines ReplicaState and ReplicaWatermarkV1.
/// `types.rs` must NOT contain duplicate definitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaState {
    Bootstrapping,
    InstallingSnapshot,
    CatchingUp,
    Current,
    Lagging,
    OfflineUsable,
    AwaitingGap,
    GapDetected,
    Quarantined,
    /// Terminal: rejected at bootstrap or snapshot installation.
    Rejected,
    /// Terminal: retired (explicit operator action or promotion close).
    Retired,
}

impl ReplicaState {
    /// Returns `true` if `next` is a legal transition per the adopted
    /// state-machine contract.  Terminal states (`Rejected`, `Retired`)
    /// have no outgoing non-self transitions.
    pub fn can_transition_to(self, next: Self) -> bool {
        use ReplicaState::*;
        matches!(
            (self, next),
            // Terminal states — no transitions out
            (Rejected, Rejected) | (Retired, Retired)
                // Bootstrapping → next
                | (Bootstrapping, InstallingSnapshot | Quarantined | Rejected | Retired)
                // Installing snapshot → next
                | (InstallingSnapshot, CatchingUp | Quarantined | Rejected | Retired)
                // Catching up → next
                | (CatchingUp, Current | AwaitingGap | Quarantined | Retired)
                // Current → next
                | (
                    Current,
                    Lagging | OfflineUsable | InstallingSnapshot | Quarantined | Retired
                )
                // Lagging / OfflineUsable → next
                | (
                    Lagging | OfflineUsable,
                    Current | AwaitingGap | InstallingSnapshot | Quarantined | Retired
                )
                // Awaiting gap → next
                | (AwaitingGap, CatchingUp | GapDetected | Quarantined | Retired)
                // Gap detected → next
                | (GapDetected, InstallingSnapshot | Quarantined | Rejected | Retired)
                // Quarantined → next
                | (Quarantined, InstallingSnapshot | Rejected | Retired)
        )
    }
}

/// Canonical owner of ReplicaWatermarkV1.
///
/// Tracks the highest contiguous sequence and its head digest within one
/// writer epoch.  Sequence overflow is explicitly rejected rather than
/// silently wrapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaWatermarkV1 {
    pub sequence: u64,
    pub head_digest: [u8; 32],
}

impl ReplicaWatermarkV1 {
    pub fn new(sequence: u64, head_digest: [u8; 32]) -> Self {
        Self {
            sequence,
            head_digest,
        }
    }

    /// Returns `true` if the proposed `sequence` and `previous` digest
    /// form a valid next hop from this watermark.
    ///
    /// Returns `false` (never panics) for:
    /// - sequence overflow (u64::MAX + 1)
    /// - wrong predecessor digest
    /// - non-contiguous sequence number
    pub fn accepts_next(&self, sequence: u64, previous: [u8; 32]) -> bool {
        // Reject overflow: sequence must be strictly greater than self.sequence
        if sequence <= self.sequence {
            return false;
        }
        // Reject gaps: must be exactly +1
        match self.sequence.checked_add(1) {
            Some(next) if sequence == next => {}
            _ => return false,
        }
        // Reject wrong predecessor
        previous == self.head_digest
    }
}

/// Validate identity collision between two envelopes.
///
/// Both envelopes are validated structurally and cryptographically
/// *before* identity comparison.  If either fails validation the
/// result is an error even if identities would match.
pub fn validate_identity_collision(
    first: &MemoryMutationEnvelopeV1,
    second: &MemoryMutationEnvelopeV1,
) -> Result<(), ReplicationError> {
    // Validate both envelopes first (structural + cryptographic)
    validate_envelope(first)
        .map_err(|e| ReplicationError::PreCollisionValidation(format!("first envelope: {e}")))?;
    validate_envelope(second)
        .map_err(|e| ReplicationError::PreCollisionValidation(format!("second envelope: {e}")))?;

    if !same_identity(first, second) {
        return Ok(());
    }
    // Compare canonical envelope digest, not signing preimage bytes.
    // Same scoped identity + same digest = idempotent duplicate.
    // Same scoped identity + different digest = typed conflict/fork evidence.
    let first_digest = canonical_digest(first)?;
    let second_digest = canonical_digest(second)?;
    if first_digest == second_digest {
        Ok(())
    } else {
        Err(ReplicationError::IdentityCollision)
    }
}
