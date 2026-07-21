//! Integration test: device-owned replication vertical slice.
//!
//! Exercises the full pipeline:
//!   1. Journal a mutation on a device primary
//!   2. Export the journal
//!   3. Sync to a server replica
//!   4. Verify idempotent replay

use mnemes::replica::{ApplyOutcome, ReplicaApplier};
use mnemes::sync;
use rusqlite::Connection;

/// Create an in-memory database with the mutation_journal table.
fn device_db() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE mutation_journal (
            journal_id INTEGER PRIMARY KEY AUTOINCREMENT,
            home_device_id TEXT NOT NULL,
            store_id TEXT NOT NULL,
            sequence INTEGER NOT NULL,
            operation_kind TEXT NOT NULL,
            payload BLOB NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE UNIQUE INDEX idx_journal_seq ON mutation_journal(home_device_id, store_id, sequence);
        ",
    )
    .unwrap();
    conn
}

/// Create a file-backed database in a temp directory that stays alive for the duration.
fn replica_db() -> (Connection, tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("replica.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
        "CREATE TABLE mutation_journal (
            journal_id INTEGER PRIMARY KEY AUTOINCREMENT,
            home_device_id TEXT NOT NULL,
            store_id TEXT NOT NULL,
            sequence INTEGER NOT NULL,
            operation_kind TEXT NOT NULL,
            payload BLOB NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE UNIQUE INDEX idx_journal_seq ON mutation_journal(home_device_id, store_id, sequence);
        ",
    )
    .unwrap();
    (conn, dir, path)
}

#[test]
fn canonical_operation_journal_fails_closed_without_payload() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE operation_journal (
            operation_id TEXT PRIMARY KEY,
            caller_idempotency_key TEXT NOT NULL UNIQUE,
            operation_kind TEXT NOT NULL,
            payload_digest TEXT NOT NULL,
            principal TEXT NOT NULL,
            caller_id TEXT NOT NULL,
            before_epoch INTEGER NOT NULL,
            after_epoch INTEGER NOT NULL,
            affected_ids_json TEXT NOT NULL,
            content_digest TEXT NOT NULL,
            committed_at TEXT NOT NULL
        )",
    )
    .unwrap();

    let error = sync::export_operation_journal(&conn, "device-1", "store-1", 1, 10)
        .expect_err("metadata-only canonical journal must not fabricate replay bytes");
    assert!(error.to_string().contains("no replayable payload"));
}

#[test]
fn export_empty_journal() {
    let conn = device_db();
    let (entries, next_seq, has_more) =
        sync::export_device_journal(&conn, "device-1", "store-1", 1, 10).unwrap();
    assert!(entries.is_empty());
    assert_eq!(next_seq, 1);
    assert!(!has_more);
}

#[test]
fn export_after_append() {
    let conn = device_db();
    conn.execute(
        "INSERT INTO mutation_journal (home_device_id, store_id, sequence, operation_kind, payload)
         VALUES ('device-1', 'store-1', 1, 'add_fact', ?1)",
        [b"hello"],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO mutation_journal (home_device_id, store_id, sequence, operation_kind, payload)
         VALUES ('device-1', 'store-1', 2, 'add_fact', ?1)",
        [b"world"],
    )
    .unwrap();

    let (entries, next_seq, has_more) =
        sync::export_device_journal(&conn, "device-1", "store-1", 1, 10).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].sequence, 1);
    assert_eq!(entries[0].operation_kind, "add_fact");
    assert_eq!(entries[0].payload, b"hello");
    assert_eq!(entries[1].sequence, 2);
    assert_eq!(next_seq, 3);
    assert!(!has_more);
}

#[test]
fn export_detects_gap() {
    let conn = device_db();
    conn.execute(
        "INSERT INTO mutation_journal (home_device_id, store_id, sequence, operation_kind, payload)
         VALUES ('device-1', 'store-1', 1, 'add_fact', ?1)",
        [b"seq1"],
    )
    .unwrap();
    // Insert seq 3, skipping seq 2
    conn.execute(
        "INSERT INTO mutation_journal (home_device_id, store_id, sequence, operation_kind, payload)
         VALUES ('device-1', 'store-1', 3, 'add_fact', ?1)",
        [b"seq3"],
    )
    .unwrap();

    // Export from seq 1: should only get seq 1, detecting gap
    let (entries, next_seq, has_more) =
        sync::export_device_journal(&conn, "device-1", "store-1", 1, 10).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].sequence, 1);
    assert_eq!(next_seq, 2);
    assert!(!has_more, "gap must set has_more=false");
}

#[test]
fn apply_idempotent_replay() {
    let (_conn, _dir, replica_path) = replica_db();
    let applier = ReplicaApplier::new(&replica_path, "device-1", "store-1");

    let call_count = std::cell::Cell::new(0);

    // First apply — should succeed
    let result = applier
        .apply_entry(1, "add_fact", b"data", &|_c| {
            call_count.set(call_count.get() + 1);
            Ok(())
        })
        .unwrap();
    assert_eq!(result, ApplyOutcome::Applied { sequence: 1 });
    assert_eq!(call_count.get(), 1);

    // Same sequence — should be already applied
    let result = applier
        .apply_entry(1, "add_fact", b"data", &|_c| {
            call_count.set(call_count.get() + 1);
            Ok(())
        })
        .unwrap();
    assert_eq!(result, ApplyOutcome::AlreadyApplied { sequence: 1 });
    assert_eq!(call_count.get(), 1, "replay fn must not be called again");
}

#[test]
fn failed_replay_is_atomic_and_retryable() {
    let (_conn, _dir, replica_path) = replica_db();
    let applier = ReplicaApplier::new(&replica_path, "device-1", "store-1");

    let error = applier
        .apply_entry(1, "add_fact", b"payload-1", &|conn| {
            conn.execute_batch("CREATE TABLE replay_side_effect (id INTEGER);")
                .map_err(|e| mnemes::MnemesError::Replication(e.to_string()))?;
            Err(mnemes::MnemesError::Replication(
                "synthetic replay failure".into(),
            ))
        })
        .expect_err("failed replay must be returned");
    assert!(error.to_string().contains("synthetic replay failure"));

    let conn = Connection::open(&replica_path).unwrap();
    let journal_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM mutation_journal", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(journal_count, 0, "failed replay must not publish an ACK");
    let side_effect_exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='replay_side_effect')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!side_effect_exists, "replay side effects must roll back");

    let outcome = applier
        .apply_entry(1, "add_fact", b"payload-1", &|_| Ok(()))
        .unwrap();
    assert_eq!(outcome, ApplyOutcome::Applied { sequence: 1 });
}

#[test]
fn same_sequence_with_changed_payload_is_rejected() {
    let (_conn, _dir, replica_path) = replica_db();
    let applier = ReplicaApplier::new(&replica_path, "device-1", "store-1");
    applier
        .apply_entry(1, "add_fact", b"original", &|_| Ok(()))
        .unwrap();

    let error = applier
        .apply_entry(1, "add_fact", b"tampered", &|_| Ok(()))
        .expect_err("same sequence with different bytes must fail closed");
    assert!(error.to_string().contains("payload mismatch"));
}

#[test]
fn full_sync_cycle() {
    let device_conn = device_db();
    let (_replica_conn, _dir, replica_path) = replica_db();
    let applier = ReplicaApplier::new(&replica_path, "device-1", "store-1");

    // Seed some journal entries on the device
    for i in 1..=5 {
        device_conn
            .execute(
                "INSERT INTO mutation_journal
                 (home_device_id, store_id, sequence, operation_kind, payload)
                 VALUES ('device-1', 'store-1', ?1, 'add_fact', ?2)",
                rusqlite::params![i, format!("payload-{i}").as_bytes()],
            )
            .unwrap();
    }

    // Check pending
    assert!(sync::has_pending(&device_conn, "device-1", "store-1", 0).unwrap());
    assert!(!sync::has_pending(&device_conn, "device-1", "store-1", 5).unwrap());

    // Export the batch directly (skip admission for this test)
    let (entries, next_seq, has_more) =
        sync::export_device_journal(&device_conn, "device-1", "store-1", 1, 10).unwrap();
    assert_eq!(entries.len(), 5);
    assert_eq!(next_seq, 6);
    assert!(!has_more);

    // Apply each entry via the applier
    for entry in &entries {
        let outcome = applier
            .apply_entry(
                entry.sequence,
                &entry.operation_kind,
                &entry.payload,
                &|_conn| Ok(()),
            )
            .unwrap();
        assert_eq!(
            outcome,
            ApplyOutcome::Applied {
                sequence: entry.sequence
            }
        );
    }

    // Re-export — should be empty (already synced)
    let (entries2, _, _) =
        sync::export_device_journal(&device_conn, "device-1", "store-1", 1, 10).unwrap();
    assert_eq!(entries2.len(), 5); // entries are always exportable from device db

    // Re-apply should be idempotent
    let outcome = applier
        .apply_entry(1, "add_fact", b"payload-1", &|_conn| Ok(()))
        .unwrap();
    assert_eq!(outcome, ApplyOutcome::AlreadyApplied { sequence: 1 });
}
