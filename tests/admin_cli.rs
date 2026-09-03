use mnemes::{DeviceId, FactSupersedeAdmission, MnemesStore};
use rusqlite::Connection;
use semantic_memory::{MemoryConfig, MockEmbedder};
use serde_json::Value;
use std::process::Command;
use std::sync::{Mutex, MutexGuard};
use tempfile::TempDir;

/// The CLI tests below spawn the real `mnemes-admin` binary. Each one
/// initializes its own SQLite store plus key material, and running them
/// concurrently on a loaded or cold-cache machine starves those inits:
/// `bootstrap` exits non-zero intermittently and the bare
/// `assert!(status.success())` reports only the exit code, which is how
/// this arrived as an unexplained CI failure (mnemes PR #3, run
/// 33723242946). Serialize them behind one process-wide lock.
static CLI_SERIAL: Mutex<()> = Mutex::new(());

fn cli_serial() -> MutexGuard<'static, ()> {
    // A poisoned lock means some other test panicked while holding it.
    // The serialization guarantee still holds, so recover rather than
    // cascade a confusing poisoning panic across unrelated tests.
    CLI_SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Run a CLI invocation and assert success, printing stdout/stderr on
/// failure so a non-zero exit is diagnosable from the CI log.
fn assert_cli_ok(output: &std::process::Output, what: &str) {
    assert!(
        output.status.success(),
        "{what} failed with {:?}\n--- stdout ---\n{}\n--- stderr ---\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

#[test]
fn bootstrap_cli_accepts_documented_arguments_and_rejects_second_bootstrap() {
    let _serial = cli_serial();
    let dir = TempDir::new().unwrap();
    let bin = env!("CARGO_BIN_EXE_mnemes-admin");

    let first = Command::new(bin)
        .args([
            "bootstrap",
            dir.path().to_str().unwrap(),
            "msi-test",
            "linux",
            "msi",
            "service",
        ])
        .output()
        .unwrap();
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let value: Value = serde_json::from_slice(&first.stdout).unwrap();
    assert_eq!(value["profile"], "operator");
    assert!(value["device_id"].is_string());
    assert!(value["actor_id"].is_string());
    assert!(value["credential"].is_string());

    let second = Command::new(bin)
        .args([
            "bootstrap",
            dir.path().to_str().unwrap(),
            "duplicate",
            "linux",
            "msi",
        ])
        .output()
        .unwrap();
    assert!(!second.status.success());
    assert!(String::from_utf8_lossy(&second.stderr).contains("bootstrap failed"));
}

#[test]
fn fact_create_admit_cli_persists_public_key_without_echoing_it() {
    let _serial = cli_serial();
    let dir = TempDir::new().unwrap();
    let bin = env!("CARGO_BIN_EXE_mnemes-admin");
    let bootstrap = Command::new(bin)
        .args([
            "bootstrap",
            dir.path().to_str().unwrap(),
            "test",
            "linux",
            "host",
        ])
        .output()
        .unwrap();
    assert_cli_ok(&bootstrap, "bootstrap");
    let device_id = serde_json::from_slice::<Value>(&bootstrap.stdout).unwrap()["device_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let public_key = "ab".repeat(32);
    let output = Command::new(bin)
        .args([
            "fact-create-admit",
            dir.path().to_str().unwrap(),
            &device_id,
            "store-a",
            "ns-a",
            "principal-a",
            "1",
            &public_key,
            "100",
            "200",
            "7",
            "fence-a",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["device_id"], device_id);
    assert_eq!(value["store_id"], "store-a");
    assert_eq!(value["namespace"], "ns-a");
    assert_eq!(value["principal_id"], "principal-a");
    assert_eq!(value["key_version"], 1);
    assert_eq!(value["stream_epoch"], 7);
    assert_eq!(value["fencing_token"], "fence-a");
    assert!(!String::from_utf8_lossy(&output.stdout).contains(&public_key));
    let conn = Connection::open(dir.path().join("pooled.db")).unwrap();
    let row: (String, String, String, i64, Vec<u8>, i64, String) = conn.query_row(
        "SELECT store_id,namespace,principal_id,stream_epoch,public_key,cutoff_at,fencing_token FROM fact_create_admissions",
        [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?))).unwrap();
    assert_eq!(
        (row.0, row.1, row.2, row.3, row.5, row.6),
        (
            "store-a".into(),
            "ns-a".into(),
            "principal-a".into(),
            7,
            200,
            "fence-a".into()
        )
    );
    assert_eq!(row.4, vec![0xab; 32]);
}

#[test]
fn fact_create_admit_cli_rejects_invalid_key_before_store_open() {
    let _serial = cli_serial();
    let dir = TempDir::new().unwrap();
    let valid_device_id = "11111111-1111-4111-8111-111111111111";
    let output = Command::new(env!("CARGO_BIN_EXE_mnemes-admin"))
        .args([
            "fact-create-admit",
            dir.path().to_str().unwrap(),
            valid_device_id,
            "store",
            "ns",
            "principal",
            "1",
            "zz",
            "100",
            "200",
            "1",
            "fence",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid public key hex"));
    assert!(!dir.path().join("pooled.db").exists());

    let short = Command::new(env!("CARGO_BIN_EXE_mnemes-admin"))
        .args([
            "fact-create-admit",
            dir.path().to_str().unwrap(),
            valid_device_id,
            "store",
            "ns",
            "principal",
            "1",
            "ab",
            "100",
            "200",
            "1",
            "fence",
        ])
        .output()
        .unwrap();
    assert!(!short.status.success());
    assert!(String::from_utf8_lossy(&short.stderr).contains("public key must be exactly 32 bytes"));
    assert!(!dir.path().join("pooled.db").exists());
}

#[test]
fn fact_create_revoke_cli_marks_existing_test_admission_revoked() {
    let _serial = cli_serial();
    let dir = TempDir::new().unwrap();
    let bin = env!("CARGO_BIN_EXE_mnemes-admin");
    let bootstrap = Command::new(bin)
        .args([
            "bootstrap",
            dir.path().to_str().unwrap(),
            "test",
            "linux",
            "host",
        ])
        .output()
        .unwrap();
    assert_cli_ok(&bootstrap, "bootstrap");
    let device_id = serde_json::from_slice::<Value>(&bootstrap.stdout).unwrap()["device_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let admitted = Command::new(bin)
        .args([
            "fact-create-admit",
            dir.path().to_str().unwrap(),
            &device_id,
            "store-revoke",
            "ns-revoke",
            "principal-revoke",
            "2",
            &"cd".repeat(32),
            "0",
            "200",
            "9",
            "fence-revoke",
        ])
        .output()
        .unwrap();
    assert_cli_ok(&admitted, "admitted");
    let revoked = Command::new(bin)
        .args([
            "fact-create-revoke",
            dir.path().to_str().unwrap(),
            &device_id,
            "store-revoke",
            "ns-revoke",
            "principal-revoke",
            "2",
        ])
        .output()
        .unwrap();
    assert!(
        revoked.status.success(),
        "{}",
        String::from_utf8_lossy(&revoked.stderr)
    );
    let value: Value = serde_json::from_slice(&revoked.stdout).unwrap();
    assert_eq!(value["device_id"], device_id);
    assert_eq!(value["store_id"], "store-revoke");
    assert_eq!(value["namespace"], "ns-revoke");
    assert_eq!(value["principal_id"], "principal-revoke");
    assert_eq!(value["key_version"], 2);
    let conn = Connection::open(dir.path().join("pooled.db")).unwrap();
    let revoked: i64 = conn
        .query_row("SELECT revoked FROM fact_create_admissions", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(revoked, 1);
}

#[tokio::test]
async fn fact_supersede_revoke_cli_marks_store_admission_revoked_without_key_material() {
    let dir = TempDir::new().unwrap();
    let bin = env!("CARGO_BIN_EXE_mnemes-admin");
    // Scoped so the guard is dropped before the first await below: clippy
    // rejects a std MutexGuard held across an await point
    // (clippy::await_holding_lock), and the guarded work here is only the
    // synchronous CLI spawn.
    let device_id = {
        let _serial = cli_serial();
        let bootstrap = Command::new(bin)
            .args([
                "bootstrap",
                dir.path().to_str().unwrap(),
                "test",
                "linux",
                "host",
            ])
            .output()
            .unwrap();
        assert_cli_ok(&bootstrap, "bootstrap");
        serde_json::from_slice::<Value>(&bootstrap.stdout).unwrap()["device_id"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    let store = MnemesStore::open_with_embedder(
        dir.path().to_path_buf(),
        MemoryConfig {
            base_dir: dir.path().to_path_buf(),
            ..Default::default()
        },
        Box::new(MockEmbedder::new(768)),
    )
    .unwrap();
    store
        .admit_fact_supersede_key(FactSupersedeAdmission {
            device_id: DeviceId::parse(&device_id).unwrap(),
            store_id: "store-revoke".into(),
            replacement_namespace: "ns-revoke".into(),
            principal_id: "principal-revoke".into(),
            key_version: 2,
            public_key: [0xcd; 32],
            activated_at: 0,
            cutoff_at: 200,
            store_epoch: 3,
            writer_epoch: 4,
            fencing_token: "fence-revoke".into(),
        })
        .await
        .unwrap();
    let revoked = Command::new(bin)
        .args([
            "fact-supersede-revoke",
            dir.path().to_str().unwrap(),
            &device_id,
            "store-revoke",
            "ns-revoke",
            "principal-revoke",
            "2",
        ])
        .output()
        .unwrap();
    assert!(
        revoked.status.success(),
        "{}",
        String::from_utf8_lossy(&revoked.stderr)
    );
    let output = String::from_utf8(revoked.stdout).unwrap();
    let value: Value = serde_json::from_str(&output).unwrap();
    assert_eq!(value["device_id"], device_id);
    assert_eq!(value["replacement_namespace"], "ns-revoke");
    assert_eq!(value["principal_id"], "principal-revoke");
    assert_eq!(value["key_version"], 2);
    assert!(value["revoked"].as_bool().unwrap());
    assert!(!output.contains(&"cd".repeat(32)));
    let conn = Connection::open(dir.path().join("pooled.db")).unwrap();
    let revoked: i64 = conn
        .query_row(
            "SELECT revoked FROM fact_supersede_admissions \
             WHERE device_id=?1 AND store_id=?2 AND replacement_namespace=?3 \
               AND principal_id=?4 AND key_version=?5",
            [
                &device_id,
                "store-revoke",
                "ns-revoke",
                "principal-revoke",
                "2",
            ],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(revoked, 1);
}

#[test]
fn bootstrap_cli_accepts_default_actor_kind() {
    let _serial = cli_serial();
    let dir = TempDir::new().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_mnemes-admin"))
        .args([
            "bootstrap",
            dir.path().to_str().unwrap(),
            "laptop-test",
            "linux",
            "laptop",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
