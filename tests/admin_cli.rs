use serde_json::Value;
use std::process::Command;
use tempfile::TempDir;

#[test]
fn bootstrap_cli_accepts_documented_arguments_and_rejects_second_bootstrap() {
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
fn bootstrap_cli_accepts_default_actor_kind() {
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
