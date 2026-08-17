//! mnemes-sync-client — device-side typed replication driver.
//!
//! Exports one verified `mutation_journal` record from the local semantic-memory
//! store, wraps it in a `SignedFactCreateBatchV1`, POSTs it to the authority's
//! `/v1/replication/fact-create/v1`, and advances a local watermark ONLY on a
//! typed `Applied`/`Duplicate` ACK (per typed-replication-operations Phase 6).
//!
//! One journal record per invocation (`--once`); the caller (hook/timer) decides
//! cadence. Never advances on transport/auth/admission errors.

use ed25519_dalek::{SigningKey, VerifyingKey};
use mnemes::replication::{FactCreateTransportEntryV1, SignedFactCreateBatchV1};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

const WIRE: &str = "/v1/replication/fact-create/v1";
const OP_FACT_CREATE: &str = "fact.create";
const SCHEMA_FACT_CREATE: &str = "semantic_memory.fact.create.v1";
const RECORD_VERIFIED: &str = "verified_v1";

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Watermark {
    protocol_version: u16,
    home_device_id: String,
    store_id: String,
    stream_epoch: u64,
    next_sequence: i64,
    last_batch_id: Option<String>,
    last_disposition: Option<String>,
    updated_at: String,
}

#[derive(Deserialize, Debug)]
struct Ack {
    protocol: String,
    batch_id: String,
    #[serde(default)]
    request_digest: Option<String>,
    home_device_id: String,
    store_id: String,
    stream_epoch: u64,
    accepted_head: i64,
    disposition: String,
}

#[derive(Debug)]
enum ClientError {
    Msg(String),
}
impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::Msg(m) => write!(f, "{m}"),
        }
    }
}
impl std::error::Error for ClientError {}
fn err<T>(m: impl Into<String>) -> Result<T, ClientError> {
    Err(ClientError::Msg(m.into()))
}

fn load_seed(path: &Path) -> Result<[u8; 32], ClientError> {
    let raw = fs::read_to_string(path).map_err(|e| ClientError::Msg(format!("key read: {e}")))?;
    let trimmed = raw.trim();
    if let Ok(v) = hex::decode(trimmed) {
        if v.len() == 32 {
            let mut out = [0u8; 32];
            out.copy_from_slice(&v);
            return Ok(out);
        }
    }
    let bytes = trimmed.as_bytes();
    if bytes.len() == 32 {
        let mut out = [0u8; 32];
        out.copy_from_slice(bytes);
        return Ok(out);
    }
    err("key file must be 32-byte seed as hex or raw bytes")
}

fn load_credential(env_file: &Path) -> Result<String, ClientError> {
    let text = fs::read_to_string(env_file)
        .map_err(|e| ClientError::Msg(format!("credential env read: {e}")))?;
    for line in text.lines() {
        let line = line.trim();
        if let Some(v) = line.strip_prefix("MNEME_CREDENTIAL=") {
            let v = v.trim().trim_matches('"').trim_matches('\'');
            if v.is_empty() {
                return err("MNEME_CREDENTIAL is empty");
            }
            return Ok(v.to_string());
        }
    }
    err("MNEME_CREDENTIAL not found in env file")
}

fn read_journal_entry(
    conn: &Connection,
    device: &str,
    store: &str,
    epoch: u64,
    after_sequence: i64,
) -> Result<Option<(i64, Vec<u8>, [u8; 32], [u8; 32], [u8; 32])>, ClientError> {
    let mut stmt = conn
        .prepare(
            "SELECT sequence, payload, payload_digest, predecessor_digest, envelope_digest, \
             operation_kind, payload_schema, record_state \
             FROM mutation_journal \
             WHERE home_device_id = ?1 AND store_id = ?2 AND stream_epoch = ?3 AND sequence >= ?4 \
             ORDER BY sequence ASC LIMIT 1",
        )
        .map_err(|e| ClientError::Msg(format!("journal query: {e}")))?;
    let mut rows = stmt
        .query(rusqlite::params![device, store, epoch as i64, after_sequence])
        .map_err(|e| ClientError::Msg(format!("journal query: {e}")))?;
    let Some(row) = rows.next().map_err(|e| ClientError::Msg(format!("row: {e}")))?
    else {
        return Ok(None);
    };
    let seq: i64 = row.get(0).map_err(|e| ClientError::Msg(format!("seq: {e}")))?;
    let payload: Vec<u8> = row.get(1).map_err(|e| ClientError::Msg(format!("payload: {e}")))?;
    let op: String = row.get(5).map_err(|e| ClientError::Msg(format!("op: {e}")))?;
    let schema: String = row.get(6).map_err(|e| ClientError::Msg(format!("schema: {e}")))?;
    let state: String = row.get(7).map_err(|e| ClientError::Msg(format!("state: {e}")))?;
    if op != OP_FACT_CREATE || schema != SCHEMA_FACT_CREATE || state != RECORD_VERIFIED {
        return err(format!(
            "journal row {seq} is not a verified fact-create record (op={op}, schema={schema}, state={state})"
        ));
    }
    let d = |idx: usize| -> Result<[u8; 32], ClientError> {
        let v: Vec<u8> = row.get(idx).map_err(|e| ClientError::Msg(format!("digest: {e}")))?;
        let mut out = [0u8; 32];
        if v.len() != 32 {
            return err(format!("digest col {idx} length {}", v.len()));
        }
        out.copy_from_slice(&v);
        Ok(out)
    };
    let payload_digest = d(2)?;
    let predecessor_digest = d(3)?;
    let envelope_digest = d(4)?;
    Ok(Some((seq, payload, payload_digest, predecessor_digest, envelope_digest)))
}

fn write_watermark(path: &Path, wm: &Watermark) -> Result<(), ClientError> {
    let tmp = path.with_extension("tmp");
    let json = serde_json::to_string_pretty(wm)
        .map_err(|e| ClientError::Msg(format!("wm serialize: {e}")))?;
    let mut f = fs::File::create(&tmp).map_err(|e| ClientError::Msg(format!("wm write: {e}")))?;
    f.write_all(json.as_bytes())
        .map_err(|e| ClientError::Msg(format!("wm write: {e}")))?;
    fs::set_permissions(&tmp, fs::Permissions::from_mode(0o600))
        .map_err(|e| ClientError::Msg(format!("wm chmod: {e}")))?;
    fs::rename(&tmp, path).map_err(|e| ClientError::Msg(format!("wm rename: {e}")))?;
    Ok(())
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn main() -> Result<(), ClientError> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut db = None;
    let mut device = None;
    let mut store = None;
    let mut epoch: Option<u64> = None;
    let mut key = None;
    let mut principal = None;
    let mut key_version: Option<u64> = None;
    let mut fencing = None;
    let mut url = None;
    let mut credential_env = None;
    let mut watermark: Option<PathBuf> = None;
    let mut gen_key = None;
    let mut observed_at: Option<u64> = None;

    let mut i = 0;
    while i < args.len() {
        let a = args[i].as_str();
        let need = |idx: usize| -> String {
            args.get(idx + 1).cloned().unwrap_or_else(|| {
                eprintln!("missing value for {a}");
                std::process::exit(2);
            })
        };
        match a {
            "--db" => { db = Some(need(i)); i += 2; }
            "--home-device-id" => { device = Some(need(i)); i += 2; }
            "--store-id" => { store = Some(need(i)); i += 2; }
            "--stream-epoch" => { epoch = need(i).parse().ok(); i += 2; }
            "--key" => { key = Some(need(i)); i += 2; }
            "--principal" => { principal = Some(need(i)); i += 2; }
            "--key-version" => { key_version = need(i).parse().ok(); i += 2; }
            "--fencing-token" => { fencing = Some(need(i)); i += 2; }
            "--url" => { url = Some(need(i)); i += 2; }
            "--credential-file" => { credential_env = Some(need(i)); i += 2; }
            "--watermark" => { watermark = Some(PathBuf::from(need(i))); i += 2; }
            "--observed-at" => { observed_at = need(i).parse().ok(); i += 2; }
            "--gen-key" => { gen_key = Some(need(i)); i += 2; }
            h => {
                println!("usage: mnemes-sync-client [--gen-key <path>] | [--db .. --home-device-id .. --store-id .. --stream-epoch N --key .. --principal .. --key-version N --fencing-token .. --url .. [--credential-file ..] [--watermark ..] [--observed-at N]]");
                println!("unknown arg: {h}");
                std::process::exit(2);
            }
        }
    }

    if let Some(kpath) = gen_key {
        use rand::RngCore;
        let mut seed = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut seed);
        let sk = SigningKey::from_bytes(&seed);
        let vk: VerifyingKey = sk.verifying_key();
        let mut f = fs::File::create(&kpath)
            .map_err(|e| ClientError::Msg(format!("key write: {e}")))?;
        f.write_all(hex::encode(&seed).as_bytes())
            .map_err(|e| ClientError::Msg(format!("key write: {e}")))?;
        fs::set_permissions(&kpath, fs::Permissions::from_mode(0o600))
            .map_err(|e| ClientError::Msg(format!("key chmod: {e}")))?;
        println!("{}", hex::encode(vk.to_bytes()));
        return Ok(());
    }

    let db = db.ok_or_else(|| ClientError::Msg("--db required".into()))?;
    let device = device.ok_or_else(|| ClientError::Msg("--home-device-id required".into()))?;
    let store = store.ok_or_else(|| ClientError::Msg("--store-id required".into()))?;
    let epoch = epoch.ok_or_else(|| ClientError::Msg("--stream-epoch required".into()))?;
    let key = key.ok_or_else(|| ClientError::Msg("--key required".into()))?;
    let principal = principal.ok_or_else(|| ClientError::Msg("--principal required".into()))?;
    let key_version =
        key_version.ok_or_else(|| ClientError::Msg("--key-version required".into()))?;
    let fencing = fencing.ok_or_else(|| ClientError::Msg("--fencing-token required".into()))?;
    let url = url.ok_or_else(|| ClientError::Msg("--url required".into()))?.trim_end_matches('/').to_string();
    let credential_env = credential_env.unwrap_or_else(|| {
        format!("{}/.config/mnemes/client.env", std::env::var("HOME").unwrap_or_default())
    });
    let watermark = watermark.unwrap_or_else(|| {
        PathBuf::from(format!(
            "{}/.local/state/mnemes/fact-create-watermark.json",
            std::env::var("HOME").unwrap_or_default()
        ))
    });

    let seed = load_seed(Path::new(&key))?;
    let signing_key = SigningKey::from_bytes(&seed);
    let credential = load_credential(Path::new(&credential_env))?;

    let wm: Watermark = if watermark.exists() {
        let text = fs::read_to_string(&watermark)
            .map_err(|e| ClientError::Msg(format!("watermark read: {e}")))?;
        let w: Watermark = serde_json::from_str(&text)
            .map_err(|e| ClientError::Msg(format!("watermark parse: {e}")))?;
        if w.home_device_id != device || w.store_id != store || w.stream_epoch != epoch {
            return err("watermark identity mismatch — refusing to advance a foreign stream");
        }
        w
    } else {
        Watermark {
            protocol_version: 1,
            home_device_id: device.clone(),
            store_id: store.clone(),
            stream_epoch: epoch,
            next_sequence: 0,
            last_batch_id: None,
            last_disposition: None,
            updated_at: String::new(),
        }
    };

    let conn = Connection::open_with_flags(
        &db,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|e| ClientError::Msg(format!("db open: {e}")))?;

    let Some((seq, payload, payload_digest, predecessor_digest, envelope_digest)) =
        read_journal_entry(&conn, &device, &store, epoch, wm.next_sequence)?
    else {
        let out = serde_json::json!({
            "ok": true, "action": "up_to_date", "next_sequence": wm.next_sequence
        });
        println!("{out}");
        return Ok(());
    };

    let entry = FactCreateTransportEntryV1 {
        sequence: seq,
        payload,
        payload_digest,
        predecessor_digest,
        journal_envelope_digest: envelope_digest,
    };

    let batch_id = uuid::Uuid::new_v4().to_string();
    let mut batch = SignedFactCreateBatchV1::new(
        batch_id.clone(),
        device.clone(),
        store.clone(),
        epoch,
        seq,
        vec![entry],
        principal.clone(),
        key_version,
        observed_at.unwrap_or_else(now_unix),
        fencing.clone(),
    )
    .map_err(|e| ClientError::Msg(format!("batch build: {e}")))?;
    batch
        .sign(&signing_key)
        .map_err(|e| ClientError::Msg(format!("sign: {e}")))?;

    let body = serde_json::to_vec(&batch)
        .map_err(|e| ClientError::Msg(format!("serialize: {e}")))?;

    // async HTTP via tokio
    let ack: Ack = tokio::runtime::Runtime::new()
        .map_err(|e| ClientError::Msg(format!("runtime: {e}")))?
        .block_on(async {
            let http = reqwest::Client::new();
            let resp = http
                .post(format!("{url}{WIRE}"))
                .bearer_auth(&credential)
                .header("content-type", "application/json")
                .body(body.clone())
                .send()
                .await
                .map_err(|e| ClientError::Msg(format!("http: {e}")))?;
            let status = resp.status();
            let text = resp
                .text()
                .await
                .map_err(|e| ClientError::Msg(format!("body: {e}")))?;
            if status.is_success() {
                serde_json::from_str(&text)
                    .map_err(|e| ClientError::Msg(format!("ack parse: {e} — {text}")))
            } else {
                Err(ClientError::Msg(format!(
                    "HTTP {status}: {text}"
                )))
            }
        })?;

    if ack.disposition != "accepted" {
        return err(format!("unexpected disposition {}", ack.disposition));
    }
    if ack.batch_id != batch_id {
        return err("ack batch_id mismatch");
    }
    let next_sequence = ack.accepted_head + 1;
    let wm2 = Watermark {
        next_sequence,
        last_batch_id: Some(batch_id),
        last_disposition: Some(ack.disposition.clone()),
        updated_at: chrono::Utc::now().to_rfc3339(),
        ..wm
    };
    write_watermark(&watermark, &wm2)?;
    let out = serde_json::json!({
        "ok": true,
        "action": "synced",
        "sequence": seq,
        "accepted_head": ack.accepted_head,
        "disposition": ack.disposition,
        "next_sequence": next_sequence,
        "store_id": store,
        "stream_epoch": epoch,
    });
    println!("{out}");
    Ok(())
}
