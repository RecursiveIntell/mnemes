use mnemes::{ActorKind, Device, DeviceId, FactCreateAdmission, MnemesStore};
use semantic_memory::MemoryConfig;
use serde_json::json;
use std::env;
use std::path::PathBuf;

fn usage() -> ! {
    eprintln!(
        "usage: mnemes-admin bootstrap <DATA_DIR> <LABEL> <PLATFORM> <HOSTNAME> [ACTOR_KIND]"
    );
    eprintln!("usage: mnemes-admin fact-create-admit <DATA_DIR> <DEVICE_ID> <STORE_ID> <NAMESPACE> <PRINCIPAL_ID> <KEY_VERSION> <PUBLIC_KEY_HEX_32_BYTES> <ACTIVATED_AT_UNIX_SECONDS> <CUTOFF_AT_UNIX_SECONDS> <STREAM_EPOCH> <FENCING_TOKEN>");
    eprintln!("usage: mnemes-admin fact-create-revoke <DATA_DIR> <DEVICE_ID> <STORE_ID> <NAMESPACE> <PRINCIPAL_ID> <KEY_VERSION>");
    std::process::exit(1);
}

fn fail(message: impl std::fmt::Display) -> ! {
    eprintln!("{message}");
    std::process::exit(1);
}

fn parse_u64(raw: &str, name: &str) -> u64 {
    let value = raw
        .parse::<u64>()
        .unwrap_or_else(|_| fail(format!("invalid {name}")));
    if value > i64::MAX as u64 {
        fail(format!("{name} out of range"));
    }
    value
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("bootstrap") => bootstrap(args).await,
        Some("fact-create-admit") => fact_create_admit(args).await,
        Some("fact-create-revoke") => fact_create_revoke(args).await,
        _ => usage(),
    }
}

async fn bootstrap(args: Vec<String>) {
    if args.len() < 5 || args.len() > 6 {
        usage();
    }
    let data_dir = PathBuf::from(&args[1]);
    let label = args[2].clone();
    let platform = args[3].clone();
    let hostname = args[4].clone();
    let actor_kind = match args.get(5) {
        Some(raw) => match ActorKind::parse(raw) {
            ActorKind::Unknown(_) => fail(format!("invalid actor kind: {raw}")),
            known => known,
        },
        None => ActorKind::Human,
    };
    let store = MnemesStore::open(
        data_dir,
        MemoryConfig {
            ..Default::default()
        },
    )
    .unwrap_or_else(|error| fail(format!("failed to open store\n{error}")));
    match store
        .bootstrap(
            Device::new(DeviceId::new(), label, platform, hostname),
            actor_kind,
        )
        .await
    {
        Ok((device_id, actor_id, credential, created_at)) => println!(
            "{}",
            json!({"device_id": device_id.to_string(), "actor_id": actor_id.to_string(), "credential": credential, "profile": "operator", "created_at": created_at})
        ),
        Err(error) => fail(format!("bootstrap failed\n{error}")),
    }
}

async fn fact_create_revoke(args: Vec<String>) {
    if args.len() != 7 {
        usage();
    }
    let data_dir = PathBuf::from(&args[1]);
    let device_id = DeviceId::parse(&args[2]).unwrap_or_else(|_| fail("invalid device_id"));
    for (value, name) in [
        (&args[3], "store_id"),
        (&args[4], "namespace"),
        (&args[5], "principal_id"),
    ] {
        if value.is_empty() {
            fail(format!("empty {name}"));
        }
    }
    let key_version = parse_u64(&args[6], "key_version");
    if key_version == 0 {
        fail("key_version must be nonzero");
    }
    let store = MnemesStore::open(
        data_dir,
        MemoryConfig {
            ..Default::default()
        },
    )
    .unwrap_or_else(|error| fail(format!("failed to open store\n{error}")));
    store
        .revoke_fact_create_key(&device_id, &args[3], &args[4], &args[5], key_version)
        .await
        .unwrap_or_else(|error| fail(format!("fact-create revocation failed\n{error}")));
    println!(
        "{}",
        json!({"device_id": args[2], "store_id": args[3], "namespace": args[4], "principal_id": args[5], "key_version": key_version, "revoked": true})
    );
}

async fn fact_create_admit(args: Vec<String>) {
    if args.len() != 12 {
        usage();
    }
    let data_dir = PathBuf::from(&args[1]);
    let device_id = DeviceId::parse(&args[2]).unwrap_or_else(|_| fail("invalid device_id"));
    for (value, name) in [
        (&args[3], "store_id"),
        (&args[4], "namespace"),
        (&args[5], "principal_id"),
        (&args[11], "fencing_token"),
    ] {
        if value.is_empty() {
            fail(format!("empty {name}"));
        }
    }
    let key_version = parse_u64(&args[6], "key_version");
    if key_version == 0 {
        fail("key_version must be nonzero");
    }
    let public_key_vec = hex::decode(&args[7]).unwrap_or_else(|_| fail("invalid public key hex"));
    let public_key: [u8; 32] = public_key_vec
        .try_into()
        .unwrap_or_else(|_| fail("public key must be exactly 32 bytes"));
    let activated_at = parse_u64(&args[8], "activated_at");
    let cutoff_at = parse_u64(&args[9], "cutoff_at");
    if activated_at > cutoff_at {
        fail("invalid duration window");
    }
    let stream_epoch = parse_u64(&args[10], "stream_epoch");
    if stream_epoch == 0 {
        fail("stream_epoch must be nonzero");
    }
    let store = MnemesStore::open(
        data_dir,
        MemoryConfig {
            ..Default::default()
        },
    )
    .unwrap_or_else(|error| fail(format!("failed to open store\n{error}")));
    let admission = FactCreateAdmission {
        device_id,
        store_id: args[3].clone(),
        namespace: args[4].clone(),
        principal_id: args[5].clone(),
        key_version,
        public_key,
        activated_at,
        cutoff_at,
        stream_epoch,
        fencing_token: args[11].clone(),
    };
    store
        .admit_fact_create_key(admission)
        .await
        .unwrap_or_else(|error| fail(format!("fact-create admission failed\n{error}")));
    println!(
        "{}",
        json!({"device_id": args[2], "store_id": args[3], "namespace": args[4], "principal_id": args[5], "key_version": key_version, "activated_at": activated_at, "cutoff_at": cutoff_at, "stream_epoch": stream_epoch, "fencing_token": args[11]})
    );
}
