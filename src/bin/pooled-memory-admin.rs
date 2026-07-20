use pooled_memory::{ActorKind, Device, DeviceId, PooledMemoryStore};
use semantic_memory::MemoryConfig;
use serde_json::json;
use std::env;
use std::path::PathBuf;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.first().map(String::as_str) != Some("bootstrap") || args.len() < 5 || args.len() > 6 {
        eprintln!("usage: pooled-memory-admin bootstrap <DATA_DIR> <LABEL> <PLATFORM> <HOSTNAME> [ACTOR_KIND]");
        std::process::exit(1);
    }

    let mut args = args.into_iter();
    let _ = args.next();
    let data_dir = PathBuf::from(args.next().unwrap_or_default());
    let label = args.next().unwrap_or_default();
    let platform = args.next().unwrap_or_default();
    let hostname = args.next().unwrap_or_default();
    let actor_kind = match args.next() {
        Some(raw) => match ActorKind::parse(&raw) {
            ActorKind::Unknown(_) => {
                eprintln!("invalid actor kind: {raw}");
                std::process::exit(1);
            }
            known => known,
        },
        None => ActorKind::Human,
    };

    let store = match PooledMemoryStore::open(
        data_dir,
        MemoryConfig {
            ..Default::default()
        },
    ) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("failed to open store");
            eprintln!("{error}");
            std::process::exit(1);
        }
    };

    let device = Device::new(DeviceId::new(), label, platform, hostname);
    let result = store.bootstrap(device, actor_kind).await;

    match result {
        Ok((device_id, actor_id, credential, created_at)) => {
            let output = json!({
                "device_id": device_id.to_string(),
                "actor_id": actor_id.to_string(),
                "credential": credential,
                "profile": "operator",
                "created_at": created_at,
            });
            println!("{}", output);
        }
        Err(error) => {
            eprintln!("bootstrap failed");
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}
