use mnemes::server::{
    build_memory_store, build_router, build_router_with_run_pack_attestation_key,
};
use std::env;
use std::net::SocketAddr;

fn configured_run_pack_attestation_key() -> Result<Option<Vec<u8>>, String> {
    let value = match env::var("MNEMES_RUN_PACK_ATTESTATION_KEY_HEX") {
        Ok(value) if !value.is_empty() => value,
        Ok(_) | Err(env::VarError::NotPresent) => return Ok(None),
        Err(env::VarError::NotUnicode(_)) => {
            return Err(
                "MNEMES_RUN_PACK_ATTESTATION_KEY_HEX must be valid UTF-8 hexadecimal".into(),
            )
        }
    };
    let key = hex::decode(value)
        .map_err(|_| "MNEMES_RUN_PACK_ATTESTATION_KEY_HEX must be hexadecimal".to_string())?;
    if key.len() < 32 {
        return Err("MNEMES_RUN_PACK_ATTESTATION_KEY_HEX must decode to at least 32 bytes".into());
    }
    Ok(Some(key))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if env::args().any(|value| value == "--help" || value == "-h") {
        eprintln!("usage: mnemes-server [PORT] [DATA_DIR]");
        eprintln!("       PORT defaults to 3000, DATA_DIR defaults to ./data/mnemes");
        return Ok(());
    }

    let args: Vec<String> = env::args().skip(1).collect();
    let port = args
        .first()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(3000);

    let cli_base_dir = args.get(1).cloned().unwrap_or_default();
    let env_base_dir = env::var("MNEMES_DATA_DIR").unwrap_or_default();
    let base_dir = if !env_base_dir.is_empty() {
        env_base_dir
    } else if !cli_base_dir.is_empty() {
        cli_base_dir
    } else {
        "./data/mnemes".to_string()
    };

    let store = build_memory_store(&base_dir)?;
    let app = match configured_run_pack_attestation_key()
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?
    {
        Some(key) => build_router_with_run_pack_attestation_key(store, key),
        None => build_router(store),
    };
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], port))).await?;
    let local_addr = listener.local_addr()?;

    println!("mnemes server listening on http://{local_addr}/v1");
    axum::serve(listener, app).await?;
    Ok(())
}
