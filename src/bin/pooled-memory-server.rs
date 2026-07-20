use pooled_memory::server::{build_memory_store, build_router};
use std::env;
use std::net::SocketAddr;

#[tokio::main]
async fn main() {
    if env::args().any(|value| value == "--help" || value == "-h") {
        eprintln!("usage: pooled-memory-server [PORT] [DATA_DIR]");
        eprintln!("       PORT defaults to 3000, DATA_DIR defaults to ./data/pooled-memory");
        return;
    }

    let args: Vec<String> = env::args().skip(1).collect();
    let port = args
        .first()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(3000);

    let cli_base_dir = args.get(1).cloned().unwrap_or_default();
    let env_base_dir = env::var("POOLED_MEMORY_DATA_DIR").unwrap_or_default();
    let base_dir = if !env_base_dir.is_empty() {
        env_base_dir
    } else if !cli_base_dir.is_empty() {
        cli_base_dir
    } else {
        "./data/pooled-memory".to_string()
    };

    let store = build_memory_store(&base_dir)
        .unwrap_or_else(|error| panic!("failed to initialize store: {error}"));

    let app = build_router(store);
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], port)))
        .await
        .unwrap_or_else(|error| panic!("failed to bind loopback listener: {error}"));
    let local_addr = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("failed to resolve local listener address: {error}"));

    println!("pooled-memory server listening on http://{local_addr}/v1");
    axum::serve(listener, app)
        .await
        .unwrap_or_else(|error| panic!("pooled-memory server stopped: {error}"));
}
