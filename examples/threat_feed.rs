use std::path::PathBuf;

use fyntr::run;

#[actix_rt::main]
async fn main() -> anyhow::Result<()> {
    // Optional: to enable logging, set RUST_LOG (for example, RUST_LOG=info)
    // and uncomment the line below to initialize env_logger:
    // env_logger::init();

    let threat_feed = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/threat-feed.txt")
        });

    let handle = run::server()
        .bind("127.0.0.1")
        .port(0) // 0 lets the OS pick an available port
        .threat_feed_file(threat_feed)
        .reject_threats()
        .background()
        .await?;

    println!("Fyntr listening on {}", handle.listen_addr());

    // ... run your app ...

    handle.shutdown().await?;
    Ok(())
}
