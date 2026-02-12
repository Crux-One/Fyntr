use fyntr::run;

#[actix_rt::main]
async fn main() -> anyhow::Result<()> {
    // Optional: enables logs via RUST_LOG (e.g., RUST_LOG=info).
    env_logger::init();

    let handle = run::builder()
        .bind("127.0.0.1")
        .port(0) // 0 lets the OS pick an available port
        .max_connections(512)
        .start()
        .await?;

    println!("Fyntr listening on {}", handle.listen_addr());

    // ... run your app ...

    handle.shutdown().await?;
    Ok(())
}
