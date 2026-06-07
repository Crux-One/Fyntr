use fyntr::run;

#[actix_rt::main]
async fn main() -> anyhow::Result<()> {
    // Optional: to enable logging, set RUST_LOG (for example, RUST_LOG=info)
    // and uncomment the line below to initialize env_logger:
    // env_logger::init();

    let handle = run::server()
        .bind("127.0.0.1")
        .port(0) // 0 lets the OS pick an available HTTP CONNECT port
        .socks5_port(0) // 0 lets the OS pick an available SOCKS5 port
        .max_connections(512)
        .background()
        .await?;

    println!("Fyntr HTTP CONNECT listening on {}", handle.listen_addr());
    if let Some(addr) = handle.socks5_listen_addr() {
        println!("Fyntr SOCKS5 listening on {}", addr);
    }

    // ... run your app ...

    handle.shutdown().await?;
    Ok(())
}
