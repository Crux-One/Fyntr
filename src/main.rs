use clap::Parser;
use fyntr::{
    limits::max_connections_from_raw,
    run::{self, DEFAULT_BIND, DEFAULT_MAX_CONNECTIONS, DEFAULT_PORT},
};
use std::net::IpAddr;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Address to bind on
    #[clap(long, env = "FYNTR_BIND", default_value_t = DEFAULT_BIND)]
    bind: IpAddr,

    /// Port to listen on
    #[clap(long, env = "FYNTR_PORT", default_value_t = DEFAULT_PORT)]
    port: u16,

    /// Maximum number of concurrent connections allowed
    #[clap(long, env = "FYNTR_MAX_CONNECTIONS", default_value_t = DEFAULT_MAX_CONNECTIONS)]
    max_connections: usize,
}

#[actix_rt::main]
async fn main() -> Result<(), anyhow::Error> {
    let cli = Cli::parse();

    run::server_with_bind(
        cli.bind,
        cli.port,
        max_connections_from_raw(cli.max_connections),
    )
    .await
}
