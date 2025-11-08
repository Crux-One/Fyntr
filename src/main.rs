use clap::Parser;
use fyntr::run::{self, DEFAULT_MAX_CONNECTIONS, DEFAULT_PORT};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
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

    run::server(cli.port, cli.max_connections).await
}
