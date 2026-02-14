use clap::Parser;
use env_logger::Env;
use fyntr::run::{self, BindAddress, DEFAULT_BIND, DEFAULT_MAX_CONNECTIONS, DEFAULT_PORT};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Address to bind on
    #[clap(long, env = "FYNTR_BIND", default_value_t = BindAddress::Ip(DEFAULT_BIND))]
    bind: BindAddress,

    /// Port to listen on
    #[clap(long, env = "FYNTR_PORT", default_value_t = DEFAULT_PORT)]
    port: u16,

    /// Maximum number of concurrent connections allowed
    #[clap(long, env = "FYNTR_MAX_CONNECTIONS", default_value_t = DEFAULT_MAX_CONNECTIONS)]
    max_connections: usize,
}

#[actix_rt::main]
async fn main() -> Result<(), anyhow::Error> {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info")).try_init();
    let cli = Cli::parse();

    run::builder()
        .bind(cli.bind)
        .port(cli.port)
        .max_connections(cli.max_connections)
        .run()
        .await
}
