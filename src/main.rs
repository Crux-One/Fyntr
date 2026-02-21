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

    /// CONNECT destination ports to allow (repeat flag or comma-separate).
    #[clap(
        long,
        env = "FYNTR_ALLOW_PORT",
        value_delimiter = ',',
        value_parser = clap::value_parser!(u16).range(1..)
    )]
    allow_port: Vec<u16>,

    /// Disable implicit default CONNECT allow port (443).
    #[clap(long, env = "FYNTR_NO_DEFAULT_ALLOW_PORT")]
    no_default_allow_port: bool,

    /// CIDR ranges to deny for CONNECT destinations (repeat flag or comma-separate).
    #[clap(long, env = "FYNTR_DENY_CIDR", value_delimiter = ',')]
    deny_cidr: Vec<String>,

    /// CIDR ranges to explicitly allow for CONNECT destinations.
    #[clap(long, env = "FYNTR_ALLOW_CIDR", value_delimiter = ',')]
    allow_cidr: Vec<String>,

    /// Domains (or suffixes) to explicitly allow for CONNECT destinations.
    #[clap(long, env = "FYNTR_ALLOW_DOMAIN", value_delimiter = ',')]
    allow_domain: Vec<String>,
}

#[actix_rt::main]
async fn main() -> Result<(), anyhow::Error> {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info")).try_init();
    let cli = Cli::parse();

    let mut server = run::server()
        .bind(cli.bind)
        .port(cli.port)
        .max_connections(cli.max_connections);

    if cli.no_default_allow_port {
        server = server.no_default_allow_port();
    }
    for port in cli.allow_port {
        server = server.allow_port(port);
    }
    for cidr in cli.deny_cidr {
        server = server.deny_cidr(cidr)?;
    }
    for cidr in cli.allow_cidr {
        server = server.allow_cidr(cidr)?;
    }
    for domain in cli.allow_domain {
        server = server.allow_domain(domain);
    }

    server.foreground().await
}
