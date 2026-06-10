use std::{path::PathBuf, time::Duration};

use clap::{Parser, ValueEnum};
use env_logger::Env;
use fyntr::run::{
    self, BindAddress, DEFAULT_BIND, DEFAULT_IDLE_TIMEOUT_SECS, DEFAULT_MAX_CONNECTIONS,
    DEFAULT_PORT,
};

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ThreatActionCli {
    Warn,
    Block,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Address to bind on
    #[clap(long, env = "FYNTR_BIND", default_value_t = BindAddress::Ip(DEFAULT_BIND))]
    bind: BindAddress,

    /// Port to listen on
    #[clap(long, env = "FYNTR_PORT", default_value_t = DEFAULT_PORT)]
    port: u16,

    /// Optional address for the SOCKS5 listener. Defaults to --bind when --socks5-port is set.
    #[clap(long, env = "FYNTR_SOCKS5_BIND")]
    socks5_bind: Option<BindAddress>,

    /// Enable a no-auth SOCKS5 CONNECT-only listener on this port.
    #[clap(long, env = "FYNTR_SOCKS5_PORT")]
    socks5_port: Option<u16>,

    /// Maximum number of concurrent connections allowed
    #[clap(long, env = "FYNTR_MAX_CONNECTIONS", default_value_t = DEFAULT_MAX_CONNECTIONS)]
    max_connections: usize,

    /// Established tunnel idle timeout in seconds (0 to disable)
    #[clap(
        long = "idle-timeout",
        env = "FYNTR_IDLE_TIMEOUT",
        default_value_t = DEFAULT_IDLE_TIMEOUT_SECS
    )]
    idle_timeout: u64,

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

    /// Local threat feed files with domain/IP entries to warn on or block.
    #[clap(long, env = "FYNTR_THREAT_FEED_FILE", value_delimiter = ',')]
    threat_feed_file: Vec<PathBuf>,

    /// Action to take when a CONNECT target matches a configured threat feed.
    #[clap(long, env = "FYNTR_THREAT_ACTION", value_enum, default_value = "warn")]
    threat_action: ThreatActionCli,
}

#[actix_rt::main]
async fn main() -> Result<(), anyhow::Error> {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info")).try_init();
    let cli = Cli::parse();

    let mut server = run::server()
        .bind(cli.bind)
        .port(cli.port)
        .max_connections(cli.max_connections)
        .idle_timeout(Duration::from_secs(cli.idle_timeout));

    if let Some(bind) = cli.socks5_bind {
        server = server.socks5_bind(bind);
    }
    if let Some(port) = cli.socks5_port {
        server = server.socks5_port(port);
    }

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
    for path in cli.threat_feed_file {
        server = server.threat_feed_file(path);
    }
    if matches!(cli.threat_action, ThreatActionCli::Block) {
        server = server.reject_threats();
    }

    server.foreground().await
}
