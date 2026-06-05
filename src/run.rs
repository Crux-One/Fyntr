use std::{
    fmt,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use actix::prelude::*;
use anyhow::{Context, Result, anyhow};
use log::{debug, error, info, warn};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, lookup_host},
    sync::oneshot,
    task::JoinHandle,
};

use crate::{
    actors::scheduler::{
        PendingConnectionReservation, Scheduler, Shutdown as SchedulerShutdown,
        TryStartConnectionTask,
    },
    flow::FlowId,
    http::connect::handle_connect_proxy,
    limits::{MaxConnections, max_connections_from_raw, max_connections_value},
    security::connect_policy::{ConnectCidr, ConnectPolicy, ConnectPolicyConfig},
    socks5::handle_socks5_proxy,
    threat::{ThreatAction, ThreatIndex},
};

pub const DEFAULT_PORT: u16 = 9999;
pub const DEFAULT_MAX_CONNECTIONS: usize = 1000;
pub const DEFAULT_BIND: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
const DEFAULT_QUANTUM: usize = 8 * 1024; // 8 KB
const DEFAULT_TICK_MS: u64 = 5; // 5 ms
const FD_PER_CONNECTION: u64 = 2; // client + upstream socket
const FD_HEADROOM: u64 = 64; // listener, DNS, logs, etc.

/// Address to bind the server to (IP or hostname).
///
/// Hostnames are resolved at start time.
#[derive(Debug, Clone)]
pub enum BindAddress {
    Ip(IpAddr),
    Host(String),
}

impl BindAddress {
    fn looks_like_host_with_port(host: &str) -> bool {
        match host.rsplit_once(':') {
            Some((name, port)) if !name.is_empty() && !name.contains(':') => {
                !port.is_empty() && port.chars().all(|c| c.is_ascii_digit())
            }
            _ => false,
        }
    }

    fn validate_hostname(host: &str) -> std::result::Result<(), BindAddressParseError> {
        if host.parse::<SocketAddr>().is_ok() || Self::looks_like_host_with_port(host) {
            return Err(BindAddressParseError::ContainsPort);
        }

        if host.len() > 253 {
            return Err(BindAddressParseError::HostTooLong);
        }

        if !host
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-')
        {
            return Err(BindAddressParseError::InvalidHostChars);
        }

        Ok(())
    }

    fn parse_trimmed(value: &str) -> std::result::Result<Self, BindAddressParseError> {
        let trimmed = value.trim();

        if trimmed.is_empty() {
            return Err(BindAddressParseError::Empty);
        }

        if let Ok(addr) = trimmed.parse::<IpAddr>() {
            return Ok(BindAddress::Ip(addr));
        }

        Self::validate_hostname(trimmed)?;
        Ok(BindAddress::Host(trimmed.to_string()))
    }

    fn parse_owned(value: String) -> std::result::Result<Self, BindAddressParseError> {
        let trimmed = value.trim();

        if trimmed.is_empty() {
            return Err(BindAddressParseError::Empty);
        }

        if let Ok(addr) = trimmed.parse::<IpAddr>() {
            return Ok(BindAddress::Ip(addr));
        }

        Self::validate_hostname(trimmed)?;

        if trimmed.len() == value.len() {
            Ok(BindAddress::Host(value))
        } else {
            Ok(BindAddress::Host(trimmed.to_string()))
        }
    }

    async fn resolve(self, port: u16) -> Result<Vec<SocketAddr>> {
        match self {
            BindAddress::Ip(addr) => Ok(vec![SocketAddr::new(addr, port)]),
            BindAddress::Host(host) => {
                let resolved: Vec<SocketAddr> = lookup_host((host.as_str(), port))
                    .await
                    .with_context(|| format!("failed to resolve bind hostname: {}", host))?
                    .collect();
                let addrs = order_bind_addrs(resolved);

                if addrs.is_empty() {
                    return Err(anyhow!("no bind addresses found for hostname: {}", host));
                }
                Ok(addrs)
            }
        }
    }
}

impl fmt::Display for BindAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BindAddress::Ip(addr) => write!(f, "{}", addr),
            BindAddress::Host(host) => f.write_str(host),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindAddressParseError {
    Empty,
    ContainsPort,
    HostTooLong,
    InvalidHostChars,
}

impl fmt::Display for BindAddressParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("bind address must not be empty"),
            Self::ContainsPort => {
                f.write_str("bind address must not include a port; use --port/FYNTR_PORT")
            }
            Self::HostTooLong => f.write_str("bind hostname must be 253 characters or fewer"),
            Self::InvalidHostChars => {
                f.write_str("bind hostname contains invalid characters; allowed: a-z A-Z 0-9 . -")
            }
        }
    }
}

impl std::error::Error for BindAddressParseError {}

// Parsed by clap for --bind / FYNTR_BIND.
impl FromStr for BindAddress {
    type Err = BindAddressParseError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Self::parse_trimmed(s)
    }
}

impl From<IpAddr> for BindAddress {
    fn from(value: IpAddr) -> Self {
        BindAddress::Ip(value)
    }
}

impl From<&str> for BindAddress {
    fn from(value: &str) -> Self {
        Self::parse_trimmed(value)
            .expect("invalid bind address in BindAddress::from(&str); use BindAddress::from_str for fallible parsing")
    }
}

impl From<String> for BindAddress {
    fn from(value: String) -> Self {
        Self::parse_owned(value).expect(
            "invalid bind address in BindAddress::from(String); use BindAddress::from_str for fallible parsing",
        )
    }
}

fn order_bind_addrs(addrs: Vec<SocketAddr>) -> Vec<SocketAddr> {
    let (ipv4_addrs, ipv6_addrs): (Vec<_>, Vec<_>) =
        addrs.into_iter().partition(|addr| addr.is_ipv4());
    // Prefer IPv4 addresses but retain IPv6 as a fallback.
    let mut ordered = ipv4_addrs;
    ordered.extend(ipv6_addrs);
    ordered
}

/// Handle to a running server instance.
///
/// Use this to inspect the listen address or to initiate shutdown.
///
/// Dropping the handle without calling [`Self::shutdown`] will *not* wait for the server task to finish.
/// A best-effort shutdown signal is sent on drop, but in-flight tasks may still continue briefly.
pub struct ServerHandle {
    listen_addr: SocketAddr,
    socks5_listen_addr: Option<SocketAddr>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    join_handle: Option<JoinHandle<Result<()>>>,
}

impl ServerHandle {
    /// Returns the socket address the server is listening on.
    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }

    /// Returns the SOCKS5 listen address when the optional SOCKS5 listener is enabled.
    pub fn socks5_listen_addr(&self) -> Option<SocketAddr> {
        self.socks5_listen_addr
    }

    /// Stops accepting new connections but does not terminate in-flight tasks.
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        match self.join_handle.take() {
            Some(handle) => match handle.await {
                Ok(result) => result,
                Err(err) => Err(anyhow!("server task join failed: {}", err)),
            },
            None => Ok(()),
        }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

/// Builder for configuring and starting a server instance.
pub struct ServerBuilder {
    bind: BindAddress,
    port: u16,
    max_connections: MaxConnections,
    connect_policy: ConnectPolicyConfig,
    threat_feed_files: Vec<PathBuf>,
    socks5_bind: Option<BindAddress>,
    socks5_port: Option<u16>,
}

impl Default for ServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ServerBuilder {
    pub fn new() -> Self {
        Self {
            bind: BindAddress::from(DEFAULT_BIND),
            port: DEFAULT_PORT,
            max_connections: max_connections_from_raw(DEFAULT_MAX_CONNECTIONS),
            connect_policy: ConnectPolicyConfig::default(),
            threat_feed_files: Vec::new(),
            socks5_bind: None,
            socks5_port: None,
        }
    }

    /// Sets the bind address (IP address or hostname).
    pub fn bind<B>(mut self, bind: B) -> Self
    where
        B: Into<BindAddress>,
    {
        self.bind = bind.into();
        self
    }

    /// Sets the port to listen on.
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Sets the maximum number of concurrent connections (0 for unlimited).
    pub fn max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections_from_raw(max_connections);
        self
    }

    /// Adds a destination port to the CONNECT allow list.
    /// Port 0 is invalid and will be ignored when the policy is built.
    pub fn allow_port(mut self, port: u16) -> Self {
        self.connect_policy.allowed_ports.push(port);
        self
    }

    /// Disables implicit default port allowance (443) for CONNECT destinations.
    pub fn no_default_allow_port(mut self) -> Self {
        self.connect_policy.include_default_allow_port = false;
        self
    }

    /// Adds a CIDR range to the CONNECT deny list.
    pub fn deny_cidr<S>(mut self, cidr: S) -> Result<Self>
    where
        S: AsRef<str>,
    {
        let parsed = cidr.as_ref().parse::<ConnectCidr>()?;
        self.connect_policy.deny_cidrs.push(parsed);
        Ok(self)
    }

    /// Adds a CIDR range to the CONNECT allow list.
    pub fn allow_cidr<S>(mut self, cidr: S) -> Result<Self>
    where
        S: AsRef<str>,
    {
        let parsed = cidr.as_ref().parse::<ConnectCidr>()?;
        self.connect_policy.allow_cidrs.push(parsed);
        Ok(self)
    }

    /// Adds a domain or suffix to the CONNECT allow list.
    pub fn allow_domain<S>(mut self, domain: S) -> Self
    where
        S: AsRef<str>,
    {
        self.connect_policy
            .allow_domains
            .push(domain.as_ref().to_string());
        self
    }

    /// Loads threat domain/IP entries from a local feed file at startup.
    ///
    /// Supported MVP formats are plain domain/IP lines and AdGuard-style `||host^` rules.
    /// Unsupported rules are skipped, but the server fails to start if no supported entries remain.
    pub fn threat_feed_file<P>(mut self, path: P) -> Self
    where
        P: Into<PathBuf>,
    {
        self.threat_feed_files.push(path.into());
        self
    }

    /// Rejects CONNECT targets that match a configured threat feed.
    ///
    /// The default action is warn-only.
    pub fn reject_threats(mut self) -> Self {
        self.connect_policy.threat_action = ThreatAction::Block;
        self
    }

    /// Sets the optional SOCKS5 listener bind address.
    ///
    /// This has no effect unless [`Self::socks5_port`] is also set. When omitted, the SOCKS5
    /// listener uses the main HTTP CONNECT bind address.
    pub fn socks5_bind<B>(mut self, bind: B) -> Self
    where
        B: Into<BindAddress>,
    {
        self.socks5_bind = Some(bind.into());
        self
    }

    /// Enables a no-auth SOCKS5 CONNECT-only listener on the given port.
    pub fn socks5_port(mut self, port: u16) -> Self {
        self.socks5_port = Some(port);
        self
    }

    fn build_connect_policy(&mut self) -> Result<ConnectPolicy> {
        if self.threat_feed_files.is_empty()
            && matches!(self.connect_policy.threat_action, ThreatAction::Block)
        {
            return Err(anyhow!(
                "--threat-action block requires at least one --threat-feed-file"
            ));
        }

        if !self.threat_feed_files.is_empty() {
            self.connect_policy.threat_index =
                Some(ThreatIndex::from_feed_files(&self.threat_feed_files)?);
        }

        Ok(ConnectPolicy::from_config(std::mem::take(
            &mut self.connect_policy,
        )))
    }

    /// Runs the server in the background and returns a handle for shutdown.
    ///
    /// Returns an error if address resolution or binding fails.
    pub async fn background(mut self) -> Result<ServerHandle> {
        let bind_addrs = self.bind.clone().resolve(self.port).await?;
        let socks5_bind_addrs = self.resolve_socks5_bind_addrs().await?;
        let connect_policy = Arc::new(self.build_connect_policy()?);
        start_with_addrs(
            bind_addrs,
            socks5_bind_addrs,
            self.max_connections,
            connect_policy,
        )
        .await
    }

    /// Runs the server in the foreground to completion without returning a handle.
    ///
    /// Returns an error if address resolution or binding fails.
    pub async fn foreground(mut self) -> Result<()> {
        let bind_addrs = self.bind.clone().resolve(self.port).await?;
        let socks5_bind_addrs = self.resolve_socks5_bind_addrs().await?;
        let connect_policy = Arc::new(self.build_connect_policy()?);
        server_with_addrs(
            bind_addrs,
            socks5_bind_addrs,
            self.max_connections,
            connect_policy,
        )
        .await
    }

    #[deprecated(note = "use background() instead")]
    pub async fn start(self) -> Result<ServerHandle> {
        self.background().await
    }

    #[deprecated(note = "use foreground() instead")]
    pub async fn run(self) -> Result<()> {
        self.foreground().await
    }

    async fn resolve_socks5_bind_addrs(&self) -> Result<Option<Vec<SocketAddr>>> {
        let Some(port) = self.socks5_port else {
            return Ok(None);
        };

        let bind = self
            .socks5_bind
            .clone()
            .unwrap_or_else(|| self.bind.clone());
        Ok(Some(bind.resolve(port).await?))
    }
}

/// Creates a new `ServerBuilder` with default settings.
pub fn server() -> ServerBuilder {
    ServerBuilder::new()
}

async fn server_with_addrs(
    bind_addrs: Vec<SocketAddr>,
    socks5_bind_addrs: Option<Vec<SocketAddr>>,
    max_connections: MaxConnections,
    connect_policy: Arc<ConnectPolicy>,
) -> Result<()> {
    let (listener, max_connections) = prepare_listener(bind_addrs, max_connections).await?;
    let socks5_listener = prepare_optional_listener(socks5_bind_addrs).await?;
    run_server(
        listener,
        socks5_listener,
        max_connections,
        connect_policy,
        None,
    )
    .await
}

async fn start_with_addrs(
    bind_addrs: Vec<SocketAddr>,
    socks5_bind_addrs: Option<Vec<SocketAddr>>,
    max_connections: MaxConnections,
    connect_policy: Arc<ConnectPolicy>,
) -> Result<ServerHandle> {
    let (listener, max_connections) = prepare_listener(bind_addrs, max_connections).await?;
    let listen_addr = listener
        .local_addr()
        .context("failed to resolve listen address")?;
    let socks5_listener = prepare_optional_listener(socks5_bind_addrs).await?;
    let socks5_listen_addr = socks5_listener
        .as_ref()
        .map(TcpListener::local_addr)
        .transpose()
        .context("failed to resolve SOCKS5 listen address")?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let join_handle = actix::spawn(run_server(
        listener,
        socks5_listener,
        max_connections,
        connect_policy,
        Some(shutdown_rx),
    ));

    Ok(ServerHandle {
        listen_addr,
        socks5_listen_addr,
        shutdown_tx: Some(shutdown_tx),
        join_handle: Some(join_handle),
    })
}

async fn prepare_optional_listener(
    bind_addrs: Option<Vec<SocketAddr>>,
) -> Result<Option<TcpListener>> {
    let Some(bind_addrs) = bind_addrs else {
        return Ok(None);
    };
    bind_listener(bind_addrs).await.map(Some)
}

async fn prepare_listener(
    bind_addrs: Vec<SocketAddr>,
    max_connections: MaxConnections,
) -> Result<(TcpListener, MaxConnections)> {
    if bind_addrs.is_empty() {
        return Err(anyhow!("no bind addresses resolved"));
    }
    let max_connections = cap_max_connections(max_connections);
    ensure_nofile_limits(max_connections);

    Ok((bind_listener(bind_addrs).await?, max_connections))
}

async fn bind_listener(bind_addrs: Vec<SocketAddr>) -> Result<TcpListener> {
    let mut last_err = None;
    for addr in bind_addrs {
        match TcpListener::bind(addr).await {
            Ok(listener) => {
                let bound_addr = listener
                    .local_addr()
                    .context("failed to resolve listen address")?;
                maybe_warn_non_loopback(bound_addr);
                info!("Starting Fyntr on {}", bound_addr);
                return Ok(listener);
            }
            Err(err) => last_err = Some((addr, err)),
        }
    }

    let (addr, err) = last_err.expect("bind attempts should yield at least one error");
    Err(anyhow!(
        "failed to bind to any resolved address (last error on {}): {}",
        addr,
        err
    ))
}

async fn run_server(
    listener: TcpListener,
    socks5_listener: Option<TcpListener>,
    max_connections: MaxConnections,
    connect_policy: Arc<ConnectPolicy>,
    mut shutdown_rx: Option<oneshot::Receiver<()>>,
) -> Result<()> {
    // Start the scheduler
    let quantum = DEFAULT_QUANTUM;
    let tick = Duration::from_millis(DEFAULT_TICK_MS);
    let scheduler = Scheduler::new(quantum, tick)
        .with_max_connections(max_connections)
        .start();

    // Start the flow ID counter
    let flow_counter = Arc::new(AtomicUsize::new(1));

    loop {
        let accept_result = if let Some(socks5_listener) = socks5_listener.as_ref() {
            if let Some(rx) = shutdown_rx.as_mut() {
                tokio::select! {
                    biased;
                    _ = rx => {
                        info!("Shutdown signal received; stopping accept loop and requesting scheduler shutdown");
                        scheduler.do_send(SchedulerShutdown);
                        break;
                    }
                    accept = listener.accept() => accept.map(|accepted| (accepted, InboundProtocol::HttpConnect)),
                    accept = socks5_listener.accept() => accept.map(|accepted| (accepted, InboundProtocol::Socks5)),
                }
            } else {
                tokio::select! {
                    accept = listener.accept() => accept.map(|accepted| (accepted, InboundProtocol::HttpConnect)),
                    accept = socks5_listener.accept() => accept.map(|accepted| (accepted, InboundProtocol::Socks5)),
                }
            }
        } else if let Some(rx) = shutdown_rx.as_mut() {
            tokio::select! {
                biased;
                _ = rx => {
                    info!("Shutdown signal received; stopping accept loop and requesting scheduler shutdown");
                    scheduler.do_send(SchedulerShutdown);
                    break;
                }
                accept = listener.accept() => accept.map(|accepted| (accepted, InboundProtocol::HttpConnect)),
            }
        } else {
            listener
                .accept()
                .await
                .map(|accepted| (accepted, InboundProtocol::HttpConnect))
        };

        let ((client_stream, client_addr), protocol) = accept_result?;
        let flow_id = FlowId(flow_counter.fetch_add(1, Ordering::SeqCst));

        info!(
            "flow{}: new {} connection from {}",
            flow_id.0,
            protocol.as_str(),
            client_addr
        );

        let scheduler = scheduler.clone();
        let pending_reservation = match scheduler.send(TryStartConnectionTask { flow_id }).await {
            Ok(Ok(())) => PendingConnectionReservation::new(scheduler.clone(), flow_id),
            Ok(Err(err)) => {
                warn!(
                    "flow{}: rejecting {} connection from {} before parsing: {}",
                    flow_id.0,
                    protocol.as_str(),
                    client_addr,
                    err
                );
                reject_over_capacity(client_stream, protocol).await;
                continue;
            }
            Err(err) => {
                error!(
                    "flow{}: scheduler unavailable while reserving connection task: {}",
                    flow_id.0, err
                );
                reject_over_capacity(client_stream, protocol).await;
                continue;
            }
        };

        let connect_policy = connect_policy.clone();

        // Handle each connection in a dedicated task
        actix::spawn(async move {
            let result = match protocol {
                InboundProtocol::HttpConnect => {
                    handle_connect_proxy(
                        client_stream,
                        client_addr,
                        flow_id,
                        scheduler,
                        connect_policy,
                        pending_reservation,
                    )
                    .await
                }
                InboundProtocol::Socks5 => {
                    handle_socks5_proxy(
                        client_stream,
                        client_addr,
                        flow_id,
                        scheduler,
                        connect_policy,
                        pending_reservation,
                    )
                    .await
                }
            };
            if let Err(e) = result {
                error!("flow{}: error: {}", flow_id.0, e);
            }
        });
    }

    Ok(())
}

#[derive(Clone, Copy)]
enum InboundProtocol {
    HttpConnect,
    Socks5,
}

impl InboundProtocol {
    fn as_str(self) -> &'static str {
        match self {
            Self::HttpConnect => "HTTP CONNECT",
            Self::Socks5 => "SOCKS5",
        }
    }
}

async fn reject_over_capacity(mut client_stream: tokio::net::TcpStream, protocol: InboundProtocol) {
    match protocol {
        InboundProtocol::HttpConnect => {
            if let Err(err) = client_stream
                .write_all(b"HTTP/1.1 503 Service Unavailable\r\n\r\n")
                .await
            {
                debug!("failed to write over-capacity response: {}", err);
            }
        }
        InboundProtocol::Socks5 => {
            debug!("closing over-capacity SOCKS5 connection before handshake");
        }
    }
}

fn maybe_warn_non_loopback(addr: SocketAddr) {
    if !addr.ip().is_loopback() {
        warn!(
            "binding to a non-loopback address ({}) without auth may allow anyone on the network to use this proxy; use a firewall or bind to loopback to restrict access",
            addr
        );
    }
}

fn ensure_nofile_limits(max_connections: MaxConnections) {
    let required = required_nofile(max_connections);
    let limits = get_nofile_limits();
    // On Unix, try to raise the soft FD limit if it's below what's required
    // for the configured max connections. This is best-effort; failure only
    // results in warnings.
    #[cfg(unix)]
    if let (Some(required), Some((soft, hard))) = (required, limits)
        && soft < required
    {
        let target_soft = required.min(hard);
        match rlimit::setrlimit(rlimit::Resource::NOFILE, target_soft, hard) {
            Ok(_) => info!("Raised soft FD limit to {}", target_soft),
            Err(e) => warn!(
                "failed to raise soft FD limit to {} (hard={}): {}",
                target_soft, hard, e
            ),
        }
    }

    // Re-check after any adjustment attempt so logs and warnings reflect reality.
    let limits = get_nofile_limits();
    if let Some((soft, hard)) = limits {
        info!("FD limits: soft={}, hard={}", soft, hard);
    }

    for warning in nofile_warnings(max_connections, limits) {
        warn!("{}", warning);
    }
}

fn cap_max_connections(max_connections: MaxConnections) -> MaxConnections {
    let max_connections = max_connections?.get();
    let per_conn = FD_PER_CONNECTION as usize;
    let headroom = FD_HEADROOM as usize;
    let max_by_fd = usize::MAX.saturating_sub(headroom).saturating_div(per_conn);

    if max_connections > max_by_fd {
        warn!(
            "max_connections ({}) is too large; clamping to {} to avoid overflow",
            max_connections, max_by_fd
        );
        max_connections_from_raw(max_by_fd)
    } else {
        max_connections_from_raw(max_connections)
    }
}

fn required_nofile(max_connections: MaxConnections) -> Option<u64> {
    max_connections_value(max_connections).map(|max| {
        (max as u64)
            .saturating_mul(FD_PER_CONNECTION)
            .saturating_add(FD_HEADROOM)
    })
}

/// Returns the RLIMIT_NOFILE soft and hard limits, if available.
///
/// Soft: the current effective per-process limit (what actually applies now).
/// Hard: the ceiling for soft; only root can raise soft beyond hard.
#[cfg(unix)]
fn get_nofile_limits() -> Option<(u64, u64)> {
    rlimit::getrlimit(rlimit::Resource::NOFILE).ok()
}

#[cfg(not(unix))]
fn get_nofile_limits() -> Option<(u64, u64)> {
    None
}

fn nofile_warnings(max_connections: MaxConnections, limits: Option<(u64, u64)>) -> Vec<String> {
    let mut warnings = Vec::new();

    let (soft, hard) = match limits {
        Some((soft, hard)) => (soft, hard),
        None => {
            if cfg!(unix) {
                warnings.push("failed to get nofile limits".to_string());
            } else {
                warnings.push("FD limit checking not available on this platform".to_string());
            }
            return warnings;
        }
    };

    if max_connections.is_none() {
        warnings.push(format!(
            "max_connections is unlimited; soft FD limit is {}; set a specific --max-connections limit or raise ulimit to avoid EMFILE",
            soft
        ));
    } else if let Some(required) = required_nofile(max_connections)
        && required > soft
    {
        let max_connections = max_connections_value(max_connections).unwrap_or(0);
        warnings.push(format!(
            "required FDs ({}) for max_connections ({}) exceed soft FD limit (soft={}, hard={}); increase ulimit or lower max_connections to avoid EMFILE",
            required, max_connections, soft, hard
        ));
    }

    warnings
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    #[test]
    fn required_nofile_unlimited_is_none() {
        assert_eq!(required_nofile(max_connections_from_raw(0)), None);
    }

    #[test]
    fn required_nofile_scales_with_connections() {
        assert_eq!(
            required_nofile(max_connections_from_raw(1)),
            Some(FD_PER_CONNECTION + FD_HEADROOM)
        );
        assert_eq!(
            required_nofile(max_connections_from_raw(10)),
            Some(10 * FD_PER_CONNECTION + FD_HEADROOM)
        );
    }

    #[test]
    fn warns_when_max_connections_unlimited() {
        let warnings = nofile_warnings(max_connections_from_raw(0), Some((1024, 2048)));
        assert!(
            warnings
                .iter()
                .any(|w| w.contains("max_connections is unlimited")),
            "should warn when max_connections is zero"
        );
    }

    #[test]
    fn warns_when_max_connections_exceeds_soft_limit() {
        let warnings = nofile_warnings(max_connections_from_raw(5000), Some((4000, 8000)));
        assert!(
            warnings.iter().any(|w| w.contains("required FDs")),
            "should warn when requested max exceeds soft limit"
        );
    }

    #[test]
    fn warns_when_required_fds_exceed_soft_limit_even_if_connections_do_not() {
        // 600 connections require 600*2 + 64 = 1264 FDs, which exceeds soft=1000.
        let warnings = nofile_warnings(max_connections_from_raw(600), Some((1000, 2000)));
        assert!(
            warnings
                .iter()
                .any(|w| w.contains("required FDs") && w.contains("exceed soft FD limit")),
            "should warn based on required FDs, not raw connections"
        );
    }

    #[test]
    fn no_warning_when_within_limits() {
        let warnings = nofile_warnings(max_connections_from_raw(100), Some((1000, 2000)));
        assert!(warnings.is_empty(), "no warnings expected within limits");
    }

    #[test]
    fn reject_threats_requires_threat_feed_file() {
        let err = ServerBuilder::new().reject_threats().build_connect_policy();

        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("--threat-feed-file"));
    }

    #[test]
    fn warns_when_limits_unavailable() {
        let warnings = nofile_warnings(max_connections_from_raw(100), None);
        if cfg!(unix) {
            assert!(
                warnings
                    .iter()
                    .any(|w| w.contains("failed to get nofile limits")),
                "should warn when limits cannot be fetched"
            );
        } else {
            assert!(
                warnings
                    .iter()
                    .any(|w| w.contains("FD limit checking not available")),
                "should warn when FD limits are unsupported"
            );
        }
    }

    #[test]
    fn bind_address_from_str_parses_ip_or_host() {
        match BindAddress::from("127.0.0.1") {
            BindAddress::Ip(addr) => assert!(addr.is_loopback()),
            _ => panic!("expected IPv4 loopback to parse as BindAddress::Ip"),
        }

        match BindAddress::from("::1") {
            BindAddress::Ip(addr) => assert!(addr.is_loopback()),
            _ => panic!("expected IPv6 loopback to parse as BindAddress::Ip"),
        }

        match BindAddress::from("localhost") {
            BindAddress::Host(host) => assert_eq!(host, "localhost"),
            _ => panic!("expected hostname to parse as BindAddress::Host"),
        }
    }

    #[test]
    fn bind_address_from_string_parses_ip_or_host() {
        let v4 = "127.0.0.1".to_string();
        match BindAddress::from(v4) {
            BindAddress::Ip(addr) => assert!(addr.is_loopback()),
            _ => panic!("expected IPv4 loopback to parse as BindAddress::Ip"),
        }

        let v6 = "::1".to_string();
        match BindAddress::from(v6) {
            BindAddress::Ip(addr) => assert!(addr.is_loopback()),
            _ => panic!("expected IPv6 loopback to parse as BindAddress::Ip"),
        }

        let host = "localhost".to_string();
        match BindAddress::from(host) {
            BindAddress::Host(value) => assert_eq!(value, "localhost"),
            _ => panic!("expected hostname to parse as BindAddress::Host"),
        }
    }

    #[test]
    fn bind_address_from_str_trait_parses_ip_or_host() {
        let ip: BindAddress = "127.0.0.1".parse().expect("valid parse");
        assert!(matches!(ip, BindAddress::Ip(addr) if addr.is_loopback()));

        let ipv6: BindAddress = "::1".parse().expect("valid parse");
        assert!(matches!(ipv6, BindAddress::Ip(addr) if addr.is_loopback()));

        let host: BindAddress = "localhost".parse().expect("valid parse");
        assert!(matches!(host, BindAddress::Host(value) if value == "localhost"));
    }

    #[test]
    fn bind_address_from_str_trait_rejects_empty_or_port() {
        let empty = "".parse::<BindAddress>().expect_err("empty should fail");
        assert!(matches!(empty, BindAddressParseError::Empty));

        let whitespace = "   "
            .parse::<BindAddress>()
            .expect_err("whitespace should fail");
        assert!(matches!(whitespace, BindAddressParseError::Empty));

        let with_port = "127.0.0.1:9999"
            .parse::<BindAddress>()
            .expect_err("ip with port should fail");
        assert!(matches!(with_port, BindAddressParseError::ContainsPort));

        let bracketed_v6_with_port = "[::1]:9999"
            .parse::<BindAddress>()
            .expect_err("ipv6 with port should fail");
        assert!(matches!(
            bracketed_v6_with_port,
            BindAddressParseError::ContainsPort
        ));

        let host_with_port = "localhost:9999"
            .parse::<BindAddress>()
            .expect_err("hostname with port should fail");
        assert!(matches!(
            host_with_port,
            BindAddressParseError::ContainsPort
        ));

        let host_with_out_of_range_port = "example.invalid:65536"
            .parse::<BindAddress>()
            .expect_err("hostname with out-of-range port should fail");
        assert!(matches!(
            host_with_out_of_range_port,
            BindAddressParseError::ContainsPort
        ));

        let too_long = format!("{}.example.invalid", "a".repeat(238));
        let too_long_err = too_long
            .parse::<BindAddress>()
            .expect_err("hostname > 253 chars should fail");
        assert!(matches!(too_long_err, BindAddressParseError::HostTooLong));

        let invalid_chars = "local_host"
            .parse::<BindAddress>()
            .expect_err("hostname with invalid chars should fail");
        assert!(matches!(
            invalid_chars,
            BindAddressParseError::InvalidHostChars
        ));
    }

    #[test]
    #[should_panic(expected = "invalid bind address in BindAddress::from(&str)")]
    fn bind_address_from_str_panics_on_invalid_whitespace_input() {
        let _ = BindAddress::from("   ");
    }

    #[actix_rt::test]
    async fn server_background_shutdown_smoke() {
        let handle = server()
            .bind("127.0.0.1")
            .port(0)
            .max_connections(1)
            .background()
            .await
            .expect("background");

        let listen_addr = handle.listen_addr();
        assert!(listen_addr.ip().is_loopback(), "expected loopback bind");
        assert_ne!(listen_addr.port(), 0, "expected OS-assigned port");

        handle.shutdown().await.expect("shutdown");
        // Retry briefly to allow the shutdown to fully propagate.
        let mut connect_result = TcpStream::connect(listen_addr).await;
        if connect_result.is_ok() {
            for _ in 0..4 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                connect_result = TcpStream::connect(listen_addr).await;
                if connect_result.is_err() {
                    break;
                }
            }
        }
        assert!(
            connect_result.is_err(),
            "expected connection to fail after shutdown"
        );
    }

    #[actix_rt::test]
    async fn server_rejects_when_pending_connections_reach_limit() {
        let handle = server()
            .bind("127.0.0.1")
            .port(0)
            .max_connections(1)
            .background()
            .await
            .expect("background");

        let listen_addr = handle.listen_addr();
        let _held_pending_connection = TcpStream::connect(listen_addr).await.unwrap();

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
        loop {
            let mut rejected = TcpStream::connect(listen_addr).await.unwrap();
            let mut response = Vec::new();
            match tokio::time::timeout(
                std::time::Duration::from_millis(100),
                rejected.read_to_end(&mut response),
            )
            .await
            {
                Ok(Ok(_)) if response == b"HTTP/1.1 503 Service Unavailable\r\n\r\n" => break,
                Ok(Ok(_)) => panic!("unexpected rejection response: {:?}", response),
                Ok(Err(err)) => panic!("failed to read rejection response: {}", err),
                Err(_) if tokio::time::Instant::now() < deadline => continue,
                Err(_) => panic!("timed out waiting for pending connection rejection"),
            }
        }

        handle.shutdown().await.expect("shutdown");
    }

    #[actix_rt::test]
    async fn server_accepts_socks5_connect_ipv4() {
        let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend_addr = backend.local_addr().unwrap();
        let backend_task = actix::spawn(async move {
            let (mut stream, _) = backend.accept().await.unwrap();
            let mut request = [0_u8; 4];
            stream.read_exact(&mut request).await.unwrap();
            assert_eq!(&request, b"ping");
            stream.write_all(b"pong").await.unwrap();
        });

        let handle = server()
            .bind("127.0.0.1")
            .port(0)
            .socks5_port(0)
            .allow_port(backend_addr.port())
            .allow_cidr("127.0.0.0/8")
            .unwrap()
            .background()
            .await
            .expect("background");

        let socks5_addr = handle.socks5_listen_addr().expect("socks5 listener");
        let mut client = TcpStream::connect(socks5_addr).await.unwrap();
        client.write_all(&[0x05, 0x01, 0x00]).await.unwrap();
        let mut method = [0_u8; 2];
        client.read_exact(&mut method).await.unwrap();
        assert_eq!(method, [0x05, 0x00]);

        let mut request = vec![0x05, 0x01, 0x00, 0x01, 127, 0, 0, 1];
        request.extend_from_slice(&backend_addr.port().to_be_bytes());
        client.write_all(&request).await.unwrap();

        let mut reply = [0_u8; 10];
        client.read_exact(&mut reply).await.unwrap();
        assert_eq!(reply[0], 0x05);
        assert_eq!(reply[1], 0x00);

        client.write_all(b"ping").await.unwrap();
        let mut response = [0_u8; 4];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"pong");

        backend_task.await.unwrap();
        handle.shutdown().await.expect("shutdown");
    }

    #[actix_rt::test]
    async fn server_accepts_socks5_domainname_with_local_resolution() {
        let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend_addr = backend.local_addr().unwrap();
        let backend_task = actix::spawn(async move {
            let (mut stream, _) = backend.accept().await.unwrap();
            let mut request = [0_u8; 4];
            stream.read_exact(&mut request).await.unwrap();
            assert_eq!(&request, b"ping");
            stream.write_all(b"pong").await.unwrap();
        });

        let handle = server()
            .bind("127.0.0.1")
            .port(0)
            .socks5_port(0)
            .allow_port(backend_addr.port())
            .allow_cidr("127.0.0.0/8")
            .unwrap()
            .allow_cidr("::1/128")
            .unwrap()
            .allow_domain("localhost")
            .background()
            .await
            .expect("background");

        let socks5_addr = handle.socks5_listen_addr().expect("socks5 listener");
        let mut client = TcpStream::connect(socks5_addr).await.unwrap();
        client.write_all(&[0x05, 0x01, 0x00]).await.unwrap();
        let mut method = [0_u8; 2];
        client.read_exact(&mut method).await.unwrap();
        assert_eq!(method, [0x05, 0x00]);

        let domain = b"localhost";
        let mut request = vec![0x05, 0x01, 0x00, 0x03, domain.len() as u8];
        request.extend_from_slice(domain);
        request.extend_from_slice(&backend_addr.port().to_be_bytes());
        client.write_all(&request).await.unwrap();

        let mut reply = [0_u8; 10];
        client.read_exact(&mut reply).await.unwrap();
        assert_eq!(reply[0], 0x05);
        assert_eq!(reply[1], 0x00);

        client.write_all(b"ping").await.unwrap();
        let mut response = [0_u8; 4];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"pong");

        backend_task.await.unwrap();
        handle.shutdown().await.expect("shutdown");
    }

    #[actix_rt::test]
    async fn server_foreground_smoke() {
        let server_task = actix::spawn(async move {
            server()
                .bind("127.0.0.1")
                .port(0)
                .max_connections(1)
                .foreground()
                .await
        });

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(
            !server_task.is_finished(),
            "expected foreground() task to keep running"
        );

        server_task.abort();
        let join_result = server_task.await;
        assert!(
            join_result.is_err() && join_result.unwrap_err().is_cancelled(),
            "expected foreground() task to be cancelled"
        );
    }

    #[test]
    fn warns_only_limits_unavailable_when_unlimited_and_no_limits() {
        let warnings = nofile_warnings(max_connections_from_raw(0), None);
        assert_eq!(warnings.len(), 1, "should emit a single warning");
        if cfg!(unix) {
            assert!(
                warnings[0].contains("failed to get nofile limits"),
                "should only warn about missing limits"
            );
        } else {
            assert!(
                warnings[0].contains("FD limit checking not available"),
                "should only warn about unsupported limits"
            );
        }
    }

    #[test]
    fn caps_max_connections_to_avoid_overflow() {
        let per_conn = FD_PER_CONNECTION as usize;
        let headroom = FD_HEADROOM as usize;
        let max_by_fd = usize::MAX.saturating_sub(headroom).saturating_div(per_conn);

        assert_eq!(cap_max_connections(None), None, "none remains unlimited");
        assert_eq!(
            cap_max_connections(max_connections_from_raw(max_by_fd)),
            max_connections_from_raw(max_by_fd),
            "value at cap is unchanged"
        );
        assert_eq!(
            cap_max_connections(max_connections_from_raw(max_by_fd.saturating_add(1))),
            max_connections_from_raw(max_by_fd),
            "value above cap is clamped"
        );
    }

    #[test]
    fn order_bind_addrs_prefers_ipv4_first() {
        let addrs = vec![
            SocketAddr::new(IpAddr::V6(std::net::Ipv6Addr::LOCALHOST), 9999),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9999),
            SocketAddr::new(IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED), 9999),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)), 9999),
        ];

        let ordered = order_bind_addrs(addrs);

        assert!(
            ordered.iter().take(2).all(|addr| addr.is_ipv4()),
            "expected IPv4 addresses to come first"
        );
        assert!(
            ordered.iter().skip(2).all(|addr| addr.is_ipv6()),
            "expected IPv6 addresses after IPv4"
        );
    }
}
