use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use actix::prelude::*;
use anyhow::{Context, Result, anyhow};
use log::{error, info, warn};
use tokio::{
    net::{TcpListener, lookup_host},
    sync::oneshot,
    task::JoinHandle,
};

use crate::{
    actors::scheduler::{
        ConnectionTaskFinished, ConnectionTaskStarted, Scheduler, Shutdown as SchedulerShutdown,
    },
    flow::FlowId,
    http::connect::handle_connect_proxy,
    limits::{MaxConnections, max_connections_from_raw, max_connections_value},
};

pub const DEFAULT_PORT: u16 = 9999;
pub const DEFAULT_MAX_CONNECTIONS: usize = 1000;
pub const DEFAULT_BIND: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
const DEFAULT_QUANTUM: usize = 8 * 1024; // 8 KB
const DEFAULT_TICK_MS: u64 = 5; // 5 ms
const FD_PER_CONNECTION: u64 = 2; // client + upstream socket
const FD_HEADROOM: u64 = 64; // listener, DNS, logs, etc.

struct ConnectionTaskGuard {
    scheduler: Addr<Scheduler>,
}

impl ConnectionTaskGuard {
    fn new(scheduler: Addr<Scheduler>) -> Self {
        scheduler.do_send(ConnectionTaskStarted);
        Self { scheduler }
    }
}

impl Drop for ConnectionTaskGuard {
    fn drop(&mut self) {
        self.scheduler.do_send(ConnectionTaskFinished);
    }
}

/// Address to bind the server to (IP or hostname).
///
/// Hostnames are resolved at start time.
#[derive(Debug, Clone)]
pub enum BindAddress {
    Ip(IpAddr),
    Host(String),
}

impl BindAddress {
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

impl From<IpAddr> for BindAddress {
    fn from(value: IpAddr) -> Self {
        BindAddress::Ip(value)
    }
}

impl From<&str> for BindAddress {
    fn from(value: &str) -> Self {
        if let Ok(addr) = value.parse::<IpAddr>() {
            addr.into()
        } else {
            BindAddress::Host(value.to_string())
        }
    }
}

impl From<String> for BindAddress {
    fn from(value: String) -> Self {
        if let Ok(addr) = value.parse::<IpAddr>() {
            addr.into()
        } else {
            BindAddress::Host(value)
        }
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
/// Dropping the handle without calling [`shutdown`] will *not* wait for the server task to finish.
/// A best-effort shutdown signal is sent on drop, but in-flight tasks may still continue briefly.
pub struct ServerHandle {
    listen_addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
    join_handle: Option<JoinHandle<Result<()>>>,
}

impl ServerHandle {
    /// Returns the socket address the server is listening on.
    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
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

    /// Starts the server and returns a handle for shutdown.
    ///
    /// Returns an error if address resolution or binding fails.
    pub async fn start(self) -> Result<ServerHandle> {
        let bind_addrs = self.bind.resolve(self.port).await?;
        start_with_addrs(bind_addrs, self.max_connections).await
    }

    /// Runs the server to completion without returning a handle.
    ///
    /// Returns an error if address resolution or binding fails.
    pub async fn run(self) -> Result<()> {
        let bind_addrs = self.bind.resolve(self.port).await?;
        server_with_addrs(bind_addrs, self.max_connections).await
    }
}

/// Creates a new `ServerBuilder` with default settings.
pub fn builder() -> ServerBuilder {
    ServerBuilder::new()
}

/// Runs the proxy server on `DEFAULT_BIND` with the given port and connection limit.
///
/// This starts the accept loop and will run until the task is stopped.
/// Use `server_with_bind` to specify the bind address explicitly.
pub async fn server(port: u16, max_connections: MaxConnections) -> Result<()> {
    server_with_bind(DEFAULT_BIND, port, max_connections).await
}

/// Runs the proxy server bound to the given IP address and port.
///
/// This only stops accepting new connections when the server task ends;
/// in-flight connections are not forcibly terminated.
pub async fn server_with_bind(
    bind: IpAddr,
    port: u16,
    max_connections: MaxConnections,
) -> Result<()> {
    server_with_addrs(vec![SocketAddr::new(bind, port)], max_connections).await
}

/// Starts the proxy server on `DEFAULT_BIND` and returns a handle for shutdown.
///
/// The returned `ServerHandle` can be used to stop accepting new connections.
pub async fn start(port: u16, max_connections: MaxConnections) -> Result<ServerHandle> {
    start_with_bind(DEFAULT_BIND, port, max_connections).await
}

/// Starts the proxy server bound to the given IP address and port.
///
/// The returned `ServerHandle` can be used to stop accepting new connections.
pub async fn start_with_bind(
    bind: IpAddr,
    port: u16,
    max_connections: MaxConnections,
) -> Result<ServerHandle> {
    start_with_addrs(vec![SocketAddr::new(bind, port)], max_connections).await
}

async fn server_with_addrs(
    bind_addrs: Vec<SocketAddr>,
    max_connections: MaxConnections,
) -> Result<()> {
    let (listener, max_connections) = prepare_listener(bind_addrs, max_connections).await?;
    run_server(listener, max_connections, None).await
}

async fn start_with_addrs(
    bind_addrs: Vec<SocketAddr>,
    max_connections: MaxConnections,
) -> Result<ServerHandle> {
    let (listener, max_connections) = prepare_listener(bind_addrs, max_connections).await?;
    let listen_addr = listener
        .local_addr()
        .context("failed to resolve listen address")?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let join_handle = actix::spawn(run_server(listener, max_connections, Some(shutdown_rx)));

    Ok(ServerHandle {
        listen_addr,
        shutdown_tx: Some(shutdown_tx),
        join_handle: Some(join_handle),
    })
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

    let mut last_err = None;
    for addr in bind_addrs {
        match TcpListener::bind(addr).await {
            Ok(listener) => {
                let bound_addr = listener
                    .local_addr()
                    .context("failed to resolve listen address")?;
                maybe_warn_non_loopback(bound_addr);
                info!("Starting Fyntr on {}", bound_addr);
                return Ok((listener, max_connections));
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
    max_connections: MaxConnections,
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
        let accept_result = if let Some(rx) = shutdown_rx.as_mut() {
            tokio::select! {
                biased;
                _ = rx => {
                    info!("Shutdown signal received; stopping accept loop and requesting scheduler shutdown");
                    scheduler.do_send(SchedulerShutdown);
                    break;
                }
                accept = listener.accept() => accept,
            }
        } else {
            listener.accept().await
        };

        let (client_stream, client_addr) = accept_result?;
        let flow_id = FlowId(flow_counter.fetch_add(1, Ordering::SeqCst));

        info!("flow{}: new connection from {}", flow_id.0, client_addr);

        let scheduler = scheduler.clone();
        let connection_task_guard = ConnectionTaskGuard::new(scheduler.clone());

        // Handle each connection in a dedicated task
        actix::spawn(async move {
            let result = handle_connect_proxy(client_stream, client_addr, flow_id, scheduler).await;
            // Keep the guard alive across the await so pending_connection_tasks
            // is decremented only after the connection task truly finishes.
            drop(connection_task_guard);
            if let Err(e) = result {
                error!("flow{}: error: {}", flow_id.0, e);
            }
        });
    }

    Ok(())
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

    #[actix_rt::test]
    async fn builder_start_shutdown_smoke() {
        let handle = builder()
            .bind("127.0.0.1")
            .port(0)
            .max_connections(1)
            .start()
            .await
            .expect("start");

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
    async fn builder_run_smoke() {
        let server_task = actix::spawn(async move {
            builder()
                .bind("127.0.0.1")
                .port(0)
                .max_connections(1)
                .run()
                .await
        });

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(
            !server_task.is_finished(),
            "expected run() task to keep running"
        );

        server_task.abort();
        let join_result = server_task.await;
        assert!(
            join_result.is_err() && join_result.unwrap_err().is_cancelled(),
            "expected run() task to be cancelled"
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
