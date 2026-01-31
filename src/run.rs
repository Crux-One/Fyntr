use std::{
    net::{IpAddr, Ipv4Addr},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use actix::prelude::*;
use anyhow::Result;
use env_logger::Env;
use log::{error, info, warn};
use tokio::net::TcpListener;

use crate::{
    actors::scheduler::Scheduler,
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

pub async fn server(bind: IpAddr, port: u16, max_connections: MaxConnections) -> Result<()> {
    bootstrap();
    let max_connections = cap_max_connections(max_connections);
    ensure_nofile_limits(max_connections);

    let proxy_listen_addr = format!("{}:{}", bind, port);
    info!("Starting Fyntr on {}", proxy_listen_addr);
    let listener = TcpListener::bind(proxy_listen_addr).await?;

    // Start the scheduler
    let quantum = DEFAULT_QUANTUM;
    let tick = Duration::from_millis(DEFAULT_TICK_MS);
    let scheduler = Scheduler::new(quantum, tick)
        .with_max_connections(max_connections)
        .start();

    // Start the flow ID counter
    let flow_counter = Arc::new(AtomicUsize::new(1));

    loop {
        let (client_stream, client_addr) = listener.accept().await?;
        let flow_id = FlowId(flow_counter.fetch_add(1, Ordering::SeqCst));

        info!("flow{}: new connection from {}", flow_id.0, client_addr);

        let scheduler = scheduler.clone();

        // Handle each connection in a dedicated task
        actix::spawn(async move {
            if let Err(e) =
                handle_connect_proxy(client_stream, client_addr, flow_id, scheduler).await
            {
                error!("flow{}: error: {}", flow_id.0, e);
            }
        });
    }
}

fn bootstrap() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
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
}
