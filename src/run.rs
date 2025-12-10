use std::{
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

use crate::{actors::scheduler::Scheduler, flow::FlowId, http::connect::handle_connect_proxy};

pub const DEFAULT_PORT: u16 = 9999;
pub const DEFAULT_MAX_CONNECTIONS: usize = 1000;
const DEFAULT_QUANTUM: usize = 8 * 1024; // 8 KB
const DEFAULT_TICK_MS: u64 = 5; // 5 ms

pub async fn server(port: u16, max_connections: usize) -> Result<()> {
    bootstrap();
    check_nofile_limits(max_connections);

    let proxy_listen_addr = format!("127.0.0.1:{}", port);
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

fn check_nofile_limits(max_connections: usize) {
    let limits = get_nofile_limits();
    let (soft, hard) = limits.unwrap_or((u64::MAX, u64::MAX));
    info!("FD limit: soft={}, hard={}", soft, hard);

    for warning in nofile_warnings(max_connections, limits, soft, hard) {
        warn!("{}", warning);
    }
}

/// Returns the RLIMIT_NOFILE soft and hard limits, if available.
///
/// Soft: the current effective per-process limit (what actually applies now).
/// Hard: the ceiling for soft; only root can raise soft beyond hard.
fn get_nofile_limits() -> Option<(u64, u64)> {
    rlimit::getrlimit(rlimit::Resource::NOFILE).ok()
}

fn nofile_warnings(
    max_connections: usize,
    limits: Option<(u64, u64)>,
    soft: u64,
    hard: u64,
) -> Vec<String> {
    let mut warnings = Vec::new();

    if limits.is_none() {
        warnings.push("failed to get nofile limits".to_string());
    }

    if max_connections == 0 {
        warnings.push(format!("max_connections is unlimited but soft FD limit is {}; set a specific --max-connections limit or raise ulimit to avoid EMFILE", soft));
    } else if (max_connections as u64) > soft {
        warnings.push(format!("max_connections ({}) exceeds soft FD limit (soft={}, hard={}); increase ulimit or lower max_connections to avoid EMFILE", max_connections, soft, hard));
    }

    warnings
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn warns_when_max_connections_unlimited() {
        let warnings = nofile_warnings(0, Some((1024, 2048)), 1024, 2048);
        assert!(
            warnings
                .iter()
                .any(|w| w.contains("max_connections is unlimited")),
            "should warn when max_connections is zero"
        );
    }

    #[test]
    fn warns_when_max_connections_exceeds_soft_limit() {
        let warnings = nofile_warnings(5000, Some((4000, 8000)), 4000, 8000);
        assert!(
            warnings.iter().any(|w| w.contains("exceeds soft FD limit")),
            "should warn when requested max exceeds soft limit"
        );
    }

    #[test]
    fn no_warning_when_within_limits() {
        let warnings = nofile_warnings(100, Some((1000, 2000)), 1000, 2000);
        assert!(warnings.is_empty(), "no warnings expected within limits");
    }

    #[test]
    fn warns_when_limits_unavailable() {
        let warnings = nofile_warnings(100, None, u64::MAX, u64::MAX);
        assert!(
            warnings
                .iter()
                .any(|w| w.contains("failed to get nofile limits")),
            "should warn when limits cannot be fetched"
        );
    }
}
