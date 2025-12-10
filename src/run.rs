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
    let (soft, hard) = if let Some((soft, hard)) = get_nofile_limits() {
        (soft, hard)
    } else {
        warn!("failed to get nofile limits");
        (u64::MAX, u64::MAX)
    };

    info!("FD limit: soft={}, hard={}", soft, hard);

    if max_connections == 0 {
        warn!(
            "max_connections is unlimited but soft FD limit is {}; set a specific --max-connections limit or raise ulimit to avoid EMFILE",
            soft
        );
    } else if (max_connections as u64) > soft {
        warn!(
            "max_connections ({}) exceeds soft FD limit (soft={}, hard={}); increase ulimit or lower max_connections to avoid EMFILE",
            max_connections, soft, hard
        );
    }
}

/// Returns the RLIMIT_NOFILE soft and hard limits, if available.
///
/// Soft: the current effective per-process limit (what actually applies now).
/// Hard: the ceiling for soft; only root can raise soft beyond hard.
fn get_nofile_limits() -> Option<(u64, u64)> {
    rlimit::getrlimit(rlimit::Resource::NOFILE).ok()
}
