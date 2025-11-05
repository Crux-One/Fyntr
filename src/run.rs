use std::{
    env,
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
    dns::{DnsCacheActor, DnsCacheConfig},
    flow::FlowId,
    http::connect::handle_connect_proxy,
};

const DEFAULT_PORT: u16 = 9999;
const DEFAULT_QUANTUM: usize = 8 * 1024; // 8 KB
const DEFAULT_TICK_MS: u64 = 5; // 5 ms
const DEFAULT_MAX_CONNECTIONS: usize = 1000;
const RESERVED_FILE_DESCRIPTORS: u64 = 32;
const APPROX_FDS_PER_FLOW: u64 = 4;

pub async fn server() -> Result<()> {
    bootstrap();

    let proxy_listen_addr = format!("127.0.0.1:{}", DEFAULT_PORT);
    info!("Starting Fyntr on {}", proxy_listen_addr);
    let listener = TcpListener::bind(proxy_listen_addr).await?;

    // Start the scheduler
    let quantum = DEFAULT_QUANTUM;
    let tick = Duration::from_millis(DEFAULT_TICK_MS);
    let max_connections = get_max_connections();
    let scheduler = Scheduler::new(quantum, tick)
        .with_max_connections(max_connections)
        .start();

    let dns_cache = DnsCacheActor::new(DnsCacheConfig::default())?.start();

    // Start the flow ID counter
    let flow_counter = Arc::new(AtomicUsize::new(1));

    loop {
        let (client_stream, client_addr) = listener.accept().await?;
        let flow_id = FlowId(flow_counter.fetch_add(1, Ordering::SeqCst));

        info!("flow{}: new connection from {}", flow_id.0, client_addr);

        let scheduler = scheduler.clone();
        let dns_cache = dns_cache.clone();

        // Handle each connection in a dedicated task
        actix::spawn(async move {
            if let Err(e) =
                handle_connect_proxy(client_stream, client_addr, flow_id, scheduler, dns_cache)
                    .await
            {
                error!("flow{}: error: {}", flow_id.0, e);
            }
        });
    }
}

fn bootstrap() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
}

fn get_max_connections() -> usize {
    if let Ok(value) = env::var("FYNTR_MAX_CONNECTIONS") {
        match value.parse::<usize>() {
            Ok(parsed) => {
                info!(
                    "Using max connections override from FYNTR_MAX_CONNECTIONS={}",
                    parsed
                );
                return parsed;
            }
            Err(err) => {
                warn!(
                    "Ignoring invalid FYNTR_MAX_CONNECTIONS='{}': {}",
                    value, err
                );
            }
        }
    }

    if let Some((limit, soft_limit)) = infer_connection_limit_from_rlimit() {
        if limit < DEFAULT_MAX_CONNECTIONS {
            info!(
                "Adjusting max connections to {} based on process open file soft limit {}",
                limit, soft_limit
            );
        }
        return limit;
    }

    DEFAULT_MAX_CONNECTIONS
}

fn infer_connection_limit_from_rlimit() -> Option<(usize, u64)> {
    let soft_limit = fd_soft_limit()?;

    let reserved = RESERVED_FILE_DESCRIPTORS.min(soft_limit);
    let available = soft_limit.saturating_sub(reserved);
    let flows = (available / APPROX_FDS_PER_FLOW).max(1);

    let capped = flows
        .min(DEFAULT_MAX_CONNECTIONS as u64) as usize;

    Some((capped, soft_limit))
}

#[cfg(unix)]
fn fd_soft_limit() -> Option<u64> {
    use std::mem::MaybeUninit;

    unsafe {
        let mut limit = MaybeUninit::<libc::rlimit>::uninit();
        if libc::getrlimit(libc::RLIMIT_NOFILE, limit.as_mut_ptr()) == 0 {
            Some(limit.assume_init().rlim_cur as u64)
        } else {
            None
        }
    }
}

#[cfg(not(unix))]
fn fd_soft_limit() -> Option<u64> {
    None
}
