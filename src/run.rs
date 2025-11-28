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
use log::{error, info};
use tokio::net::TcpListener;

use crate::{
    actors::scheduler::{Scheduler, Shutdown},
    flow::FlowId,
    http::connect::handle_connect_proxy,
};

pub const DEFAULT_PORT: u16 = 9999;
pub const DEFAULT_MAX_CONNECTIONS: usize = 1000;
const DEFAULT_QUANTUM: usize = 8 * 1024; // 8 KB
const DEFAULT_TICK_MS: u64 = 5; // 5 ms

pub async fn server(port: u16, max_connections: usize) -> Result<()> {
    bootstrap();

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

    let shutdown_signal = shutdown_signal();
    tokio::pin!(shutdown_signal);

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (client_stream, client_addr) = accept_result?;
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
            _ = &mut shutdown_signal => {
                info!("Shutdown signal received, stopping listener");
                break;
            }
        }
    }

    info!("Stopping scheduler");
    if let Err(err) = scheduler.send(Shutdown).await {
        error!("Failed to stop scheduler gracefully: {}", err);
    }

    System::current().stop();

    Ok(())
}

fn bootstrap() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    }
}
