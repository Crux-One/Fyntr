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

#[cfg(test)]
mod tests {
    use super::*;
    use actix::MailboxError;
    use std::time::Duration;
    use tokio::time::timeout;

    /// Tests that the shutdown_signal function is properly configured to listen
    /// for termination signals. We verify this by checking that it doesn't complete
    /// immediately (i.e., it's waiting for a signal).
    #[actix_rt::test]
    async fn shutdown_signal_waits_for_signal() {
        let shutdown_future = shutdown_signal();
        tokio::pin!(shutdown_future);

        // The shutdown_signal should NOT complete immediately - it should be waiting
        // Use a longer timeout to ensure reliability across different systems
        let result = timeout(Duration::from_millis(200), shutdown_future).await;
        assert!(
            result.is_err(),
            "shutdown_signal should not complete without receiving a signal"
        );
    }

    /// Tests that the scheduler correctly handles the Shutdown message
    /// by stopping itself, which is part of the graceful shutdown sequence.
    /// This mirrors the behavior in server() when shutdown_signal completes.
    #[actix_rt::test]
    async fn scheduler_handles_graceful_shutdown() {
        // Create scheduler with connection limit, similar to production configuration
        let scheduler = Scheduler::new(1024, Duration::from_millis(10))
            .with_max_connections(100)
            .start();

        // Send the shutdown message (simulating what happens after shutdown_signal completes)
        let result = scheduler.send(Shutdown).await;
        assert!(
            result.is_ok(),
            "Shutdown message should be delivered successfully"
        );

        // Give the scheduler time to process the shutdown
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify the scheduler has stopped by checking that sending another message fails
        // with MailboxError::Closed, confirming the actor actually stopped rather than a timeout
        let result = scheduler.send(Shutdown).await;
        assert!(
            matches!(result, Err(MailboxError::Closed)),
            "Scheduler should have closed its mailbox after shutdown, but got: {:?}",
            result
        );
    }
}
