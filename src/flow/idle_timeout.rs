use actix::prelude::*;
use log::info;
use tokio::{
    sync::watch,
    time::{Duration, Instant, sleep_until},
};

use super::{FlowId, connection::unregister_flow};
use crate::actors::scheduler::Scheduler;

pub(super) type TunnelShutdownReceiver = watch::Receiver<bool>;

#[derive(Clone)]
pub(super) struct TunnelTrafficSignal {
    activity_tx: watch::Sender<Instant>,
    shutdown_tx: watch::Sender<bool>,
}

impl TunnelTrafficSignal {
    pub(super) fn new() -> Self {
        let (activity_tx, _activity_rx) = watch::channel(Instant::now());
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        Self {
            activity_tx,
            shutdown_tx,
        }
    }

    pub(super) fn record(&self) {
        let _ = self.activity_tx.send(Instant::now());
    }

    pub(super) fn subscribe_shutdown(&self) -> TunnelShutdownReceiver {
        self.shutdown_tx.subscribe()
    }

    fn subscribe_activity(&self) -> watch::Receiver<Instant> {
        self.activity_tx.subscribe()
    }

    fn shutdown_sender(&self) -> watch::Sender<bool> {
        self.shutdown_tx.clone()
    }
}

pub(super) fn start_idle_timeout_monitor(
    flow_id: FlowId,
    scheduler: Addr<Scheduler>,
    traffic_signal: &TunnelTrafficSignal,
    idle_timeout: Duration,
) {
    let mut activity_rx = traffic_signal.subscribe_activity();
    let shutdown_tx = traffic_signal.shutdown_sender();

    actix::spawn(async move {
        loop {
            let deadline = *activity_rx.borrow() + idle_timeout;
            tokio::select! {
                _ = sleep_until(deadline) => {
                    let idle_for = Instant::now().saturating_duration_since(*activity_rx.borrow());
                    if idle_for >= idle_timeout {
                        info!(
                            "flow{}: idle timeout after {:.2}s; closing tunnel",
                            flow_id.0,
                            idle_for.as_secs_f64()
                        );
                        let _ = shutdown_tx.send(true);
                        unregister_flow(scheduler, flow_id).await;
                        return;
                    }
                }
                changed = activity_rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
            }
        }
    });
}
