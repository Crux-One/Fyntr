use actix::prelude::*;
use bytes::Bytes;
use log::{debug, warn};
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    net::tcp::OwnedWriteHalf,
    sync::watch,
};

#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use tokio::sync::Semaphore;

use crate::{
    actors::scheduler::{Scheduler, WriteCompleted, WriteFailed},
    flow::{
        FlowId,
        idle_timeout::{TunnelActivity, TunnelLifecycle},
    },
};

/// The sole owner of a flow's upstream write half.  `AtomicResponse` keeps
/// mailbox packets behind the in-progress write, preserving wire order.
pub(crate) struct FlowWriter {
    flow_id: FlowId,
    backend_write: Option<OwnedWriteHalf>,
    scheduler: Addr<Scheduler>,
    lifecycle: TunnelLifecycle,
    cancel_rx: watch::Receiver<bool>,
    #[cfg(test)]
    write_gate: Option<Arc<Semaphore>>,
}

impl FlowWriter {
    pub(crate) fn new(
        flow_id: FlowId,
        backend_write: OwnedWriteHalf,
        scheduler: Addr<Scheduler>,
        lifecycle: TunnelLifecycle,
        cancel_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            flow_id,
            backend_write: Some(backend_write),
            scheduler,
            lifecycle,
            cancel_rx,
            #[cfg(test)]
            write_gate: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_write_gate(mut self, write_gate: Arc<Semaphore>) -> Self {
        self.write_gate = Some(write_gate);
        self
    }
}

impl Actor for FlowWriter {
    type Context = Context<Self>;
}

#[derive(Message)]
#[rtype(result = "()")]
pub(crate) struct WritePacket(pub Bytes);

#[derive(Message)]
#[rtype(result = "()")]
pub(crate) struct StopWriter;

enum WriteOutcome<W> {
    Written(W, std::io::Result<()>),
    Cancelled,
}

async fn write_with_cancellation<W>(
    mut backend_write: W,
    packet: Bytes,
    mut shutdown_rx: watch::Receiver<bool>,
    mut cancel_rx: watch::Receiver<bool>,
) -> WriteOutcome<W>
where
    W: AsyncWrite + Unpin,
{
    if *shutdown_rx.borrow() || *cancel_rx.borrow() {
        return WriteOutcome::Cancelled;
    }

    tokio::select! {
        result = backend_write.write_all(&packet) => WriteOutcome::Written(backend_write, result),
        _ = shutdown_rx.changed() => WriteOutcome::Cancelled,
        _ = cancel_rx.changed() => WriteOutcome::Cancelled,
    }
}

impl Handler<WritePacket> for FlowWriter {
    type Result = AtomicResponse<Self, ()>;

    fn handle(&mut self, msg: WritePacket, _ctx: &mut Self::Context) -> Self::Result {
        let bytes = msg.0.len();
        let flow_id = self.flow_id;
        let scheduler = self.scheduler.clone();
        let lifecycle = self.lifecycle.clone();
        let shutdown_rx = lifecycle.subscribe_shutdown();
        let cancel_rx = self.cancel_rx.clone();
        #[cfg(test)]
        let write_gate = self.write_gate.clone();
        let backend_write = self
            .backend_write
            .take()
            .expect("writer accepts one packet at a time");
        AtomicResponse::new(Box::pin(
            async move {
                #[cfg(test)]
                if let Some(write_gate) = write_gate {
                    let mut gate_shutdown_rx = shutdown_rx.clone();
                    let mut gate_cancel_rx = cancel_rx.clone();
                    if *gate_shutdown_rx.borrow() || *gate_cancel_rx.borrow() {
                        return WriteOutcome::Cancelled;
                    }
                    tokio::select! {
                        permit = write_gate.acquire() => {
                            permit.expect("test write gate unexpectedly closed").forget();
                        }
                        _ = gate_shutdown_rx.changed() => return WriteOutcome::Cancelled,
                        _ = gate_cancel_rx.changed() => return WriteOutcome::Cancelled,
                    }
                }

                write_with_cancellation(backend_write, msg.0, shutdown_rx, cancel_rx).await
            }
            .into_actor(self)
            .map(move |outcome, act, ctx| match outcome {
                WriteOutcome::Written(backend_write, Ok(())) => {
                    act.backend_write = Some(backend_write);
                    lifecycle.record_activity(TunnelActivity::BackendWrite);
                    scheduler.do_send(WriteCompleted { flow_id, bytes });
                }
                WriteOutcome::Written(_backend_write, Err(error)) => {
                    warn!("flow{}: backend write error: {}", flow_id.0, error);
                    scheduler.do_send(WriteFailed { flow_id, bytes });
                    ctx.stop();
                }
                WriteOutcome::Cancelled => {
                    debug!("flow{}: backend writer cancelled", flow_id.0);
                    scheduler.do_send(WriteFailed { flow_id, bytes });
                    ctx.stop();
                }
            }),
        ))
    }
}

impl Handler<StopWriter> for FlowWriter {
    type Result = ();
    fn handle(&mut self, _msg: StopWriter, ctx: &mut Self::Context) {
        ctx.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::duplex,
        time::{Duration, timeout},
    };

    #[actix_rt::test]
    async fn cancellation_interrupts_a_blocked_write() {
        let (writer, _reader) = duplex(1);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (cancel_tx, cancel_rx) = watch::channel(false);

        let write = write_with_cancellation(
            writer,
            Bytes::from(vec![0_u8; 1024]),
            shutdown_rx,
            cancel_rx,
        );
        tokio::pin!(write);
        tokio::select! {
            _ = &mut write => panic!("write unexpectedly completed while the reader was blocked"),
            _ = tokio::task::yield_now() => {}
        }

        cancel_tx.send_replace(true);
        let outcome = timeout(Duration::from_secs(1), write)
            .await
            .expect("cancellation should promptly interrupt the blocked write");
        assert!(matches!(outcome, WriteOutcome::Cancelled));
    }
}
