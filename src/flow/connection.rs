use super::FlowId;

use actix::prelude::*;
use bytes::Bytes;
use log::{debug, info, warn};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    time::{Duration, Instant, sleep},
};

use crate::{
    actors::{
        queue::{Close, Enqueue, EnqueueError, QueueActor},
        scheduler::{CanAcceptConnection, RecordDownstreamBytes, Scheduler, Unregister},
    },
    util::{format_bytes, format_rate},
};

use super::idle_timeout::{
    TunnelActivity, TunnelLifecycle, TunnelShutdownReceiver, start_idle_timeout_monitor,
};

const BUFFER_SIZE: usize = 8 * 1024; // 8 KB
const ENQUEUE_BACKPRESSURE_RETRY_BASE_DELAY: Duration = Duration::from_millis(5);
const ENQUEUE_BACKPRESSURE_RETRY_MAX_DELAY: Duration = Duration::from_millis(100);
const ENQUEUE_BACKPRESSURE_LOG_EVERY: u32 = 200;
const UNREGISTER_RETRY_LIMIT: usize = 3;
const UNREGISTER_RETRY_DELAY_MS: u64 = 50;
const UNREGISTER_RETRY_MAX_DELAY_SECS: u64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TunnelCloseReason {
    IdleTimeout,
    ClientClosed,
    BackendClosed,
    ClientReadError,
    BackendReadError,
    ClientWriteError,
    QueueClosed,
}

#[derive(Debug)]
pub(crate) enum SchedulerCapacityError {
    AtCapacity {
        phase: &'static str,
    },
    Unavailable {
        phase: &'static str,
        source: MailboxError,
    },
}

impl std::fmt::Display for SchedulerCapacityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AtCapacity { phase } => write!(f, "scheduler at capacity {}", phase),
            Self::Unavailable { phase, source } => {
                write!(f, "failed to check capacity {}: {}", phase, source)
            }
        }
    }
}

impl std::error::Error for SchedulerCapacityError {}

pub(crate) async fn ensure_scheduler_capacity(
    scheduler: &Addr<Scheduler>,
    phase: &'static str,
) -> Result<(), SchedulerCapacityError> {
    match scheduler.send(CanAcceptConnection).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(SchedulerCapacityError::AtCapacity { phase }),
        Err(source) => Err(SchedulerCapacityError::Unavailable { phase, source }),
    }
}

/// RAII guard that tears down queue and scheduler registrations if a flow exits early.
///
/// Dropping sends `Close` to an unregistered queue, or `Unregister` for a registered flow, unless
/// `disarm` was called after the relay actors took ownership of the flow lifecycle.
pub(crate) struct FlowCleanup {
    flow_id: FlowId,
    scheduler: Addr<Scheduler>,
    queue_tx: Option<Addr<QueueActor>>,
    unregister_on_drop: bool,
    disarmed: bool,
}

impl FlowCleanup {
    pub(crate) fn new(flow_id: FlowId, scheduler: Addr<Scheduler>) -> Self {
        Self {
            flow_id,
            scheduler,
            queue_tx: None,
            unregister_on_drop: false,
            disarmed: false,
        }
    }

    pub(crate) fn watch_queue(&mut self, queue_tx: Addr<QueueActor>) {
        self.queue_tx = Some(queue_tx);
    }

    pub(crate) fn mark_registered(&mut self) {
        self.unregister_on_drop = true;
    }

    pub(crate) fn disarm(&mut self) {
        self.disarmed = true;
    }
}

impl Drop for FlowCleanup {
    fn drop(&mut self) {
        if self.disarmed {
            return;
        }

        if !self.unregister_on_drop
            && let Some(queue) = &self.queue_tx
        {
            queue.do_send(Close);
        }

        if self.unregister_on_drop {
            self.scheduler.do_send(Unregister {
                flow_id: self.flow_id,
            });
        }
    }
}

#[derive(Clone)]
pub(crate) struct TunnelContext {
    pub(crate) flow_id: FlowId,
    pub(crate) scheduler: Addr<Scheduler>,
    pub(crate) lifecycle: TunnelLifecycle,
    pub(crate) idle_timeout: Option<Duration>,
}

impl TunnelContext {
    fn record_activity(&self, activity: TunnelActivity) {
        self.lifecycle.record_activity(activity);
    }

    fn subscribe_shutdown(&self) -> TunnelShutdownReceiver {
        self.lifecycle.subscribe_shutdown()
    }

    fn initial_idle_shutdown_requested(
        &self,
        shutdown_rx: &TunnelShutdownReceiver,
        relay_name: &str,
    ) -> bool {
        if !*shutdown_rx.borrow() {
            return false;
        }

        debug!(
            "flow{}: {} relay stopping after idle timeout",
            self.flow_id.0, relay_name
        );
        true
    }

    fn start_idle_timeout_monitor(&self) {
        if let Some(timeout) = self.idle_timeout {
            start_idle_timeout_monitor(
                self.flow_id,
                self.scheduler.clone(),
                &self.lifecycle,
                timeout,
            );
        }
    }

    async fn unregister(&self, reason: TunnelCloseReason) {
        unregister_flow(self.scheduler.clone(), self.flow_id, reason).await;
    }
}

pub(crate) struct TunnelIo {
    pub(crate) client_read: OwnedReadHalf,
    pub(crate) queue_tx: Addr<QueueActor>,
    pub(crate) backend_read: OwnedReadHalf,
    pub(crate) client_write: OwnedWriteHalf,
}

pub(crate) struct BidirectionalTunnel {
    pub(crate) context: TunnelContext,
    pub(crate) io: TunnelIo,
}

pub(crate) fn start_bidirectional_tunnel(tunnel: BidirectionalTunnel) {
    let BidirectionalTunnel { context, io } = tunnel;

    context.record_activity(TunnelActivity::TunnelStarted);
    context.start_idle_timeout_monitor();

    ClientToBackendActor::new(context.clone(), io.client_read, io.queue_tx).start();
    BackendToClientActor::new(context, io.backend_read, io.client_write).start();
}

pub(crate) struct ClientToBackendActor {
    context: TunnelContext,
    client: Option<OwnedReadHalf>,
    queue_tx: Addr<QueueActor>,
}

impl ClientToBackendActor {
    fn new(context: TunnelContext, client: OwnedReadHalf, queue_tx: Addr<QueueActor>) -> Self {
        Self {
            context,
            client: Some(client),
            queue_tx,
        }
    }

    async fn enqueue_with_backpressure(
        flow_id: FlowId,
        queue_tx: &Addr<QueueActor>,
        chunk: Bytes,
    ) -> bool {
        let mut retries = 0u32;
        let mut delay = ENQUEUE_BACKPRESSURE_RETRY_BASE_DELAY;
        loop {
            match queue_tx.send(Enqueue(chunk.clone())).await {
                Ok(Ok(())) => {
                    if retries > 0 {
                        debug!(
                            "flow{}: queue pressure relieved after {} retries",
                            flow_id.0, retries
                        );
                    }
                    return true;
                }
                Ok(Err(EnqueueError::QueueBufferExceeded {
                    attempted_total,
                    max_buffered_bytes,
                })) => {
                    retries = retries.saturating_add(1);
                    if retries == 1 || retries.is_multiple_of(ENQUEUE_BACKPRESSURE_LOG_EVERY) {
                        warn!(
                            "flow{}: queue buffered bytes {} exceeds limit {}; pausing upstream reads",
                            flow_id.0, attempted_total, max_buffered_bytes
                        );
                    }
                    sleep(delay).await;
                    delay = delay
                        .saturating_mul(2)
                        .min(ENQUEUE_BACKPRESSURE_RETRY_MAX_DELAY);
                }
                Ok(Err(e)) => {
                    warn!("flow{}: enqueue rejected: {}", flow_id.0, e);
                    return false;
                }
                Err(e) => {
                    warn!("flow{}: failed to enqueue chunk: {}", flow_id.0, e);
                    return false;
                }
            }
        }
    }
}

impl Actor for ClientToBackendActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let tunnel_context = self.context.clone();
        let flow_id = tunnel_context.flow_id;
        let queue_tx = self.queue_tx.clone();
        let mut shutdown_rx = tunnel_context.subscribe_shutdown();
        let mut client = self
            .client
            .take()
            .expect("ClientToBackendActor started without client stream");

        ctx.spawn(
            async move {
                if tunnel_context
                    .initial_idle_shutdown_requested(&shutdown_rx, "client-to-backend")
                {
                    tunnel_context.unregister(TunnelCloseReason::IdleTimeout).await;
                    return;
                }

                let mut buf = vec![0u8; BUFFER_SIZE];
                let mut total_read = 0u64;
                let start_time = Instant::now();
                let close_reason;

                loop {
                    tokio::select! {
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                debug!(
                                    "flow{}: client-to-backend relay stopping after idle timeout",
                                    flow_id.0
                                );
                                close_reason = TunnelCloseReason::IdleTimeout;
                                break;
                            }
                        }
                        read_result = client.read(&mut buf) => {
                            match read_result {
                                Ok(0) => {
                                    let elapsed = start_time.elapsed().as_secs_f64();
                                    let (total_value, total_unit) = format_bytes(total_read);

                                    if let Some((avg_value, avg_unit)) = format_rate(total_read, elapsed) {
                                        info!(
                                            "flow{}: client connection closed (upstream total: {:.2} {} in {:.2}s, avg: {:.2} {})",
                                            flow_id.0, total_value, total_unit, elapsed, avg_value, avg_unit
                                        );
                                    } else {
                                        info!(
                                            "flow{}: client connection closed (upstream total: {:.2} {} in {:.2}s)",
                                            flow_id.0, total_value, total_unit, elapsed
                                        );
                                    }

                                    close_reason = TunnelCloseReason::ClientClosed;
                                    break;
                                }
                                Ok(n) => {
                                    tunnel_context.record_activity(TunnelActivity::ClientRead);
                                    total_read += n as u64;
                                    let chunk = Bytes::copy_from_slice(&buf[..n]);
                                    if !Self::enqueue_with_backpressure(flow_id, &queue_tx, chunk).await {
                                        close_reason = TunnelCloseReason::QueueClosed;
                                        break;
                                    }
                                }
                                Err(e) => {
                                    warn!("flow{}: read error: {}", flow_id.0, e);
                                    close_reason = TunnelCloseReason::ClientReadError;
                                    break;
                                }
                            }
                        }
                    }
                }

                tunnel_context.unregister(close_reason).await;
            }
            .into_actor(self)
            .map(|_, _act, ctx| ctx.stop()),
        );
    }
}

pub(super) async fn unregister_flow(
    scheduler: Addr<Scheduler>,
    flow_id: FlowId,
    reason: TunnelCloseReason,
) {
    // Unregister this flow from the scheduler to signal completion and trigger cleanup.
    let mut delay = Duration::from_millis(UNREGISTER_RETRY_DELAY_MS);
    let max_delay = Duration::from_secs(UNREGISTER_RETRY_MAX_DELAY_SECS);
    for attempt in 1..=UNREGISTER_RETRY_LIMIT {
        match scheduler.send(Unregister { flow_id }).await {
            Ok(_) => {
                debug!(
                    "flow{}: successfully unregistered from scheduler ({:?})",
                    flow_id.0, reason
                );
                return;
            }
            Err(e) if attempt == UNREGISTER_RETRY_LIMIT => {
                warn!(
                    "flow{}: failed to unregister from scheduler after {} attempts ({:?}): {}",
                    flow_id.0, attempt, reason, e
                );
            }
            Err(e) => {
                warn!(
                    "flow{}: unregister attempt {}/{} failed ({:?}): {}; retrying",
                    flow_id.0, attempt, UNREGISTER_RETRY_LIMIT, reason, e
                );
                sleep(delay).await;
                delay = delay.saturating_mul(2).min(max_delay);
            }
        }
    }
}

pub(crate) struct BackendToClientActor {
    context: TunnelContext,
    backend: Option<OwnedReadHalf>,
    client: Option<OwnedWriteHalf>,
}

impl BackendToClientActor {
    fn new(context: TunnelContext, backend: OwnedReadHalf, client: OwnedWriteHalf) -> Self {
        Self {
            context,
            backend: Some(backend),
            client: Some(client),
        }
    }
}

impl Actor for BackendToClientActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let tunnel_context = self.context.clone();
        let flow_id = tunnel_context.flow_id;
        let mut backend = self
            .backend
            .take()
            .expect("BackendToClientActor started without backend stream");
        let mut client = self
            .client
            .take()
            .expect("BackendToClientActor started without client stream");
        let scheduler = tunnel_context.scheduler.clone();
        let mut shutdown_rx = tunnel_context.subscribe_shutdown();

        ctx.spawn(
            async move {
                if tunnel_context
                    .initial_idle_shutdown_requested(&shutdown_rx, "backend-to-client")
                {
                    return;
                }

                let mut buf = vec![0u8; BUFFER_SIZE];
                let mut total_read = 0u64;
                let start_time = Instant::now();
                let close_reason;
                loop {
                    tokio::select! {
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                debug!(
                                    "flow{}: backend-to-client relay stopping after idle timeout",
                                    flow_id.0
                                );
                                close_reason = TunnelCloseReason::IdleTimeout;
                                break;
                            }
                        }
                        read_result = backend.read(&mut buf) => {
                            match read_result {
                                Ok(0) => {
                                    let elapsed = start_time.elapsed().as_secs_f64();
                                    let (total_value, total_unit) = format_bytes(total_read);

                                    if let Some((avg_value, avg_unit)) = format_rate(total_read, elapsed) {
                                        info!(
                                            "flow{}: backend connection closed (downstream total: {:.2} {} in {:.2}s, avg: {:.2} {})",
                                            flow_id.0, total_value, total_unit, elapsed, avg_value, avg_unit
                                        );
                                    } else {
                                        info!(
                                            "flow{}: backend connection closed (downstream total: {:.2} {} in {:.2}s)",
                                            flow_id.0, total_value, total_unit, elapsed
                                        );
                                    }
                                    close_reason = TunnelCloseReason::BackendClosed;
                                    break;
                                }
                                Ok(n) => {
                                    tunnel_context.record_activity(TunnelActivity::BackendRead);
                                    total_read += n as u64;
                                    tokio::select! {
                                        write_result = client.write_all(&buf[..n]) => {
                                            if let Err(e) = write_result {
                                                warn!("flow{}: client write error: {}", flow_id.0, e);
                                                close_reason = TunnelCloseReason::ClientWriteError;
                                                break;
                                            }
                                        }
                                        changed = shutdown_rx.changed() => {
                                            if changed.is_err() || *shutdown_rx.borrow() {
                                                debug!(
                                                    "flow{}: backend-to-client relay stopping during client write after idle timeout",
                                                    flow_id.0
                                                );
                                                close_reason = TunnelCloseReason::IdleTimeout;
                                                break;
                                            }
                                        }
                                    }
                                    scheduler.do_send(RecordDownstreamBytes { bytes: n });
                                }
                                Err(e) => {
                                    warn!("flow{}: backend read error: {}", flow_id.0, e);
                                    close_reason = TunnelCloseReason::BackendReadError;
                                    break;
                                }
                            }
                        }
                    }
                }
                // Backend-side close reasons are informational here: they are logged for
                // observability, but do not drive scheduler unregister. Flow lifecycle
                // ownership remains with the existing unregister path.
                debug!(
                    "flow{}: backend-to-client relay finished ({:?})",
                    flow_id.0, close_reason
                );
            }
            .into_actor(self)
            .map(|_, _act, ctx| ctx.stop()),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actors::{
        queue::{AddQuantum, Dequeue, Enqueue, EnqueueError, QueueActor},
        scheduler::{Register, Scheduler},
    };
    use crate::limits::{MAX_DEQUEUE_BYTES, MAX_QUEUE_PACKET_BYTES};
    use crate::test_utils::make_backend_write;
    use tokio::{
        net::{TcpListener, TcpStream},
        time::{Duration, sleep, timeout},
    };

    async fn build_server_read_half() -> OwnedReadHalf {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let accept_handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            stream
        });

        let client = TcpStream::connect(addr).await.unwrap();
        drop(client);

        let server = accept_handle.await.unwrap();
        let (read_half, _write_half) = server.into_split();
        read_half
    }

    async fn build_live_read_half() -> (OwnedReadHalf, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let accept_handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            stream
        });

        let client = TcpStream::connect(addr).await.unwrap();
        let server = accept_handle.await.unwrap();
        let (read_half, _write_half) = server.into_split();
        (read_half, client)
    }

    async fn build_live_write_half() -> (OwnedWriteHalf, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let accept_handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            stream
        });

        let client = TcpStream::connect(addr).await.unwrap();
        let server = accept_handle.await.unwrap();
        let (_read_half, write_half) = server.into_split();
        (write_half, client)
    }

    async fn fill_queue_until_buffer_exceeded(queue: &Addr<QueueActor>) {
        let packet = Bytes::from(vec![0u8; MAX_QUEUE_PACKET_BYTES]);
        loop {
            match queue.send(Enqueue(packet.clone())).await.unwrap() {
                Ok(()) => {}
                Err(EnqueueError::QueueBufferExceeded { .. }) => break,
                Err(other) => panic!("unexpected enqueue error while filling queue: {other}"),
            }
        }
    }

    #[actix_rt::test]
    async fn client_disconnect_triggers_unregister_and_queue_stop() {
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600)).start();

        let queue = QueueActor::new().start();
        let backend_write = make_backend_write().await;
        scheduler
            .send(Register {
                flow_id: FlowId(5),
                queue_addr: queue.clone(),
                backend_write,
                tunnel_lifecycle: TunnelLifecycle::new(),
            })
            .await
            .unwrap()
            .unwrap();

        let client_read = build_server_read_half().await;
        let tunnel_lifecycle = TunnelLifecycle::new();
        ClientToBackendActor::new(
            TunnelContext {
                flow_id: FlowId(5),
                scheduler,
                lifecycle: tunnel_lifecycle,
                idle_timeout: None,
            },
            client_read,
            queue.clone(),
        )
        .start();

        let mut stopped = false;
        for _ in 0..20 {
            if queue.send(Dequeue { max_bytes: 1024 }).await.is_err() {
                stopped = true;
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }

        assert!(
            stopped,
            "queue should stop after client disconnect unregisters flow"
        );
    }

    #[actix_rt::test]
    async fn client_to_backend_actor_honors_initial_idle_shutdown() {
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600)).start();

        let queue = QueueActor::new().start();
        let backend_write = make_backend_write().await;
        scheduler
            .send(Register {
                flow_id: FlowId(8),
                queue_addr: queue.clone(),
                backend_write,
                tunnel_lifecycle: TunnelLifecycle::new(),
            })
            .await
            .unwrap()
            .unwrap();

        let (client_read, _client_peer) = build_live_read_half().await;
        let tunnel_lifecycle = TunnelLifecycle::new();
        tunnel_lifecycle.shutdown_for_test();
        ClientToBackendActor::new(
            TunnelContext {
                flow_id: FlowId(8),
                scheduler,
                lifecycle: tunnel_lifecycle,
                idle_timeout: None,
            },
            client_read,
            queue.clone(),
        )
        .start();

        let mut stopped = false;
        for _ in 0..20 {
            if queue.send(Dequeue { max_bytes: 1024 }).await.is_err() {
                stopped = true;
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }

        assert!(
            stopped,
            "queue should stop when relay starts after idle shutdown was already requested"
        );
    }

    #[actix_rt::test]
    async fn backend_to_client_actor_honors_initial_idle_shutdown() {
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600)).start();

        let (backend_read, _backend_peer) = build_live_read_half().await;
        let (client_write, mut client_peer) = build_live_write_half().await;
        let tunnel_lifecycle = TunnelLifecycle::new();
        tunnel_lifecycle.shutdown_for_test();
        BackendToClientActor::new(
            TunnelContext {
                flow_id: FlowId(9),
                scheduler,
                lifecycle: tunnel_lifecycle,
                idle_timeout: None,
            },
            backend_read,
            client_write,
        )
        .start();

        let mut buf = [0_u8; 1];
        let read_result = timeout(Duration::from_secs(1), client_peer.read(&mut buf))
            .await
            .expect("backend-to-client actor should stop promptly after initial idle shutdown")
            .expect("client peer read should complete without I/O error");

        assert_eq!(
            read_result, 0,
            "client write half should close when relay starts after idle shutdown was already requested"
        );
    }

    #[actix_rt::test]
    async fn idle_timeout_unregisters_flow_and_stops_queue() {
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600)).start();

        let queue = QueueActor::new().start();
        let backend_write = make_backend_write().await;
        scheduler
            .send(Register {
                flow_id: FlowId(6),
                queue_addr: queue.clone(),
                backend_write,
                tunnel_lifecycle: TunnelLifecycle::new(),
            })
            .await
            .unwrap()
            .unwrap();

        let (client_read, _client_peer) = build_live_read_half().await;
        let (backend_read, _backend_peer) = build_live_read_half().await;
        let (client_write, _client_write_peer) = build_live_write_half().await;
        let tunnel_lifecycle = TunnelLifecycle::new();
        start_bidirectional_tunnel(BidirectionalTunnel {
            context: TunnelContext {
                flow_id: FlowId(6),
                scheduler,
                lifecycle: tunnel_lifecycle,
                idle_timeout: Some(Duration::from_millis(20)),
            },
            io: TunnelIo {
                client_read,
                queue_tx: queue.clone(),
                backend_read,
                client_write,
            },
        });

        let mut stopped = false;
        for _ in 0..20 {
            if queue.send(Dequeue { max_bytes: 1024 }).await.is_err() {
                stopped = true;
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }

        assert!(stopped, "queue should stop after tunnel idle timeout");
    }

    #[actix_rt::test]
    async fn idle_timeout_starts_when_tunnel_starts() {
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600)).start();

        let queue = QueueActor::new().start();
        let backend_write = make_backend_write().await;
        let tunnel_lifecycle = TunnelLifecycle::new();
        scheduler
            .send(Register {
                flow_id: FlowId(7),
                queue_addr: queue.clone(),
                backend_write,
                tunnel_lifecycle: tunnel_lifecycle.clone(),
            })
            .await
            .unwrap()
            .unwrap();

        sleep(Duration::from_millis(120)).await;

        let (client_read, _client_peer) = build_live_read_half().await;
        let (backend_read, _backend_peer) = build_live_read_half().await;
        let (client_write, _client_write_peer) = build_live_write_half().await;
        start_bidirectional_tunnel(BidirectionalTunnel {
            context: TunnelContext {
                flow_id: FlowId(7),
                scheduler,
                lifecycle: tunnel_lifecycle,
                idle_timeout: Some(Duration::from_millis(100)),
            },
            io: TunnelIo {
                client_read,
                queue_tx: queue.clone(),
                backend_read,
                client_write,
            },
        });

        sleep(Duration::from_millis(20)).await;

        assert!(
            queue.send(Dequeue { max_bytes: 1024 }).await.is_ok(),
            "idle timeout should start from tunnel start, not signal creation"
        );
    }

    #[actix_rt::test]
    async fn enqueue_with_backpressure_retries_until_queue_has_capacity() {
        let queue = QueueActor::new().start();
        fill_queue_until_buffer_exceeded(&queue).await;

        let queue_for_drain = queue.clone();
        let drain_handle = tokio::spawn(async move {
            sleep(Duration::from_millis(20)).await;
            queue_for_drain.do_send(AddQuantum(MAX_QUEUE_PACKET_BYTES));
            queue_for_drain
                .send(Dequeue {
                    max_bytes: MAX_DEQUEUE_BYTES,
                })
                .await
                .expect("dequeue request should succeed")
        });

        let enqueue_result = timeout(
            Duration::from_secs(1),
            ClientToBackendActor::enqueue_with_backpressure(
                FlowId(999),
                &queue,
                Bytes::from(vec![1u8; 1024]),
            ),
        )
        .await
        .expect("enqueue_with_backpressure should not hang");

        let dequeue_result = drain_handle
            .await
            .expect("drain task should complete without panic");
        assert!(
            dequeue_result.is_some(),
            "drain dequeue should return Some once quantum is added"
        );

        assert!(
            enqueue_result,
            "enqueue should eventually succeed once queue capacity is available"
        );
    }
}
