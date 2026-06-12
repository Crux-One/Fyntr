use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};

mod connection_admission;
mod flow_stats;
mod quantum_strategy;

use actix::prelude::*;
use log::{debug, info, trace, warn};
use tokio::{io::AsyncWriteExt, net::tcp::OwnedWriteHalf, sync::Mutex, time::Instant};

use crate::{
    actors::queue::{AddQuantum, BindScheduler, Dequeue, DequeueResult, QueueActor, StopNow},
    flow::{FlowId, idle_timeout::TunnelTrafficSignal},
    limits::{MAX_DEQUEUE_BYTES, MaxConnections, max_connections_display},
    util::{format_bytes, format_rate},
};

use self::connection_admission::ConnectionAdmission;
use self::flow_stats::FlowStats;

pub(crate) use crate::actors::connection_limit::RegisterError;

#[derive(Message)]
#[rtype(result = "Result<(), RegisterError>")]
pub(crate) struct Register {
    pub flow_id: FlowId,
    pub queue_addr: Addr<QueueActor>,
    pub backend_write: Arc<Mutex<OwnedWriteHalf>>,
    pub traffic_signal: TunnelTrafficSignal,
}

/// Returns whether the scheduler can admit another connection right now.
#[derive(Message)]
#[rtype(result = "bool")]
pub(crate) struct CanAcceptConnection;

#[derive(Message)]
#[rtype(result = "Result<(), RegisterError>")]
pub(crate) struct TryStartConnectionTask {
    pub flow_id: FlowId,
}

#[derive(Message)]
#[rtype(result = "()")]
pub(crate) struct Unregister {
    pub flow_id: FlowId,
}

#[derive(Message)]
#[rtype(result = "()")]
pub(crate) struct FlowReady {
    pub flow_id: FlowId,
}

#[derive(Message)]
#[rtype(result = "()")]
pub(crate) struct RecordDownstreamBytes {
    pub bytes: usize,
}

struct FlowEntry {
    queue_addr: Addr<QueueActor>,
    backend_write: Arc<Mutex<OwnedWriteHalf>>,
    traffic_signal: TunnelTrafficSignal,
    stats: FlowStats,
}

impl FlowEntry {
    fn new(
        queue_addr: Addr<QueueActor>,
        backend_write: Arc<Mutex<OwnedWriteHalf>>,
        traffic_signal: TunnelTrafficSignal,
    ) -> Self {
        Self {
            queue_addr,
            backend_write,
            traffic_signal,
            stats: FlowStats::new(),
        }
    }

    fn queue_addr(&self) -> Addr<QueueActor> {
        self.queue_addr.clone()
    }

    fn backend_write(&self) -> Arc<Mutex<OwnedWriteHalf>> {
        self.backend_write.clone()
    }

    fn traffic_signal(&self) -> TunnelTrafficSignal {
        self.traffic_signal.clone()
    }

    fn update_stats(&mut self, bytes: usize) {
        self.stats.update(bytes);
    }

    /// Calculates the optimal Deficit Round Robin (DRR) quantum for this flow using the shared
    /// strategy and any available packet statistics.
    ///
    /// The call remains a thin wrapper that forwards to the reusable `DrrQuantumStrategy`, keeping
    /// the decision logic centralized while allowing each flow to supply its own packet history.
    fn recommended_quantum(&self, default_quantum: usize) -> usize {
        quantum_strategy::DEFAULT_DRR_QUANTUM_STRATEGY
            .recommended_quantum(self.stats.avg_packet_size(), default_quantum)
    }
}

pub(crate) struct Scheduler {
    flows: HashMap<FlowId, FlowEntry>,
    ready_queue: VecDeque<FlowId>,
    ready_set: HashSet<FlowId>,
    default_quantum: usize,
    tick: Duration,
    total_client_to_backend_bytes: u64,
    total_backend_to_client_bytes: u64,
    global_start_time: Option<Instant>,
    total_ticks: u64,
    admission: ConnectionAdmission,
    shutdown_requested: bool,
}
impl Actor for Scheduler {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let tick = self.tick;
        ctx.run_interval(tick, |_act, ctx| {
            ctx.address().do_send(QuantumTick);
        });
    }
}

// QuantumTick message and handler
#[derive(Message)]
#[rtype(result = "()")]
pub(crate) struct QuantumTick;

#[derive(Message)]
#[rtype(result = "()")]
pub(crate) struct Shutdown;

#[derive(Message)]
#[rtype(result = "()")]
pub(crate) struct ConnectionTaskFinished {
    pub flow_id: FlowId,
}

pub(crate) struct PendingConnectionReservation {
    scheduler: Addr<Scheduler>,
    flow_id: FlowId,
    armed: bool,
}

impl PendingConnectionReservation {
    pub(crate) fn new(scheduler: Addr<Scheduler>, flow_id: FlowId) -> Self {
        Self {
            scheduler,
            flow_id,
            armed: true,
        }
    }

    #[cfg(test)]
    pub(crate) fn already_consumed(scheduler: Addr<Scheduler>, flow_id: FlowId) -> Self {
        Self {
            scheduler,
            flow_id,
            armed: false,
        }
    }

    pub(crate) fn consumed_by_register(mut self) {
        self.armed = false;
    }
}

impl Drop for PendingConnectionReservation {
    fn drop(&mut self) {
        if self.armed {
            self.scheduler.do_send(ConnectionTaskFinished {
                flow_id: self.flow_id,
            });
        }
    }
}

impl Handler<QuantumTick> for Scheduler {
    type Result = ();

    fn handle(&mut self, _msg: QuantumTick, ctx: &mut Self::Context) -> Self::Result {
        self.total_ticks += 1;
        self.distribute_quantum(ctx);
        self.round_robin_once(ctx);
        if self.total_ticks.is_multiple_of(500) {
            self.log_stats();
        }
    }
}

impl Handler<Shutdown> for Scheduler {
    type Result = ();

    fn handle(&mut self, _msg: Shutdown, ctx: &mut Self::Context) -> Self::Result {
        self.shutdown_requested = true;
        if self.should_stop() {
            ctx.stop();
        }
    }
}

impl Handler<TryStartConnectionTask> for Scheduler {
    type Result = Result<(), RegisterError>;

    fn handle(&mut self, msg: TryStartConnectionTask, _ctx: &mut Self::Context) -> Self::Result {
        self.admission.try_start_connection_task(msg.flow_id)
    }
}

impl Handler<ConnectionTaskFinished> for Scheduler {
    type Result = ();

    fn handle(&mut self, msg: ConnectionTaskFinished, ctx: &mut Self::Context) -> Self::Result {
        self.finish_pending_connection_task(msg.flow_id);

        if self.should_stop() {
            ctx.stop();
        }
    }
}

impl Handler<RecordDownstreamBytes> for Scheduler {
    type Result = ();

    fn handle(&mut self, msg: RecordDownstreamBytes, _ctx: &mut Self::Context) -> Self::Result {
        self.total_backend_to_client_bytes += msg.bytes as u64;
        self.ensure_global_start_time();
    }
}

impl Scheduler {
    pub(crate) fn new(quantum: usize, tick: Duration) -> Self {
        Self {
            flows: HashMap::new(),
            ready_queue: VecDeque::new(),
            ready_set: HashSet::new(),
            default_quantum: quantum,
            tick,
            total_client_to_backend_bytes: 0,
            total_backend_to_client_bytes: 0,
            global_start_time: None,
            total_ticks: 0,
            admission: ConnectionAdmission::new(),
            shutdown_requested: false,
        }
    }

    /// Configure the scheduler with a maximum concurrent connection limit.
    ///
    /// Use `None` to allow unlimited connections.
    pub(crate) fn with_max_connections(mut self, max_connections: MaxConnections) -> Self {
        self.admission.set_max_connections(max_connections);
        self
    }

    fn try_increment_connection_count(&self) -> Result<(), RegisterError> {
        self.admission.try_acquire_registered()
    }

    fn decrement_connection_count(&self) {
        self.admission.release_registered();
    }

    fn current_connection_count(&self) -> usize {
        self.admission.current_connection_count()
    }

    fn finish_pending_connection_task(&mut self, flow_id: FlowId) -> bool {
        self.admission.finish_pending_connection_task(flow_id)
    }

    fn max_connections(&self) -> MaxConnections {
        self.admission.max_connections()
    }

    fn should_stop(&self) -> bool {
        self.shutdown_requested
            && self.flows.is_empty()
            && self.admission.pending_connection_tasks() == 0
    }

    fn log_connection_count(&self, flow_id: FlowId, action: &str) {
        let pending = self.admission.pending_connection_tasks();
        match self.max_connections() {
            Some(limit) => info!(
                "flow{}: {} (connections: {}/{}, pending_connect_tasks: {})",
                flow_id.0,
                action,
                self.current_connection_count(),
                limit,
                pending
            ),
            None => info!(
                "flow{}: {} (connections: {}, pending_connect_tasks: {})",
                flow_id.0,
                action,
                self.current_connection_count(),
                pending
            ),
        };
    }

    fn register(
        &mut self,
        id: FlowId,
        queue_addr: Addr<QueueActor>,
        backend_write: Arc<Mutex<OwnedWriteHalf>>,
        traffic_signal: TunnelTrafficSignal,
    ) {
        self.finish_pending_connection_task(id);
        self.flows.insert(
            id,
            FlowEntry::new(queue_addr, backend_write, traffic_signal),
        );
    }

    fn unregister(&mut self, id: FlowId) -> bool {
        self.remove_flows(&[id]) > 0
    }

    fn flow_mut(&mut self, id: FlowId) -> Option<&mut FlowEntry> {
        self.flows.get_mut(&id)
    }

    fn flow(&self, id: FlowId) -> Option<&FlowEntry> {
        self.flows.get(&id)
    }

    fn mark_flow_ready(&mut self, id: FlowId) {
        if self.flow(id).is_some() && self.ready_set.insert(id) {
            self.ready_queue.push_back(id);
        }
    }

    fn remove_flow_from_ready(&mut self, id: FlowId) {
        if self.ready_set.remove(&id) {
            // retain walks the whole queue, so this removal stays O(n) just like
            // a manual position+remove scan would.
            self.ready_queue.retain(|flow_id| *flow_id != id);
        }
    }
}

// Handler for Register
impl Handler<Register> for Scheduler {
    type Result = Result<(), RegisterError>;

    fn handle(&mut self, msg: Register, ctx: &mut Self::Context) -> Self::Result {
        self.try_increment_connection_count()?;
        let flow_id = msg.flow_id;
        let queue_addr = msg.queue_addr;
        let backend_write = msg.backend_write;
        let traffic_signal = msg.traffic_signal;

        self.register(flow_id, queue_addr.clone(), backend_write, traffic_signal);
        queue_addr.do_send(BindScheduler {
            flow_id,
            scheduler: ctx.address(),
        });
        self.log_connection_count(flow_id, "registered to scheduler");

        Ok(())
    }
}

impl Handler<CanAcceptConnection> for Scheduler {
    type Result = bool;

    fn handle(&mut self, _msg: CanAcceptConnection, _ctx: &mut Self::Context) -> Self::Result {
        self.admission.can_accept_connection()
    }
}

// Handler for Unregister
impl Handler<Unregister> for Scheduler {
    type Result = ();

    fn handle(&mut self, msg: Unregister, ctx: &mut Self::Context) -> Self::Result {
        if self.unregister(msg.flow_id) {
            self.decrement_connection_count();
            self.log_connection_count(msg.flow_id, "unregistered from scheduler");
        } else {
            debug!(
                "flow{}: unregister requested but flow not found",
                msg.flow_id.0
            );
        }

        if self.should_stop() {
            ctx.stop();
        }
    }
}

impl Handler<FlowReady> for Scheduler {
    type Result = ();

    fn handle(&mut self, msg: FlowReady, _ctx: &mut Self::Context) -> Self::Result {
        // FlowReady notifications can race; mark_flow_ready handles the existence check
        // and ready_set.insert filters duplicates.
        self.mark_flow_ready(msg.flow_id);
    }
}

impl Scheduler {
    fn distribute_quantum(&mut self, _ctx: &mut <Self as Actor>::Context) {
        for (&flow_id, flow) in &self.flows {
            let quantum = flow.recommended_quantum(self.default_quantum);
            trace!("flow{}: assigned quantum {}", flow_id.0, quantum);
            flow.queue_addr().do_send(AddQuantum(quantum));
        }
    }

    fn round_robin_once(&mut self, ctx: &mut <Self as Actor>::Context) {
        let ready_count = self.ready_queue.len();
        for _ in 0..ready_count {
            let Some(flow) = self.ready_queue.pop_front() else {
                break;
            };

            let maybe_target = self.flow(flow).map(|entry| {
                (
                    entry.queue_addr(),
                    entry.backend_write(),
                    entry.traffic_signal(),
                )
            });

            if let Some((queue_addr, backend_write, traffic_signal)) = maybe_target {
                self.request_dequeue(flow, queue_addr, backend_write, traffic_signal, ctx);
            }
            self.ready_set.remove(&flow);
        }
    }

    fn request_dequeue(
        &self,
        flow: FlowId,
        queue_addr: Addr<QueueActor>,
        backend_write: Arc<Mutex<OwnedWriteHalf>>,
        traffic_signal: TunnelTrafficSignal,
        ctx: &mut <Self as Actor>::Context,
    ) {
        queue_addr
            .send(Dequeue {
                max_bytes: MAX_DEQUEUE_BYTES,
            })
            .into_actor(self)
            .map(move |res, act, ctx| {
                act.handle_dequeue_response(flow, backend_write, traffic_signal, res, ctx);
            })
            .spawn(ctx);
    }

    fn handle_dequeue_response(
        &mut self,
        flow: FlowId,
        backend_write: Arc<Mutex<OwnedWriteHalf>>,
        traffic_signal: TunnelTrafficSignal,
        res: Result<Option<DequeueResult>, MailboxError>,
        ctx: &mut <Self as Actor>::Context,
    ) {
        match res {
            Ok(Some(result)) => {
                self.handle_dequeue_success(flow, backend_write, traffic_signal, result)
            }
            Ok(None) => self.handle_empty_dequeue(flow),
            Err(e) => self.handle_dequeue_error(flow, e, ctx),
        }
    }

    fn handle_dequeue_success(
        &mut self,
        flow: FlowId,
        backend_write: Arc<Mutex<OwnedWriteHalf>>,
        traffic_signal: TunnelTrafficSignal,
        result: DequeueResult,
    ) {
        if self.flow(flow).is_none() {
            debug!(
                "flow{}: skipping dequeued backend write because flow is no longer registered",
                flow.0
            );
            return;
        }

        debug!(
            "flow{}: dequeue granted (remaining_queue={})",
            flow.0, result.remaining
        );

        let packet_len = result.packet.len();
        self.update_flow_stats(flow, packet_len);
        self.record_client_to_backend_bytes(packet_len);
        if result.ready_for_more {
            self.mark_flow_ready(flow);
        }
        traffic_signal.record();
        self.spawn_backend_write(flow, backend_write, result.packet, traffic_signal);
    }

    fn handle_empty_dequeue(&mut self, flow: FlowId) {
        self.remove_flow_from_ready(flow);
    }

    fn handle_dequeue_error(
        &mut self,
        flow: FlowId,
        error: MailboxError,
        ctx: &mut <Self as Actor>::Context,
    ) {
        warn!("flow{}: dequeue response error: {}", flow.0, error);
        self.remove_flow_from_ready(flow);
        ctx.address().do_send(Unregister { flow_id: flow });
    }

    fn update_flow_stats(&mut self, flow: FlowId, bytes: usize) {
        if let Some(entry) = self.flow_mut(flow) {
            entry.update_stats(bytes);
        }
    }

    fn record_client_to_backend_bytes(&mut self, bytes: usize) {
        self.total_client_to_backend_bytes += bytes as u64;
        self.ensure_global_start_time();
    }

    fn ensure_global_start_time(&mut self) {
        if self.global_start_time.is_none() {
            self.global_start_time = Some(Instant::now());
        }
    }

    fn spawn_backend_write(
        &self,
        flow: FlowId,
        backend_write: Arc<Mutex<OwnedWriteHalf>>,
        data: bytes::Bytes,
        traffic_signal: TunnelTrafficSignal,
    ) {
        actix::spawn(async move {
            let mut shutdown_rx = traffic_signal.subscribe_shutdown();

            if *shutdown_rx.borrow() {
                return;
            }

            let mut bw = tokio::select! {
                guard = backend_write.lock() => guard,
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        debug!(
                            "flow{}: backend write task stopping while waiting for write lock after idle timeout",
                            flow.0
                        );
                        return;
                    }
                    backend_write.lock().await
                }
            };

            if *shutdown_rx.borrow() {
                return;
            }

            tokio::select! {
                write_result = bw.write_all(&data) => {
                    if let Err(e) = write_result {
                        warn!("flow{}: backend write error: {}", flow.0, e);
                    } else {
                        traffic_signal.record();
                    }
                }
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        debug!(
                            "flow{}: backend write task stopping after idle timeout",
                            flow.0
                        );
                    }
                }
            }
        });
    }

    fn remove_flows(&mut self, ids: &[FlowId]) -> usize {
        if ids.is_empty() {
            return 0;
        }

        let mut removed_count = 0;
        let mut queue_needs_cleanup = false;

        for &id in ids {
            if let Some(entry) = self.flows.remove(&id) {
                debug!("flow{}: stopping queue on unregister", id.0);
                entry.queue_addr().do_send(StopNow);
                removed_count += 1;
                // Also clean up ready state
                if self.ready_set.remove(&id) {
                    queue_needs_cleanup = true;
                }
            }
        }

        if queue_needs_cleanup {
            self.ready_queue
                .retain(|flow_id| self.ready_set.contains(flow_id));
        }

        removed_count
    }

    fn log_stats(&self) {
        let flow_count = self.flows.len();
        let (tx_value, tx_unit) = format_bytes(self.total_client_to_backend_bytes);
        let (rx_value, rx_unit) = format_bytes(self.total_backend_to_client_bytes);
        let max_display = max_connections_display(self.max_connections());
        debug!(
            "⏱ scheduler: ticks={}, active connections={}/{}, total_tx={:.2} {}, total_rx={:.2} {}",
            self.total_ticks, flow_count, max_display, tx_value, tx_unit, rx_value, rx_unit
        );
        if let Some(global_start) = self.global_start_time {
            let elapsed = global_start.elapsed().as_secs_f64();
            if elapsed > 0.0 {
                let mut segments = Vec::new();
                if self.total_client_to_backend_bytes > 0
                    && let Some((avg_value, avg_unit)) =
                        format_rate(self.total_client_to_backend_bytes, elapsed)
                {
                    segments.push(format!("tx={:.2} {}", avg_value, avg_unit));
                }
                if self.total_backend_to_client_bytes > 0
                    && let Some((avg_value, avg_unit)) =
                        format_rate(self.total_backend_to_client_bytes, elapsed)
                {
                    segments.push(format!("rx={:.2} {}", avg_value, avg_unit));
                }

                if !segments.is_empty() {
                    debug!(
                        "   ⤷ avg throughput: {} over {:.2}s",
                        segments.join(", "),
                        elapsed
                    );
                }
            }
        }
    }
}

#[cfg(test)]
pub(super) struct InspectReply {
    pub connections: usize,
    pub ready_queue_len: usize,
    pub flow_ids: Vec<FlowId>,
    pub total_client_to_backend_bytes: u64,
    pub total_backend_to_client_bytes: u64,
    pub pending_connection_tasks: usize,
}

#[cfg(test)]
#[derive(Message)]
#[rtype(result = "InspectReply")]
pub(super) struct InspectState;

#[cfg(test)]
#[derive(Message)]
#[rtype(result = "()")]
pub(super) struct RecordUpstreamBytesTest {
    pub bytes: usize,
}

#[cfg(test)]
impl Handler<InspectState> for Scheduler {
    type Result = MessageResult<InspectState>;

    fn handle(&mut self, _msg: InspectState, _ctx: &mut Context<Self>) -> Self::Result {
        MessageResult(InspectReply {
            connections: self.current_connection_count(),
            ready_queue_len: self.ready_queue.len(),
            flow_ids: self.flows.keys().copied().collect(),
            total_client_to_backend_bytes: self.total_client_to_backend_bytes,
            total_backend_to_client_bytes: self.total_backend_to_client_bytes,
            pending_connection_tasks: self.admission.pending_connection_tasks(),
        })
    }
}

#[cfg(test)]
impl Handler<RecordUpstreamBytesTest> for Scheduler {
    type Result = ();

    fn handle(&mut self, msg: RecordUpstreamBytesTest, _ctx: &mut Context<Self>) -> Self::Result {
        self.record_client_to_backend_bytes(msg.bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::limits::max_connections_from_raw;
    use crate::test_utils::make_backend_write;
    use tokio::{
        io::AsyncReadExt,
        net::{TcpListener, TcpStream},
        time::{sleep, timeout},
    };

    fn test_traffic_signal() -> TunnelTrafficSignal {
        TunnelTrafficSignal::new()
    }

    async fn make_live_backend_write() -> (Arc<Mutex<OwnedWriteHalf>>, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let accept_handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            stream
        });

        let peer = TcpStream::connect(addr).await.unwrap();
        let server_stream = accept_handle.await.unwrap();
        let (_read_half, write_half) = server_stream.into_split();
        (Arc::new(Mutex::new(write_half)), peer)
    }

    #[actix_rt::test]
    async fn record_downstream_bytes_updates_totals() {
        let scheduler = Scheduler::new(1024, Duration::from_millis(10)).start();

        scheduler
            .send(RecordDownstreamBytes { bytes: 1024 })
            .await
            .unwrap();
        scheduler
            .send(RecordDownstreamBytes { bytes: 2048 })
            .await
            .unwrap();

        let reply = scheduler.send(super::InspectState).await.unwrap();
        assert_eq!(
            reply.total_backend_to_client_bytes, 3072,
            "downstream byte counter should accumulate all messages"
        );
        assert_eq!(
            reply.total_client_to_backend_bytes, 0,
            "upstream counter should remain untouched"
        );
    }

    #[actix_rt::test]
    async fn record_upstream_bytes_updates_totals() {
        let scheduler = Scheduler::new(1024, Duration::from_millis(10)).start();

        scheduler
            .send(super::RecordUpstreamBytesTest { bytes: 512 })
            .await
            .unwrap();
        scheduler
            .send(super::RecordUpstreamBytesTest { bytes: 256 })
            .await
            .unwrap();

        let reply = scheduler.send(super::InspectState).await.unwrap();
        assert_eq!(
            reply.total_client_to_backend_bytes, 768,
            "upstream byte counter should accumulate all messages"
        );
        assert_eq!(
            reply.total_backend_to_client_bytes, 0,
            "downstream counter should remain untouched"
        );
    }

    #[actix_rt::test]
    async fn upstream_dequeue_refreshes_tunnel_traffic_signal() {
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600)).start();

        let queue = QueueActor::new().start();
        let (backend_write, _backend_peer) = make_live_backend_write().await;
        let traffic_signal = TunnelTrafficSignal::new();
        let mut activity_rx = traffic_signal.subscribe_activity();
        scheduler
            .send(Register {
                flow_id: FlowId(11),
                queue_addr: queue.clone(),
                backend_write,
                traffic_signal,
            })
            .await
            .unwrap()
            .unwrap();

        queue
            .send(crate::actors::queue::Enqueue(bytes::Bytes::from_static(
                b"upstream progress",
            )))
            .await
            .unwrap()
            .unwrap();
        scheduler
            .send(FlowReady {
                flow_id: FlowId(11),
            })
            .await
            .unwrap();
        scheduler.send(QuantumTick).await.unwrap();

        timeout(Duration::from_secs(1), activity_rx.changed())
            .await
            .expect("upstream dequeue should refresh tunnel traffic before timeout")
            .expect("traffic signal sender should remain alive");
    }

    #[actix_rt::test]
    async fn backend_write_waiting_for_lock_stops_after_idle_shutdown() {
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600)).start();

        let queue = QueueActor::new().start();
        let (backend_write, mut backend_peer) = make_live_backend_write().await;
        let write_guard = backend_write.lock().await;
        let traffic_signal = TunnelTrafficSignal::new();
        scheduler
            .send(Register {
                flow_id: FlowId(12),
                queue_addr: queue.clone(),
                backend_write: backend_write.clone(),
                traffic_signal: traffic_signal.clone(),
            })
            .await
            .unwrap()
            .unwrap();

        queue
            .send(crate::actors::queue::Enqueue(bytes::Bytes::from_static(
                b"must not be written after shutdown",
            )))
            .await
            .unwrap()
            .unwrap();
        scheduler
            .send(FlowReady {
                flow_id: FlowId(12),
            })
            .await
            .unwrap();
        scheduler.send(QuantumTick).await.unwrap();

        traffic_signal.shutdown_for_test();
        sleep(Duration::from_millis(10)).await;
        drop(write_guard);

        let mut buf = [0u8; 64];
        let read_result = timeout(Duration::from_millis(50), backend_peer.read(&mut buf)).await;
        assert!(
            read_result.is_err(),
            "backend write task should exit while waiting for the write lock after idle shutdown"
        );
    }

    #[actix_rt::test]
    async fn stale_dequeue_response_after_unregister_does_not_write_to_backend() {
        let mut scheduler = Scheduler::new(1024, Duration::from_secs(3600));
        let queue = QueueActor::new().start();
        let (backend_write, mut backend_peer) = make_live_backend_write().await;
        let traffic_signal = TunnelTrafficSignal::new();
        let flow = FlowId(13);

        scheduler.register(flow, queue, backend_write.clone(), traffic_signal.clone());
        assert!(scheduler.unregister(flow));

        scheduler.handle_dequeue_success(
            flow,
            backend_write,
            traffic_signal,
            DequeueResult {
                packet: bytes::Bytes::from_static(b"stale dequeue response"),
                remaining: 0,
                ready_for_more: false,
            },
        );

        let mut buf = [0u8; 64];
        let read_result = timeout(Duration::from_millis(50), backend_peer.read(&mut buf)).await;
        match read_result {
            Err(_) | Ok(Ok(0)) => {}
            Ok(Ok(n)) => panic!(
                "stale dequeue responses after unregister must not write to the backend, read {n} bytes"
            ),
            Ok(Err(e)) => panic!("backend peer read failed unexpectedly: {e}"),
        }
    }

    #[actix_rt::test]
    async fn register_respects_max_connection_limit() {
        let scheduler = Scheduler::new(1024, Duration::from_millis(10))
            .with_max_connections(max_connections_from_raw(1))
            .start();

        let queue1 = QueueActor::new().start();
        let backend_write1 = make_backend_write().await;
        let result1 = scheduler
            .send(Register {
                flow_id: FlowId(1),
                queue_addr: queue1,
                backend_write: backend_write1,
                traffic_signal: test_traffic_signal(),
            })
            .await
            .unwrap();
        assert!(result1.is_ok(), "first registration should succeed");

        let queue2 = QueueActor::new().start();
        let backend_write2 = make_backend_write().await;
        let result2 = scheduler
            .send(Register {
                flow_id: FlowId(2),
                queue_addr: queue2,
                backend_write: backend_write2,
                traffic_signal: test_traffic_signal(),
            })
            .await
            .unwrap();
        assert!(
            matches!(result2, Err(RegisterError::MaxConnectionsReached { .. })),
            "second registration should be rejected when at limit"
        );
    }

    #[actix_rt::test]
    async fn register_allows_new_connection_after_unregister() {
        let scheduler = Scheduler::new(1024, Duration::from_millis(10))
            .with_max_connections(max_connections_from_raw(1))
            .start();

        let queue1 = QueueActor::new().start();
        let backend_write1 = make_backend_write().await;
        scheduler
            .send(Register {
                flow_id: FlowId(10),
                queue_addr: queue1,
                backend_write: backend_write1,
                traffic_signal: test_traffic_signal(),
            })
            .await
            .unwrap()
            .unwrap();

        scheduler
            .send(Unregister {
                flow_id: FlowId(10),
            })
            .await
            .unwrap();

        let queue2 = QueueActor::new().start();
        let backend_write2 = make_backend_write().await;
        let result = scheduler
            .send(Register {
                flow_id: FlowId(20),
                queue_addr: queue2,
                backend_write: backend_write2,
                traffic_signal: test_traffic_signal(),
            })
            .await
            .unwrap();
        assert!(
            result.is_ok(),
            "registration should succeed after a connection is unregistered"
        );
    }

    #[actix_rt::test]
    async fn can_accept_connection_returns_false_when_at_capacity() {
        let scheduler = Scheduler::new(1024, Duration::from_millis(10))
            .with_max_connections(max_connections_from_raw(1))
            .start();

        let queue = QueueActor::new().start();
        let backend_write = make_backend_write().await;
        scheduler
            .send(Register {
                flow_id: FlowId(1),
                queue_addr: queue,
                backend_write,
                traffic_signal: test_traffic_signal(),
            })
            .await
            .unwrap()
            .unwrap();

        let can_accept = scheduler.send(CanAcceptConnection).await.unwrap();
        assert!(!can_accept, "should refuse connections when at limit");
    }

    #[actix_rt::test]
    async fn can_accept_connection_returns_true_when_below_capacity() {
        let scheduler = Scheduler::new(1024, Duration::from_millis(10))
            .with_max_connections(max_connections_from_raw(2))
            .start();

        let queue = QueueActor::new().start();
        let backend_write = make_backend_write().await;
        scheduler
            .send(Register {
                flow_id: FlowId(1),
                queue_addr: queue,
                backend_write,
                traffic_signal: test_traffic_signal(),
            })
            .await
            .unwrap()
            .unwrap();

        let can_accept = scheduler.send(CanAcceptConnection).await.unwrap();
        assert!(can_accept, "should allow connection while under limit");
    }

    #[actix_rt::test]
    async fn can_accept_connection_returns_true_when_unlimited() {
        let scheduler = Scheduler::new(1024, Duration::from_millis(10))
            .with_max_connections(max_connections_from_raw(0))
            .start();

        let queue = QueueActor::new().start();
        let backend_write = make_backend_write().await;
        scheduler
            .send(Register {
                flow_id: FlowId(1),
                queue_addr: queue,
                backend_write,
                traffic_signal: test_traffic_signal(),
            })
            .await
            .unwrap()
            .unwrap();

        let can_accept = scheduler.send(CanAcceptConnection).await.unwrap();
        assert!(can_accept, "unlimited scheduler should always allow");
    }

    #[actix_rt::test]
    async fn try_start_connection_task_respects_active_and_pending_limit() {
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600))
            .with_max_connections(max_connections_from_raw(2))
            .start();

        scheduler
            .send(TryStartConnectionTask { flow_id: FlowId(1) })
            .await
            .unwrap()
            .unwrap();

        let queue = QueueActor::new().start();
        let backend_write = make_backend_write().await;
        scheduler
            .send(Register {
                flow_id: FlowId(42),
                queue_addr: queue,
                backend_write,
                traffic_signal: test_traffic_signal(),
            })
            .await
            .unwrap()
            .unwrap();

        let result = scheduler
            .send(TryStartConnectionTask { flow_id: FlowId(2) })
            .await
            .unwrap();
        assert!(
            matches!(result, Err(RegisterError::MaxConnectionsReached { .. })),
            "active + pending connections should consume the configured limit"
        );
    }

    #[actix_rt::test]
    async fn try_start_connection_task_allows_new_task_after_pending_finishes() {
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600))
            .with_max_connections(max_connections_from_raw(1))
            .start();

        scheduler
            .send(TryStartConnectionTask { flow_id: FlowId(1) })
            .await
            .unwrap()
            .unwrap();
        scheduler
            .send(ConnectionTaskFinished { flow_id: FlowId(1) })
            .await
            .unwrap();

        scheduler
            .send(TryStartConnectionTask { flow_id: FlowId(2) })
            .await
            .unwrap()
            .unwrap();
    }

    #[actix_rt::test]
    async fn try_start_connection_task_rejects_duplicate_flow_id() {
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600))
            .with_max_connections(max_connections_from_raw(2))
            .start();

        scheduler
            .send(TryStartConnectionTask { flow_id: FlowId(1) })
            .await
            .unwrap()
            .unwrap();

        let result = scheduler
            .send(TryStartConnectionTask { flow_id: FlowId(1) })
            .await
            .unwrap();
        assert!(
            matches!(
                result,
                Err(RegisterError::DuplicateConnectionTask { flow_id: FlowId(1) })
            ),
            "duplicate pending flow IDs should not share one reservation"
        );

        let reply = scheduler.send(super::InspectState).await.unwrap();
        assert_eq!(
            reply.pending_connection_tasks, 1,
            "duplicate reservations should not increment the pending counter"
        );
    }

    #[actix_rt::test]
    async fn register_consumes_pending_connection_task_reservation() {
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600))
            .with_max_connections(max_connections_from_raw(2))
            .start();

        scheduler
            .send(TryStartConnectionTask { flow_id: FlowId(1) })
            .await
            .unwrap()
            .unwrap();

        let queue = QueueActor::new().start();
        let backend_write = make_backend_write().await;
        scheduler
            .send(Register {
                flow_id: FlowId(1),
                queue_addr: queue,
                backend_write,
                traffic_signal: test_traffic_signal(),
            })
            .await
            .unwrap()
            .unwrap();

        let reply = scheduler.send(super::InspectState).await.unwrap();
        assert_eq!(reply.connections, 1, "registered flow should be active");
        assert_eq!(
            reply.pending_connection_tasks, 0,
            "registration should consume the pending reservation"
        );

        scheduler
            .send(TryStartConnectionTask { flow_id: FlowId(2) })
            .await
            .unwrap()
            .unwrap();
    }

    #[actix_rt::test]
    async fn unregister_updates_connection_count_and_ready_queue() {
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600))
            .with_max_connections(max_connections_from_raw(5))
            .start();

        for id in [FlowId(1), FlowId(2), FlowId(3)] {
            let queue = QueueActor::new().start();
            let backend_write = make_backend_write().await;
            scheduler
                .send(Register {
                    flow_id: id,
                    queue_addr: queue,
                    backend_write,
                    traffic_signal: test_traffic_signal(),
                })
                .await
                .unwrap()
                .unwrap();
        }

        scheduler.send(QuantumTick).await.unwrap();
        scheduler.send(QuantumTick).await.unwrap();

        scheduler
            .send(Unregister { flow_id: FlowId(2) })
            .await
            .unwrap();

        let reply = scheduler.send(super::InspectState).await.unwrap();

        assert_eq!(reply.connections, 2, "connection count should decrement");
        assert_eq!(
            reply.ready_queue_len, 0,
            "ready queue should be empty without pending notifications"
        );
        let mut flow_ids = reply.flow_ids;
        flow_ids.sort();
        assert_eq!(
            flow_ids,
            vec![FlowId(1), FlowId(3)],
            "flow2 should be removed"
        );
    }

    #[actix_rt::test]
    async fn unregister_stops_queue_actor() {
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600)).start();

        let queue = QueueActor::new().start();
        let backend_write = make_backend_write().await;
        scheduler
            .send(Register {
                flow_id: FlowId(99),
                queue_addr: queue.clone(),
                backend_write,
                traffic_signal: test_traffic_signal(),
            })
            .await
            .unwrap()
            .unwrap();

        scheduler
            .send(Unregister {
                flow_id: FlowId(99),
            })
            .await
            .unwrap();

        let mut stopped = false;
        for _ in 0..20 {
            if queue
                .send(Dequeue {
                    max_bytes: usize::MAX,
                })
                .await
                .is_err()
            {
                stopped = true;
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }

        assert!(stopped, "queue actor should stop after unregister");
    }

    #[actix_rt::test]
    async fn shutdown_waits_for_pending_connection_tasks() {
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600)).start();

        scheduler
            .send(TryStartConnectionTask { flow_id: FlowId(1) })
            .await
            .unwrap()
            .unwrap();
        scheduler.send(Shutdown).await.unwrap();

        let reply = scheduler.send(super::InspectState).await.unwrap();
        assert_eq!(
            reply.pending_connection_tasks, 1,
            "pending tasks should remain tracked during shutdown"
        );

        scheduler
            .send(ConnectionTaskFinished { flow_id: FlowId(1) })
            .await
            .unwrap();

        let mut stopped = false;
        for _ in 0..20 {
            if scheduler.send(CanAcceptConnection).await.is_err() {
                stopped = true;
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }

        assert!(
            stopped,
            "scheduler should stop once shutdown is requested and pending tasks drain"
        );
    }

    #[actix_rt::test]
    async fn dequeue_error_triggers_unregister() {
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600)).start();

        let queue = QueueActor::new().start();
        let backend_write = make_backend_write().await;
        scheduler
            .send(Register {
                flow_id: FlowId(7),
                queue_addr: queue.clone(),
                backend_write,
                traffic_signal: test_traffic_signal(),
            })
            .await
            .unwrap()
            .unwrap();

        queue.do_send(StopNow);
        scheduler
            .send(FlowReady { flow_id: FlowId(7) })
            .await
            .unwrap();
        scheduler.send(QuantumTick).await.unwrap();

        let mut unregistered = false;
        for _ in 0..20 {
            let reply = scheduler.send(super::InspectState).await.unwrap();
            if reply.connections == 0 && reply.flow_ids.is_empty() {
                unregistered = true;
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }

        assert!(
            unregistered,
            "flow should be unregistered after dequeue error"
        );
    }

    #[actix_rt::test]
    async fn test_recommended_quantum_selection() {
        let queue = QueueActor::new().start();
        let backend_write = make_backend_write().await;
        let default_quantum = 8192;

        // Case 1: No stats -> default_quantum
        let flow = FlowEntry::new(queue.clone(), backend_write.clone(), test_traffic_signal());
        assert_eq!(
            flow.recommended_quantum(default_quantum),
            default_quantum,
            "Should return default quantum when no stats available"
        );

        // Case 2: Small packets (< 200 bytes) -> MIN_QUANTUM (1500)
        let mut flow = FlowEntry::new(queue.clone(), backend_write.clone(), test_traffic_signal());
        flow.update_stats(100); // Set avg to 100
        assert_eq!(
            flow.recommended_quantum(default_quantum),
            1500,
            "Should return MIN_QUANTUM for small packets"
        );

        // Case 3: Normal packets -> Scaled (avg * 10)
        let mut flow = FlowEntry::new(queue.clone(), backend_write.clone(), test_traffic_signal());
        flow.update_stats(500); // Set avg to 500
        // Target = 500 * 10 = 5000
        assert_eq!(
            flow.recommended_quantum(default_quantum),
            5000,
            "Should scale quantum for normal packets"
        );

        // Case 4: Large packets -> MAX_QUANTUM (16384)
        let mut flow = FlowEntry::new(queue.clone(), backend_write.clone(), test_traffic_signal());
        flow.update_stats(2000); // Set avg to 2000
        // Target = 2000 * 10 = 20000 -> Clamped to 16384
        assert_eq!(
            flow.recommended_quantum(default_quantum),
            16384,
            "Should clamp to MAX_QUANTUM for large packets"
        );
    }

    #[actix_rt::test]
    async fn test_quantum_adapts_with_filtered_average() {
        let queue = QueueActor::new().start();
        let backend_write = make_backend_write().await;
        let default_quantum = 4096;

        let mut flow = FlowEntry::new(queue, backend_write, test_traffic_signal());

        // Initial tiny packets force the minimum quantum.
        flow.update_stats(100);
        assert_eq!(
            flow.recommended_quantum(default_quantum),
            1500,
            "Small packets should produce MIN_QUANTUM"
        );

        // Sustained large packets should cause the EMA to rise, eventually yielding
        // the maximum quantum due to clamping.
        for _ in 0..25 {
            flow.update_stats(2000);
        }

        assert_eq!(
            flow.recommended_quantum(default_quantum),
            16384,
            "EMA should adapt and drive the quantum to MAX_QUANTUM after sustained large packets"
        );
    }
}
