use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use actix::prelude::*;
use log::{debug, info, trace, warn};
use tokio::{io::AsyncWriteExt, net::tcp::OwnedWriteHalf, sync::Mutex, time::Instant};

use crate::{
    actors::{
        connection_limit::ConnectionLimiter,
        queue::{AddQuantum, BindScheduler, Dequeue, DequeueResult, QueueActor, StopNow},
    },
    flow::FlowId,
    util::{format_bytes, format_rate},
};

pub(crate) use crate::actors::connection_limit::RegisterError;

#[derive(Message)]
#[rtype(result = "Result<(), RegisterError>")]
pub(crate) struct Register {
    pub flow_id: FlowId,
    pub queue_addr: Addr<QueueActor>,
    pub backend_write: Arc<Mutex<OwnedWriteHalf>>,
}

/// Returns whether the scheduler can admit another connection right now.
#[derive(Message)]
#[rtype(result = "bool")]
pub(crate) struct CanAcceptConnection;

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

/// Tracks statistics for a flow, including bytes sent, packets sent, and average packet size using a low-pass filter.
///
/// The average packet size is maintained using an exponential moving average (EMA) with a smoothing factor.
/// This helps smooth out short-term fluctuations while responding to longer-term trends in packet sizes.
#[derive(Debug)]
struct FlowStats {
    bytes_sent: u64,
    packets_sent: u64,
    start_time: Option<Instant>,
    avg_packet_size_lpf: Option<f64>,
}

impl FlowStats {
    fn new() -> Self {
        Self {
            bytes_sent: 0,
            packets_sent: 0,
            start_time: None,
            avg_packet_size_lpf: None,
        }
    }

    /// Updates the flow statistics with a new packet size sample.
    ///
    /// Uses an exponential moving average to compute the average packet size, reducing noise from individual packet variations.
    /// The smoothing factor ALPHA = 0.2 provides a balance: 20% weight to the current sample and 80% to the previous average,
    /// allowing the average to adapt to changes while filtering out transient spikes.
    fn update(&mut self, bytes: usize) {
        const ALPHA: f64 = 0.2;

        self.bytes_sent += bytes as u64;
        self.packets_sent += 1;
        if self.start_time.is_none() {
            self.start_time = Some(Instant::now());
        }

        let sample = bytes as f64;
        self.avg_packet_size_lpf = Some(match self.avg_packet_size_lpf {
            Some(prev) => prev + ALPHA * (sample - prev),
            None => sample,
        });
    }

    fn avg_packet_size(&self) -> Option<usize> {
        self.avg_packet_size_lpf.map(|avg| avg.round() as usize)
    }
}

struct FlowEntry {
    queue_addr: Addr<QueueActor>,
    backend_write: Arc<Mutex<OwnedWriteHalf>>,
    stats: FlowStats,
}

impl FlowEntry {
    fn new(queue_addr: Addr<QueueActor>, backend_write: Arc<Mutex<OwnedWriteHalf>>) -> Self {
        Self {
            queue_addr,
            backend_write,
            stats: FlowStats::new(),
        }
    }

    fn queue_addr(&self) -> Addr<QueueActor> {
        self.queue_addr.clone()
    }

    fn backend_write(&self) -> Arc<Mutex<OwnedWriteHalf>> {
        self.backend_write.clone()
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
        DEFAULT_DRR_QUANTUM_STRATEGY
            .recommended_quantum(self.stats.avg_packet_size(), default_quantum)
    }
}

#[derive(Clone, Copy, Debug)]
struct QuantumConfig {
    min_quantum: usize,
    max_quantum: usize,
    target_burst_packets: usize,
    small_packet_threshold: usize,
}

/// Tuning defaults chosen to balance small-packet latency against large-packet throughput.
///
/// * `min_quantum (1500 bytes)`: Standard Ethernet MTU, ensuring even latency-sensitive flows get
///   at least one full-sized packet per scheduling turn.
/// * `max_quantum (16 KiB)`: Prevents any single flow from monopolizing airtime while still
///   amortizing scheduler overhead for bulk traffic.
/// * `target_burst_packets (10)`: Empirically smooths throughput without making interactive
///   traffic wait excessively between turns.
/// * `small_packet_threshold (200 bytes)`: Heuristic cutoff for “chatty” or control-plane flows
///   (e.g., TCP ACKs, SSH, VoIP) that benefit from shorter quanta to reduce jitter.
const DEFAULT_QUANTUM_CONFIG: QuantumConfig = QuantumConfig {
    min_quantum: 1_500,
    max_quantum: 16 * 1024,
    target_burst_packets: 10,
    small_packet_threshold: 200,
};

#[derive(Clone, Copy, Debug)]
struct DrrQuantumStrategy {
    config: QuantumConfig,
}

/// Immutable default strategy reused by every flow to avoid repeatedly constructing the same tuning profile.
const DEFAULT_DRR_QUANTUM_STRATEGY: DrrQuantumStrategy = DrrQuantumStrategy {
    config: DEFAULT_QUANTUM_CONFIG,
};

impl DrrQuantumStrategy {
    /// Calculates the optimal Deficit Round Robin (DRR) quantum based on historical packet
    /// statistics.
    ///
    /// Strategy overview:
    /// - If we have no packet history, fall back to the caller-provided default to avoid guessing.
    /// - Small packets (below `small_packet_threshold`) get the minimum quantum to keep the
    ///   scheduler cycling quickly and minimize jitter for interactive traffic.
    /// - Otherwise we scale linearly to target `target_burst_packets` per turn and clamp the result
    ///   between `min_quantum` and `max_quantum` so bulk flows gain efficiency without starving
    ///   others.
    fn recommended_quantum(&self, avg_packet_size: Option<usize>, default_quantum: usize) -> usize {
        match avg_packet_size {
            Some(avg) if avg < self.config.small_packet_threshold => self.config.min_quantum,
            Some(avg) => {
                let target = avg.saturating_mul(self.config.target_burst_packets);
                target.clamp(self.config.min_quantum, self.config.max_quantum)
            }
            None => default_quantum,
        }
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
    connection_limiter: ConnectionLimiter,
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
            connection_limiter: ConnectionLimiter::new(),
        }
    }

    /// Configure the scheduler with a maximum concurrent connection limit.
    ///
    /// A value of `0` disables the limit (treated as unlimited).
    pub(crate) fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.connection_limiter.set_max_connections(max_connections);
        self
    }

    fn try_increment_connection_count(&self) -> Result<(), RegisterError> {
        self.connection_limiter.try_acquire()
    }

    fn decrement_connection_count(&self) {
        self.connection_limiter.release();
    }

    fn current_connection_count(&self) -> usize {
        self.connection_limiter.current()
    }

    fn max_connections(&self) -> Option<usize> {
        self.connection_limiter.max_connections()
    }

    fn log_connection_count(&self, flow_id: FlowId, action: &str) {
        match self.max_connections() {
            Some(limit) => info!(
                "flow{}: {} (connections: {}/{})",
                flow_id.0,
                action,
                self.current_connection_count(),
                limit
            ),
            None => info!(
                "flow{}: {} (connections: {})",
                flow_id.0,
                action,
                self.current_connection_count()
            ),
        };
    }

    fn register(
        &mut self,
        id: FlowId,
        queue_addr: Addr<QueueActor>,
        backend_write: Arc<Mutex<OwnedWriteHalf>>,
    ) {
        self.flows
            .insert(id, FlowEntry::new(queue_addr, backend_write));
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

        self.register(flow_id, queue_addr.clone(), backend_write);
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
        match self.max_connections() {
            Some(limit) => self.current_connection_count() < limit,
            None => true,
        }
    }
}

// Handler for Unregister
impl Handler<Unregister> for Scheduler {
    type Result = ();

    fn handle(&mut self, msg: Unregister, _ctx: &mut Self::Context) -> Self::Result {
        if self.unregister(msg.flow_id) {
            self.decrement_connection_count();
            self.log_connection_count(msg.flow_id, "unregistered from scheduler");
        } else {
            debug!(
                "flow{}: unregister requested but flow not found",
                msg.flow_id.0
            );
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

            let maybe_target = self
                .flow(flow)
                .map(|entry| (entry.queue_addr(), entry.backend_write()));

            if let Some((queue_addr, backend_write)) = maybe_target {
                self.request_dequeue(flow, queue_addr, backend_write, ctx);
            }
            self.ready_set.remove(&flow);
        }
    }

    fn request_dequeue(
        &self,
        flow: FlowId,
        queue_addr: Addr<QueueActor>,
        backend_write: Arc<Mutex<OwnedWriteHalf>>,
        ctx: &mut <Self as Actor>::Context,
    ) {
        queue_addr
            .send(Dequeue {
                // scheduler always requests full packets today, but we keep the
                // API field so a future byte cap can hook in without churn.
                max_bytes: usize::MAX,
            })
            .into_actor(self)
            .map(move |res, act, ctx| {
                act.handle_dequeue_response(flow, backend_write, res, ctx);
            })
            .spawn(ctx);
    }

    fn handle_dequeue_response(
        &mut self,
        flow: FlowId,
        backend_write: Arc<Mutex<OwnedWriteHalf>>,
        res: Result<Option<DequeueResult>, MailboxError>,
        ctx: &mut <Self as Actor>::Context,
    ) {
        match res {
            Ok(Some(result)) => self.handle_dequeue_success(flow, backend_write, result),
            Ok(None) => self.handle_empty_dequeue(flow),
            Err(e) => self.handle_dequeue_error(flow, e, ctx),
        }
    }

    fn handle_dequeue_success(
        &mut self,
        flow: FlowId,
        backend_write: Arc<Mutex<OwnedWriteHalf>>,
        result: DequeueResult,
    ) {
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
        self.spawn_backend_write(flow, backend_write, result.packet);
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
    ) {
        actix::spawn(async move {
            let mut bw = backend_write.lock().await;
            if let Err(e) = bw.write_all(&data).await {
                warn!("flow{}: backend write error: {}", flow.0, e);
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
        let max_display = self.max_connections().map(|limit| limit.to_string());
        debug!(
            "⏱ scheduler: ticks={}, active connections={}/{}, total_tx={:.2} {}, total_rx={:.2} {}",
            self.total_ticks,
            flow_count,
            max_display.as_deref().unwrap_or("∞"),
            tx_value,
            tx_unit,
            rx_value,
            rx_unit
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
    use crate::test_utils::make_backend_write;
    use tokio::time::sleep;

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
    async fn register_respects_max_connection_limit() {
        let scheduler = Scheduler::new(1024, Duration::from_millis(10))
            .with_max_connections(1)
            .start();

        let queue1 = QueueActor::new().start();
        let backend_write1 = make_backend_write().await;
        let result1 = scheduler
            .send(Register {
                flow_id: FlowId(1),
                queue_addr: queue1,
                backend_write: backend_write1,
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
            .with_max_connections(1)
            .start();

        let queue1 = QueueActor::new().start();
        let backend_write1 = make_backend_write().await;
        scheduler
            .send(Register {
                flow_id: FlowId(10),
                queue_addr: queue1,
                backend_write: backend_write1,
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
            .with_max_connections(1)
            .start();

        let queue = QueueActor::new().start();
        let backend_write = make_backend_write().await;
        scheduler
            .send(Register {
                flow_id: FlowId(1),
                queue_addr: queue,
                backend_write,
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
            .with_max_connections(2)
            .start();

        let queue = QueueActor::new().start();
        let backend_write = make_backend_write().await;
        scheduler
            .send(Register {
                flow_id: FlowId(1),
                queue_addr: queue,
                backend_write,
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
            .with_max_connections(0)
            .start();

        let queue = QueueActor::new().start();
        let backend_write = make_backend_write().await;
        scheduler
            .send(Register {
                flow_id: FlowId(1),
                queue_addr: queue,
                backend_write,
            })
            .await
            .unwrap()
            .unwrap();

        let can_accept = scheduler.send(CanAcceptConnection).await.unwrap();
        assert!(can_accept, "unlimited scheduler should always allow");
    }

    #[actix_rt::test]
    async fn unregister_updates_connection_count_and_ready_queue() {
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600))
            .with_max_connections(5)
            .start();

        for id in [FlowId(1), FlowId(2), FlowId(3)] {
            let queue = QueueActor::new().start();
            let backend_write = make_backend_write().await;
            scheduler
                .send(Register {
                    flow_id: id,
                    queue_addr: queue,
                    backend_write,
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
    async fn dequeue_error_triggers_unregister() {
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600)).start();

        let queue = QueueActor::new().start();
        let backend_write = make_backend_write().await;
        scheduler
            .send(Register {
                flow_id: FlowId(7),
                queue_addr: queue.clone(),
                backend_write,
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
        let flow = FlowEntry::new(queue.clone(), backend_write.clone());
        assert_eq!(
            flow.recommended_quantum(default_quantum),
            default_quantum,
            "Should return default quantum when no stats available"
        );

        // Case 2: Small packets (< 200 bytes) -> MIN_QUANTUM (1500)
        let mut flow = FlowEntry::new(queue.clone(), backend_write.clone());
        flow.update_stats(100); // Set avg to 100
        assert_eq!(
            flow.recommended_quantum(default_quantum),
            1500,
            "Should return MIN_QUANTUM for small packets"
        );

        // Case 3: Normal packets -> Scaled (avg * 10)
        let mut flow = FlowEntry::new(queue.clone(), backend_write.clone());
        flow.update_stats(500); // Set avg to 500
        // Target = 500 * 10 = 5000
        assert_eq!(
            flow.recommended_quantum(default_quantum),
            5000,
            "Should scale quantum for normal packets"
        );

        // Case 4: Large packets -> MAX_QUANTUM (16384)
        let mut flow = FlowEntry::new(queue.clone(), backend_write.clone());
        flow.update_stats(2000); // Set avg to 2000
        // Target = 2000 * 10 = 20000 -> Clamped to 16384
        assert_eq!(
            flow.recommended_quantum(default_quantum),
            16384,
            "Should clamp to MAX_QUANTUM for large packets"
        );
    }

    #[test]
    fn test_drr_quantum_strategy_cases() {
        let strategy = DEFAULT_DRR_QUANTUM_STRATEGY;
        let config = strategy.config;
        let default_quantum = 4096;

        // Validate DRR quantum selection across key regimes: no stats, small packets,
        // burst-scaled packets, and clamped large packets.
        let cases = vec![
            (None, default_quantum, "no stats uses default"),
            (
                Some(config.small_packet_threshold - 1),
                config.min_quantum,
                "small packets get minimum quantum",
            ),
            (
                Some(config.small_packet_threshold + 50),
                (config.small_packet_threshold + 50)
                    .saturating_mul(config.target_burst_packets)
                    .clamp(config.min_quantum, config.max_quantum),
                "moderate packets scale by burst target",
            ),
            (
                Some(config.max_quantum + 1),
                config.max_quantum,
                "large packets clamp at maximum",
            ),
        ];

        for (avg, expected, label) in cases {
            assert_eq!(
                strategy.recommended_quantum(avg, default_quantum),
                expected,
                "{}",
                label
            );
        }
    }
    #[actix_rt::test]
    async fn test_quantum_adapts_with_filtered_average() {
        let queue = QueueActor::new().start();
        let backend_write = make_backend_write().await;
        let default_quantum = 4096;

        let mut flow = FlowEntry::new(queue, backend_write);

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

    #[test]
    fn test_flow_stats_ema() {
        let mut stats = FlowStats::new();

        // Initial state
        assert!(stats.avg_packet_size().is_none());

        // First packet: initializes average
        stats.update(1000);
        assert_eq!(stats.avg_packet_size(), Some(1000));

        // Second packet: 2000 bytes
        // ALPHA = 0.2
        // avg = 1000 + 0.2 * (2000 - 1000) = 1000 + 200 = 1200
        stats.update(2000);
        assert_eq!(stats.avg_packet_size(), Some(1200));

        // Third packet: 500 bytes
        // avg = 1200 + 0.2 * (500 - 1200) = 1200 + 0.2 * (-700) = 1200 - 140 = 1060
        stats.update(500);
        assert_eq!(stats.avg_packet_size(), Some(1060));

        // Convergence test: constant stream of 2000 bytes
        // It should approach 2000.
        for _ in 0..50 {
            stats.update(2000);
        }
        // After many updates, it should be very close to 2000.
        let avg = stats.avg_packet_size().unwrap();
        assert!(
            (1900..=2000).contains(&avg),
            "Average should converge to 2000, got {}",
            avg
        );
    }
}
