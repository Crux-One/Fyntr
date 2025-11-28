use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use actix::prelude::*;
use log::{debug, info, warn};
use tokio::{io::AsyncWriteExt, net::tcp::OwnedWriteHalf, sync::Mutex, time::Instant};

use crate::{
    actors::queue::{AddQuantum, BindScheduler, Dequeue, DequeueResult, QueueActor},
    flow::FlowId,
    util::{format_bytes, format_rate},
};

#[derive(Message)]
#[rtype(result = "Result<(), RegisterError>")]
pub(crate) struct Register {
    pub flow_id: FlowId,
    pub queue_addr: Addr<QueueActor>,
    pub backend_write: Arc<Mutex<OwnedWriteHalf>>,
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

#[derive(Debug)]
pub(crate) enum RegisterError {
    MaxConnectionsReached { max: usize },
}

impl fmt::Display for RegisterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MaxConnectionsReached { max } => {
                write!(f, "maximum connection limit ({}) reached", max)
            }
        }
    }
}

impl std::error::Error for RegisterError {}

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

    fn update(&mut self, bytes: usize) {
        self.bytes_sent += bytes as u64;
        self.packets_sent += 1;
        if self.start_time.is_none() {
            self.start_time = Some(Instant::now());
        }

        const ALPHA: f64 = 0.2;
        let sample = bytes as f64;
        self.avg_packet_size_lpf = Some(match self.avg_packet_size_lpf {
            Some(prev) => prev + ALPHA * (sample - prev),
            None => sample,
        });
    }

    fn avg_packet_size(&self) -> Option<usize> {
        self.avg_packet_size_lpf.map(|avg| avg as usize)
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

    fn recommended_quantum(&self, default_quantum: usize) -> usize {
        const SMALL_PACKET_THRESHOLD: usize = 200;
        const LATENCY_OPTIMIZED_QUANTUM: usize = 1500;
        const THROUGHPUT_OPTIMIZED_QUANTUM: usize = 16 * 1024;

        match self.stats.avg_packet_size() {
            Some(avg) if avg < SMALL_PACKET_THRESHOLD => LATENCY_OPTIMIZED_QUANTUM,
            Some(_) => THROUGHPUT_OPTIMIZED_QUANTUM,
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
    total_bytes_sent: u64,
    global_start_time: Option<Instant>,
    total_ticks: u64,
    max_connections: Option<usize>,
    current_connections: AtomicUsize,
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

impl Scheduler {
    pub(crate) fn new(quantum: usize, tick: Duration) -> Self {
        Self {
            flows: HashMap::new(),
            ready_queue: VecDeque::new(),
            ready_set: HashSet::new(),
            default_quantum: quantum,
            tick,
            total_bytes_sent: 0,
            global_start_time: None,
            total_ticks: 0,
            max_connections: None,
            current_connections: AtomicUsize::new(0),
        }
    }

    /// Configure the scheduler with a maximum concurrent connection limit.
    ///
    /// A value of `0` disables the limit (treated as unlimited).
    pub(crate) fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = if max_connections == 0 {
            None
        } else {
            Some(max_connections)
        };
        self
    }

    /// Atomically attempts to reserve a connection slot.
    /// Returns true if a slot was reserved, false if the limit was reached.
    fn try_increment_connection_count(&self) -> Result<(), RegisterError> {
        match self.max_connections {
            Some(limit) => {
                let mut current = self.current_connections.load(Ordering::Acquire);
                loop {
                    if current >= limit {
                        warn!(
                            "scheduler: connection rejected - max connections ({}) reached",
                            limit
                        );
                        return Err(RegisterError::MaxConnectionsReached { max: limit });
                    }

                    match self.current_connections.compare_exchange(
                        current,
                        current + 1,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => return Ok(()),
                        Err(observed) => current = observed,
                    }
                }
            }
            None => {
                self.current_connections.fetch_add(1, Ordering::AcqRel);
                Ok(())
            }
        }
    }

    fn decrement_connection_count(&self) {
        let result =
            self.current_connections
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    (current > 0).then_some(current - 1)
                });
        if result.is_err() {
            warn!("scheduler: attempted to decrement connection count below zero");
        }
    }

    fn current_connection_count(&self) -> usize {
        self.current_connections.load(Ordering::Acquire)
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
        match self.max_connections {
            Some(limit) => info!(
                "flow{}: registered to scheduler (connections: {}/{})",
                flow_id.0,
                self.current_connection_count(),
                limit
            ),
            None => info!(
                "flow{}: registered to scheduler (connections: {})",
                flow_id.0,
                self.current_connection_count()
            ),
        };

        Ok(())
    }
}

// Handler for Unregister
impl Handler<Unregister> for Scheduler {
    type Result = ();

    fn handle(&mut self, msg: Unregister, _ctx: &mut Self::Context) -> Self::Result {
        if self.unregister(msg.flow_id) {
            self.decrement_connection_count();
            match self.max_connections {
                Some(limit) => info!(
                    "flow{}: unregistered from scheduler (connections: {}/{})",
                    msg.flow_id.0,
                    self.current_connection_count(),
                    limit
                ),
                None => info!(
                    "flow{}: unregistered from scheduler (connections: {})",
                    msg.flow_id.0,
                    self.current_connection_count()
                ),
            };
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
        for flow in self.flows.values() {
            let quantum = flow.recommended_quantum(self.default_quantum);
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
        self.update_global_stats(packet_len);
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

    fn update_global_stats(&mut self, bytes: usize) {
        self.total_bytes_sent += bytes as u64;
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
            if self.flows.remove(&id).is_some() {
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
        let (total_value, total_unit) = format_bytes(self.total_bytes_sent);
        let max_display = self.max_connections.map(|limit| limit.to_string());
        debug!(
            "⏱ scheduler: ticks={}, active connections={}/{}, total={:.2} {}",
            self.total_ticks,
            flow_count,
            max_display.as_deref().unwrap_or("∞"),
            total_value,
            total_unit
        );
        if let Some(global_start) = self.global_start_time {
            let elapsed = global_start.elapsed().as_secs_f64();
            if elapsed > 0.0
                && let Some((avg_value, avg_unit)) = format_rate(self.total_bytes_sent, elapsed)
            {
                debug!(
                    "   ⤷ avg throughput: {:.2} {} over {:.2}s",
                    avg_value, avg_unit, elapsed
                );
            }
        }
    }
}

#[cfg(test)]
pub(super) struct InspectReply {
    pub connections: usize,
    pub ready_queue_len: usize,
    pub flow_ids: Vec<FlowId>,
}

#[cfg(test)]
#[derive(Message)]
#[rtype(result = "InspectReply")]
pub(super) struct InspectState;

#[cfg(test)]
impl Handler<InspectState> for Scheduler {
    type Result = MessageResult<InspectState>;

    fn handle(&mut self, _msg: InspectState, _ctx: &mut Context<Self>) -> Self::Result {
        MessageResult(InspectReply {
            connections: self.current_connection_count(),
            ready_queue_len: self.ready_queue.len(),
            flow_ids: self.flows.keys().copied().collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::make_backend_write;

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
}
