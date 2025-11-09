use std::{
    collections::HashSet,
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
    actors::queue::{AddQuantum, Dequeue, DequeueResult, QueueActor},
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
    start_time: Option<Instant>,
}

impl FlowStats {
    fn new() -> Self {
        Self {
            bytes_sent: 0,
            start_time: None,
        }
    }

    fn update(&mut self, bytes: usize) {
        self.bytes_sent += bytes as u64;
        if self.start_time.is_none() {
            self.start_time = Some(Instant::now());
        }
    }
}

struct FlowEntry {
    id: FlowId,
    queue_addr: Addr<QueueActor>,
    backend_write: Arc<Mutex<OwnedWriteHalf>>,
    stats: FlowStats,
}

impl FlowEntry {
    fn new(
        id: FlowId,
        queue_addr: Addr<QueueActor>,
        backend_write: Arc<Mutex<OwnedWriteHalf>>,
    ) -> Self {
        Self {
            id,
            queue_addr,
            backend_write,
            stats: FlowStats::new(),
        }
    }

    fn id(&self) -> FlowId {
        self.id
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
}

pub(crate) struct Scheduler {
    flows: Vec<FlowEntry>,
    quantum: usize,
    tick: Duration,
    rr_index: usize,
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
            flows: vec![],
            quantum,
            tick,
            rr_index: 0,
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
            .push(FlowEntry::new(id, queue_addr, backend_write));
    }

    fn unregister(&mut self, id: FlowId) -> bool {
        self.remove_flows(&[id]) > 0
    }

    fn flow_mut(&mut self, id: FlowId) -> Option<&mut FlowEntry> {
        self.flows.iter_mut().find(|flow| flow.id() == id)
    }
}

// Handler for Register
impl Handler<Register> for Scheduler {
    type Result = Result<(), RegisterError>;

    fn handle(&mut self, msg: Register, _ctx: &mut Self::Context) -> Self::Result {
        self.try_increment_connection_count()?;
        self.register(msg.flow_id, msg.queue_addr, msg.backend_write);
        match self.max_connections {
            Some(limit) => info!(
                "flow{}: registered to scheduler (connections: {}/{})",
                msg.flow_id.0,
                self.current_connection_count(),
                limit
            ),
            None => info!(
                "flow{}: registered to scheduler (connections: {})",
                msg.flow_id.0,
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

impl Scheduler {
    fn distribute_quantum(&mut self, _ctx: &mut <Self as Actor>::Context) {
        for flow in &self.flows {
            flow.queue_addr().do_send(AddQuantum(self.quantum));
        }
    }

    fn round_robin_once(&mut self, ctx: &mut <Self as Actor>::Context) {
        if self.flows.is_empty() {
            return;
        }

        let n = self.flows.len();
        let start = self.rr_index;

        for step in 0..n {
            let idx = (start + step) % n;
            let flow = self.flows[idx].id();
            let queue_addr = self.flows[idx].queue_addr();
            let backend_write = self.flows[idx].backend_write();

            self.request_dequeue(flow, queue_addr, backend_write, ctx);
        }

        if !self.flows.is_empty() {
            self.rr_index = (start + 1) % self.flows.len();
        } else {
            self.rr_index = 0;
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
        self.spawn_backend_write(flow, backend_write, result.packet);
    }

    fn handle_empty_dequeue(&self, _flow: FlowId) {
        // The queue is empty; no data to dequeue for this flow at this tick.
    }

    fn handle_dequeue_error(
        &mut self,
        flow: FlowId,
        error: MailboxError,
        ctx: &mut <Self as Actor>::Context,
    ) {
        warn!("flow{}: dequeue response error: {}", flow.0, error);
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
        let to_remove: HashSet<FlowId> = ids.iter().copied().collect();
        let before = self.flows.len();

        self.flows.retain(|flow| !to_remove.contains(&flow.id()));

        if self.flows.is_empty() {
            self.rr_index = 0;
        } else {
            self.rr_index %= self.flows.len();
        }

        before.saturating_sub(self.flows.len())
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
mod tests {
    use super::*;
    use tokio::net::{TcpListener, TcpStream};

    async fn make_backend_write() -> Arc<Mutex<OwnedWriteHalf>> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let accept_handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            stream
        });

        let client_stream = TcpStream::connect(addr).await.unwrap();
        let server_stream = accept_handle.await.unwrap();
        drop(client_stream);

        let (_read_half, write_half) = server_stream.into_split();
        Arc::new(Mutex::new(write_half))
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
}
