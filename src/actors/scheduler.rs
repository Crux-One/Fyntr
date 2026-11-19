use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::Duration,
};

mod connection_admission;
mod flow_stats;
mod quantum_strategy;

use actix::prelude::*;
use log::{debug, info, warn};
use tokio::{net::tcp::OwnedWriteHalf, sync::watch, time::Instant};

#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use tokio::sync::Semaphore;

use crate::{
    actors::{
        flow_writer::{FlowWriter, StopWriter, WritePacket},
        queue::{
            BindScheduler, QueueActor, ReleasePendingBytes, ServiceTurn, StopNow, TurnResult,
            TurnStatus,
        },
    },
    flow::{
        FlowId,
        idle_timeout::{TunnelActivity, TunnelLifecycle},
    },
    limits::{MAX_QUEUE_BUFFERED_BYTES, MaxConnections, max_connections_display},
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
    pub backend_write: OwnedWriteHalf,
    pub tunnel_lifecycle: TunnelLifecycle,
}

/// Returns whether the scheduler can admit another connection right now.
#[derive(Message)]
#[rtype(result = "bool")]
pub(crate) struct CanAcceptConnection;

#[derive(Message)]
#[rtype(result = "Result<(), RegisterError>")]
pub(crate) struct TryReserveConnectionTask {
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

struct FlowResources {
    queue_addr: Addr<QueueActor>,
    writer_addr: Addr<FlowWriter>,
    writer_cancel_tx: watch::Sender<bool>,
    tunnel_lifecycle: TunnelLifecycle,
}

impl FlowResources {
    fn new(
        queue_addr: Addr<QueueActor>,
        writer_addr: Addr<FlowWriter>,
        writer_cancel_tx: watch::Sender<bool>,
        tunnel_lifecycle: TunnelLifecycle,
    ) -> Self {
        Self {
            queue_addr,
            writer_addr,
            writer_cancel_tx,
            tunnel_lifecycle,
        }
    }

    fn queue_addr(&self) -> Addr<QueueActor> {
        self.queue_addr.clone()
    }

    fn writer_addr(&self) -> Addr<FlowWriter> {
        self.writer_addr.clone()
    }

    fn cancel_writer(&self) {
        self.writer_cancel_tx.send_replace(true);
    }

    fn tunnel_lifecycle(&self) -> TunnelLifecycle {
        self.tunnel_lifecycle.clone()
    }
}

struct FlowEntry {
    stats: FlowStats,
    writer_pending_bytes: usize,
    write_completions: u64,
}

impl FlowEntry {
    fn new() -> Self {
        Self {
            stats: FlowStats::new(),
            writer_pending_bytes: 0,
            write_completions: 0,
        }
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

#[derive(Message)]
#[rtype(result = "()")]
pub(crate) struct WriteCompleted {
    pub flow_id: FlowId,
    pub bytes: usize,
}

#[derive(Message)]
#[rtype(result = "()")]
pub(crate) struct WriteFailed {
    pub flow_id: FlowId,
    pub bytes: usize,
}

struct SchedulerState {
    flows: HashMap<FlowId, FlowEntry>,
    ready_queue: VecDeque<FlowId>,
    ready_set: HashSet<FlowId>,
    default_quantum: usize,
    total_client_to_backend_bytes: u64,
    total_backend_to_client_bytes: u64,
    global_start_time: Option<Instant>,
    total_turns: u64,
    writer_blocked: HashSet<FlowId>,
    admission: ConnectionAdmission,
    shutdown_requested: bool,
}

impl SchedulerState {
    fn new(default_quantum: usize) -> Self {
        Self {
            flows: HashMap::new(),
            ready_queue: VecDeque::new(),
            ready_set: HashSet::new(),
            default_quantum,
            total_client_to_backend_bytes: 0,
            total_backend_to_client_bytes: 0,
            global_start_time: None,
            total_turns: 0,
            writer_blocked: HashSet::new(),
            admission: ConnectionAdmission::new(),
            shutdown_requested: false,
        }
    }

    fn register(&mut self, flow_id: FlowId) -> Result<(), RegisterError> {
        if self.flows.contains_key(&flow_id) {
            return Err(RegisterError::DuplicateRegisteredConnection { flow_id });
        }

        self.admission.try_acquire_registered_connection()?;
        self.admission.release_connection_task_reservation(flow_id);
        self.flows.insert(flow_id, FlowEntry::new());
        Ok(())
    }

    fn unregister(&mut self, flow_id: FlowId) -> bool {
        if self.flows.remove(&flow_id).is_none() {
            return false;
        }

        self.admission.release_registered_connection();
        self.remove_flow_from_ready(flow_id);
        self.writer_blocked.remove(&flow_id);
        true
    }

    fn mark_flow_ready(&mut self, flow_id: FlowId) {
        if self.flows.contains_key(&flow_id)
            && !self.writer_blocked.contains(&flow_id)
            && self.ready_set.insert(flow_id)
        {
            self.ready_queue.push_back(flow_id);
        }
    }

    fn remove_flow_from_ready(&mut self, flow_id: FlowId) {
        if self.ready_set.remove(&flow_id) {
            self.ready_queue
                .retain(|queued_flow_id| *queued_flow_id != flow_id);
        }
    }

    fn next_turn(&mut self) -> Option<(FlowId, usize, usize)> {
        while let Some(flow_id) = self.ready_queue.pop_front() {
            self.ready_set.remove(&flow_id);
            let Some(flow) = self.flows.get(&flow_id) else {
                continue;
            };
            let capacity = MAX_QUEUE_BUFFERED_BYTES - flow.writer_pending_bytes;
            if capacity == 0 {
                self.writer_blocked.insert(flow_id);
                continue;
            }
            return Some((
                flow_id,
                flow.recommended_quantum(self.default_quantum),
                capacity,
            ));
        }
        None
    }

    fn on_turn_result(&mut self, flow_id: FlowId, result: TurnResult) -> Vec<bytes::Bytes> {
        self.total_turns += 1;
        let Some(flow) = self.flows.get_mut(&flow_id) else {
            return Vec::new();
        };
        for packet in &result.packets {
            flow.writer_pending_bytes += packet.len();
            assert!(flow.writer_pending_bytes <= MAX_QUEUE_BUFFERED_BYTES);
            flow.update_stats(packet.len());
            self.total_client_to_backend_bytes += packet.len() as u64;
        }
        if !result.packets.is_empty() {
            self.ensure_global_start_time();
        }
        match result.status {
            TurnStatus::More => self.mark_flow_ready(flow_id),
            TurnStatus::WriterBlocked => {
                self.remove_flow_from_ready(flow_id);
                self.writer_blocked.insert(flow_id);
            }
            // Do not erase a notification queued after this turn emptied the queue.
            TurnStatus::Empty => {}
        }
        result.packets
    }

    fn finish_write(&mut self, flow_id: FlowId, bytes: usize) -> Option<usize> {
        let flow = self.flows.get_mut(&flow_id)?;
        flow.writer_pending_bytes = flow
            .writer_pending_bytes
            .checked_sub(bytes)
            .expect("completed writer bytes exceed scheduler pending bytes");
        flow.write_completions += 1;
        Some(flow.writer_pending_bytes)
    }

    fn record_downstream_bytes(&mut self, bytes: usize) {
        self.total_backend_to_client_bytes += bytes as u64;
        self.ensure_global_start_time();
    }

    fn ensure_global_start_time(&mut self) {
        if self.global_start_time.is_none() {
            self.global_start_time = Some(Instant::now());
        }
    }

    fn should_stop(&self) -> bool {
        self.shutdown_requested
            && self.flows.is_empty()
            && self.admission.pending_connection_tasks() == 0
    }
}

pub(crate) struct Scheduler {
    state: SchedulerState,
    flow_resources: HashMap<FlowId, FlowResources>,
    turn_in_flight: bool,
    completed_turns: usize,
    yielding: bool,
    #[cfg(test)]
    runtime_yields: usize,
    #[cfg(test)]
    writer_gate: Option<Arc<Semaphore>>,
}
impl Actor for Scheduler {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_interval(Duration::from_millis(2500), |act, _ctx| act.log_stats());
    }
}

// Continuation after yielding to the runtime.
#[derive(Message)]
#[rtype(result = "()")]
struct ContinueService;

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

impl Handler<ContinueService> for Scheduler {
    type Result = ();

    fn handle(&mut self, _msg: ContinueService, ctx: &mut Self::Context) {
        self.yielding = false;
        self.completed_turns = 0;
        self.service_next(ctx);
    }
}

impl Handler<Shutdown> for Scheduler {
    type Result = ();

    fn handle(&mut self, _msg: Shutdown, ctx: &mut Self::Context) -> Self::Result {
        self.state.shutdown_requested = true;
        if self.state.should_stop() {
            ctx.stop();
        }
    }
}

impl Handler<TryReserveConnectionTask> for Scheduler {
    type Result = Result<(), RegisterError>;

    fn handle(&mut self, msg: TryReserveConnectionTask, _ctx: &mut Self::Context) -> Self::Result {
        self.state
            .admission
            .try_reserve_connection_task(msg.flow_id)
    }
}

impl Handler<ConnectionTaskFinished> for Scheduler {
    type Result = ();

    fn handle(&mut self, msg: ConnectionTaskFinished, ctx: &mut Self::Context) -> Self::Result {
        self.state
            .admission
            .release_connection_task_reservation(msg.flow_id);

        if self.state.should_stop() {
            ctx.stop();
        }
    }
}

impl Handler<RecordDownstreamBytes> for Scheduler {
    type Result = ();

    fn handle(&mut self, msg: RecordDownstreamBytes, _ctx: &mut Self::Context) -> Self::Result {
        self.state.record_downstream_bytes(msg.bytes);
    }
}

impl Scheduler {
    pub(crate) fn new(quantum: usize) -> Self {
        Self {
            state: SchedulerState::new(quantum),
            flow_resources: HashMap::new(),
            turn_in_flight: false,
            completed_turns: 0,
            yielding: false,
            #[cfg(test)]
            runtime_yields: 0,
            #[cfg(test)]
            writer_gate: None,
        }
    }

    #[cfg(test)]
    fn with_writer_gate(mut self, writer_gate: Arc<Semaphore>) -> Self {
        self.writer_gate = Some(writer_gate);
        self
    }

    /// Configure the scheduler with a maximum concurrent connection limit.
    ///
    /// Use `None` to allow unlimited connections.
    pub(crate) fn with_max_connections(mut self, max_connections: MaxConnections) -> Self {
        self.state.admission.set_max_connections(max_connections);
        self
    }

    fn current_connection_count(&self) -> usize {
        self.state.admission.current_connection_count()
    }

    fn max_connections(&self) -> MaxConnections {
        self.state.admission.max_connections()
    }

    fn log_connection_count(&self, flow_id: FlowId, action: &str) {
        let pending = self.state.admission.pending_connection_tasks();
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
}

// Handler for Register
impl Handler<Register> for Scheduler {
    type Result = Result<(), RegisterError>;

    fn handle(&mut self, msg: Register, ctx: &mut Self::Context) -> Self::Result {
        self.state.register(msg.flow_id)?;
        let flow_id = msg.flow_id;
        let (writer_cancel_tx, writer_cancel_rx) = watch::channel(false);
        let writer = FlowWriter::new(
            flow_id,
            msg.backend_write,
            ctx.address(),
            msg.tunnel_lifecycle.clone(),
            writer_cancel_rx,
        );
        #[cfg(test)]
        let writer = if let Some(writer_gate) = &self.writer_gate {
            writer.with_write_gate(writer_gate.clone())
        } else {
            writer
        };
        self.flow_resources.insert(
            flow_id,
            FlowResources::new(
                msg.queue_addr.clone(),
                writer.start(),
                writer_cancel_tx,
                msg.tunnel_lifecycle,
            ),
        );
        msg.queue_addr.do_send(BindScheduler {
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
        self.state.admission.has_registered_capacity()
    }
}

// Handler for Unregister
impl Handler<Unregister> for Scheduler {
    type Result = ();

    fn handle(&mut self, msg: Unregister, ctx: &mut Self::Context) -> Self::Result {
        if self.state.unregister(msg.flow_id) {
            if let Some(resources) = self.flow_resources.remove(&msg.flow_id) {
                debug!("flow{}: stopping queue on unregister", msg.flow_id.0);
                resources.queue_addr().do_send(StopNow);
                resources.cancel_writer();
                resources.writer_addr().do_send(StopWriter);
            }
            self.log_connection_count(msg.flow_id, "unregistered from scheduler");
        } else {
            debug!(
                "flow{}: unregister requested but flow not found",
                msg.flow_id.0
            );
        }

        if self.state.should_stop() {
            ctx.stop();
        }
    }
}

impl Handler<FlowReady> for Scheduler {
    type Result = ();

    fn handle(&mut self, msg: FlowReady, ctx: &mut Self::Context) -> Self::Result {
        // FlowReady notifications can race; mark_flow_ready handles the existence check
        // and ready_set.insert filters duplicates.
        self.state.mark_flow_ready(msg.flow_id);
        self.service_next(ctx);
    }
}

impl Handler<WriteCompleted> for Scheduler {
    type Result = ();

    fn handle(&mut self, msg: WriteCompleted, ctx: &mut Self::Context) {
        if self.state.finish_write(msg.flow_id, msg.bytes).is_some()
            && let Some(resources) = self.flow_resources.get(&msg.flow_id)
        {
            resources
                .queue_addr()
                .do_send(ReleasePendingBytes(msg.bytes));
            if self.state.writer_blocked.remove(&msg.flow_id) {
                self.state.mark_flow_ready(msg.flow_id);
            }
            self.service_next(ctx);
        }
    }
}

impl Handler<WriteFailed> for Scheduler {
    type Result = ();

    fn handle(&mut self, msg: WriteFailed, ctx: &mut Self::Context) {
        if self.state.finish_write(msg.flow_id, msg.bytes).is_some()
            && let Some(resources) = self.flow_resources.get(&msg.flow_id)
        {
            resources
                .queue_addr()
                .do_send(ReleasePendingBytes(msg.bytes));
            ctx.address().do_send(Unregister {
                flow_id: msg.flow_id,
            });
        }
    }
}

impl Scheduler {
    fn service_next(&mut self, ctx: &mut Context<Self>) {
        const MAX_TURNS_PER_RUN: usize = 32;
        if self.turn_in_flight || self.yielding {
            return;
        }
        if self.state.ready_queue.is_empty() {
            self.completed_turns = 0;
            return;
        }
        if self.completed_turns >= MAX_TURNS_PER_RUN {
            self.yielding = true;
            #[cfg(test)]
            {
                self.runtime_yields += 1;
            }
            let scheduler = ctx.address();
            // A separate runtime task is required: self-notification alone can
            // keep an Actix actor running without returning to Tokio.
            actix_rt::spawn(async move {
                tokio::task::yield_now().await;
                scheduler.do_send(ContinueService);
            });
            return;
        }
        let Some((flow, quantum, writer_capacity)) = self.state.next_turn() else {
            self.completed_turns = 0;
            return;
        };
        let Some(resources) = self.flow_resources.get(&flow) else {
            return;
        };
        let completions = self.state.flows[&flow].write_completions;
        self.turn_in_flight = true;
        resources
            .queue_addr()
            .send(ServiceTurn {
                quantum,
                writer_capacity,
            })
            .into_actor(self)
            .map(move |result, act, ctx| {
                act.turn_in_flight = false;
                act.completed_turns += 1;
                match result {
                    Ok(mut result) => {
                        if result.status == TurnStatus::WriterBlocked
                            && act
                                .state
                                .flows
                                .get(&flow)
                                .is_some_and(|entry| entry.write_completions != completions)
                        {
                            result.status = TurnStatus::More;
                        }
                        for packet in act.state.on_turn_result(flow, result) {
                            act.write(flow, packet);
                        }
                    }
                    Err(error) => {
                        warn!("flow{}: service response error: {}", flow.0, error);
                        ctx.address().do_send(Unregister { flow_id: flow });
                    }
                }
                act.service_next(ctx);
            })
            .spawn(ctx);
    }

    fn write(&self, flow: FlowId, packet: bytes::Bytes) {
        let Some(resources) = self.flow_resources.get(&flow) else {
            debug!(
                "flow{}: skipping dequeued backend write because flow is no longer registered",
                flow.0
            );
            return;
        };
        let tunnel_lifecycle = resources.tunnel_lifecycle();
        tunnel_lifecycle.record_activity(TunnelActivity::QueueDequeued);
        resources.writer_addr().do_send(WritePacket(packet));
    }

    fn log_stats(&self) {
        let flow_count = self.state.flows.len();
        let (tx_value, tx_unit) = format_bytes(self.state.total_client_to_backend_bytes);
        let (rx_value, rx_unit) = format_bytes(self.state.total_backend_to_client_bytes);
        let max_display = max_connections_display(self.max_connections());
        debug!(
            "⏱ scheduler: turns={}, active connections={}/{}, total_tx={:.2} {}, total_rx={:.2} {}",
            self.state.total_turns, flow_count, max_display, tx_value, tx_unit, rx_value, rx_unit
        );
        if let Some(global_start) = self.state.global_start_time {
            let elapsed = global_start.elapsed().as_secs_f64();
            if elapsed > 0.0 {
                let mut segments = Vec::new();
                if self.state.total_client_to_backend_bytes > 0
                    && let Some((avg_value, avg_unit)) =
                        format_rate(self.state.total_client_to_backend_bytes, elapsed)
                {
                    segments.push(format!("tx={:.2} {}", avg_value, avg_unit));
                }
                if self.state.total_backend_to_client_bytes > 0
                    && let Some((avg_value, avg_unit)) =
                        format_rate(self.state.total_backend_to_client_bytes, elapsed)
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
    pub runtime_yields: usize,
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
#[rtype(result = "Option<usize>")]
struct InspectWriterPending {
    flow_id: FlowId,
}

#[cfg(test)]
#[derive(Message)]
#[rtype(result = "Option<Addr<FlowWriter>>")]
struct InspectWriterAddr {
    flow_id: FlowId,
}

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
            ready_queue_len: self.state.ready_queue.len(),
            runtime_yields: self.runtime_yields,
            flow_ids: self.state.flows.keys().copied().collect(),
            total_client_to_backend_bytes: self.state.total_client_to_backend_bytes,
            total_backend_to_client_bytes: self.state.total_backend_to_client_bytes,
            pending_connection_tasks: self.state.admission.pending_connection_tasks(),
        })
    }
}

#[cfg(test)]
impl Handler<RecordUpstreamBytesTest> for Scheduler {
    type Result = ();

    fn handle(&mut self, msg: RecordUpstreamBytesTest, _ctx: &mut Context<Self>) -> Self::Result {
        self.state.total_client_to_backend_bytes += msg.bytes as u64;
        self.state.ensure_global_start_time();
    }
}

#[cfg(test)]
impl Handler<InspectWriterPending> for Scheduler {
    type Result = Option<usize>;

    fn handle(&mut self, msg: InspectWriterPending, _ctx: &mut Context<Self>) -> Self::Result {
        self.state
            .flows
            .get(&msg.flow_id)
            .map(|flow| flow.writer_pending_bytes)
    }
}

#[cfg(test)]
impl Handler<InspectWriterAddr> for Scheduler {
    type Result = MessageResult<InspectWriterAddr>;

    fn handle(&mut self, msg: InspectWriterAddr, _ctx: &mut Context<Self>) -> Self::Result {
        MessageResult(
            self.flow_resources
                .get(&msg.flow_id)
                .map(FlowResources::writer_addr),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::limits::{MAX_DEQUEUE_BYTES, max_connections_from_raw};
    use crate::test_utils::make_backend_write;
    use tokio::{
        io::AsyncReadExt,
        net::{TcpListener, TcpStream},
        time::{sleep, timeout},
    };

    fn test_tunnel_lifecycle() -> TunnelLifecycle {
        TunnelLifecycle::new()
    }

    async fn make_live_backend_halves()
    -> (OwnedWriteHalf, tokio::net::tcp::OwnedReadHalf, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let accept_handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            stream
        });

        let peer = TcpStream::connect(addr).await.unwrap();
        let server_stream = accept_handle.await.unwrap();
        let (read_half, write_half) = server_stream.into_split();
        (write_half, read_half, peer)
    }

    async fn make_live_backend_write() -> (OwnedWriteHalf, TcpStream) {
        let (write_half, _read_half, peer) = make_live_backend_halves().await;
        (write_half, peer)
    }

    #[actix_rt::test]
    async fn active_rounds_progress_with_frozen_time_and_yield_to_other_tasks() {
        let scheduler = Scheduler::new(1).start();
        let queue = QueueActor::new().start();
        let (backend_write, mut peer) = make_live_backend_write().await;
        let packet = bytes::Bytes::from(vec![7; 1024]);
        queue
            .send(crate::actors::queue::Enqueue(packet.clone()))
            .await
            .unwrap()
            .unwrap();
        tokio::time::pause();
        let started = Instant::now();
        scheduler
            .send(Register {
                flow_id: FlowId(201),
                queue_addr: queue,
                backend_write,
                tunnel_lifecycle: TunnelLifecycle::new(),
            })
            .await
            .unwrap()
            .unwrap();
        let observer = tokio::spawn(async {
            tokio::task::yield_now().await;
            42
        });
        // Keep polling ready work: frozen time must never advance to a timer.
        for _ in 0..10_000 {
            if scheduler
                .send(InspectState)
                .await
                .unwrap()
                .total_client_to_backend_bytes
                == 1024
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        let state = scheduler.send(InspectState).await.unwrap();
        assert_eq!(state.total_client_to_backend_bytes, 1024);
        assert!(
            state.runtime_yields >= 31,
            "zero-packet turns must count toward the work bound"
        );
        assert_eq!(observer.await.unwrap(), 42);
        assert_eq!(
            Instant::now(),
            started,
            "delivery must not depend on timer advancement"
        );
        let mut received = vec![0; 1024];
        peer.read_exact(&mut received).await.unwrap();
        assert_eq!(received, packet);
    }

    #[derive(Message)]
    #[rtype(result = "()")]
    struct ClearWriterGate;

    impl Handler<ClearWriterGate> for Scheduler {
        type Result = ();
        fn handle(&mut self, _: ClearWriterGate, _: &mut Context<Self>) {
            self.writer_gate = None;
        }
    }

    #[actix_rt::test]
    async fn blocked_writer_allows_another_flow_and_recovers_on_completion() {
        let gate = Arc::new(Semaphore::new(0));
        let scheduler = Scheduler::new(MAX_DEQUEUE_BYTES)
            .with_writer_gate(gate.clone())
            .start();
        let queue = QueueActor::new().start();
        let packet = bytes::Bytes::from(vec![1; MAX_DEQUEUE_BYTES]);
        for _ in 0..MAX_QUEUE_BUFFERED_BYTES / packet.len() {
            queue
                .send(crate::actors::queue::Enqueue(packet.clone()))
                .await
                .unwrap()
                .unwrap();
        }
        let (backend_write, mut slow_peer) = make_live_backend_write().await;
        scheduler
            .send(Register {
                flow_id: FlowId(202),
                queue_addr: queue.clone(),
                backend_write,
                tunnel_lifecycle: TunnelLifecycle::new(),
            })
            .await
            .unwrap()
            .unwrap();
        timeout(Duration::from_secs(1), async {
            while scheduler
                .send(InspectWriterPending {
                    flow_id: FlowId(202),
                })
                .await
                .unwrap()
                != Some(MAX_QUEUE_BUFFERED_BYTES)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        scheduler.send(ClearWriterGate).await.unwrap();
        let fast_queue = QueueActor::new().start();
        let (backend_write, mut fast_peer) = make_live_backend_write().await;
        scheduler
            .send(Register {
                flow_id: FlowId(203),
                queue_addr: fast_queue.clone(),
                backend_write,
                tunnel_lifecycle: TunnelLifecycle::new(),
            })
            .await
            .unwrap()
            .unwrap();
        fast_queue
            .send(crate::actors::queue::Enqueue(bytes::Bytes::from_static(
                b"fast",
            )))
            .await
            .unwrap()
            .unwrap();
        let mut received = [0; 4];
        timeout(Duration::from_secs(1), fast_peer.read_exact(&mut received))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&received, b"fast");
        assert_eq!(
            scheduler
                .send(InspectWriterPending {
                    flow_id: FlowId(202)
                })
                .await
                .unwrap(),
            Some(MAX_QUEUE_BUFFERED_BYTES)
        );
        gate.add_permits(1);
        let mut received = vec![0; MAX_DEQUEUE_BYTES];
        timeout(Duration::from_secs(1), slow_peer.read_exact(&mut received))
            .await
            .unwrap()
            .unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                let state = queue
                    .send(crate::actors::queue::InspectBufferedState)
                    .await
                    .unwrap();
                if state.writer_pending_bytes == MAX_QUEUE_BUFFERED_BYTES - MAX_DEQUEUE_BYTES {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        queue
            .send(crate::actors::queue::Enqueue(packet))
            .await
            .unwrap()
            .unwrap();
        timeout(Duration::from_secs(1), async {
            while scheduler
                .send(InspectWriterPending {
                    flow_id: FlowId(202),
                })
                .await
                .unwrap()
                != Some(MAX_QUEUE_BUFFERED_BYTES)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("completion must reopen capacity and resume the queued flow");
        scheduler
            .send(Unregister {
                flow_id: FlowId(202),
            })
            .await
            .unwrap();
    }

    #[actix_rt::test]
    async fn record_downstream_bytes_updates_totals() {
        let scheduler = Scheduler::new(1024).start();

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
        let scheduler = Scheduler::new(1024).start();

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
    async fn upstream_dequeue_refreshes_tunnel_lifecycle() {
        let scheduler = Scheduler::new(1024).start();

        let queue = QueueActor::new().start();
        let (backend_write, _backend_peer) = make_live_backend_write().await;
        let tunnel_lifecycle = TunnelLifecycle::new();
        let mut activity_rx = tunnel_lifecycle.subscribe_activity();
        scheduler
            .send(Register {
                flow_id: FlowId(11),
                queue_addr: queue.clone(),
                backend_write,
                tunnel_lifecycle,
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

        timeout(Duration::from_secs(1), activity_rx.changed())
            .await
            .expect("upstream dequeue should refresh tunnel traffic before timeout")
            .expect("traffic signal sender should remain alive");
    }

    #[actix_rt::test]
    async fn backend_write_waiting_for_lock_stops_after_idle_shutdown() {
        let scheduler = Scheduler::new(1024).start();

        let queue = QueueActor::new().start();
        let (backend_write, mut backend_peer) = make_live_backend_write().await;
        let tunnel_lifecycle = TunnelLifecycle::new();
        scheduler
            .send(Register {
                flow_id: FlowId(12),
                queue_addr: queue.clone(),
                backend_write,
                tunnel_lifecycle: tunnel_lifecycle.clone(),
            })
            .await
            .unwrap()
            .unwrap();

        tunnel_lifecycle.shutdown_for_test();
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
        sleep(Duration::from_millis(10)).await;

        let mut buf = [0u8; 64];
        let read_result = timeout(Duration::from_millis(50), backend_peer.read(&mut buf)).await;
        assert!(
            matches!(read_result, Err(_) | Ok(Ok(0))),
            "idle shutdown must not write the queued payload"
        );
    }

    #[actix_rt::test]
    async fn backend_write_error_unregisters_flow_and_releases_capacity() {
        let scheduler = Scheduler::new(1024)
            .with_max_connections(max_connections_from_raw(1))
            .start();
        let queue = QueueActor::new().start();
        let (backend_write, mut backend_read, backend_peer) = make_live_backend_halves().await;
        let flow_id = FlowId(15);

        scheduler
            .send(Register {
                flow_id,
                queue_addr: queue.clone(),
                backend_write,
                tunnel_lifecycle: test_tunnel_lifecycle(),
            })
            .await
            .unwrap()
            .unwrap();

        #[allow(deprecated)]
        backend_peer.set_linger(Some(Duration::ZERO)).unwrap();
        drop(backend_peer);

        let mut reset_probe = [0; 1];
        let reset_result = timeout(Duration::from_secs(1), backend_read.read(&mut reset_probe))
            .await
            .expect("server should observe the peer reset before attempting a backend write");
        assert!(
            reset_result.is_err(),
            "server read half should observe the backend peer reset"
        );

        queue
            .send(crate::actors::queue::Enqueue(bytes::Bytes::from_static(
                b"write after backend reset",
            )))
            .await
            .unwrap()
            .unwrap();
        scheduler.send(FlowReady { flow_id }).await.unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                if scheduler
                    .send(super::InspectState)
                    .await
                    .unwrap()
                    .connections
                    == 0
                {
                    break;
                }
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("backend reset should cause a write error");

        let reply = scheduler.send(super::InspectState).await.unwrap();
        assert_eq!(
            reply.connections, 0,
            "a backend write error should promptly unregister the flow"
        );
        assert!(
            reply.flow_ids.is_empty(),
            "unregistered flow should be removed"
        );
        assert!(
            scheduler.send(CanAcceptConnection).await.unwrap(),
            "unregistering after a backend write error should release admission capacity"
        );
    }

    #[actix_rt::test]
    async fn flow_writer_preserves_dequeue_order() {
        let scheduler = Scheduler::new(1024).start();
        let queue = QueueActor::new().start();
        let (backend_write, mut backend_peer) = make_live_backend_write().await;
        let tunnel_lifecycle = TunnelLifecycle::new();
        let flow = FlowId(14);
        let packets: [&[u8]; 3] = [b"first-", b"second-", b"third"];

        scheduler
            .send(Register {
                flow_id: flow,
                queue_addr: queue.clone(),
                backend_write,
                tunnel_lifecycle,
            })
            .await
            .unwrap()
            .unwrap();

        for packet in packets {
            queue
                .send(crate::actors::queue::Enqueue(
                    bytes::Bytes::copy_from_slice(packet),
                ))
                .await
                .unwrap()
                .unwrap();
        }

        scheduler.send(FlowReady { flow_id: flow }).await.unwrap();
        for _ in 0..packets.len() {}

        let mut received = vec![0; packets.iter().map(|packet| packet.len()).sum()];
        timeout(
            Duration::from_secs(1),
            backend_peer.read_exact(&mut received),
        )
        .await
        .expect("releasing the writer should deliver every pending packet")
        .expect("live backend peer should receive the pending packets");
        let buffered = queue
            .send(crate::actors::queue::InspectBufferedState)
            .await
            .unwrap();
        assert_eq!(
            buffered.packet_count, 0,
            "QueueActor buffered packets should be drained after dequeue"
        );
        assert_eq!(
            buffered.buffered_bytes, 0,
            "QueueActor buffered bytes should be zero"
        );

        assert_eq!(
            received,
            packets.concat(),
            "the per-flow writer should reach the backend in dequeue order"
        );
    }

    #[actix_rt::test]
    async fn blocked_flow_writer_keeps_total_pending_within_budget() {
        let write_gate = Arc::new(Semaphore::new(0));
        let scheduler = Scheduler::new(MAX_DEQUEUE_BYTES)
            .with_writer_gate(write_gate)
            .start();
        let queue = QueueActor::new().start();
        let (backend_write, _backend_peer) = make_live_backend_write().await;
        let flow = FlowId(15);
        scheduler
            .send(Register {
                flow_id: flow,
                queue_addr: queue.clone(),
                backend_write,
                tunnel_lifecycle: TunnelLifecycle::new(),
            })
            .await
            .unwrap()
            .unwrap();

        let packet = bytes::Bytes::from(vec![0_u8; MAX_DEQUEUE_BYTES]);
        let packet_count = MAX_QUEUE_BUFFERED_BYTES / MAX_DEQUEUE_BYTES;
        for _ in 0..packet_count {
            queue
                .send(crate::actors::queue::Enqueue(packet.clone()))
                .await
                .unwrap()
                .unwrap();
        }
        scheduler.send(FlowReady { flow_id: flow }).await.unwrap();

        timeout(Duration::from_secs(2), async {
            loop {
                if scheduler
                    .send(InspectWriterPending { flow_id: flow })
                    .await
                    .unwrap()
                    == Some(MAX_QUEUE_BUFFERED_BYTES)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("active queues should drain without timer ticks");

        let state = queue
            .send(crate::actors::queue::InspectBufferedState)
            .await
            .unwrap();
        assert_eq!(state.buffered_bytes, 0);
        assert_eq!(state.writer_pending_bytes, MAX_QUEUE_BUFFERED_BYTES);
        assert!(matches!(
            queue
                .send(crate::actors::queue::Enqueue(bytes::Bytes::from_static(
                    b"x"
                )))
                .await
                .unwrap(),
            Err(crate::actors::queue::EnqueueError::QueueBufferExceeded { .. })
        ));

        let writer = scheduler
            .send(InspectWriterAddr { flow_id: flow })
            .await
            .unwrap()
            .expect("registered flow should expose its writer in tests");
        scheduler.send(Unregister { flow_id: flow }).await.unwrap();
        timeout(Duration::from_secs(1), async {
            while writer.connected() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("unregister should stop a writer blocked by the test gate");
    }

    #[test]
    fn stale_turn_after_unregister_does_not_write() {
        let mut state = SchedulerState::new(1024);
        let flow = FlowId(13);
        state.register(flow).unwrap();
        state.unregister(flow);
        assert!(
            state
                .on_turn_result(
                    flow,
                    TurnResult {
                        packets: vec![bytes::Bytes::from_static(b"stale")],
                        status: TurnStatus::More,
                    }
                )
                .is_empty()
        );
        assert!(state.ready_queue.is_empty());
    }

    #[test]
    fn saturated_flow_parks_while_another_progresses() {
        let mut state = SchedulerState::new(1024);
        let slow = FlowId(101);
        let fast = FlowId(102);
        state.register(slow).unwrap();
        state.register(fast).unwrap();
        state.flows.get_mut(&slow).unwrap().writer_pending_bytes = MAX_QUEUE_BUFFERED_BYTES;
        state.mark_flow_ready(slow);
        state.mark_flow_ready(fast);
        assert_eq!(state.next_turn().unwrap().0, fast);
        assert!(state.writer_blocked.contains(&slow));
        assert!(state.next_turn().is_none());
        state.finish_write(slow, MAX_QUEUE_BUFFERED_BYTES).unwrap();
        state.writer_blocked.remove(&slow);
        state.mark_flow_ready(slow);
        assert_eq!(state.next_turn().unwrap().0, slow);
    }

    #[test]
    fn backlogged_flows_rotate_and_notifications_survive_empty_result() {
        let mut state = SchedulerState::new(1024);
        let first = FlowId(101);
        let second = FlowId(102);
        state.register(first).unwrap();
        state.register(second).unwrap();
        state.mark_flow_ready(first);
        state.mark_flow_ready(second);
        assert_eq!(state.next_turn().unwrap().0, first);
        state.on_turn_result(
            first,
            TurnResult {
                packets: vec![],
                status: TurnStatus::More,
            },
        );
        assert_eq!(state.next_turn().unwrap().0, second);
        assert_eq!(state.next_turn().unwrap().0, first);
        // Enqueue notification arrives after a turn empties its queue, before its reply.
        state.mark_flow_ready(first);
        state.on_turn_result(
            first,
            TurnResult {
                packets: vec![],
                status: TurnStatus::Empty,
            },
        );
        assert_eq!(state.next_turn().unwrap().0, first);
        assert!(state.next_turn().is_none());
    }

    #[actix_rt::test]
    async fn register_respects_max_connection_limit() {
        let scheduler = Scheduler::new(1024)
            .with_max_connections(max_connections_from_raw(1))
            .start();

        let queue1 = QueueActor::new().start();
        let backend_write1 = make_backend_write().await;
        let result1 = scheduler
            .send(Register {
                flow_id: FlowId(1),
                queue_addr: queue1,
                backend_write: backend_write1,
                tunnel_lifecycle: test_tunnel_lifecycle(),
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
                tunnel_lifecycle: test_tunnel_lifecycle(),
            })
            .await
            .unwrap();
        assert!(
            matches!(result2, Err(RegisterError::MaxConnectionsReached { .. })),
            "second registration should be rejected when at limit"
        );
    }

    #[actix_rt::test]
    async fn register_rejects_duplicate_flow_id_without_consuming_capacity() {
        let scheduler = Scheduler::new(1024)
            .with_max_connections(max_connections_from_raw(2))
            .start();
        let flow_id = FlowId(1);

        scheduler
            .send(Register {
                flow_id,
                queue_addr: QueueActor::new().start(),
                backend_write: make_backend_write().await,
                tunnel_lifecycle: test_tunnel_lifecycle(),
            })
            .await
            .unwrap()
            .unwrap();

        let duplicate = scheduler
            .send(Register {
                flow_id,
                queue_addr: QueueActor::new().start(),
                backend_write: make_backend_write().await,
                tunnel_lifecycle: test_tunnel_lifecycle(),
            })
            .await
            .unwrap();
        assert!(matches!(
            duplicate,
            Err(RegisterError::DuplicateRegisteredConnection { flow_id: FlowId(1) })
        ));

        let reply = scheduler.send(super::InspectState).await.unwrap();
        assert_eq!(
            reply.connections, 1,
            "duplicate registration must not use capacity"
        );

        scheduler.send(Unregister { flow_id }).await.unwrap();
        scheduler
            .send(Register {
                flow_id: FlowId(2),
                queue_addr: QueueActor::new().start(),
                backend_write: make_backend_write().await,
                tunnel_lifecycle: test_tunnel_lifecycle(),
            })
            .await
            .unwrap()
            .unwrap();
    }

    #[actix_rt::test]
    async fn register_allows_new_connection_after_unregister() {
        let scheduler = Scheduler::new(1024)
            .with_max_connections(max_connections_from_raw(1))
            .start();

        let queue1 = QueueActor::new().start();
        let backend_write1 = make_backend_write().await;
        scheduler
            .send(Register {
                flow_id: FlowId(10),
                queue_addr: queue1,
                backend_write: backend_write1,
                tunnel_lifecycle: test_tunnel_lifecycle(),
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
                tunnel_lifecycle: test_tunnel_lifecycle(),
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
        let scheduler = Scheduler::new(1024)
            .with_max_connections(max_connections_from_raw(1))
            .start();

        let queue = QueueActor::new().start();
        let backend_write = make_backend_write().await;
        scheduler
            .send(Register {
                flow_id: FlowId(1),
                queue_addr: queue,
                backend_write,
                tunnel_lifecycle: test_tunnel_lifecycle(),
            })
            .await
            .unwrap()
            .unwrap();

        let can_accept = scheduler.send(CanAcceptConnection).await.unwrap();
        assert!(!can_accept, "should refuse connections when at limit");
    }

    #[actix_rt::test]
    async fn can_accept_connection_returns_true_when_below_capacity() {
        let scheduler = Scheduler::new(1024)
            .with_max_connections(max_connections_from_raw(2))
            .start();

        let queue = QueueActor::new().start();
        let backend_write = make_backend_write().await;
        scheduler
            .send(Register {
                flow_id: FlowId(1),
                queue_addr: queue,
                backend_write,
                tunnel_lifecycle: test_tunnel_lifecycle(),
            })
            .await
            .unwrap()
            .unwrap();

        let can_accept = scheduler.send(CanAcceptConnection).await.unwrap();
        assert!(can_accept, "should allow connection while under limit");
    }

    #[actix_rt::test]
    async fn can_accept_connection_returns_true_when_unlimited() {
        let scheduler = Scheduler::new(1024)
            .with_max_connections(max_connections_from_raw(0))
            .start();

        let queue = QueueActor::new().start();
        let backend_write = make_backend_write().await;
        scheduler
            .send(Register {
                flow_id: FlowId(1),
                queue_addr: queue,
                backend_write,
                tunnel_lifecycle: test_tunnel_lifecycle(),
            })
            .await
            .unwrap()
            .unwrap();

        let can_accept = scheduler.send(CanAcceptConnection).await.unwrap();
        assert!(can_accept, "unlimited scheduler should always allow");
    }

    #[actix_rt::test]
    async fn try_reserve_connection_task_respects_registered_and_pending_limit() {
        let scheduler = Scheduler::new(1024)
            .with_max_connections(max_connections_from_raw(2))
            .start();

        scheduler
            .send(TryReserveConnectionTask { flow_id: FlowId(1) })
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
                tunnel_lifecycle: test_tunnel_lifecycle(),
            })
            .await
            .unwrap()
            .unwrap();

        let result = scheduler
            .send(TryReserveConnectionTask { flow_id: FlowId(2) })
            .await
            .unwrap();
        assert!(
            matches!(result, Err(RegisterError::MaxConnectionsReached { .. })),
            "active + pending connections should consume the configured limit"
        );
    }

    #[actix_rt::test]
    async fn try_reserve_connection_task_allows_new_task_after_release() {
        let scheduler = Scheduler::new(1024)
            .with_max_connections(max_connections_from_raw(1))
            .start();

        scheduler
            .send(TryReserveConnectionTask { flow_id: FlowId(1) })
            .await
            .unwrap()
            .unwrap();
        scheduler
            .send(ConnectionTaskFinished { flow_id: FlowId(1) })
            .await
            .unwrap();

        scheduler
            .send(TryReserveConnectionTask { flow_id: FlowId(2) })
            .await
            .unwrap()
            .unwrap();
    }

    #[actix_rt::test]
    async fn try_reserve_connection_task_rejects_duplicate_flow_id() {
        let scheduler = Scheduler::new(1024)
            .with_max_connections(max_connections_from_raw(1))
            .start();

        scheduler
            .send(TryReserveConnectionTask { flow_id: FlowId(1) })
            .await
            .unwrap()
            .unwrap();

        let result = scheduler
            .send(TryReserveConnectionTask { flow_id: FlowId(1) })
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
            "duplicate reservations should not add another pending flow ID"
        );
    }

    #[actix_rt::test]
    async fn register_consumes_pending_connection_task_reservation() {
        let scheduler = Scheduler::new(1024)
            .with_max_connections(max_connections_from_raw(2))
            .start();

        scheduler
            .send(TryReserveConnectionTask { flow_id: FlowId(1) })
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
                tunnel_lifecycle: test_tunnel_lifecycle(),
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
            .send(TryReserveConnectionTask { flow_id: FlowId(2) })
            .await
            .unwrap()
            .unwrap();
    }

    #[actix_rt::test]
    async fn unregister_updates_connection_count_and_ready_queue() {
        let scheduler = Scheduler::new(1024)
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
                    tunnel_lifecycle: test_tunnel_lifecycle(),
                })
                .await
                .unwrap()
                .unwrap();
        }

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
        let scheduler = Scheduler::new(1024).start();

        let queue = QueueActor::new().start();
        let backend_write = make_backend_write().await;
        scheduler
            .send(Register {
                flow_id: FlowId(99),
                queue_addr: queue.clone(),
                backend_write,
                tunnel_lifecycle: test_tunnel_lifecycle(),
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
                .send(crate::actors::queue::InspectBufferedState)
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
        let scheduler = Scheduler::new(1024).start();

        scheduler
            .send(TryReserveConnectionTask { flow_id: FlowId(1) })
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
        let scheduler = Scheduler::new(1024).start();

        let queue = QueueActor::new().start();
        let backend_write = make_backend_write().await;
        scheduler
            .send(Register {
                flow_id: FlowId(7),
                queue_addr: queue.clone(),
                backend_write,
                tunnel_lifecycle: test_tunnel_lifecycle(),
            })
            .await
            .unwrap()
            .unwrap();

        queue.do_send(StopNow);
        scheduler
            .send(FlowReady { flow_id: FlowId(7) })
            .await
            .unwrap();

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
        let default_quantum = 8192;

        // Case 1: No stats -> default_quantum
        let flow = FlowEntry::new();
        assert_eq!(
            flow.recommended_quantum(default_quantum),
            default_quantum,
            "Should return default quantum when no stats available"
        );

        // Case 2: Small packets (< 200 bytes) -> MIN_QUANTUM (1500)
        let mut flow = FlowEntry::new();
        flow.update_stats(100); // Set avg to 100
        assert_eq!(
            flow.recommended_quantum(default_quantum),
            1500,
            "Should return MIN_QUANTUM for small packets"
        );

        // Case 3: Normal packets -> Scaled (avg * 10)
        let mut flow = FlowEntry::new();
        flow.update_stats(500); // Set avg to 500
        // Target = 500 * 10 = 5000
        assert_eq!(
            flow.recommended_quantum(default_quantum),
            5000,
            "Should scale quantum for normal packets"
        );

        // Case 4: Large packets -> MAX_QUANTUM (16384)
        let mut flow = FlowEntry::new();
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
        let default_quantum = 4096;

        let mut flow = FlowEntry::new();

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
