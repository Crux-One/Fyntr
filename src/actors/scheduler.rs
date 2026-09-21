use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

mod connection_admission;
mod flow_stats;
mod quantum_strategy;

use actix::prelude::*;
use log::{debug, info, trace, warn};
#[cfg(test)]
use tokio::net::tcp::OwnedReadHalf;
#[cfg(test)]
use tokio::sync::Notify;
use tokio::{io::AsyncWriteExt, net::tcp::OwnedWriteHalf, sync::Mutex, time::Instant};

use crate::{
    actors::queue::{AddQuantum, BindScheduler, Dequeue, DequeueResult, QueueActor, StopNow},
    flow::{
        FlowId,
        idle_timeout::{TunnelActivity, TunnelLifecycle},
    },
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
    backend_write: Arc<Mutex<OwnedWriteHalf>>,
    tunnel_lifecycle: TunnelLifecycle,
}

impl FlowResources {
    fn new(
        queue_addr: Addr<QueueActor>,
        backend_write: Arc<Mutex<OwnedWriteHalf>>,
        tunnel_lifecycle: TunnelLifecycle,
    ) -> Self {
        Self {
            queue_addr,
            backend_write,
            tunnel_lifecycle,
        }
    }

    fn queue_addr(&self) -> Addr<QueueActor> {
        self.queue_addr.clone()
    }

    fn backend_write(&self) -> Arc<Mutex<OwnedWriteHalf>> {
        self.backend_write.clone()
    }

    fn tunnel_lifecycle(&self) -> TunnelLifecycle {
        self.tunnel_lifecycle.clone()
    }
}

struct FlowEntry {
    stats: FlowStats,
}

impl FlowEntry {
    fn new() -> Self {
        Self {
            stats: FlowStats::new(),
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

struct SchedulerState {
    flows: HashMap<FlowId, FlowEntry>,
    ready_queue: VecDeque<FlowId>,
    ready_set: HashSet<FlowId>,
    default_quantum: usize,
    total_client_to_backend_bytes: u64,
    total_backend_to_client_bytes: u64,
    global_start_time: Option<Instant>,
    total_ticks: u64,
    admission: ConnectionAdmission,
    shutdown_requested: bool,
}

/// Test-only synchronization for observing write tasks after queue dequeue.
///
/// This deliberately observes work outside `QueueActor`; it is not a production
/// accounting mechanism or a scheduler limit.
#[cfg(test)]
struct BackendWriteObserver {
    started: AtomicUsize,
    finished: AtomicUsize,
    errors: AtomicUsize,
    started_notify: Notify,
    finished_notify: Notify,
    errors_notify: Notify,
}

#[cfg(test)]
impl BackendWriteObserver {
    fn new() -> Self {
        Self {
            started: AtomicUsize::new(0),
            finished: AtomicUsize::new(0),
            errors: AtomicUsize::new(0),
            started_notify: Notify::new(),
            finished_notify: Notify::new(),
            errors_notify: Notify::new(),
        }
    }

    fn mark_started(&self) {
        self.started.fetch_add(1, Ordering::SeqCst);
        self.started_notify.notify_waiters();
    }

    fn mark_finished(&self) {
        self.finished.fetch_add(1, Ordering::SeqCst);
        self.finished_notify.notify_waiters();
    }

    fn mark_error(&self) {
        self.errors.fetch_add(1, Ordering::SeqCst);
        self.errors_notify.notify_waiters();
    }

    fn started(&self) -> usize {
        self.started.load(Ordering::SeqCst)
    }

    fn finished(&self) -> usize {
        self.finished.load(Ordering::SeqCst)
    }

    async fn wait_for_started(&self, expected: usize) {
        loop {
            let notified = self.started_notify.notified();
            if self.started() >= expected {
                return;
            }
            notified.await;
        }
    }

    async fn wait_for_finished(&self, expected: usize) {
        loop {
            let notified = self.finished_notify.notified();
            if self.finished() >= expected {
                return;
            }
            notified.await;
        }
    }

    async fn wait_for_error(&self, expected: usize) {
        loop {
            let notified = self.errors_notify.notified();
            if self.errors.load(Ordering::SeqCst) >= expected {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
struct BackendWriteTaskCompletion(Arc<BackendWriteObserver>);

#[cfg(test)]
impl Drop for BackendWriteTaskCompletion {
    fn drop(&mut self) {
        self.0.mark_finished();
    }
}

enum SchedulerEffect {
    AddQuantum {
        flow_id: FlowId,
        quantum: usize,
    },
    RequestDequeue {
        flow_id: FlowId,
    },
    Write {
        flow_id: FlowId,
        packet: bytes::Bytes,
    },
    Unregister {
        flow_id: FlowId,
    },
}

enum DequeueEvent {
    Packet(DequeueResult),
    Empty,
    Failed,
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
            total_ticks: 0,
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
        true
    }

    fn mark_flow_ready(&mut self, flow_id: FlowId) {
        if self.flows.contains_key(&flow_id) && self.ready_set.insert(flow_id) {
            self.ready_queue.push_back(flow_id);
        }
    }

    fn remove_flow_from_ready(&mut self, flow_id: FlowId) {
        if self.ready_set.remove(&flow_id) {
            self.ready_queue
                .retain(|queued_flow_id| *queued_flow_id != flow_id);
        }
    }

    fn on_tick(&mut self) -> (Vec<SchedulerEffect>, bool) {
        self.total_ticks += 1;
        let mut effects = self
            .flows
            .iter()
            .map(|(&flow_id, flow)| SchedulerEffect::AddQuantum {
                flow_id,
                quantum: flow.recommended_quantum(self.default_quantum),
            })
            .collect::<Vec<_>>();

        let ready_count = self.ready_queue.len();
        for _ in 0..ready_count {
            let Some(flow_id) = self.ready_queue.pop_front() else {
                break;
            };
            self.ready_set.remove(&flow_id);
            if self.flows.contains_key(&flow_id) {
                effects.push(SchedulerEffect::RequestDequeue { flow_id });
            }
        }

        (effects, self.total_ticks.is_multiple_of(500))
    }

    fn on_dequeue_result(
        &mut self,
        flow_id: FlowId,
        event: DequeueEvent,
    ) -> Option<SchedulerEffect> {
        match event {
            DequeueEvent::Packet(result) => self.on_dequeue_success(flow_id, result),
            DequeueEvent::Empty => {
                self.remove_flow_from_ready(flow_id);
                None
            }
            DequeueEvent::Failed => {
                self.remove_flow_from_ready(flow_id);
                Some(SchedulerEffect::Unregister { flow_id })
            }
        }
    }

    fn on_dequeue_success(
        &mut self,
        flow_id: FlowId,
        result: DequeueResult,
    ) -> Option<SchedulerEffect> {
        let flow = self.flows.get_mut(&flow_id)?;
        flow.update_stats(result.packet.len());
        self.total_client_to_backend_bytes += result.packet.len() as u64;
        self.ensure_global_start_time();
        if result.ready_for_more {
            self.mark_flow_ready(flow_id);
        }
        Some(SchedulerEffect::Write {
            flow_id,
            packet: result.packet,
        })
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
    tick: Duration,
    #[cfg(test)]
    backend_write_observer: Option<Arc<BackendWriteObserver>>,
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
        let (effects, should_log) = self.state.on_tick();
        self.execute_effects(effects, ctx);
        if should_log {
            self.log_stats();
        }
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
    pub(crate) fn new(quantum: usize, tick: Duration) -> Self {
        Self {
            state: SchedulerState::new(quantum),
            flow_resources: HashMap::new(),
            tick,
            #[cfg(test)]
            backend_write_observer: None,
        }
    }

    #[cfg(test)]
    fn with_backend_write_observer(mut self, observer: Arc<BackendWriteObserver>) -> Self {
        self.backend_write_observer = Some(observer);
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
        self.flow_resources.insert(
            flow_id,
            FlowResources::new(
                msg.queue_addr.clone(),
                msg.backend_write,
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

    fn handle(&mut self, msg: FlowReady, _ctx: &mut Self::Context) -> Self::Result {
        // FlowReady notifications can race; mark_flow_ready handles the existence check
        // and ready_set.insert filters duplicates.
        self.state.mark_flow_ready(msg.flow_id);
    }
}

impl Scheduler {
    fn execute_effects(&mut self, effects: Vec<SchedulerEffect>, ctx: &mut Context<Self>) {
        for effect in effects {
            match effect {
                SchedulerEffect::AddQuantum { flow_id, quantum } => {
                    trace!("flow{}: assigned quantum {}", flow_id.0, quantum);
                    if let Some(resources) = self.flow_resources.get(&flow_id) {
                        resources.queue_addr().do_send(AddQuantum(quantum));
                    }
                }
                SchedulerEffect::RequestDequeue { flow_id } => self.request_dequeue(flow_id, ctx),
                SchedulerEffect::Write { flow_id, packet } => {
                    self.write(flow_id, packet, ctx.address())
                }
                SchedulerEffect::Unregister { flow_id } => {
                    ctx.address().do_send(Unregister { flow_id });
                }
            }
        }
    }

    fn request_dequeue(&self, flow: FlowId, ctx: &mut Context<Self>) {
        let Some(resources) = self.flow_resources.get(&flow) else {
            return;
        };
        let queue_addr = resources.queue_addr();
        queue_addr
            .send(Dequeue {
                max_bytes: MAX_DEQUEUE_BYTES,
            })
            .into_actor(self)
            .map(move |res, act, ctx| {
                if let Ok(Some(result)) = &res {
                    debug!(
                        "flow{}: dequeue granted (remaining_queue={})",
                        flow.0, result.remaining
                    );
                }
                if let Err(error) = &res {
                    warn!("flow{}: dequeue response error: {}", flow.0, error);
                }
                let event = match res {
                    Ok(Some(result)) => DequeueEvent::Packet(result),
                    Ok(None) => DequeueEvent::Empty,
                    Err(_) => DequeueEvent::Failed,
                };
                if let Some(effect) = act.state.on_dequeue_result(flow, event) {
                    act.execute_effects(vec![effect], ctx);
                }
            })
            .spawn(ctx);
    }

    fn write(&self, flow: FlowId, packet: bytes::Bytes, scheduler: Addr<Self>) {
        let Some(resources) = self.flow_resources.get(&flow) else {
            debug!(
                "flow{}: skipping dequeued backend write because flow is no longer registered",
                flow.0
            );
            return;
        };
        let backend_write = resources.backend_write();
        let tunnel_lifecycle = resources.tunnel_lifecycle();
        tunnel_lifecycle.record_activity(TunnelActivity::QueueDequeued);
        self.spawn_backend_write(flow, backend_write, packet, tunnel_lifecycle, scheduler);
    }

    fn spawn_backend_write(
        &self,
        flow: FlowId,
        backend_write: Arc<Mutex<OwnedWriteHalf>>,
        data: bytes::Bytes,
        tunnel_lifecycle: TunnelLifecycle,
        scheduler: Addr<Self>,
    ) {
        #[cfg(test)]
        let observer = self.backend_write_observer.clone();

        actix::spawn(async move {
            #[cfg(test)]
            let _completion = observer.as_ref().map(|observer| {
                observer.mark_started();
                BackendWriteTaskCompletion(observer.clone())
            });

            let mut shutdown_rx = tunnel_lifecycle.subscribe_shutdown();

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
                        scheduler.do_send(Unregister { flow_id: flow });
                        #[cfg(test)]
                        if let Some(observer) = &observer {
                            observer.mark_error();
                        }
                    } else {
                        tunnel_lifecycle.record_activity(TunnelActivity::BackendWrite);
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

    fn log_stats(&self) {
        let flow_count = self.state.flows.len();
        let (tx_value, tx_unit) = format_bytes(self.state.total_client_to_backend_bytes);
        let (rx_value, rx_unit) = format_bytes(self.state.total_backend_to_client_bytes);
        let max_display = max_connections_display(self.max_connections());
        debug!(
            "⏱ scheduler: ticks={}, active connections={}/{}, total_tx={:.2} {}, total_rx={:.2} {}",
            self.state.total_ticks, flow_count, max_display, tx_value, tx_unit, rx_value, rx_unit
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
            ready_queue_len: self.state.ready_queue.len(),
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
mod tests {
    use super::*;
    use crate::limits::max_connections_from_raw;
    use crate::test_utils::make_backend_write;
    use tokio::{
        io::AsyncReadExt,
        net::{TcpListener, TcpStream},
        time::{sleep, timeout},
    };

    fn test_tunnel_lifecycle() -> TunnelLifecycle {
        TunnelLifecycle::new()
    }

    async fn make_live_backend_halves() -> (Arc<Mutex<OwnedWriteHalf>>, OwnedReadHalf, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let accept_handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            stream
        });

        let peer = TcpStream::connect(addr).await.unwrap();
        let server_stream = accept_handle.await.unwrap();
        let (read_half, write_half) = server_stream.into_split();
        (Arc::new(Mutex::new(write_half)), read_half, peer)
    }

    async fn make_live_backend_write() -> (Arc<Mutex<OwnedWriteHalf>>, TcpStream) {
        let (write_half, _read_half, peer) = make_live_backend_halves().await;
        (write_half, peer)
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
    async fn upstream_dequeue_refreshes_tunnel_lifecycle() {
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600)).start();

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
        let tunnel_lifecycle = TunnelLifecycle::new();
        scheduler
            .send(Register {
                flow_id: FlowId(12),
                queue_addr: queue.clone(),
                backend_write: backend_write.clone(),
                tunnel_lifecycle: tunnel_lifecycle.clone(),
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

        tunnel_lifecycle.shutdown_for_test();
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
    async fn backend_write_error_unregisters_flow_and_releases_capacity() {
        let observer = Arc::new(BackendWriteObserver::new());
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600))
            .with_max_connections(max_connections_from_raw(1))
            .with_backend_write_observer(observer.clone())
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
        scheduler.send(QuantumTick).await.unwrap();
        timeout(Duration::from_secs(1), observer.wait_for_error(1))
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
    async fn dequeued_packets_become_out_of_queue_pending_writes_while_backend_is_blocked() {
        let observer = Arc::new(BackendWriteObserver::new());
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600))
            .with_backend_write_observer(observer.clone())
            .start();
        let queue = QueueActor::new().start();
        let (backend_write, mut backend_peer) = make_live_backend_write().await;
        let write_guard = backend_write.lock().await;
        let tunnel_lifecycle = TunnelLifecycle::new();
        let flow = FlowId(14);
        let packets: [&[u8]; 3] = [b"first-", b"second-", b"third"];

        scheduler
            .send(Register {
                flow_id: flow,
                queue_addr: queue.clone(),
                backend_write: backend_write.clone(),
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
        for expected_started in 1..=packets.len() {
            scheduler.send(QuantumTick).await.unwrap();
            timeout(
                Duration::from_secs(1),
                observer.wait_for_started(expected_started),
            )
            .await
            .expect(
                "each scheduler tick should dequeue one packet and start its backend write task",
            );
        }

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
            "QueueActor buffered bytes should be zero; they do not include pending backend writes"
        );
        assert_eq!(
            observer.started(),
            packets.len(),
            "one out-of-queue backend write task should start for every dequeued packet"
        );
        assert_eq!(
            observer.finished(),
            0,
            "held backend write mutex keeps all out-of-queue write operations unfinished"
        );

        drop(write_guard);

        let mut received = vec![0; packets.iter().map(|packet| packet.len()).sum()];
        timeout(
            Duration::from_secs(1),
            backend_peer.read_exact(&mut received),
        )
        .await
        .expect("releasing the writer should deliver every pending packet")
        .expect("live backend peer should receive the pending packets");
        assert_eq!(
            received,
            packets.concat(),
            "mutex-serialized pending writes should reach the backend in dequeue order"
        );
        timeout(
            Duration::from_secs(1),
            observer.wait_for_finished(packets.len()),
        )
        .await
        .expect("all pending backend write tasks should finish after the writer is released");
        assert_eq!(
            observer.finished(),
            packets.len(),
            "no out-of-queue backend write task should remain unfinished"
        );
    }

    #[actix_rt::test]
    async fn stale_dequeue_response_after_unregister_does_not_write_to_backend() {
        let mut state = SchedulerState::new(1024);
        let flow = FlowId(13);

        state.register(flow).unwrap();
        assert!(state.unregister(flow));

        let effect = state.on_dequeue_result(
            flow,
            DequeueEvent::Packet(DequeueResult {
                packet: bytes::Bytes::from_static(b"stale dequeue response"),
                remaining: 0,
                ready_for_more: false,
            }),
        );
        assert!(effect.is_none(), "stale dequeues must not produce writes");
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
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600))
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
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600))
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
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600))
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
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600))
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
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600))
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
                    tunnel_lifecycle: test_tunnel_lifecycle(),
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
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600)).start();

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
