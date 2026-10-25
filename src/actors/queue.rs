use std::collections::VecDeque;
use std::{error::Error, fmt};

use actix::prelude::*;
use bytes::Bytes;

use crate::{
    actors::scheduler::{FlowReady, Scheduler},
    flow::FlowId,
    limits::{MAX_DEQUEUE_BYTES, MAX_QUEUE_BUFFERED_BYTES, MAX_QUEUE_PACKET_BYTES},
};

#[derive(Message)]
#[rtype(result = "Result<(), EnqueueError>")]
pub(crate) struct Enqueue(pub Bytes);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EnqueueError {
    PacketTooLarge {
        packet_bytes: usize,
        max_packet_bytes: usize,
    },
    QueueBufferExceeded {
        attempted_total: usize,
        max_buffered_bytes: usize,
    },
}

impl fmt::Display for EnqueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PacketTooLarge {
                packet_bytes,
                max_packet_bytes,
            } => write!(
                f,
                "packet size {} exceeds max packet size {}",
                packet_bytes, max_packet_bytes
            ),
            Self::QueueBufferExceeded {
                attempted_total,
                max_buffered_bytes,
            } => write!(
                f,
                "queued and writer-pending bytes {} exceeds limit {}",
                attempted_total, max_buffered_bytes
            ),
        }
    }
}

impl Error for EnqueueError {}

/// Atomically replenish the active queue and reserve one bounded batch for its writer.
#[derive(Message)]
#[rtype(result = "TurnResult")]
pub(crate) struct ServiceTurn {
    pub quantum: usize,
    pub writer_capacity: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TurnStatus {
    Empty,
    More,
    WriterBlocked,
}

#[derive(Debug)]
pub(crate) struct TurnResult {
    pub packets: Vec<Bytes>,
    pub status: TurnStatus,
}

/// Releases bytes that a dequeue reserved atomically after the corresponding
/// writer operation finishes.
#[derive(Message)]
#[rtype(result = "()")]
pub(crate) struct ReleasePendingBytes(pub usize);

#[derive(Message)]
#[rtype(result = "()")]
/// Gracefully close the queue: mark it as closing, drain all queued data, then stop.
/// Use this when you want in-flight and queued packets to be delivered before shutdown.
pub(crate) struct Close;

#[derive(Message)]
#[rtype(result = "()")]
/// Immediately stop the queue and drop any queued data without draining.
/// Use this instead of [`Close`] when a fast, best-effort shutdown is required.
/// Used by the scheduler on Unregister to forcibly tear down the queue.
pub(crate) struct StopNow;

#[derive(Default)]
pub(crate) struct QueueState {
    buf: VecDeque<Bytes>,
    buffered_bytes: usize,
    writer_pending_bytes: usize,
    deficit: usize,
    closing: bool,
}

impl QueueState {
    pub(crate) fn enqueue(&mut self, data: Bytes) -> Result<(), EnqueueError> {
        let packet_len = data.len();
        if packet_len > MAX_QUEUE_PACKET_BYTES {
            return Err(EnqueueError::PacketTooLarge {
                packet_bytes: packet_len,
                max_packet_bytes: MAX_QUEUE_PACKET_BYTES,
            });
        }

        let attempted_total = self
            .buffered_bytes
            .saturating_add(self.writer_pending_bytes)
            .saturating_add(packet_len);
        if attempted_total > MAX_QUEUE_BUFFERED_BYTES {
            return Err(EnqueueError::QueueBufferExceeded {
                attempted_total,
                max_buffered_bytes: MAX_QUEUE_BUFFERED_BYTES,
            });
        }

        self.buf.push_back(data);
        self.buffered_bytes = self.buffered_bytes.saturating_add(packet_len);
        Ok(())
    }

    fn service_turn(&mut self, quantum: usize, writer_capacity: usize) -> TurnResult {
        const MAX_PACKETS_PER_TURN: usize = 64;
        if !self.buf.is_empty() {
            self.deficit = self.deficit.saturating_add(quantum);
        }
        let mut packets = Vec::new();
        let mut bytes = 0;
        while let Some(front) = self.buf.front() {
            if packets.len() == MAX_PACKETS_PER_TURN
                || front.len() > self.deficit
                || front.len() > MAX_DEQUEUE_BYTES - bytes
                || front.len() > writer_capacity - bytes
            {
                break;
            }
            let packet = self.buf.pop_front().unwrap();
            self.deficit -= packet.len();
            bytes += packet.len();
            packets.push(packet);
        }
        self.buffered_bytes -= bytes;
        self.writer_pending_bytes += bytes;
        let status = match self.buf.front() {
            None => {
                self.deficit = 0;
                TurnStatus::Empty
            }
            Some(front) if front.len() > writer_capacity - bytes => TurnStatus::WriterBlocked,
            Some(_) => TurnStatus::More,
        };
        TurnResult { packets, status }
    }

    pub(crate) fn close(&mut self) -> bool {
        self.closing = true;
        self.buf.is_empty()
    }

    pub(crate) fn stop_now(&mut self) {
        self.buf.clear();
        self.buffered_bytes = 0;
    }

    pub(crate) fn release_pending_bytes(&mut self, completed_bytes: usize) {
        self.writer_pending_bytes = self
            .writer_pending_bytes
            .checked_sub(completed_bytes)
            .expect("completed writer bytes exceed the queue's pending reservation");
    }
}

pub(crate) struct QueueActor {
    state: QueueState,
    notifier: Option<QueueNotifier>,
    ready_notified: bool,
}

struct QueueNotifier {
    flow_id: FlowId,
    scheduler: Addr<Scheduler>,
}

impl Actor for QueueActor {
    type Context = Context<Self>;
}

impl QueueActor {
    pub(crate) fn new() -> Self {
        Self {
            state: QueueState::default(),
            notifier: None,
            ready_notified: false,
        }
    }

    fn bind_scheduler(&mut self, flow_id: FlowId, scheduler: Addr<Scheduler>) {
        self.notifier = Some(QueueNotifier { flow_id, scheduler });
        self.ready_notified = false;
        self.maybe_notify_ready();
    }

    fn maybe_notify_ready(&mut self) {
        if self.ready_notified {
            return;
        }

        let Some(notifier) = &self.notifier else {
            return;
        };

        if !self.state.buf.is_empty() {
            notifier.scheduler.do_send(FlowReady {
                flow_id: notifier.flow_id,
            });
            self.ready_notified = true;
        }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub(crate) struct BindScheduler {
    pub flow_id: FlowId,
    pub scheduler: Addr<Scheduler>,
}

// Enqueue
impl Handler<Enqueue> for QueueActor {
    type Result = MessageResult<Enqueue>;

    fn handle(&mut self, msg: Enqueue, _ctx: &mut Self::Context) -> Self::Result {
        let result = self.state.enqueue(msg.0);
        if result.is_ok() {
            self.maybe_notify_ready();
        }
        MessageResult(result)
    }
}

impl Handler<ServiceTurn> for QueueActor {
    type Result = MessageResult<ServiceTurn>;

    fn handle(&mut self, msg: ServiceTurn, ctx: &mut Self::Context) -> Self::Result {
        let result = self.state.service_turn(msg.quantum, msg.writer_capacity);
        // The scheduler owns remaining backlog until drained or blocked. A later
        // enqueue on an empty queue must send a fresh notification.
        self.ready_notified = result.status != TurnStatus::Empty;
        if self.state.closing && self.state.buf.is_empty() {
            ctx.stop();
        }
        MessageResult(result)
    }
}

impl Handler<ReleasePendingBytes> for QueueActor {
    type Result = ();

    fn handle(&mut self, msg: ReleasePendingBytes, _ctx: &mut Self::Context) {
        self.state.release_pending_bytes(msg.0);
    }
}

impl Handler<BindScheduler> for QueueActor {
    type Result = ();

    fn handle(&mut self, msg: BindScheduler, _ctx: &mut Self::Context) -> Self::Result {
        self.bind_scheduler(msg.flow_id, msg.scheduler);
    }
}
// Close
impl Handler<Close> for QueueActor {
    type Result = ();

    fn handle(&mut self, _msg: Close, ctx: &mut Self::Context) -> Self::Result {
        if self.state.close() {
            ctx.stop();
        }
    }
}

// StopNow
impl Handler<StopNow> for QueueActor {
    type Result = ();

    fn handle(&mut self, _msg: StopNow, ctx: &mut Self::Context) -> Self::Result {
        self.state.stop_now();
        ctx.stop();
    }
}

#[cfg(test)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct BufferedState {
    pub packet_count: usize,
    pub buffered_bytes: usize,
    pub writer_pending_bytes: usize,
}

#[cfg(test)]
#[derive(Message)]
#[rtype(result = "BufferedState")]
pub(crate) struct InspectBufferedState;

#[cfg(test)]
impl Handler<InspectBufferedState> for QueueActor {
    type Result = MessageResult<InspectBufferedState>;

    fn handle(&mut self, _msg: InspectBufferedState, _ctx: &mut Self::Context) -> Self::Result {
        MessageResult(BufferedState {
            packet_count: self.state.buf.len(),
            buffered_bytes: self.state.buffered_bytes,
            writer_pending_bytes: self.state.writer_pending_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_turn_batches_fifo_within_quantum_and_reserves_bytes() {
        let mut state = QueueState::default();
        for data in [b"aaa".as_slice(), b"bbbb", b"cc"] {
            state.enqueue(Bytes::copy_from_slice(data)).unwrap();
        }
        let first = state.service_turn(7, 100);
        assert_eq!(
            first.packets,
            vec![Bytes::from_static(b"aaa"), Bytes::from_static(b"bbbb")]
        );
        assert_eq!(first.status, TurnStatus::More);
        assert_eq!(state.writer_pending_bytes, 7);
        assert_eq!(state.buffered_bytes, 2);
        let second = state.service_turn(7, 100);
        assert_eq!(second.packets, vec![Bytes::from_static(b"cc")]);
        assert_eq!(second.status, TurnStatus::Empty);
        assert_eq!(state.deficit, 0);
    }

    #[test]
    fn large_packet_accumulates_deficit_on_active_turns_only() {
        let mut state = QueueState::default();
        for _ in 0..100 {
            state.service_turn(16 * 1024, 100_000);
        }
        assert_eq!(state.deficit, 0);
        let packet = Bytes::from(vec![0; MAX_QUEUE_PACKET_BYTES]);
        state.enqueue(packet.clone()).unwrap();
        for _ in 0..3 {
            let turn = state.service_turn(16 * 1024, 100_000);
            assert!(turn.packets.is_empty());
            assert_eq!(turn.status, TurnStatus::More);
        }
        let turn = state.service_turn(16 * 1024, 100_000);
        assert_eq!(turn.packets, vec![packet]);
        assert_eq!(turn.status, TurnStatus::Empty);
        assert_eq!(state.deficit, 0);
    }

    #[test]
    fn turn_distinguishes_writer_capacity_from_deficit_and_bounds_tiny_packets() {
        let mut state = QueueState::default();
        state.enqueue(Bytes::from_static(b"abc")).unwrap();
        let turn = state.service_turn(10, 2);
        assert_eq!(turn.status, TurnStatus::WriterBlocked);
        assert!(turn.packets.is_empty());
        assert_eq!(state.writer_pending_bytes, 0);
        assert_eq!(state.service_turn(10, 3).packets.len(), 1);
        for _ in 0..100 {
            state.enqueue(Bytes::new()).unwrap();
        }
        let turn = state.service_turn(10, 0);
        assert_eq!(turn.packets.len(), 64);
        assert_eq!(turn.status, TurnStatus::More);
        assert_eq!(state.service_turn(10, 0).packets.len(), 36);
    }

    #[test]
    fn turn_byte_limit_and_combined_reservation_limit_remain_enforced() {
        let mut state = QueueState::default();
        let packet = Bytes::from(vec![0; MAX_QUEUE_PACKET_BYTES]);
        for _ in 0..MAX_QUEUE_BUFFERED_BYTES / packet.len() {
            state.enqueue(packet.clone()).unwrap();
        }
        let turn = state.service_turn(usize::MAX, MAX_QUEUE_BUFFERED_BYTES);
        assert_eq!(turn.packets.len(), 1);
        assert_eq!(state.writer_pending_bytes, MAX_DEQUEUE_BYTES);
        assert_eq!(
            state.buffered_bytes + state.writer_pending_bytes,
            MAX_QUEUE_BUFFERED_BYTES
        );
        assert!(matches!(
            state.enqueue(Bytes::from_static(b"x")),
            Err(EnqueueError::QueueBufferExceeded { .. })
        ));
    }

    #[actix_rt::test]
    async fn close_drains_backlog_before_stopping() {
        let queue = QueueActor::new().start();
        queue
            .send(Enqueue(Bytes::from_static(b"payload")))
            .await
            .unwrap()
            .unwrap();
        queue.send(Close).await.unwrap();
        let turn = queue
            .send(ServiceTurn {
                quantum: 1024,
                writer_capacity: 1024,
            })
            .await
            .unwrap();
        assert_eq!(turn.packets, vec![Bytes::from_static(b"payload")]);
        assert_eq!(turn.status, TurnStatus::Empty);
        assert!(queue.send(InspectBufferedState).await.is_err());
    }

    #[actix_rt::test]
    async fn close_empty_and_stop_now_stop_the_queue() {
        let queue = QueueActor::new().start();
        queue.send(Close).await.unwrap();
        assert!(queue.send(InspectBufferedState).await.is_err());
        let queue = QueueActor::new().start();
        queue
            .send(Enqueue(Bytes::from_static(b"payload")))
            .await
            .unwrap()
            .unwrap();
        queue.send(StopNow).await.unwrap();
        assert!(queue.send(InspectBufferedState).await.is_err());
    }

    #[test]
    fn enqueue_rejects_oversized_packet() {
        let mut state = QueueState::default();
        let data = Bytes::from(vec![0u8; MAX_QUEUE_PACKET_BYTES + 1]);
        let result = state.enqueue(data);
        assert_eq!(
            result,
            Err(EnqueueError::PacketTooLarge {
                packet_bytes: MAX_QUEUE_PACKET_BYTES + 1,
                max_packet_bytes: MAX_QUEUE_PACKET_BYTES,
            })
        );
    }

    #[test]
    fn enqueue_rejects_when_buffer_limit_exceeded() {
        let mut state = QueueState::default();
        let chunk = Bytes::from(vec![0u8; MAX_QUEUE_PACKET_BYTES]);
        let full_chunks = MAX_QUEUE_BUFFERED_BYTES / MAX_QUEUE_PACKET_BYTES;
        for _ in 0..full_chunks {
            assert!(state.enqueue(chunk.clone()).is_ok());
        }

        let result = state.enqueue(Bytes::from_static(b"x"));
        assert_eq!(
            result,
            Err(EnqueueError::QueueBufferExceeded {
                attempted_total: MAX_QUEUE_BUFFERED_BYTES + 1,
                max_buffered_bytes: MAX_QUEUE_BUFFERED_BYTES,
            })
        );
    }

    #[test]
    fn dequeue_reserves_bytes_before_another_enqueue_can_refill_capacity() {
        let mut state = QueueState::default();
        let chunk = Bytes::from(vec![0u8; MAX_QUEUE_PACKET_BYTES]);
        for _ in 0..(MAX_QUEUE_BUFFERED_BYTES / MAX_QUEUE_PACKET_BYTES) {
            state.enqueue(chunk.clone()).unwrap();
        }
        state.service_turn(usize::MAX, MAX_QUEUE_PACKET_BYTES);

        assert_eq!(
            state.buffered_bytes + state.writer_pending_bytes,
            MAX_QUEUE_BUFFERED_BYTES
        );
        assert!(matches!(
            state.enqueue(Bytes::from_static(b"x")),
            Err(EnqueueError::QueueBufferExceeded { .. })
        ));
    }

    #[actix_rt::test]
    async fn completion_does_not_erase_a_newer_dequeue_reservation() {
        let queue = QueueActor::new().start();
        let chunk = Bytes::from(vec![0_u8; MAX_QUEUE_PACKET_BYTES]);
        for _ in 0..(MAX_QUEUE_BUFFERED_BYTES / MAX_QUEUE_PACKET_BYTES) {
            queue.send(Enqueue(chunk.clone())).await.unwrap().unwrap();
        }
        for _ in 0..2 {
            let turn = queue
                .send(ServiceTurn {
                    quantum: MAX_QUEUE_PACKET_BYTES,
                    writer_capacity: MAX_QUEUE_PACKET_BYTES,
                })
                .await
                .unwrap();
            assert_eq!(turn.packets.len(), 1);
        }
        queue
            .send(ReleasePendingBytes(MAX_QUEUE_PACKET_BYTES))
            .await
            .unwrap();

        queue.send(Enqueue(chunk.clone())).await.unwrap().unwrap();
        assert!(matches!(
            queue.send(Enqueue(chunk)).await.unwrap(),
            Err(EnqueueError::QueueBufferExceeded { .. })
        ));
        let state = queue.send(InspectBufferedState).await.unwrap();
        assert_eq!(state.writer_pending_bytes, MAX_QUEUE_PACKET_BYTES);
        assert_eq!(
            state.buffered_bytes + state.writer_pending_bytes,
            MAX_QUEUE_BUFFERED_BYTES
        );
    }
}
