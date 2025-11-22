use std::collections::VecDeque;

use actix::prelude::*;
use bytes::Bytes;

use crate::{
    actors::scheduler::{FlowReady, Scheduler},
    flow::FlowId,
};

#[derive(Message)]
#[rtype(result = "()")]
pub(crate) struct Enqueue(pub Bytes);

#[derive(Message)]
#[rtype(result = "()")]
pub(crate) struct AddQuantum(pub usize);

#[derive(Message)]
#[rtype(result = "Option<DequeueResult>")]
pub(crate) struct Dequeue {
    pub max_bytes: usize,
}

#[derive(Message)]
#[rtype(result = "()")]
pub(crate) struct Close;

#[derive(Debug)]
pub(crate) struct DequeueResult {
    pub packet: Bytes,
    pub remaining: usize,
    pub ready_for_more: bool,
}

#[derive(Default)]
pub(crate) struct QueueState {
    buf: VecDeque<Bytes>,
    deficit: usize,
    closing: bool,
}

impl QueueState {
    pub(crate) fn enqueue(&mut self, data: Bytes) {
        self.buf.push_back(data);
    }

    pub(crate) fn add_quantum(&mut self, quantum: usize) {
        self.deficit = self.deficit.saturating_add(quantum);
    }

    pub(crate) fn dequeue(&mut self, max_bytes: usize) -> (Option<DequeueResult>, bool) {
        let dequeue_result = if let Some(front) = self.buf.front() {
            if front.len() <= self.deficit && front.len() <= max_bytes {
                let pkt = self.buf.pop_front().unwrap();
                self.deficit -= pkt.len();
                let ready_for_more = self
                    .buf
                    .front()
                    .is_some_and(|next| next.len() <= self.deficit && next.len() <= max_bytes);
                Some(DequeueResult {
                    packet: pkt,
                    remaining: self.buf.len(),
                    ready_for_more,
                })
            } else {
                None
            }
        } else {
            None
        };

        let should_stop = self.closing && self.buf.is_empty();
        (dequeue_result, should_stop)
    }

    fn has_ready_packet(&self, max_bytes: usize) -> bool {
        self.buf
            .front()
            .is_some_and(|front| front.len() <= self.deficit && front.len() <= max_bytes)
    }

    pub(crate) fn close(&mut self) -> bool {
        self.closing = true;
        self.buf.is_empty()
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

        // Scheduler currently dequeues with usize::MAX as the byte limit, so
        // we mirror that constraint here when deciding whether to notify.
        if self.state.has_ready_packet(usize::MAX) {
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
    type Result = ();

    fn handle(&mut self, msg: Enqueue, _ctx: &mut Self::Context) -> Self::Result {
        self.state.enqueue(msg.0);
        self.maybe_notify_ready();
    }
}

// AddQuantum
impl Handler<AddQuantum> for QueueActor {
    type Result = ();

    fn handle(&mut self, msg: AddQuantum, _ctx: &mut Self::Context) -> Self::Result {
        self.state.add_quantum(msg.0);
        self.maybe_notify_ready();
    }
}

// Dequeue
impl Handler<Dequeue> for QueueActor {
    type Result = MessageResult<Dequeue>;

    fn handle(&mut self, msg: Dequeue, ctx: &mut Self::Context) -> Self::Result {
        let (dequeue_result, should_stop) = self.state.dequeue(msg.max_bytes);

        self.ready_notified = dequeue_result
            .as_ref()
            .is_some_and(|result| result.ready_for_more);

        if should_stop {
            ctx.stop();
        }

        MessageResult(dequeue_result)
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

#[cfg(test)]
mod tests {
    use super::*;
    use actix::MailboxError;

    #[actix_rt::test]
    async fn test_enqueue_and_dequeue() {
        let addr = QueueActor::new().start();

        // Add quantum to allow dequeue (must be >= data.len()).
        // This simulates a scenario where the quantum is not enough at first, so the dequeue will fail until enough quantum is added.
        addr.do_send(AddQuantum(4));

        // Enqueue a packet into the queue actor.
        let data = Bytes::from_static(b"hello");
        addr.do_send(Enqueue(data.clone()));

        // Attempt to dequeue. Since the quantum is insufficient, this should return None (dequeue not allowed yet).
        let res = addr.send(Dequeue { max_bytes: 1024 }).await.unwrap();
        assert!(res.is_none());

        // Add more quantum to reach the required amount for the packet size.
        addr.do_send(AddQuantum(1));
        let res = addr.send(Dequeue { max_bytes: 1024 }).await.unwrap();
        assert!(res.is_some());

        let result = res.unwrap();
        assert_eq!(result.packet, data);
        assert_eq!(result.remaining, 0);
    }

    #[actix_rt::test]
    async fn test_close_behavior() {
        let addr = QueueActor::new().start();

        addr.do_send(Close);

        // Confirm that the actor stops immediately after receiving Close.
        // After stopping, sending messages to the actor should fail (either error or None).
        let res = addr.send(Dequeue { max_bytes: 1024 }).await;
        assert_actor_stopped_or_empty(res);
    }

    #[actix_rt::test]
    async fn test_dequeue_respects_max_bytes() {
        let addr = QueueActor::new().start();

        let payload = Bytes::from_static(b"0123456789");
        addr.do_send(Enqueue(payload.clone()));

        // Provide sufficient quantum for the packet, but restrict max_bytes below packet length.
        addr.do_send(AddQuantum(usize::MAX));
        let res = addr
            .send(Dequeue {
                max_bytes: payload.len() - 1,
            })
            .await
            .unwrap();
        assert!(
            res.is_none(),
            "dequeue should be blocked by max_bytes constraint"
        );

        // Allow dequeue by lifting the max_bytes restriction.
        let res = addr
            .send(Dequeue {
                max_bytes: payload.len(),
            })
            .await
            .unwrap();
        let result = res.expect("packet should dequeue once max_bytes allows it");
        assert_eq!(result.packet, payload);
        assert_eq!(result.remaining, 0);
    }

    #[actix_rt::test]
    async fn test_fifo_and_remaining_count() {
        let addr = QueueActor::new().start();

        let first = Bytes::from_static(b"first");
        let second = Bytes::from_static(b"second");
        addr.do_send(Enqueue(first.clone()));
        addr.do_send(Enqueue(second.clone()));

        addr.do_send(AddQuantum(usize::MAX));

        let res_first = addr
            .send(Dequeue {
                max_bytes: usize::MAX,
            })
            .await
            .unwrap()
            .expect("first dequeue should succeed");
        assert_eq!(res_first.packet, first);
        assert_eq!(res_first.remaining, 1);

        let res_second = addr
            .send(Dequeue {
                max_bytes: usize::MAX,
            })
            .await
            .unwrap()
            .expect("second dequeue should succeed");
        assert_eq!(res_second.packet, second);
        assert_eq!(res_second.remaining, 0);
    }

    #[actix_rt::test]
    async fn test_close_after_drain() {
        let addr = QueueActor::new().start();

        let payload = Bytes::from_static(b"payload");
        addr.do_send(Enqueue(payload.clone()));

        // Initiate closing while there is still data buffered.
        addr.do_send(Close);
        addr.do_send(AddQuantum(usize::MAX));

        // First dequeue should return the buffered packet.
        let result = addr
            .send(Dequeue {
                max_bytes: usize::MAX,
            })
            .await
            .unwrap()
            .expect("queue should dequeue buffered packet before stopping");
        assert_eq!(result.packet, payload);
        assert_eq!(result.remaining, 0);

        // Subsequent messages should fail because the actor stops after draining.
        let res = addr
            .send(Dequeue {
                max_bytes: usize::MAX,
            })
            .await;
        assert_actor_stopped_or_empty(res);
    }

    // Helper to assert that the actor is stopped or the queue is empty.
    fn assert_actor_stopped_or_empty<T>(res: Result<Option<T>, MailboxError>) {
        match res {
            Ok(None) | Err(_) => (),
            Ok(Some(_)) => panic!("expected actor to be stopped or queue empty"),
        }
    }
}
