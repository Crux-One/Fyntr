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
        queue::{Enqueue, QueueActor},
        scheduler::{RecordDownstreamBytes, Scheduler, Unregister},
    },
    util::{format_bytes, format_rate},
};

const BUFFER_SIZE: usize = 8 * 1024; // 8 KB
const UNREGISTER_RETRY_LIMIT: usize = 3;
const UNREGISTER_RETRY_DELAY_MS: u64 = 50;
const UNREGISTER_RETRY_MAX_DELAY_SECS: u64 = 1;

pub(crate) struct ClientToBackendActor {
    flow_id: FlowId,
    client: Option<OwnedReadHalf>,
    queue_tx: Addr<QueueActor>,
    scheduler: Addr<Scheduler>,
}

impl ClientToBackendActor {
    pub(crate) fn new(
        flow_id: FlowId,
        client: OwnedReadHalf,
        queue_tx: Addr<QueueActor>,
        scheduler: Addr<Scheduler>,
    ) -> Self {
        Self {
            flow_id,
            client: Some(client),
            queue_tx,
            scheduler,
        }
    }
}

impl Actor for ClientToBackendActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let flow_id = self.flow_id;
        let queue_tx = self.queue_tx.clone();
        let scheduler = self.scheduler.clone();
        let mut client = self
            .client
            .take()
            .expect("ClientToBackendActor started without client stream");

        ctx.spawn(
            async move {
                let mut buf = vec![0u8; BUFFER_SIZE];
                let mut total_read = 0u64;
                let start_time = Instant::now();

                loop {
                    match client.read(&mut buf).await {
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

                            break;
                        }
                        Ok(n) => {
                            total_read += n as u64;
                            let chunk = Bytes::copy_from_slice(&buf[..n]);
                            if let Err(e) = queue_tx.send(Enqueue(chunk)).await {
                                warn!("flow{}: failed to enqueue chunk: {}", flow_id.0, e);
                                break;
                            }
                        }
                        Err(e) => {
                            warn!("flow{}: read error: {}", flow_id.0, e);
                            break;
                        }
                    }
                }

                unregister_flow(scheduler, flow_id).await;
            }
            .into_actor(self)
            .map(|_, _act, ctx| ctx.stop()),
        );
    }
}

async fn unregister_flow(scheduler: Addr<Scheduler>, flow_id: FlowId) {
    // Unregister this flow from the scheduler to signal completion and trigger cleanup.
    let mut delay = Duration::from_millis(UNREGISTER_RETRY_DELAY_MS);
    let max_delay = Duration::from_secs(UNREGISTER_RETRY_MAX_DELAY_SECS);
    for attempt in 1..=UNREGISTER_RETRY_LIMIT {
        match scheduler.send(Unregister { flow_id }).await {
            Ok(_) => {
                debug!(
                    "flow{}: successfully unregistered from scheduler",
                    flow_id.0
                );
                return;
            }
            Err(e) if attempt == UNREGISTER_RETRY_LIMIT => {
                warn!(
                    "flow{}: failed to unregister from scheduler after {} attempts: {}",
                    flow_id.0, attempt, e
                );
            }
            Err(e) => {
                warn!(
                    "flow{}: unregister attempt {}/{} failed: {}; retrying",
                    flow_id.0, attempt, UNREGISTER_RETRY_LIMIT, e
                );
                sleep(delay).await;
                delay = delay.saturating_mul(2).min(max_delay);
            }
        }
    }
}

pub(crate) struct BackendToClientActor {
    flow_id: FlowId,
    backend: Option<OwnedReadHalf>,
    client: Option<OwnedWriteHalf>,
    scheduler: Addr<Scheduler>,
}

impl BackendToClientActor {
    pub(crate) fn new(
        flow_id: FlowId,
        backend: OwnedReadHalf,
        client: OwnedWriteHalf,
        scheduler: Addr<Scheduler>,
    ) -> Self {
        Self {
            flow_id,
            backend: Some(backend),
            client: Some(client),
            scheduler,
        }
    }
}

impl Actor for BackendToClientActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let flow_id = self.flow_id;
        let mut backend = self
            .backend
            .take()
            .expect("BackendToClientActor started without backend stream");
        let mut client = self
            .client
            .take()
            .expect("BackendToClientActor started without client stream");
        let scheduler = self.scheduler.clone();

        ctx.spawn(
            async move {
                let mut buf = vec![0u8; BUFFER_SIZE];
                let mut total_read = 0u64;
                let start_time = Instant::now();
                loop {
                    match backend.read(&mut buf).await {
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
                            break;
                        }
                        Ok(n) => {
                            total_read += n as u64;
                            if let Err(e) = client.write_all(&buf[..n]).await {
                                warn!("flow{}: client write error: {}", flow_id.0, e);
                                break;
                            }
                            scheduler.do_send(RecordDownstreamBytes { bytes: n });
                        }
                        Err(e) => {
                            warn!("flow{}: backend read error: {}", flow_id.0, e);
                            break;
                        }
                    }
                }
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
        queue::{Dequeue, QueueActor},
        scheduler::{Register, Scheduler},
    };
    use crate::test_utils::make_backend_write;
    use tokio::{
        net::{TcpListener, TcpStream},
        time::{Duration, sleep},
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
            })
            .await
            .unwrap()
            .unwrap();

        let client_read = build_server_read_half().await;
        ClientToBackendActor::new(FlowId(5), client_read, queue.clone(), scheduler).start();

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
}
