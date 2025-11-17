use std::{net::SocketAddr, ops::ControlFlow, sync::Arc};

use actix::prelude::*;
use anyhow::{Result, anyhow};

use log::{error, info, warn};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::Mutex,
};

use crate::{
    actors::{
        queue::{Close, QueueActor},
        scheduler::{Register, RegisterError, Scheduler, Unregister},
    },
    flow::{
        FlowId,
        connection::{BackendToClientActor, ClientToBackendActor},
    },
    http::request::{read_request_line, send_connect_response, skip_headers},
};

struct ConnectSession {
    flow_id: FlowId,
    client_addr: SocketAddr,
    scheduler: Addr<Scheduler>,
    client_reader: BufReader<OwnedReadHalf>,
    client_write: OwnedWriteHalf,
}

struct ValidatedSession {
    session: ConnectSession,
    target_host: String,
    target_port: u16,
}

struct BackendConnectedSession {
    session: ConnectSession,
    queue_tx: Addr<QueueActor>,
    backend_read: OwnedReadHalf,
    backend_write: Arc<Mutex<OwnedWriteHalf>>,
}

struct RegisteredSession {
    session: ConnectSession,
    queue_tx: Addr<QueueActor>,
    backend_read: OwnedReadHalf,
}

impl ConnectSession {
    fn new(
        client_stream: TcpStream,
        flow_id: FlowId,
        client_addr: SocketAddr,
        scheduler: Addr<Scheduler>,
    ) -> Self {
        let (client_read, client_write) = client_stream.into_split();
        Self {
            flow_id,
            client_addr,
            scheduler,
            client_reader: BufReader::new(client_read),
            client_write,
        }
    }

    async fn validate_request(self) -> Result<ControlFlow<(), ValidatedSession>> {
        let mut session = self;
        let request_line = read_request_line(&mut session.client_reader).await?;

        if !request_line.is_http_1x() {
            warn!(
                "flow{}: unsupported HTTP version {} from {}",
                session.flow_id.0, request_line.version, session.client_addr
            );
            let response = b"HTTP/1.1 505 HTTP Version Not Supported\r\n\r\n";
            session.client_write.write_all(response).await?;
            return Ok(ControlFlow::Break(()));
        }

        if !request_line.is_connect_method() {
            warn!(
                "flow{}: unsupported method {} from {}",
                session.flow_id.0, request_line.method, session.client_addr
            );
            let response = b"HTTP/1.1 405 Method Not Allowed\r\n\r\n";
            session.client_write.write_all(response).await?;
            return Ok(ControlFlow::Break(()));
        }

        let (target_host, target_port) = request_line.parse_connect_target()?;
        info!(
            "flow{}: CONNECT {}:{}",
            session.flow_id.0, target_host, target_port
        );

        skip_headers(&mut session.client_reader).await?;

        Ok(ControlFlow::Continue(ValidatedSession {
            session,
            target_host,
            target_port,
        }))
    }
}

impl ValidatedSession {
    async fn establish_backend_connection(
        self,
    ) -> Result<ControlFlow<(), BackendConnectedSession>> {
        let ValidatedSession {
            mut session,
            target_host,
            target_port,
        } = self;

        let backend_addr = format!("{}:{}", target_host, target_port);
        let backend_stream = match TcpStream::connect(&backend_addr).await {
            Ok(stream) => stream,
            Err(e) => {
                error!(
                    "flow{}: failed to connect to {}: {}",
                    session.flow_id.0, backend_addr, e
                );
                let response = b"HTTP/1.1 502 Bad Gateway\r\n\r\n";
                session.client_write.write_all(response).await?;
                return Ok(ControlFlow::Break(()));
            }
        };

        backend_stream
            .set_nodelay(true)
            .map_err(|e| anyhow!("Failed to set TCP_NODELAY: {}", e))?;

        info!("flow{}: connected to {}", session.flow_id.0, backend_addr);

        let (backend_read, backend_write) = backend_stream.into_split();
        let queue_tx = QueueActor::new().start();

        Ok(ControlFlow::Continue(BackendConnectedSession {
            session,
            queue_tx,
            backend_read,
            backend_write: Arc::new(Mutex::new(backend_write)),
        }))
    }
}

impl BackendConnectedSession {
    async fn register_flow(self) -> Result<ControlFlow<(), RegisteredSession>> {
        let BackendConnectedSession {
            mut session,
            queue_tx,
            backend_read,
            backend_write,
        } = self;

        match session
            .scheduler
            .send(Register {
                flow_id: session.flow_id,
                queue_addr: queue_tx.clone(),
                backend_write: backend_write.clone(),
            })
            .await
        {
            Ok(Ok(())) => Ok(ControlFlow::Continue(RegisteredSession {
                session,
                queue_tx,
                backend_read,
            })),
            Ok(Err(RegisterError::MaxConnectionsReached { max })) => {
                queue_tx.do_send(Close);
                warn!(
                    "flow{}: registration rejected - max connections ({}) reached",
                    session.flow_id.0, max
                );
                session
                    .client_write
                    .write_all(b"HTTP/1.1 503 Service Unavailable\r\n\r\n")
                    .await?;
                Ok(ControlFlow::Break(()))
            }
            Err(e) => {
                queue_tx.do_send(Close);
                error!(
                    "flow{}: failed to register with scheduler: {}",
                    session.flow_id.0, e
                );
                Err(anyhow!("Scheduler registration failed"))
            }
        }
    }
}

impl RegisteredSession {
    async fn finalize(self) -> Result<()> {
        let RegisteredSession {
            session,
            queue_tx,
            backend_read,
        } = self;

        let ConnectSession {
            flow_id,
            scheduler,
            client_reader,
            mut client_write,
            ..
        } = session;

        if let Err(e) = send_connect_response(&mut client_write).await {
            queue_tx.do_send(Close);
            if let Err(err) = scheduler.send(Unregister { flow_id }).await {
                warn!(
                    "flow{}: failed to unregister after handshake error: {}",
                    flow_id.0, err
                );
            }
            return Err(e);
        }

        let client_read = client_reader.into_inner();

        ClientToBackendActor::new(flow_id, client_read, queue_tx.clone(), scheduler.clone())
            .start();
        BackendToClientActor::new(flow_id, backend_read, client_write).start();

        Ok(())
    }
}

/// Handle the CONNECT method and operate as an HTTPS proxy
pub(crate) async fn handle_connect_proxy(
    client_stream: TcpStream,
    client_addr: SocketAddr,
    flow_id: FlowId,
    scheduler: Addr<Scheduler>,
) -> Result<(), anyhow::Error> {
    let session = ConnectSession::new(client_stream, flow_id, client_addr, scheduler);

    let validated = match session.validate_request().await? {
        ControlFlow::Continue(validated) => validated,
        ControlFlow::Break(()) => return Ok(()),
    };

    let backend_connected = match validated.establish_backend_connection().await? {
        ControlFlow::Continue(connected) => connected,
        ControlFlow::Break(()) => return Ok(()),
    };

    let registered = match backend_connected.register_flow().await? {
        ControlFlow::Continue(registered) => registered,
        ControlFlow::Break(()) => return Ok(()),
    };

    registered.finalize().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::make_backend_write;
    use std::{future::Future, time::Duration};

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    async fn read_response(mut stream: TcpStream) -> Vec<u8> {
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        buf
    }

    async fn drive_proxy(
        listener: TcpListener,
        scheduler: Addr<Scheduler>,
        client_task: impl Future<Output = Vec<u8>>,
    ) -> Vec<u8> {
        let server = async move {
            let (stream, peer) = listener.accept().await.unwrap();
            handle_connect_proxy(stream, peer, FlowId(1), scheduler)
                .await
                .unwrap();
        };

        let ((), response) = tokio::join!(server, client_task);
        response
    }

    #[actix_rt::test]
    async fn returns_405_for_non_connect_method() {
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600)).start();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let response = drive_proxy(listener, scheduler, async move {
            let mut client = TcpStream::connect(addr).await.unwrap();
            client
                .write_all(b"GET / HTTP/1.1\r\nHost: example\r\n\r\n")
                .await
                .unwrap();
            read_response(client).await
        })
        .await;

        assert_eq!(response, b"HTTP/1.1 405 Method Not Allowed\r\n\r\n");
    }

    #[actix_rt::test]
    async fn returns_505_for_unsupported_http_version() {
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600)).start();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let response = drive_proxy(listener, scheduler, async move {
            let mut client = TcpStream::connect(addr).await.unwrap();
            client
                .write_all(b"CONNECT example.com:443 HTTP/2\r\nHost: example\r\n\r\n")
                .await
                .unwrap();
            read_response(client).await
        })
        .await;

        assert_eq!(response, b"HTTP/1.1 505 HTTP Version Not Supported\r\n\r\n");
    }

    #[actix_rt::test]
    async fn returns_502_when_backend_connection_fails() {
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600)).start();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // Choose an ephemeral port to ensure the outbound CONNECT attempt fails quickly.
        // The listener is dropped immediately after capturing the port, making the port unreachable.
        let unreachable_port = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            drop(listener);
            port
        };

        let response = drive_proxy(listener, scheduler, async move {
            let mut client = TcpStream::connect(addr).await.unwrap();
            let request = format!(
                "CONNECT 127.0.0.1:{} HTTP/1.1\r\nHost: example\r\n\r\n",
                unreachable_port
            );
            client.write_all(request.as_bytes()).await.unwrap();
            read_response(client).await
        })
        .await;

        assert_eq!(response, b"HTTP/1.1 502 Bad Gateway\r\n\r\n");
    }

    #[actix_rt::test]
    async fn returns_503_when_scheduler_is_at_capacity() {
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600))
            .with_max_connections(1)
            .start();

        let queue = QueueActor::new().start();
        let backend_write = make_backend_write().await;
        scheduler
            .send(Register {
                flow_id: FlowId(42),
                queue_addr: queue,
                backend_write,
            })
            .await
            .unwrap()
            .unwrap();

        let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend_addr: SocketAddr = backend_listener.local_addr().unwrap();
        let backend_accept = tokio::spawn(async move {
            let (_stream, _) = backend_listener.accept().await.unwrap();
        });

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let response = drive_proxy(listener, scheduler, async move {
            let mut client = TcpStream::connect(addr).await.unwrap();
            let request = format!("CONNECT {} HTTP/1.1\r\nHost: example\r\n\r\n", backend_addr);
            client.write_all(request.as_bytes()).await.unwrap();
            read_response(client).await
        })
        .await;

        assert_eq!(response, b"HTTP/1.1 503 Service Unavailable\r\n\r\n");
        backend_accept.await.unwrap();
    }
}
