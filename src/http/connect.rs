use std::{net::SocketAddr, sync::Arc};

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
    queue_tx: Option<Addr<QueueActor>>,
    backend_read: Option<OwnedReadHalf>,
    backend_write: Option<Arc<Mutex<OwnedWriteHalf>>>,
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
            queue_tx: None,
            backend_read: None,
            backend_write: None,
        }
    }

    async fn validate_request(&mut self) -> Result<Option<(String, u16)>> {
        let request_line = read_request_line(&mut self.client_reader).await?;

        if !request_line.is_http_1x() {
            warn!(
                "flow{}: unsupported HTTP version {} from {}",
                self.flow_id.0, request_line.version, self.client_addr
            );
            let response = b"HTTP/1.1 505 HTTP Version Not Supported\r\n\r\n";
            self.client_write.write_all(response).await?;
            return Ok(None);
        }

        if !request_line.is_connect_method() {
            warn!(
                "flow{}: unsupported method {} from {}",
                self.flow_id.0, request_line.method, self.client_addr
            );
            let response = b"HTTP/1.1 405 Method Not Allowed\r\n\r\n";
            self.client_write.write_all(response).await?;
            return Ok(None);
        }

        let (target_host, target_port) = request_line.parse_connect_target()?;
        info!(
            "flow{}: CONNECT {}:{}",
            self.flow_id.0, target_host, target_port
        );

        skip_headers(&mut self.client_reader).await?;

        Ok(Some((target_host, target_port)))
    }

    async fn establish_backend_connection(
        &mut self,
        target_host: &str,
        target_port: u16,
    ) -> Result<Option<TcpStream>> {
        let backend_addr = format!("{}:{}", target_host, target_port);
        let backend_stream = match TcpStream::connect(&backend_addr).await {
            Ok(stream) => stream,
            Err(e) => {
                error!(
                    "flow{}: failed to connect to {}: {}",
                    self.flow_id.0, backend_addr, e
                );
                let response = b"HTTP/1.1 502 Bad Gateway\r\n\r\n";
                self.client_write.write_all(response).await?;
                return Ok(None);
            }
        };

        backend_stream
            .set_nodelay(true)
            .map_err(|e| anyhow!("Failed to set TCP_NODELAY: {}", e))?;

        info!("flow{}: connected to {}", self.flow_id.0, backend_addr);

        Ok(Some(backend_stream))
    }

    fn initialize_backend(&mut self, backend_stream: TcpStream) {
        let (backend_read, backend_write) = backend_stream.into_split();
        self.backend_read = Some(backend_read);
        self.backend_write = Some(Arc::new(Mutex::new(backend_write)));
        self.queue_tx = Some(QueueActor::new().start());
    }

    async fn register_flow(&mut self) -> Result<bool> {
        let queue_tx = self
            .queue_tx
            .as_ref()
            .expect("queue must be initialized before registration");
        let backend_write = self
            .backend_write
            .as_ref()
            .expect("backend must be initialized before registration");

        match self
            .scheduler
            .send(Register {
                flow_id: self.flow_id,
                queue_addr: queue_tx.clone(),
                backend_write: backend_write.clone(),
            })
            .await
        {
            Ok(Ok(())) => Ok(true),
            Ok(Err(RegisterError::MaxConnectionsReached { max })) => {
                queue_tx.do_send(Close);
                warn!(
                    "flow{}: registration rejected - max connections ({}) reached",
                    self.flow_id.0, max
                );
                let response = b"HTTP/1.1 503 Service Unavailable\r\n\r\n";
                self.client_write.write_all(response).await?;
                Ok(false)
            }
            Err(e) => {
                queue_tx.do_send(Close);
                error!(
                    "flow{}: failed to register with scheduler: {}",
                    self.flow_id.0, e
                );
                Err(anyhow!("Scheduler registration failed"))
            }
        }
    }

    async fn finalize(mut self) -> Result<()> {
        let queue_tx = self
            .queue_tx
            .take()
            .expect("queue must be initialized before finalization");
        let scheduler = self.scheduler.clone();

        if let Err(e) = send_connect_response(&mut self.client_write).await {
            queue_tx.do_send(Close);
            if let Err(err) = scheduler
                .send(Unregister {
                    flow_id: self.flow_id,
                })
                .await
            {
                warn!(
                    "flow{}: failed to unregister after handshake error: {}",
                    self.flow_id.0, err
                );
            }
            return Err(e);
        }

        let client_read = self.client_reader.into_inner();
        let backend_read = self
            .backend_read
            .take()
            .expect("backend must be initialized before finalization");
        let client_write = self.client_write;

        ClientToBackendActor::new(
            self.flow_id,
            client_read,
            queue_tx.clone(),
            scheduler.clone(),
        )
        .start();
        BackendToClientActor::new(self.flow_id, backend_read, client_write).start();

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
    let mut session = ConnectSession::new(client_stream, flow_id, client_addr, scheduler);

    let Some((target_host, target_port)) = session.validate_request().await? else {
        return Ok(());
    };

    let Some(backend_stream) = session
        .establish_backend_connection(&target_host, target_port)
        .await?
    else {
        return Ok(());
    };

    session.initialize_backend(backend_stream);

    if !session.register_flow().await? {
        return Ok(());
    }

    session.finalize().await
}
