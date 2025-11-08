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

/// Handle the CONNECT method and operate as an HTTPS proxy
pub(crate) async fn handle_connect_proxy(
    client_stream: TcpStream,
    client_addr: SocketAddr,
    flow_id: FlowId,
    scheduler: Addr<Scheduler>,
) -> Result<(), anyhow::Error> {
    // Split the client stream (wrap the read half in a BufReader)
    let (client_read, mut client_write) = client_stream.into_split();
    let mut client_reader = BufReader::new(client_read);

    let Some((target_host, target_port)) =
        validate_connect_request(&mut client_reader, &mut client_write, flow_id, client_addr)
            .await?
    else {
        return Ok(());
    };

    let Some(backend_stream) =
        establish_backend_connection(&target_host, target_port, flow_id, &mut client_write).await?
    else {
        return Ok(());
    };

    // Split the backend stream
    let (backend_read, backend_write) = backend_stream.into_split();
    let backend_write = Arc::new(Mutex::new(backend_write));

    // Start the QueueActor
    let queue_tx = QueueActor::new().start();

    if !register_flow(
        &scheduler,
        flow_id,
        &queue_tx,
        backend_write.clone(),
        &mut client_write,
    )
    .await?
    {
        return Ok(());
    }

    finalize_proxy(
        flow_id,
        client_reader,
        client_write,
        backend_read,
        queue_tx,
        scheduler,
    )
    .await
}

async fn validate_connect_request(
    client_reader: &mut BufReader<OwnedReadHalf>,
    client_write: &mut OwnedWriteHalf,
    flow_id: FlowId,
    client_addr: SocketAddr,
) -> Result<Option<(String, u16)>> {
    let request_line = read_request_line(client_reader).await?;

    if !request_line.is_http_1x() {
        warn!(
            "flow{}: unsupported HTTP version {} from {}",
            flow_id.0, request_line.version, client_addr
        );
        let response = b"HTTP/1.1 505 HTTP Version Not Supported\r\n\r\n";
        client_write.write_all(response).await?;
        return Ok(None);
    }

    if !request_line.is_connect_method() {
        warn!(
            "flow{}: unsupported method {} from {}",
            flow_id.0, request_line.method, client_addr
        );
        let response = b"HTTP/1.1 405 Method Not Allowed\r\n\r\n";
        client_write.write_all(response).await?;
        return Ok(None);
    }

    let (target_host, target_port) = request_line.parse_connect_target()?;
    info!("flow{}: CONNECT {}:{}", flow_id.0, target_host, target_port);

    skip_headers(client_reader).await?;

    Ok(Some((target_host, target_port)))
}

async fn establish_backend_connection(
    target_host: &str,
    target_port: u16,
    flow_id: FlowId,
    client_write: &mut OwnedWriteHalf,
) -> Result<Option<TcpStream>> {
    let backend_addr = format!("{}:{}", target_host, target_port);
    let backend_stream = match TcpStream::connect(&backend_addr).await {
        Ok(stream) => stream,
        Err(e) => {
            error!(
                "flow{}: failed to connect to {}: {}",
                flow_id.0, backend_addr, e
            );
            let response = b"HTTP/1.1 502 Bad Gateway\r\n\r\n";
            client_write.write_all(response).await?;
            return Ok(None);
        }
    };

    backend_stream
        .set_nodelay(true)
        .map_err(|e| anyhow!("Failed to set TCP_NODELAY: {}", e))?;

    info!("flow{}: connected to {}", flow_id.0, backend_addr);

    Ok(Some(backend_stream))
}

async fn register_flow(
    scheduler: &Addr<Scheduler>,
    flow_id: FlowId,
    queue_tx: &Addr<QueueActor>,
    backend_write: Arc<Mutex<OwnedWriteHalf>>,
    client_write: &mut OwnedWriteHalf,
) -> Result<bool> {
    match scheduler
        .send(Register {
            flow_id,
            queue_addr: queue_tx.clone(),
            backend_write,
        })
        .await
    {
        Ok(Ok(())) => Ok(true),
        Ok(Err(RegisterError::MaxConnectionsReached { max })) => {
            queue_tx.do_send(Close);
            warn!(
                "flow{}: registration rejected - max connections ({}) reached",
                flow_id.0, max
            );
            let response = b"HTTP/1.1 503 Service Unavailable\r\n\r\n";
            client_write.write_all(response).await?;
            Ok(false)
        }
        Err(e) => {
            queue_tx.do_send(Close);
            error!(
                "flow{}: failed to register with scheduler: {}",
                flow_id.0, e
            );
            Err(anyhow!("Scheduler registration failed"))
        }
    }
}

async fn finalize_proxy(
    flow_id: FlowId,
    client_reader: BufReader<OwnedReadHalf>,
    mut client_write: OwnedWriteHalf,
    backend_read: OwnedReadHalf,
    queue_tx: Addr<QueueActor>,
    scheduler: Addr<Scheduler>,
) -> Result<()> {
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

    ClientToBackendActor::new(flow_id, client_read, queue_tx.clone(), scheduler.clone()).start();
    BackendToClientActor::new(flow_id, backend_read, client_write).start();

    Ok(())
}
