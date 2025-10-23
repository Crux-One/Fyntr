use std::{net::SocketAddr, sync::Arc};

use actix::prelude::*;
use anyhow::{Result, anyhow};

use log::{error, info, warn};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::TcpStream,
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

    // Read the HTTP request line
    let request_line = read_request_line(&mut client_reader).await?;

    // Only support HTTP/1.x
    if !request_line.is_http_1x() {
        warn!(
            "flow{}: unsupported HTTP version {} from {}",
            flow_id.0, request_line.version, client_addr
        );
        let response = b"HTTP/1.1 505 HTTP Version Not Supported\r\n\r\n";
        client_write.write_all(response).await?;
        return Ok(());
    }

    // Return an error if the method is not CONNECT
    if !request_line.is_connect_method() {
        warn!(
            "flow{}: unsupported method {} from {}",
            flow_id.0, request_line.method, client_addr
        );
        let response = b"HTTP/1.1 405 Method Not Allowed\r\n\r\n";
        client_write.write_all(response).await?;
        return Ok(());
    }

    // Extract the CONNECT target (host:port)
    let (target_host, target_port) = request_line.parse_connect_target()?;
    info!("flow{}: CONNECT {}:{}", flow_id.0, target_host, target_port);

    // Skip headers until the blank line
    skip_headers(&mut client_reader).await?;

    // Connect to the backend server
    let backend_addr = format!("{}:{}", target_host, target_port);
    let backend_stream = match TcpStream::connect(&backend_addr).await {
        Ok(stream) => {
            stream
                .set_nodelay(true)
                .map_err(|e| anyhow!("Failed to set TCP_NODELAY: {}", e))?;
            stream
        }
        Err(e) => {
            error!(
                "flow{}: failed to connect to {}: {}",
                flow_id.0, backend_addr, e
            );
            let response = b"HTTP/1.1 502 Bad Gateway\r\n\r\n";
            client_write.write_all(response).await?;
            return Ok(());
        }
    };

    info!("flow{}: connected to {}", flow_id.0, backend_addr);

    // Split the backend stream
    let (backend_read, backend_write) = backend_stream.into_split();
    let backend_write = Arc::new(Mutex::new(backend_write));

    // Start the QueueActor
    let queue_tx = QueueActor::new().start();

    // Register with the scheduler
    match scheduler
        .send(Register {
            flow_id,
            queue_addr: queue_tx.clone(),
            backend_write: backend_write.clone(),
        })
        .await
    {
        Ok(Ok(())) => {}
        Ok(Err(RegisterError::MaxConnectionsReached { max })) => {
            queue_tx.do_send(Close);
            warn!(
                "flow{}: registration rejected - max connections ({}) reached",
                flow_id.0, max
            );
            let response = b"HTTP/1.1 503 Service Unavailable\r\n\r\n";
            client_write.write_all(response).await?;
            return Ok(());
        }
        Err(e) => {
            queue_tx.do_send(Close);
            error!(
                "flow{}: failed to register with scheduler: {}",
                flow_id.0, e
            );
            return Err(anyhow!("Scheduler registration failed"));
        }
    }

    // Send the "200 Connection Established" response
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

    // Recover the original OwnedReadHalf from the BufReader
    let client_read = client_reader.into_inner();

    // Start bidirectional transparent forwarding via actors
    // client → backend (with DRR control)
    ClientToBackendActor::new(flow_id, client_read, queue_tx, scheduler.clone()).start();

    // backend → client (no throttling)
    BackendToClientActor::new(flow_id, backend_read, client_write).start();

    Ok(())
}
