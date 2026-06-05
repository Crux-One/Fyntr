use std::{future::Future, io, net::SocketAddr, pin::Pin};

use tokio::net::TcpStream;

use crate::{flow::FlowId, security::connect_policy::ResolvedConnectTarget};

use super::backoff::connect_to_any_with_backoff;

pub(super) type UpstreamDialFuture<'a> =
    Pin<Box<dyn Future<Output = io::Result<UpstreamConnection>> + Send + 'a>>;

pub(super) struct UpstreamConnection {
    pub(super) stream: TcpStream,
    pub(super) connected_addr: SocketAddr,
}

pub(super) trait UpstreamDialer {
    fn dial<'a>(
        &'a self,
        flow_id: FlowId,
        target: &'a ResolvedConnectTarget,
    ) -> UpstreamDialFuture<'a>;
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct DirectConnector;

impl UpstreamDialer for DirectConnector {
    fn dial<'a>(
        &'a self,
        flow_id: FlowId,
        target: &'a ResolvedConnectTarget,
    ) -> UpstreamDialFuture<'a> {
        Box::pin(async move {
            let (stream, connected_addr) =
                connect_to_any_with_backoff(flow_id, &target.addrs).await?;
            Ok(UpstreamConnection {
                stream,
                connected_addr,
            })
        })
    }
}
