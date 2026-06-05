use std::{future::Future, io, net::SocketAddr, pin::Pin};

use tokio::net::TcpStream;

use crate::{flow::FlowId, security::connect_policy::ResolvedConnectTarget};

use super::backoff::connect_to_any_with_backoff;

pub(crate) type UpstreamDialFuture<'a> =
    Pin<Box<dyn Future<Output = io::Result<UpstreamConnection>> + Send + 'a>>;

pub(crate) struct UpstreamConnection {
    pub(crate) stream: TcpStream,
    pub(crate) connected_addr: SocketAddr,
}

pub(crate) trait UpstreamDialer {
    fn dial<'a>(
        &'a self,
        flow_id: FlowId,
        target: &'a ResolvedConnectTarget,
    ) -> UpstreamDialFuture<'a>;
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct DirectConnector;

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
