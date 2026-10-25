use std::{sync::Arc, time::Duration};

use actix::Addr;

use crate::{
    actors::scheduler::Scheduler, http::connect::upstream::DirectConnector,
    security::connect_policy::ConnectPolicy,
};

/// Server-wide dependencies shared by HTTP CONNECT and SOCKS5 flows.
#[derive(Clone)]
pub(crate) struct ProxyContext {
    pub(crate) scheduler: Addr<Scheduler>,
    pub(crate) connect_policy: Arc<ConnectPolicy>,
    pub(crate) idle_timeout: Option<Duration>,
    pub(crate) connector: DirectConnector,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct FlowId(pub(crate) usize);

pub(crate) mod connection;
pub(crate) mod idle_timeout;
