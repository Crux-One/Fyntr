#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct FlowId(pub(crate) usize);

pub(crate) mod connection;
pub(crate) mod idle_timeout;
