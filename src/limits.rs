use std::num::NonZeroUsize;

pub type MaxConnections = Option<NonZeroUsize>;

pub(crate) const MAX_REQUEST_LINE_BYTES: usize = 8 * 1024;
pub(crate) const MAX_HEADER_LINE_BYTES: usize = 8 * 1024;
pub(crate) const MAX_HEADER_BYTES: usize = 32 * 1024;
pub(crate) const MAX_HEADER_LINES: usize = 100;

pub(crate) const MAX_QUEUE_PACKET_BYTES: usize = 64 * 1024;
pub(crate) const MAX_QUEUE_BUFFERED_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const MAX_DEQUEUE_BYTES: usize = MAX_QUEUE_PACKET_BYTES;

#[inline]
pub(crate) fn max_connections_from_raw(raw: usize) -> MaxConnections {
    NonZeroUsize::new(raw)
}

#[inline]
pub(crate) fn max_connections_value(max: MaxConnections) -> Option<usize> {
    max.map(|n| n.get())
}

#[inline]
pub(crate) fn max_connections_display(max: MaxConnections) -> String {
    max_connections_value(max)
        .map(|limit| limit.to_string())
        .unwrap_or_else(|| "∞".to_string())
}
