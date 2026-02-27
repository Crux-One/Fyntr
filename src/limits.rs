use std::num::NonZeroUsize;

pub type MaxConnections = Option<NonZeroUsize>;

pub(crate) const MAX_REQUEST_LINE_BYTES: usize = 8 * 1024;
pub(crate) const MAX_HEADER_LINE_BYTES: usize = 8 * 1024;
pub(crate) const MAX_HEADER_BYTES: usize = 32 * 1024;
pub(crate) const MAX_HEADER_LINES: usize = 100;
// Caps DNS fan-out to keep CONNECT dial latency bounded.
// With CONNECT_MAX_ATTEMPTS=3 and CONNECT_ATTEMPT_TIMEOUT=3s, this yields
// about 72s worst-case active dial time (3 * 8 * 3s), plus backoff sleeps (~0.6s).
pub(crate) const MAX_RESOLVED_CONNECT_ADDRS: usize = 8;

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
