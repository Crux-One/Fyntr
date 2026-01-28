use std::num::NonZeroUsize;

pub type MaxConnections = Option<NonZeroUsize>;

#[inline]
pub fn max_connections_from_raw(raw: usize) -> MaxConnections {
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
