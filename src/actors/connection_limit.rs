use std::sync::atomic::{AtomicUsize, Ordering};

use log::warn;

#[derive(Debug)]
pub(crate) enum RegisterError {
    MaxConnectionsReached { max: usize },
}

impl std::fmt::Display for RegisterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MaxConnectionsReached { max } => {
                write!(f, "maximum connection limit ({}) reached", max)
            }
        }
    }
}

impl std::error::Error for RegisterError {}

/// Tracks and enforces a maximum number of concurrent connections.
#[derive(Debug)]
pub(crate) struct ConnectionLimiter {
    max_connections: Option<usize>,
    current_connections: AtomicUsize,
}

impl ConnectionLimiter {
    pub(crate) fn new() -> Self {
        Self {
            max_connections: None,
            current_connections: AtomicUsize::new(0),
        }
    }

    pub(crate) fn set_max_connections(&mut self, max_connections: usize) {
        self.max_connections = if max_connections == 0 {
            None
        } else {
            Some(max_connections)
        };
    }

    pub(crate) fn max_connections(&self) -> Option<usize> {
        self.max_connections
    }

    pub(crate) fn current(&self) -> usize {
        self.current_connections.load(Ordering::Acquire)
    }

    pub(crate) fn try_acquire(&self) -> Result<(), RegisterError> {
        match self.max_connections {
            Some(limit) => {
                let mut current = self.current_connections.load(Ordering::Acquire);
                loop {
                    if current >= limit {
                        warn!(
                            "scheduler: connection rejected - max connections ({}) reached",
                            limit
                        );
                        return Err(RegisterError::MaxConnectionsReached { max: limit });
                    }

                    match self.current_connections.compare_exchange(
                        current,
                        current + 1,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => return Ok(()),
                        Err(observed) => current = observed,
                    }
                }
            }
            None => {
                self.current_connections.fetch_add(1, Ordering::AcqRel);
                Ok(())
            }
        }
    }

    pub(crate) fn release(&self) {
        let result = self
            .current_connections
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current > 0).then_some(current - 1)
            });
        if result.is_err() {
            warn!("scheduler: attempted to decrement connection count below zero");
        }
    }
}
