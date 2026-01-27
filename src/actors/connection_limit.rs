use std::sync::atomic::{AtomicUsize, Ordering};

use log::warn;

use crate::limits::MaxConnections;

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
    max_connections: MaxConnections,
    current_connections: AtomicUsize,
}

impl ConnectionLimiter {
    pub(crate) fn new() -> Self {
        Self {
            max_connections: None,
            current_connections: AtomicUsize::new(0),
        }
    }

    pub(crate) fn set_max_connections(&mut self, max_connections: MaxConnections) {
        self.max_connections = max_connections;
    }

    pub(crate) fn max_connections(&self) -> MaxConnections {
        self.max_connections
    }

    pub(crate) fn current(&self) -> usize {
        self.current_connections.load(Ordering::Acquire)
    }

    pub(crate) fn try_acquire(&self) -> Result<(), RegisterError> {
        match self.max_connections {
            Some(limit) => {
                let limit = limit.get();
                let mut current = self.current_connections.load(Ordering::Acquire);
                loop {
                    if current >= limit {
                        warn!("connection rejected - max connections ({}) reached", limit);
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
        let result =
            self.current_connections
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    (current > 0).then_some(current - 1)
                });
        if result.is_err() {
            warn!("attempted to decrement connection count below zero");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::limits::max_connections_from_raw;
    use std::sync::{Arc, Barrier, atomic::AtomicUsize};
    use std::thread;

    #[test]
    fn acquire_and_release_under_limit() {
        let mut limiter = ConnectionLimiter::new();
        limiter.set_max_connections(max_connections_from_raw(2));

        limiter.try_acquire().expect("first acquire should succeed");
        limiter
            .try_acquire()
            .expect("second acquire should succeed");
        assert_eq!(limiter.current(), 2);

        limiter.release();
        limiter.release();
        assert_eq!(limiter.current(), 0);
    }

    #[test]
    fn rejects_when_limit_reached() {
        let mut limiter = ConnectionLimiter::new();
        limiter.set_max_connections(max_connections_from_raw(1));

        limiter.try_acquire().expect("first acquire should succeed");
        let err = limiter
            .try_acquire()
            .expect_err("second acquire should fail");
        assert!(matches!(
            err,
            RegisterError::MaxConnectionsReached { max: 1 }
        ));
        assert_eq!(limiter.current(), 1);
    }

    #[test]
    fn zero_max_treated_as_unlimited() {
        let mut limiter = ConnectionLimiter::new();
        limiter.set_max_connections(max_connections_from_raw(0));

        for _ in 0..3 {
            limiter
                .try_acquire()
                .expect("unlimited limiter should allow acquire");
        }
        assert_eq!(limiter.current(), 3);

        for _ in 0..3 {
            limiter.release();
        }
        assert_eq!(limiter.current(), 0);
    }

    #[test]
    fn concurrent_acquire_respects_limit() {
        let limiter = Arc::new({
            let mut l = ConnectionLimiter::new();
            l.set_max_connections(max_connections_from_raw(3));
            l
        });

        let attempts = 10;
        let successes = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(attempts));

        let mut handles = Vec::new();
        for _ in 0..attempts {
            let limiter = Arc::clone(&limiter);
            let successes = Arc::clone(&successes);
            let barrier = Arc::clone(&barrier);

            handles.push(thread::spawn(move || {
                barrier.wait(); // start together
                let acquired = limiter.try_acquire().is_ok();
                if acquired {
                    successes.fetch_add(1, Ordering::AcqRel);
                }
                barrier.wait(); // ensure all attempts recorded before releasing
                if acquired {
                    limiter.release();
                }
            }));
        }

        for handle in handles {
            handle.join().expect("thread should not panic");
        }

        assert_eq!(
            successes.load(Ordering::Acquire),
            3,
            "only three threads should acquire concurrently"
        );
        assert_eq!(limiter.current(), 0);
    }
}
