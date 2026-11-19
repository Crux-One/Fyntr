use std::{future::Future, io, net::SocketAddr, pin::Pin, sync::Arc, time::Duration};

use tokio::{
    net::TcpStream,
    sync::Mutex,
    time::{Instant, sleep, timeout},
};

use crate::{flow::FlowId, security::connect_policy::ResolvedConnectTarget};

use super::backoff::connect_to_any_with_backoff_with_log_target;

const UPSTREAM_LOG_TARGET: &str = module_path!();

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

const CONNECT_START_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

/// Shared per server, across protocols, addresses and retry attempts.
#[derive(Debug)]
pub(super) struct ConnectStartLimiter {
    interval: Duration,
    last_start: Mutex<Option<Instant>>,
}

impl ConnectStartLimiter {
    pub(super) fn new(interval: Duration) -> Self {
        Self {
            interval,
            last_start: Mutex::new(None),
        }
    }

    pub(super) async fn wait(&self) -> io::Result<()> {
        if self.interval.is_zero() {
            return Ok(());
        }
        let deadline = Instant::now() + CONNECT_START_WAIT_TIMEOUT;
        timeout(CONNECT_START_WAIT_TIMEOUT, async {
            // FIFO lock includes the spacing wait, but never the network connect.
            // State changes only on grant, so cancellation reserves no future slot.
            let mut last_start = self.last_start.lock().await;
            if Instant::now() >= deadline {
                return Err(permit_timeout());
            }
            if let Some(last) = *last_start {
                let remaining = self.interval.saturating_sub(last.elapsed());
                if remaining >= CONNECT_START_WAIT_TIMEOUT {
                    // Avoid Instant overflow even for Duration::MAX. The outer
                    // timeout bounds both queueing and spacing for this permit.
                    sleep(CONNECT_START_WAIT_TIMEOUT).await;
                    return Err(permit_timeout());
                }
                if !remaining.is_zero() {
                    sleep(remaining).await;
                }
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(permit_timeout());
            }
            *last_start = Some(now);
            Ok(())
        })
        .await
        .map_err(|_| permit_timeout())?
    }
}

fn permit_timeout() -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        "timed out waiting for an outbound TCP connection start permit after 30s",
    )
}

#[derive(Clone, Debug)]
pub(crate) struct DirectConnector {
    limiter: Arc<ConnectStartLimiter>,
}

impl Default for DirectConnector {
    fn default() -> Self {
        Self::new(Duration::ZERO)
    }
}

impl DirectConnector {
    pub(crate) fn new(interval: Duration) -> Self {
        Self {
            limiter: Arc::new(ConnectStartLimiter::new(interval)),
        }
    }
}

impl UpstreamDialer for DirectConnector {
    fn dial<'a>(
        &'a self,
        flow_id: FlowId,
        target: &'a ResolvedConnectTarget,
    ) -> UpstreamDialFuture<'a> {
        Box::pin(async move {
            let (stream, connected_addr) = connect_to_any_with_backoff_with_log_target(
                flow_id,
                &target.addrs,
                UPSTREAM_LOG_TARGET,
                &self.limiter,
            )
            .await?;
            Ok(UpstreamConnection {
                stream,
                connected_addr,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{future::poll_fn, task::Poll};
    use tokio::time::advance;

    async fn assert_pending<F: Future>(mut future: Pin<&mut F>) {
        poll_fn(|cx| {
            assert!(future.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn disabled_and_first_permits_do_not_wait() {
        for interval in [Duration::ZERO, Duration::from_millis(200)] {
            let limiter = ConnectStartLimiter::new(interval);
            let started = Instant::now();
            limiter.wait().await.unwrap();
            assert_eq!(started, Instant::now());
            if interval.is_zero() {
                for _ in 0..100 {
                    limiter.wait().await.unwrap();
                }
                assert_eq!(started, Instant::now());
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_permits_are_fifo_and_spaced() {
        let interval = Duration::from_millis(200);
        let limiter = ConnectStartLimiter::new(interval);
        limiter.wait().await.unwrap();
        let started = Instant::now();
        let mut second = Box::pin(limiter.wait());
        let mut third = Box::pin(limiter.wait());
        assert_pending(second.as_mut()).await;
        assert_pending(third.as_mut()).await;
        advance(interval).await;
        second.await.unwrap();
        assert_eq!(Instant::now() - started, interval);
        assert_pending(third.as_mut()).await;
        advance(interval).await;
        third.await.unwrap();
        assert_eq!(Instant::now() - started, interval * 2);
    }

    #[tokio::test(start_paused = true)]
    async fn idle_and_delayed_wakeup_do_not_bank_permits() {
        let interval = Duration::from_millis(200);
        let limiter = ConnectStartLimiter::new(interval);
        limiter.wait().await.unwrap();
        advance(Duration::from_secs(10)).await;
        let idle_end = Instant::now();
        limiter.wait().await.unwrap();
        assert_eq!(Instant::now(), idle_end);
        let mut next = Box::pin(limiter.wait());
        assert_pending(next.as_mut()).await;
        // Even if the task resumes much later, its actual grant time is the base.
        advance(Duration::from_secs(10)).await;
        next.await.unwrap();
        let resumed = Instant::now();
        let mut following = Box::pin(limiter.wait());
        assert_pending(following.as_mut()).await;
        advance(interval).await;
        following.await.unwrap();
        assert_eq!(Instant::now() - resumed, interval);
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_queued_and_sleeping_waiters_reserves_no_slots() {
        let interval = Duration::from_millis(200);
        let limiter = ConnectStartLimiter::new(interval);
        limiter.wait().await.unwrap();
        let started = Instant::now();
        let mut sleeping = Box::pin(limiter.wait());
        let mut queued = Box::pin(limiter.wait());
        let mut follower = Box::pin(limiter.wait());
        assert_pending(sleeping.as_mut()).await;
        assert_pending(queued.as_mut()).await;
        assert_pending(follower.as_mut()).await;
        advance(interval / 2).await;
        drop(queued);
        drop(sleeping);
        assert_pending(follower.as_mut()).await;
        advance(interval / 2).await;
        follower.await.unwrap();
        assert_eq!(Instant::now() - started, interval);
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_includes_queueing_and_releases_lock() {
        let limiter = ConnectStartLimiter::new(Duration::from_secs(20));
        limiter.wait().await.unwrap();
        let started = Instant::now();
        let mut second = Box::pin(limiter.wait());
        let mut third = Box::pin(limiter.wait());
        assert_pending(second.as_mut()).await;
        assert_pending(third.as_mut()).await;
        advance(Duration::from_secs(20)).await;
        second.await.unwrap();
        assert_pending(third.as_mut()).await;
        advance(Duration::from_secs(10)).await;
        assert_eq!(third.await.unwrap_err().kind(), io::ErrorKind::TimedOut);
        assert_eq!(Instant::now() - started, CONNECT_START_WAIT_TIMEOUT);
        // The timed-out sleeper did not update the last start or retain its lock.
        limiter.wait().await.unwrap();
        assert_eq!(Instant::now() - started, Duration::from_secs(40));
    }

    #[tokio::test(start_paused = true)]
    async fn overdue_waiter_cannot_grant_when_resumed_after_deadline() {
        let limiter = ConnectStartLimiter::new(Duration::from_secs(1));
        limiter.wait().await.unwrap();
        let mut waiting = Box::pin(limiter.wait());
        assert_pending(waiting.as_mut()).await;
        advance(Duration::from_secs(31)).await;
        assert_eq!(waiting.await.unwrap_err().kind(), io::ErrorKind::TimedOut);
        // No overdue reservation was recorded; a new waiter can start immediately.
        let resumed = Instant::now();
        limiter.wait().await.unwrap();
        assert_eq!(Instant::now(), resumed);
    }

    #[tokio::test(start_paused = true)]
    async fn extreme_interval_times_out_without_instant_overflow() {
        let limiter = ConnectStartLimiter::new(Duration::MAX);
        limiter.wait().await.unwrap();
        let started = Instant::now();
        assert_eq!(
            limiter.wait().await.unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert_eq!(Instant::now() - started, CONNECT_START_WAIT_TIMEOUT);
        assert!(limiter.last_start.try_lock().is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn connector_clones_share_gate_but_independent_connectors_do_not() {
        let interval = Duration::from_millis(200);
        let connector = DirectConnector::new(interval);
        let clone = connector.clone();
        let separate = DirectConnector::new(interval);
        connector.limiter.wait().await.unwrap();
        let started = Instant::now();
        separate.limiter.wait().await.unwrap();
        assert_eq!(Instant::now(), started);
        let mut shared = Box::pin(clone.limiter.wait());
        assert_pending(shared.as_mut()).await;
        advance(interval).await;
        shared.await.unwrap();
    }
}
