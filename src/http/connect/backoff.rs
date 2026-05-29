use std::{net::SocketAddr, time::Duration};

use log::warn;
use tokio::{
    net::TcpStream,
    time::{sleep, timeout},
};

use crate::flow::FlowId;

use super::CONNECT_LOG_TARGET;

const CONNECT_MAX_ATTEMPTS: usize = 3;
pub(super) const CONNECT_BACKOFF_BASE: Duration = Duration::from_millis(200);
pub(super) const CONNECT_BACKOFF_MAX: Duration = Duration::from_secs(3);
const CONNECT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(3);

#[cfg(test)]
pub(super) async fn connect_with_backoff(
    flow_id: FlowId,
    backend_addr: SocketAddr,
) -> std::io::Result<TcpStream> {
    let mut delay = CONNECT_BACKOFF_BASE;
    let mut attempt = 1;

    loop {
        match TcpStream::connect(backend_addr).await {
            Ok(stream) => return Ok(stream),
            Err(err) if attempt == CONNECT_MAX_ATTEMPTS => return Err(err),
            Err(err) => {
                warn!(
                    target: CONNECT_LOG_TARGET,
                    "flow{}: connect attempt {}/{} to {} failed: {}; backing off {:?}",
                    flow_id.0, attempt, CONNECT_MAX_ATTEMPTS, backend_addr, err, delay
                );
                sleep(delay).await;
                delay = (delay.saturating_mul(2)).min(CONNECT_BACKOFF_MAX);
                attempt += 1;
            }
        }
    }
}

pub(super) async fn connect_to_any_with_backoff(
    flow_id: FlowId,
    backend_addrs: &[SocketAddr],
) -> std::io::Result<(TcpStream, SocketAddr)> {
    if backend_addrs.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "no backend addresses to connect",
        ));
    }

    // Use round-robin retries across resolved addresses to avoid spending all retries
    // on the first address when later addresses may already be reachable.
    // A full Happy Eyeballs (RFC 8305) strategy can be added later if needed.
    let mut delay = CONNECT_BACKOFF_BASE;
    let mut last_err = None;
    for attempt in 1..=CONNECT_MAX_ATTEMPTS {
        for &addr in backend_addrs {
            let connect = TcpStream::connect(addr);
            match timeout(CONNECT_ATTEMPT_TIMEOUT, connect).await {
                Ok(Ok(stream)) => return Ok((stream, addr)),
                Ok(Err(err)) => last_err = Some(err),
                Err(_) => {
                    last_err = Some(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!(
                            "connect attempt to {} timed out after {:?}",
                            addr, CONNECT_ATTEMPT_TIMEOUT
                        ),
                    ));
                }
            }
        }

        if attempt < CONNECT_MAX_ATTEMPTS {
            warn!(
                target: CONNECT_LOG_TARGET,
                "flow{}: connect round {}/{} failed for all {} addresses; backing off {:?}",
                flow_id.0,
                attempt,
                CONNECT_MAX_ATTEMPTS,
                backend_addrs.len(),
                delay
            );
            sleep(delay).await;
            delay = (delay.saturating_mul(2)).min(CONNECT_BACKOFF_MAX);
        }
    }

    Err(last_err.expect("backend_addrs checked as non-empty"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backoff_sequence(steps: usize) -> Vec<Duration> {
        let mut delay = CONNECT_BACKOFF_BASE;
        let mut seq = Vec::with_capacity(steps);
        for _ in 0..steps {
            seq.push(delay);
            delay = (delay.saturating_mul(2)).min(CONNECT_BACKOFF_MAX);
        }
        seq
    }

    #[test]
    fn backoff_delays_cap_at_max() {
        let delays = backoff_sequence(6);

        assert_eq!(delays[0], CONNECT_BACKOFF_BASE);
        assert_eq!(delays[1], CONNECT_BACKOFF_BASE.saturating_mul(2));
        assert_eq!(delays[2], CONNECT_BACKOFF_BASE.saturating_mul(4));
        assert_eq!(delays[3], CONNECT_BACKOFF_BASE.saturating_mul(8));
        assert_eq!(delays[4], CONNECT_BACKOFF_MAX);
        assert_eq!(delays[5], CONNECT_BACKOFF_MAX);
    }
}
