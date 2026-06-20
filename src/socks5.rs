use std::{
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use actix::prelude::*;
use anyhow::{Result, anyhow};
use log::{info, warn};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::Mutex,
    time::timeout,
};

use crate::{
    actors::{
        queue::QueueActor,
        scheduler::{PendingConnectionReservation, Register, RegisterError, Scheduler},
    },
    flow::{
        FlowId,
        connection::{
            BidirectionalTunnel, FlowCleanup, SchedulerCapacityError, TunnelContext, TunnelIo,
            ensure_scheduler_capacity, start_bidirectional_tunnel,
        },
        idle_timeout::TunnelLifecycle,
    },
    http::connect::upstream::{DirectConnector, UpstreamDialer},
    security::connect_policy::{ConnectPolicy, ConnectPolicyError, ResolvedConnectTarget},
    threat::{
        ThreatAction, log_mixed_script_host_for_target, log_threat_match_for_target,
        log_threat_matches_for_target,
    },
};

const SOCKS5_VERSION: u8 = 0x05;
const SOCKS5_NO_AUTH: u8 = 0x00;
const SOCKS5_NO_ACCEPTABLE_METHODS: u8 = 0xff;
const SOCKS5_CMD_CONNECT: u8 = 0x01;
const SOCKS5_ATYP_IPV4: u8 = 0x01;
const SOCKS5_ATYP_DOMAINNAME: u8 = 0x03;
const SOCKS5_ATYP_IPV6: u8 = 0x04;
const SOCKS5_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);
const SOCKS5_LOG_TARGET: &str = module_path!();
const SOCKS5_TARGET_FIELD: &str = "socks5_target";

type Socks5Result<T> = Result<T, Socks5FlowError>;

#[derive(Debug)]
enum Socks5FlowError {
    ResponseSent,
    Fatal(anyhow::Error),
}

impl From<anyhow::Error> for Socks5FlowError {
    fn from(err: anyhow::Error) -> Self {
        Self::Fatal(err)
    }
}

impl From<std::io::Error> for Socks5FlowError {
    fn from(err: std::io::Error) -> Self {
        Self::Fatal(err.into())
    }
}

#[derive(Clone, Copy)]
enum Socks5Reply {
    Succeeded = 0x00,
    GeneralFailure = 0x01,
    ConnectionNotAllowed = 0x02,
    NetworkUnreachable = 0x03,
    HostUnreachable = 0x04,
    ConnectionRefused = 0x05,
    CommandNotSupported = 0x07,
    AddressTypeNotSupported = 0x08,
}

struct Socks5Target {
    host: String,
    port: u16,
}

struct Socks5Authority<'a> {
    host: &'a str,
    port: u16,
}

impl fmt::Display for Socks5Authority<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

struct Socks5Session {
    flow_id: FlowId,
    client_addr: SocketAddr,
    scheduler: Addr<Scheduler>,
    connect_policy: Arc<ConnectPolicy>,
    client_reader: BufReader<OwnedReadHalf>,
    client_write: OwnedWriteHalf,
    pending_reservation: Option<PendingConnectionReservation>,
    idle_timeout: Option<Duration>,
}

/// Handles a no-auth SOCKS5 CONNECT flow and relays it through Fyntr's existing scheduler.
pub(crate) async fn handle_socks5_proxy(
    client_stream: TcpStream,
    client_addr: SocketAddr,
    flow_id: FlowId,
    scheduler: Addr<Scheduler>,
    connect_policy: Arc<ConnectPolicy>,
    pending_reservation: PendingConnectionReservation,
    idle_timeout: Option<Duration>,
) -> Result<()> {
    let session = Socks5Session::new(
        client_stream,
        flow_id,
        client_addr,
        scheduler,
        connect_policy,
        pending_reservation,
        idle_timeout,
    );

    match run_socks5_flow(session).await {
        Ok(()) | Err(Socks5FlowError::ResponseSent) => Ok(()),
        Err(Socks5FlowError::Fatal(err)) => Err(err),
    }
}

async fn run_socks5_flow(mut session: Socks5Session) -> Socks5Result<()> {
    negotiate_no_auth(&mut session).await?;
    let target = read_connect_request(&mut session).await?;
    let authority = Socks5Authority {
        host: &target.host,
        port: target.port,
    };
    info!("flow{}: SOCKS5 CONNECT {}", session.flow_id.0, authority);
    log_mixed_script_host(
        session.flow_id,
        &target.host,
        &authority,
        session.client_addr,
    );

    if let Some(threat_match) = session.connect_policy.lookup_threat_host(&target.host) {
        let threat_action = session.connect_policy.threat_action();
        log_threat_match_for_target(
            SOCKS5_LOG_TARGET,
            session.flow_id,
            threat_action,
            SOCKS5_TARGET_FIELD,
            &authority,
            session.client_addr,
            &threat_match,
        );
        if matches!(threat_action, ThreatAction::Block) {
            warn!(
                "flow{}: SOCKS5 target blocked by threat feed",
                session.flow_id.0
            );
            return session
                .respond_failure(Socks5Reply::ConnectionNotAllowed)
                .await;
        }
    }

    reject_if_scheduler_at_capacity(&mut session, "before policy resolution").await?;

    let resolved = match session
        .connect_policy
        .resolve_and_authorize(&target.host, target.port)
        .await
    {
        Ok(target) => target,
        Err(ConnectPolicyError::Denied(reason)) => {
            warn!(
                "flow{}: SOCKS5 CONNECT {} denied for {}: {}",
                session.flow_id.0, authority, session.client_addr, reason
            );
            return session
                .respond_failure(Socks5Reply::ConnectionNotAllowed)
                .await;
        }
        Err(ConnectPolicyError::ResolveFailed(reason)) => {
            warn!(
                "flow{}: failed to resolve SOCKS5 CONNECT {} for {}: {}",
                session.flow_id.0, authority, session.client_addr, reason
            );
            return session.respond_failure(Socks5Reply::HostUnreachable).await;
        }
    };

    handle_resolved_threat_matches(&mut session, &authority, &resolved).await?;
    reject_if_scheduler_at_capacity(&mut session, "before dialing").await?;

    let connector = DirectConnector;
    let upstream_connection = match connector.dial(session.flow_id, &resolved).await {
        Ok(connection) => connection,
        Err(err) => {
            warn!(
                "flow{}: failed to connect SOCKS5 target {} after retries: {}",
                session.flow_id.0, authority, err
            );
            return session.respond_failure(reply_for_connect_error(&err)).await;
        }
    };

    let backend_stream = upstream_connection.stream;
    let connected_addr = upstream_connection.connected_addr;
    backend_stream
        .set_nodelay(true)
        .map_err(|err| anyhow!("Failed to set TCP_NODELAY: {}", err))?;
    let bound_addr = backend_stream
        .local_addr()
        .map_err(|err| anyhow!("Failed to resolve upstream local address: {}", err))?;

    info!(
        "flow{}: SOCKS5 connected to {} via {}",
        session.flow_id.0, authority, connected_addr
    );

    let (backend_read, backend_write) = backend_stream.into_split();
    let queue_tx = QueueActor::new().start();
    let backend_write = Arc::new(Mutex::new(backend_write));
    let tunnel_lifecycle = TunnelLifecycle::new();
    let mut cleanup = FlowCleanup::new(session.flow_id, session.scheduler.clone());
    cleanup.watch_queue(queue_tx.clone());

    match session
        .scheduler
        .send(Register {
            flow_id: session.flow_id,
            queue_addr: queue_tx.clone(),
            backend_write: backend_write.clone(),
            tunnel_lifecycle: tunnel_lifecycle.clone(),
        })
        .await
    {
        Ok(Ok(())) => {
            if let Some(reservation) = session.pending_reservation.take() {
                reservation.consumed_by_register();
            }
            cleanup.mark_registered();
        }
        Ok(Err(RegisterError::MaxConnectionsReached { max })) => {
            warn!(
                "flow{}: SOCKS5 registration rejected - max connections ({}) reached",
                session.flow_id.0, max
            );
            return session.respond_failure(Socks5Reply::GeneralFailure).await;
        }
        Ok(Err(err)) => {
            return Err(Socks5FlowError::Fatal(anyhow!(
                "Scheduler registration rejected: {}",
                err
            )));
        }
        Err(err) => {
            return Err(Socks5FlowError::Fatal(anyhow!(
                "Scheduler registration failed: {}",
                err
            )));
        }
    }

    send_success_reply(&mut session.client_write, bound_addr).await?;

    let Socks5Session {
        flow_id,
        scheduler,
        client_reader,
        client_write,
        idle_timeout,
        ..
    } = session;
    let client_read = client_reader.into_inner();
    start_bidirectional_tunnel(BidirectionalTunnel {
        context: TunnelContext {
            flow_id,
            scheduler,
            lifecycle: tunnel_lifecycle,
            idle_timeout,
        },
        io: TunnelIo {
            client_read,
            queue_tx,
            backend_read,
            client_write,
        },
    });

    cleanup.disarm();
    Ok(())
}

async fn negotiate_no_auth(session: &mut Socks5Session) -> Socks5Result<()> {
    let mut header = [0_u8; 2];
    read_with_timeout(
        session.flow_id,
        &mut session.client_reader,
        &mut header,
        "greeting header",
    )
    .await?;

    if header[0] != SOCKS5_VERSION {
        return Err(Socks5FlowError::Fatal(anyhow!(
            "unsupported SOCKS version {} from {}",
            header[0],
            session.client_addr
        )));
    }
    if header[1] == 0 {
        session
            .client_write
            .write_all(&[SOCKS5_VERSION, SOCKS5_NO_ACCEPTABLE_METHODS])
            .await?;
        return Err(Socks5FlowError::ResponseSent);
    }

    let mut methods = vec![0_u8; usize::from(header[1])];
    read_with_timeout(
        session.flow_id,
        &mut session.client_reader,
        &mut methods,
        "greeting methods",
    )
    .await?;

    if !methods.contains(&SOCKS5_NO_AUTH) {
        session
            .client_write
            .write_all(&[SOCKS5_VERSION, SOCKS5_NO_ACCEPTABLE_METHODS])
            .await?;
        return Err(Socks5FlowError::ResponseSent);
    }

    session
        .client_write
        .write_all(&[SOCKS5_VERSION, SOCKS5_NO_AUTH])
        .await?;
    Ok(())
}

async fn read_connect_request(session: &mut Socks5Session) -> Socks5Result<Socks5Target> {
    let mut header = [0_u8; 4];
    read_with_timeout(
        session.flow_id,
        &mut session.client_reader,
        &mut header,
        "request header",
    )
    .await?;

    if header[0] != SOCKS5_VERSION {
        return session.respond_failure(Socks5Reply::GeneralFailure).await;
    }
    if header[1] != SOCKS5_CMD_CONNECT {
        return session
            .respond_failure(Socks5Reply::CommandNotSupported)
            .await;
    }
    if header[2] != 0x00 {
        return session.respond_failure(Socks5Reply::GeneralFailure).await;
    }

    let host = match header[3] {
        SOCKS5_ATYP_IPV4 => {
            let mut octets = [0_u8; 4];
            read_with_timeout(
                session.flow_id,
                &mut session.client_reader,
                &mut octets,
                "IPv4 address",
            )
            .await?;
            IpAddr::V4(Ipv4Addr::from(octets)).to_string()
        }
        SOCKS5_ATYP_DOMAINNAME => {
            let mut len = [0_u8; 1];
            read_with_timeout(
                session.flow_id,
                &mut session.client_reader,
                &mut len,
                "domain length",
            )
            .await?;
            if len[0] == 0 {
                return session
                    .respond_failure(Socks5Reply::AddressTypeNotSupported)
                    .await;
            }
            let mut name = vec![0_u8; usize::from(len[0])];
            read_with_timeout(
                session.flow_id,
                &mut session.client_reader,
                &mut name,
                "domain",
            )
            .await?;
            match String::from_utf8(name) {
                Ok(host) => host,
                Err(err) => {
                    warn!(
                        "flow{}: invalid SOCKS5 domain name: {}",
                        session.flow_id.0, err
                    );
                    return session
                        .respond_failure(Socks5Reply::AddressTypeNotSupported)
                        .await;
                }
            }
        }
        SOCKS5_ATYP_IPV6 => {
            let mut octets = [0_u8; 16];
            read_with_timeout(
                session.flow_id,
                &mut session.client_reader,
                &mut octets,
                "IPv6 address",
            )
            .await?;
            IpAddr::V6(Ipv6Addr::from(octets)).to_string()
        }
        _ => {
            return session
                .respond_failure(Socks5Reply::AddressTypeNotSupported)
                .await;
        }
    };

    let mut port = [0_u8; 2];
    read_with_timeout(
        session.flow_id,
        &mut session.client_reader,
        &mut port,
        "port",
    )
    .await?;
    Ok(Socks5Target {
        host,
        port: u16::from_be_bytes(port),
    })
}

async fn read_with_timeout(
    flow_id: FlowId,
    reader: &mut BufReader<OwnedReadHalf>,
    buf: &mut [u8],
    phase: &str,
) -> Socks5Result<()> {
    match timeout(SOCKS5_HANDSHAKE_TIMEOUT, reader.read_exact(buf)).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(err)) => Err(Socks5FlowError::Fatal(err.into())),
        Err(_) => Err(Socks5FlowError::Fatal(anyhow!(
            "flow{}: timed out waiting for SOCKS5 {} after {:?}",
            flow_id.0,
            phase,
            SOCKS5_HANDSHAKE_TIMEOUT
        ))),
    }
}

async fn reject_if_scheduler_at_capacity(
    session: &mut Socks5Session,
    phase: &'static str,
) -> Socks5Result<()> {
    match ensure_scheduler_capacity(&session.scheduler, phase).await {
        Ok(()) => Ok(()),
        Err(SchedulerCapacityError::AtCapacity { .. }) => {
            warn!(
                "flow{}: SOCKS5 scheduler at capacity {}",
                session.flow_id.0, phase
            );
            session.respond_failure(Socks5Reply::GeneralFailure).await
        }
        Err(err @ SchedulerCapacityError::Unavailable { .. }) => {
            Err(Socks5FlowError::Fatal(err.into()))
        }
    }
}

async fn handle_resolved_threat_matches(
    session: &mut Socks5Session,
    authority: &Socks5Authority<'_>,
    target: &ResolvedConnectTarget,
) -> Socks5Result<()> {
    let threat_matches = session.connect_policy.resolved_threat_matches(target);
    if threat_matches.is_empty() {
        return Ok(());
    }

    let threat_action = session.connect_policy.threat_action();
    log_threat_matches_for_target(
        SOCKS5_LOG_TARGET,
        session.flow_id,
        threat_action,
        SOCKS5_TARGET_FIELD,
        authority,
        session.client_addr,
        &threat_matches,
    );

    if matches!(threat_action, ThreatAction::Block) {
        return session
            .respond_failure(Socks5Reply::ConnectionNotAllowed)
            .await;
    }
    Ok(())
}

fn log_mixed_script_host(
    flow_id: FlowId,
    raw_host: &str,
    authority: &Socks5Authority<'_>,
    client_addr: SocketAddr,
) {
    log_mixed_script_host_for_target(
        SOCKS5_LOG_TARGET,
        flow_id,
        raw_host,
        SOCKS5_TARGET_FIELD,
        authority,
        client_addr,
    );
}

async fn send_success_reply(
    writer: &mut OwnedWriteHalf,
    bound_addr: SocketAddr,
) -> Socks5Result<()> {
    write_reply_with_bound_addr(writer, Socks5Reply::Succeeded, bound_addr).await?;
    Ok(())
}

async fn write_reply_with_bound_addr(
    writer: &mut OwnedWriteHalf,
    reply: Socks5Reply,
    bound_addr: SocketAddr,
) -> std::io::Result<()> {
    writer
        .write_all(&reply_with_bound_addr_bytes(reply, bound_addr))
        .await
}

fn reply_with_bound_addr_bytes(reply: Socks5Reply, bound_addr: SocketAddr) -> Vec<u8> {
    let mut response = vec![SOCKS5_VERSION, reply as u8, 0x00];
    match bound_addr.ip() {
        IpAddr::V4(addr) => {
            response.push(SOCKS5_ATYP_IPV4);
            response.extend_from_slice(&addr.octets());
        }
        IpAddr::V6(addr) => {
            response.push(SOCKS5_ATYP_IPV6);
            response.extend_from_slice(&addr.octets());
        }
    }
    response.extend_from_slice(&bound_addr.port().to_be_bytes());
    response
}

async fn write_reply(writer: &mut OwnedWriteHalf, reply: Socks5Reply) -> std::io::Result<()> {
    writer
        .write_all(&[
            SOCKS5_VERSION,
            reply as u8,
            0x00,
            SOCKS5_ATYP_IPV4,
            0,
            0,
            0,
            0,
            0,
            0,
        ])
        .await
}

fn reply_for_connect_error(err: &std::io::Error) -> Socks5Reply {
    match err.kind() {
        std::io::ErrorKind::ConnectionRefused => Socks5Reply::ConnectionRefused,
        std::io::ErrorKind::NetworkUnreachable => Socks5Reply::NetworkUnreachable,
        std::io::ErrorKind::TimedOut => Socks5Reply::HostUnreachable,
        _ => Socks5Reply::HostUnreachable,
    }
}

impl Socks5Session {
    fn new(
        client_stream: TcpStream,
        flow_id: FlowId,
        client_addr: SocketAddr,
        scheduler: Addr<Scheduler>,
        connect_policy: Arc<ConnectPolicy>,
        pending_reservation: PendingConnectionReservation,
        idle_timeout: Option<Duration>,
    ) -> Self {
        let (client_read, client_write) = client_stream.into_split();
        Self {
            flow_id,
            client_addr,
            scheduler,
            connect_policy,
            client_reader: BufReader::new(client_read),
            client_write,
            pending_reservation: Some(pending_reservation),
            idle_timeout,
        }
    }

    async fn respond_failure<T>(&mut self, reply: Socks5Reply) -> Socks5Result<T> {
        write_reply(&mut self.client_write, reply).await?;
        Err(Socks5FlowError::ResponseSent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn success_reply_uses_ipv4_bound_address() {
        let bound_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 54321);

        let reply = reply_with_bound_addr_bytes(Socks5Reply::Succeeded, bound_addr);

        assert_eq!(
            reply,
            vec![0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0xd4, 0x31]
        );
    }

    #[test]
    fn success_reply_uses_ipv6_bound_address() {
        let bound_addr =
            SocketAddr::new(IpAddr::V6("2001:db8::1".parse::<Ipv6Addr>().unwrap()), 443);

        let reply = reply_with_bound_addr_bytes(Socks5Reply::Succeeded, bound_addr);

        assert_eq!(reply[0..4], [0x05, 0x00, 0x00, 0x04]);
        assert_eq!(
            &reply[4..20],
            &"2001:db8::1".parse::<Ipv6Addr>().unwrap().octets()
        );
        assert_eq!(u16::from_be_bytes([reply[20], reply[21]]), 443);
    }
}
