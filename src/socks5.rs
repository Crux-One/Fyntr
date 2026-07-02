use std::{
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
    connect_target::{RequestedHost, TargetAuthority},
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
    host: RequestedHost,
    port: u16,
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
    let authority = TargetAuthority::new(target.host.raw(), target.port);
    info!("flow{}: SOCKS5 CONNECT {}", session.flow_id.0, authority);
    log_mixed_script_host(
        session.flow_id,
        target.host.raw(),
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
            RequestedHost::from_ip(IpAddr::V4(Ipv4Addr::from(octets)))
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
                Ok(host) => {
                    if is_numeric_host_name(&host) {
                        warn!(
                            "flow{}: invalid SOCKS5 domain name: numeric host names are not accepted",
                            session.flow_id.0
                        );
                        return session
                            .respond_failure(Socks5Reply::AddressTypeNotSupported)
                            .await;
                    }

                    match RequestedHost::parse_domain(&host) {
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
            RequestedHost::from_ip(IpAddr::V6(Ipv6Addr::from(octets)))
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

fn is_numeric_host_name(host: &str) -> bool {
    let host = host.strip_suffix('.').unwrap_or(host);
    if host.is_empty() {
        return false;
    }

    if is_ipv4_number_component(host) {
        return true;
    }

    host.contains('.')
        && host
            .split('.')
            .all(|part| !part.is_empty() && is_ipv4_number_component(part))
}

fn is_ipv4_number_component(value: &str) -> bool {
    let Some(rest) = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
    else {
        return value.chars().all(|c| c.is_ascii_digit());
    };

    !rest.is_empty() && rest.chars().all(|c| c.is_ascii_hexdigit())
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
    authority: &TargetAuthority<'_>,
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
    authority: &TargetAuthority<'_>,
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
    use crate::{
        actors::scheduler::TryStartConnectionTask, limits::max_connections_from_raw,
        security::connect_policy::ConnectPolicyConfig, test_utils::make_backend_write,
        threat::ThreatIndex,
    };
    use tokio::{net::TcpListener, task, time};

    const NO_AUTH_GREETING: &[u8] = &[SOCKS5_VERSION, 0x01, SOCKS5_NO_AUTH];

    async fn read_response(mut stream: TcpStream) -> Vec<u8> {
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        response
    }

    async fn exchange(
        scheduler: Addr<Scheduler>,
        connect_policy: Arc<ConnectPolicy>,
        input: Vec<u8>,
    ) -> (Result<()>, Vec<u8>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = async move {
            let (stream, peer) = listener.accept().await.unwrap();
            let flow_id = FlowId(1);
            let pending_reservation = scheduler
                .send(TryStartConnectionTask { flow_id })
                .await
                .unwrap()
                .map_or_else(
                    |_| PendingConnectionReservation::already_consumed(scheduler.clone(), flow_id),
                    |_| PendingConnectionReservation::new(scheduler.clone(), flow_id),
                );

            handle_socks5_proxy(
                stream,
                peer,
                flow_id,
                scheduler,
                connect_policy,
                pending_reservation,
                None,
            )
            .await
        };
        let client = async move {
            let mut client = TcpStream::connect(addr).await.unwrap();
            client.write_all(&input).await.unwrap();
            read_response(client).await
        };

        tokio::join!(server, client)
    }

    fn default_policy() -> Arc<ConnectPolicy> {
        Arc::new(ConnectPolicy::from_config(ConnectPolicyConfig::default()))
    }

    fn blocking_threat_policy(feed: &str) -> Arc<ConnectPolicy> {
        Arc::new(ConnectPolicy::from_config(ConnectPolicyConfig {
            threat_index: Some(feed.parse::<ThreatIndex>().unwrap()),
            threat_action: ThreatAction::Block,
            ..ConnectPolicyConfig::default()
        }))
    }

    fn ipv4_request(command: u8, addr: Ipv4Addr, port: u16) -> Vec<u8> {
        let mut request = vec![SOCKS5_VERSION, command, 0x00, SOCKS5_ATYP_IPV4];
        request.extend_from_slice(&addr.octets());
        request.extend_from_slice(&port.to_be_bytes());
        request
    }

    fn domain_request(host: &str, port: u16) -> Vec<u8> {
        let mut request = vec![
            SOCKS5_VERSION,
            SOCKS5_CMD_CONNECT,
            0x00,
            SOCKS5_ATYP_DOMAINNAME,
            host.len() as u8,
        ];
        request.extend_from_slice(host.as_bytes());
        request.extend_from_slice(&port.to_be_bytes());
        request
    }

    fn with_no_auth_greeting(request: &[u8]) -> Vec<u8> {
        let mut input = NO_AUTH_GREETING.to_vec();
        input.extend_from_slice(request);
        input
    }

    fn expected_failure_exchange(reply: Socks5Reply) -> Vec<u8> {
        let mut expected = vec![SOCKS5_VERSION, SOCKS5_NO_AUTH];
        expected.extend_from_slice(&[
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
        ]);
        expected
    }

    #[test]
    fn numeric_host_name_matches_legacy_ipv4_forms() {
        assert!(is_numeric_host_name("127.0.0.1"));
        assert!(is_numeric_host_name("127.1"));
        assert!(is_numeric_host_name("8.8.8.8"));
        assert!(is_numeric_host_name("127.0.0.1."));
        assert!(is_numeric_host_name("2130706433"));
        assert!(is_numeric_host_name("0x7f000001"));
        assert!(is_numeric_host_name("017700000001"));
        assert!(is_numeric_host_name("0x7f.1"));

        assert!(!is_numeric_host_name(".."));
        assert!(!is_numeric_host_name("..."));
        assert!(!is_numeric_host_name("example.invalid"));
        assert!(!is_numeric_host_name("api.127.example"));
    }

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

    #[actix_rt::test]
    async fn rejects_when_no_supported_authentication_method_is_offered() {
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600)).start();
        let input = vec![SOCKS5_VERSION, 0x01, 0x02];

        let (result, response) = exchange(scheduler, default_policy(), input).await;

        result.unwrap();
        assert_eq!(response, [SOCKS5_VERSION, SOCKS5_NO_ACCEPTABLE_METHODS]);
    }

    #[actix_rt::test]
    async fn rejects_unsupported_command_with_command_not_supported_reply() {
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600)).start();
        let request = ipv4_request(0x02, Ipv4Addr::LOCALHOST, 443);
        let input = with_no_auth_greeting(&request);
        let expected = expected_failure_exchange(Socks5Reply::CommandNotSupported);

        let (result, response) = exchange(scheduler, default_policy(), input).await;

        result.unwrap();
        assert_eq!(response, expected);
    }

    #[actix_rt::test]
    async fn rejects_target_denied_by_connect_policy() {
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600)).start();
        let request = ipv4_request(SOCKS5_CMD_CONNECT, Ipv4Addr::LOCALHOST, 443);
        let input = with_no_auth_greeting(&request);
        let expected = expected_failure_exchange(Socks5Reply::ConnectionNotAllowed);

        let (result, response) = exchange(scheduler, default_policy(), input).await;

        result.unwrap();
        assert_eq!(response, expected);
    }

    #[actix_rt::test]
    async fn rejects_target_blocked_by_threat_feed() {
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600)).start();
        let request = domain_request("api.example.invalid", 443);
        let input = with_no_auth_greeting(&request);
        let expected = expected_failure_exchange(Socks5Reply::ConnectionNotAllowed);

        let (result, response) = exchange(
            scheduler,
            blocking_threat_policy("||example.invalid^\n"),
            input,
        )
        .await;

        result.unwrap();
        assert_eq!(response, expected);
    }

    #[actix_rt::test]
    async fn rejects_absolute_idna_domain_matching_normalized_threat() {
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600)).start();
        let request = domain_request("BÜCHER.DE.", 443);
        let input = with_no_auth_greeting(&request);
        let expected = expected_failure_exchange(Socks5Reply::ConnectionNotAllowed);

        let (result, response) =
            exchange(scheduler, blocking_threat_policy("||bücher.de^\n"), input).await;

        result.unwrap();
        assert_eq!(response, expected);
    }

    #[actix_rt::test]
    async fn rejects_numeric_looking_domainnames() {
        let expected = expected_failure_exchange(Socks5Reply::AddressTypeNotSupported);

        for host in [
            "127.0.0.1",
            "127.1",
            "8.8.8.8",
            "127.0.0.1.",
            "2130706433",
            "0x7f000001",
            "017700000001",
        ] {
            let scheduler = Scheduler::new(1024, Duration::from_secs(3600)).start();
            let request = domain_request(host, 443);
            let input = with_no_auth_greeting(&request);

            let (result, response) = exchange(scheduler, default_policy(), input).await;

            result.unwrap();
            assert_eq!(response, expected, "unexpected response for {host:?}");
        }
    }

    #[actix_rt::test]
    async fn rejects_noncanonical_domainname_syntax() {
        let expected = expected_failure_exchange(Socks5Reply::AddressTypeNotSupported);

        for host in [
            "",
            " example.invalid",
            "example.invalid ",
            "[example.invalid]",
            "example..invalid",
            "example.invalid..",
            "..",
            "...",
        ] {
            let scheduler = Scheduler::new(1024, Duration::from_secs(3600)).start();
            let input = with_no_auth_greeting(&domain_request(host, 443));

            let (result, response) = exchange(scheduler, default_policy(), input).await;

            result.unwrap();
            assert_eq!(response, expected, "unexpected response for {host:?}");
        }
    }

    #[actix_rt::test]
    async fn rejects_before_policy_resolution_when_scheduler_is_at_capacity() {
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600))
            .with_max_connections(max_connections_from_raw(1))
            .start();
        let queue = QueueActor::new().start();
        scheduler
            .send(Register {
                flow_id: FlowId(42),
                queue_addr: queue,
                backend_write: make_backend_write().await,
                tunnel_lifecycle: TunnelLifecycle::new(),
            })
            .await
            .unwrap()
            .unwrap();

        let request = ipv4_request(SOCKS5_CMD_CONNECT, Ipv4Addr::LOCALHOST, 443);
        let input = with_no_auth_greeting(&request);
        let expected = expected_failure_exchange(Socks5Reply::GeneralFailure);

        let (result, response) = exchange(scheduler, default_policy(), input).await;

        result.unwrap();
        assert_eq!(response, expected);
    }

    #[actix_rt::test]
    async fn times_out_waiting_for_greeting() {
        time::pause();
        let scheduler = Scheduler::new(1024, Duration::from_secs(3600)).start();
        let response_task = tokio::spawn(exchange(scheduler, default_policy(), Vec::new()));

        task::yield_now().await;
        time::advance(SOCKS5_HANDSHAKE_TIMEOUT + Duration::from_millis(1)).await;
        task::yield_now().await;
        let (result, response) = response_task.await.unwrap();

        let error = result.unwrap_err().to_string();
        assert!(error.contains("timed out waiting for SOCKS5 greeting header"));
        assert!(response.is_empty());
    }
}
