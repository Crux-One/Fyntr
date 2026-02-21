use std::{
    collections::HashSet,
    fmt, io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    str::FromStr,
};

use anyhow::anyhow;
use tokio::net::lookup_host;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ConnectCidr {
    V4 { network: u32, prefix: u8 },
    V6 { network: u128, prefix: u8 },
}

impl ConnectCidr {
    fn contains(self, ip: IpAddr) -> bool {
        match (self, ip) {
            (Self::V4 { network, prefix }, IpAddr::V4(addr)) => {
                let mask = ipv4_mask(prefix);
                (u32::from(addr) & mask) == network
            }
            (Self::V6 { network, prefix }, IpAddr::V6(addr)) => {
                let mask = ipv6_mask(prefix);
                (u128::from(addr) & mask) == network
            }
            _ => false,
        }
    }
}

impl fmt::Display for ConnectCidr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::V4 { network, prefix } => write!(f, "{}/{}", Ipv4Addr::from(*network), prefix),
            Self::V6 { network, prefix } => write!(f, "{}/{}", Ipv6Addr::from(*network), prefix),
        }
    }
}

impl FromStr for ConnectCidr {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let trimmed = value.trim();
        let (ip_raw, prefix_raw) = trimmed
            .split_once('/')
            .ok_or_else(|| anyhow!("invalid CIDR '{}': expected ip/prefix", value))?;
        let prefix = prefix_raw
            .parse::<u8>()
            .map_err(|_| anyhow!("invalid CIDR '{}': invalid prefix '{}'", value, prefix_raw))?;
        let ip = ip_raw
            .parse::<IpAddr>()
            .map_err(|_| anyhow!("invalid CIDR '{}': invalid IP '{}'", value, ip_raw))?;

        match ip {
            IpAddr::V4(addr) => {
                if prefix > 32 {
                    return Err(anyhow!(
                        "invalid CIDR '{}': IPv4 prefix must be <= 32",
                        value
                    ));
                }
                let mask = ipv4_mask(prefix);
                Ok(Self::V4 {
                    network: u32::from(addr) & mask,
                    prefix,
                })
            }
            IpAddr::V6(addr) => {
                if prefix > 128 {
                    return Err(anyhow!(
                        "invalid CIDR '{}': IPv6 prefix must be <= 128",
                        value
                    ));
                }
                let mask = ipv6_mask(prefix);
                Ok(Self::V6 {
                    network: u128::from(addr) & mask,
                    prefix,
                })
            }
        }
    }
}

fn ipv4_mask(prefix: u8) -> u32 {
    if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix)
    }
}

fn ipv6_mask(prefix: u8) -> u128 {
    if prefix == 0 {
        0
    } else {
        u128::MAX << (128 - prefix)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ConnectPolicyConfig {
    pub(crate) allowed_ports: Vec<u16>,
    pub(crate) include_default_allow_port: bool,
    pub(crate) deny_cidrs: Vec<ConnectCidr>,
    pub(crate) allow_cidrs: Vec<ConnectCidr>,
    pub(crate) allow_domains: Vec<String>,
}

impl Default for ConnectPolicyConfig {
    fn default() -> Self {
        Self {
            allowed_ports: Vec::new(),
            include_default_allow_port: true,
            deny_cidrs: default_denied_cidrs(),
            allow_cidrs: Vec::new(),
            allow_domains: Vec::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ConnectPolicy {
    allowed_ports: HashSet<u16>,
    deny_cidrs: Vec<ConnectCidr>,
    allow_cidrs: Vec<ConnectCidr>,
    allow_domains: Vec<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedConnectTarget {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) addrs: Vec<SocketAddr>,
}

#[derive(Debug)]
pub(crate) enum ConnectPolicyError {
    Denied(String),
    ResolveFailed(String),
}

impl ConnectPolicy {
    pub(crate) fn from_config(config: ConnectPolicyConfig) -> Self {
        let allowed_ports = if config.allowed_ports.is_empty() && config.include_default_allow_port
        {
            HashSet::from([443])
        } else {
            config
                .allowed_ports
                .into_iter()
                .filter(|port| *port != 0)
                .collect()
        };

        let allow_domains = config
            .allow_domains
            .into_iter()
            .map(|domain| normalize_domain(&domain))
            .filter(|domain| !domain.is_empty())
            .collect();

        Self {
            allowed_ports,
            deny_cidrs: config.deny_cidrs,
            allow_cidrs: config.allow_cidrs,
            allow_domains,
        }
    }

    pub(crate) async fn resolve_and_authorize(
        &self,
        host: &str,
        port: u16,
    ) -> Result<ResolvedConnectTarget, ConnectPolicyError> {
        if !self.allowed_ports.contains(&port) {
            return Err(ConnectPolicyError::Denied(format!(
                "port {} is not in allow list",
                port
            )));
        }

        let addrs = resolve_host(host, port)
            .await
            .map_err(|err| ConnectPolicyError::ResolveFailed(err.to_string()))?;
        if addrs.is_empty() {
            return Err(ConnectPolicyError::ResolveFailed(format!(
                "host {} resolved to no addresses",
                host
            )));
        }

        let domain_allowed = self.is_domain_allowed(host);
        for addr in &addrs {
            let ip = addr.ip();
            if self.is_ip_allowed(ip) || domain_allowed {
                continue;
            }
            if let Some(cidr) = self
                .deny_cidrs
                .iter()
                .copied()
                .find(|cidr| cidr.contains(ip))
            {
                return Err(ConnectPolicyError::Denied(format!(
                    "resolved address {} is blocked by {}",
                    ip, cidr
                )));
            }
        }

        Ok(ResolvedConnectTarget {
            host: host.to_string(),
            port,
            addrs,
        })
    }

    fn is_ip_allowed(&self, ip: IpAddr) -> bool {
        self.allow_cidrs
            .iter()
            .copied()
            .any(|cidr| cidr.contains(ip))
    }

    fn is_domain_allowed(&self, host: &str) -> bool {
        let host = normalize_domain(host);
        if host.is_empty() {
            return false;
        }
        self.allow_domains.iter().any(|allowed| {
            host == *allowed
                || (host.len() > allowed.len()
                    && host.ends_with(allowed)
                    && host.as_bytes()[host.len() - allowed.len() - 1] == b'.')
        })
    }
}

fn normalize_domain(value: &str) -> String {
    value.trim().trim_end_matches('.').to_ascii_lowercase()
}

fn default_denied_cidrs() -> Vec<ConnectCidr> {
    // Default blocklist to reduce SSRF blast radius.
    // These ranges cover loopback, RFC1918 private space, link-local, and unspecified addresses.
    // Use allow_cidrs/allow_domains for explicit exceptions when needed.
    [
        "127.0.0.0/8",
        "10.0.0.0/8",
        "172.16.0.0/12",
        "192.168.0.0/16",
        "169.254.0.0/16",
        "100.64.0.0/10",
        "0.0.0.0/8",
        "::1/128",
        "::/128",
        "fc00::/7",
        "fe80::/10",
    ]
    .iter()
    .map(|value| value.parse().expect("default CIDR must parse"))
    .collect()
}

async fn resolve_host(host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(vec![SocketAddr::new(ip, port)]);
    }

    let mut addrs: Vec<SocketAddr> = lookup_host((host, port)).await?.collect();
    if addrs.is_empty() {
        return Ok(addrs);
    }

    addrs.sort_unstable();
    addrs.dedup();
    Ok(addrs)
}

impl fmt::Display for ConnectPolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Denied(reason) => write!(f, "connect target denied: {}", reason),
            Self::ResolveFailed(reason) => write!(f, "connect target resolve failed: {}", reason),
        }
    }
}

impl std::error::Error for ConnectPolicyError {}

impl From<ConnectPolicyError> for io::Error {
    fn from(value: ConnectPolicyError) -> Self {
        match value {
            ConnectPolicyError::Denied(reason) => {
                io::Error::new(io::ErrorKind::PermissionDenied, reason)
            }
            ConnectPolicyError::ResolveFailed(reason) => io::Error::other(reason),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cidr_contains_ipv4() {
        let cidr: ConnectCidr = "192.168.1.0/24".parse().unwrap();
        assert!(cidr.contains(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 5))));
        assert!(!cidr.contains(IpAddr::V4(Ipv4Addr::new(192, 168, 2, 5))));
    }

    #[tokio::test]
    async fn blocks_private_ipv4_by_default() {
        let policy = ConnectPolicy::from_config(ConnectPolicyConfig::default());
        let err = policy
            .resolve_and_authorize("127.0.0.1", 443)
            .await
            .unwrap_err();
        assert!(matches!(err, ConnectPolicyError::Denied(_)));
    }

    #[tokio::test]
    async fn allows_private_ipv4_when_allow_cidr_matches() {
        let mut config = ConnectPolicyConfig::default();
        config.allow_cidrs.push("127.0.0.0/8".parse().unwrap());
        let policy = ConnectPolicy::from_config(config);
        let result = policy.resolve_and_authorize("127.0.0.1", 443).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn blocks_disallowed_port() {
        let policy = ConnectPolicy::from_config(ConnectPolicyConfig::default());
        let err = policy
            .resolve_and_authorize("example.com", 22)
            .await
            .unwrap_err();
        assert!(matches!(err, ConnectPolicyError::Denied(_)));
    }

    #[tokio::test]
    async fn no_default_allow_port_disables_443_implicit_allow() {
        let config = ConnectPolicyConfig {
            include_default_allow_port: false,
            ..ConnectPolicyConfig::default()
        };
        let policy = ConnectPolicy::from_config(config);
        let err = policy
            .resolve_and_authorize("example.com", 443)
            .await
            .unwrap_err();
        assert!(matches!(err, ConnectPolicyError::Denied(_)));
    }

    #[tokio::test]
    async fn port_zero_in_allow_list_is_ignored() {
        let config = ConnectPolicyConfig {
            include_default_allow_port: false,
            allowed_ports: vec![0, 443],
            allow_cidrs: vec!["127.0.0.0/8".parse().unwrap()],
            ..ConnectPolicyConfig::default()
        };
        let policy = ConnectPolicy::from_config(config);

        let allowed = policy.resolve_and_authorize("127.0.0.1", 443).await;
        assert!(allowed.is_ok());

        let denied = policy.resolve_and_authorize("127.0.0.1", 0).await;
        assert!(matches!(denied, Err(ConnectPolicyError::Denied(_))));
    }

    #[tokio::test]
    async fn allow_domain_overrides_deny_cidr() {
        let mut config = ConnectPolicyConfig::default();
        config.allow_domains.push("localhost".to_string());
        let policy = ConnectPolicy::from_config(config);
        let result = policy.resolve_and_authorize("localhost", 443).await;
        assert!(result.is_ok());
    }
}
