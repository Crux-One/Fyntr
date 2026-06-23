use std::{
    fmt,
    net::{IpAddr, SocketAddr},
};

use idna::AsciiDenyList;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct NormalizedDomain(Box<str>);

impl NormalizedDomain {
    pub(crate) fn from_suffix(value: &str) -> Result<Self, HostNormalizeError> {
        let value = value.trim().trim_start_matches('.');
        match normalize_host(value)? {
            NormalizedHost::Domain(domain) => Ok(domain),
            NormalizedHost::Ip(_) => Err(HostNormalizeError::InvalidDomain),
        }
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn into_string(self) -> String {
        self.0.into()
    }

    pub(crate) fn matches_suffix(&self, suffix: &Self) -> bool {
        let host = self.as_str();
        let suffix = suffix.as_str();
        host == suffix
            || (host.len() > suffix.len()
                && host.ends_with(suffix)
                && host.as_bytes()[host.len() - suffix.len() - 1] == b'.')
    }
}

impl fmt::Display for NormalizedDomain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum NormalizedHost {
    Domain(NormalizedDomain),
    Ip(IpAddr),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HostNormalizeError {
    Empty,
    Idna,
    InvalidDomain,
}

impl fmt::Display for HostNormalizeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("host must not be empty"),
            Self::Idna => f.write_str("host could not be converted to IDNA ASCII form"),
            Self::InvalidDomain => f.write_str("host is not a valid domain name"),
        }
    }
}

impl std::error::Error for HostNormalizeError {}

pub(crate) fn normalize_host(value: &str) -> Result<NormalizedHost, HostNormalizeError> {
    let mut host = value.trim();
    if host.is_empty() {
        return Err(HostNormalizeError::Empty);
    }

    if let Some(stripped) = host.strip_prefix('[').and_then(|h| h.strip_suffix(']')) {
        host = stripped;
    }

    let host = host.trim_end_matches('.');
    if host.is_empty() {
        return Err(HostNormalizeError::Empty);
    }

    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(NormalizedHost::Ip(canonicalize_ip(ip)));
    }

    let ascii_host = idna::domain_to_ascii_cow(host.as_bytes(), AsciiDenyList::URL)
        .map_err(|_| HostNormalizeError::Idna)?
        .into_owned();

    if !is_valid_domain(&ascii_host) {
        return Err(HostNormalizeError::InvalidDomain);
    }

    Ok(NormalizedHost::Domain(NormalizedDomain(
        ascii_host.into_boxed_str(),
    )))
}

pub(crate) fn canonicalize_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(addr) => addr.to_ipv4().map_or(IpAddr::V6(addr), IpAddr::V4),
        IpAddr::V4(_) => ip,
    }
}

pub(crate) fn canonicalize_socket_addr(addr: SocketAddr) -> SocketAddr {
    SocketAddr::new(canonicalize_ip(addr.ip()), addr.port())
}

fn is_valid_domain(value: &str) -> bool {
    if value.len() > 253 || value.starts_with('.') || value.ends_with('.') {
        return false;
    }

    value.split('.').all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
    })
}

pub(crate) struct TargetAuthority<'a> {
    host: &'a str,
    port: u16,
}

impl<'a> TargetAuthority<'a> {
    pub(crate) fn new(host: &'a str, port: u16) -> Self {
        Self { host, port }
    }
}

impl fmt::Display for TargetAuthority<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_ascii_domain() {
        let NormalizedHost::Domain(domain) = normalize_host(" EVIL.COM. ").unwrap() else {
            panic!("expected domain");
        };
        assert_eq!(domain.as_str(), "evil.com");
    }

    #[test]
    fn converts_idna_domain_to_ascii() {
        let NormalizedHost::Domain(domain) = normalize_host("bücher.de").unwrap() else {
            panic!("expected domain");
        };
        assert_eq!(domain.as_str(), "xn--bcher-kva.de");
    }

    #[test]
    fn canonicalizes_ipv4_mapped_ipv6() {
        assert_eq!(
            normalize_host("::ffff:1.2.3.4").unwrap(),
            NormalizedHost::Ip("1.2.3.4".parse().unwrap())
        );
    }

    #[test]
    fn normalizes_unicode_domain_suffix_for_matching() {
        let suffix = NormalizedDomain::from_suffix(".BÜCHER.DE.").unwrap();
        let NormalizedHost::Domain(host) = normalize_host("api.bücher.de").unwrap() else {
            panic!("expected domain");
        };

        assert_eq!(suffix.as_str(), "xn--bcher-kva.de");
        assert!(host.matches_suffix(&suffix));
    }

    #[test]
    fn formats_ipv6_authority_with_brackets() {
        assert_eq!(
            TargetAuthority::new("2001:db8::1", 443).to_string(),
            "[2001:db8::1]:443"
        );
    }
}
