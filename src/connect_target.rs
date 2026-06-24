use std::{
    borrow::Cow,
    fmt,
    net::{IpAddr, SocketAddr},
};

use idna::AsciiDenyList;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct NormalizedDomain(Box<str>);

impl NormalizedDomain {
    pub(crate) fn from_suffix(value: &str) -> Result<Self, HostNormalizeError> {
        let value = value.trim().trim_start_matches('.');
        match normalize_lenient_host(value)? {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RequestHostParseError {
    Empty,
    Whitespace,
    MultipleTrailingDots,
    Idna,
    InvalidDomain,
}

impl fmt::Display for RequestHostParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("request host must not be empty"),
            Self::Whitespace => f.write_str("request host must not contain whitespace"),
            Self::MultipleTrailingDots => {
                f.write_str("request host must not contain multiple trailing dots")
            }
            Self::Idna => f.write_str("request host could not be converted to IDNA ASCII form"),
            Self::InvalidDomain => f.write_str("request host is not a valid domain name"),
        }
    }
}

impl std::error::Error for RequestHostParseError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RequestedHost {
    LiteralIp {
        raw: Box<str>,
        canonical: IpAddr,
    },
    Domain {
        raw: Box<str>,
        normalized: NormalizedDomain,
        absolute: bool,
    },
}

impl RequestedHost {
    /// Parses an HTTP CONNECT host where an unqualified IP string denotes a literal address.
    pub(crate) fn parse_inferred(value: &str) -> Result<Self, RequestHostParseError> {
        validate_request_host_syntax(value)?;

        if !value.ends_with('.')
            && let Ok(ip) = value.parse::<IpAddr>()
        {
            return Ok(Self::literal_with_raw(value, ip));
        }

        Self::parse_domain(value)
    }

    /// Parses a protocol field explicitly typed as a domain name, such as SOCKS5 DOMAINNAME.
    pub(crate) fn parse_domain(value: &str) -> Result<Self, RequestHostParseError> {
        validate_request_host_syntax(value)?;
        let absolute = value.ends_with('.');
        let domain = value.strip_suffix('.').unwrap_or(value);
        if domain.ends_with('.') {
            return Err(RequestHostParseError::MultipleTrailingDots);
        }

        let normalized = normalize_domain_exact(domain).map_err(|err| match err {
            HostNormalizeError::Empty => RequestHostParseError::Empty,
            HostNormalizeError::Idna => RequestHostParseError::Idna,
            HostNormalizeError::InvalidDomain => RequestHostParseError::InvalidDomain,
        })?;

        Ok(Self::Domain {
            raw: value.into(),
            normalized,
            absolute,
        })
    }

    pub(crate) fn from_ip(ip: IpAddr) -> Self {
        let raw = ip.to_string();
        Self::literal_with_raw(&raw, ip)
    }

    pub(crate) fn from_ip_with_raw(raw: &str, ip: IpAddr) -> Self {
        Self::literal_with_raw(raw, ip)
    }

    fn literal_with_raw(raw: &str, ip: IpAddr) -> Self {
        Self::LiteralIp {
            raw: raw.into(),
            canonical: canonicalize_ip(ip),
        }
    }

    pub(crate) fn raw(&self) -> &str {
        match self {
            Self::LiteralIp { raw, .. } | Self::Domain { raw, .. } => raw,
        }
    }

    pub(crate) fn normalized_domain(&self) -> Option<&NormalizedDomain> {
        match self {
            Self::Domain { normalized, .. } => Some(normalized),
            Self::LiteralIp { .. } => None,
        }
    }

    pub(crate) fn canonical_ip(&self) -> Option<IpAddr> {
        match self {
            Self::LiteralIp { canonical, .. } => Some(*canonical),
            Self::Domain { .. } => None,
        }
    }

    pub(crate) fn dns_query_name(&self) -> Option<Cow<'_, str>> {
        match self {
            Self::LiteralIp { .. } => None,
            Self::Domain {
                normalized,
                absolute,
                ..
            } if *absolute => Some(Cow::Owned(format!("{}.", normalized.as_str()))),
            Self::Domain { normalized, .. } => Some(Cow::Borrowed(normalized.as_str())),
        }
    }
}

impl fmt::Display for RequestedHost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.raw())
    }
}

fn validate_request_host_syntax(value: &str) -> Result<(), RequestHostParseError> {
    if value.is_empty() {
        return Err(RequestHostParseError::Empty);
    }
    if value.chars().any(char::is_whitespace) {
        return Err(RequestHostParseError::Whitespace);
    }
    Ok(())
}

pub(crate) fn normalize_lenient_host(value: &str) -> Result<NormalizedHost, HostNormalizeError> {
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

    normalize_domain_exact(host).map(NormalizedHost::Domain)
}

fn normalize_domain_exact(value: &str) -> Result<NormalizedDomain, HostNormalizeError> {
    if value.is_empty() {
        return Err(HostNormalizeError::Empty);
    }

    let ascii_host = idna::domain_to_ascii_cow(value.as_bytes(), AsciiDenyList::URL)
        .map_err(|_| HostNormalizeError::Idna)?
        .into_owned();

    if !is_valid_domain(&ascii_host) {
        return Err(HostNormalizeError::InvalidDomain);
    }

    Ok(NormalizedDomain(ascii_host.into_boxed_str()))
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
        let NormalizedHost::Domain(domain) = normalize_lenient_host(" EVIL.COM. ").unwrap() else {
            panic!("expected domain");
        };
        assert_eq!(domain.as_str(), "evil.com");
    }

    #[test]
    fn lenient_normalizer_preserves_configuration_and_feed_syntax() {
        let NormalizedHost::Domain(bracketed) = normalize_lenient_host(" [EVIL.COM] ").unwrap()
        else {
            panic!("expected domain");
        };
        let NormalizedHost::Domain(multiple_trailing_dots) =
            normalize_lenient_host(" EVIL.COM... ").unwrap()
        else {
            panic!("expected domain");
        };

        assert_eq!(bracketed.as_str(), "evil.com");
        assert_eq!(multiple_trailing_dots.as_str(), "evil.com");
    }

    #[test]
    fn converts_idna_domain_to_ascii() {
        let NormalizedHost::Domain(domain) = normalize_lenient_host("bücher.de").unwrap() else {
            panic!("expected domain");
        };
        assert_eq!(domain.as_str(), "xn--bcher-kva.de");
    }

    #[test]
    fn canonicalizes_ipv4_mapped_ipv6() {
        assert_eq!(
            normalize_lenient_host("::ffff:1.2.3.4").unwrap(),
            NormalizedHost::Ip("1.2.3.4".parse().unwrap())
        );
    }

    #[test]
    fn normalizes_unicode_domain_suffix_for_matching() {
        let suffix = NormalizedDomain::from_suffix(".BÜCHER.DE.").unwrap();
        let NormalizedHost::Domain(host) = normalize_lenient_host("api.bücher.de").unwrap() else {
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

    #[test]
    fn request_parser_infers_plain_ip_literal() {
        let host = RequestedHost::parse_inferred("203.0.113.10").unwrap();

        assert_eq!(host.raw(), "203.0.113.10");
        assert_eq!(host.canonical_ip(), Some("203.0.113.10".parse().unwrap()));
        assert_eq!(host.dns_query_name(), None);
    }

    #[test]
    fn parsed_ip_preserves_original_text() {
        let ip = "2001:db8::1".parse().unwrap();
        let host = RequestedHost::from_ip_with_raw("2001:0db8:0:0:0:0:0:1", ip);

        assert_eq!(host.raw(), "2001:0db8:0:0:0:0:0:1");
        assert_eq!(host.canonical_ip(), Some(ip));
    }

    #[test]
    fn request_parser_preserves_absolute_numeric_domain() {
        let host = RequestedHost::parse_inferred("203.0.113.10.").unwrap();

        assert_eq!(host.canonical_ip(), None);
        assert_eq!(
            host.dns_query_name(),
            Some(Cow::Owned("203.0.113.10.".to_string()))
        );
    }

    #[test]
    fn explicit_domain_parser_does_not_reclassify_numeric_name_as_ip() {
        let host = RequestedHost::parse_domain("203.0.113.10").unwrap();

        assert_eq!(host.canonical_ip(), None);
        assert_eq!(host.dns_query_name(), Some(Cow::Borrowed("203.0.113.10")));
    }

    #[test]
    fn request_parser_preserves_absolute_idna_domain() {
        let host = RequestedHost::parse_domain("BÜCHER.DE.").unwrap();

        assert_eq!(
            host.dns_query_name(),
            Some(Cow::Owned("xn--bcher-kva.de.".to_string()))
        );
    }

    #[test]
    fn request_parser_rejects_multiple_trailing_dots() {
        assert_eq!(
            RequestedHost::parse_domain("example.com..").unwrap_err(),
            RequestHostParseError::MultipleTrailingDots
        );
    }

    #[test]
    fn request_parser_rejects_whitespace_instead_of_trimming() {
        assert_eq!(
            RequestedHost::parse_domain(" example.com ").unwrap_err(),
            RequestHostParseError::Whitespace
        );
    }

    #[test]
    fn request_parser_rejects_bracketed_domain() {
        assert_eq!(
            RequestedHost::parse_domain("[example.com]").unwrap_err(),
            RequestHostParseError::Idna
        );
    }
}
