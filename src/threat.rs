use std::{collections::HashSet, fmt, fs, net::IpAddr, path::Path, str::FromStr};

use anyhow::{Context, Result, anyhow};
use log::{info, warn};

#[derive(Clone, Debug)]
pub(crate) struct ThreatIndex {
    domains: HashSet<Box<str>>,
    ips: HashSet<IpAddr>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ThreatMatch {
    Domain {
        host: String,
        matched_domain: Box<str>,
    },
    Ip {
        host: String,
        ip: IpAddr,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ThreatAction {
    Warn,
    Block,
}

impl ThreatAction {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Warn => "warn",
            Self::Block => "block",
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ThreatFeedStats {
    pub(crate) domains: usize,
    pub(crate) ips: usize,
    pub(crate) skipped: usize,
}

impl ThreatIndex {
    pub(crate) fn from_feed_files(paths: &[impl AsRef<Path>]) -> Result<Self> {
        let mut merged = Self {
            domains: HashSet::new(),
            ips: HashSet::new(),
        };
        let mut total_skipped = 0usize;

        for path in paths {
            let path = path.as_ref();
            let content = fs::read_to_string(path)
                .with_context(|| format!("failed to read threat feed file {}", path.display()))?;
            let (index, stats) = Self::from_feed_content(&content)?;

            total_skipped += stats.skipped;
            info!(
                "loaded threat feed file={} domains={} ips={} skipped={}",
                path.display(),
                stats.domains,
                stats.ips,
                stats.skipped
            );
            if stats.skipped > 0 {
                warn!(
                    "skipped unsupported threat feed rules file={} skipped={}",
                    path.display(),
                    stats.skipped
                );
            }

            merged.domains.extend(index.domains);
            merged.ips.extend(index.ips);
        }

        if merged.domains.is_empty() && merged.ips.is_empty() {
            return Err(anyhow!("threat feeds contain no supported entries"));
        }

        info!(
            "built threat feed index files={} domains={} ips={} skipped={}",
            paths.len(),
            merged.domains.len(),
            merged.ips.len(),
            total_skipped
        );

        Ok(merged)
    }

    pub(crate) fn from_feed_content(content: &str) -> Result<(Self, ThreatFeedStats)> {
        let mut domains = HashSet::new();
        let mut ips = HashSet::new();
        let mut skipped = 0usize;

        for line in content.lines() {
            match parse_feed_line(line) {
                FeedLine::Entry(entry) => match normalize_entry(entry) {
                    Some(NormalizedEntry::Domain(domain)) => {
                        domains.insert(domain);
                    }
                    Some(NormalizedEntry::Ip(ip)) => {
                        ips.insert(ip);
                    }
                    None => skipped += 1,
                },
                FeedLine::Skip => skipped += 1,
                FeedLine::Ignore => {}
            }
        }

        if domains.is_empty() && ips.is_empty() {
            return Err(anyhow!("threat feed contains no supported entries"));
        }

        let stats = ThreatFeedStats {
            domains: domains.len(),
            ips: ips.len(),
            skipped,
        };

        Ok((Self { domains, ips }, stats))
    }

    pub(crate) fn lookup_host(&self, host: &str) -> Option<ThreatMatch> {
        let normalized = normalize_request_host(host)?;
        if let Ok(ip) = normalized.parse::<IpAddr>() {
            let ip = canonicalize_ip(ip);
            return self.ips.contains(&ip).then_some(ThreatMatch::Ip {
                host: normalized,
                ip,
            });
        }

        let mut candidate = normalized.as_str();
        loop {
            if let Some(matched_domain) = self.domains.get(candidate) {
                return Some(ThreatMatch::Domain {
                    host: normalized,
                    matched_domain: matched_domain.clone(),
                });
            }

            let Some(dot_index) = candidate.find('.') else {
                break;
            };
            candidate = &candidate[dot_index + 1..];
        }

        None
    }
}

enum FeedLine<'a> {
    Entry(&'a str),
    Skip,
    Ignore,
}

enum NormalizedEntry {
    Domain(Box<str>),
    Ip(IpAddr),
}

fn parse_feed_line(line: &str) -> FeedLine<'_> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('!') || trimmed.starts_with('#') {
        return FeedLine::Ignore;
    }

    if trimmed.starts_with("@@") || trimmed.contains('$') {
        return FeedLine::Skip;
    }

    if let Some(rest) = trimmed.strip_prefix("||") {
        let Some((host, suffix)) = rest.split_once('^') else {
            return FeedLine::Skip;
        };
        if !suffix.is_empty() {
            return FeedLine::Skip;
        }
        return FeedLine::Entry(host);
    }

    if trimmed.contains('/') || trimmed.contains('*') || trimmed.contains('^') {
        return FeedLine::Skip;
    }

    FeedLine::Entry(trimmed)
}

fn normalize_entry(value: &str) -> Option<NormalizedEntry> {
    let normalized = normalize_request_host(value)?;
    if let Ok(ip) = normalized.parse::<IpAddr>() {
        return Some(NormalizedEntry::Ip(canonicalize_ip(ip)));
    }

    is_valid_domain(&normalized).then(|| NormalizedEntry::Domain(normalized.into_boxed_str()))
}

fn normalize_request_host(value: &str) -> Option<String> {
    let mut host = value.trim();
    if host.is_empty() {
        return None;
    }

    if let Some(stripped) = host.strip_prefix('[').and_then(|h| h.strip_suffix(']')) {
        host = stripped;
    }

    let normalized = host.trim_end_matches('.').to_ascii_lowercase();
    (!normalized.is_empty()).then_some(normalized)
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

fn canonicalize_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(addr) => addr.to_ipv4().map_or(IpAddr::V6(addr), IpAddr::V4),
        IpAddr::V4(_) => ip,
    }
}

impl FromStr for ThreatIndex {
    type Err = anyhow::Error;

    fn from_str(content: &str) -> Result<Self, Self::Err> {
        Self::from_feed_content(content).map(|(index, _)| index)
    }
}

impl fmt::Display for ThreatMatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Domain {
                host,
                matched_domain,
            } => write!(f, "host {} matched threat domain {}", host, matched_domain),
            Self::Ip { host, ip } => write!(f, "host {} matched threat IP {}", host, ip),
        }
    }
}

impl ThreatMatch {
    pub(crate) fn match_type(&self) -> &'static str {
        match self {
            Self::Domain { .. } => "domain",
            Self::Ip { .. } => "ip",
        }
    }

    pub(crate) fn host(&self) -> Option<&str> {
        match self {
            Self::Domain { host, .. } => Some(host),
            Self::Ip { host, .. } => Some(host),
        }
    }

    pub(crate) fn matched(&self) -> String {
        match self {
            Self::Domain { matched_domain, .. } => matched_domain.to_string(),
            Self::Ip { ip, .. } => ip.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_adguard_domain_and_ip_entries() {
        let feed = "\
! Title: Malicious URL Blocklist
||evil.com^
||api.bad.example^
||1.10.209.143^
# local comment
";

        let (index, stats) = ThreatIndex::from_feed_content(feed).unwrap();

        assert_eq!(stats.domains, 2);
        assert_eq!(stats.ips, 1);
        assert_eq!(stats.skipped, 0);
        assert_eq!(
            index.lookup_host("login.evil.com"),
            Some(ThreatMatch::Domain {
                host: "login.evil.com".to_string(),
                matched_domain: "evil.com".into()
            })
        );
        assert_eq!(
            index.lookup_host("1.10.209.143"),
            Some(ThreatMatch::Ip {
                host: "1.10.209.143".to_string(),
                ip: "1.10.209.143".parse().unwrap()
            })
        );
    }

    #[test]
    fn skips_unsupported_rules_without_failing() {
        let feed = "\
||evil.com^$important
@@||allowed.example^
/regex/
||valid.example^
";

        let (index, stats) = ThreatIndex::from_feed_content(feed).unwrap();

        assert_eq!(stats.domains, 1);
        assert_eq!(stats.ips, 0);
        assert_eq!(stats.skipped, 3);
        assert!(index.lookup_host("api.valid.example").is_some());
    }

    #[test]
    fn rejects_feed_with_no_supported_entries() {
        let err = ThreatIndex::from_feed_content("! comments only\n# none\n").unwrap_err();
        assert!(err.to_string().contains("no supported entries"));
    }

    #[test]
    fn normalizes_domains_for_suffix_matching() {
        let index: ThreatIndex = "||Evil.COM.^".parse().unwrap();

        assert!(index.lookup_host("API.EVIL.COM.").is_some());
        assert!(index.lookup_host("notevil.com").is_none());
    }

    #[test]
    fn canonicalizes_ipv4_mapped_ipv6_for_ip_matching() {
        let index: ThreatIndex = "||1.2.3.4^".parse().unwrap();

        assert_eq!(
            index.lookup_host("::ffff:1.2.3.4"),
            Some(ThreatMatch::Ip {
                host: "::ffff:1.2.3.4".to_string(),
                ip: "1.2.3.4".parse().unwrap()
            })
        );
    }

    #[test]
    fn merges_multiple_feed_files() {
        let dir = std::env::temp_dir();
        let first = dir.join(format!("fyntr-threat-feed-first-{}", std::process::id()));
        let second = dir.join(format!("fyntr-threat-feed-second-{}", std::process::id()));
        fs::write(&first, "||evil.com^\n").unwrap();
        fs::write(&second, "||1.2.3.4^\n").unwrap();

        let index = ThreatIndex::from_feed_files(&[&first, &second]).unwrap();

        assert!(index.lookup_host("api.evil.com").is_some());
        assert!(index.lookup_host("1.2.3.4").is_some());

        let _ = fs::remove_file(first);
        let _ = fs::remove_file(second);
    }
}
