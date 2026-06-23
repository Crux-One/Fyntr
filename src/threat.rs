use std::{
    collections::HashSet,
    fmt, fs,
    net::{IpAddr, SocketAddr},
    path::Path,
    str::FromStr,
};

use anyhow::{Context, Result, anyhow};
use log::{info, warn};
use unicode_script::{Script, UnicodeScript};

use crate::{
    connect_target::{NormalizedHost, canonicalize_ip, normalize_host},
    flow::FlowId,
};

#[derive(Clone, Debug)]
pub(crate) struct ThreatIndex {
    domains: HashSet<Box<str>>,
    ips: HashSet<IpAddr>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ThreatMatch {
    Domain {
        raw_host: String,
        ascii_host: String,
        matched_domain: Box<str>,
    },
    Ip {
        raw_host: String,
        matched_ip: IpAddr,
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

pub(crate) fn log_threat_match_for_target(
    log_target: &'static str,
    flow_id: FlowId,
    action: ThreatAction,
    target_field: &str,
    target_authority: &impl fmt::Display,
    client_addr: SocketAddr,
    threat_match: &ThreatMatch,
) {
    match threat_match {
        ThreatMatch::Domain {
            raw_host,
            ascii_host,
            matched_domain,
        } => warn!(
            target: log_target,
            "flow{}: threat_match=true action={} match_type=domain raw_host={} ascii_host={} matched_domain={} {}={} client_addr={}",
            flow_id.0,
            action.as_str(),
            raw_host,
            ascii_host,
            matched_domain,
            target_field,
            target_authority,
            client_addr
        ),
        ThreatMatch::Ip {
            raw_host,
            matched_ip,
        } => warn!(
            target: log_target,
            "flow{}: threat_match=true action={} match_type=ip raw_host={} matched_ip={} {}={} client_addr={}",
            flow_id.0,
            action.as_str(),
            raw_host,
            matched_ip,
            target_field,
            target_authority,
            client_addr
        ),
    }
}

pub(crate) fn log_threat_matches_for_target(
    log_target: &'static str,
    flow_id: FlowId,
    action: ThreatAction,
    target_field: &str,
    target_authority: &impl fmt::Display,
    client_addr: SocketAddr,
    threat_matches: &[ThreatMatch],
) {
    let should_block = matches!(action, ThreatAction::Block);
    let log_count = if should_block {
        1
    } else {
        threat_matches.len()
    };

    for threat_match in threat_matches.iter().take(log_count) {
        log_threat_match_for_target(
            log_target,
            flow_id,
            action,
            target_field,
            target_authority,
            client_addr,
            threat_match,
        );
    }
}

pub(crate) fn log_mixed_script_host_for_target(
    log_target: &'static str,
    flow_id: FlowId,
    raw_host: &str,
    target_field: &str,
    target_authority: &impl fmt::Display,
    client_addr: SocketAddr,
) {
    if raw_host.parse::<IpAddr>().is_ok() || !has_mixed_scripts(raw_host) {
        return;
    }

    let Ok(NormalizedHost::Domain(ascii_host)) = normalize_host(raw_host) else {
        return;
    };

    warn!(
        target: log_target,
        "flow{}: mixed_script=true reason=mixed_script_host raw_host={} ascii_host={} {}={} client_addr={}",
        flow_id.0, raw_host, ascii_host, target_field, target_authority, client_addr
    );
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ThreatFeedStats {
    pub(crate) domains: usize,
    pub(crate) ips: usize,
    pub(crate) skipped: usize,
}

impl ThreatIndex {
    pub(crate) fn from_feed_files<P>(paths: &[P]) -> Result<Self>
    where
        P: AsRef<Path>,
    {
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

        let stats = ThreatFeedStats {
            domains: domains.len(),
            ips: ips.len(),
            skipped,
        };

        Ok((Self { domains, ips }, stats))
    }

    pub(crate) fn lookup_host(&self, host: &str) -> Option<ThreatMatch> {
        let ascii_host = match normalize_host(host).ok()? {
            NormalizedHost::Domain(domain) => domain.into_string(),
            NormalizedHost::Ip(ip) => return self.lookup_ip(host, ip),
        };

        let mut candidate = ascii_host.as_str();
        loop {
            if let Some(matched_domain) = self.domains.get(candidate) {
                return Some(ThreatMatch::Domain {
                    raw_host: host.to_string(),
                    ascii_host,
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

    pub(crate) fn lookup_ip(&self, host: &str, ip: IpAddr) -> Option<ThreatMatch> {
        let ip = canonicalize_ip(ip);
        self.ips.contains(&ip).then_some(ThreatMatch::Ip {
            raw_host: host.to_string(),
            matched_ip: ip,
        })
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
    match normalize_host(value).ok()? {
        NormalizedHost::Domain(domain) => Some(NormalizedEntry::Domain(
            domain.into_string().into_boxed_str(),
        )),
        NormalizedHost::Ip(ip) => Some(NormalizedEntry::Ip(ip)),
    }
}

pub(crate) fn has_mixed_scripts(host: &str) -> bool {
    if host.is_ascii() {
        return false;
    }

    host.split('.').any(label_has_mixed_strong_scripts)
}

fn label_has_mixed_strong_scripts(label: &str) -> bool {
    let mut scripts = 0u8;

    for ch in label.chars() {
        match ch.script() {
            Script::Latin => scripts |= 0b001,
            Script::Cyrillic => scripts |= 0b010,
            Script::Greek => scripts |= 0b100,
            _ => {}
        }

        if scripts.count_ones() > 1 {
            return true;
        }
    }

    false
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
                raw_host,
                ascii_host,
                matched_domain,
            } => write!(
                f,
                "host {} normalized to {} matched threat domain {}",
                raw_host, ascii_host, matched_domain
            ),
            Self::Ip {
                raw_host,
                matched_ip,
            } => write!(f, "host {} matched threat IP {}", raw_host, matched_ip),
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
                raw_host: "login.evil.com".to_string(),
                ascii_host: "login.evil.com".to_string(),
                matched_domain: "evil.com".into()
            })
        );
        assert_eq!(
            index.lookup_host("1.10.209.143"),
            Some(ThreatMatch::Ip {
                raw_host: "1.10.209.143".to_string(),
                matched_ip: "1.10.209.143".parse().unwrap()
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
    fn parses_feed_content_with_no_supported_entries() {
        let (index, stats) = ThreatIndex::from_feed_content("! comments only\n# none\n").unwrap();

        assert_eq!(stats.domains, 0);
        assert_eq!(stats.ips, 0);
        assert_eq!(stats.skipped, 0);
        assert!(index.lookup_host("example.com").is_none());
    }

    #[test]
    fn normalizes_domains_for_suffix_matching() {
        let index: ThreatIndex = "||Evil.COM.^".parse().unwrap();

        assert_eq!(
            index.lookup_host("API.EVIL.COM."),
            Some(ThreatMatch::Domain {
                raw_host: "API.EVIL.COM.".to_string(),
                ascii_host: "api.evil.com".to_string(),
                matched_domain: "evil.com".into(),
            })
        );
        assert!(index.lookup_host("notevil.com").is_none());
    }

    #[test]
    fn normalizes_idna_feed_entries_before_insertion() {
        let (index, stats) = ThreatIndex::from_feed_content("||bücher.de^\n").unwrap();

        assert_eq!(stats.domains, 1);
        assert!(index.domains.contains("xn--bcher-kva.de"));
        assert!(!index.domains.contains("bücher.de"));
    }

    #[test]
    fn normalizes_idna_request_hosts_before_lookup() {
        let index: ThreatIndex = "||bücher.de^".parse().unwrap();

        assert_eq!(
            index.lookup_host("BÜCHER.DE."),
            Some(ThreatMatch::Domain {
                raw_host: "BÜCHER.DE.".to_string(),
                ascii_host: "xn--bcher-kva.de".to_string(),
                matched_domain: "xn--bcher-kva.de".into(),
            })
        );
    }

    #[test]
    fn suffix_matching_uses_ascii_slices() {
        let index: ThreatIndex = "||bücher.de^".parse().unwrap();

        assert_eq!(
            index.lookup_host("api.login.bücher.de"),
            Some(ThreatMatch::Domain {
                raw_host: "api.login.bücher.de".to_string(),
                ascii_host: "api.login.xn--bcher-kva.de".to_string(),
                matched_domain: "xn--bcher-kva.de".into(),
            })
        );
    }

    #[test]
    fn invalid_feed_domain_is_skipped() {
        let (index, stats) =
            ThreatIndex::from_feed_content("||bad host.com^\n||evil.com^\n").unwrap();

        assert_eq!(stats.domains, 1);
        assert_eq!(stats.skipped, 1);
        assert!(index.lookup_host("evil.com").is_some());
        assert!(index.lookup_host("bad host.com").is_none());
    }

    #[test]
    fn invalid_request_host_does_not_match() {
        let index: ThreatIndex = "||evil.com^".parse().unwrap();

        assert!(index.lookup_host("bad host.com").is_none());
    }

    #[test]
    fn detects_latin_cyrillic_mixed_script_label() {
        assert!(has_mixed_scripts("fаke.invalid"));
        assert!(has_mixed_scripts("fаkе.invalid"));
    }

    #[test]
    fn ordinary_ascii_host_is_not_mixed_script() {
        assert!(!has_mixed_scripts("example.com"));
    }

    #[test]
    fn latin_non_ascii_host_is_not_mixed_script() {
        assert!(!has_mixed_scripts("bücher.de"));
    }

    #[test]
    fn ignored_scripts_do_not_create_mixed_script_warning() {
        assert!(!has_mixed_scripts("日本語.invalid"));
        assert!(!has_mixed_scripts("日本abc.invalid"));
        assert!(!has_mixed_scripts("東京-example.jp"));
    }

    #[test]
    fn strong_scripts_in_separate_labels_are_not_mixed_script() {
        assert!(!has_mixed_scripts("жд.invalid"));
    }

    #[test]
    fn detects_latin_greek_mixed_script_label() {
        assert!(has_mixed_scripts("fαke.invalid"));
    }

    #[test]
    fn canonicalizes_ipv4_mapped_ipv6_for_ip_matching() {
        let index: ThreatIndex = "||1.2.3.4^".parse().unwrap();

        assert_eq!(
            index.lookup_host("::ffff:1.2.3.4"),
            Some(ThreatMatch::Ip {
                raw_host: "::ffff:1.2.3.4".to_string(),
                matched_ip: "1.2.3.4".parse().unwrap()
            })
        );
    }

    #[test]
    fn merges_multiple_feed_files() {
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("first.txt");
        let second = dir.path().join("second.txt");
        fs::write(&first, "||evil.com^\n").unwrap();
        fs::write(&second, "||1.2.3.4^\n").unwrap();

        let index = ThreatIndex::from_feed_files(&[&first, &second]).unwrap();

        assert!(index.lookup_host("api.evil.com").is_some());
        assert!(index.lookup_host("1.2.3.4").is_some());
    }

    #[test]
    fn merges_multiple_feed_files_when_one_file_has_no_supported_entries() {
        let dir = tempfile::tempdir().unwrap();
        let empty = dir.path().join("empty.txt");
        let valid = dir.path().join("valid.txt");
        fs::write(&empty, "! comments only\n# none\n").unwrap();
        fs::write(&valid, "||evil.com^\n").unwrap();

        let index = ThreatIndex::from_feed_files(&[&empty, &valid]).unwrap();

        assert!(index.lookup_host("api.evil.com").is_some());
    }

    #[test]
    fn rejects_feed_files_when_merged_index_has_no_supported_entries() {
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("unsupported-first.txt");
        let second = dir.path().join("unsupported-second.txt");
        fs::write(&first, "! comments only\n").unwrap();
        fs::write(&second, "||evil.com^$important\n").unwrap();

        let err = ThreatIndex::from_feed_files(&[&first, &second]).unwrap_err();

        assert!(err.to_string().contains("no supported entries"));
    }
}
