use std::{
    collections::{HashMap, hash_map::Entry},
    env,
    net::IpAddr,
    num::NonZeroUsize,
    time::{Duration, Instant},
};

use actix::prelude::*;
use anyhow::{Context, Result, anyhow};
use hickory_resolver::{ResolveError, TokioResolver};
use idna::domain_to_ascii;
use log::debug;
use lru::LruCache;
use tokio::{sync::oneshot, time::timeout};

#[derive(Clone)]
pub(crate) struct DnsCacheConfig {
    pub capacity: NonZeroUsize,
    pub min_ttl: Duration,
    pub max_ttl: Duration,
    pub prefetch_ratio: f32,
    pub prefetch_min_margin: Duration,
    pub stale_grace: Duration,
    pub negative_ttl: Duration,
    pub resolve_timeout: Duration,
    pub prefetch_batch: usize,
    pub ipv6_first: bool,
}

impl Default for DnsCacheConfig {
    fn default() -> Self {
        Self {
            capacity: NonZeroUsize::new(1_024).expect("non zero"),
            min_ttl: Duration::from_secs(5),
            max_ttl: Duration::from_secs(3600),
            // Conservative defaults to avoid opening too many sockets during background refresh.
            prefetch_ratio: 0.1,
            prefetch_min_margin: Duration::from_secs(2),
            stale_grace: Duration::from_secs(45),
            negative_ttl: Duration::from_secs(4),
            resolve_timeout: Duration::from_millis(450),
            prefetch_batch: 3,
            ipv6_first: true,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DnsLookupResult {
    pub ips: Vec<IpAddr>,
    pub cache_hit: bool,
    pub served_stale: bool,
}

#[derive(Message)]
#[rtype(result = "Result<DnsLookupResult, anyhow::Error>")]
pub(crate) struct ResolveHost {
    pub host: String,
}

pub(crate) struct DnsCacheActor {
    resolver: TokioResolver,
    store: LruCache<String, CacheEntry>,
    inflight: HashMap<String, Vec<oneshot::Sender<LookupResponse>>>,
    config: DnsCacheConfig,
    no_proxy: Option<NoProxyMatcher>,
    prefetch_triggered: u64,
    negative_cached: u64,
    active_lookups: u64,
    max_active_lookups: u64,
}

impl DnsCacheActor {
    pub fn new(config: DnsCacheConfig) -> Result<Self> {
        let resolver = build_default_resolver().context("failed to build DNS resolver")?;
        Ok(Self::with_resolver(resolver, config))
    }

    pub fn with_resolver(resolver: TokioResolver, config: DnsCacheConfig) -> Self {
        let store = LruCache::new(config.capacity);
        let no_proxy = NoProxyMatcher::from_env();
        Self {
            resolver,
            store,
            inflight: HashMap::new(),
            config,
            no_proxy,
            prefetch_triggered: 0,
            negative_cached: 0,
            active_lookups: 0,
            max_active_lookups: 0,
        }
    }

    fn normalize_host(&self, host: &str) -> Result<String> {
        let trimmed = host.trim_end_matches('.');
        let ascii = domain_to_ascii(trimmed)
            .with_context(|| format!("failed to normalize host: {}", host))?;
        Ok(ascii.to_lowercase())
    }

    fn should_bypass_cache(&self, host: &str) -> bool {
        self.no_proxy
            .as_ref()
            .map(|matcher| matcher.matches(host))
            .unwrap_or(false)
    }

    fn check_cache(
        &mut self,
        key: &str,
        now: Instant,
        ctx: &mut actix::Context<Self>,
    ) -> CacheCheck {
        let (outcome, should_prefetch) = match self.store.get(key) {
            Some(CacheEntry::Positive(entry)) => {
                if now >= entry.expires_at && now >= entry.stale_until {
                    return CacheCheck::Miss;
                }

                let served_stale = now >= entry.expires_at;
                let should_prefetch = now >= entry.soft_expire_at;

                (
                    CacheCheck::Hit(DnsLookupResult {
                        ips: entry.ips.clone(),
                        cache_hit: true,
                        served_stale,
                    }),
                    should_prefetch,
                )
            }
            Some(CacheEntry::Negative(entry)) => {
                if now >= entry.expires_at {
                    return CacheCheck::Miss;
                }
                (CacheCheck::Negative(entry.message.clone()), false)
            }
            None => return CacheCheck::Miss,
        };

        if should_prefetch {
            self.prefetch(key.to_owned(), ctx);
        }

        outcome
    }

    fn prefetch(&mut self, key: String, ctx: &mut actix::Context<Self>) {
        if self.inflight.contains_key(&key) {
            return;
        }
        self.prefetch_triggered += 1;
        debug!(
            "dns_cache: prefetch queued for {} (total={})",
            key, self.prefetch_triggered
        );
        self.queue_lookup(key, ctx);
    }

    fn queue_lookup(&mut self, key: String, ctx: &mut actix::Context<Self>) {
        if self.inflight.contains_key(&key) {
            return;
        }

        self.inflight.insert(key.clone(), Vec::new());
        self.on_lookup_started();
        let resolver = self.resolver.clone();
        let config = self.config.clone();
        ctx.spawn(
            async move {
                let result = perform_lookup(resolver, key.clone(), config).await;
                (key, result)
            }
            .into_actor(self)
            .map(|(key, result), act, _ctx| {
                act.finish_lookup(key, result, false);
            }),
        );
    }

    fn queue_lookup_with_waiter(
        &mut self,
        key: String,
        ctx: &mut actix::Context<Self>,
    ) -> oneshot::Receiver<LookupResponse> {
        let (tx, rx) = oneshot::channel();
        let spawn_lookup = match self.inflight.entry(key.clone()) {
            Entry::Occupied(mut occupied) => {
                occupied.get_mut().push(tx);
                false
            }
            Entry::Vacant(vacant) => {
                vacant.insert(vec![tx]);
                true
            }
        };

        if spawn_lookup {
            let resolver = self.resolver.clone();
            let config = self.config.clone();
            self.on_lookup_started();
            ctx.spawn(
                async move {
                    let result = perform_lookup(resolver, key.clone(), config).await;
                    (key, result)
                }
                .into_actor(self)
                .map(|(key, result), act, _ctx| {
                    act.finish_lookup(key, result, true);
                }),
            );
        }

        rx
    }

    fn finish_lookup(
        &mut self,
        key: String,
        result: Result<LookupFresh, LookupFailure>,
        notify_waiters: bool,
    ) {
        self.on_lookup_finished();
        let now = Instant::now();

        match result {
            Ok(fresh) => {
                let ttl = clamp_duration(fresh.ttl, self.config.min_ttl, self.config.max_ttl);
                let expires_at = now + ttl;
                let mut margin = duration_ratio(ttl, self.config.prefetch_ratio);
                if margin < self.config.prefetch_min_margin {
                    margin = self.config.prefetch_min_margin;
                }
                if margin >= ttl {
                    margin = ttl / 2;
                }

                let soft_expire_at = expires_at
                    .checked_sub(margin)
                    .unwrap_or(expires_at - (ttl / 2));

                let entry = CacheEntry::Positive(CachePositive {
                    ips: fresh.ips.clone(),
                    expires_at,
                    soft_expire_at,
                    stale_until: expires_at + self.config.stale_grace,
                });

                self.store.put(key.clone(), entry);

                if notify_waiters {
                    if let Some(waiters) = self.inflight.remove(&key) {
                        let response = LookupResponse::Success(DnsLookupResult {
                            ips: fresh.ips,
                            cache_hit: false,
                            served_stale: false,
                        });
                        for sender in waiters {
                            let _ = sender.send(response.clone());
                        }
                    }
                } else {
                    self.inflight.remove(&key);
                }
            }
            Err(failure) => {
                let message = failure.message;
                if notify_waiters {
                    if let Some(waiters) = self.inflight.remove(&key) {
                        let response = LookupResponse::Error(message.clone());
                        for sender in waiters {
                            let _ = sender.send(response.clone());
                        }
                    }
                } else {
                    self.inflight.remove(&key);
                }

                let expires_at = now + self.config.negative_ttl;
                self.negative_cached += 1;
                debug!(
                    "dns_cache: negative cache stored for {} (total={})",
                    key, self.negative_cached
                );
                self.store.put(
                    key,
                    CacheEntry::Negative(CacheNegative {
                        expires_at,
                        message,
                    }),
                );
            }
        }
    }

    fn run_prefetch_pass(&mut self, ctx: &mut actix::Context<Self>) {
        let now = Instant::now();
        let mut scheduled = 0usize;
        let mut pending = Vec::new();

        for (key, entry) in self.store.iter() {
            if scheduled >= self.config.prefetch_batch {
                break;
            }

            if let CacheEntry::Positive(entry) = entry {
                if now >= entry.soft_expire_at && now < entry.stale_until {
                    if !self.inflight.contains_key(key) {
                        pending.push((*key).to_string());
                        scheduled += 1;
                    }
                }
            }
        }

        for key in pending {
            self.prefetch(key, ctx);
        }
    }

    fn on_lookup_started(&mut self) {
        self.active_lookups += 1;
        if self.active_lookups > self.max_active_lookups {
            self.max_active_lookups = self.active_lookups;
            debug!(
                "dns_cache: lookup started (active={} new_max={})",
                self.active_lookups, self.max_active_lookups
            );
        } else {
            debug!("dns_cache: lookup started (active={})", self.active_lookups);
        }
    }

    fn on_lookup_finished(&mut self) {
        if self.active_lookups > 0 {
            self.active_lookups -= 1;
        }
        debug!(
            "dns_cache: lookup finished (active={} max={})",
            self.active_lookups, self.max_active_lookups
        );
    }
}

impl Actor for DnsCacheActor {
    type Context = actix::Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_interval(Duration::from_secs(5), |actor, ctx| {
            actor.run_prefetch_pass(ctx);
        });
    }
}

impl Handler<ResolveHost> for DnsCacheActor {
    type Result = ResponseActFuture<Self, Result<DnsLookupResult>>;

    fn handle(&mut self, msg: ResolveHost, ctx: &mut Self::Context) -> Self::Result {
        let ResolveHost { host, .. } = msg;

        let key = match self.normalize_host(&host) {
            Ok(k) => k,
            Err(e) => return Box::pin(async move { Err(e) }.into_actor(self)),
        };

        if self.should_bypass_cache(&key) {
            let resolver = self.resolver.clone();
            let config = self.config.clone();
            return Box::pin(
                async move {
                    let fresh = perform_lookup(resolver, key, config)
                        .await
                        .map_err(|err| anyhow!(err.message))?;
                    Ok(DnsLookupResult {
                        ips: fresh.ips,
                        cache_hit: false,
                        served_stale: false,
                    })
                }
                .into_actor(self),
            );
        }

        match self.check_cache(&key, Instant::now(), ctx) {
            CacheCheck::Hit(result) => {
                return Box::pin(async move { Ok(result) }.into_actor(self));
            }
            CacheCheck::Negative(message) => {
                return Box::pin(async move { Err(anyhow!(message)) }.into_actor(self));
            }
            CacheCheck::Miss => {}
        }

        let rx = self.queue_lookup_with_waiter(key.clone(), ctx);
        Box::pin(
            async move {
                match rx.await {
                    Ok(LookupResponse::Success(result)) => Ok(result),
                    Ok(LookupResponse::Error(message)) => Err(anyhow!(message)),
                    Err(_) => Err(anyhow!("DNS lookup cancelled")),
                }
            }
            .into_actor(self),
        )
    }
}

#[derive(Clone)]
enum LookupResponse {
    Success(DnsLookupResult),
    Error(String),
}

struct LookupFresh {
    ips: Vec<IpAddr>,
    ttl: Duration,
}

struct LookupFailure {
    message: String,
}

enum CacheEntry {
    Positive(CachePositive),
    Negative(CacheNegative),
}

struct CachePositive {
    ips: Vec<IpAddr>,
    expires_at: Instant,
    soft_expire_at: Instant,
    stale_until: Instant,
}

struct CacheNegative {
    expires_at: Instant,
    message: String,
}

enum CacheCheck {
    Hit(DnsLookupResult),
    Negative(String),
    Miss,
}

struct NoProxyMatcher {
    entries: Vec<String>,
}

impl NoProxyMatcher {
    fn from_env() -> Option<Self> {
        let value = env::var("NO_PROXY")
            .or_else(|_| env::var("no_proxy"))
            .ok()?;
        let entries = value
            .split(',')
            .map(|s| s.trim().to_ascii_lowercase())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();
        if entries.is_empty() {
            None
        } else {
            Some(Self { entries })
        }
    }

    fn matches(&self, host: &str) -> bool {
        let host = host.trim_end_matches('.');
        for entry in &self.entries {
            if entry == "*" {
                return true;
            }
            if host.eq(entry) {
                return true;
            }
            if entry.starts_with('.') {
                if host.ends_with(entry) {
                    return true;
                }
                continue;
            }
            if host.ends_with(entry) {
                let diff = host.len().saturating_sub(entry.len());
                if diff == 0 {
                    return true;
                }
                if host.as_bytes()[diff - 1] == b'.' {
                    return true;
                }
            }
        }
        false
    }
}

async fn perform_lookup(
    resolver: TokioResolver,
    host: String,
    config: DnsCacheConfig,
) -> Result<LookupFresh, LookupFailure> {
    let start = Instant::now();
    let lookup = match timeout(config.resolve_timeout, resolver.lookup_ip(host.clone())).await {
        Ok(Ok(lookup)) => lookup,
        Ok(Err(err)) => {
            return Err(LookupFailure {
                message: format!("DNS lookup failed: {}", err),
            });
        }
        Err(_) => {
            return Err(LookupFailure {
                message: "DNS lookup timeout".to_string(),
            });
        }
    };

    let mut ips_v6 = Vec::new();
    let mut ips_v4 = Vec::new();

    for ip in lookup.iter() {
        match ip {
            IpAddr::V6(addr) => ips_v6.push(IpAddr::V6(addr)),
            IpAddr::V4(addr) => ips_v4.push(IpAddr::V4(addr)),
        }
    }

    if ips_v4.is_empty() && ips_v6.is_empty() {
        return Err(LookupFailure {
            message: "DNS lookup returned no addresses".to_string(),
        });
    }

    let mut combined = Vec::with_capacity(ips_v4.len() + ips_v6.len());
    if config.ipv6_first {
        combined.extend(ips_v6.into_iter());
        combined.extend(ips_v4.into_iter());
    } else {
        combined.extend(ips_v4.into_iter());
        combined.extend(ips_v6.into_iter());
    }

    combined.sort();
    combined.dedup();

    let ttl = lookup
        .valid_until()
        .checked_duration_since(start)
        .unwrap_or(Duration::from_secs(0));

    Ok(LookupFresh { ips: combined, ttl })
}

fn clamp_duration(value: Duration, min: Duration, max: Duration) -> Duration {
    if value < min {
        min
    } else if value > max {
        max
    } else {
        value
    }
}

fn duration_ratio(value: Duration, ratio: f32) -> Duration {
    if ratio <= 0.0 {
        return Duration::from_secs(0);
    }
    let seconds = value.as_secs_f32() * ratio;
    if seconds <= 0.0 {
        Duration::from_secs(0)
    } else {
        Duration::from_secs_f32(seconds)
    }
}

fn build_default_resolver() -> Result<TokioResolver, ResolveError> {
    #[cfg(any(unix, target_os = "windows"))]
    {
        Ok(TokioResolver::builder_tokio()?.build())
    }
    #[cfg(not(any(unix, target_os = "windows")))]
    {
        use hickory_resolver::{config::ResolverConfig, name_server::TokioConnectionProvider};

        let builder = TokioResolver::builder_with_config(
            ResolverConfig::default(),
            TokioConnectionProvider::default(),
        );
        Ok(builder.build())
    }
}
