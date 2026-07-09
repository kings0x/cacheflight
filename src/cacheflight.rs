use crate::{
    CacheBackend, CacheMissReason, Error, MetricsHooks, NoopMetrics, RecomputeOutcome,
    RecomputeReason, Result,
};
use std::{
    collections::HashMap,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, watch};

// ── Type-state markers ──────────────────────────────────────────────────────

/// Zero-sized marker: no expiry strategy has been configured yet.
pub struct NoExpiry;

/// Zero-sized marker: a flat TTL has been configured via `.ttl()`.
pub struct HasFlatExpiry;

/// Zero-sized marker: stale-while-revalidate has been configured.
pub struct HasSwrExpiry;

/// Zero-sized marker: probabilistic early expiration is not enabled.
pub struct NoXfetch;

/// Zero-sized marker: probabilistic early expiration is enabled.
pub struct HasXfetch;

// ── Entry wire format ───────────────────────────────────────────────────────

const ENTRY_MAGIC: &[u8; 4] = b"SFG1";
const ENTRY_HEADER_LEN: usize = 28;

// ── Flight coordination ─────────────────────────────────────────────────────

type SharedFlightResult = Result<Arc<Vec<u8>>>;

struct Flight {
    notifier: watch::Sender<Option<SharedFlightResult>>,
}

impl Flight {
    fn new() -> Self {
        let (notifier, _) = watch::channel(None);
        Self { notifier }
    }
}

enum CachedEntryState<'a> {
    Fresh(&'a [u8], f64),
    Stale(&'a [u8], f64),
    Expired,
    Invalid,
}

// ── Expiry configuration (internal) ─────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
enum ExpiryStrategy {
    Flat {
        ttl: Duration,
    },
    Swr {
        fresh_ttl: Duration,
        stale_ttl: Duration,
    },
}

impl ExpiryStrategy {
    fn fresh_ttl(&self) -> Duration {
        match self {
            Self::Flat { ttl } => *ttl,
            Self::Swr { fresh_ttl, .. } => *fresh_ttl,
        }
    }

    fn stale_ttl(&self) -> Duration {
        match self {
            Self::Flat { .. } => Duration::ZERO,
            Self::Swr { stale_ttl, .. } => *stale_ttl,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct EntryConfig {
    expiry: ExpiryStrategy,
    beta: Option<f64>,
}

// ── Public return types ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum LookupState {
    CacheHit,
    Stale,
    Recomputed,
    Shared,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupResult {
    value: Vec<u8>,
    state: LookupState,
}

impl LookupResult {
    fn new(value: Vec<u8>, state: LookupState) -> Self {
        Self { value, state }
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }

    pub fn into_value(self) -> Vec<u8> {
        self.value
    }

    pub fn state(&self) -> LookupState {
        self.state
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComputeValue {
    value: Vec<u8>,
    cache: bool,
}

impl ComputeValue {
    pub fn cache(value: Vec<u8>) -> Self {
        Self { value, cache: true }
    }

    pub fn do_not_cache(value: Vec<u8>) -> Self {
        Self {
            value,
            cache: false,
        }
    }

    fn into_parts(self) -> (Vec<u8>, bool) {
        (self.value, self.cache)
    }
}

// ── RunBuilder ──────────────────────────────────────────────────────────────

/// Per-call builder returned by [`CacheFlight::run`].
///
/// Chain override methods before `.await` — or just await directly to use
/// the instance-level defaults.
pub struct RunBuilder<'a, B, E, X, F> {
    cf: &'a CacheFlight<B, E, X>,
    key: String,
    work: F,
    fresh_override: Option<Duration>,
    stale_override: Option<Duration>,
    beta_override: Option<f64>,
}

impl<'a, B, X, F> RunBuilder<'a, B, HasFlatExpiry, X, F> {
    pub fn ttl(mut self, duration: Duration) -> Self {
        self.fresh_override = Some(duration);
        self
    }
}

impl<'a, B, X, F> RunBuilder<'a, B, HasSwrExpiry, X, F> {
    pub fn fresh_for(mut self, duration: Duration) -> Self {
        self.fresh_override = Some(duration);
        self
    }

    pub fn stale_for(mut self, duration: Duration) -> Self {
        self.stale_override = Some(duration);
        self
    }

    pub fn stale_while_revalidate(mut self, fresh: Duration, stale: Duration) -> Self {
        self.fresh_override = Some(fresh);
        self.stale_override = Some(stale);
        self
    }
}

impl<'a, B, E, F> RunBuilder<'a, B, E, HasXfetch, F> {
    pub fn beta(mut self, beta: f64) -> Self {
        self.beta_override = Some(beta);
        self
    }
}

impl<'a, B, E, X, F, Fut> IntoFuture for RunBuilder<'a, B, E, X, F>
where
    B: CacheBackend + 'static,
    E: Send + Sync + 'static,
    X: Send + Sync + 'static,
    F: Fn() -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<Vec<u8>>> + Send + 'static,
{
    type Output = Result<LookupResult>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let RunBuilder {
                cf,
                key,
                work,
                fresh_override,
                stale_override,
                beta_override,
            } = self;

            let effective_fresh = fresh_override.unwrap_or_else(|| cf.config.expiry.fresh_ttl());
            let effective_stale = stale_override.unwrap_or_else(|| cf.config.expiry.stale_ttl());
            let effective_beta = beta_override.or(cf.config.beta);
            let total = effective_fresh
                .checked_add(effective_stale)
                .unwrap_or(Duration::MAX);

            let wrapped = {
                let work = work.clone();
                move || {
                    let work = work.clone();
                    async move { work().await.map(ComputeValue::cache) }
                }
            };

            match cf.cache.get(&key).await {
                Ok(Some(cached)) => match classify_cached_entry(&cached, now_millis()) {
                    CachedEntryState::Fresh(payload, delta_ema) => {
                        if let Some(beta) = effective_beta {
                            let fresh_until_millis =
                                u64::from_be_bytes(cached[4..12].try_into().unwrap()) as f64;
                            let now = now_millis() as f64;
                            let r = fastrand::f64().max(f64::EPSILON);
                            if now - delta_ema * beta * r.ln() >= fresh_until_millis {
                                cf.metrics.on_xfetch_early_refresh(&key);
                                cf.spawn_stale_refresh(
                                    key.clone(),
                                    wrapped,
                                    total,
                                    effective_fresh,
                                    delta_ema,
                                    RecomputeReason::ProbabilisticEarlyExpiration,
                                )
                                .await;
                                return Ok(LookupResult::new(
                                    payload.to_vec(),
                                    LookupState::CacheHit,
                                ));
                            }
                        }

                        cf.metrics.on_cache_hit(&key);
                        return Ok(LookupResult::new(payload.to_vec(), LookupState::CacheHit));
                    }
                    CachedEntryState::Stale(payload, delta_ema)
                        if effective_stale > Duration::ZERO =>
                    {
                        cf.metrics.on_cache_stale_hit(&key);
                        cf.spawn_stale_refresh(
                            key.clone(),
                            wrapped,
                            total,
                            effective_fresh,
                            delta_ema,
                            RecomputeReason::StaleWhileRevalidate,
                        )
                        .await;
                        return Ok(LookupResult::new(payload.to_vec(), LookupState::Stale));
                    }
                    CachedEntryState::Stale(_, _) | CachedEntryState::Expired => {
                        cf.metrics.on_cache_miss(&key, CacheMissReason::Expired);
                    }
                    CachedEntryState::Invalid => {
                        cf.metrics.on_cache_miss(&key, CacheMissReason::Invalid);
                    }
                },
                Ok(None) => {
                    cf.metrics.on_cache_miss(&key, CacheMissReason::Missing);
                }
                Err(error) => {
                    cf.metrics.on_cache_read_failed(&key, &error);
                    cf.metrics
                        .on_cache_miss(&key, CacheMissReason::BackendError);
                }
            }

            let (flight, joined_existing) = cf.start_or_join_flight(&key).await;

            if joined_existing {
                cf.metrics.on_deduplicated(&key, RecomputeReason::ColdMiss);
                return cf.wait_for_flight(flight, LookupState::Shared).await;
            }

            cf.run_recompute(
                key,
                flight,
                wrapped,
                total,
                effective_fresh,
                None,
                RecomputeReason::ColdMiss,
                LookupState::Recomputed,
            )
            .await
        })
    }
}

// ── CacheFlight ─────────────────────────────────────────────────────────────

/// Deduplicates concurrent recomputation for the same key, with optional
/// stale-while-revalidate and probabilistic early expiration.
pub struct CacheFlight<B, E, X> {
    cache: Arc<B>,
    metrics: Arc<dyn MetricsHooks>,
    config: EntryConfig,
    flights: Arc<Mutex<HashMap<String, Arc<Flight>>>>,
    _phantom: PhantomData<(E, X)>,
}

impl<B, E, X> Clone for CacheFlight<B, E, X> {
    fn clone(&self) -> Self {
        Self {
            cache: Arc::clone(&self.cache),
            metrics: Arc::clone(&self.metrics),
            config: self.config,
            flights: Arc::clone(&self.flights),
            _phantom: PhantomData,
        }
    }
}

// ── Construction (NoExpiry, NoXfetch) ───────────────────────────────────────

impl<B: CacheBackend> CacheFlight<B, NoExpiry, NoXfetch> {
    pub fn new(cache: B) -> Self {
        Self::with_metrics(cache, NoopMetrics)
    }

    pub fn with_metrics(cache: B, metrics: impl MetricsHooks) -> Self {
        Self {
            cache: Arc::new(cache),
            metrics: Arc::new(metrics),
            config: EntryConfig {
                expiry: ExpiryStrategy::Flat {
                    ttl: Duration::ZERO,
                },
                beta: None,
            },
            flights: Arc::new(Mutex::new(HashMap::new())),
            _phantom: PhantomData,
        }
    }

    pub fn ttl(self, duration: Duration) -> CacheFlight<B, HasFlatExpiry, NoXfetch> {
        CacheFlight {
            cache: self.cache,
            metrics: self.metrics,
            config: EntryConfig {
                expiry: ExpiryStrategy::Flat { ttl: duration },
                beta: None,
            },
            flights: self.flights,
            _phantom: PhantomData,
        }
    }

    pub fn stale_while_revalidate(
        self,
        fresh: Duration,
        stale: Duration,
    ) -> CacheFlight<B, HasSwrExpiry, NoXfetch> {
        CacheFlight {
            cache: self.cache,
            metrics: self.metrics,
            config: EntryConfig {
                expiry: ExpiryStrategy::Swr {
                    fresh_ttl: fresh,
                    stale_ttl: stale,
                },
                beta: None,
            },
            flights: self.flights,
            _phantom: PhantomData,
        }
    }
}

// ── Add XFetch on top of any expiry strategy ───────────────────────────────

impl<B: CacheBackend, X> CacheFlight<B, HasFlatExpiry, X> {
    pub fn probabilistic_expiry(self, beta: f64) -> CacheFlight<B, HasFlatExpiry, HasXfetch> {
        CacheFlight {
            cache: self.cache,
            metrics: self.metrics,
            config: EntryConfig {
                expiry: self.config.expiry,
                beta: Some(beta),
            },
            flights: self.flights,
            _phantom: PhantomData,
        }
    }
}

impl<B: CacheBackend, X> CacheFlight<B, HasSwrExpiry, X> {
    pub fn probabilistic_expiry(self, beta: f64) -> CacheFlight<B, HasSwrExpiry, HasXfetch> {
        CacheFlight {
            cache: self.cache,
            metrics: self.metrics,
            config: EntryConfig {
                expiry: self.config.expiry,
                beta: Some(beta),
            },
            flights: self.flights,
            _phantom: PhantomData,
        }
    }

    pub fn fresh_ttl(&self) -> Duration {
        self.config.expiry.fresh_ttl()
    }

    pub fn stale_ttl(&self) -> Duration {
        self.config.expiry.stale_ttl()
    }
}

// ── run() — gated on HasFlatExpiry ──────────────────────────────────────────

impl<B: CacheBackend, X> CacheFlight<B, HasFlatExpiry, X> {
    pub fn run<F, Fut>(
        &self,
        key: impl Into<String>,
        work: F,
    ) -> RunBuilder<'_, B, HasFlatExpiry, X, F>
    where
        F: Fn() -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<Vec<u8>>> + Send + 'static,
    {
        RunBuilder {
            cf: self,
            key: key.into(),
            work,
            fresh_override: None,
            stale_override: None,
            beta_override: None,
        }
    }
}

// ── run() — gated on HasSwrExpiry ──────────────────────────────────────────

impl<B: CacheBackend, X> CacheFlight<B, HasSwrExpiry, X> {
    pub fn run<F, Fut>(
        &self,
        key: impl Into<String>,
        work: F,
    ) -> RunBuilder<'_, B, HasSwrExpiry, X, F>
    where
        F: Fn() -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<Vec<u8>>> + Send + 'static,
    {
        RunBuilder {
            cf: self,
            key: key.into(),
            work,
            fresh_override: None,
            stale_override: None,
            beta_override: None,
        }
    }
}

// ── Internal helpers (available on all variants) ────────────────────────────

impl<B: CacheBackend, E: Send + Sync + 'static, X: Send + Sync + 'static> CacheFlight<B, E, X> {
    async fn start_or_join_flight(&self, key: &str) -> (Arc<Flight>, bool) {
        let mut flights = self.flights.lock().await;

        if let Some(flight) = flights.get(key) {
            return (flight.clone(), true);
        }

        let flight = Arc::new(Flight::new());
        flights.insert(key.to_owned(), flight.clone());
        (flight, false)
    }

    async fn wait_for_flight(
        &self,
        flight: Arc<Flight>,
        state: LookupState,
    ) -> Result<LookupResult> {
        let mut receiver = flight.notifier.subscribe();

        loop {
            if let Some(result) = receiver.borrow().clone() {
                return result.map(|value| LookupResult::new((*value).clone(), state));
            }

            if receiver.changed().await.is_err() {
                return Err(Error::internal(
                    "the in-flight leader finished without publishing a result",
                ));
            }
        }
    }

    async fn spawn_stale_refresh<F, Fut>(
        &self,
        key: String,
        work: F,
        total_ttl: Duration,
        fresh_ttl: Duration,
        previous_delta_ema: f64,
        reason: RecomputeReason,
    ) where
        F: Fn() -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<ComputeValue>> + Send + 'static,
    {
        let (flight, joined_existing) = self.start_or_join_flight(&key).await;

        if joined_existing {
            self.metrics.on_deduplicated(&key, reason);
            return;
        }

        let this = self.clone();
        tokio::spawn(async move {
            let _ = this
                .run_recompute(
                    key,
                    flight,
                    work,
                    total_ttl,
                    fresh_ttl,
                    Some(previous_delta_ema),
                    reason,
                    LookupState::Recomputed,
                )
                .await;
        });
    }

    async fn run_recompute<F, Fut>(
        &self,
        key: String,
        flight: Arc<Flight>,
        work: F,
        total_ttl: Duration,
        fresh_ttl: Duration,
        previous_delta_ema: Option<f64>,
        reason: RecomputeReason,
        state: LookupState,
    ) -> Result<LookupResult>
    where
        F: Fn() -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<ComputeValue>> + Send + 'static,
    {
        self.metrics.on_recompute_started(&key, reason);
        let started_at = Instant::now();

        let result = match work().await {
            Ok(computed) => {
                let (value, should_cache) = computed.into_parts();

                if should_cache {
                    let elapsed = started_at.elapsed();
                    let delta_ema = compute_new_delta_ema(previous_delta_ema, elapsed);
                    let stale_ttl = total_ttl.checked_sub(fresh_ttl).unwrap_or(Duration::ZERO);
                    let encoded_entry =
                        encode_cached_entry(&value, fresh_ttl, stale_ttl, delta_ema);

                    if let Err(error) = self.cache.set(&key, encoded_entry, total_ttl).await {
                        self.metrics.on_cache_write_failed(&key, &error);
                    }
                }

                Ok(Arc::new(value))
            }
            Err(error) => Err(error),
        };

        self.metrics.on_recompute_finished(
            &key,
            reason,
            if result.is_ok() {
                RecomputeOutcome::Success
            } else {
                RecomputeOutcome::Error
            },
            started_at.elapsed(),
        );

        let _ = flight.notifier.send(Some(result.clone()));
        self.finish_flight(&key).await;

        result.map(|value| LookupResult::new((*value).clone(), state))
    }

    async fn finish_flight(&self, key: &str) {
        self.flights.lock().await.remove(key);
    }
}

// ── Encoding / decoding helpers ─────────────────────────────────────────────

fn encode_cached_entry(
    value: &[u8],
    fresh_ttl: Duration,
    stale_ttl: Duration,
    delta_ema_millis: f64,
) -> Vec<u8> {
    let now = now_millis();
    let fresh_until = now.saturating_add(duration_to_millis(fresh_ttl));
    let stale_until = now.saturating_add(duration_to_millis(
        fresh_ttl.checked_add(stale_ttl).unwrap_or(Duration::MAX),
    ));

    let mut bytes = Vec::with_capacity(ENTRY_HEADER_LEN + value.len());
    bytes.extend_from_slice(ENTRY_MAGIC);
    bytes.extend_from_slice(&fresh_until.to_be_bytes());
    bytes.extend_from_slice(&stale_until.to_be_bytes());
    bytes.extend_from_slice(&delta_ema_millis.to_be_bytes());
    bytes.extend_from_slice(value);

    bytes
}

fn classify_cached_entry(bytes: &[u8], now: u64) -> CachedEntryState<'_> {
    if bytes.len() < ENTRY_HEADER_LEN || &bytes[..4] != ENTRY_MAGIC {
        return CachedEntryState::Invalid;
    }

    let fresh_until =
        u64::from_be_bytes(bytes[4..12].try_into().expect("header fresh_until slice"));
    let stale_until =
        u64::from_be_bytes(bytes[12..20].try_into().expect("header stale_until slice"));
    let delta_ema = f64::from_be_bytes(bytes[20..28].try_into().expect("header delta_ema slice"));
    let payload = &bytes[ENTRY_HEADER_LEN..];

    if now < fresh_until {
        CachedEntryState::Fresh(payload, delta_ema)
    } else if now < stale_until {
        CachedEntryState::Stale(payload, delta_ema)
    } else {
        CachedEntryState::Expired
    }
}

fn compute_new_delta_ema(previous: Option<f64>, elapsed: Duration) -> f64 {
    const ALPHA: f64 = 0.2;
    let new_delta = duration_to_millis_f64(elapsed);

    match previous {
        Some(prev) => ALPHA * new_delta + (1.0 - ALPHA) * prev,
        None => new_delta,
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn duration_to_millis(d: Duration) -> u64 {
    d.as_millis().min(u128::from(u64::MAX)) as u64
}

fn duration_to_millis_f64(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}
