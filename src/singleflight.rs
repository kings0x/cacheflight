use crate::{
    CacheBackend, CacheMissReason, CachePolicy, MetricsHooks, NoopMetrics, RecomputeOutcome,
    RecomputeReason, Result, error::Error,
};
use std::{
    collections::HashMap,
    future::Future,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, watch};

const ENTRY_MAGIC: &[u8; 4] = b"SFG1";
const ENTRY_HEADER_LEN: usize = 20;

type SharedFlightResult = Result<Arc<Vec<u8>>>;

#[derive(Debug)]
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
    Fresh(&'a [u8]),
    Stale(&'a [u8]),
    Expired,
    Invalid,
}

/// Controls whether a recomputed value should be written back to cache.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComputeValue {
    value: Vec<u8>,
    cache: bool,
    cache_policy: Option<CachePolicy>,
}

impl ComputeValue {
    /// Stores `value` using the default policy configured on the engine.
    pub fn cache(value: Vec<u8>) -> Self {
        Self {
            value,
            cache: true,
            cache_policy: None,
        }
    }

    /// Stores `value` using a specific cache policy for this write.
    pub fn cache_with_policy(value: Vec<u8>, policy: CachePolicy) -> Self {
        Self {
            value,
            cache: true,
            cache_policy: Some(policy),
        }
    }

    /// Returns `value` to callers without writing it to cache.
    pub fn do_not_cache(value: Vec<u8>) -> Self {
        Self {
            value,
            cache: false,
            cache_policy: None,
        }
    }

    fn into_parts(self) -> (Vec<u8>, bool, Option<CachePolicy>) {
        (self.value, self.cache, self.cache_policy)
    }
}

/// The path a lookup took through the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum LookupState {
    /// The returned bytes came directly from a fresh cache entry.
    CacheHit,
    /// The caller received a stale value while a refresh ran in the background.
    Stale,
    /// This caller executed the recomputation closure.
    Recomputed,
    /// This caller waited for another in-flight recomputation to finish.
    Shared,
}

/// The bytes returned by the engine plus the path they took.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupResult {
    value: Vec<u8>,
    state: LookupState,
}

impl LookupResult {
    fn new(value: Vec<u8>, state: LookupState) -> Self {
        Self { value, state }
    }

    /// Returns the returned bytes.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Consumes the result and returns the owned bytes.
    pub fn into_value(self) -> Vec<u8> {
        self.value
    }

    /// Returns how the value was produced.
    pub fn state(&self) -> LookupState {
        self.state
    }
}

/// Deduplicates concurrent recomputation for the same key and stores the
/// resulting bytes in the configured cache backend.
pub struct CacheFlight<C, M = NoopMetrics> {
    cache: Arc<C>,
    metrics: Arc<M>,
    policy: CachePolicy,
    flights: Arc<Mutex<HashMap<String, Arc<Flight>>>>,
}

impl<C, M> Clone for CacheFlight<C, M> {
    fn clone(&self) -> Self {
        Self {
            cache: Arc::clone(&self.cache),
            metrics: Arc::clone(&self.metrics),
            policy: self.policy,
            flights: Arc::clone(&self.flights),
        }
    }
}

impl<C> CacheFlight<C, NoopMetrics>
where
    C: CacheBackend,
{
    /// Creates a new engine with the provided cache backend and cache policy.
    pub fn new(cache: C, policy: CachePolicy) -> Self {
        Self::with_metrics(cache, policy, NoopMetrics)
    }
}

impl<C, M> CacheFlight<C, M>
where
    C: CacheBackend,
    M: MetricsHooks,
{
    /// Creates a new engine with custom metrics hooks.
    pub fn with_metrics(cache: C, policy: CachePolicy, metrics: M) -> Self {
        Self {
            cache: Arc::new(cache),
            metrics: Arc::new(metrics),
            policy,
            flights: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Returns the cache policy used for new writes and stale handling.
    pub fn policy(&self) -> CachePolicy {
        self.policy
    }

    /// Returns a shared reference to the cache backend.
    pub fn cache(&self) -> &C {
        self.cache.as_ref()
    }

    /// Convenience method that calls [`Self::get_or_compute`].
    pub async fn run<F, Fut>(
        &self,
        key: impl Into<String>,
        work: F,
    ) -> Result<LookupResult>
    where
        F: Fn() -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<Vec<u8>>> + Send + 'static,
    {
        self.get_or_compute(key, work).await
    }

    /// Reads from cache when possible and deduplicates concurrent
    /// recomputation on misses.
    ///
    /// `work` must return raw bytes that are safe to store in the configured
    /// cache backend. The closure is `Clone` so stale refreshes can trigger
    /// background recomputation without blocking the caller.
    pub async fn get_or_compute<F, Fut>(
        &self,
        key: impl Into<String>,
        work: F,
    ) -> Result<LookupResult>
    where
        F: Fn() -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<Vec<u8>>> + Send + 'static,
    {
        self.get_or_compute_with(key, self.policy, move || {
            let work = work.clone();
            async move { work().await.map(ComputeValue::cache) }
        })
        .await
    }

    /// Like [`Self::get_or_compute`], but allows a caller to override the
    /// cache policy for the specific lookup.
    pub async fn get_or_compute_with_policy<F, Fut>(
        &self,
        key: impl Into<String>,
        policy: CachePolicy,
        work: F,
    ) -> Result<LookupResult>
    where
        F: Fn() -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<Vec<u8>>> + Send + 'static,
    {
        self.get_or_compute_with(key, policy, move || {
            let work = work.clone();
            async move {
                work()
                    .await
                    .map(|value| ComputeValue::cache_with_policy(value, policy))
            }
        })
        .await
    }

    /// Advanced form of [`Self::get_or_compute`] that can decide whether the
    /// recomputed value should be cached at all.
    pub async fn get_or_compute_with<F, Fut>(
        &self,
        key: impl Into<String>,
        lookup_policy: CachePolicy,
        work: F,
    ) -> Result<LookupResult>
    where
        F: Fn() -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<ComputeValue>> + Send + 'static,
    {
        let key = key.into();

        match self.cache.get(&key).await {
            Ok(Some(cached)) => match classify_cached_entry(&cached, now_millis()) {
                CachedEntryState::Fresh(value) => {
                    self.metrics.on_cache_hit(&key);
                    return Ok(LookupResult::new(value.to_vec(), LookupState::CacheHit));
                }
                CachedEntryState::Stale(value) if lookup_policy.allows_stale_while_revalidate() => {
                    self.metrics.on_cache_stale_hit(&key);
                    self.spawn_stale_refresh(key.clone(), lookup_policy, work.clone())
                        .await;
                    return Ok(LookupResult::new(value.to_vec(), LookupState::Stale));
                }
                CachedEntryState::Stale(_) | CachedEntryState::Expired => {
                    self.metrics.on_cache_miss(&key, CacheMissReason::Expired);
                }
                CachedEntryState::Invalid => {
                    self.metrics.on_cache_miss(&key, CacheMissReason::Invalid);
                }
            },
            Ok(None) => {
                self.metrics.on_cache_miss(&key, CacheMissReason::Missing);
            }
            Err(error) => {
                self.metrics.on_cache_read_failed(&key, &error);
                self.metrics
                    .on_cache_miss(&key, CacheMissReason::BackendError);
            }
        }

        let (flight, joined_existing) = self.start_or_join_flight(&key).await;

        if joined_existing {
            self.metrics
                .on_deduplicated(&key, RecomputeReason::ColdMiss);
            return self.wait_for_flight(flight, LookupState::Shared).await;
        }

        self.run_recompute(
            key,
            flight,
            lookup_policy,
            work,
            RecomputeReason::ColdMiss,
            LookupState::Recomputed,
        )
        .await
    }

    async fn spawn_stale_refresh<F, Fut>(&self, key: String, lookup_policy: CachePolicy, work: F)
    where
        F: Fn() -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<ComputeValue>> + Send + 'static,
    {
        let (flight, joined_existing) = self.start_or_join_flight(&key).await;

        if joined_existing {
            self.metrics
                .on_deduplicated(&key, RecomputeReason::StaleWhileRevalidate);
            return;
        }

        let this = self.clone();
        tokio::spawn(async move {
            let _ = this
                .run_recompute(
                    key,
                    flight,
                    lookup_policy,
                    work,
                    RecomputeReason::StaleWhileRevalidate,
                    LookupState::Recomputed,
                )
                .await;
        });
    }

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

    async fn run_recompute<F, Fut>(
        &self,
        key: String,
        flight: Arc<Flight>,
        fallback_policy: CachePolicy,
        work: F,
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
            Ok(value) => {
                let (value, should_cache, cache_policy) = value.into_parts();

                if should_cache {
                    let cache_policy = cache_policy.unwrap_or(fallback_policy);
                    let encoded_entry = encode_cached_entry(&value, cache_policy);

                    if let Err(error) = self
                        .cache
                        .set(&key, encoded_entry, cache_policy.total_ttl())
                        .await
                    {
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

fn encode_cached_entry(value: &[u8], policy: CachePolicy) -> Vec<u8> {
    let now = now_millis();
    let fresh_until = now.saturating_add(duration_to_millis(policy.fresh_ttl()));
    let stale_until = now.saturating_add(duration_to_millis(policy.total_ttl()));
    let mut bytes = Vec::with_capacity(ENTRY_HEADER_LEN + value.len());

    bytes.extend_from_slice(ENTRY_MAGIC);
    bytes.extend_from_slice(&fresh_until.to_be_bytes());
    bytes.extend_from_slice(&stale_until.to_be_bytes());
    bytes.extend_from_slice(value);

    bytes
}

fn classify_cached_entry(bytes: &[u8], now: u64) -> CachedEntryState<'_> {
    if bytes.len() < ENTRY_HEADER_LEN || &bytes[..4] != ENTRY_MAGIC {
        return CachedEntryState::Invalid;
    }

    let fresh_until =
        u64::from_be_bytes(bytes[4..12].try_into().expect("header slice has length 8"));
    let stale_until =
        u64::from_be_bytes(bytes[12..20].try_into().expect("header slice has length 8"));
    let payload = &bytes[20..];

    if now < fresh_until {
        CachedEntryState::Fresh(payload)
    } else if now < stale_until {
        CachedEntryState::Stale(payload)
    } else {
        CachedEntryState::Expired
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn duration_to_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}
