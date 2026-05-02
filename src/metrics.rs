use crate::Error;
use std::time::Duration;

/// Why a cache lookup had to recompute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CacheMissReason {
    /// No entry existed for the key.
    Missing,
    /// An entry existed but had aged out.
    Expired,
    /// An entry existed but was not encoded in the expected format.
    Invalid,
    /// The cache backend could not be read successfully.
    BackendError,
}

/// Why a recomputation was started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RecomputeReason {
    /// The lookup had no usable cached value.
    ColdMiss,
    /// A stale value was served while a refresh ran in the background.
    StaleWhileRevalidate,
}

/// The final recomputation result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RecomputeOutcome {
    /// The recomputation completed successfully.
    Success,
    /// The recomputation failed and all waiters saw the error.
    Error,
}

/// Production observability hooks.
///
/// The methods are synchronous on purpose so callers can cheaply increment
/// counters, record histograms, or fan out to their own telemetry systems.
pub trait MetricsHooks: Send + Sync + 'static {
    /// Called when a fresh cached value is served.
    fn on_cache_hit(&self, _key: &str) {}

    /// Called when a stale cached value is served immediately.
    fn on_cache_stale_hit(&self, _key: &str) {}

    /// Called when the engine needs to recompute because the cache had no
    /// usable value.
    fn on_cache_miss(&self, _key: &str, _reason: CacheMissReason) {}

    /// Called when the cache backend itself could not be read.
    fn on_cache_read_failed(&self, _key: &str, _error: &Error) {}

    /// Called when a caller joins an already-running recomputation instead of
    /// starting a new one.
    fn on_deduplicated(&self, _key: &str, _reason: RecomputeReason) {}

    /// Called immediately before a recomputation starts.
    fn on_recompute_started(&self, _key: &str, _reason: RecomputeReason) {}

    /// Called after a recomputation completes, along with its runtime.
    fn on_recompute_finished(
        &self,
        _key: &str,
        _reason: RecomputeReason,
        _outcome: RecomputeOutcome,
        _duration: Duration,
    ) {
    }

    /// Called when a recomputed value could not be written back to cache.
    fn on_cache_write_failed(&self, _key: &str, _error: &Error) {}
}

/// Default no-op metrics implementation.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopMetrics;

impl MetricsHooks for NoopMetrics {}
