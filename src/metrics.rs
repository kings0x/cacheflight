use crate::Error;
use std::time::Duration;

/// Why a cache lookup had to recompute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheMissReason {
    Missing,
    Expired,
    Invalid,
}

/// Why a recomputation was started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecomputeReason {
    ColdMiss,
    StaleWhileRevalidate,
}

/// The final recomputation result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecomputeOutcome {
    Success,
    Error,
}

/// Production observability hooks.
///
/// The methods are synchronous on purpose so callers can cheaply increment
/// counters, record histograms, or fan out to their own telemetry systems.
pub trait MetricsHooks: Send + Sync + 'static {
    fn on_cache_hit(&self, _key: &str) {}

    fn on_cache_stale_hit(&self, _key: &str) {}

    fn on_cache_miss(&self, _key: &str, _reason: CacheMissReason) {}

    fn on_deduplicated(&self, _key: &str, _reason: RecomputeReason) {}

    fn on_recompute_started(&self, _key: &str, _reason: RecomputeReason) {}

    fn on_recompute_finished(
        &self,
        _key: &str,
        _reason: RecomputeReason,
        _outcome: RecomputeOutcome,
        _duration: Duration,
    ) {
    }

    fn on_cache_write_failed(&self, _key: &str, _error: &Error) {}
}

/// Default no-op metrics implementation.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopMetrics;

impl MetricsHooks for NoopMetrics {}
