use crate::Error;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CacheMissReason {
    Missing,
    Expired,
    Invalid,
    BackendError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RecomputeReason {
    ColdMiss,
    StaleWhileRevalidate,
    ProbabilisticEarlyExpiration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RecomputeOutcome {
    Success,
    Error,
}

pub trait MetricsHooks: Send + Sync + 'static {
    fn on_cache_hit(&self, _key: &str) {}

    fn on_cache_stale_hit(&self, _key: &str) {}

    fn on_cache_miss(&self, _key: &str, _reason: CacheMissReason) {}

    fn on_cache_read_failed(&self, _key: &str, _error: &Error) {}

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

    fn on_xfetch_early_refresh(&self, _key: &str) {}
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopMetrics;

impl MetricsHooks for NoopMetrics {}
