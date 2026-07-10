#![allow(dead_code)]

use cacheflight::{CacheMissReason, Error, MetricsHooks, RecomputeOutcome, RecomputeReason};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::time::sleep;

#[derive(Clone, Default)]
pub struct TestMetrics {
    inner: Arc<Mutex<MetricsSnapshot>>,
}

#[derive(Debug, Clone, Default)]
pub struct MetricsSnapshot {
    pub hits: usize,
    pub stale_hits: usize,
    pub cache_read_failures: usize,
    pub cache_write_failures: usize,
    pub misses: Vec<CacheMissReason>,
    pub deduplicated: Vec<RecomputeReason>,
    pub recompute_started: Vec<RecomputeReason>,
    pub recompute_finished: Vec<(RecomputeReason, RecomputeOutcome)>,
    pub xfetch_early_refreshes: usize,
    pub background_refresh_failures: usize,
}

impl TestMetrics {
    pub fn snapshot(&self) -> MetricsSnapshot {
        self.inner.lock().expect("metrics mutex poisoned").clone()
    }
}

impl MetricsHooks for TestMetrics {
    fn on_background_refresh_failed(&self, _key: &str, _reason: RecomputeReason, _error: &Error) {
        self.inner
            .lock()
            .expect("metrics mutex poisoned")
            .background_refresh_failures += 1;
    }

    fn on_cache_hit(&self, _key: &str) {
        self.inner.lock().expect("metrics mutex poisoned").hits += 1;
    }

    fn on_cache_stale_hit(&self, _key: &str) {
        self.inner
            .lock()
            .expect("metrics mutex poisoned")
            .stale_hits += 1;
    }

    fn on_cache_miss(&self, _key: &str, reason: CacheMissReason) {
        self.inner
            .lock()
            .expect("metrics mutex poisoned")
            .misses
            .push(reason);
    }

    fn on_cache_read_failed(&self, _key: &str, _error: &Error) {
        self.inner
            .lock()
            .expect("metrics mutex poisoned")
            .cache_read_failures += 1;
    }

    fn on_deduplicated(&self, _key: &str, reason: RecomputeReason) {
        self.inner
            .lock()
            .expect("metrics mutex poisoned")
            .deduplicated
            .push(reason);
    }

    fn on_recompute_started(&self, _key: &str, reason: RecomputeReason) {
        self.inner
            .lock()
            .expect("metrics mutex poisoned")
            .recompute_started
            .push(reason);
    }

    fn on_recompute_finished(
        &self,
        _key: &str,
        reason: RecomputeReason,
        outcome: RecomputeOutcome,
        _duration: Duration,
    ) {
        self.inner
            .lock()
            .expect("metrics mutex poisoned")
            .recompute_finished
            .push((reason, outcome));
    }

    fn on_cache_write_failed(&self, _key: &str, _error: &Error) {
        self.inner
            .lock()
            .expect("metrics mutex poisoned")
            .cache_write_failures += 1;
    }

    fn on_xfetch_early_refresh(&self, _key: &str) {
        self.inner
            .lock()
            .expect("metrics mutex poisoned")
            .xfetch_early_refreshes += 1;
    }
}

pub async fn wait_until(
    timeout: Duration,
    interval: Duration,
    mut condition: impl FnMut() -> bool,
) {
    let started = Instant::now();

    while started.elapsed() < timeout {
        if condition() {
            return;
        }

        sleep(interval).await;
    }

    panic!("condition was not satisfied within {:?}", timeout);
}
