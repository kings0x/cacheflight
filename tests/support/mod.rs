#![allow(dead_code)]

use singleflight::{
    CacheBackend, CacheMissReason, Error, MetricsHooks, RecomputeOutcome, RecomputeReason, Result,
    async_trait,
};
use std::{
    collections::HashMap,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{sync::Mutex as AsyncMutex, time::sleep};

#[derive(Clone, Default)]
pub struct MemoryCache {
    entries: Arc<AsyncMutex<HashMap<String, CacheEntry>>>,
    fail_next_get: Arc<AtomicUsize>,
    fail_next_set: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct CacheEntry {
    value: Vec<u8>,
    expires_at: Instant,
}

impl MemoryCache {
    pub fn fail_one_get(&self) {
        self.fail_next_get.fetch_add(1, Ordering::SeqCst);
    }

    pub fn fail_one_set(&self) {
        self.fail_next_set.fetch_add(1, Ordering::SeqCst);
    }

    pub async fn insert_raw(&self, key: impl Into<String>, value: Vec<u8>, ttl: Duration) {
        self.entries.lock().await.insert(
            key.into(),
            CacheEntry {
                value,
                expires_at: Instant::now() + ttl,
            },
        );
    }
}

#[async_trait]
impl CacheBackend for MemoryCache {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let should_fail = self
            .fail_next_get
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |pending| {
                (pending > 0).then(|| pending - 1)
            })
            .is_ok();

        if should_fail {
            return Err(Error::cache_read(io::Error::other(
                "forced cache read failure",
            )));
        }

        let mut entries = self.entries.lock().await;

        match entries.get(key) {
            Some(entry) if entry.expires_at > Instant::now() => Ok(Some(entry.value.clone())),
            Some(_) => {
                entries.remove(key);
                Ok(None)
            }
            None => Ok(None),
        }
    }

    async fn set(&self, key: &str, value: Vec<u8>, ttl: Duration) -> Result<()> {
        let should_fail = self
            .fail_next_set
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |pending| {
                (pending > 0).then(|| pending - 1)
            })
            .is_ok();

        if should_fail {
            return Err(Error::cache_write(io::Error::other(
                "forced cache write failure",
            )));
        }

        self.entries.lock().await.insert(
            key.to_owned(),
            CacheEntry {
                value,
                expires_at: Instant::now() + ttl,
            },
        );
        Ok(())
    }
}

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
}

impl TestMetrics {
    pub fn snapshot(&self) -> MetricsSnapshot {
        self.inner.lock().expect("metrics mutex poisoned").clone()
    }
}

impl MetricsHooks for TestMetrics {
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
