use crate::{CacheBackend, Error, Result, async_trait};
use dashmap::DashMap;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct MemoryCache {
    entries: DashMap<String, CacheEntry>,
    fail_next_get: Arc<AtomicUsize>,
    fail_next_set: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct CacheEntry {
    value: Vec<u8>,
    expires_at: Instant,
}

impl MemoryCache {
    /// Creates an empty in-memory cache.
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
            fail_next_get: Arc::new(AtomicUsize::new(0)),
            fail_next_set: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Makes the next `get` call fail with a cache read error.
    pub fn fail_one_get(&self) {
        self.fail_next_get.fetch_add(1, Ordering::SeqCst);
    }

    /// Makes the next `set` call fail with a cache write error.
    pub fn fail_one_set(&self) {
        self.fail_next_set.fetch_add(1, Ordering::SeqCst);
    }

    /// Inserts a raw (possibly invalid) value directly, bypassing the wire format.
    pub fn insert_raw(&self, key: impl Into<String>, value: Vec<u8>, ttl: Duration) {
        self.entries.insert(
            key.into(),
            CacheEntry {
                value,
                expires_at: Instant::now() + ttl,
            },
        );
    }
}

impl Default for MemoryCache {
    fn default() -> Self {
        Self::new()
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

        if let Some(entry) = self.entries.get(key) {
            if entry.expires_at > Instant::now() {
                return Ok(Some(entry.value.clone()));
            }
            drop(entry);
            self.entries.remove(key);
        }
        Ok(None)
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

        self.entries.insert(
            key.to_owned(),
            CacheEntry {
                value,
                expires_at: Instant::now() + ttl,
            },
        );
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.entries.remove(key);
        Ok(())
    }

    async fn delete_by_prefix(&self, prefix: &str) -> Result<u64> {
        let mut count = 0u64;
        self.entries.retain(|k, _| {
            if k.starts_with(prefix) {
                count += 1;
                false
            } else {
                true
            }
        });
        Ok(count)
    }
}
