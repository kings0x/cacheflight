use cacheflight::{CacheBackend, Result, async_trait};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

/// A tiny in-memory cache used only by the examples.
///
/// Real applications can replace this with Redis, Memcached, a database-backed
/// cache, or their own in-process store by implementing `CacheBackend`.
#[derive(Clone, Default)]
pub struct MemoryCache {
    inner: Arc<Mutex<HashMap<String, CacheEntry>>>,
}

#[derive(Clone)]
struct CacheEntry {
    value: Vec<u8>,
    expires_at: Instant,
}

impl MemoryCache {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl CacheBackend for MemoryCache {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let mut guard = self.inner.lock().await;

        match guard.get(key) {
            Some(entry) if entry.expires_at > Instant::now() => Ok(Some(entry.value.clone())),
            Some(_) => {
                guard.remove(key);
                Ok(None)
            }
            None => Ok(None),
        }
    }

    async fn set(&self, key: &str, value: Vec<u8>, ttl: Duration) -> Result<()> {
        self.inner.lock().await.insert(
            key.to_owned(),
            CacheEntry {
                value,
                expires_at: Instant::now() + ttl,
            },
        );

        Ok(())
    }
}
