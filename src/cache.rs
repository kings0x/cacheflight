use crate::Result;
use async_trait::async_trait;
use std::time::Duration;

/// The only cache abstraction the library depends on.
///
/// Implement this trait for whatever backing store you already use. The
/// library only reads and writes raw bytes plus a TTL, so callers remain in
/// control of serialization and the actual cache technology.
#[async_trait]
pub trait CacheBackend: Send + Sync + 'static {
    /// Returns the raw cached bytes for `key`, if they are still available.
    ///
    /// Backends should return `Ok(None)` for a normal cache miss and use
    /// [`crate::Error::cache_read`] when the cache itself cannot be queried.
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;

    /// Stores raw cached bytes for `key` with the provided TTL.
    async fn set(&self, key: &str, value: Vec<u8>, ttl: Duration) -> Result<()>;
}
