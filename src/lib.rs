#![forbid(unsafe_code)]

//! Deduplicates concurrent async work for the same cache key.
//!
//! Users implement [`CacheBackend`] for their cache store (or use the provided
//! [`MemoryCache`] or [`RedisCache`]), then call
//! [`CacheFlight::run()`] with a key and a work closure.
//!
//! # Example
//!
//! ```no_run
//! use cacheflight::{CacheFlight, MemoryCache, Result};
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let cf = CacheFlight::new(MemoryCache::new())
//!         .ttl(Duration::from_secs(30));
//!
//!     let result = cf.run("user:42", || async {
//!         let body = br#"{"id":42,"name":"Ada"}"#.to_vec();
//!         Ok(body)
//!     }).await?;
//!
//!     assert_eq!(result.value(), br#"{"id":42,"name":"Ada"}"#);
//!     Ok(())
//! }
//! ```

mod cache;
mod cacheflight;
mod error;
mod memory;
mod metrics;
#[cfg(feature = "redis")]
mod redis;

pub use async_trait::async_trait;
pub use cache::CacheBackend;
pub use cacheflight::{
    CacheFlight, ComputeValue, HasFlatExpiry, HasSwrExpiry, HasXfetch, LookupResult, LookupState,
    NoExpiry, NoXfetch,
};
pub use error::{Error, ErrorSource, Result};
pub use memory::MemoryCache;
pub use metrics::{CacheMissReason, MetricsHooks, NoopMetrics, RecomputeOutcome, RecomputeReason};
#[cfg(feature = "redis")]
pub use redis::RedisCache;
