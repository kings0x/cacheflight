//! `singleflight` deduplicates concurrent work for the same cache key while
//! remaining agnostic about the cache backend and payload serialization.
//!
//! The crate operates on raw `Vec<u8>` payloads. You serialize before entering
//! the library and deserialize after leaving it. That keeps the core reusable
//! across Redis, in-memory caches, HTTP middleware, and custom backends.
//!
//! # Main Pieces
//!
//! - [`CacheBackend`] lets you plug in any cache implementation.
//! - [`SingleFlight`] coordinates in-flight work and supports
//!   stale-while-revalidate.
//! - [`MetricsHooks`] exposes observability points for production monitoring.
//! - [`SingleFlightLayer`] is the low-level generic Tower integration.
//! - [`HttpSingleFlightLayer`] is the ergonomic HTTP middleware layer for
//!   Tower and Axum-style services.
//!
//! # Example
//!
//! ```no_run
//! use singleflight::{CacheBackend, CachePolicy, Result, SingleFlight, async_trait};
//! use std::{collections::HashMap, sync::Arc, time::{Duration, Instant}};
//! use tokio::sync::Mutex;
//!
//! #[derive(Clone, Default)]
//! struct MemoryCache {
//!     inner: Arc<Mutex<HashMap<String, (Vec<u8>, Instant)>>>,
//! }
//!
//! #[async_trait]
//! impl CacheBackend for MemoryCache {
//!     async fn get(&self, key: &str) -> Option<Vec<u8>> {
//!         let mut guard = self.inner.lock().await;
//!         match guard.get(key) {
//!             Some((value, expires_at)) if *expires_at > Instant::now() => Some(value.clone()),
//!             Some(_) => {
//!                 guard.remove(key);
//!                 None
//!             }
//!             None => None,
//!         }
//!     }
//!
//!     async fn set(&self, key: &str, value: Vec<u8>, ttl: Duration) -> Result<()> {
//!         self.inner
//!             .lock()
//!             .await
//!             .insert(key.to_owned(), (value, Instant::now() + ttl));
//!         Ok(())
//!     }
//! }
//!
//! # async fn demo() -> Result<()> {
//! let cache = MemoryCache::default();
//! let policy = CachePolicy::new(Duration::from_secs(30))
//!     .with_stale_while_revalidate(Duration::from_secs(120));
//! let singleflight = SingleFlight::new(cache, policy);
//!
//! let result = singleflight
//!     .get_or_compute("user:42", || async {
//!         let body = br#"{"id":42,"name":"Ada"}"#.to_vec();
//!         Ok(body)
//!     })
//!     .await?;
//!
//! assert_eq!(result.value(), br#"{"id":42,"name":"Ada"}"#);
//! # Ok(())
//! # }
//! ```

mod cache;
mod error;
mod metrics;
mod policy;
mod singleflight;
mod tower;

pub use async_trait::async_trait;
pub use cache::CacheBackend;
pub use error::{Error, ErrorSource, Result};
pub use metrics::{CacheMissReason, MetricsHooks, NoopMetrics, RecomputeOutcome, RecomputeReason};
pub use policy::CachePolicy;
pub use singleflight::{ComputeValue, LookupResult, LookupState, SingleFlight};
pub use tower::{
    BytesPolicy, HttpSingleFlightLayer, HttpSingleFlightService, SingleFlightLayer,
    SingleFlightService, TowerCachePolicy,
};
