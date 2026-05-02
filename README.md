# `singleflight`

`singleflight` deduplicates concurrent async work for the same key and stores the result in a user-provided cache backend. It supports plain cache hits, cold-miss coordination, and stale-while-revalidate refreshes without forcing a specific cache store or serialization format.

## What it gives you

- A small `CacheBackend` trait for plugging in Redis, Memcached, in-process caches, or your own store.
- A core `SingleFlight` engine for non-HTTP workloads.
- Optional Tower/Axum-friendly middleware helpers for request deduplication and cached response replay.
- Metrics hooks for production observability.
- Safe fallback behavior when cache reads fail: the engine recomputes instead of failing the caller.

## Installation

```toml
[dependencies]
singleflight = "0.1"
```

## Core example

```rust
use singleflight::{CacheBackend, CachePolicy, Result, SingleFlight, async_trait};
use std::{collections::HashMap, sync::Arc, time::{Duration, Instant}};
use tokio::sync::Mutex;

#[derive(Clone, Default)]
struct MemoryCache {
    inner: Arc<Mutex<HashMap<String, (Vec<u8>, Instant)>>>,
}

#[async_trait]
impl CacheBackend for MemoryCache {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let mut guard = self.inner.lock().await;

        match guard.get(key) {
            Some((value, expires_at)) if *expires_at > Instant::now() => Ok(Some(value.clone())),
            Some(_) => {
                guard.remove(key);
                Ok(None)
            }
            None => Ok(None),
        }
    }

    async fn set(&self, key: &str, value: Vec<u8>, ttl: Duration) -> Result<()> {
        self.inner
            .lock()
            .await
            .insert(key.to_owned(), (value, Instant::now() + ttl));
        Ok(())
    }
}

# async fn demo() -> Result<()> {
let cache = MemoryCache::default();
let policy = CachePolicy::new(Duration::from_secs(30))
    .with_stale_while_revalidate(Duration::from_secs(120));
let singleflight = SingleFlight::new(cache, policy);

let result = singleflight
    .get_or_compute("user:42", || async {
        Ok(br#"{"id":42,"name":"Ada"}"#.to_vec())
    })
    .await?;

assert_eq!(result.value(), br#"{"id":42,"name":"Ada"}"#);
# Ok(())
# }
```

## HTTP middleware safety notes

The HTTP layer is intentionally conservative, but it is still application-level caching middleware, not a full RFC cache.

- By default it only caches `GET` and `HEAD`.
- It bypasses requests carrying `Authorization`, `Proxy-Authorization`, `Cookie`, or `Range`.
- Default keys include method, host, path, normalized query ordering, and common `Accept*` headers.
- It buffers full response bodies before replaying them from cache, so set `max_response_bytes` to a value that fits your workload.
- If responses vary on tenant IDs, locale headers, feature flags, or other request metadata, use `key_with` to include that variance explicitly.

## Benchmarks

Yes, benchmarks are worth having for this crate because its value is mostly about coordination overhead under load. The included Criterion benchmark covers cache-hit and contended cold-miss paths:

```bash
cargo bench
```

For a real release process, it is worth tracking benchmark results over time so regressions in lock contention, request fan-in, or HTTP buffering show up before publish.

## Status

The crate ships with tests for:

- concurrent cold-miss deduplication
- stale-while-revalidate refresh behavior
- shared error propagation
- cache read/write failure handling
- Tower integration
- HTTP middleware defaults and safety guards

API docs: <https://docs.rs/singleflight>
