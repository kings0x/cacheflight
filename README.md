# `cacheflight`

`cacheflight` deduplicates concurrent async work for the same cache key and stores the result in a user-provided cache backend. It supports flat TTL, stale-while-revalidate (SWR), and probabilistic early expiration (XFetch) — all without forcing a specific cache store or serialization format.

## Features

- **Cold-miss deduplication** — when N callers race for the same missing key, only one recomputes; the rest share the result.
- **Stale-while-revalidate** — serve a stale value immediately while a background refresh runs.
- **Probabilistic early expiration (XFetch)** — trigger background refreshes before the TTL expires, proportional to compute time, smoothing out thundering herds.
- **Type-state API** — invalid configurations are caught at compile time (e.g. calling `stale_for()` on a flat-TTL instance won't compile).
- **Plugable backends** — implement `CacheBackend` for any store, or use the built-in `MemoryCache` or `RedisCache`.
- **Observability hooks** — `MetricsHooks` trait with no-op defaults, re-implement for production metrics.
- **Safe fallback** — cache read failures degrade to recompute instead of failing the caller.
- **Cache invalidation** — `invalidate(key)` and `invalidate_prefix(prefix)` to evict entries programmatically.

## Installation

```toml
[dependencies]
cacheflight = "0.1"
```

Redis support is included by default. To use only the in-memory backend:

```toml
[dependencies]
cacheflight = { version = "0.1", default-features = false }
```

## Quick start

```rust
use cacheflight::{CacheFlight, MemoryCache, LookupState};
use std::time::Duration;

#[tokio::main]
async fn main() -> cacheflight::Result<()> {
    let cf = CacheFlight::new(MemoryCache::new())
        .ttl(Duration::from_secs(30));

    // Five concurrent callers — only one recomputes.
    let mut tasks = Vec::new();
    for _ in 0..5 {
        let cf = cf.clone();
        tasks.push(tokio::spawn(async move {
            cf.run("user:42", || async {
                // This runs only once.
                Ok(br#"{"id":42,"name":"Ada"}"#.to_vec())
            }).await
        }));
    }

    for task in tasks {
        let result = task.await??;
        println!("state={:?}, value={}", result.state(), String::from_utf8_lossy(result.value()));
    }

    // Subsequent reads hit the cache.
    let cached = cf.run("user:42", || async {
        unreachable!("should not recompute")
    }).await?;
    assert_eq!(cached.state(), LookupState::CacheHit);

    // Invalidate when the underlying data changes.
    cf.invalidate("user:42").await?;

    Ok(())
}
```

## Backends

### MemoryCache (built-in, no extra dependencies)

An in-process cache backed by `DashMap`. Supports failure injection for testing:

```rust
use cacheflight::MemoryCache;

let cache = MemoryCache::new();
cache.fail_one_get();   // next get() fails with a cache read error
cache.fail_one_set();   // next set() fails with a cache write error
cache.insert_raw("k", b"raw".to_vec(), ttl); // bypasses wire format
```

### RedisCache (behind the `redis` feature, enabled by default)

```rust
use cacheflight::RedisCache;

let cache = RedisCache::new("redis://127.0.0.1/").await?;
```

### Custom backend

Implement `CacheBackend` for any store:

```rust
use cacheflight::{CacheBackend, Result, async_trait};
use std::time::Duration;

#[async_trait]
impl CacheBackend for MyStore {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> { /* ... */ }
    async fn set(&self, key: &str, value: Vec<u8>, ttl: Duration) -> Result<()> { /* ... */ }
    async fn delete(&self, key: &str) -> Result<()> { /* ... */ }
    async fn delete_by_prefix(&self, prefix: &str) -> Result<u64> { /* ... */ }
}
```

## Expiry strategies

The API uses Rust's type system to guide configuration:

### Flat TTL

```rust
let cf = CacheFlight::new(cache)
    .ttl(Duration::from_secs(30));
```

Once the TTL expires, the entry is treated as expired and the next caller recomputes.

### Stale-while-revalidate

```rust
let cf = CacheFlight::new(cache)
    .stale_while_revalidate(
        Duration::from_secs(30),   // fresh window
        Duration::from_secs(120),  // stale window
    );
```

Callers within the fresh window get a `CacheHit`. During the stale window, callers get the stale value immediately while a **single** background refresh runs. If multiple callers arrive during the stale window, they all share the same in-flight refresh.

### Probabilistic early expiration (XFetch)

```rust
let cf = CacheFlight::new(cache)
    .ttl(Duration::from_secs(30))
    .probabilistic_expiry(2.0);
```

XFetch may trigger a background refresh *before* the TTL expires, based on the smoothed compute duration (delta EMA) and the `beta` parameter. Higher `beta` makes early refreshes more likely. Callers see `CacheHit` while the refresh runs in the background.

XFetch also works on top of SWR:

```rust
let cf = CacheFlight::new(cache)
    .stale_while_revalidate(Duration::from_secs(30), Duration::from_secs(120))
    .probabilistic_expiry(2.0);
```

## Per-call overrides

The `run()` method returns a `RunBuilder` that supports per-call overrides:

```rust
cf.run("key", work)
    .ttl(Duration::from_secs(10))          // flat TTL only
    .await?;

cf.run("key", work)
    .fresh_for(Duration::from_secs(5))     // SWR only
    .stale_for(Duration::from_secs(60))    // SWR only
    .await?;

cf.run("key", work)
    .beta(4.0)                              // XFetch only
    .await?;
```

## Cache invalidation

```rust
// Invalidate a single key.
cf.invalidate("user:42").await?;

// Invalidate all keys matching a prefix.
let count = cf.invalidate_prefix("session:").await?;
println!("invalidated {count} sessions");
```

## Metrics

Implement `MetricsHooks` to observe cache behavior. All methods have no-op defaults — override only what you need. See [docs.rs](https://docs.rs/cacheflight) for the full trait definition.

```rust
use cacheflight::{MetricsHooks, CacheMissReason, RecomputeReason};

struct MyMetrics;

impl MetricsHooks for MyMetrics {
    fn on_cache_hit(&self, key: &str) { /* ... */ }
    fn on_cache_miss(&self, key: &str, reason: CacheMissReason) { /* ... */ }
}

let cf = CacheFlight::with_metrics(cache, MyMetrics).ttl(Duration::from_secs(30));
```

## License

Licensed under either of [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-Apache-2.0) at your option.
