mod common;

use common::MemoryCache;
use singleflight::{
    CacheMissReason, CachePolicy, LookupState, MetricsHooks, RecomputeOutcome, RecomputeReason,
    Result, SingleFlight,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::time::sleep;

// Run with:
// cargo run --example basic_singleflight
//
// What this example teaches:
// 1. How to provide your own cache backend by implementing `CacheBackend`.
// 2. How `SingleFlight` deduplicates concurrent work for the same key.
// 3. How metrics hooks can be used to observe cache and recomputation behavior.
//
// The library stores raw bytes only. In a real application you would usually
// serialize your type to bytes before returning it from the closure.

#[derive(Clone, Default)]
struct ExampleMetrics {
    cache_hits: Arc<AtomicUsize>,
    cache_misses: Arc<AtomicUsize>,
    deduplicated_requests: Arc<AtomicUsize>,
    recomputes: Arc<AtomicUsize>,
}

impl MetricsHooks for ExampleMetrics {
    fn on_cache_hit(&self, _key: &str) {
        self.cache_hits.fetch_add(1, Ordering::SeqCst);
    }

    fn on_cache_miss(&self, _key: &str, _reason: CacheMissReason) {
        self.cache_misses.fetch_add(1, Ordering::SeqCst);
    }

    fn on_deduplicated(&self, _key: &str, _reason: RecomputeReason) {
        self.deduplicated_requests.fetch_add(1, Ordering::SeqCst);
    }

    fn on_recompute_finished(
        &self,
        _key: &str,
        _reason: RecomputeReason,
        outcome: RecomputeOutcome,
        _duration: Duration,
    ) {
        if outcome == RecomputeOutcome::Success {
            self.recomputes.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cache = MemoryCache::new();
    let metrics = ExampleMetrics::default();
    let singleflight = SingleFlight::with_metrics(
        cache,
        CachePolicy::new(Duration::from_secs(30)),
        metrics.clone(),
    );

    let upstream_calls = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::new();

    // These five requests all ask for the same key at the same time.
    // Only one closure should actually execute.
    for request_id in 1..=5 {
        let singleflight = singleflight.clone();
        let upstream_calls = upstream_calls.clone();

        tasks.push(tokio::spawn(async move {
            let result = singleflight
                .get_or_compute("user:42", move || {
                    let upstream_calls = upstream_calls.clone();

                    async move {
                        let invocation = upstream_calls.fetch_add(1, Ordering::SeqCst) + 1;
                        println!(
                            "request {request_id}: leader is fetching the upstream value (call #{invocation})"
                        );
                        sleep(Duration::from_millis(100)).await;

                        Ok(br#"{"id":42,"name":"Ada"}"#.to_vec())
                    }
                })
                .await
                .expect("singleflight request should succeed");

            println!(
                "request {request_id}: state={:?}, body={}",
                result.state(),
                String::from_utf8_lossy(result.value())
            );

            result
        }));
    }

    for task in tasks {
        let result = task.await.expect("task panicked");
        assert!(matches!(
            result.state(),
            LookupState::Recomputed | LookupState::Shared
        ));
        assert_eq!(result.value(), br#"{"id":42,"name":"Ada"}"#);
    }

    // A later request for the same key is served straight from cache.
    let cached = singleflight
        .get_or_compute("user:42", || async {
            unreachable!("the cache hit should avoid recomputing")
        })
        .await?;

    assert_eq!(cached.state(), LookupState::CacheHit);
    assert_eq!(upstream_calls.load(Ordering::SeqCst), 1);

    println!();
    println!("Summary");
    println!("-------");
    println!(
        "Upstream was called {} time(s). Five concurrent callers shared the same work.",
        upstream_calls.load(Ordering::SeqCst)
    );
    println!(
        "Metrics: cache_hits={}, cache_misses={}, deduplicated={}, recomputes={}",
        metrics.cache_hits.load(Ordering::SeqCst),
        metrics.cache_misses.load(Ordering::SeqCst),
        metrics.deduplicated_requests.load(Ordering::SeqCst),
        metrics.recomputes.load(Ordering::SeqCst),
    );
    println!(
        "The important pattern is: implement CacheBackend, build SingleFlight, then call get_or_compute(key, work)."
    );

    Ok(())
}
