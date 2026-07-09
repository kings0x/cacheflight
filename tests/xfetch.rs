mod support;

use cacheflight::{CacheFlight, LookupState};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use support::TestMetrics;
use tokio::time::sleep;

/// XFetch with a high beta and slow compute triggers a background refresh
/// before the flat TTL expires.  The caller sees CacheHit while the refresh
/// runs in the background.
#[tokio::test]
async fn xfetch_triggers_background_refresh_during_fresh_window() {
    let cache = support::MemoryCache::default();
    let metrics = TestMetrics::default();
    let cf = CacheFlight::with_metrics(cache, metrics.clone())
        .ttl(Duration::from_millis(500))
        .probabilistic_expiry(10.0);

    let recomputes = Arc::new(AtomicUsize::new(0));

    // Prime the cache with a slow compute (large delta_ema).
    cf.run("k", {
        let recomputes = recomputes.clone();
        move || {
            let recomputes = recomputes.clone();
            async move {
                recomputes.fetch_add(1, Ordering::SeqCst);
                sleep(Duration::from_millis(80)).await;
                Ok(b"value".to_vec())
            }
        }
    })
    .await
    .expect("prime");

    assert_eq!(recomputes.load(Ordering::SeqCst), 1);

    // Read repeatedly.  XFetch should trigger an early refresh.
    for _ in 0..20 {
        let prev = recomputes.load(Ordering::SeqCst);
        let result = cf
            .run("k", {
                let recomputes = recomputes.clone();
                move || {
                    let recomputes = recomputes.clone();
                    async move {
                        // XFetch may call this as a background refresh.
                        recomputes.fetch_add(1, Ordering::SeqCst);
                        sleep(Duration::from_millis(80)).await;
                        Ok(b"value".to_vec())
                    }
                }
            })
            .await
            .expect("fresh read");

        assert_eq!(result.state(), LookupState::CacheHit);
        assert_eq!(result.value(), b"value");

        if prev < recomputes.load(Ordering::SeqCst) {
            // XFetch triggered a background refresh.
            break;
        }

        sleep(Duration::from_millis(15)).await;
    }

    assert!(
        recomputes.load(Ordering::SeqCst) >= 2,
        "XFetch should have triggered at least one background refresh, got {}",
        recomputes.load(Ordering::SeqCst)
    );

    assert!(
        metrics.snapshot().xfetch_early_refreshes >= 1,
        "on_xfetch_early_refresh should have been called"
    );
}

/// When beta is zero, XFetch never triggers regardless of compute duration.
#[tokio::test]
async fn xfetch_does_not_trigger_when_beta_is_zero() {
    let cache = support::MemoryCache::default();
    let metrics = TestMetrics::default();
    let cf = CacheFlight::with_metrics(cache, metrics.clone())
        .ttl(Duration::from_millis(500))
        .probabilistic_expiry(0.0);

    let recomputes = Arc::new(AtomicUsize::new(0));

    cf.run("k", {
        let recomputes = recomputes.clone();
        move || {
            let recomputes = recomputes.clone();
            async move {
                recomputes.fetch_add(1, Ordering::SeqCst);
                sleep(Duration::from_millis(80)).await;
                Ok(b"value".to_vec())
            }
        }
    })
    .await
    .expect("prime");

    for _ in 0..10 {
        cf.run("k", {
            let recomputes = recomputes.clone();
            move || {
                let recomputes = recomputes.clone();
                async move {
                    recomputes.fetch_add(1, Ordering::SeqCst);
                    sleep(Duration::from_millis(80)).await;
                    Ok(b"value".to_vec())
                }
            }
        })
        .await
        .expect("read");

        sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(
        recomputes.load(Ordering::SeqCst),
        1,
        "beta=0 should never trigger XFetch"
    );
    assert_eq!(metrics.snapshot().xfetch_early_refreshes, 0);
}

/// Without calling probabilistic_expiry, XFetch is not compiled into the
/// cacheflight instance, so no early refresh can occur.
#[tokio::test]
async fn xfetch_requires_probabilistic_expiry() {
    let cache = support::MemoryCache::default();
    let metrics = TestMetrics::default();
    // HasFlatExpiry, NoXfetch — no beta possible.
    let cf = CacheFlight::with_metrics(cache, metrics.clone()).ttl(Duration::from_millis(500));

    let recomputes = Arc::new(AtomicUsize::new(0));

    cf.run("k", {
        let recomputes = recomputes.clone();
        move || {
            let recomputes = recomputes.clone();
            async move {
                recomputes.fetch_add(1, Ordering::SeqCst);
                sleep(Duration::from_millis(80)).await;
                Ok(b"value".to_vec())
            }
        }
    })
    .await
    .expect("prime");

    for _ in 0..10 {
        cf.run("k", {
            let recomputes = recomputes.clone();
            move || {
                let recomputes = recomputes.clone();
                async move {
                    recomputes.fetch_add(1, Ordering::SeqCst);
                    sleep(Duration::from_millis(80)).await;
                    Ok(b"value".to_vec())
                }
            }
        })
        .await
        .expect("read");

        sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(recomputes.load(Ordering::SeqCst), 1);
}

/// With SWR + XFetch, an early refresh can trigger during the fresh window
/// just like with flat TTL.  Stale hits are returned once the entry ages.
#[tokio::test]
async fn xfetch_triggers_during_swr_fresh_window() {
    let cache = support::MemoryCache::default();
    let metrics = TestMetrics::default();
    let cf = CacheFlight::with_metrics(cache, metrics.clone())
        .stale_while_revalidate(Duration::from_millis(300), Duration::from_millis(1000))
        .probabilistic_expiry(10.0);

    let recomputes = Arc::new(AtomicUsize::new(0));

    cf.run("k", {
        let recomputes = recomputes.clone();
        move || {
            let recomputes = recomputes.clone();
            async move {
                recomputes.fetch_add(1, Ordering::SeqCst);
                sleep(Duration::from_millis(80)).await;
                Ok(b"swr-value".to_vec())
            }
        }
    })
    .await
    .expect("prime");

    for _ in 0..20 {
        let prev = recomputes.load(Ordering::SeqCst);
        let result = cf
            .run("k", {
                let recomputes = recomputes.clone();
                move || {
                    let recomputes = recomputes.clone();
                    async move {
                        recomputes.fetch_add(1, Ordering::SeqCst);
                        sleep(Duration::from_millis(80)).await;
                        Ok(b"swr-value".to_vec())
                    }
                }
            })
            .await
            .expect("read");

        // During the fresh window the state is CacheHit.
        assert!(matches!(
            result.state(),
            LookupState::CacheHit | LookupState::Stale
        ));

        if prev < recomputes.load(Ordering::SeqCst) {
            break;
        }

        sleep(Duration::from_millis(10)).await;
    }

    assert!(
        recomputes.load(Ordering::SeqCst) >= 2,
        "XFetch should have triggered a background refresh during SWR fresh window"
    );

    // Also verify stale delivery works after the fresh window.
    sleep(Duration::from_millis(400)).await;

    let stale = cf
        .run("k", {
            let recomputes = recomputes.clone();
            move || {
                let recomputes = recomputes.clone();
                async move {
                    recomputes.fetch_add(1, Ordering::SeqCst);
                    sleep(Duration::from_millis(80)).await;
                    Ok(b"swr-value".to_vec())
                }
            }
        })
        .await
        .expect("stale read");

    assert_eq!(stale.state(), LookupState::Stale);
    assert_eq!(stale.value(), b"swr-value");
}

/// When compute is very fast (tiny delta_ema), XFetch is unlikely to trigger
/// early because delta_ema * beta * (-ln(r)) is small.
#[tokio::test]
async fn xfetch_rarely_triggers_with_fast_compute() {
    let cache = support::MemoryCache::default();
    let metrics = TestMetrics::default();
    let cf = CacheFlight::with_metrics(cache, metrics.clone())
        .ttl(Duration::from_millis(500))
        .probabilistic_expiry(2.0);

    let recomputes = Arc::new(AtomicUsize::new(0));

    cf.run("k", {
        let recomputes = recomputes.clone();
        move || {
            let recomputes = recomputes.clone();
            async move {
                recomputes.fetch_add(1, Ordering::SeqCst);
                // Very fast compute — delta_ema ≈ 1ms.
                sleep(Duration::from_millis(1)).await;
                Ok(b"value".to_vec())
            }
        }
    })
    .await
    .expect("prime");

    // delta_ema * beta = 1 * 2 = 2ms.  XFetch would need
    // fresh_until - now <= 2 * (-ln(r)).  This is very unlikely
    // to trigger since fresh_until is 500ms away.
    for _ in 0..10 {
        let prev = recomputes.load(Ordering::SeqCst);
        cf.run("k", {
            let recomputes = recomputes.clone();
            move || {
                let recomputes = recomputes.clone();
                async move {
                    recomputes.fetch_add(1, Ordering::SeqCst);
                    sleep(Duration::from_millis(1)).await;
                    Ok(b"value".to_vec())
                }
            }
        })
        .await
        .expect("read");

        sleep(Duration::from_millis(10)).await;

        // If it somehow triggered, note it but don't fail.
        if prev < recomputes.load(Ordering::SeqCst) {
            break;
        }
    }

    // At most one extra recompute (the initial prime).
    assert!(
        recomputes.load(Ordering::SeqCst) <= 2,
        "fast compute should rarely trigger XFetch"
    );
}

/// Concurrent readers during an XFetch-triggered background refresh share the
/// same ongoing recompute and only one background refresh starts.
#[tokio::test]
async fn xfetch_deduplicates_concurrent_background_refreshes() {
    let cache = support::MemoryCache::default();
    let metrics = TestMetrics::default();
    let cf = CacheFlight::with_metrics(cache, metrics.clone())
        .ttl(Duration::from_millis(500))
        .probabilistic_expiry(10.0);

    let recomputes = Arc::new(AtomicUsize::new(0));

    // Prime the cache.
    cf.run("k", {
        let recomputes = recomputes.clone();
        move || {
            let recomputes = recomputes.clone();
            async move {
                recomputes.fetch_add(1, Ordering::SeqCst);
                sleep(Duration::from_millis(80)).await;
                Ok(b"value".to_vec())
            }
        }
    })
    .await
    .expect("prime");
    assert_eq!(recomputes.load(Ordering::SeqCst), 1);

    // Fire 10 concurrent reads.  The first one whose XFetch check fires
    // starts a background refresh; the rest should join the existing flight.
    let mut tasks = Vec::new();
    for _ in 0..10 {
        let cf = cf.clone();
        let recomputes = recomputes.clone();
        tasks.push(tokio::spawn(async move {
            cf.run("k", move || {
                let recomputes = recomputes.clone();
                async move {
                    recomputes.fetch_add(1, Ordering::SeqCst);
                    sleep(Duration::from_millis(80)).await;
                    Ok(b"value".to_vec())
                }
            })
            .await
        }));
    }

    for task in tasks {
        let result = task.await.expect("join").expect("read");
        assert_eq!(result.state(), LookupState::CacheHit);
    }

    // Wait for background refresh to complete and update the cache.
    sleep(Duration::from_millis(300)).await;

    // Only 2 recomputes: initial prime + 1 background refresh.
    assert_eq!(
        recomputes.load(Ordering::SeqCst),
        2,
        "concurrent reads should deduplicate to one background refresh"
    );
    assert!(
        metrics.snapshot().xfetch_early_refreshes >= 1,
        "at least one XFetch early refresh should have been triggered"
    );

    // At least one deduplicated event should have occurred.
    let dedup_count = metrics
        .snapshot()
        .deduplicated
        .iter()
        .filter(|r| {
            matches!(
                r,
                cacheflight::RecomputeReason::ProbabilisticEarlyExpiration
            )
        })
        .count();
    assert!(
        dedup_count >= 1,
        "should have deduplicated XFetch triggers, got {dedup_count}"
    );
}

/// Multiple XFetch early-refresh events for different keys work independently.
#[tokio::test]
async fn xfetch_independent_keys() {
    let cache = support::MemoryCache::default();
    let metrics = TestMetrics::default();
    let cf = CacheFlight::with_metrics(cache, metrics.clone())
        .ttl(Duration::from_millis(500))
        .probabilistic_expiry(10.0);

    let recomputes = Arc::new(AtomicUsize::new(0));

    // Prime two independent keys.
    for key in ["a", "b"] {
        cf.run(key, {
            let recomputes = recomputes.clone();
            move || {
                let recomputes = recomputes.clone();
                async move {
                    recomputes.fetch_add(1, Ordering::SeqCst);
                    sleep(Duration::from_millis(80)).await;
                    Ok(b"value".to_vec())
                }
            }
        })
        .await
        .expect("prime");
    }
    assert_eq!(recomputes.load(Ordering::SeqCst), 2);

    let refreshes_before = metrics.snapshot().xfetch_early_refreshes;

    // Read keys until both have triggered at least one XFetch refresh.
    for _ in 0..30 {
        for key in ["a", "b"] {
            cf.run(key, {
                let recomputes = recomputes.clone();
                move || {
                    let recomputes = recomputes.clone();
                    async move {
                        recomputes.fetch_add(1, Ordering::SeqCst);
                        sleep(Duration::from_millis(80)).await;
                        Ok(b"value".to_vec())
                    }
                }
            })
            .await
            .expect("read");
        }

        sleep(Duration::from_millis(15)).await;

        if metrics.snapshot().xfetch_early_refreshes >= refreshes_before + 2 {
            return;
        }
    }

    panic!(
        "both keys should trigger XFetch: refreshes = {} (expected >= {})",
        metrics.snapshot().xfetch_early_refreshes,
        refreshes_before + 2,
    );
}
