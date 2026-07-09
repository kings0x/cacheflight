mod support;

use cacheflight::{
    CacheFlight, CacheMissReason, Error, LookupState, RecomputeOutcome, RecomputeReason,
};
use std::{
    error::Error as _,
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use support::{MemoryCache, TestMetrics, wait_until};
use tokio::{sync::Notify, time::sleep};

#[tokio::test]
async fn deduplicates_concurrent_cold_requests_and_populates_cache() {
    let cache = MemoryCache::default();
    let metrics = TestMetrics::default();
    let cf = CacheFlight::with_metrics(cache, metrics.clone()).ttl(Duration::from_millis(250));
    let recomputes = Arc::new(AtomicUsize::new(0));

    let mut tasks = Vec::new();
    for _ in 0..10 {
        let cf = cf.clone();
        let recomputes = recomputes.clone();

        tasks.push(tokio::spawn(async move {
            cf.run("user:42", move || {
                let recomputes = recomputes.clone();
                async move {
                    recomputes.fetch_add(1, Ordering::SeqCst);
                    sleep(Duration::from_millis(40)).await;
                    Ok(b"payload".to_vec())
                }
            })
            .await
        }));
    }

    let mut recomputed = 0;
    let mut shared = 0;
    for task in tasks {
        let result = task.await.expect("task panicked").expect("request failed");
        assert_eq!(result.value(), b"payload");

        match result.state() {
            LookupState::Recomputed => recomputed += 1,
            LookupState::Shared => shared += 1,
            other => panic!("unexpected state: {other:?}"),
        }
    }

    assert_eq!(recomputes.load(Ordering::SeqCst), 1);
    assert_eq!(recomputed, 1);
    assert_eq!(shared, 9);

    let cached = cf
        .run("user:42", || async {
            unreachable!("cache hit should bypass recompute")
        })
        .await
        .expect("cache hit should succeed");
    assert_eq!(cached.state(), LookupState::CacheHit);
    assert_eq!(cached.value(), b"payload");

    let snapshot = metrics.snapshot();
    assert_eq!(
        snapshot
            .deduplicated
            .iter()
            .filter(|&&reason| reason == RecomputeReason::ColdMiss)
            .count(),
        9
    );
    assert_eq!(
        snapshot
            .recompute_started
            .iter()
            .filter(|&&reason| reason == RecomputeReason::ColdMiss)
            .count(),
        1
    );
    assert_eq!(
        snapshot
            .recompute_finished
            .iter()
            .filter(|&&(reason, outcome)| {
                reason == RecomputeReason::ColdMiss && outcome == RecomputeOutcome::Success
            })
            .count(),
        1
    );
}

#[tokio::test]
async fn serves_stale_immediately_and_refreshes_once_in_background() {
    let cache = MemoryCache::default();
    let metrics = TestMetrics::default();
    let cf = CacheFlight::with_metrics(cache, metrics.clone())
        .stale_while_revalidate(Duration::from_millis(120), Duration::from_millis(250));
    let recomputes = Arc::new(AtomicUsize::new(0));
    let refresh_started = Arc::new(Notify::new());
    let release_refresh = Arc::new(Notify::new());

    let recomputes_for_initial = recomputes.clone();
    let initial = cf
        .run("profile:1", move || {
            let recomputes = recomputes_for_initial.clone();
            async move {
                let attempt = recomputes.fetch_add(1, Ordering::SeqCst) + 1;
                sleep(Duration::from_millis(20)).await;
                Ok(format!("value-{attempt}").into_bytes())
            }
        })
        .await
        .expect("initial fill should succeed");
    assert_eq!(initial.state(), LookupState::Recomputed);
    assert_eq!(initial.value(), b"value-1");

    sleep(Duration::from_millis(140)).await;

    let refresh_work = {
        let recomputes = recomputes.clone();
        let refresh_started = refresh_started.clone();
        let release_refresh = release_refresh.clone();
        move || {
            let recomputes = recomputes.clone();
            let refresh_started = refresh_started.clone();
            let release_refresh = release_refresh.clone();
            async move {
                let attempt = recomputes.fetch_add(1, Ordering::SeqCst) + 1;

                if attempt == 2 {
                    refresh_started.notify_waiters();
                    release_refresh.notified().await;
                }

                Ok(format!("value-{attempt}").into_bytes())
            }
        }
    };

    let started = Instant::now();
    let stale_one = cf
        .run("profile:1", refresh_work.clone())
        .await
        .expect("stale request should succeed");
    assert!(started.elapsed() < Duration::from_millis(50));
    assert_eq!(stale_one.state(), LookupState::Stale);
    assert_eq!(stale_one.value(), b"value-1");

    refresh_started.notified().await;

    let stale_two = cf
        .run("profile:1", refresh_work.clone())
        .await
        .expect("stale request should succeed");
    assert_eq!(stale_two.state(), LookupState::Stale);
    assert_eq!(stale_two.value(), b"value-1");

    assert_eq!(recomputes.load(Ordering::SeqCst), 2);
    release_refresh.notify_waiters();

    wait_until(Duration::from_secs(1), Duration::from_millis(20), || {
        recomputes.load(Ordering::SeqCst) == 2
    })
    .await;

    let mut refreshed = None;
    for _ in 0..20 {
        let result = cf
            .run("profile:1", || async {
                unreachable!("background refresh should have repopulated the cache")
            })
            .await
            .expect("cached response should succeed");

        if result.value() == b"value-2" {
            refreshed = Some(result);
            break;
        }

        sleep(Duration::from_millis(10)).await;
    }

    let refreshed = refreshed.expect("background refresh should eventually publish the new value");
    assert!(matches!(
        refreshed.state(),
        LookupState::CacheHit | LookupState::Stale
    ));
    assert_eq!(refreshed.value(), b"value-2");

    let snapshot = metrics.snapshot();
    assert!(snapshot.stale_hits >= 2);
    assert!(
        snapshot
            .deduplicated
            .iter()
            .filter(|&&reason| reason == RecomputeReason::StaleWhileRevalidate)
            .count()
            >= 1
    );
    assert!(
        snapshot
            .recompute_started
            .iter()
            .filter(|&&reason| reason == RecomputeReason::StaleWhileRevalidate)
            .count()
            >= 1
    );
}

#[tokio::test]
async fn expired_entries_block_when_stale_while_revalidate_is_disabled() {
    let cache = MemoryCache::default();
    let cf = CacheFlight::new(cache).ttl(Duration::from_millis(40));
    let recomputes = Arc::new(AtomicUsize::new(0));

    let recomputes_for_initial = recomputes.clone();
    cf.run("session:1", move || {
        let recomputes = recomputes_for_initial.clone();
        async move {
            let attempt = recomputes.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(format!("session-{attempt}").into_bytes())
        }
    })
    .await
    .expect("initial fill should succeed");

    sleep(Duration::from_millis(60)).await;

    let started = Instant::now();
    let result = cf
        .run("session:1", move || {
            let recomputes = recomputes.clone();
            async move {
                let attempt = recomputes.fetch_add(1, Ordering::SeqCst) + 1;
                sleep(Duration::from_millis(50)).await;
                Ok(format!("session-{attempt}").into_bytes())
            }
        })
        .await
        .expect("expired entry should recompute");

    assert!(started.elapsed() >= Duration::from_millis(45));
    assert_eq!(result.state(), LookupState::Recomputed);
    assert_eq!(result.value(), b"session-2");
}

#[tokio::test]
async fn retries_after_recompute_failure_and_shares_the_error() {
    let cache = MemoryCache::default();
    let cf = CacheFlight::new(cache).ttl(Duration::from_millis(250));
    let attempts = Arc::new(AtomicUsize::new(0));

    let failing_work = {
        let attempts = attempts.clone();
        move || {
            let attempts = attempts.clone();
            async move {
                let current = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                sleep(Duration::from_millis(30)).await;

                if current == 1 {
                    Err(Error::operation(io::Error::other("boom")))
                } else {
                    Ok(format!("value-{current}").into_bytes())
                }
            }
        }
    };

    let request_one = {
        let cf = cf.clone();
        let failing_work = failing_work.clone();
        tokio::spawn(async move { cf.run("orders", failing_work).await })
    };
    let request_two = {
        let cf = cf.clone();
        let failing_work = failing_work.clone();
        tokio::spawn(async move { cf.run("orders", failing_work).await })
    };

    for task in [request_one, request_two] {
        let error = task
            .await
            .expect("task panicked")
            .expect_err("both callers should receive the shared error");
        assert_eq!(error.to_string(), "recomputation failed");
        assert_eq!(
            error
                .source()
                .expect("source should be present")
                .to_string(),
            "boom"
        );
    }

    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    let retry = cf
        .run("orders", failing_work)
        .await
        .expect("retry should succeed");
    assert_eq!(retry.state(), LookupState::Recomputed);
    assert_eq!(retry.value(), b"value-2");
}

#[tokio::test]
async fn ignores_invalid_cached_entries_and_records_the_miss_reason() {
    let cache = MemoryCache::default();
    cache
        .insert_raw(
            "broken",
            b"not-a-valid-cacheflight-entry".to_vec(),
            Duration::from_secs(1),
        )
        .await;

    let metrics = TestMetrics::default();
    let cf = CacheFlight::with_metrics(cache, metrics.clone()).ttl(Duration::from_millis(250));

    let value = cf
        .run("broken", || async { Ok(b"recovered".to_vec()) })
        .await
        .expect("invalid entries should be recomputed");

    assert_eq!(value.state(), LookupState::Recomputed);
    assert_eq!(value.value(), b"recovered");
    assert!(
        metrics
            .snapshot()
            .misses
            .contains(&CacheMissReason::Invalid)
    );
}

#[tokio::test]
async fn cache_read_failures_are_reported_and_recomputed() {
    let cache = MemoryCache::default();
    cache.fail_one_get();

    let metrics = TestMetrics::default();
    let cf = CacheFlight::with_metrics(cache, metrics.clone()).ttl(Duration::from_millis(250));

    let value = cf
        .run("read-error", || async { Ok(b"recovered".to_vec()) })
        .await
        .expect("cache read failures should fall back to recompute");

    assert_eq!(value.state(), LookupState::Recomputed);
    assert_eq!(value.value(), b"recovered");

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.cache_read_failures, 1);
    assert!(snapshot.misses.contains(&CacheMissReason::BackendError));
}

#[tokio::test]
async fn cache_write_failures_are_reported_without_failing_the_request() {
    let cache = MemoryCache::default();
    cache.fail_one_set();

    let metrics = TestMetrics::default();
    let cf =
        CacheFlight::with_metrics(cache.clone(), metrics.clone()).ttl(Duration::from_millis(250));
    let recomputes = Arc::new(AtomicUsize::new(0));

    let work = {
        let recomputes = recomputes.clone();
        move || {
            let recomputes = recomputes.clone();
            async move {
                let attempt = recomputes.fetch_add(1, Ordering::SeqCst) + 1;
                Ok(format!("value-{attempt}").into_bytes())
            }
        }
    };

    let first = cf
        .run("payments", work.clone())
        .await
        .expect("cache write failures should not fail the caller");
    assert_eq!(first.value(), b"value-1");

    let second = cf
        .run("payments", work)
        .await
        .expect("missing cache entry should recompute");
    assert_eq!(second.value(), b"value-2");
    assert_eq!(recomputes.load(Ordering::SeqCst), 2);
    assert_eq!(metrics.snapshot().cache_write_failures, 1);
}
