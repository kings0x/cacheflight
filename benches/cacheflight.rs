use cacheflight::{CacheFlight, MemoryCache};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::runtime::Runtime;

fn bench_cacheflight(c: &mut Criterion) {
    let runtime = Runtime::new().expect("tokio runtime should build");
    let mut group = c.benchmark_group("cacheflight");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function("cache_hit", |b| {
        let cf = CacheFlight::new(MemoryCache::default()).ttl(Duration::from_secs(30));

        runtime.block_on(async {
            cf.run("cache-hit", || async { Ok(b"payload".to_vec()) })
                .await
                .expect("priming cache should succeed");
        });

        b.to_async(&runtime).iter(|| async {
            let result = cf
                .run("cache-hit", || async {
                    unreachable!("cache-hit benchmark should not recompute")
                })
                .await
                .expect("cache hit should succeed");

            criterion::black_box(result);
        });
    });

    group.bench_function("cold_miss_single", |b| {
        let counter = AtomicUsize::new(0);

        b.to_async(&runtime).iter_batched(
            || {
                let cf = CacheFlight::new(MemoryCache::default()).ttl(Duration::from_secs(30));
                let key = format!("cold-single-{}", counter.fetch_add(1, Ordering::Relaxed));
                (cf, key)
            },
            |(cf, key)| async move {
                let result = cf
                    .run(&key, || async { Ok(b"payload".to_vec()) })
                    .await
                    .expect("cold miss should succeed");
                criterion::black_box(result);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("contended_cold_miss_32_callers", |b| {
        let cf = CacheFlight::new(MemoryCache::default()).ttl(Duration::from_secs(30));
        let sequence = AtomicUsize::new(0);

        b.to_async(&runtime).iter_batched(
            || format!("cold-miss-{}", sequence.fetch_add(1, Ordering::Relaxed)),
            |key| {
                let cf = cf.clone();

                async move {
                    let recomputes = Arc::new(AtomicUsize::new(0));
                    let mut tasks = Vec::with_capacity(32);

                    for _ in 0..32 {
                        let cf = cf.clone();
                        let recomputes = Arc::clone(&recomputes);
                        let key = key.clone();

                        tasks.push(tokio::spawn(async move {
                            cf.run(key, move || {
                                let recomputes = Arc::clone(&recomputes);

                                async move {
                                    recomputes.fetch_add(1, Ordering::SeqCst);
                                    Ok(b"payload".to_vec())
                                }
                            })
                            .await
                            .expect("cold miss should succeed")
                        }));
                    }

                    for task in tasks {
                        criterion::black_box(task.await.expect("task should complete"));
                    }

                    assert_eq!(recomputes.load(Ordering::SeqCst), 1);
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("stale_while_revalidate_hit", |b| {
        let counter = AtomicUsize::new(0);

        b.to_async(&runtime).iter_batched(
            || {
                let cf = CacheFlight::new(MemoryCache::default())
                    .stale_while_revalidate(Duration::from_millis(5), Duration::from_secs(30));
                let key = format!("stale-{}", counter.fetch_add(1, Ordering::Relaxed));
                (cf, key)
            },
            |(cf, key)| async move {
                // Prime — fresh for 5 ms.
                cf.run(&key, || async { Ok(b"payload".to_vec()) })
                    .await
                    .expect("prime");

                tokio::time::sleep(Duration::from_millis(10)).await;

                // Now the entry is stale.  This triggers a background refresh
                // and returns Stale immediately.
                let result = cf
                    .run(&key, || async { Ok(b"payload".to_vec()) })
                    .await
                    .expect("stale hit should succeed");

                criterion::black_box(result);
            },
            BatchSize::NumIterations(1),
        );
    });

    group.bench_function("xfetch_early_refresh", |b| {
        let counter = AtomicUsize::new(0);

        b.to_async(&runtime).iter_batched(
            || {
                let cf = CacheFlight::new(MemoryCache::default())
                    .ttl(Duration::from_millis(500))
                    .probabilistic_expiry(100.0);
                let key = format!("xfetch-{}", counter.fetch_add(1, Ordering::Relaxed));
                (cf, key)
            },
            |(cf, key)| async move {
                // Prime with a slow compute so delta_ema is large.
                cf.run(&key, || async {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok(b"payload".to_vec())
                })
                .await
                .expect("prime");

                // Second read — XFetch should trigger an early refresh
                // because beta is high and delta_ema is large.
                let result = cf
                    .run(&key, || async { Ok(b"payload".to_vec()) })
                    .await
                    .expect("xfetch read should succeed");

                criterion::black_box(result);
            },
            BatchSize::NumIterations(1),
        );
    });

    group.bench_function("payload_100b", |b| {
        let counter = AtomicUsize::new(0);
        let small = vec![0u8; 100];

        b.to_async(&runtime).iter_batched(
            || {
                let cf = CacheFlight::new(MemoryCache::default()).ttl(Duration::from_secs(30));
                let key = format!("small-{}", counter.fetch_add(1, Ordering::Relaxed));
                let value = small.clone();
                (cf, key, value)
            },
            |(cf, key, value)| async move {
                let result = cf
                    .run(&key, move || {
                        let v = value.clone();
                        async move { Ok(v) }
                    })
                    .await
                    .expect("cold miss should succeed");
                criterion::black_box(result);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("payload_100kb", |b| {
        let counter = AtomicUsize::new(0);
        let large = vec![0u8; 100_000];

        b.to_async(&runtime).iter_batched(
            || {
                let cf = CacheFlight::new(MemoryCache::default()).ttl(Duration::from_secs(30));
                let key = format!("large-{}", counter.fetch_add(1, Ordering::Relaxed));
                let value = large.clone();
                (cf, key, value)
            },
            |(cf, key, value)| async move {
                let result = cf
                    .run(&key, move || {
                        let v = value.clone();
                        async move { Ok(v) }
                    })
                    .await
                    .expect("cold miss should succeed");
                criterion::black_box(result);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("high_contention_mixed", |b| {
        let cf = CacheFlight::new(MemoryCache::default())
            .stale_while_revalidate(Duration::from_millis(5), Duration::from_secs(30));

        // Prime 4 keys — 2 fresh (just primed), 2 about to go stale.
        runtime.block_on(async {
            for i in 0..2 {
                cf.run(&format!("mixed-{i}"), || async { Ok(b"payload".to_vec()) })
                    .await
                    .expect("prime fresh");
            }
            for i in 2..4 {
                cf.run(&format!("mixed-{i}"), || async {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    Ok(b"payload".to_vec())
                })
                .await
                .expect("prime stale");
            }
            // Keys 2 and 3 are now stale (primed >5 ms ago).
            tokio::time::sleep(Duration::from_millis(5)).await;
        });

        b.to_async(&runtime).iter(|| async {
            let mut tasks = Vec::with_capacity(32);

            for i in 0..32 {
                let cf = cf.clone();
                let key = format!("mixed-{}", i % 4);

                tasks.push(tokio::spawn(async move {
                    let result = cf
                        .run(&key, || async { Ok(b"payload".to_vec()) })
                        .await
                        .expect("mixed workload read should succeed");
                    criterion::black_box(result);
                }));
            }

            for task in tasks {
                let _ = task.await;
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_cacheflight);
criterion_main!(benches);
