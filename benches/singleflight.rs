use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use singleflight::{CacheBackend, CachePolicy, Result, SingleFlight, async_trait};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{runtime::Runtime, sync::Mutex};

#[derive(Clone, Default)]
struct MemoryCache {
    entries: Arc<Mutex<HashMap<String, CacheEntry>>>,
}

#[derive(Clone)]
struct CacheEntry {
    value: Vec<u8>,
    expires_at: Instant,
}

#[async_trait]
impl CacheBackend for MemoryCache {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let mut entries = self.entries.lock().await;

        match entries.get(key) {
            Some(entry) if entry.expires_at > Instant::now() => Ok(Some(entry.value.clone())),
            Some(_) => {
                entries.remove(key);
                Ok(None)
            }
            None => Ok(None),
        }
    }

    async fn set(&self, key: &str, value: Vec<u8>, ttl: Duration) -> Result<()> {
        self.entries.lock().await.insert(
            key.to_owned(),
            CacheEntry {
                value,
                expires_at: Instant::now() + ttl,
            },
        );
        Ok(())
    }
}

fn bench_singleflight(c: &mut Criterion) {
    let runtime = Runtime::new().expect("tokio runtime should build");
    let mut group = c.benchmark_group("singleflight");

    group.bench_function("cache_hit", |b| {
        let singleflight = SingleFlight::new(
            MemoryCache::default(),
            CachePolicy::new(Duration::from_secs(30)),
        );

        runtime.block_on(async {
            singleflight
                .get_or_compute("cache-hit", || async { Ok(b"payload".to_vec()) })
                .await
                .expect("priming cache should succeed");
        });

        b.to_async(&runtime).iter(|| async {
            let result = singleflight
                .get_or_compute("cache-hit", || async {
                    unreachable!("cache-hit benchmark should not recompute")
                })
                .await
                .expect("cache hit should succeed");

            criterion::black_box(result);
        });
    });

    group.bench_function("contended_cold_miss_32_callers", |b| {
        let singleflight = SingleFlight::new(
            MemoryCache::default(),
            CachePolicy::new(Duration::from_secs(30)),
        );
        let sequence = AtomicUsize::new(0);

        b.to_async(&runtime).iter_batched(
            || format!("cold-miss-{}", sequence.fetch_add(1, Ordering::Relaxed)),
            |key| {
                let singleflight = singleflight.clone();

                async move {
                    let recomputes = Arc::new(AtomicUsize::new(0));
                    let mut tasks = Vec::with_capacity(32);

                    for _ in 0..32 {
                        let singleflight = singleflight.clone();
                        let recomputes = Arc::clone(&recomputes);
                        let key = key.clone();

                        tasks.push(tokio::spawn(async move {
                            singleflight
                                .get_or_compute(key, move || {
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

    group.finish();
}

criterion_group!(benches, bench_singleflight);
criterion_main!(benches);
