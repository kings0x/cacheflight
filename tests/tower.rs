mod support;

use singleflight_rs::{
    BytesPolicy, CachePolicy, Error, Result, SingleFlight, SingleFlightLayer, TowerCachePolicy,
};
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use support::{MemoryCache, wait_until};
use tokio::{sync::Notify, time::sleep};
use tower::{Layer, ServiceExt, service_fn};

#[derive(Clone, Default)]
struct StringPolicy;

impl TowerCachePolicy<String, String> for StringPolicy {
    fn cache_key(&self, request: &String) -> Option<String> {
        Some(request.clone())
    }

    fn encode_response(&self, response: &String) -> Result<Vec<u8>> {
        Ok(response.as_bytes().to_vec())
    }

    fn decode_response(&self, bytes: &[u8]) -> Result<String> {
        String::from_utf8(bytes.to_vec()).map_err(Error::decode)
    }
}

#[derive(Clone, Default)]
struct OptionalStringPolicy;

impl TowerCachePolicy<String, String> for OptionalStringPolicy {
    fn cache_key(&self, request: &String) -> Option<String> {
        (!request.starts_with("skip:")).then(|| request.clone())
    }

    fn encode_response(&self, response: &String) -> Result<Vec<u8>> {
        Ok(response.as_bytes().to_vec())
    }

    fn decode_response(&self, bytes: &[u8]) -> Result<String> {
        String::from_utf8(bytes.to_vec()).map_err(Error::decode)
    }
}

#[tokio::test]
async fn tower_layer_deduplicates_and_caches_custom_response_types() {
    let cache = MemoryCache::default();
    let singleflight = SingleFlight::new(cache, CachePolicy::new(Duration::from_millis(250)));
    let calls = Arc::new(AtomicUsize::new(0));

    let service = SingleFlightLayer::new(singleflight, StringPolicy).layer(service_fn({
        let calls = calls.clone();
        move |request: String| {
            let calls = calls.clone();
            async move {
                let current = calls.fetch_add(1, Ordering::SeqCst) + 1;
                sleep(Duration::from_millis(40)).await;
                Ok::<_, io::Error>(format!("{request}-response-{current}"))
            }
        }
    }));

    let mut tasks = Vec::new();
    for _ in 0..6 {
        let service = service.clone();
        tasks.push(tokio::spawn(async move {
            service
                .oneshot("inventory".to_owned())
                .await
                .expect("middleware request should succeed")
        }));
    }

    for task in tasks {
        assert_eq!(
            task.await.expect("task panicked"),
            "inventory-response-1".to_owned()
        );
    }

    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let cached = service
        .clone()
        .oneshot("inventory".to_owned())
        .await
        .expect("cached response should succeed");
    assert_eq!(cached, "inventory-response-1");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn tower_layer_serves_stale_while_refreshing_in_the_background() {
    let cache = MemoryCache::default();
    let singleflight = SingleFlight::new(
        cache,
        CachePolicy::new(Duration::from_millis(120))
            .with_stale_while_revalidate(Duration::from_millis(250)),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let refresh_started = Arc::new(Notify::new());
    let release_refresh = Arc::new(Notify::new());

    let service = SingleFlightLayer::new(
        singleflight,
        BytesPolicy::new(|request: &String| request.clone()),
    )
    .layer(service_fn({
        let calls = calls.clone();
        let refresh_started = refresh_started.clone();
        let release_refresh = release_refresh.clone();
        move |_request: String| {
            let calls = calls.clone();
            let refresh_started = refresh_started.clone();
            let release_refresh = release_refresh.clone();
            async move {
                let current = calls.fetch_add(1, Ordering::SeqCst) + 1;

                if current == 2 {
                    refresh_started.notify_waiters();
                    release_refresh.notified().await;
                } else {
                    sleep(Duration::from_millis(80)).await;
                }

                Ok::<_, io::Error>(format!("payload-{current}").into_bytes())
            }
        }
    }));

    let first = service
        .clone()
        .oneshot("catalog".to_owned())
        .await
        .expect("initial request should succeed");
    assert_eq!(first, b"payload-1".to_vec());

    sleep(Duration::from_millis(140)).await;

    let started = Instant::now();
    let stale_one = service
        .clone()
        .oneshot("catalog".to_owned())
        .await
        .expect("stale request should succeed");
    assert!(started.elapsed() < Duration::from_millis(50));
    assert_eq!(stale_one, b"payload-1".to_vec());

    refresh_started.notified().await;

    let stale_two = service
        .clone()
        .oneshot("catalog".to_owned())
        .await
        .expect("stale request should succeed");
    assert_eq!(stale_two, b"payload-1".to_vec());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    release_refresh.notify_waiters();

    wait_until(Duration::from_secs(1), Duration::from_millis(20), || {
        calls.load(Ordering::SeqCst) == 2
    })
    .await;

    let mut refreshed = None;
    for _ in 0..20 {
        let result = service
            .clone()
            .oneshot("catalog".to_owned())
            .await
            .expect("refreshed request should succeed");

        if result == b"payload-2".to_vec() {
            refreshed = Some(result);
            break;
        }

        sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(
        refreshed.expect("background refresh should publish the new payload"),
        b"payload-2".to_vec()
    );
}

#[tokio::test]
async fn tower_layer_can_bypass_caching_for_selected_requests() {
    let cache = MemoryCache::default();
    let singleflight = SingleFlight::new(cache, CachePolicy::new(Duration::from_secs(1)));
    let calls = Arc::new(AtomicUsize::new(0));

    let service = SingleFlightLayer::new(singleflight, OptionalStringPolicy).layer(service_fn({
        let calls = calls.clone();
        move |request: String| {
            let calls = calls.clone();
            async move {
                let current = calls.fetch_add(1, Ordering::SeqCst) + 1;
                Ok::<_, io::Error>(format!("{request}-{current}"))
            }
        }
    }));

    let first = service
        .clone()
        .oneshot("skip:live".to_owned())
        .await
        .expect("request should succeed");
    let second = service
        .clone()
        .oneshot("skip:live".to_owned())
        .await
        .expect("request should succeed");

    assert_eq!(first, "skip:live-1");
    assert_eq!(second, "skip:live-2");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
