mod common;

use bytes::Bytes;
use common::MemoryCache;
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use singleflight::{HttpSingleFlightLayer, Result};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::time::sleep;
use tower::{Layer, ServiceExt, service_fn};

// Run with:
// cargo run --example tower_layer
//
// This is the ergonomic HTTP middleware path.
// The user only configures:
// - the cache backend
// - the TTL behavior
// - optional request selection or key customization
//
// The middleware handles:
// - request deduplication
// - cache lookup and write
// - stale-while-revalidate
// - HTTP status filtering
// - response body buffering and replay

async fn body_text(response: Response<Full<Bytes>>) -> String {
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body should collect")
        .to_bytes();

    String::from_utf8(bytes.to_vec()).expect("body should be valid utf-8")
}

#[tokio::main]
async fn main() -> Result<()> {
    let cache = MemoryCache::new();
    let upstream_calls = Arc::new(AtomicUsize::new(0));

    // This is the whole middleware setup that application code usually needs.
    let layer = HttpSingleFlightLayer::new(cache, Duration::from_secs(30))
        .stale_while_revalidate(Duration::from_secs(120))
        .predicate(|request: &Request<()>| request.uri().path().starts_with("/products"))
        .cache_status(|status| status.is_success());

    let service = layer.layer(service_fn({
        let upstream_calls = upstream_calls.clone();
        move |request: Request<()>| {
            let upstream_calls = upstream_calls.clone();

            async move {
                let invocation = upstream_calls.fetch_add(1, Ordering::SeqCst) + 1;
                println!(
                    "origin service call #{invocation} for {} {}",
                    request.method(),
                    request.uri()
                );

                sleep(Duration::from_millis(100)).await;

                Ok::<_, std::io::Error>(
                    Response::builder()
                        .status(StatusCode::OK)
                        .body(Full::new(Bytes::from(format!(
                            "response for {}",
                            request.uri()
                        ))))
                        .expect("response should build"),
                )
            }
        }
    }));

    let request = || {
        Request::builder()
            .uri("/products/42?expand=reviews")
            .body(())
            .expect("request should build")
    };

    let one = service
        .clone()
        .oneshot(request())
        .await
        .expect("request should succeed");
    let two = service
        .clone()
        .oneshot(request())
        .await
        .expect("request should succeed");
    let three = service
        .clone()
        .oneshot(request())
        .await
        .expect("request should succeed");

    println!("response one: {}", body_text(one).await);
    println!("response two: {}", body_text(two).await);
    println!("response three: {}", body_text(three).await);
    println!("upstream calls: {}", upstream_calls.load(Ordering::SeqCst));
    println!();
    println!("Ergonomic summary:");
    println!("1. implement CacheBackend for your cache");
    println!("2. build HttpSingleFlightLayer with TTLs and optional filters");
    println!("3. wrap your Tower or Axum service");
    println!("4. the middleware handles dedupe and cache replay for you");

    Ok(())
}
