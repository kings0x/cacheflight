mod common;

use axum::{
    Router,
    body::{Body, to_bytes},
    error_handling::HandleErrorLayer,
    extract::Path,
    http::{Request, StatusCode},
    response::IntoResponse,
    routing::get,
};
use common::MemoryCache;
use singleflight_rs::{HttpSingleFlightLayer, Result};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::time::sleep;
use tower::{ServiceBuilder, ServiceExt};

// Run with:
// cargo run --example axum_router
//
// This is the shortest realistic Axum-style setup.
// The app only needs to:
// 1. provide a cache backend
// 2. build `HttpSingleFlightLayer`
// 3. attach it to the router
//
// Everything else - cache lookup, in-flight deduplication, stale handling,
// and replaying cached responses - is handled by the middleware.

#[tokio::main]
async fn main() -> Result<()> {
    let cache = MemoryCache::new();
    let origin_calls = Arc::new(AtomicUsize::new(0));

    let layer = HttpSingleFlightLayer::new(cache, Duration::from_secs(30))
        .stale_while_revalidate(Duration::from_secs(120))
        .predicate(|request: &Request<Body>| request.uri().path().starts_with("/products"))
        .cache_status(|status| status.is_success());

    let app = Router::new()
        .route(
            "/products/{id}",
            get({
                let origin_calls = origin_calls.clone();
                move |Path(id): Path<u64>| {
                    let origin_calls = origin_calls.clone();
                    async move {
                        let invocation = origin_calls.fetch_add(1, Ordering::SeqCst) + 1;
                        println!("origin handler call #{invocation} for product {id}");

                        sleep(Duration::from_millis(100)).await;

                        (
                            StatusCode::OK,
                            format!(r#"{{"id":{id},"name":"product-{id}"}}"#),
                        )
                            .into_response()
                    }
                }
            }),
        )
        .layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(
                    |error: singleflight_rs::Error| async move {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("singleflight middleware error: {error}"),
                        )
                    },
                ))
                .layer(layer),
        );

    let request = || {
        Request::builder()
            .uri("/products/42")
            .body(Body::empty())
            .expect("request should build")
    };

    let first = app
        .clone()
        .oneshot(request())
        .await
        .map_err(singleflight_rs::Error::operation)?;
    let second = app
        .clone()
        .oneshot(request())
        .await
        .map_err(singleflight_rs::Error::operation)?;
    let third = app
        .clone()
        .oneshot(request())
        .await
        .map_err(singleflight_rs::Error::operation)?;

    println!(
        "response one: {}",
        String::from_utf8_lossy(&to_bytes(first.into_body(), usize::MAX).await.expect("body"))
    );
    println!(
        "response two: {}",
        String::from_utf8_lossy(
            &to_bytes(second.into_body(), usize::MAX)
                .await
                .expect("body")
        )
    );
    println!(
        "response three: {}",
        String::from_utf8_lossy(&to_bytes(third.into_body(), usize::MAX).await.expect("body"))
    );
    println!("origin calls: {}", origin_calls.load(Ordering::SeqCst));
    println!();
    println!("This is the intended Axum usage shape:");
    println!("let layer = HttpSingleFlightLayer::new(cache, ttl)");
    println!("    .stale_while_revalidate(stale_ttl)");
    println!("    .predicate(|req| ...);");
    println!("let app = Router::new().route(...).layer(");
    println!("    ServiceBuilder::new()");
    println!("        .layer(HandleErrorLayer::new(...))");
    println!("        .layer(layer)");
    println!(");");

    Ok(())
}
