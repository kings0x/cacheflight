mod support;

use bytes::Bytes;
use http::{
    Method, Request, Response, StatusCode,
    header::{ACCEPT, AUTHORIZATION, COOKIE, HOST, RANGE},
};
use http_body_util::{BodyExt, Full};
use singleflight::HttpSingleFlightLayer;
use std::{
    error::Error as _,
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use support::MemoryCache;
use tokio::time::sleep;
use tower::{Layer, ServiceExt, service_fn};

async fn response_body(response: Response<Full<Bytes>>) -> Bytes {
    response
        .into_body()
        .collect()
        .await
        .expect("body collection should succeed")
        .to_bytes()
}

#[tokio::test]
async fn http_layer_turns_internal_failures_into_http_500_by_default() {
    let cache = MemoryCache::default();

    let service = HttpSingleFlightLayer::new(cache, Duration::from_secs(1)).layer(service_fn(
        |_request: Request<()>| async move {
            Err::<Response<Full<Bytes>>, _>(io::Error::other("origin exploded"))
        },
    ));

    let response = service
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/users")
                .body(())
                .expect("request should build"),
        )
        .await
        .expect("HTTP layer should be infallible");

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        response_body(response).await,
        Bytes::from("singleflight middleware error")
    );
}

#[tokio::test]
async fn http_layer_defaults_to_safe_methods_and_success_statuses() {
    let cache = MemoryCache::default();
    let calls = Arc::new(AtomicUsize::new(0));

    let service = HttpSingleFlightLayer::new(cache, Duration::from_secs(1)).layer(service_fn({
        let calls = calls.clone();
        move |request: Request<()>| {
            let calls = calls.clone();
            async move {
                let current = calls.fetch_add(1, Ordering::SeqCst) + 1;
                let status = if request.uri().path() == "/boom" {
                    StatusCode::INTERNAL_SERVER_ERROR
                } else {
                    StatusCode::OK
                };

                Ok::<_, io::Error>(
                    Response::builder()
                        .status(status)
                        .body(Full::new(Bytes::from(format!(
                            "{}:{}:{current}",
                            request.method(),
                            request.uri().path()
                        ))))
                        .expect("response should build"),
                )
            }
        }
    }));

    let first_get = service
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/users")
                .body(())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");
    let second_get = service
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/users")
                .body(())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(response_body(first_get).await, Bytes::from("GET:/users:1"));
    assert_eq!(response_body(second_get).await, Bytes::from("GET:/users:1"));
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let first_post = service
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/users")
                .body(())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");
    let second_post = service
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/users")
                .body(())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(
        response_body(first_post).await,
        Bytes::from("POST:/users:2")
    );
    assert_eq!(
        response_body(second_post).await,
        Bytes::from("POST:/users:3")
    );

    let first_error = service
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/boom")
                .body(())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");
    let second_error = service
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/boom")
                .body(())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(first_error.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(second_error.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(response_body(first_error).await, Bytes::from("GET:/boom:4"));
    assert_eq!(
        response_body(second_error).await,
        Bytes::from("GET:/boom:5")
    );
}

#[tokio::test]
async fn http_layer_bypasses_private_and_range_requests_by_default() {
    let cache = MemoryCache::default();
    let calls = Arc::new(AtomicUsize::new(0));

    let service = HttpSingleFlightLayer::new(cache, Duration::from_secs(1)).layer(service_fn({
        let calls = calls.clone();
        move |request: Request<()>| {
            let calls = calls.clone();
            async move {
                let current = calls.fetch_add(1, Ordering::SeqCst) + 1;
                Ok::<_, io::Error>(
                    Response::builder()
                        .status(StatusCode::OK)
                        .body(Full::new(Bytes::from(format!(
                            "{}:{}:{current}",
                            request.method(),
                            request.uri().path()
                        ))))
                        .expect("response should build"),
                )
            }
        }
    }));

    let auth_one = service
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/private")
                .header(AUTHORIZATION, "Bearer token-a")
                .body(())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");
    let auth_two = service
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/private")
                .header(AUTHORIZATION, "Bearer token-a")
                .body(())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(response_body(auth_one).await, Bytes::from("GET:/private:1"));
    assert_eq!(response_body(auth_two).await, Bytes::from("GET:/private:2"));

    let cookie_one = service
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/private")
                .header(COOKIE, "session=abc")
                .body(())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");
    let cookie_two = service
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/private")
                .header(COOKIE, "session=abc")
                .body(())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(
        response_body(cookie_one).await,
        Bytes::from("GET:/private:3")
    );
    assert_eq!(
        response_body(cookie_two).await,
        Bytes::from("GET:/private:4")
    );

    let range_one = service
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/video")
                .header(RANGE, "bytes=0-99")
                .body(())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");
    let range_two = service
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/video")
                .header(RANGE, "bytes=0-99")
                .body(())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(response_body(range_one).await, Bytes::from("GET:/video:5"));
    assert_eq!(response_body(range_two).await, Bytes::from("GET:/video:6"));
}

#[tokio::test]
async fn http_layer_normalizes_query_order_in_default_keys() {
    let cache = MemoryCache::default();
    let calls = Arc::new(AtomicUsize::new(0));

    let service = HttpSingleFlightLayer::new(cache, Duration::from_secs(1)).layer(service_fn({
        let calls = calls.clone();
        move |request: Request<()>| {
            let calls = calls.clone();
            async move {
                let current = calls.fetch_add(1, Ordering::SeqCst) + 1;
                Ok::<_, io::Error>(
                    Response::builder()
                        .status(StatusCode::OK)
                        .body(Full::new(Bytes::from(format!(
                            "{}:{}:{current}",
                            request.method(),
                            request.uri()
                        ))))
                        .expect("response should build"),
                )
            }
        }
    }));

    let first = service
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/search?b=2&a=1")
                .body(())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");
    let second = service
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/search?a=1&b=2")
                .body(())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(
        response_body(first).await,
        Bytes::from("GET:/search?b=2&a=1:1")
    );
    assert_eq!(
        response_body(second).await,
        Bytes::from("GET:/search?b=2&a=1:1")
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn http_layer_default_keys_vary_by_host_and_accept_headers() {
    let cache = MemoryCache::default();
    let calls = Arc::new(AtomicUsize::new(0));

    let service = HttpSingleFlightLayer::new(cache, Duration::from_secs(1)).layer(service_fn({
        let calls = calls.clone();
        move |_request: Request<()>| {
            let calls = calls.clone();
            async move {
                let current = calls.fetch_add(1, Ordering::SeqCst) + 1;
                Ok::<_, io::Error>(
                    Response::builder()
                        .status(StatusCode::OK)
                        .body(Full::new(Bytes::from(format!("call-{current}"))))
                        .expect("response should build"),
                )
            }
        }
    }));

    let json_one = service
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/users")
                .header(HOST, "api-a.example")
                .header(ACCEPT, "application/json")
                .body(())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");
    let json_two = service
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/users")
                .header(HOST, "api-a.example")
                .header(ACCEPT, "application/json")
                .body(())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(response_body(json_one).await, Bytes::from("call-1"));
    assert_eq!(response_body(json_two).await, Bytes::from("call-1"));

    let html = service
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/users")
                .header(HOST, "api-a.example")
                .header(ACCEPT, "text/html")
                .body(())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");
    let other_host = service
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/users")
                .header(HOST, "api-b.example")
                .header(ACCEPT, "text/html")
                .body(())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(response_body(html).await, Bytes::from("call-2"));
    assert_eq!(response_body(other_host).await, Bytes::from("call-3"));
}

#[tokio::test]
async fn http_layer_supports_predicates_custom_keys_and_per_request_ttls() {
    let cache = MemoryCache::default();
    let calls = Arc::new(AtomicUsize::new(0));

    let service = HttpSingleFlightLayer::new(cache, Duration::from_millis(500))
        .predicate(|request: &Request<()>| request.uri().path().starts_with("/cache"))
        .key_with(|request: &Request<()>| format!("{}:{}", request.method(), request.uri().path()))
        .ttl_with(|request: &Request<()>| {
            if request.uri().path().starts_with("/cache/hot") {
                Duration::from_millis(50)
            } else {
                Duration::from_secs(1)
            }
        })
        .cache_status(|status| status.is_success())
        .layer(service_fn({
            let calls = calls.clone();
            move |request: Request<()>| {
                let calls = calls.clone();
                async move {
                    let current = calls.fetch_add(1, Ordering::SeqCst) + 1;
                    Ok::<_, io::Error>(
                        Response::builder()
                            .status(StatusCode::OK)
                            .body(Full::new(Bytes::from(format!(
                                "{}:{}:{current}",
                                request.method(),
                                request.uri().path()
                            ))))
                            .expect("response should build"),
                    )
                }
            }
        }));

    let skip_one = service
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/skip")
                .body(())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");
    let skip_two = service
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/skip")
                .body(())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(response_body(skip_one).await, Bytes::from("GET:/skip:1"));
    assert_eq!(response_body(skip_two).await, Bytes::from("GET:/skip:2"));

    let hot_one = service
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/cache/hot?a=1")
                .body(())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");
    let hot_two = service
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/cache/hot?b=2")
                .body(())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(
        response_body(hot_one).await,
        Bytes::from("GET:/cache/hot:3")
    );
    assert_eq!(
        response_body(hot_two).await,
        Bytes::from("GET:/cache/hot:3")
    );

    sleep(Duration::from_millis(80)).await;

    let hot_three = service
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/cache/hot?c=3")
                .body(())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(
        response_body(hot_three).await,
        Bytes::from("GET:/cache/hot:4")
    );

    let cold_one = service
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/cache/cold")
                .body(())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    sleep(Duration::from_millis(80)).await;

    let cold_two = service
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/cache/cold")
                .body(())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(
        response_body(cold_one).await,
        Bytes::from("GET:/cache/cold:5")
    );
    assert_eq!(
        response_body(cold_two).await,
        Bytes::from("GET:/cache/cold:5")
    );
}

#[tokio::test]
async fn http_layer_enforces_max_response_bytes() {
    let cache = MemoryCache::default();
    let calls = Arc::new(AtomicUsize::new(0));

    let service = HttpSingleFlightLayer::new(cache, Duration::from_secs(1))
        .max_response_bytes(8)
        .error_response_with(|error| {
            let body = error
                .source()
                .map(|source| source.to_string())
                .unwrap_or_else(|| error.to_string());

            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Full::new(Bytes::from(body)))
                .expect("response should build")
        })
        .layer(service_fn({
            let calls = calls.clone();
            move |_request: Request<()>| {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, io::Error>(
                        Response::builder()
                            .status(StatusCode::OK)
                            .body(Full::new(Bytes::from_static(b"0123456789")))
                            .expect("response should build"),
                    )
                }
            }
        }));

    let first = service
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/large")
                .body(())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");
    let second = service
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/large")
                .body(())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(first.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(second.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        response_body(first).await,
        Bytes::from("length limit exceeded")
    );
    assert_eq!(
        response_body(second).await,
        Bytes::from("length limit exceeded")
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
