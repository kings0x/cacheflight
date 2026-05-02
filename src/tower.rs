use crate::{CacheBackend, CachePolicy, ComputeValue, Error, MetricsHooks, Result, SingleFlight};
use bytes::Bytes;
use http::{
    Method, Request, Response, StatusCode, Version,
    header::{
        ACCEPT, ACCEPT_ENCODING, ACCEPT_LANGUAGE, AUTHORIZATION, CONTENT_TYPE, COOKIE, HOST,
        HeaderName, HeaderValue, PROXY_AUTHORIZATION, RANGE,
    },
};
use http_body::Body as HttpBody;
use http_body_util::{BodyExt, Full, Limited};
use std::{
    convert::Infallible,
    error::Error as StdError,
    fmt::Write as _,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::Mutex;
use tower::{Layer, Service, ServiceExt};

const HTTP_RESPONSE_MAGIC: &[u8; 4] = b"HTP1";
const DEFAULT_MAX_RESPONSE_BYTES: usize = 1024 * 1024;

type RequestPredicate<ReqBody> = Arc<dyn Fn(&Request<ReqBody>) -> bool + Send + Sync>;
type RequestKeyFn<ReqBody> = Arc<dyn Fn(&Request<ReqBody>) -> String + Send + Sync>;
type RequestTtlFn<ReqBody> = Arc<dyn Fn(&Request<ReqBody>) -> Duration + Send + Sync>;
type StatusPredicate = Arc<dyn Fn(StatusCode) -> bool + Send + Sync>;
type ErrorHandler = Arc<dyn Fn(Error) -> Response<Full<Bytes>> + Send + Sync>;

/// Defines how a Tower request maps to a cache key and how responses are
/// encoded into raw bytes.
pub trait TowerCachePolicy<Request, Response>: Clone + Send + Sync + 'static {
    /// Returns a cache key for the request. Returning `None` bypasses the
    /// middleware for that request.
    fn cache_key(&self, request: &Request) -> Option<String>;

    /// Encodes the response into raw bytes before storing it.
    fn encode_response(&self, response: &Response) -> Result<Vec<u8>>;

    /// Decodes raw cached bytes back into a response.
    fn decode_response(&self, bytes: &[u8]) -> Result<Response>;
}

/// Helper policy for services that already return `Vec<u8>`.
#[derive(Clone)]
pub struct BytesPolicy<F> {
    key_fn: F,
}

impl<F> BytesPolicy<F> {
    /// Creates a policy that uses `key_fn` to derive the cache key.
    pub fn new(key_fn: F) -> Self {
        Self { key_fn }
    }
}

impl<Request, F> TowerCachePolicy<Request, Vec<u8>> for BytesPolicy<F>
where
    F: Fn(&Request) -> String + Clone + Send + Sync + 'static,
{
    fn cache_key(&self, request: &Request) -> Option<String> {
        Some((self.key_fn)(request))
    }

    fn encode_response(&self, response: &Vec<u8>) -> Result<Vec<u8>> {
        Ok(response.clone())
    }

    fn decode_response(&self, bytes: &[u8]) -> Result<Vec<u8>> {
        Ok(bytes.to_vec())
    }
}

/// Tower layer that caches and deduplicates requests through [`SingleFlight`].
#[derive(Clone)]
pub struct SingleFlightLayer<C, P, M = crate::NoopMetrics> {
    singleflight: SingleFlight<C, M>,
    policy: P,
}

impl<C, P, M> SingleFlightLayer<C, P, M>
where
    C: CacheBackend,
    M: MetricsHooks,
    P: Clone,
{
    /// Creates a new layer from an existing singleflight engine and request
    /// policy.
    pub fn new(singleflight: SingleFlight<C, M>, policy: P) -> Self {
        Self {
            singleflight,
            policy,
        }
    }
}

impl<S, C, P, M> Layer<S> for SingleFlightLayer<C, P, M>
where
    C: CacheBackend,
    M: MetricsHooks,
    P: Clone,
{
    type Service = SingleFlightService<S, C, P, M>;

    fn layer(&self, inner: S) -> Self::Service {
        SingleFlightService {
            inner,
            singleflight: self.singleflight.clone(),
            policy: self.policy.clone(),
        }
    }
}

/// Tower service produced by [`SingleFlightLayer`].
#[derive(Clone)]
pub struct SingleFlightService<S, C, P, M = crate::NoopMetrics> {
    inner: S,
    singleflight: SingleFlight<C, M>,
    policy: P,
}

impl<S, Request, Response, C, P, M> Service<Request> for SingleFlightService<S, C, P, M>
where
    S: Service<Request, Response = Response> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: StdError + Send + Sync + 'static,
    Request: Clone + Send + 'static,
    Response: Send + 'static,
    C: CacheBackend,
    M: MetricsHooks,
    P: TowerCachePolicy<Request, Response>,
{
    type Response = Response;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Response>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let inner = self.inner.clone();
        let singleflight = self.singleflight.clone();
        let policy = self.policy.clone();

        Box::pin(async move {
            let Some(key) = policy.cache_key(&request) else {
                return inner.oneshot(request).await.map_err(Error::operation);
            };

            let request_for_work = request.clone();
            let inner_for_work = inner.clone();
            let policy_for_work = policy.clone();
            let cached: crate::LookupResult = singleflight
                .get_or_compute(key, move || {
                    let request = request_for_work.clone();
                    let inner = inner_for_work.clone();
                    let policy = policy_for_work.clone();

                    async move {
                        let response = inner.oneshot(request).await.map_err(Error::operation)?;
                        policy.encode_response(&response)
                    }
                })
                .await?;

            policy.decode_response(cached.value())
        })
    }
}

/// Ergonomic HTTP-aware singleflight layer for Tower and Axum-style services.
///
/// This layer is meant for the common case where you want:
/// - sensible HTTP defaults
/// - minimal user code
/// - request filtering, keying, per-request TTLs, and status filtering
///
/// Defaults:
/// - only `GET` and `HEAD` are cached
/// - requests carrying `Authorization`, `Proxy-Authorization`, `Cookie`, or
///   `Range` headers are bypassed
/// - only successful (`2xx`) responses are cached
/// - keys use `METHOD + host + path + sorted query + common content-negotiation headers`
/// - response bodies are buffered up to 1 MiB before being cached or replayed
pub struct HttpSingleFlightLayer<C, ReqBody, M = crate::NoopMetrics> {
    singleflight: SingleFlight<C, M>,
    predicate: RequestPredicate<ReqBody>,
    key_fn: RequestKeyFn<ReqBody>,
    ttl_fn: RequestTtlFn<ReqBody>,
    stale_while_revalidate: Option<Duration>,
    cache_status: StatusPredicate,
    max_response_bytes: usize,
    error_handler: ErrorHandler,
    _marker: PhantomData<fn(ReqBody)>,
}

impl<C, ReqBody, M> Clone for HttpSingleFlightLayer<C, ReqBody, M> {
    fn clone(&self) -> Self {
        Self {
            singleflight: self.singleflight.clone(),
            predicate: Arc::clone(&self.predicate),
            key_fn: Arc::clone(&self.key_fn),
            ttl_fn: Arc::clone(&self.ttl_fn),
            stale_while_revalidate: self.stale_while_revalidate,
            cache_status: Arc::clone(&self.cache_status),
            max_response_bytes: self.max_response_bytes,
            error_handler: Arc::clone(&self.error_handler),
            _marker: PhantomData,
        }
    }
}

impl<C, ReqBody> HttpSingleFlightLayer<C, ReqBody, crate::NoopMetrics>
where
    C: CacheBackend,
    ReqBody: 'static,
{
    /// Creates an HTTP layer with safe defaults:
    /// - `GET` and `HEAD` only
    /// - authenticated, cookie-bearing, and range requests bypass caching
    /// - `2xx` responses only
    /// - host-aware, query-normalized request keys with common `Accept*` variance
    /// - response bodies buffered up to 1 MiB
    pub fn new(cache: C, ttl: Duration) -> Self {
        Self::with_metrics(cache, ttl, crate::NoopMetrics)
    }
}

impl<C, ReqBody, M> HttpSingleFlightLayer<C, ReqBody, M>
where
    C: CacheBackend,
    M: MetricsHooks,
    ReqBody: 'static,
{
    /// Creates an HTTP layer with custom metrics hooks.
    pub fn with_metrics(cache: C, ttl: Duration, metrics: M) -> Self {
        let policy = CachePolicy::new(ttl);

        Self {
            singleflight: SingleFlight::with_metrics(cache, policy, metrics),
            predicate: Arc::new(default_request_predicate::<ReqBody>),
            key_fn: Arc::new(default_cache_key::<ReqBody>),
            ttl_fn: Arc::new(move |_| ttl),
            stale_while_revalidate: None,
            cache_status: Arc::new(|status: StatusCode| status.is_success()),
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            error_handler: Arc::new(default_http_error_response),
            _marker: PhantomData,
        }
    }

    /// Serves stale responses immediately while a background refresh runs.
    pub fn stale_while_revalidate(mut self, ttl: Duration) -> Self {
        self.stale_while_revalidate = Some(ttl);
        self
    }

    /// Controls which requests are eligible for singleflight and caching.
    pub fn predicate<F>(mut self, predicate: F) -> Self
    where
        F: Fn(&Request<ReqBody>) -> bool + Send + Sync + 'static,
    {
        self.predicate = Arc::new(predicate);
        self
    }

    /// Overrides the cache key derivation logic.
    pub fn key_with<F>(mut self, key_fn: F) -> Self
    where
        F: Fn(&Request<ReqBody>) -> String + Send + Sync + 'static,
    {
        self.key_fn = Arc::new(key_fn);
        self
    }

    /// Computes the fresh TTL per request while preserving the configured
    /// stale-while-revalidate window.
    pub fn ttl_with<F>(mut self, ttl_fn: F) -> Self
    where
        F: Fn(&Request<ReqBody>) -> Duration + Send + Sync + 'static,
    {
        self.ttl_fn = Arc::new(ttl_fn);
        self
    }

    /// Controls which status codes are written to cache.
    pub fn cache_status<F>(mut self, predicate: F) -> Self
    where
        F: Fn(StatusCode) -> bool + Send + Sync + 'static,
    {
        self.cache_status = Arc::new(predicate);
        self
    }

    /// Sets the maximum response body size the middleware will buffer.
    ///
    /// The HTTP middleware always collects the full body in order to replay it
    /// from cache, so this limit protects the process from unbounded buffering.
    /// Responses larger than the limit are converted into the configured error
    /// response and are not cached.
    pub fn max_response_bytes(mut self, max: usize) -> Self {
        self.max_response_bytes = max;
        self
    }

    /// Overrides how internal middleware errors are converted into HTTP
    /// responses. By default the layer returns a `500 Internal Server Error`
    /// with a short plain-text body.
    pub fn error_response_with<F>(mut self, handler: F) -> Self
    where
        F: Fn(Error) -> Response<Full<Bytes>> + Send + Sync + 'static,
    {
        self.error_handler = Arc::new(handler);
        self
    }
}

impl<S, C, ReqBody, M> Layer<S> for HttpSingleFlightLayer<C, ReqBody, M>
where
    C: CacheBackend,
    M: MetricsHooks,
{
    type Service = HttpSingleFlightService<S, C, ReqBody, M>;

    fn layer(&self, inner: S) -> Self::Service {
        HttpSingleFlightService {
            inner,
            singleflight: self.singleflight.clone(),
            predicate: Arc::clone(&self.predicate),
            key_fn: Arc::clone(&self.key_fn),
            ttl_fn: Arc::clone(&self.ttl_fn),
            stale_while_revalidate: self.stale_while_revalidate,
            cache_status: Arc::clone(&self.cache_status),
            max_response_bytes: self.max_response_bytes,
            error_handler: Arc::clone(&self.error_handler),
            _marker: PhantomData,
        }
    }
}

/// Tower service produced by [`HttpSingleFlightLayer`].
pub struct HttpSingleFlightService<S, C, ReqBody, M = crate::NoopMetrics> {
    inner: S,
    singleflight: SingleFlight<C, M>,
    predicate: RequestPredicate<ReqBody>,
    key_fn: RequestKeyFn<ReqBody>,
    ttl_fn: RequestTtlFn<ReqBody>,
    stale_while_revalidate: Option<Duration>,
    cache_status: StatusPredicate,
    max_response_bytes: usize,
    error_handler: ErrorHandler,
    _marker: PhantomData<fn(ReqBody)>,
}

impl<S, C, ReqBody, M> Clone for HttpSingleFlightService<S, C, ReqBody, M>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            singleflight: self.singleflight.clone(),
            predicate: Arc::clone(&self.predicate),
            key_fn: Arc::clone(&self.key_fn),
            ttl_fn: Arc::clone(&self.ttl_fn),
            stale_while_revalidate: self.stale_while_revalidate,
            cache_status: Arc::clone(&self.cache_status),
            max_response_bytes: self.max_response_bytes,
            error_handler: Arc::clone(&self.error_handler),
            _marker: PhantomData,
        }
    }
}

impl<S, C, ReqBody, ResBody, M> Service<Request<ReqBody>>
    for HttpSingleFlightService<S, C, ReqBody, M>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: StdError + Send + Sync + 'static,
    C: CacheBackend,
    M: MetricsHooks,
    ReqBody: Send + 'static,
    ResBody: HttpBody<Data = Bytes> + Send + 'static,
    ResBody::Error: StdError + Send + Sync + 'static,
{
    type Response = Response<Full<Bytes>>;
    type Error = Infallible;
    type Future =
        Pin<Box<dyn Future<Output = std::result::Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        let inner = self.inner.clone();
        let singleflight = self.singleflight.clone();
        let predicate = Arc::clone(&self.predicate);
        let key_fn = Arc::clone(&self.key_fn);
        let ttl_fn = Arc::clone(&self.ttl_fn);
        let cache_status = Arc::clone(&self.cache_status);
        let max_response_bytes = self.max_response_bytes;
        let error_handler = Arc::clone(&self.error_handler);
        let stale_while_revalidate = self.stale_while_revalidate;

        Box::pin(async move {
            let response = async {
                if !(predicate)(&request) {
                    let response = inner.oneshot(request).await.map_err(Error::operation)?;
                    return buffer_http_response(response, max_response_bytes).await;
                }

                let key = (key_fn)(&request);
                let fresh_ttl = (ttl_fn)(&request);
                let request_policy = with_optional_stale(fresh_ttl, stale_while_revalidate);
                let request_slot = Arc::new(Mutex::new(Some(request)));

                let cached = singleflight
                    .get_or_compute_with(key, request_policy, {
                        let request_slot = Arc::clone(&request_slot);
                        let inner = inner.clone();
                        let cache_status = Arc::clone(&cache_status);

                        move || {
                            let request_slot = Arc::clone(&request_slot);
                            let inner = inner.clone();
                            let cache_status = Arc::clone(&cache_status);

                            async move {
                                let request = request_slot.lock().await.take().ok_or_else(|| {
                                    Error::internal(
                                        "the HTTP middleware request was consumed more than once",
                                    )
                                })?;

                                let response =
                                    inner.oneshot(request).await.map_err(Error::operation)?;
                                let (encoded, status) =
                                    encode_http_response(response, max_response_bytes).await?;

                                if (cache_status)(status) {
                                    Ok(ComputeValue::cache_with_policy(encoded, request_policy))
                                } else {
                                    Ok(ComputeValue::do_not_cache(encoded))
                                }
                            }
                        }
                    })
                    .await?;

                decode_http_response(cached.value())
            }
            .await;

            Ok(match response {
                Ok(response) => response,
                Err(error) => (error_handler)(error),
            })
        })
    }
}

fn with_optional_stale(
    fresh_ttl: Duration,
    stale_while_revalidate: Option<Duration>,
) -> CachePolicy {
    match stale_while_revalidate {
        Some(stale) => CachePolicy::new(fresh_ttl).with_stale_while_revalidate(stale),
        None => CachePolicy::new(fresh_ttl),
    }
}

fn default_request_predicate<ReqBody>(request: &Request<ReqBody>) -> bool {
    matches!(*request.method(), Method::GET | Method::HEAD)
        && !request.headers().contains_key(AUTHORIZATION)
        && !request.headers().contains_key(PROXY_AUTHORIZATION)
        && !request.headers().contains_key(COOKIE)
        && !request.headers().contains_key(RANGE)
}

fn default_cache_key<ReqBody>(request: &Request<ReqBody>) -> String {
    let mut key = String::new();
    write!(&mut key, "method={}|", request.method()).expect("writing to string should not fail");

    if let Some(host) = request
        .uri()
        .authority()
        .map(|authority| authority.as_str())
        .or_else(|| header_value_to_str(request.headers().get(HOST)))
    {
        key.push_str("host=");
        key.push_str(host);
        key.push('|');
    }

    key.push_str("path=");
    key.push_str(request.uri().path());

    if let Some(query) = request.uri().query() {
        let normalized = normalize_query(query);

        if !normalized.is_empty() {
            key.push('|');
            key.push_str("query=");
            key.push_str(&normalized);
        }
    }

    append_header_key(&mut key, ACCEPT, request.headers().get(ACCEPT));
    append_header_key(
        &mut key,
        ACCEPT_ENCODING,
        request.headers().get(ACCEPT_ENCODING),
    );
    append_header_key(
        &mut key,
        ACCEPT_LANGUAGE,
        request.headers().get(ACCEPT_LANGUAGE),
    );

    key
}

fn normalize_query(query: &str) -> String {
    let mut parts: Vec<&str> = query.split('&').filter(|part| !part.is_empty()).collect();
    parts.sort_unstable();
    parts.join("&")
}

fn default_http_error_response(_error: Error) -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .header(CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Full::new(Bytes::from_static(
            b"cacheflight middleware error",
        )))
        .expect("default HTTP error response should build")
}

async fn buffer_http_response<ResBody>(
    response: Response<ResBody>,
    max_response_bytes: usize,
) -> Result<Response<Full<Bytes>>>
where
    ResBody: HttpBody<Data = Bytes> + Send + 'static,
    ResBody::Error: StdError + Send + Sync + 'static,
{
    let (encoded, _) = encode_http_response(response, max_response_bytes).await?;
    decode_http_response(&encoded)
}

async fn encode_http_response<ResBody>(
    response: Response<ResBody>,
    max_response_bytes: usize,
) -> Result<(Vec<u8>, StatusCode)>
where
    ResBody: HttpBody<Data = Bytes> + Send + 'static,
    ResBody::Error: StdError + Send + Sync + 'static,
{
    let (parts, body) = response.into_parts();
    let status = parts.status;
    let body = Limited::new(body, max_response_bytes)
        .collect()
        .await
        .map_err(|error| Error::encode(std::io::Error::other(error.to_string())))?
        .to_bytes();
    let mut bytes = Vec::new();

    bytes.extend_from_slice(HTTP_RESPONSE_MAGIC);
    bytes.push(encode_version(parts.version)?);
    bytes.extend_from_slice(&parts.status.as_u16().to_be_bytes());
    let header_count = u32::try_from(parts.headers.len()).map_err(|_| {
        Error::encode(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "too many HTTP headers to encode",
        ))
    })?;
    bytes.extend_from_slice(&header_count.to_be_bytes());

    for (name, value) in &parts.headers {
        let name = name.as_str().as_bytes();
        let value = value.as_bytes();
        let name_len = u16::try_from(name.len()).map_err(|_| {
            Error::encode(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "HTTP header name is too long to encode",
            ))
        })?;
        let value_len = u32::try_from(value.len()).map_err(|_| {
            Error::encode(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "HTTP header value is too long to encode",
            ))
        })?;

        bytes.extend_from_slice(&name_len.to_be_bytes());
        bytes.extend_from_slice(name);
        bytes.extend_from_slice(&value_len.to_be_bytes());
        bytes.extend_from_slice(value);
    }

    let body_len = u64::try_from(body.len()).map_err(|_| {
        Error::encode(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "HTTP body is too large to encode",
        ))
    })?;
    bytes.extend_from_slice(&body_len.to_be_bytes());
    bytes.extend_from_slice(&body);

    Ok((bytes, status))
}

fn decode_http_response(bytes: &[u8]) -> Result<Response<Full<Bytes>>> {
    if bytes.len() < 19 || &bytes[..4] != HTTP_RESPONSE_MAGIC {
        return Err(Error::decode(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid cached HTTP response",
        )));
    }

    let mut cursor = 4;
    let version = decode_version(read_u8(bytes, &mut cursor)?)?;
    let status = StatusCode::from_u16(read_u16(bytes, &mut cursor)?).map_err(Error::decode)?;
    let header_count = read_u32(bytes, &mut cursor)? as usize;
    let mut response = Response::builder().status(status).version(version);

    for _ in 0..header_count {
        let name_len = read_u16(bytes, &mut cursor)? as usize;
        let name = read_exact(bytes, &mut cursor, name_len)?;
        let value_len = read_u32(bytes, &mut cursor)? as usize;
        let value = read_exact(bytes, &mut cursor, value_len)?;

        response = response.header(
            http::header::HeaderName::from_bytes(name).map_err(Error::decode)?,
            http::header::HeaderValue::from_bytes(value).map_err(Error::decode)?,
        );
    }

    let body_len = read_u64(bytes, &mut cursor)? as usize;
    let body = Bytes::copy_from_slice(read_exact(bytes, &mut cursor, body_len)?);

    response
        .body(Full::new(body))
        .map_err(|error| Error::decode(std::io::Error::other(error.to_string())))
}

fn encode_version(version: Version) -> Result<u8> {
    match version {
        Version::HTTP_09 => Ok(0),
        Version::HTTP_10 => Ok(1),
        Version::HTTP_11 => Ok(2),
        Version::HTTP_2 => Ok(3),
        Version::HTTP_3 => Ok(4),
        _ => Err(Error::encode(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "unsupported HTTP version",
        ))),
    }
}

fn header_value_to_str(value: Option<&HeaderValue>) -> Option<&str> {
    value.and_then(|value| value.to_str().ok())
}

fn append_header_key(key: &mut String, name: HeaderName, value: Option<&HeaderValue>) {
    if let Some(value) = value {
        key.push('|');
        key.push_str(name.as_str());
        key.push('=');

        for byte in value.as_bytes() {
            write!(key, "{byte:02x}").expect("writing to string should not fail");
        }
    }
}

fn decode_version(value: u8) -> Result<Version> {
    match value {
        0 => Ok(Version::HTTP_09),
        1 => Ok(Version::HTTP_10),
        2 => Ok(Version::HTTP_11),
        3 => Ok(Version::HTTP_2),
        4 => Ok(Version::HTTP_3),
        _ => Err(Error::decode(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "unsupported cached HTTP version",
        ))),
    }
}

fn read_exact<'a>(bytes: &'a [u8], cursor: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = cursor.saturating_add(len);

    if end > bytes.len() {
        return Err(Error::decode(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "cached HTTP response ended unexpectedly",
        )));
    }

    let slice = &bytes[*cursor..end];
    *cursor = end;
    Ok(slice)
}

fn read_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8> {
    Ok(read_exact(bytes, cursor, 1)?[0])
}

fn read_u16(bytes: &[u8], cursor: &mut usize) -> Result<u16> {
    let value = read_exact(bytes, cursor, 2)?;
    Ok(u16::from_be_bytes([value[0], value[1]]))
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    let value = read_exact(bytes, cursor, 4)?;
    Ok(u32::from_be_bytes([value[0], value[1], value[2], value[3]]))
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64> {
    let value = read_exact(bytes, cursor, 8)?;
    Ok(u64::from_be_bytes([
        value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
    ]))
}
