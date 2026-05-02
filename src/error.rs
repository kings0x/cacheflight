use std::{error::Error as StdError, fmt, sync::Arc};

/// A clonable error source suitable for sharing across concurrent waiters.
pub type ErrorSource = Arc<dyn StdError + Send + Sync>;

/// Convenient result alias used across the crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors surfaced by the singleflight engine and Tower middleware.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Error {
    /// The cache backend could not be queried for an existing value.
    CacheRead(ErrorSource),
    /// The upstream computation itself failed.
    Operation(ErrorSource),
    /// The cache backend rejected a write.
    CacheWrite(ErrorSource),
    /// The Tower/HTTP middleware could not serialize a response.
    Encode(ErrorSource),
    /// Cached bytes could not be turned back into a response.
    Decode(ErrorSource),
    /// Internal coordination failed unexpectedly.
    Internal(Arc<str>),
}

impl Error {
    /// Wraps a cache read failure.
    pub fn cache_read<E>(source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::CacheRead(Arc::new(source))
    }

    /// Wraps an upstream recomputation failure.
    pub fn operation<E>(source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::Operation(Arc::new(source))
    }

    /// Wraps a cache write failure.
    pub fn cache_write<E>(source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::CacheWrite(Arc::new(source))
    }

    /// Wraps a response encoding failure from the Tower middleware.
    pub fn encode<E>(source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::Encode(Arc::new(source))
    }

    /// Wraps a cached response decoding failure from the Tower middleware.
    pub fn decode<E>(source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::Decode(Arc::new(source))
    }

    /// Creates an internal coordination error.
    pub fn internal(message: impl Into<Arc<str>>) -> Self {
        Self::Internal(message.into())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CacheRead(_) => write!(f, "cache read failed"),
            Self::Operation(_) => write!(f, "recomputation failed"),
            Self::CacheWrite(_) => write!(f, "cache write failed"),
            Self::Encode(_) => write!(f, "response encoding failed"),
            Self::Decode(_) => write!(f, "cached response decoding failed"),
            Self::Internal(message) => write!(f, "{message}"),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::CacheRead(source)
            | Self::Operation(source)
            | Self::CacheWrite(source)
            | Self::Encode(source)
            | Self::Decode(source) => Some(source.as_ref()),
            Self::Internal(_) => None,
        }
    }
}
