use std::{error::Error as StdError, fmt, sync::Arc};
use thiserror::Error;

/// Wraps an inner error for use as a source in [`enum@Error`].
#[derive(Debug)]
pub struct ErrorSource(Arc<dyn StdError + Send + Sync>);

impl Clone for ErrorSource {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl fmt::Display for ErrorSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl StdError for ErrorSource {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.0.as_ref())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("cache read failed")]
    CacheRead(#[source] ErrorSource),
    #[error("recomputation failed")]
    Operation(#[source] ErrorSource),
    #[error("cache write failed")]
    CacheWrite(#[source] ErrorSource),
    #[error("{0}")]
    Internal(Arc<str>),
}

impl Error {
    pub fn cache_read<E>(source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::CacheRead(ErrorSource(Arc::new(source)))
    }

    pub fn operation<E>(source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::Operation(ErrorSource(Arc::new(source)))
    }

    pub fn cache_write<E>(source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::CacheWrite(ErrorSource(Arc::new(source)))
    }

    pub fn internal(message: impl Into<Arc<str>>) -> Self {
        Self::Internal(message.into())
    }
}
