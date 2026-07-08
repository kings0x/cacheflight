use std::{error::Error as StdError, fmt, sync::Arc};

pub type ErrorSource = Arc<dyn StdError + Send + Sync>;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Error {
    CacheRead(ErrorSource),
    Operation(ErrorSource),
    CacheWrite(ErrorSource),
    Internal(Arc<str>),
}

impl Error {
    pub fn cache_read<E>(source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::CacheRead(Arc::new(source))
    }

    pub fn operation<E>(source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::Operation(Arc::new(source))
    }

    pub fn cache_write<E>(source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::CacheWrite(Arc::new(source))
    }

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
            Self::Internal(message) => write!(f, "{message}"),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::CacheRead(source) | Self::Operation(source) | Self::CacheWrite(source) => {
                Some(source.as_ref())
            }
            Self::Internal(_) => None,
        }
    }
}
