use std::time::Duration;

/// Controls freshness and stale-while-revalidate behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CachePolicy {
    fresh_ttl: Duration,
    stale_while_revalidate: Option<Duration>,
}

impl CachePolicy {
    /// Creates a policy with no stale window.
    pub fn new(fresh_ttl: Duration) -> Self {
        Self {
            fresh_ttl,
            stale_while_revalidate: None,
        }
    }

    /// Enables stale-while-revalidate for the provided extra duration.
    pub fn with_stale_while_revalidate(mut self, ttl: Duration) -> Self {
        self.stale_while_revalidate = Some(ttl);
        self
    }

    /// Returns the freshness window.
    pub fn fresh_ttl(&self) -> Duration {
        self.fresh_ttl
    }

    /// Returns the stale-while-revalidate window, if enabled.
    pub fn stale_while_revalidate(&self) -> Option<Duration> {
        self.stale_while_revalidate
    }

    /// Returns the TTL written to the underlying cache.
    pub fn total_ttl(&self) -> Duration {
        self.fresh_ttl
            .checked_add(self.stale_while_revalidate.unwrap_or_default())
            .unwrap_or(Duration::MAX)
    }

    /// Returns whether stale responses may be served while a refresh happens in
    /// the background.
    pub fn allows_stale_while_revalidate(&self) -> bool {
        self.stale_while_revalidate.is_some()
    }
}
