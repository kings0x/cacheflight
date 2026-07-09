use crate::{CacheBackend, Error, Result, async_trait};
use redis::aio::ConnectionManager;
use std::time::Duration;

#[derive(Clone)]
pub struct RedisCache {
    conn: ConnectionManager,
}

impl RedisCache {
    pub async fn new(url: &str) -> Result<Self> {
        let client = redis::Client::open(url).map_err(|e| Error::internal(e.to_string()))?;
        let conn = ConnectionManager::new(client)
            .await
            .map_err(|e| Error::internal(e.to_string()))?;
        Ok(Self { conn })
    }
}

#[async_trait]
impl CacheBackend for RedisCache {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        redis::cmd("GET")
            .arg(key)
            .query_async::<Option<Vec<u8>>>(&mut self.conn.clone())
            .await
            .map_err(Error::cache_read)
    }

    async fn set(&self, key: &str, value: Vec<u8>, ttl: Duration) -> Result<()> {
        let ttl_secs = ttl.as_secs().max(1) as i64;
        redis::cmd("SETEX")
            .arg(key)
            .arg(ttl_secs)
            .arg(value)
            .query_async::<()>(&mut self.conn.clone())
            .await
            .map_err(Error::cache_write)
    }
}
