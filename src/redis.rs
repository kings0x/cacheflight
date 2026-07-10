use crate::{CacheBackend, Error, Result, async_trait};
use redis::aio::ConnectionManager;
use std::time::Duration;

#[derive(Clone)]
pub struct RedisCache {
    conn: ConnectionManager,
}

impl RedisCache {
    /// Creates a new Redis-backed cache from a connection URL.
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

    async fn delete(&self, key: &str) -> Result<()> {
        redis::cmd("DEL")
            .arg(key)
            .query_async::<()>(&mut self.conn.clone())
            .await
            .map_err(Error::cache_write)
    }

    async fn delete_by_prefix(&self, prefix: &str) -> Result<u64> {
        let pattern = format!("{}*", prefix);
        let script = redis::Script::new(
            r#"
                local cursor = '0'
                local count = 0
                repeat
                    local result = redis.call('SCAN', cursor, 'MATCH', ARGV[1], 'COUNT', 1000)
                    cursor = result[1]
                    local keys = result[2]
                    if #keys > 0 then
                        count = count + #keys
                        redis.call('DEL', unpack(keys))
                    end
                until cursor == '0'
                return count
            "#,
        );
        script
            .arg(&pattern)
            .invoke_async::<i64>(&mut self.conn.clone())
            .await
            .map_err(Error::cache_write)
            .map(|n| n as u64)
    }
}
