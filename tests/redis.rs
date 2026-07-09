use cacheflight::{CacheFlight, LookupState, RedisCache, Result};
use std::time::Duration;

fn redis_url() -> String {
    use testcontainers::GenericImage;
    use testcontainers::clients::Cli;

    let client: &'static Cli = Box::leak(Box::new(Cli::default()));
    let image = GenericImage::new("redis", "7-alpine").with_exposed_port(6379);
    let container = client.run(image);
    let port = container.get_host_port_ipv4(6379);
    std::mem::forget(container);
    format!("redis://127.0.0.1:{port}/")
}

/// Requires Docker to be running.
/// Run with: cargo test --test redis -- --ignored
#[ignore]
#[tokio::test]
async fn redis_cache_basic_dedup() -> Result<()> {
    let url = redis_url();
    let cache = RedisCache::new(&url).await?;
    let cf = CacheFlight::new(cache).ttl(Duration::from_secs(30));

    let result = cf
        .run("dedup-key", || async { Ok(b"redis-value".to_vec()) })
        .await?;

    assert_eq!(result.state(), LookupState::Recomputed);
    assert_eq!(result.value(), b"redis-value");

    let cached = cf
        .run("dedup-key", || async {
            unreachable!("should read from cache")
        })
        .await?;

    assert_eq!(cached.state(), LookupState::CacheHit);
    assert_eq!(cached.value(), b"redis-value");

    Ok(())
}

/// Requires Docker to be running.
/// Run with: cargo test --test redis -- --ignored
#[ignore]
#[tokio::test]
async fn redis_cache_with_swr() -> Result<()> {
    let url = redis_url();
    let cache = RedisCache::new(&url).await?;
    let cf = CacheFlight::new(cache)
        .stale_while_revalidate(Duration::from_millis(100), Duration::from_secs(10));

    cf.run("swr-key", || async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        Ok(b"swr-value".to_vec())
    })
    .await?;

    tokio::time::sleep(Duration::from_millis(150)).await;

    let stale = cf
        .run("swr-key", || async {
            unreachable!("background refresh should serve this")
        })
        .await?;

    assert_eq!(stale.state(), LookupState::Stale);
    assert_eq!(stale.value(), b"swr-value");

    Ok(())
}
