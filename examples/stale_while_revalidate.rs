mod common;

use cacheflight::{CacheFlight, CachePolicy, LookupState, Result};
use common::MemoryCache;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};
use tokio::{sync::Notify, time::sleep};

#[tokio::main]
async fn main() -> Result<()> {
    let cache = MemoryCache::new();
    let cf = CacheFlight::new(
        cache,
        CachePolicy::new(Duration::from_millis(150))
            .with_stale_while_revalidate(Duration::from_secs(2)),
    );

    let versions = Arc::new(AtomicUsize::new(0));

    let initial = cf
        .run("dashboard:home", {
            let versions = versions.clone();
            move || {
                let versions = versions.clone();
                async move {
                    let version = versions.fetch_add(1, Ordering::SeqCst) + 1;
                    sleep(Duration::from_millis(60)).await;
                    Ok(format!("dashboard-version-{version}").into_bytes())
                }
            }
        })
        .await?;

    println!(
        "initial request: state={:?}, value={}",
        initial.state(),
        String::from_utf8_lossy(initial.value())
    );

    sleep(Duration::from_millis(200)).await;

    let refresh_started = Arc::new(Notify::new());
    let release_refresh = Arc::new(Notify::new());

    let refresh_work = {
        let versions = versions.clone();
        let refresh_started = refresh_started.clone();
        let release_refresh = release_refresh.clone();

        move || {
            let versions = versions.clone();
            let refresh_started = refresh_started.clone();
            let release_refresh = release_refresh.clone();

            async move {
                let version = versions.fetch_add(1, Ordering::SeqCst) + 1;

                refresh_started.notify_waiters();
                release_refresh.notified().await;

                Ok(format!("dashboard-version-{version}").into_bytes())
            }
        }
    };

    let started = Instant::now();
    let stale = cf
        .run("dashboard:home", refresh_work.clone())
        .await?;

    println!(
        "stale request: state={:?}, returned in {:?}, value={}",
        stale.state(),
        started.elapsed(),
        String::from_utf8_lossy(stale.value())
    );
    assert_eq!(stale.state(), LookupState::Stale);

    refresh_started.notified().await;

    let during_refresh = cf
        .run("dashboard:home", refresh_work.clone())
        .await?;

    println!(
        "second stale request during refresh: state={:?}, value={}",
        during_refresh.state(),
        String::from_utf8_lossy(during_refresh.value())
    );
    assert_eq!(during_refresh.state(), LookupState::Stale);

    release_refresh.notify_waiters();

    let refreshed = loop {
        let result = cf
            .run("dashboard:home", || async {
                unreachable!("the background refresh should update the cache")
            })
            .await?;

        if result.value() == b"dashboard-version-2" {
            break result;
        }

        sleep(Duration::from_millis(10)).await;
    };

    println!(
        "after background refresh: state={:?}, value={}",
        refreshed.state(),
        String::from_utf8_lossy(refreshed.value())
    );
    println!();
    println!("This is the stale-while-revalidate contract:");
    println!("1. callers do not wait once the entry is stale but still usable");
    println!("2. only one background refresh is started");
    println!("3. later callers see the refreshed value once it lands");

    Ok(())
}
