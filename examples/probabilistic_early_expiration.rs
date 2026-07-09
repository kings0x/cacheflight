mod common;

use cacheflight::{CacheFlight, Result};
use common::MemoryCache;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<()> {
    // ── Flat TTL + XFetch ──────────────────────────────────────────────────
    //
    // Entry is fresh for 200 ms.  XFetch (beta=2.0) may trigger a background
    // refresh *before* the 200 ms window expires, based on the smoothed delta
    // EMA and random jitter.  Callers see CacheHit while the refresh runs.
    let cf = CacheFlight::new(MemoryCache::new())
        .ttl(Duration::from_millis(200))
        .probabilistic_expiry(2.0);

    let recomputes = Arc::new(AtomicUsize::new(0));

    // Prime the cache.
    cf.run("key:flat", {
        let recomputes = recomputes.clone();
        move || {
            let recomputes = recomputes.clone();
            async move {
                recomputes.fetch_add(1, Ordering::SeqCst);
                sleep(Duration::from_millis(30)).await;
                Ok(b"flat-value".to_vec())
            }
        }
    })
    .await?;

    // Read repeatedly.  XFetch will eventually trigger an early refresh.
    for i in 1..=10 {
        let prev = recomputes.load(Ordering::SeqCst);
        let result = cf
            .run("key:flat", {
                let recomputes = recomputes.clone();
                move || {
                    let recomputes = recomputes.clone();
                    async move {
                        // XFetch may trigger this as a background refresh.
                        recomputes.fetch_add(1, Ordering::SeqCst);
                        sleep(Duration::from_millis(30)).await;
                        Ok(b"flat-value".to_vec())
                    }
                }
            })
            .await?;

        println!(
            "flat-ttl  | read {i:>2}: state={:?}, recomputes={prev}",
            result.state(),
        );

        sleep(Duration::from_millis(30)).await;
    }

    // ── Stale-while-revalidate + XFetch ────────────────────────────────────
    //
    // Fresh for 150 ms, stale for 500 ms.  XFetch may trigger a background
    // refresh during the fresh window.  While stale, callers get Stale
    // immediately and a background refresh is always started.
    let cf = CacheFlight::new(MemoryCache::new())
        .stale_while_revalidate(Duration::from_millis(150), Duration::from_millis(500))
        .probabilistic_expiry(2.0);

    let recomputes = Arc::new(AtomicUsize::new(0));

    cf.run("key:swr", {
        let recomputes = recomputes.clone();
        move || {
            let recomputes = recomputes.clone();
            async move {
                recomputes.fetch_add(1, Ordering::SeqCst);
                sleep(Duration::from_millis(30)).await;
                Ok(b"swr-value".to_vec())
            }
        }
    })
    .await?;

    for i in 1..=15 {
        let prev = recomputes.load(Ordering::SeqCst);
        let result = cf
            .run("key:swr", {
                let recomputes = recomputes.clone();
                move || {
                    let recomputes = recomputes.clone();
                    async move {
                        // XFetch or stale refresh may trigger this.
                        recomputes.fetch_add(1, Ordering::SeqCst);
                        sleep(Duration::from_millis(30)).await;
                        Ok(b"swr-value".to_vec())
                    }
                }
            })
            .await?;

        println!(
            "swr+xfetch | read {i:>2}: state={:?}, recomputes={prev}",
            result.state(),
        );

        sleep(Duration::from_millis(30)).await;
    }

    println!();
    println!("XFetch probabilistically expires entries before their TTL");
    println!("based on the observed compute duration and beta parameter.");
    println!("The type-state API keeps configuration at compile time:");
    println!("  .ttl(d).probabilistic_expiry(beta)  => CacheFlight<B, HasFlatExpiry, HasXfetch>");
    println!(
        "  .stale_while_revalidate(f,s).probabilistic_expiry(beta) => CacheFlight<B, HasSwrExpiry, HasXfetch>"
    );

    Ok(())
}
