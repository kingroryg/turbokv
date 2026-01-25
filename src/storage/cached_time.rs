//! Cached timestamp to eliminate syscalls for TTL/reaper checks.
//! Background thread updates every 100ms - good enough for TTL expiry.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

static CACHED_TIMESTAMP_MS: AtomicU64 = AtomicU64::new(0);
static INIT: OnceLock<()> = OnceLock::new();

/// Initialize the background timestamp updater (call once at startup)
pub fn init() {
    INIT.get_or_init(|| {
        // Set initial value
        update_timestamp();
        
        // Spawn background updater
        thread::Builder::new()
            .name("timestamp-cache".into())
            .spawn(|| loop {
                thread::sleep(Duration::from_millis(100));
                update_timestamp();
            })
            .expect("Failed to spawn timestamp thread");
    });
}

fn update_timestamp() {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    CACHED_TIMESTAMP_MS.store(now, Ordering::Relaxed);
}

/// Get cached timestamp in milliseconds (±100ms accuracy)
#[inline]
pub fn now_ms() -> u64 {
    let cached = CACHED_TIMESTAMP_MS.load(Ordering::Relaxed);
    if cached == 0 {
        // Not initialized, return real time
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    } else {
        cached
    }
}

/// Get cached timestamp in seconds
#[inline]
pub fn now_secs() -> u64 {
    now_ms() / 1000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cached_time() {
        init();
        let t1 = now_ms();
        assert!(t1 > 0);
        
        // Should be close to real time
        let real = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        assert!((real as i64 - t1 as i64).abs() < 200);
    }
}
