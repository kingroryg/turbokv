//! Process-wide coarse wall clock for the WAL write path.
//!
//! WAL timestamps are diagnostic metadata rather than ordering state. A
//! relaxed atomic load avoids placing wall-clock retrieval in every durable
//! mutation while keeping the persisted value within one update interval of
//! the system clock.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const UPDATE_INTERVAL: Duration = Duration::from_millis(100);

static CACHED_TIMESTAMP_MS: AtomicU64 = AtomicU64::new(0);
static INITIALIZATION: OnceLock<Result<(), Arc<str>>> = OnceLock::new();

pub(crate) fn init() -> io::Result<()> {
    match INITIALIZATION.get_or_init(|| {
        update_timestamp();
        thread::Builder::new()
            .name("turbokv-clock".into())
            .spawn(|| loop {
                thread::sleep(UPDATE_INTERVAL);
                update_timestamp();
            })
            .map(|_| ())
            .map_err(|error| Arc::<str>::from(error.to_string()))
    }) {
        Ok(()) => Ok(()),
        Err(message) => Err(io::Error::other(message.to_string())),
    }
}

fn update_timestamp() {
    CACHED_TIMESTAMP_MS.store(system_timestamp_ms(), Ordering::Relaxed);
}

fn system_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[inline]
pub(crate) fn now_ms() -> u64 {
    let cached = CACHED_TIMESTAMP_MS.load(Ordering::Relaxed);
    if cached == 0 {
        system_timestamp_ms()
    } else {
        cached
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn background_clock_advances_and_stays_close_to_wall_time() {
        init().unwrap();
        let initial = now_ms();
        let deadline = Instant::now() + Duration::from_secs(5);
        let cached = loop {
            let observed = now_ms();
            if observed > initial {
                break observed;
            }
            assert!(
                Instant::now() < deadline,
                "cached clock did not advance after background initialization"
            );
            thread::sleep(Duration::from_millis(10));
        };
        let wall = system_timestamp_ms();
        assert!(wall.abs_diff(cached) <= 1_000);
    }
}
