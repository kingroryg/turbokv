//! Metrics collection for TurboKV.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Metrics collector.
#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    writes: AtomicU64,
    reads: AtomicU64,
    deletes: AtomicU64,
    bytes_written: AtomicU64,
    bytes_read: AtomicU64,
    wal_writes: AtomicU64,
    wal_bytes: AtomicU64,
    memtable_flushes: AtomicU64,
    compactions: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MetricsInner {
                writes: AtomicU64::new(0),
                reads: AtomicU64::new(0),
                deletes: AtomicU64::new(0),
                bytes_written: AtomicU64::new(0),
                bytes_read: AtomicU64::new(0),
                wal_writes: AtomicU64::new(0),
                wal_bytes: AtomicU64::new(0),
                memtable_flushes: AtomicU64::new(0),
                compactions: AtomicU64::new(0),
            }),
        }
    }

    pub fn record_write(&self, bytes: u64) {
        self.inner.writes.fetch_add(1, Ordering::Relaxed);
        self.inner.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_read(&self, bytes: u64) {
        self.inner.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_delete(&self) {
        self.inner.deletes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_wal_write(&self, bytes: u64) {
        self.inner.wal_writes.fetch_add(1, Ordering::Relaxed);
        self.inner.wal_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_flush(&self) {
        self.inner.memtable_flushes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_compaction(&self) {
        self.inner.compactions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            writes: self.inner.writes.load(Ordering::Relaxed),
            reads: self.inner.reads.load(Ordering::Relaxed),
            deletes: self.inner.deletes.load(Ordering::Relaxed),
            bytes_written: self.inner.bytes_written.load(Ordering::Relaxed),
            bytes_read: self.inner.bytes_read.load(Ordering::Relaxed),
            wal_writes: self.inner.wal_writes.load(Ordering::Relaxed),
            wal_bytes: self.inner.wal_bytes.load(Ordering::Relaxed),
            memtable_flushes: self.inner.memtable_flushes.load(Ordering::Relaxed),
            compactions: self.inner.compactions.load(Ordering::Relaxed),
        }
    }
}

/// Metrics snapshot.
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub writes: u64,
    pub reads: u64,
    pub deletes: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub wal_writes: u64,
    pub wal_bytes: u64,
    pub memtable_flushes: u64,
    pub compactions: u64,
}

/// Timer for measuring operation duration.
pub struct Timer {
    start: Instant,
}

impl Timer {
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

impl Default for Timer {
    fn default() -> Self {
        Self::new()
    }
}
