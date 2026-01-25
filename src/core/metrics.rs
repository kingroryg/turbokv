//! # Metrics and Monitoring
//!
//! Provides comprehensive metrics collection for monitoring HanshiroDB.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Metrics collector
#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    // Write metrics
    events_ingested: AtomicU64,
    bytes_ingested: AtomicU64,
    ingestion_errors: AtomicU64,
    
    // Storage metrics
    wal_writes: AtomicU64,
    wal_bytes: AtomicU64,
    memtable_flushes: AtomicU64,
    compactions: AtomicU64,
    
    // Query metrics
    queries_executed: AtomicU64,
    query_errors: AtomicU64,
    vector_searches: AtomicU64,
    metadata_searches: AtomicU64,
    
    // System metrics
    active_connections: AtomicU64,
    memory_usage: AtomicU64,
    disk_usage: AtomicU64,
}

impl Metrics {
    /// Create new metrics collector
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MetricsInner {
                events_ingested: AtomicU64::new(0),
                bytes_ingested: AtomicU64::new(0),
                ingestion_errors: AtomicU64::new(0),
                wal_writes: AtomicU64::new(0),
                wal_bytes: AtomicU64::new(0),
                memtable_flushes: AtomicU64::new(0),
                compactions: AtomicU64::new(0),
                queries_executed: AtomicU64::new(0),
                query_errors: AtomicU64::new(0),
                vector_searches: AtomicU64::new(0),
                metadata_searches: AtomicU64::new(0),
                active_connections: AtomicU64::new(0),
                memory_usage: AtomicU64::new(0),
                disk_usage: AtomicU64::new(0),
            }),
        }
    }
    
    /// Record event ingestion
    pub fn record_ingestion(&self, count: u64, bytes: u64) {
        self.inner.events_ingested.fetch_add(count, Ordering::Relaxed);
        self.inner.bytes_ingested.fetch_add(bytes, Ordering::Relaxed);
    }
    
    /// Record ingestion error
    pub fn record_ingestion_error(&self) {
        self.inner.ingestion_errors.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Record WAL write
    pub fn record_wal_write(&self, bytes: u64) {
        self.inner.wal_writes.fetch_add(1, Ordering::Relaxed);
        self.inner.wal_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
    
    /// Record memtable flush
    pub fn record_flush(&self) {
        self.inner.memtable_flushes.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Record compaction
    pub fn record_compaction(&self) {
        self.inner.compactions.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Record query execution
    pub fn record_query(&self) {
        self.inner.queries_executed.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Record query error
    pub fn record_query_error(&self) {
        self.inner.query_errors.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Record vector search
    pub fn record_vector_search(&self) {
        self.inner.vector_searches.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Record metadata search
    pub fn record_metadata_search(&self) {
        self.inner.metadata_searches.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Update active connections
    pub fn update_connections(&self, count: u64) {
        self.inner.active_connections.store(count, Ordering::Relaxed);
    }
    
    /// Update memory usage
    pub fn update_memory(&self, bytes: u64) {
        self.inner.memory_usage.store(bytes, Ordering::Relaxed);
    }
    
    /// Update disk usage
    pub fn update_disk(&self, bytes: u64) {
        self.inner.disk_usage.store(bytes, Ordering::Relaxed);
    }
    
    /// Get current metrics snapshot
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            events_ingested: self.inner.events_ingested.load(Ordering::Relaxed),
            bytes_ingested: self.inner.bytes_ingested.load(Ordering::Relaxed),
            ingestion_errors: self.inner.ingestion_errors.load(Ordering::Relaxed),
            wal_writes: self.inner.wal_writes.load(Ordering::Relaxed),
            wal_bytes: self.inner.wal_bytes.load(Ordering::Relaxed),
            memtable_flushes: self.inner.memtable_flushes.load(Ordering::Relaxed),
            compactions: self.inner.compactions.load(Ordering::Relaxed),
            queries_executed: self.inner.queries_executed.load(Ordering::Relaxed),
            query_errors: self.inner.query_errors.load(Ordering::Relaxed),
            vector_searches: self.inner.vector_searches.load(Ordering::Relaxed),
            metadata_searches: self.inner.metadata_searches.load(Ordering::Relaxed),
            active_connections: self.inner.active_connections.load(Ordering::Relaxed),
            memory_usage: self.inner.memory_usage.load(Ordering::Relaxed),
            disk_usage: self.inner.disk_usage.load(Ordering::Relaxed),
        }
    }
}

/// Metrics snapshot
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub events_ingested: u64,
    pub bytes_ingested: u64,
    pub ingestion_errors: u64,
    pub wal_writes: u64,
    pub wal_bytes: u64,
    pub memtable_flushes: u64,
    pub compactions: u64,
    pub queries_executed: u64,
    pub query_errors: u64,
    pub vector_searches: u64,
    pub metadata_searches: u64,
    pub active_connections: u64,
    pub memory_usage: u64,
    pub disk_usage: u64,
}

/// Timer for measuring operation duration
pub struct Timer {
    start: Instant,
    name: String,
}

impl Timer {
    /// Start new timer
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            start: Instant::now(),
            name: name.into(),
        }
    }
    
    /// Get elapsed time
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
    
    /// Stop timer and log duration
    pub fn stop(self) {
        let duration = self.elapsed();
        tracing::debug!(
            name = %self.name,
            duration_ms = duration.as_millis(),
            "Operation completed"
        );
    }
}

/// Histogram for tracking value distributions
pub struct Histogram {
    buckets: Vec<(f64, AtomicU64)>,
    sum: AtomicU64,
    count: AtomicU64,
}

impl Histogram {
    /// Create new histogram with bucket boundaries
    pub fn new(boundaries: Vec<f64>) -> Self {
        let buckets = boundaries
            .into_iter()
            .map(|b| (b, AtomicU64::new(0)))
            .collect();
            
        Self {
            buckets,
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }
    
    /// Record a value
    pub fn record(&self, value: f64) {
        // Update sum and count
        self.sum.fetch_add(value as u64, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        
        // Find appropriate bucket
        for (boundary, count) in &self.buckets {
            if value <= *boundary {
                count.fetch_add(1, Ordering::Relaxed);
                break;
            }
        }
    }
    
    /// Get histogram statistics
    pub fn stats(&self) -> HistogramStats {
        let count = self.count.load(Ordering::Relaxed);
        let sum = self.sum.load(Ordering::Relaxed);
        
        HistogramStats {
            count,
            sum,
            mean: if count > 0 { sum as f64 / count as f64 } else { 0.0 },
        }
    }
}

/// Histogram statistics
#[derive(Debug, Clone)]
pub struct HistogramStats {
    pub count: u64,
    pub sum: u64,
    pub mean: f64,
}