//! MemTable types for TurboKV
//!
//! Generic key-value types for the in-memory storage layer.

use std::time::{Duration, Instant};

/// Entry stored in the MemTable
///
/// Value is `Option<Vec<u8>>` where:
/// - `Some(bytes)` = actual value
/// - `None` = tombstone (deleted key)
#[derive(Debug, Clone)]
pub struct MemTableEntry {
    /// The value bytes (None = tombstone/deleted)
    pub value: Option<Vec<u8>>,
    /// Sequence number for MVCC ordering
    pub sequence: u64,
    /// Timestamp when the entry was inserted
    pub timestamp: Instant,
}

impl MemTableEntry {
    /// Create a new entry with a value
    #[inline]
    pub fn new(value: Vec<u8>, sequence: u64) -> Self {
        Self {
            value: Some(value),
            sequence,
            timestamp: Instant::now(),
        }
    }

    /// Create a tombstone entry (marks key as deleted)
    #[inline]
    pub fn tombstone(sequence: u64) -> Self {
        Self {
            value: None,
            sequence,
            timestamp: Instant::now(),
        }
    }

    /// Check if this is a tombstone
    #[inline]
    pub fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }

    /// Estimate size in bytes
    #[inline]
    pub fn size_bytes(&self) -> usize {
        // size of Option + sequence (8) + Instant (~16)
        24 + self.value.as_ref().map_or(0, |v| v.len())
    }
}

/// Statistics for a MemTable
#[derive(Debug, Clone, Default)]
pub struct MemTableStats {
    /// Number of entries (including tombstones)
    pub entry_count: usize,
    /// Approximate size in bytes
    pub size_bytes: usize,
    /// Age of oldest entry
    pub oldest_entry_age: Option<Duration>,
    /// Age of newest entry
    pub newest_entry_age: Option<Duration>,
    /// Number of tombstone entries
    pub tombstone_count: usize,
}

/// Configuration for MemTable behavior
#[derive(Debug, Clone)]
pub struct MemTableConfig {
    /// Maximum size before flush (bytes)
    pub max_size: usize,
    /// Maximum number of entries
    pub max_entries: usize,
    /// Maximum age before flush
    pub max_age: Duration,
}

impl Default for MemTableConfig {
    fn default() -> Self {
        Self {
            max_size: 64 * 1024 * 1024,        // 64MB (more reasonable default)
            max_entries: 1_000_000,             // 1M entries
            max_age: Duration::from_secs(300),  // 5 minutes
        }
    }
}

/// Statistics for the MemTable manager
#[derive(Debug)]
pub struct MemTableManagerStats {
    /// Stats for the active (writable) memtable
    pub active: MemTableStats,
    /// Stats for immutable memtables awaiting flush
    pub immutable: Vec<MemTableStats>,
}
