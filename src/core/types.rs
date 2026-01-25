//! # Core Types
//!
//! Minimal type definitions for TurboKV - a generic key-value store.
//!
//! Keys and values are raw bytes (`&[u8]` / `Vec<u8>`).
//! Serialization is the caller's responsibility.

use std::time::Duration;

/// Compression algorithm options
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Compression {
    /// No compression
    None,
    /// LZ4 - fast compression
    #[default]
    Lz4,
    /// Snappy - balanced
    Snappy,
    /// Zstd - high compression ratio
    Zstd,
}

/// Compaction style
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompactionStyle {
    /// Size-tiered compaction - good for write-heavy workloads
    #[default]
    SizeTiered,
    /// Leveled compaction - good for read-heavy workloads
    Leveled,
}

/// Database configuration
#[derive(Debug, Clone)]
pub struct DbConfig {
    /// Enable WAL for durability (default: true)
    pub wal_enabled: bool,
    /// Enable Merkle chains for tamper-proof storage (default: false for speed)
    pub merkle_enabled: bool,
    /// Compression algorithm (default: Lz4)
    pub compression: Compression,
    /// Sync writes to disk immediately (default: true)
    pub sync_writes: bool,
    /// MemTable size before flush (default: 64MB)
    pub memtable_size: usize,
    /// Block cache size in bytes (default: 64MB, 0 to disable)
    pub block_cache_size: usize,
    /// Maximum number of open files (default: 1000)
    pub max_open_files: usize,
    /// Compaction style (default: SizeTiered)
    pub compaction_style: CompactionStyle,
    /// Maximum WAL file size before rotation (default: 128MB)
    pub max_wal_size: usize,
    /// Flush interval for background flush (default: 60s)
    pub flush_interval: Duration,
    /// Compaction interval (default: 300s)
    pub compaction_interval: Duration,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            wal_enabled: true,
            merkle_enabled: false, // OFF by default for speed
            compression: Compression::Lz4,
            sync_writes: true,
            memtable_size: 64 * 1024 * 1024,       // 64MB
            block_cache_size: 64 * 1024 * 1024,    // 64MB
            max_open_files: 1000,
            compaction_style: CompactionStyle::SizeTiered,
            max_wal_size: 128 * 1024 * 1024,       // 128MB
            flush_interval: Duration::from_secs(60),
            compaction_interval: Duration::from_secs(300),
        }
    }
}

impl DbConfig {
    /// Fast configuration - maximum speed, no durability guarantees
    ///
    /// Use for: caches, ephemeral data, benchmarks
    pub fn fast() -> Self {
        Self {
            wal_enabled: false,
            merkle_enabled: false,
            compression: Compression::None,
            sync_writes: false,
            memtable_size: 256 * 1024 * 1024,      // 256MB - larger to reduce flushes
            block_cache_size: 128 * 1024 * 1024,   // 128MB
            max_open_files: 1000,
            compaction_style: CompactionStyle::SizeTiered,
            max_wal_size: 256 * 1024 * 1024,
            flush_interval: Duration::from_secs(300),
            compaction_interval: Duration::from_secs(600),
        }
    }

    /// Durable configuration - data survives crashes
    ///
    /// Use for: production data that must not be lost
    pub fn durable() -> Self {
        Self {
            wal_enabled: true,
            merkle_enabled: false,
            compression: Compression::Lz4,
            sync_writes: true,
            memtable_size: 64 * 1024 * 1024,
            block_cache_size: 64 * 1024 * 1024,
            max_open_files: 1000,
            compaction_style: CompactionStyle::SizeTiered,
            max_wal_size: 128 * 1024 * 1024,
            flush_interval: Duration::from_secs(60),
            compaction_interval: Duration::from_secs(300),
        }
    }

    /// Tamper-proof configuration - cryptographic integrity verification
    ///
    /// Use for: audit logs, compliance data, legal evidence
    pub fn tamper_proof() -> Self {
        Self {
            wal_enabled: true,
            merkle_enabled: true,
            compression: Compression::Lz4,
            sync_writes: true,
            memtable_size: 64 * 1024 * 1024,
            block_cache_size: 64 * 1024 * 1024,
            max_open_files: 1000,
            compaction_style: CompactionStyle::SizeTiered,
            max_wal_size: 128 * 1024 * 1024,
            flush_interval: Duration::from_secs(60),
            compaction_interval: Duration::from_secs(300),
        }
    }
}

/// Storage statistics
#[derive(Debug, Clone, Default)]
pub struct StorageStats {
    /// Total number of keys in the database
    pub total_keys: u64,
    /// Total size of all data in bytes
    pub total_bytes: u64,
    /// Current WAL size in bytes
    pub wal_size: u64,
    /// Number of SSTable files
    pub sstable_count: u32,
    /// Current MemTable size in bytes
    pub memtable_size: u64,
    /// Whether compaction is pending
    pub compaction_pending: bool,
}

/// Compaction result
#[derive(Debug, Clone, Default)]
pub struct CompactionResult {
    /// Number of files compacted
    pub files_compacted: u32,
    /// Bytes reclaimed
    pub bytes_reclaimed: u64,
    /// Duration of compaction in milliseconds
    pub duration_ms: u64,
}

/// Write batch operation type
#[derive(Debug, Clone)]
pub enum BatchOp {
    /// Insert or update a key-value pair
    Put { key: Vec<u8>, value: Vec<u8> },
    /// Delete a key
    Delete { key: Vec<u8> },
}

/// Atomic write batch
#[derive(Debug, Clone, Default)]
pub struct WriteBatch {
    ops: Vec<BatchOp>,
}

impl WriteBatch {
    /// Create a new empty write batch
    pub fn new() -> Self {
        Self { ops: Vec::new() }
    }

    /// Create a write batch with pre-allocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self { ops: Vec::with_capacity(capacity) }
    }

    /// Add a put operation
    pub fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, value: V) {
        self.ops.push(BatchOp::Put {
            key: key.as_ref().to_vec(),
            value: value.as_ref().to_vec(),
        });
    }

    /// Add a delete operation
    pub fn delete<K: AsRef<[u8]>>(&mut self, key: K) {
        self.ops.push(BatchOp::Delete {
            key: key.as_ref().to_vec(),
        });
    }

    /// Get the operations in this batch
    pub fn ops(&self) -> &[BatchOp] {
        &self.ops
    }

    /// Get the number of operations in this batch
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Clear all operations from the batch
    pub fn clear(&mut self) {
        self.ops.clear();
    }
}
