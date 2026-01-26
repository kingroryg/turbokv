//! # TurboKV Storage Engine
//!
//! LSM-tree based storage engine optimized for high write throughput.
//!
//! ## Architecture Overview
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                      Write Path                              │
//! │                                                              │
//! │  Incoming Write ──> WAL (Optional) ──> MemTable             │
//! │                      │                       │               │
//! │                      ▼                       ▼               │
//! │                   Persist                 Flush to           │
//! │                   to Disk                 SSTable            │
//! └─────────────────────────────────────────────────────────────┘
//!
//! ┌─────────────────────────────────────────────────────────────┐
//! │                      Read Path                               │
//! │                                                              │
//! │  Query ──> Check MemTable ──> Check SSTables (newest first) │
//! │              │                    │                          │
//! │              ▼                    ▼                          │
//! │           Hot Data            Bloom Filters                  │
//! │           (Fast)              (Skip files)                   │
//! └─────────────────────────────────────────────────────────────┘
//! ```

pub mod buffer_pool;
pub mod cache;
pub mod cached_time;
pub mod compaction;
pub mod db;
pub mod direct_io;
pub mod engine;
pub mod fd;
pub mod manifest;
pub mod memtable;
pub mod partitioning;
pub mod sstable;
pub mod wal;

// Primary API - use this for most applications
pub use db::{Db, DbError, DbOptions, DbStats, Result as DbResult};

// Engine exports (lower-level API)
pub use engine::{Engine, Result as StorageResult, StorageConfig, StorageError};

// WAL exports
pub use wal::{WalConfig, WalEntry, WalError, WriteAheadLog};

// MemTable exports
pub use memtable::{MemTable, MemTableConfig, MemTableEntry, MemTableManager};

// SSTable exports
pub use sstable::{SSTableConfig, SSTableInfo, SSTableReader, SSTableWriter};

// Compaction exports
pub use compaction::{CompactionConfig, Compactor};

// Cache exports
pub use cache::{BlockCache, CacheKey, CacheStats};

// FD management exports
pub use fd::{FdConfig, FdMonitor, FdStats, SSTablePool};

// Buffer pool exports (optimization for high-throughput writes)
pub use buffer_pool::{BufferPool, PooledBuffer};

// Direct I/O exports (bypass OS cache)
pub use direct_io::{AlignedBuffer, DirectIoConfig, DirectIoWriter};

// Prefix bloom filter exports
pub use sstable::bloom::PrefixBloomFilter;
