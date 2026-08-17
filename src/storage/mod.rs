//! LSM-tree based storage engine optimized for high write throughput.
//!
//! ## Architecture
//!
//! ```text
//! Write Path: Incoming Write -> WAL (Optional) -> MemTable -> SSTable
//! Read Path:  Query -> MemTable -> SSTables (newest first, with Bloom filters)
//! ```

pub mod buffer_pool;
pub mod cache;
pub mod cached_time;
pub mod compaction;
pub mod db;
pub mod direct_io;
pub mod engine;
#[cfg(test)]
mod failpoints;
pub mod fd;
pub mod iter;
pub mod manifest;
pub mod memtable;
pub mod partitioning;
pub mod sstable;
pub mod wal;

pub use buffer_pool::{BufferPool, PooledBuffer};
pub use cache::{BlockCache, CacheKey, CacheStats};
pub use compaction::{CompactionConfig, Compactor};
pub use db::{Db, DbError, DbOptions, DbStats, Result as DbResult};
pub use direct_io::{AlignedBuffer, DirectIoConfig, DirectIoWriter};
pub use engine::{Engine, Result as StorageResult, StorageConfig, StorageError};
pub use fd::{FdConfig, FdMonitor, FdStats, SSTablePool};
pub use iter::{EntryGuard, PrefixIter, RangeIter};
pub use memtable::{MemTable, MemTableConfig, MemTableEntry, MemTableManager};
pub use sstable::bloom::PrefixBloomFilter;
pub use sstable::{SSTableConfig, SSTableInfo, SSTableReader, SSTableWriter};
pub use wal::{WalConfig, WalEntry, WalError, WriteAheadLog};
