//! LSM-tree based storage engine optimized for high write throughput.
//!
//! ## Architecture
//!
//! ```text
//! Write Path: Incoming Write -> WAL (Optional) -> MemTable -> SSTable
//! Read Path:  Query -> MemTable -> SSTables (newest first, with Bloom filters)
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub mod buffer_pool;
pub mod cache;
pub mod cached_time;
pub mod compaction;
pub mod db;
pub mod direct_io;
mod directory_lock;
pub mod engine;
#[cfg(test)]
#[allow(deprecated)]
mod failpoints;
pub mod fd;
pub mod iter;
pub mod manifest;
pub mod memtable;
pub mod partitioning;
pub mod sstable;
mod version;
pub mod wal;

pub use buffer_pool::{BufferPool, PooledBuffer};
pub use cache::{BlockCache, CacheKey, CacheStats};
pub use compaction::{CompactionConfig, Compactor};
pub use db::{Db, DbError, DbOptions, DbStats, Result as DbResult};
pub use direct_io::{AlignedBuffer, DirectIoConfig, DirectIoWriter};
pub use engine::{Engine, Result as StorageResult, StorageConfig, StorageError};
pub use fd::{FdConfig, FdMonitor, FdStats, SSTablePool};
pub use iter::{EntryGuard, PrefixIter, RangeIter, ScanEntry, ScanError, ScanResult};
pub use memtable::{MemTable, MemTableConfig, MemTableEntry, MemTableManager};
pub use sstable::bloom::PrefixBloomFilter;
pub use sstable::{SSTableConfig, SSTableInfo, SSTableReader, SSTableWriter};
pub use wal::{WalConfig, WalEntry, WalError, WriteAheadLog};

pub(crate) fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let last_incrementable = prefix.iter().rposition(|byte| *byte != u8::MAX)?;
    let mut upper = prefix[..=last_incrementable].to_vec();
    upper[last_incrementable] += 1;
    Some(upper)
}

struct InProgressGuard {
    counter: Arc<AtomicU64>,
}

impl InProgressGuard {
    fn new(counter: Arc<AtomicU64>) -> Self {
        counter.fetch_add(1, Ordering::AcqRel);
        Self { counter }
    }
}

impl Drop for InProgressGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}
