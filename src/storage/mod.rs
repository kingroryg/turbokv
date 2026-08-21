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

pub mod cache;
mod cached_time;
pub mod compaction;
pub mod db;
mod directory_lock;
pub mod engine;
#[cfg(test)]
#[allow(deprecated)]
mod failpoints;
pub mod fd;
pub mod iter;
mod maintenance;
pub mod manifest;
pub mod memtable;
pub mod sstable;
#[cfg(test)]
mod test_support;
mod version;
pub mod wal;

pub(crate) fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let last_incrementable = prefix.iter().rposition(|byte| *byte != u8::MAX)?;
    let mut upper = prefix[..=last_incrementable].to_vec();
    upper[last_incrementable] += 1;
    Some(upper)
}

pub(super) struct InProgressGuard {
    counter: Arc<AtomicU64>,
}

impl InProgressGuard {
    pub(super) fn new(counter: Arc<AtomicU64>) -> Self {
        counter.fetch_add(1, Ordering::AcqRel);
        Self { counter }
    }
}

impl Drop for InProgressGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}
