//! # MemTable - In-Memory Key-Value Storage
//!
//! Lock-free concurrent skip list for fast in-memory key-value storage.
//!
//! ## Key Optimizations (PRESERVED)
//!
//! - **crossbeam_skiplist::SkipMap** - Lock-free concurrent skip list
//! - **Atomic counters** - Lock-free size_bytes, entry_count, sequence tracking
//! - **Inline size estimation** - Fast path avoids function call overhead
//! - **Read-only flag** - Atomic coordination for flush without blocking writes

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crossbeam_skiplist::SkipMap;
use tracing::info;

use super::types::{MemTableConfig, MemTableEntry, MemTableStats};

/// Result type for MemTable operations
pub type Result<T> = std::result::Result<T, MemTableError>;

/// MemTable error types
#[derive(Debug, Clone, thiserror::Error)]
pub enum MemTableError {
    #[error("MemTable is read-only (being flushed)")]
    ReadOnly,
    #[error("MemTable is full")]
    Full,
}

/// In-memory key-value storage backed by a lock-free skip list.
///
/// Keys are byte slices that are stored in sorted order for efficient range scans.
/// Values can be actual data or tombstones (marking deleted keys).
pub struct MemTable {
    /// Lock-free skip list: key bytes -> entry
    /// Using `Vec<u8>` as key for byte-level ordering
    pub(crate) data: Arc<SkipMap<Vec<u8>, MemTableEntry>>,

    /// Approximate size in bytes (atomic for lock-free updates)
    pub(crate) size_bytes: Arc<AtomicUsize>,

    /// Number of entries including tombstones
    pub(crate) entry_count: Arc<AtomicUsize>,

    /// Number of tombstone entries
    pub(crate) tombstone_count: Arc<AtomicUsize>,

    /// Monotonically increasing sequence number for MVCC
    pub(crate) sequence: Arc<AtomicU64>,

    /// When this memtable was created
    pub(crate) created_at: Instant,

    /// Configuration
    pub(crate) config: MemTableConfig,

    /// Flag to mark memtable as read-only during flush
    read_only: Arc<AtomicBool>,
}

impl MemTable {
    /// Create a new MemTable with the given configuration
    pub fn new(config: MemTableConfig) -> Self {
        Self {
            data: Arc::new(SkipMap::new()),
            size_bytes: Arc::new(AtomicUsize::new(0)),
            entry_count: Arc::new(AtomicUsize::new(0)),
            tombstone_count: Arc::new(AtomicUsize::new(0)),
            sequence: Arc::new(AtomicU64::new(0)),
            created_at: Instant::now(),
            config,
            read_only: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Insert a key-value pair
    ///
    /// Returns the sequence number assigned to this operation.
    #[inline]
    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<u64> {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        self.insert_with_sequence(key, value, sequence)?;
        Ok(sequence)
    }

    /// Insert a key-value pair with an engine-assigned sequence number.
    ///
    /// An older mutation is ignored if a newer version of the key has already
    /// reached this memtable.
    #[inline]
    pub(crate) fn insert_with_sequence(
        &self,
        key: &[u8],
        value: &[u8],
        sequence: u64,
    ) -> Result<()> {
        if !self.prepare_mutation(key, sequence)? {
            return Ok(());
        }

        // Inline size estimation for fast path
        let entry_size = key.len() + value.len() + 32; // 32 for entry overhead

        let entry = MemTableEntry::new(value.to_vec(), sequence);

        // Check if key already exists BEFORE inserting
        let existing = self.data.get(key);
        let was_existing = existing.is_some();
        let was_tombstone = existing.map(|e| e.value().is_tombstone()).unwrap_or(false);

        self.data.insert(key.to_vec(), entry);

        self.size_bytes.fetch_add(entry_size, Ordering::Relaxed);

        if was_tombstone {
            // Replacing a tombstone: reduce tombstone count, add to entry count
            self.tombstone_count.fetch_sub(1, Ordering::Relaxed);
            self.entry_count.fetch_add(1, Ordering::Relaxed);
        } else if !was_existing {
            // New key: increment entry count
            self.entry_count.fetch_add(1, Ordering::Relaxed);
        }
        // Overwrite of existing non-tombstone: don't change counts

        Ok(())
    }

    /// Delete a key by inserting a tombstone
    ///
    /// Returns the sequence number assigned to this operation.
    #[inline]
    pub fn delete(&self, key: &[u8]) -> Result<u64> {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        self.delete_with_sequence(key, sequence)?;
        Ok(sequence)
    }

    /// Insert a tombstone with an engine-assigned sequence number.
    ///
    /// An older mutation is ignored if a newer version of the key has already
    /// reached this memtable.
    #[inline]
    pub(crate) fn delete_with_sequence(&self, key: &[u8], sequence: u64) -> Result<()> {
        if !self.prepare_mutation(key, sequence)? {
            return Ok(());
        }

        let entry = MemTableEntry::tombstone(sequence);

        // Check if we're deleting an existing non-tombstone entry
        let was_value = self
            .data
            .get(key)
            .map(|e| !e.value().is_tombstone())
            .unwrap_or(false);

        self.data.insert(key.to_vec(), entry);

        if was_value {
            self.tombstone_count.fetch_add(1, Ordering::Relaxed);
        } else {
            self.entry_count.fetch_add(1, Ordering::Relaxed);
            self.tombstone_count.fetch_add(1, Ordering::Relaxed);
        }

        // Approximate size (just key + overhead)
        self.size_bytes.fetch_add(key.len() + 32, Ordering::Relaxed);

        Ok(())
    }

    /// Get a value by key
    ///
    /// Returns `None` if the key doesn't exist or has been deleted (tombstone).
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.data.get(key).and_then(|entry| {
            let e = entry.value();
            if e.is_tombstone() {
                None
            } else {
                e.value.clone()
            }
        })
    }

    /// Check if a key exists (and is not a tombstone)
    #[inline]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.data
            .get(key)
            .map(|e| !e.value().is_tombstone())
            .unwrap_or(false)
    }

    /// Get the raw entry (including tombstones) for a key
    ///
    /// Used internally for compaction to know if a key was deleted.
    pub fn get_entry(&self, key: &[u8]) -> Option<MemTableEntry> {
        self.data.get(key).map(|e| e.value().clone())
    }

    /// Scan a range of keys
    ///
    /// Returns key-value pairs in sorted order. Tombstones are excluded.
    pub fn range(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.data
            .range(start.to_vec()..end.to_vec())
            .filter_map(|entry| {
                let e = entry.value();
                if e.is_tombstone() {
                    None
                } else {
                    e.value.clone().map(|v| (entry.key().clone(), v))
                }
            })
            .collect()
    }

    /// Scan all keys with a given prefix
    ///
    /// Returns key-value pairs in sorted order. Tombstones are excluded.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let start = prefix.to_vec();
        let end = prefix_upper_bound(prefix);

        let entries: Vec<_> = match end {
            Some(end) => self.data.range(start..end).collect(),
            None => self.data.range(start..).collect(),
        };

        entries
            .into_iter()
            .filter_map(|entry| {
                if !entry.key().starts_with(prefix) {
                    return None;
                }

                let e = entry.value();
                if e.is_tombstone() {
                    None
                } else {
                    e.value.clone().map(|v| (entry.key().clone(), v))
                }
            })
            .collect()
    }

    /// Check if the memtable should be flushed
    pub fn should_flush(&self) -> bool {
        let size = self.size_bytes.load(Ordering::Relaxed);
        let count = self.entry_count.load(Ordering::Relaxed);
        let age = self.created_at.elapsed();

        size >= self.config.max_size
            || count >= self.config.max_entries
            || age >= self.config.max_age
    }

    /// Mark this memtable as read-only (for flushing)
    pub fn set_read_only(&self) {
        self.read_only.store(true, Ordering::Release);
        info!(
            "MemTable marked as read-only (size: {} bytes, entries: {})",
            self.size_bytes.load(Ordering::Relaxed),
            self.entry_count.load(Ordering::Relaxed)
        );
    }

    /// Check if this memtable is read-only
    pub fn is_read_only(&self) -> bool {
        self.read_only.load(Ordering::Acquire)
    }

    /// Get all entries for flushing to SSTable
    ///
    /// Returns entries in sorted key order (including tombstones).
    pub fn get_all_entries(&self) -> Vec<(Vec<u8>, MemTableEntry)> {
        self.data
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }

    /// Get all entries as raw key-value pairs (for SSTable writing)
    ///
    /// Tombstones are included with `None` values.
    pub fn get_all_kv(&self) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        self.data
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().value.clone()))
            .collect()
    }

    /// Get the current sequence number
    pub fn current_sequence(&self) -> u64 {
        self.sequence.load(Ordering::Relaxed)
    }

    fn observe_sequence(&self, sequence: u64) {
        self.sequence
            .fetch_max(sequence.saturating_add(1), Ordering::Relaxed);
    }

    fn prepare_mutation(&self, key: &[u8], sequence: u64) -> Result<bool> {
        if self.read_only.load(Ordering::Acquire) {
            return Err(MemTableError::ReadOnly);
        }
        if self.should_flush() {
            return Err(MemTableError::Full);
        }

        self.observe_sequence(sequence);
        Ok(self
            .data
            .get(key)
            .map_or(true, |entry| entry.value().sequence <= sequence))
    }

    /// Get statistics for this memtable
    pub fn stats(&self) -> MemTableStats {
        let now = Instant::now();

        let (oldest, newest) = if let Some(first) = self.data.front() {
            let oldest = Some(now - first.value().timestamp);
            let newest = if let Some(last) = self.data.back() {
                Some(now - last.value().timestamp)
            } else {
                oldest
            };
            (oldest, newest)
        } else {
            (None, None)
        };

        MemTableStats {
            entry_count: self.entry_count.load(Ordering::Relaxed),
            size_bytes: self.size_bytes.load(Ordering::Relaxed),
            oldest_entry_age: oldest,
            newest_entry_age: newest,
            tombstone_count: self.tombstone_count.load(Ordering::Relaxed),
        }
    }

    /// Get the current sequence number
    pub fn sequence(&self) -> u64 {
        self.sequence.load(Ordering::Relaxed)
    }

    /// Get the approximate size in bytes
    pub fn size_bytes(&self) -> usize {
        self.size_bytes.load(Ordering::Relaxed)
    }

    /// Get the entry count
    pub fn entry_count(&self) -> usize {
        self.entry_count.load(Ordering::Relaxed)
    }

    /// Check if the memtable is empty
    pub fn is_empty(&self) -> bool {
        self.entry_count.load(Ordering::Relaxed) == 0
    }
}

fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let last_incrementable = prefix.iter().rposition(|byte| *byte != u8::MAX)?;
    let mut end = prefix[..=last_incrementable].to_vec();
    end[last_incrementable] += 1;
    Some(end)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> MemTableConfig {
        MemTableConfig {
            max_size: 1024 * 1024,
            max_entries: 1000,
            ..Default::default()
        }
    }

    #[test]
    fn test_insert_and_get() {
        let table = MemTable::new(test_config());

        table.insert(b"key1", b"value1").unwrap();
        table.insert(b"key2", b"value2").unwrap();

        assert_eq!(table.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(table.get(b"key2"), Some(b"value2".to_vec()));
        assert_eq!(table.get(b"key3"), None);
    }

    #[test]
    fn test_delete() {
        let table = MemTable::new(test_config());

        table.insert(b"key1", b"value1").unwrap();
        assert!(table.contains_key(b"key1"));

        table.delete(b"key1").unwrap();
        assert!(!table.contains_key(b"key1"));
        assert_eq!(table.get(b"key1"), None);
    }

    #[test]
    fn test_range_scan() {
        let table = MemTable::new(test_config());

        table.insert(b"a", b"1").unwrap();
        table.insert(b"b", b"2").unwrap();
        table.insert(b"c", b"3").unwrap();
        table.insert(b"d", b"4").unwrap();

        let range = table.range(b"b", b"d");
        assert_eq!(range.len(), 2);
        assert_eq!(range[0], (b"b".to_vec(), b"2".to_vec()));
        assert_eq!(range[1], (b"c".to_vec(), b"3".to_vec()));
    }

    #[test]
    fn test_prefix_scan() {
        let table = MemTable::new(test_config());

        table.insert(b"user:1", b"alice").unwrap();
        table.insert(b"user:2", b"bob").unwrap();
        table.insert(b"post:1", b"hello").unwrap();

        let users = table.scan_prefix(b"user:");
        assert_eq!(users.len(), 2);

        let posts = table.scan_prefix(b"post:");
        assert_eq!(posts.len(), 1);
    }

    #[test]
    fn test_prefix_scan_empty_prefix_returns_all_entries() {
        let table = MemTable::new(test_config());

        table.insert(b"a", b"1").unwrap();
        table.insert(b"b", b"2").unwrap();

        let entries = table.scan_prefix(b"");
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_prefix_scan_with_ff_byte() {
        let table = MemTable::new(test_config());

        table.insert(&[0xff, 0x01], b"first").unwrap();
        table.insert(&[0xff, 0x02], b"second").unwrap();
        table.insert(&[0xfe], b"other").unwrap();

        let entries = table.scan_prefix(&[0xff]);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], (vec![0xff, 0x01], b"first".to_vec()));
        assert_eq!(entries[1], (vec![0xff, 0x02], b"second".to_vec()));
    }

    #[test]
    fn test_overwrite() {
        let table = MemTable::new(test_config());

        table.insert(b"key", b"value1").unwrap();
        assert_eq!(table.get(b"key"), Some(b"value1".to_vec()));

        table.insert(b"key", b"value2").unwrap();
        assert_eq!(table.get(b"key"), Some(b"value2".to_vec()));
    }

    #[test]
    fn test_read_only() {
        let table = MemTable::new(test_config());

        table.insert(b"key", b"value").unwrap();
        table.set_read_only();

        assert!(matches!(
            table.insert(b"key2", b"value"),
            Err(MemTableError::ReadOnly)
        ));
    }

    #[test]
    fn older_explicit_sequence_cannot_replace_newer_version() {
        let table = MemTable::new(test_config());

        table.delete_with_sequence(b"key", 12).unwrap();
        table.insert_with_sequence(b"key", b"stale", 11).unwrap();

        let entry = table.get_entry(b"key").unwrap();
        assert_eq!(entry.sequence, 12);
        assert!(entry.is_tombstone());
        assert_eq!(table.current_sequence(), 13);
    }
}
