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

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use tracing::info;

use super::types::{estimated_memtable_entry_size, MemTableConfig, MemTableEntry, MemTableStats};
use crate::storage::prefix_upper_bound;

/// Raw-table, manager, and batch mutations share these same-key stripes so a
/// replacement and its physical counters remain atomic across every entry
/// point. The stripe count preserves the manager's existing contention scale.
const MUTATION_LOCK_STRIPES: usize = 65_536;
static MUTATION_LOCKS: std::sync::LazyLock<Box<[Mutex<()>]>> =
    std::sync::LazyLock::new(|| (0..MUTATION_LOCK_STRIPES).map(|_| Mutex::new(())).collect());

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

#[derive(Clone, Copy)]
struct PreviousEntry {
    tombstone: bool,
    size: usize,
}

enum MutationResult {
    Ignored,
    Applied(Option<PreviousEntry>),
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
        let _mutation = self.lock_mutation(key);
        self.insert_with_sequence_prelocked(key, value, sequence)
    }

    /// Apply an insert while the table's same-key mutation lock is held.
    pub(crate) fn insert_with_sequence_prelocked(
        &self,
        key: &[u8],
        value: &[u8],
        sequence: u64,
    ) -> Result<()> {
        self.prepare_mutation(sequence)?;
        self.apply_insert(key, value, sequence);
        Ok(())
    }

    /// Apply a batch insert after the manager has made the active table stable.
    pub(crate) fn insert_batch_entry_prelocked(&self, key: &[u8], value: &[u8], sequence: u64) {
        self.observe_sequence(sequence);
        self.apply_insert(key, value, sequence);
    }

    fn apply_insert(&self, key: &[u8], value: &[u8], sequence: u64) {
        let entry_size = estimated_memtable_entry_size(key, Some(value));

        let entry = MemTableEntry::new(value.to_vec(), sequence);
        let MutationResult::Applied(previous) = self.replace_if_not_newer(key, entry) else {
            return;
        };

        self.replace_accounted_size(previous.map_or(0, |entry| entry.size), entry_size);

        if previous.is_some_and(|entry| entry.tombstone) {
            // The physical slot already exists; only its kind changes.
            self.tombstone_count.fetch_sub(1, Ordering::Relaxed);
        } else if previous.is_none() {
            // New key: increment entry count
            self.entry_count.fetch_add(1, Ordering::Relaxed);
        }
        // Overwrite of existing non-tombstone: don't change counts
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
        let _mutation = self.lock_mutation(key);
        self.delete_with_sequence_prelocked(key, sequence)
    }

    /// Apply a tombstone while the table's same-key mutation lock is held.
    pub(crate) fn delete_with_sequence_prelocked(&self, key: &[u8], sequence: u64) -> Result<()> {
        self.prepare_mutation(sequence)?;
        self.apply_delete(key, sequence);
        Ok(())
    }

    /// Apply a batch tombstone after the manager has made the active table stable.
    pub(crate) fn delete_batch_entry_prelocked(&self, key: &[u8], sequence: u64) {
        self.observe_sequence(sequence);
        self.apply_delete(key, sequence);
    }

    fn apply_delete(&self, key: &[u8], sequence: u64) {
        let entry = MemTableEntry::tombstone(sequence);
        let MutationResult::Applied(previous) = self.replace_if_not_newer(key, entry) else {
            return;
        };

        if previous.is_some_and(|entry| !entry.tombstone) {
            self.tombstone_count.fetch_add(1, Ordering::Relaxed);
        } else if previous.is_none() {
            self.entry_count.fetch_add(1, Ordering::Relaxed);
            self.tombstone_count.fetch_add(1, Ordering::Relaxed);
        }

        self.replace_accounted_size(
            previous.map_or(0, |entry| entry.size),
            estimated_memtable_entry_size(key, None),
        );
    }

    fn replace_if_not_newer(&self, key: &[u8], replacement: MemTableEntry) -> MutationResult {
        let previous = Cell::new(None);
        // `compare_insert` may retry its comparison after a concurrent
        // structural change. These cells always describe its latest decision.
        let accepted = Cell::new(true);
        let sequence = replacement.sequence;
        self.data
            .compare_insert(key.to_vec(), replacement, |existing| {
                previous.set(None);
                if existing.sequence > sequence {
                    accepted.set(false);
                    return false;
                }
                accepted.set(true);
                previous.set(Some(PreviousEntry {
                    tombstone: existing.is_tombstone(),
                    size: estimated_memtable_entry_size(key, existing.value.as_deref()),
                }));
                true
            });
        if accepted.get() {
            MutationResult::Applied(previous.get())
        } else {
            MutationResult::Ignored
        }
    }

    fn replace_accounted_size(&self, previous: usize, replacement: usize) {
        if replacement >= previous {
            self.size_bytes
                .fetch_add(replacement - previous, Ordering::Relaxed);
        } else {
            self.size_bytes
                .fetch_sub(previous - replacement, Ordering::Relaxed);
        }
    }

    pub(crate) fn lock_mutation(&self, key: &[u8]) -> parking_lot::MutexGuard<'static, ()> {
        MUTATION_LOCKS[self.mutation_lock_index(key)].lock()
    }

    pub(crate) fn lock_mutations<'a>(
        &self,
        keys: impl Iterator<Item = &'a [u8]>,
    ) -> Vec<parking_lot::MutexGuard<'static, ()>> {
        let mut indices: Vec<_> = keys.map(|key| self.mutation_lock_index(key)).collect();
        indices.sort_unstable();
        indices.dedup();
        indices
            .into_iter()
            .map(|index| MUTATION_LOCKS[index].lock())
            .collect()
    }

    fn mutation_lock_index(&self, key: &[u8]) -> usize {
        // Mix in the table identity so equal keys in unrelated tables rarely
        // contend. Moving a table requires exclusive ownership, so its address
        // remains stable whenever concurrent methods can run.
        let mut hash = 0xcbf2_9ce4_8422_2325_u64 ^ self as *const Self as usize as u64;
        for byte in key {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        hash as usize % MUTATION_LOCK_STRIPES
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

    /// Return the lowest and highest engine sequence retained by this table.
    pub(crate) fn sequence_bounds(&self) -> Option<(u64, u64)> {
        self.data.iter().fold(None, |bounds, entry| {
            let sequence = entry.value().sequence;
            Some(
                bounds.map_or((sequence, sequence), |(min, max): (u64, u64)| {
                    (min.min(sequence), max.max(sequence))
                }),
            )
        })
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

    fn prepare_mutation(&self, sequence: u64) -> Result<()> {
        if self.read_only.load(Ordering::Acquire) {
            return Err(MemTableError::ReadOnly);
        }
        if self.should_flush() {
            return Err(MemTableError::Full);
        }

        self.observe_sequence(sequence);
        Ok(())
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
    fn replacement_transitions_preserve_physical_entry_and_tombstone_counts() {
        let table = MemTable::new(test_config());

        table.delete(b"key").unwrap();
        assert_eq!(table.stats().entry_count, 1);
        assert_eq!(table.stats().tombstone_count, 1);
        assert_eq!(table.stats().size_bytes, b"key".len() + 32);

        table.delete(b"key").unwrap();
        assert_eq!(table.stats().entry_count, 1);
        assert_eq!(table.stats().tombstone_count, 1);
        assert_eq!(table.stats().size_bytes, b"key".len() + 32);

        table.insert(b"key", b"value").unwrap();
        assert_eq!(table.stats().entry_count, 1);
        assert_eq!(table.stats().tombstone_count, 0);
        assert_eq!(table.stats().size_bytes, b"key".len() + b"value".len() + 32);

        table.insert(b"key", b"new-value").unwrap();
        assert_eq!(table.stats().entry_count, 1);
        assert_eq!(table.stats().tombstone_count, 0);
        assert_eq!(
            table.stats().size_bytes,
            b"key".len() + b"new-value".len() + 32
        );

        table.delete(b"key").unwrap();
        assert_eq!(table.stats().entry_count, 1);
        assert_eq!(table.stats().tombstone_count, 1);
        assert_eq!(table.stats().size_bytes, b"key".len() + 32);
    }

    #[test]
    fn concurrent_same_key_replacements_keep_physical_counters_bounded() {
        const WRITERS: usize = 16;

        let table = Arc::new(MemTable::new(test_config()));
        table.delete(b"shared").unwrap();
        let ready = Arc::new(std::sync::Barrier::new(WRITERS + 1));
        let writers = (0..WRITERS)
            .map(|index| {
                let table = Arc::clone(&table);
                let ready = Arc::clone(&ready);
                std::thread::spawn(move || {
                    ready.wait();
                    table
                        .insert(b"shared", format!("value-{index}").as_bytes())
                        .unwrap();
                })
            })
            .collect::<Vec<_>>();
        ready.wait();
        for writer in writers {
            writer.join().unwrap();
        }
        assert_eq!(table.stats().entry_count, 1);
        assert_eq!(table.stats().tombstone_count, 0);

        let ready = Arc::new(std::sync::Barrier::new(WRITERS + 1));
        let deleters = (0..WRITERS)
            .map(|_| {
                let table = Arc::clone(&table);
                let ready = Arc::clone(&ready);
                std::thread::spawn(move || {
                    ready.wait();
                    table.delete(b"shared").unwrap();
                })
            })
            .collect::<Vec<_>>();
        ready.wait();
        for deleter in deleters {
            deleter.join().unwrap();
        }
        assert_eq!(table.stats().entry_count, 1);
        assert_eq!(table.stats().tombstone_count, 1);
        assert_eq!(table.stats().size_bytes, b"shared".len() + 32);
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

    #[test]
    fn stale_equal_and_newer_sequences_preserve_value_and_accounting() {
        let table = MemTable::new(test_config());

        table.insert_with_sequence(b"key", b"first", 10).unwrap();
        let initial = table.stats();
        table.insert_with_sequence(b"key", b"stale", 9).unwrap();
        assert_eq!(table.get(b"key"), Some(b"first".to_vec()));
        assert_eq!(table.stats().entry_count, initial.entry_count);
        assert_eq!(table.stats().size_bytes, initial.size_bytes);

        table.insert_with_sequence(b"key", b"equal", 10).unwrap();
        assert_eq!(table.get(b"key"), Some(b"equal".to_vec()));
        assert_eq!(table.stats().entry_count, 1);
        assert_eq!(
            table.stats().size_bytes,
            estimated_memtable_entry_size(b"key", Some(b"equal"))
        );

        table.delete_with_sequence(b"key", 11).unwrap();
        assert_eq!(table.get(b"key"), None);
        assert_eq!(table.stats().entry_count, 1);
        assert_eq!(table.stats().tombstone_count, 1);
        assert_eq!(
            table.stats().size_bytes,
            estimated_memtable_entry_size(b"key", None)
        );

        table
            .insert_with_sequence(b"key", b"stale-again", 10)
            .unwrap();
        assert_eq!(table.get(b"key"), None);
        assert_eq!(table.stats().entry_count, 1);
        assert_eq!(table.stats().tombstone_count, 1);
        assert_eq!(
            table.stats().size_bytes,
            estimated_memtable_entry_size(b"key", None)
        );

        table.insert_with_sequence(b"key", b"revived", 12).unwrap();
        table.delete_with_sequence(b"other", 13).unwrap();
        assert_eq!(table.get(b"key"), Some(b"revived".to_vec()));
        assert_eq!(table.get(b"other"), None);
        assert_eq!(table.stats().entry_count, 2);
        assert_eq!(table.stats().tombstone_count, 1);
        assert_eq!(
            table.stats().size_bytes,
            estimated_memtable_entry_size(b"key", Some(b"revived"))
                + estimated_memtable_entry_size(b"other", None)
        );
        assert_eq!(table.current_sequence(), 14);
    }
}
