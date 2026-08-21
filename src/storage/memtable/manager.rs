//! # MemTable Manager
//!
//! Manages active and immutable memtables for the LSM-tree.
//!
//! The manager handles:
//! - Active memtable for current writes
//! - Immutable memtables awaiting flush to SSTables
//! - Automatic rotation when memtable is full
//! - Thread-local write buffers with shared registry for cross-thread flushing
//!
//! ## Write Buffering
//!
//! Each thread has a local write buffer per manager instance to reduce lock
//! contention. Buffers are registered in a shared registry keyed by
//! (ThreadId, ManagerId), allowing `flush_thread_local()` to flush ALL threads'
//! buffers for a specific manager - not just the calling thread's buffer.

use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::ThreadId;
use tracing::info;

use super::table::{MemTable, MemTableError, Result};
use super::types::{
    estimated_memtable_entry_size, MemTableConfig, MemTableEntry, MemTableManagerStats,
};
use crate::storage::version::VersionOrder;

/// Thread-local write buffer size (number of entries before flush to main memtable)
const THREAD_LOCAL_BUFFER_SIZE: usize = 64;
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct ImmutableGenerationId(usize);

impl ImmutableGenerationId {
    fn of(table: &Arc<MemTable>) -> Self {
        Self(Arc::as_ptr(table) as usize)
    }
}

/// Global counter for assigning unique manager IDs
static MANAGER_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A buffered insert with the sequence assigned when the mutation was issued.
struct BufferEntry {
    key: Vec<u8>,
    value: Vec<u8>,
    sequence: u64,
}

/// Registry key: (thread ID, manager ID)
type RegistryKey = (ThreadId, u64);

/// Type alias for the buffer registry to reduce complexity warnings
/// Maps (ThreadId, ManagerId) -> Buffer
type BufferRegistry = Mutex<HashMap<RegistryKey, Arc<Mutex<Vec<BufferEntry>>>>>;

/// Shared registry of all thread-local buffers for cross-thread flushing
static BUFFER_REGISTRY: std::sync::LazyLock<BufferRegistry> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

/// Manages one active memtable, frozen generations, and optional write buffers.
///
/// This low-level component provides no WAL or filesystem durability. Direct
/// mutations are visible on return. Buffered mutations own copies in a
/// per-thread registry and become visible only when that buffer is drained.
/// Point reads return owned value copies; the public range and prefix helpers
/// materialize complete owned result sets. The engine wraps this component in
/// its ordering, persistence, and snapshot barriers.
pub struct MemTableManager {
    /// Unique identifier for this manager (used for buffer registry)
    id: u64,
    /// Currently active (writable) memtable
    pub active: Arc<RwLock<Arc<MemTable>>>,
    /// Immutable memtables awaiting flush
    pub immutable: Arc<RwLock<Vec<Arc<MemTable>>>>,
    /// Stable SSTable ids reserved by immutable generations while they retry.
    flush_ids: Mutex<HashMap<ImmutableGenerationId, u64>>,
    /// Configuration for new memtables
    config: MemTableConfig,
    /// Next engine-wide mutation sequence.
    next_sequence: AtomicU64,
    /// Physical mutations currently retained in per-thread write buffers.
    buffered_versions: AtomicU64,
    /// Approximate bytes currently retained in per-thread write buffers.
    buffered_bytes: AtomicU64,
    /// Serializes only batched buffer-to-memtable publication with sampling.
    buffer_handoff: RwLock<()>,
}

impl MemTableManager {
    /// Creates a manager with an empty active table and sequence zero.
    pub fn new(config: MemTableConfig) -> Self {
        Self::new_with_next_sequence(config, 0)
    }

    /// Create a manager whose next mutation receives `next_sequence`.
    pub(crate) fn new_with_next_sequence(config: MemTableConfig, next_sequence: u64) -> Self {
        let active = Arc::new(MemTable::new(config.clone()));
        let id = MANAGER_ID_COUNTER.fetch_add(1, Ordering::Relaxed);

        Self {
            id,
            active: Arc::new(RwLock::new(active)),
            immutable: Arc::new(RwLock::new(Vec::new())),
            flush_ids: Mutex::new(HashMap::new()),
            config,
            next_sequence: AtomicU64::new(next_sequence),
            buffered_versions: AtomicU64::new(0),
            buffered_bytes: AtomicU64::new(0),
            buffer_handoff: RwLock::new(()),
        }
    }

    /// Get or create the thread-local buffer for this manager
    fn get_buffer(&self) -> Arc<Mutex<Vec<BufferEntry>>> {
        let key = (std::thread::current().id(), self.id);
        let mut registry = BUFFER_REGISTRY.lock();
        registry
            .entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(Vec::with_capacity(THREAD_LOCAL_BUFFER_SIZE))))
            .clone()
    }

    /// Copies and inserts one key-value pair into the active table.
    ///
    /// The synchronous call assigns a sequence, automatically rotates a full
    /// table, and makes the mutation visible before success. It can return a
    /// memtable capacity or read-only error and provides no durability itself.
    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<u64> {
        let sequence = self.allocate_sequence();
        self.insert_with_sequence(key, value, sequence)?;
        Ok(sequence)
    }

    /// Apply an insert using a sequence assigned by the engine or WAL.
    pub(crate) fn insert_with_sequence(
        &self,
        key: &[u8],
        value: &[u8],
        sequence: u64,
    ) -> Result<()> {
        self.observe_sequence(sequence);
        for _ in 0..5 {
            let active = self.active.read();
            match active.insert_with_sequence(key, value, sequence) {
                Ok(()) => return Ok(()),
                Err(MemTableError::Full) => {
                    drop(active);
                    self.rotate_memtable()?;
                }
                Err(e) => return Err(e),
            }
        }
        Err(MemTableError::Full)
    }

    /// Insert a key-value pair using thread-local buffering (fast path)
    ///
    /// Writes are accumulated in a thread-local buffer and batch-inserted
    /// when the buffer is full. This reduces lock contention significantly.
    ///
    /// Key and value bytes are copied into the buffer. A full buffer is drained
    /// synchronously and can return a capacity or read-only error; otherwise
    /// the mutation is not visible through the manager's read methods until a
    /// later buffer drain. Engine-level ordered barriers perform that drain
    /// before reads or non-buffered mutations.
    #[inline]
    pub fn insert_buffered(&self, key: &[u8], value: &[u8]) -> Result<u64> {
        let sequence = self.allocate_sequence();
        let buffer = self.get_buffer();
        let mut buf = buffer.lock();
        buf.push(BufferEntry {
            key: key.to_vec(),
            value: value.to_vec(),
            sequence,
        });
        self.buffered_bytes.fetch_add(
            estimated_memtable_entry_size(key, Some(value)) as u64,
            Ordering::Relaxed,
        );
        // Publish the complete buffered entry before the foreground call can
        // acknowledge it.
        self.buffered_versions.fetch_add(1, Ordering::Release);

        // Flush buffer when full
        if buf.len() >= THREAD_LOCAL_BUFFER_SIZE {
            let _handoff = self.buffer_handoff.write();
            let entries: Vec<_> = buf.drain(..).collect();
            drop(buf); // Release lock before calling flush
            let result = self.flush_buffer(&entries);
            self.release_buffered_entries(&entries);
            result?;
        }
        Ok(sequence)
    }

    /// Copies and inserts multiple pairs while rotating at batch boundaries.
    ///
    /// This low-level helper allocates a staging vector. It preserves input
    /// order and sequences, but it is not a durable database batch and an error
    /// can follow earlier in-memory application; use `Db::write_batch` for the
    /// engine's atomic visibility and recovery contract.
    pub fn insert_many(&self, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        let start_sequence = self.reserve_sequence_range(entries.len());
        let buffered: Vec<_> = entries
            .iter()
            .enumerate()
            .map(|(index, (key, value))| BufferEntry {
                key: key.clone(),
                value: value.clone(),
                sequence: start_sequence + index as u64,
            })
            .collect();
        self.flush_buffer(&buffered)
    }

    /// Apply inserts using sequences already assigned by the WAL.
    pub(crate) fn insert_many_with_sequences(
        &self,
        entries: &[(Vec<u8>, Vec<u8>)],
        sequences: &[u64],
    ) -> Result<()> {
        debug_assert_eq!(entries.len(), sequences.len());
        let buffered: Vec<_> = entries
            .iter()
            .zip(sequences)
            .map(|((key, value), &sequence)| {
                self.observe_sequence(sequence);
                BufferEntry {
                    key: key.clone(),
                    value: value.clone(),
                    sequence,
                }
            })
            .collect();
        self.flush_buffer(&buffered)
    }

    /// Apply an atomic logical batch without exposing a fallible prefix.
    ///
    /// Capacity thresholds are checked after every operation in the ordinary
    /// path. Here the active generation is held stable, all operations are
    /// applied with their preassigned sequences, and an oversized result is
    /// rotated only after the complete batch exists.
    pub(crate) fn apply_batch_with_sequences(
        &self,
        entries: &[(&[u8], Option<&[u8]>)],
        sequences: &[u64],
    ) -> Result<()> {
        debug_assert_eq!(entries.len(), sequences.len());
        for &sequence in sequences {
            self.observe_sequence(sequence);
        }

        let active = self.active.read();
        // Keep the complete batch under a sorted set of table-owned mutation
        // locks so its replacements and physical counters publish together.
        let _mutations = active.lock_mutations(entries.iter().map(|(key, _)| *key));
        if active.is_read_only() {
            return Err(MemTableError::ReadOnly);
        }
        for ((key, value), &sequence) in entries.iter().zip(sequences) {
            match value {
                Some(value) => active.insert_batch_entry_prelocked(key, value, sequence),
                None => active.delete_batch_entry_prelocked(key, sequence),
            }
        }
        let should_rotate = active.should_flush();
        drop(active);
        if should_rotate {
            self.rotate_memtable()?;
        }
        Ok(())
    }

    /// Flush thread-local buffer to main memtable
    fn flush_buffer(&self, entries: &[BufferEntry]) -> Result<()> {
        let mut start_idx = 0;
        for _ in 0..5 {
            let active = self.active.read();
            let mutations = active.lock_mutations(
                entries[start_idx..]
                    .iter()
                    .map(|entry| entry.key.as_slice()),
            );

            // Try to insert remaining entries
            let mut success = true;
            for (i, entry) in entries[start_idx..].iter().enumerate() {
                match active.insert_with_sequence_prelocked(
                    &entry.key,
                    &entry.value,
                    entry.sequence,
                ) {
                    Ok(()) => {}
                    Err(MemTableError::Full) => {
                        start_idx += i;
                        success = false;
                        break;
                    }
                    Err(e) => return Err(e),
                }
            }

            if success {
                return Ok(());
            }

            // Rotation needed
            drop(mutations);
            drop(active);
            self.rotate_memtable()?;
        }
        Err(MemTableError::Full)
    }

    /// Drains this manager's registered buffers from all threads.
    ///
    /// This iterates over the global buffer registry and flushes each buffer
    /// that belongs to this manager instance.
    /// The synchronous call can block on buffer and memtable locks, and can
    /// return a capacity or read-only error after attempting a drain. Call this
    /// before using the low-level read helpers when buffered inserts must be
    /// visible.
    pub fn flush_thread_local(&self) -> Result<()> {
        // Get all registered buffers for this manager
        let registry = BUFFER_REGISTRY.lock();

        for ((_, manager_id), buffer) in registry.iter() {
            // Only flush buffers belonging to this manager
            if *manager_id != self.id {
                continue;
            }
            let mut buf = buffer.lock();
            if !buf.is_empty() {
                let _handoff = self.buffer_handoff.write();
                let entries: Vec<_> = buf.drain(..).collect();
                drop(buf);
                let result = self.flush_buffer(&entries);
                self.release_buffered_entries(&entries);
                result?;
            }
        }
        Ok(())
    }

    /// Copies `key` and publishes a tombstone in the active table.
    ///
    /// The synchronous call assigns a sequence, automatically rotates a full
    /// table, and makes the deletion visible before success. It can return a
    /// memtable capacity or read-only error and provides no durability itself.
    pub fn delete(&self, key: &[u8]) -> Result<u64> {
        let sequence = self.allocate_sequence();
        self.delete_with_sequence(key, sequence)?;
        Ok(sequence)
    }

    /// Apply a tombstone using a sequence assigned by the engine or WAL.
    pub(crate) fn delete_with_sequence(&self, key: &[u8], sequence: u64) -> Result<()> {
        self.observe_sequence(sequence);
        for _ in 0..5 {
            let active = self.active.read();
            match active.delete_with_sequence(key, sequence) {
                Ok(()) => return Ok(()),
                Err(MemTableError::Full) => {
                    drop(active);
                    self.rotate_memtable()?;
                }
                Err(e) => return Err(e),
            }
        }
        Err(MemTableError::Full)
    }

    /// Returns an owned copy of the newest in-memory value for `key`.
    ///
    /// All active and immutable candidates are resolved by sequence and a
    /// newest tombstone returns `None`. Buffered entries must first be drained.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.get_entry(key).and_then(|entry| entry.value)
    }

    /// Check if a key exists
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.get_entry(key)
            .is_some_and(|entry| !entry.is_tombstone())
    }

    /// Get the raw entry (including tombstones) for compaction
    pub fn get_entry(&self, key: &[u8]) -> Option<MemTableEntry> {
        // Active-first capture closes the rotation handoff: if rotation happens
        // before this read, the immutable pass sees the old table; if it
        // happens after, this candidate already retains the entry.
        let mut newest = self
            .active
            .read()
            .get_entry(key)
            .map(|entry| (entry, u64::MAX));
        for (generation_rank, table) in self.immutable.read().iter().enumerate() {
            if let Some(entry) = table.get_entry(key) {
                let generation_rank = generation_rank as u64;
                if newest.as_ref().map_or(true, |(current, current_rank)| {
                    memory_entry_is_newer(&entry, generation_rank, current, *current_rank)
                }) {
                    newest = Some((entry, generation_rank));
                }
            }
        }
        newest.map(|(entry, _)| entry)
    }

    /// Materializes the half-open range `[start, end)` across all memtables.
    ///
    /// The result contains owned key/value copies in sorted order, resolves
    /// duplicate versions and tombstones, and allocates proportional to the
    /// complete result. Buffered entries must first be drained.
    pub fn range(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.range_entries(start, end)
            .into_iter()
            .filter_map(|(key, entry)| entry.value.map(|value| (key, value)))
            .collect()
    }

    /// Scan a range while retaining sequence numbers and tombstones.
    pub(crate) fn range_entries(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, MemTableEntry)> {
        self.merge_entries(|key| key >= start && key < end)
    }

    /// Materializes all matching keys across all memtables.
    ///
    /// The result contains owned key/value copies in sorted order, resolves
    /// duplicate versions and tombstones, and allocates proportional to the
    /// complete result. Buffered entries must first be drained.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.scan_prefix_entries(prefix)
            .into_iter()
            .filter_map(|(key, entry)| entry.value.map(|value| (key, value)))
            .collect()
    }

    /// Scan a prefix while retaining sequence numbers and tombstones.
    pub(crate) fn scan_prefix_entries(&self, prefix: &[u8]) -> Vec<(Vec<u8>, MemTableEntry)> {
        self.merge_entries(|key| key.starts_with(prefix))
    }

    /// Retain the current immutable read view for a streaming scan.
    ///
    /// The caller must first exclude mutations and rotate the active table.
    /// Cloned handles remain valid after a concurrent flush removes a table
    /// from the manager's live list.
    pub(crate) fn snapshot_immutable_tables(&self) -> Vec<Arc<MemTable>> {
        self.immutable.read().clone()
    }

    /// Rotate the active memtable to immutable
    fn rotate_memtable(&self) -> Result<()> {
        let mut active_lock = self.active.write();

        // Double-check if rotation is still needed
        if !active_lock.should_flush() {
            return Ok(());
        }

        info!("Rotating MemTable");

        // Mark old table as read-only
        active_lock.set_read_only();
        let old_table = active_lock.clone();

        // Add to immutable list
        self.immutable.write().push(old_table);

        // Create new active memtable
        *active_lock = Arc::new(MemTable::new(self.config.clone()));

        Ok(())
    }

    /// Removes and returns the oldest immutable memtable.
    ///
    /// This low-level operation immediately removes the generation from manager
    /// reads and must not be used by failure-retryable persistence code. The
    /// engine instead retains the FIFO entry until durable installation
    /// succeeds. The returned [`Arc`] keeps the removed table allocated.
    pub fn get_immutable_for_flush(&self) -> Option<Arc<MemTable>> {
        let mut immutable = self.immutable.write();
        if immutable.is_empty() {
            None
        } else {
            Some(immutable.remove(0))
        }
    }

    /// Borrow the oldest immutable without removing it from the read view.
    pub(crate) fn peek_immutable_for_flush(&self) -> Option<Arc<MemTable>> {
        self.immutable.read().first().cloned()
    }

    /// Reserve one stable SSTable id for this immutable across retries.
    pub(crate) fn reserved_flush_id(&self, table: &Arc<MemTable>) -> Option<u64> {
        self.flush_ids
            .lock()
            .get(&ImmutableGenerationId::of(table))
            .copied()
    }

    /// Reserve one stable SSTable id for this immutable across retries.
    pub(crate) fn reserve_flush_id(&self, table: &Arc<MemTable>, proposed_id: u64) -> u64 {
        *self
            .flush_ids
            .lock()
            .entry(ImmutableGenerationId::of(table))
            .or_insert(proposed_id)
    }

    /// Remove exactly the immutable generation whose durable install completed.
    pub(crate) fn complete_immutable_flush(&self, table: &Arc<MemTable>) -> bool {
        let mut immutable = self.immutable.write();
        let Some(front) = immutable.first() else {
            return false;
        };
        if !Arc::ptr_eq(front, table) {
            return false;
        }

        let completed = immutable.remove(0);
        self.flush_ids
            .lock()
            .remove(&ImmutableGenerationId::of(&completed));
        true
    }

    /// Lowest sequence not represented by the immutable currently installing.
    pub(crate) fn minimum_live_sequence_excluding(&self, excluded: &Arc<MemTable>) -> Option<u64> {
        self.minimum_live_sequence_except(Some(excluded))
    }

    /// Lowest sequence still retained by any active or immutable generation.
    pub(crate) fn minimum_live_sequence(&self) -> Option<u64> {
        self.minimum_live_sequence_except(None)
    }

    fn minimum_live_sequence_except(&self, excluded: Option<&Arc<MemTable>>) -> Option<u64> {
        let active_min = self.active.read().sequence_bounds().map(|(min, _)| min);
        self.immutable
            .read()
            .iter()
            .filter(|table| excluded.map_or(true, |excluded| !Arc::ptr_eq(table, excluded)))
            .filter_map(|table| table.sequence_bounds().map(|(min, _)| min))
            .chain(active_min)
            .min()
    }

    /// Check if there are immutable memtables waiting to be flushed
    pub fn has_immutable(&self) -> bool {
        !self.immutable.read().is_empty()
    }

    /// Get the number of immutable memtables
    pub fn immutable_count(&self) -> usize {
        self.immutable.read().len()
    }

    /// Get the current sequence number from the active memtable
    pub fn current_sequence(&self) -> u64 {
        self.next_sequence.load(Ordering::Acquire)
    }

    fn allocate_sequence(&self) -> u64 {
        self.next_sequence.fetch_add(1, Ordering::AcqRel)
    }

    /// Reserve a contiguous engine sequence span for one compound mutation.
    pub(crate) fn reserve_sequence_range(&self, count: usize) -> u64 {
        self.next_sequence.fetch_add(count as u64, Ordering::AcqRel)
    }

    fn observe_sequence(&self, sequence: u64) {
        self.next_sequence
            .fetch_max(sequence.saturating_add(1), Ordering::AcqRel);
    }

    fn release_buffered_entries(&self, entries: &[BufferEntry]) {
        let bytes = entries.iter().fold(0_u64, |total, entry| {
            total.saturating_add(
                estimated_memtable_entry_size(&entry.key, Some(&entry.value)) as u64,
            )
        });
        self.buffered_bytes.fetch_sub(bytes, Ordering::Relaxed);
        // The active-table mutations and byte-gauge update happen before a
        // sampler can observe this handoff as complete.
        self.buffered_versions
            .fetch_sub(entries.len() as u64, Ordering::Release);
    }

    fn merge_entries(&self, include: impl Fn(&[u8]) -> bool) -> Vec<(Vec<u8>, MemTableEntry)> {
        use std::collections::BTreeMap;

        // Capture active before immutable for the same rotation-handoff reason
        // as point reads. Arc retention keeps either physical side valid.
        let active = self.active.read().clone();
        let immutable = self.immutable.read().clone();
        let mut merged: BTreeMap<Vec<u8>, (MemTableEntry, u64)> = BTreeMap::new();
        let mut merge_table = |table: &MemTable, generation_rank: u64| {
            for (key, entry) in table.get_all_entries() {
                if include(&key)
                    && merged.get(&key).map_or(true, |(current, current_rank)| {
                        memory_entry_is_newer(&entry, generation_rank, current, *current_rank)
                    })
                {
                    merged.insert(key, (entry, generation_rank));
                }
            }
        };

        for (generation_rank, table) in immutable.iter().enumerate() {
            merge_table(table, generation_rank as u64);
        }
        merge_table(&active, u64::MAX);
        merged
            .into_iter()
            .map(|(key, (entry, _))| (key, entry))
            .collect()
    }

    /// Samples approximate physical statistics for all memtables and buffers.
    ///
    /// The synchronous sample can block briefly on buffer/table locks and is
    /// not a transactional snapshot with concurrent mutations.
    pub fn stats(&self) -> MemTableManagerStats {
        // Ordinary buffered inserts publish only atomics. This shared lock
        // coordinates sampling with the less frequent batched handoff into the
        // active table, never with the per-insert fast path.
        let _handoff = self.buffer_handoff.read();
        let buffered_versions = self.buffered_versions.load(Ordering::Acquire);
        let buffered_bytes = self.buffered_bytes.load(Ordering::Relaxed);
        let active_stats = self.active.read().stats();
        let immutable_stats: Vec<_> = self
            .immutable
            .read()
            .iter()
            .map(|table| table.stats())
            .collect();

        MemTableManagerStats {
            buffered_versions,
            buffered_bytes,
            active: active_stats,
            immutable: immutable_stats,
        }
    }

    /// Freezes a nonempty active table and appends it to the immutable FIFO.
    ///
    /// This is an in-memory visibility transition only; it does not persist or
    /// flush the generation. The synchronous call can block on manager locks.
    pub fn force_rotate(&self) -> Result<()> {
        let mut active_lock = self.active.write();

        if active_lock.is_empty() {
            return Ok(());
        }

        info!("Force rotating MemTable");
        active_lock.set_read_only();
        let old_table = active_lock.clone();
        self.immutable.write().push(old_table);
        *active_lock = Arc::new(MemTable::new(self.config.clone()));

        Ok(())
    }
}

fn memory_entry_is_newer(
    candidate: &MemTableEntry,
    candidate_rank: u64,
    current: &MemTableEntry,
    current_rank: u64,
) -> bool {
    VersionOrder::memory(candidate.sequence, candidate_rank)
        > VersionOrder::memory(current.sequence, current_rank)
}

impl Drop for MemTableManager {
    fn drop(&mut self) {
        // Clean up all buffers belonging to this manager from the registry
        let mut registry = BUFFER_REGISTRY.lock();
        registry.retain(|(_, manager_id), _| *manager_id != self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::{RngCore, SeedableRng};
    use std::collections::BTreeMap;
    use std::time::Duration;

    use crate::storage::test_support::{stress_context, stress_key_value};

    fn test_config() -> MemTableConfig {
        MemTableConfig {
            max_size: 1024 * 1024,
            max_entries: 100,
            max_age: Duration::from_secs(3600),
        }
    }

    #[test]
    fn test_insert_and_get() {
        let manager = MemTableManager::new(test_config());

        manager.insert(b"key1", b"value1").unwrap();
        manager.insert(b"key2", b"value2").unwrap();

        assert_eq!(manager.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(manager.get(b"key2"), Some(b"value2".to_vec()));
        assert_eq!(manager.get(b"key3"), None);
    }

    #[test]
    fn test_delete() {
        let manager = MemTableManager::new(test_config());

        manager.insert(b"key1", b"value1").unwrap();
        assert!(manager.contains_key(b"key1"));

        manager.delete(b"key1").unwrap();
        assert!(!manager.contains_key(b"key1"));
    }

    #[test]
    fn replacement_transitions_do_not_inflate_counts_or_rotate_early() {
        let config = MemTableConfig {
            max_size: b"same".len() + b"new-value".len() + 33,
            max_entries: 2,
            ..test_config()
        };
        let manager = MemTableManager::new(config);

        manager.delete(b"same").unwrap();
        manager.delete(b"same").unwrap();
        manager.insert(b"same", b"value").unwrap();
        manager.insert(b"same", b"new-value").unwrap();
        manager.delete(b"same").unwrap();

        let stats = manager.stats();
        assert_eq!(stats.active.entry_count, 1);
        assert_eq!(stats.active.tombstone_count, 1);
        assert_eq!(stats.active.size_bytes, b"same".len() + 32);
        assert!(stats.immutable.is_empty());

        manager.insert(b"other", b"value").unwrap();
        assert_eq!(manager.stats().active.entry_count, 2);
        assert_eq!(manager.immutable_count(), 0);
        manager.insert(b"rotates", b"value").unwrap();
        assert_eq!(manager.immutable_count(), 1);
    }

    #[test]
    fn buffered_and_active_gauges_use_the_same_entry_size_estimate() {
        let manager = MemTableManager::new(test_config());
        let key = b"buffered-key";
        let value = b"buffered-value";

        manager.insert_buffered(key, value).unwrap();
        let buffered = manager.stats();
        assert_eq!(buffered.buffered_versions, 1);
        assert_eq!(
            buffered.buffered_bytes,
            estimated_memtable_entry_size(key, Some(value)) as u64
        );

        manager.flush_thread_local().unwrap();
        let active = manager.stats();
        assert_eq!(active.buffered_versions, 0);
        assert_eq!(active.buffered_bytes, 0);
        assert_eq!(active.active.size_bytes as u64, buffered.buffered_bytes);
    }

    #[test]
    fn test_rotation() {
        let config = MemTableConfig {
            max_entries: 10,
            ..test_config()
        };
        let manager = MemTableManager::new(config);

        // Insert enough entries to trigger rotation
        for i in 0..15 {
            manager
                .insert(format!("key{}", i).as_bytes(), b"value")
                .unwrap();
        }

        // Should have at least one immutable memtable
        assert!(manager.has_immutable());
    }

    #[test]
    fn test_get_across_memtables() {
        let config = MemTableConfig {
            max_entries: 5,
            ..test_config()
        };
        let manager = MemTableManager::new(config);

        // Insert entries that will span multiple memtables
        for i in 0..12 {
            manager
                .insert(
                    format!("key{}", i).as_bytes(),
                    format!("value{}", i).as_bytes(),
                )
                .unwrap();
        }

        // Should be able to find all entries
        for i in 0..12 {
            assert!(manager.get(format!("key{}", i).as_bytes()).is_some());
        }
    }

    #[test]
    fn raw_and_managed_same_key_races_keep_newest_sequence_and_accounting() {
        const ROUNDS: u64 = 128;
        for managed_as_batch in [false, true] {
            let key: &'static [u8] = if managed_as_batch {
                b"batched"
            } else {
                b"single"
            };
            let manager = Arc::new(MemTableManager::new(test_config()));
            let table = manager.active.read().clone();

            for round in 0..ROUNDS {
                let older_sequence = round * 2;
                let newest_sequence = older_sequence + 1;
                let start = Arc::new(std::sync::Barrier::new(3));
                let managed_writer = {
                    let manager = Arc::clone(&manager);
                    let start = Arc::clone(&start);
                    std::thread::spawn(move || {
                        start.wait();
                        if managed_as_batch {
                            manager
                                .apply_batch_with_sequences(
                                    &[(key, Some(b"older".as_slice()))],
                                    &[older_sequence],
                                )
                                .unwrap();
                        } else {
                            manager
                                .insert_with_sequence(key, b"older", older_sequence)
                                .unwrap();
                        }
                    })
                };
                let raw_writer = {
                    let table = Arc::clone(&table);
                    let start = Arc::clone(&start);
                    std::thread::spawn(move || {
                        start.wait();
                        table
                            .insert_with_sequence(key, b"newest", newest_sequence)
                            .unwrap();
                    })
                };
                start.wait();
                managed_writer.join().unwrap();
                raw_writer.join().unwrap();

                let entry = manager.get_entry(key).unwrap();
                assert_eq!(entry.sequence, newest_sequence);
                assert_eq!(entry.value.as_deref(), Some(b"newest".as_slice()));
                let stats = manager.stats();
                assert_eq!(stats.active.entry_count, 1);
                assert_eq!(stats.active.tombstone_count, 0);
                assert_eq!(
                    stats.active.size_bytes,
                    estimated_memtable_entry_size(key, Some(b"newest"))
                );
            }
        }
    }

    #[test]
    fn retryable_flush_peek_retains_generation_until_exact_completion() {
        let manager = MemTableManager::new(test_config());
        manager.insert(b"key", b"value").unwrap();
        manager.force_rotate().unwrap();

        let first_attempt = manager.peek_immutable_for_flush().unwrap();
        assert_eq!(manager.immutable_count(), 1);
        assert_eq!(manager.reserve_flush_id(&first_attempt, 7), 7);

        let retry = manager.peek_immutable_for_flush().unwrap();
        assert!(Arc::ptr_eq(&first_attempt, &retry));
        assert_eq!(manager.reserved_flush_id(&retry), Some(7));
        assert_eq!(manager.reserve_flush_id(&retry, 99), 7);
        assert!(manager.complete_immutable_flush(&retry));
        assert_eq!(manager.immutable_count(), 0);
        assert_eq!(manager.reserved_flush_id(&retry), None);
        assert!(!manager.complete_immutable_flush(&retry));
    }

    #[test]
    fn sequence_and_tombstone_order_survive_multiple_rotations() {
        let config = MemTableConfig {
            max_entries: 1,
            ..test_config()
        };
        let manager = MemTableManager::new_with_next_sequence(config, 40);

        let value_sequence = manager.insert(b"ordered:key", b"old").unwrap();
        manager.insert(b"rotate:one", b"value").unwrap();
        let tombstone_sequence = manager.delete(b"ordered:key").unwrap();
        manager.insert(b"rotate:two", b"value").unwrap();

        let entry = manager.get_entry(b"ordered:key").unwrap();
        assert_eq!(value_sequence, 40);
        assert!(tombstone_sequence > value_sequence);
        assert_eq!(entry.sequence, tombstone_sequence);
        assert!(entry.is_tombstone());
        assert_eq!(manager.get(b"ordered:key"), None);
        assert!(manager.range_entries(b"ordered:", b"ordered;")[0]
            .1
            .is_tombstone());
    }

    #[test]
    fn delayed_buffered_insert_cannot_overtake_newer_tombstone() {
        let manager = MemTableManager::new(test_config());

        let insert_sequence = manager.insert_buffered(b"buffered:key", b"old").unwrap();
        let delete_sequence = manager.delete(b"buffered:key").unwrap();
        assert!(delete_sequence > insert_sequence);

        manager.flush_thread_local().unwrap();
        let entry = manager.get_entry(b"buffered:key").unwrap();
        assert_eq!(entry.sequence, delete_sequence);
        assert!(entry.is_tombstone());
    }

    fn memtable_case(seed: u64, manager: &MemTableManager, sequence: u64) -> String {
        stress_context(seed, sequence, manager.immutable_count(), "<memtable>")
    }

    fn assert_physical_accounting(seed: u64, manager: &MemTableManager) {
        let mut tables = manager.immutable.read().clone();
        tables.push(manager.active.read().clone());
        for (generation, table) in tables.into_iter().enumerate() {
            let entries = table.get_all_entries();
            let expected_bytes = entries.iter().fold(0, |total, (key, entry)| {
                total + estimated_memtable_entry_size(key, entry.value.as_deref())
            });
            let expected_tombstones = entries
                .iter()
                .filter(|(_, entry)| entry.is_tombstone())
                .count();
            let stats = table.stats();
            let context =
                stress_context(seed, manager.current_sequence(), generation, "<memtable>");
            assert_eq!(stats.entry_count, entries.len(), "{context}");
            assert_eq!(stats.tombstone_count, expected_tombstones, "{context}");
            assert_eq!(stats.size_bytes, expected_bytes, "{context}");
        }
    }

    fn assert_memtable_model(
        seed: u64,
        manager: &MemTableManager,
        expected: &BTreeMap<Vec<u8>, (Option<Vec<u8>>, u64)>,
    ) {
        let context = memtable_case(seed, manager, manager.current_sequence());
        for (key, (value, sequence)) in expected {
            let actual = manager
                .get_entry(key)
                .unwrap_or_else(|| panic!("{context}: missing key {key:?}"));
            assert_eq!(actual.sequence, *sequence, "{context}: key={key:?}");
            assert_eq!(
                actual.value.as_deref(),
                value.as_deref(),
                "{context}: key={key:?}"
            );
        }
        let actual = manager
            .scan_prefix_entries(b"stress:")
            .into_iter()
            .map(|(key, entry)| (key, (entry.value, entry.sequence)))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(&actual, expected, "{context}");
        assert_physical_accounting(seed, manager);
    }

    fn run_seeded_memtable_model(seed: u64) {
        let config = MemTableConfig {
            max_size: 2_048,
            max_entries: 7,
            max_age: Duration::from_secs(3_600),
        };
        let manager = Arc::new(MemTableManager::new(config));
        let mut expected = BTreeMap::new();
        let mut rng = StdRng::seed_from_u64(seed);

        let barrier = Arc::new(std::sync::Barrier::new(9));
        let writers = (0..8)
            .map(|writer| {
                let manager = Arc::clone(&manager);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let key = format!("stress:concurrent:{writer:02}").into_bytes();
                    let value = format!("writer:{writer:02}").into_bytes();
                    barrier.wait();
                    let sequence = manager.insert(&key, &value).unwrap();
                    (key, value, sequence)
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        for writer in writers {
            let (key, value, sequence) = writer.join().unwrap_or_else(|_| {
                panic!(
                    "{}: concurrent writer panicked",
                    memtable_case(seed, &manager, manager.current_sequence())
                )
            });
            expected.insert(key, (Some(value), sequence));
        }

        for step in 0..192_u64 {
            let (key_index, key, value) = stress_key_value(&mut rng, 96);
            let context = memtable_case(seed, &manager, manager.current_sequence());
            match step % 7 {
                0 | 1 => {
                    let sequence = manager
                        .insert(&key, &value)
                        .unwrap_or_else(|error| panic!("{context}: insert failed: {error}"));
                    expected.insert(key, (Some(value), sequence));
                }
                2 => {
                    let sequence = manager
                        .delete(&key)
                        .unwrap_or_else(|error| panic!("{context}: delete failed: {error}"));
                    expected.insert(key, (None, sequence));
                }
                3 => {
                    let second_key =
                        format!("stress:key:{:02}", (key_index + 11) % 24).into_bytes();
                    let second_value = rng.next_u64().to_le_bytes().to_vec();
                    let entries = vec![
                        (key.clone(), value.clone()),
                        (second_key.clone(), second_value.clone()),
                    ];
                    let first_sequence = manager.current_sequence();
                    manager
                        .insert_many(&entries)
                        .unwrap_or_else(|error| panic!("{context}: insert_many failed: {error}"));
                    expected.insert(key, (Some(value), first_sequence));
                    expected.insert(second_key, (Some(second_value), first_sequence + 1));
                }
                4 => manager
                    .force_rotate()
                    .unwrap_or_else(|error| panic!("{context}: force rotation failed: {error}")),
                5 => {
                    if let Some((_, current_sequence)) = expected.get(&key) {
                        if *current_sequence > 0 {
                            manager
                                .insert_with_sequence(&key, b"stale", current_sequence - 1)
                                .unwrap_or_else(|error| {
                                    panic!("{context}: delayed insert failed: {error}")
                                });
                        }
                    }
                }
                _ => assert_memtable_model(seed, &manager, &expected),
            }
            if step % 13 == 0 {
                assert_memtable_model(seed, &manager, &expected);
            }
        }

        let context = memtable_case(seed, &manager, manager.current_sequence());
        manager
            .force_rotate()
            .unwrap_or_else(|error| panic!("{context}: final rotation failed: {error}"));
        let failed_flush_generation = manager
            .peek_immutable_for_flush()
            .unwrap_or_else(|| panic!("{context}: no immutable generation after rotation"));
        let flush_id = manager.reserve_flush_id(&failed_flush_generation, seed);
        let before_failure = manager.scan_prefix_entries(b"stress:");
        let retry_generation = manager
            .peek_immutable_for_flush()
            .unwrap_or_else(|| panic!("{context}: failed generation was not retained"));
        let context = memtable_case(seed, &manager, manager.current_sequence());
        assert!(
            Arc::ptr_eq(&failed_flush_generation, &retry_generation),
            "{context}"
        );
        assert_eq!(
            manager.reserve_flush_id(&retry_generation, seed.wrapping_add(1)),
            flush_id,
            "{context}"
        );
        assert_eq!(
            manager
                .scan_prefix_entries(b"stress:")
                .into_iter()
                .map(|(key, entry)| (key, entry.value, entry.sequence))
                .collect::<Vec<_>>(),
            before_failure
                .into_iter()
                .map(|(key, entry)| (key, entry.value, entry.sequence))
                .collect::<Vec<_>>(),
            "{context}"
        );
        assert_memtable_model(seed, &manager, &expected);
    }

    #[test]
    fn seeded_overwrite_delete_accounting_concurrency_rotation_and_retry_model() {
        for seed in [
            0x0b7e_35a9_c461_82fd,
            0x619c_f024_8ad3_57e1,
            0xd8a4_13f7_6c09_be52,
        ] {
            run_seeded_memtable_model(seed);
        }
    }
}
