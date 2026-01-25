//! # MemTable Manager
//!
//! Manages active and immutable memtables for the LSM-tree.
//!
//! The manager handles:
//! - Active memtable for current writes
//! - Immutable memtables awaiting flush to SSTables
//! - Automatic rotation when memtable is full

use std::sync::Arc;
use parking_lot::RwLock;
use tracing::info;

use super::table::{MemTable, MemTableError, Result};
use super::types::{MemTableConfig, MemTableEntry, MemTableManagerStats};

/// Manages active and immutable memtables
pub struct MemTableManager {
    /// Currently active (writable) memtable
    pub active: Arc<RwLock<Arc<MemTable>>>,
    /// Immutable memtables awaiting flush
    pub immutable: Arc<RwLock<Vec<Arc<MemTable>>>>,
    /// Configuration for new memtables
    config: MemTableConfig,
}

impl MemTableManager {
    /// Create a new MemTableManager
    pub fn new(config: MemTableConfig) -> Self {
        let active = Arc::new(MemTable::new(config.clone()));

        Self {
            active: Arc::new(RwLock::new(active)),
            immutable: Arc::new(RwLock::new(Vec::new())),
            config,
        }
    }

    /// Insert a key-value pair
    ///
    /// Automatically rotates the memtable if full.
    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<u64> {
        for _ in 0..5 {
            let active = self.active.read();
            match active.insert(key, value) {
                Ok(seq) => return Ok(seq),
                Err(MemTableError::Full) => {
                    drop(active);
                    self.rotate_memtable()?;
                }
                Err(e) => return Err(e),
            }
        }
        Err(MemTableError::Full)
    }

    /// Delete a key
    ///
    /// Automatically rotates the memtable if full.
    pub fn delete(&self, key: &[u8]) -> Result<u64> {
        for _ in 0..5 {
            let active = self.active.read();
            match active.delete(key) {
                Ok(seq) => return Ok(seq),
                Err(MemTableError::Full) => {
                    drop(active);
                    self.rotate_memtable()?;
                }
                Err(e) => return Err(e),
            }
        }
        Err(MemTableError::Full)
    }

    /// Get a value by key
    ///
    /// Searches active memtable first, then immutable memtables.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        // Check active memtable first
        if let Some(value) = self.active.read().get(key) {
            return Some(value);
        }

        // Check immutable memtables (most recent first)
        for table in self.immutable.read().iter().rev() {
            if let Some(value) = table.get(key) {
                return Some(value);
            }
            // Check if it's a tombstone (deleted)
            if let Some(entry) = table.get_entry(key) {
                if entry.is_tombstone() {
                    return None; // Key was deleted
                }
            }
        }

        None
    }

    /// Check if a key exists
    pub fn contains_key(&self, key: &[u8]) -> bool {
        // Check active memtable first
        if self.active.read().contains_key(key) {
            return true;
        }

        // Check immutable memtables
        for table in self.immutable.read().iter().rev() {
            if table.contains_key(key) {
                return true;
            }
            // Check if it's a tombstone
            if let Some(entry) = table.get_entry(key) {
                if entry.is_tombstone() {
                    return false;
                }
            }
        }

        false
    }

    /// Get the raw entry (including tombstones) for compaction
    pub fn get_entry(&self, key: &[u8]) -> Option<MemTableEntry> {
        // Check active memtable first
        if let Some(entry) = self.active.read().get_entry(key) {
            return Some(entry);
        }

        // Check immutable memtables (most recent first)
        for table in self.immutable.read().iter().rev() {
            if let Some(entry) = table.get_entry(key) {
                return Some(entry);
            }
        }

        None
    }

    /// Scan a range of keys across all memtables
    pub fn range(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        use std::collections::BTreeMap;

        // Merge results from all memtables
        // Later entries override earlier ones
        let mut merged: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();

        // Add from immutable tables (oldest first)
        for table in self.immutable.read().iter() {
            for (key, entry) in table.get_all_entries() {
                if key >= start.to_vec() && key < end.to_vec() {
                    merged.insert(key, entry.value);
                }
            }
        }

        // Add from active table (newest, overrides all)
        for (key, entry) in self.active.read().get_all_entries() {
            if key >= start.to_vec() && key < end.to_vec() {
                merged.insert(key, entry.value);
            }
        }

        // Filter out tombstones and collect
        merged
            .into_iter()
            .filter_map(|(k, v)| v.map(|val| (k, val)))
            .collect()
    }

    /// Scan all keys with a given prefix across all memtables
    pub fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        use std::collections::BTreeMap;

        let mut merged: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();

        // Add from immutable tables (oldest first)
        for table in self.immutable.read().iter() {
            for (key, entry) in table.get_all_entries() {
                if key.starts_with(prefix) {
                    merged.insert(key, entry.value);
                }
            }
        }

        // Add from active table (newest)
        for (key, entry) in self.active.read().get_all_entries() {
            if key.starts_with(prefix) {
                merged.insert(key, entry.value);
            }
        }

        // Filter out tombstones
        merged
            .into_iter()
            .filter_map(|(k, v)| v.map(|val| (k, val)))
            .collect()
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

    /// Get an immutable memtable for flushing to SSTable
    ///
    /// Returns the oldest immutable memtable (FIFO order).
    pub fn get_immutable_for_flush(&self) -> Option<Arc<MemTable>> {
        let mut immutable = self.immutable.write();
        if immutable.is_empty() {
            None
        } else {
            Some(immutable.remove(0))
        }
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
        self.active.read().current_sequence()
    }

    /// Get statistics for all memtables
    pub fn stats(&self) -> MemTableManagerStats {
        let active_stats = self.active.read().stats();
        let immutable_stats: Vec<_> = self
            .immutable
            .read()
            .iter()
            .map(|table| table.stats())
            .collect();

        MemTableManagerStats {
            active: active_stats,
            immutable: immutable_stats,
        }
    }

    /// Force rotation (for testing or manual flush)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

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
    fn test_rotation() {
        let config = MemTableConfig {
            max_entries: 10,
            ..test_config()
        };
        let manager = MemTableManager::new(config);

        // Insert enough entries to trigger rotation
        for i in 0..15 {
            manager.insert(format!("key{}", i).as_bytes(), b"value").unwrap();
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
            manager.insert(format!("key{}", i).as_bytes(), format!("value{}", i).as_bytes()).unwrap();
        }

        // Should be able to find all entries
        for i in 0..12 {
            assert!(manager.get(format!("key{}", i).as_bytes()).is_some());
        }
    }
}
