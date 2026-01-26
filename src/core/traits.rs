//! # Core Traits
//!
//! Defines the fundamental interfaces for TurboKV storage engine.
//!
//! ## Design Philosophy
//!
//! 1. **BTreeMap-like API**: Familiar interface for Rust developers
//! 2. **Sync-First**: Sync API for simplicity (async wrapper available)
//! 3. **Zero-Copy**: Keys and values are borrowed where possible
//! 4. **Error Propagation**: All operations return Result

use super::types::{CompactionResult, StorageStats, WriteBatch};
use super::Result;

/// Storage engine trait - BTreeMap-like interface
///
/// This trait defines the core operations for a key-value store.
/// All keys and values are raw bytes (`&[u8]`), giving callers
/// full control over serialization.
///
/// # Example
/// ```ignore
/// let db = Db::open("./mydb", DbConfig::default())?;
///
/// // Insert
/// db.insert(b"key1", b"value1")?;
///
/// // Get
/// if let Some(value) = db.get(b"key1")? {
///     println!("Got: {:?}", value);
/// }
///
/// // Range scan
/// for (k, v) in db.range(b"a"..b"z")? {
///     println!("{:?} => {:?}", k, v);
/// }
/// ```
pub trait StorageEngine: Send + Sync {
    /// Get value by key.
    ///
    /// Returns `Ok(Some(value))` if the key exists,
    /// `Ok(None)` if the key does not exist,
    /// or an error if the operation fails.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Insert or update a key-value pair.
    ///
    /// If the key already exists, its value is overwritten.
    fn insert(&self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Delete a key.
    ///
    /// This is a no-op if the key does not exist.
    /// The deletion is recorded as a tombstone and the space
    /// is reclaimed during compaction.
    fn delete(&self, key: &[u8]) -> Result<()>;

    /// Check if a key exists.
    ///
    /// This is more efficient than `get()` when you don't need the value.
    fn contains_key(&self, key: &[u8]) -> Result<bool>;

    /// Range scan from start (inclusive) to end (exclusive).
    ///
    /// Returns all key-value pairs where `start <= key < end`.
    /// Results are sorted by key in ascending order.
    fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Scan all keys with a given prefix.
    ///
    /// Returns all key-value pairs where `key.starts_with(prefix)`.
    /// Results are sorted by key in ascending order.
    fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Apply a batch of operations atomically.
    ///
    /// Either all operations succeed or none do.
    /// This is more efficient than individual operations when
    /// making multiple changes.
    fn write_batch(&self, batch: &WriteBatch) -> Result<()>;

    /// Force flush all pending writes to disk.
    ///
    /// This ensures data durability. Note that if `sync_writes`
    /// is enabled in config, data is already synced on each write.
    fn flush(&self) -> Result<()>;

    /// Trigger manual compaction.
    ///
    /// Compaction merges SSTable files and reclaims space from
    /// deleted keys. This is normally done automatically in the
    /// background, but can be triggered manually if needed.
    fn compact(&self) -> Result<CompactionResult>;

    /// Get storage statistics.
    fn stats(&self) -> Result<StorageStats>;
}

/// Iterator over a range of key-value pairs.
///
/// This is returned by range queries and provides efficient
/// iteration without loading all results into memory.
pub trait KvIterator: Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> {
    /// Get the current key without advancing.
    fn peek_key(&self) -> Option<&[u8]>;

    /// Skip to a specific key.
    fn seek(&mut self, key: &[u8]) -> Result<()>;

    /// Check if the iterator is valid (not exhausted).
    fn valid(&self) -> bool;
}
