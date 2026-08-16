//! # TurboKV Database API
//!
//! Clean, BTreeMap-like interface for the TurboKV storage engine.
//!
//! ## Example Usage
//!
//! ```rust,no_run
//! use turbokv::{Db, DbOptions};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Open with default options
//! let db = Db::open("./my_data").await?;
//!
//! // Insert key-value pairs
//! db.insert(b"hello", b"world").await?;
//!
//! // Get values
//! if let Some(value) = db.get(b"hello").await? {
//!     println!("Got: {:?}", value);
//! }
//!
//! // Delete keys
//! db.remove(b"hello").await?;
//!
//! // Range scan
//! for (key, value) in db.range(b"a", b"z").await? {
//!     println!("{:?} -> {:?}", key, value);
//! }
//! # Ok(())
//! # }
//! ```

use std::path::Path;
use std::sync::Arc;

use crate::core::types::Compression;
use crate::core::{DbConfig, WriteBatch};

use super::engine::{Engine, StorageConfig, StorageError};
use super::sstable::CompressionType;

/// Result type for database operations
pub type Result<T> = std::result::Result<T, DbError>;

/// Database errors
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("Invalid database options: {0}")]
    InvalidOptions(String),

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Database error: {0}")]
    Other(String),
}

/// Configuration options for the database.
///
/// Use [`DbOptions::fast`], [`DbOptions::durable`], or
/// [`DbOptions::paranoid`] to select a supported durability contract. Custom
/// field combinations are validated by [`Db::open_with_options`].
#[derive(Debug, Clone)]
pub struct DbOptions {
    /// Append acknowledged mutations to the write-ahead log.
    pub wal_enabled: bool,
    /// Sync each WAL write before acknowledging it.
    ///
    /// This requires [`Self::wal_enabled`] and is rejected without it.
    pub sync_writes: bool,
    /// MemTable size in bytes before flush (default: 64MB)
    pub memtable_size: usize,
    /// Block cache size in bytes (default: 64MB, 0 to disable)
    pub block_cache_size: usize,
    /// Compression algorithm for SSTables (default: Lz4)
    pub compression: Compression,
}

impl Default for DbOptions {
    fn default() -> Self {
        Self {
            wal_enabled: true,
            sync_writes: false, // Durable mode (periodic sync, not per-write)
            memtable_size: 64 * 1024 * 1024,
            block_cache_size: 64 * 1024 * 1024,
            compression: Compression::Lz4, // Match README
        }
    }
}

impl DbOptions {
    /// In-memory acknowledgement without a write-ahead log.
    ///
    /// A successful mutation is immediately visible to this database handle,
    /// but may be lost if the process exits before [`Db::flush`] or
    /// [`Db::close`] succeeds.
    ///
    /// Best for: caches, temporary data, benchmarks
    pub fn fast() -> Self {
        Self {
            wal_enabled: false,
            sync_writes: false,
            memtable_size: 64 * 1024 * 1024,
            block_cache_size: 64 * 1024 * 1024,
            compression: Compression::Lz4,
        }
    }

    /// Process-crash durability through an unsynced write-ahead log.
    ///
    /// A mutation is appended to the WAL before it becomes visible. The WAL is
    /// not synced for every mutation, so this mode is intended to survive a
    /// TurboKV or process crash but does not promise power-loss durability for
    /// the most recent acknowledgements. [`Db::flush`] and [`Db::close`] sync
    /// pending state.
    ///
    /// Best for: most production workloads
    pub fn durable() -> Self {
        Self {
            wal_enabled: true,
            sync_writes: false,
            memtable_size: 64 * 1024 * 1024,
            block_cache_size: 64 * 1024 * 1024,
            compression: Compression::Lz4,
        }
    }

    /// Per-mutation synced write-ahead logging.
    ///
    /// A mutation is acknowledged only after its WAL write has been synced.
    /// This is the strongest durability mode and is intended for sudden power
    /// loss, subject to the filesystem and storage device honoring sync.
    ///
    /// Best for: financial transactions, critical records
    pub fn paranoid() -> Self {
        Self {
            wal_enabled: true,
            sync_writes: true,
            memtable_size: 64 * 1024 * 1024,
            block_cache_size: 64 * 1024 * 1024,
            compression: Compression::Lz4,
        }
    }

    /// Set compression algorithm
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }
}

/// TurboKV database - a fast, embedded key-value store.
///
/// Provides a BTreeMap-like API for storing and retrieving data.
///
/// # Visibility and ordering
///
/// Range and prefix results are ordered lexicographically by raw key bytes.
/// Operations supplied to one bulk or batch call are applied in iterator order,
/// so the last operation for a duplicate key determines the state observed
/// immediately after success, subject to the limitations below.
///
/// Fast mode currently buffers individual inserts. Mixing buffered inserts
/// with deletes, batches, or bulk inserts does not yet guarantee program order;
/// a later direct mutation can be overtaken when the older insert buffer is
/// drained.
///
/// Point, range, and prefix reads in this version can expose an older SSTable
/// value when a newer tombstone for the same key resides in another storage
/// layer. Applications that require cross-layer deletion consistency must not
/// rely on the affected read until that limitation is removed.
///
/// Batches guarantee ordered application and all operations are visible after
/// a successful return. This version does not provide isolation from
/// concurrent calls or all-or-nothing recovery from a torn WAL tail.
///
/// # Operational errors
///
/// Except for [`DbError::InvalidOptions`], an error can occur after storage
/// side effects. Callers must treat a failed mutation, flush, or batch as having
/// an indeterminate outcome and verify state before a non-idempotent retry.
/// A failed [`Db::close`] consumes the handle and does not promise persistence.
pub struct Db {
    engine: Arc<Engine>,
}

impl Db {
    /// Open a database at the given path with default options
    ///
    /// Creates the directory if it doesn't exist.
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_options(path, DbOptions::default()).await
    }

    /// Open a database with custom options.
    ///
    /// Returns [`DbError::InvalidOptions`] before creating database files when
    /// the requested durability combination is contradictory.
    pub async fn open_with_options<P: AsRef<Path>>(path: P, options: DbOptions) -> Result<Self> {
        if options.sync_writes && !options.wal_enabled {
            return Err(DbError::InvalidOptions(
                "sync_writes requires the write-ahead log to be enabled".to_string(),
            ));
        }

        let db_config = DbConfig {
            wal_enabled: options.wal_enabled,
            sync_writes: options.sync_writes,
            memtable_size: options.memtable_size,
            block_cache_size: options.block_cache_size,
            ..Default::default()
        };

        let mut storage_config =
            StorageConfig::from_db_config(&db_config, path.as_ref().to_path_buf());
        // Convert user-facing Compression to internal CompressionType
        storage_config.sstable_config.compression = match options.compression {
            Compression::None => CompressionType::None,
            Compression::Snappy => CompressionType::Snappy,
            Compression::Zstd => CompressionType::Zstd,
            Compression::Lz4 => CompressionType::Lz4,
        };

        let engine = Engine::open(storage_config).await?;

        Ok(Self {
            engine: Arc::new(engine),
        })
    }

    /// Insert a key-value pair.
    ///
    /// If the key already exists, the value is overwritten. Success means the
    /// mutation is visible through this database and has reached the durability
    /// point selected by [`DbOptions`]. Keys and values are arbitrary bytes;
    /// an empty value is a stored value, not a deletion.
    ///
    /// # Performance
    /// Automatically uses the optimal path based on configuration:
    /// - Fast mode (no WAL): Uses sync path with thread-local buffering
    /// - Durable modes: Uses async path for WAL writes
    pub async fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) -> Result<()> {
        self.engine.insert(key.as_ref(), value.as_ref()).await?;
        Ok(())
    }

    /// Insert multiple key-value pairs in iterator order.
    ///
    /// With WAL enabled, all entries are appended to the WAL before any entry
    /// is made visible in the memtable. If a key occurs more than once, its last
    /// value in the iterator is visible after success.
    pub async fn insert_many<I, K, V>(&self, entries: I) -> Result<()>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let entries: Vec<(Vec<u8>, Vec<u8>)> = entries
            .into_iter()
            .map(|(key, value)| (key.as_ref().to_vec(), value.as_ref().to_vec()))
            .collect();

        self.engine.insert_many(&entries).await?;
        Ok(())
    }

    /// Get the latest acknowledged value for a key.
    ///
    /// Returns `None` if the key is absent or deleted. An existing empty value
    /// is returned as `Some(Vec::new())`.
    pub async fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>> {
        Ok(self.engine.get(key.as_ref()).await?)
    }

    /// Remove a key.
    ///
    /// This is a no-op if the key doesn't exist. Success means the tombstone has
    /// reached the selected durability point. See [`Db`] for the current
    /// cross-layer deletion visibility limitation.
    pub async fn remove<K: AsRef<[u8]>>(&self, key: K) -> Result<()> {
        self.engine.delete(key.as_ref()).await?;
        Ok(())
    }

    /// Check if a key exists
    pub async fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> Result<bool> {
        Ok(self.engine.get(key.as_ref()).await?.is_some())
    }

    /// Scan a byte range in lexicographic key order.
    ///
    /// The start bound is inclusive and the end bound is exclusive.
    pub async fn range<K: AsRef<[u8]>>(&self, start: K, end: K) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(self.engine.range(start.as_ref(), end.as_ref()).await?)
    }

    /// Scan all keys with a given byte prefix in lexicographic order.
    pub async fn scan_prefix<K: AsRef<[u8]>>(&self, prefix: K) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(self.engine.scan_prefix(prefix.as_ref()).await?)
    }

    /// Scan a range of keys with guard iterator for lazy value access.
    ///
    /// Returns an iterator of [`EntryGuard`] that allows inspecting keys
    /// without loading values, enabling efficient filtering.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Count keys without loading values
    /// let count = db.range_iter(b"user:", b"user:\xff").await?.count();
    ///
    /// // Filter by key, only load matching values
    /// for guard in db.range_iter(b"user:", b"user:\xff").await? {
    ///     if guard.key().ends_with(b":active") {
    ///         let value = guard.value();
    ///         // process value
    ///     }
    /// }
    /// ```
    ///
    /// [`EntryGuard`]: super::iter::EntryGuard
    pub async fn range_iter<K: AsRef<[u8]>>(
        &self,
        start: K,
        end: K,
    ) -> Result<super::iter::RangeIter> {
        Ok(self.engine.range_iter(start.as_ref(), end.as_ref()).await?)
    }

    /// Scan keys with a prefix using guard iterator for lazy value access.
    ///
    /// Returns an iterator of [`EntryGuard`] that allows inspecting keys
    /// without loading values, enabling efficient filtering.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Get only keys
    /// let keys = db.scan_prefix_iter(b"user:").await?.keys();
    ///
    /// // Paginate results
    /// let page: Vec<_> = db.scan_prefix_iter(b"user:").await?
    ///     .paginate(offset, limit)
    ///     .map(|g| g.into_pair())
    ///     .collect();
    /// ```
    ///
    /// [`EntryGuard`]: super::iter::EntryGuard
    pub async fn scan_prefix_iter<K: AsRef<[u8]>>(
        &self,
        prefix: K,
    ) -> Result<super::iter::PrefixIter> {
        Ok(self.engine.scan_prefix_iter(prefix.as_ref()).await?)
    }

    /// Apply multiple operations in batch order.
    ///
    /// On success, every operation has been applied. With the WAL enabled, the
    /// full encoded batch is appended before any operation is applied in
    /// memory. If the same key occurs more than once, its last operation wins.
    /// See [`Db`] for the current cross-layer deletion visibility limitation.
    ///
    /// # Current atomicity limitation
    ///
    /// Concurrent calls can currently observe partial in-memory application,
    /// and recovery from a torn WAL batch can apply a valid prefix. Do not rely
    /// on isolation or crash atomicity until those guarantees are implemented.
    pub async fn write_batch(&self, batch: &WriteBatch) -> Result<()> {
        self.engine.write_batch(batch).await?;
        Ok(())
    }

    /// Flush all pending writes to durable storage.
    ///
    /// Success means thread-local write buffers have been drained, current
    /// memtable contents have been installed as SSTables, and the WAL has been
    /// synced. Concurrent writes that begin after the flush starts may require
    /// a later flush.
    pub async fn flush(&self) -> Result<()> {
        // First flush ALL thread-local buffers from ALL threads
        self.engine.flush_write_buffers()?;
        self.engine.flush().await?;
        Ok(())
    }

    /// Close the database cleanly after persisting pending writes.
    ///
    /// This consumes the database handle. Success means buffered writes have
    /// been flushed to the storage engine and its background tasks have been
    /// asked to stop. Dropping a [`Db`] does not provide this clean-close
    /// guarantee; call `close` when pending writes must be persisted.
    pub async fn close(self) -> Result<()> {
        self.engine.flush_write_buffers()?;
        self.engine.shutdown().await?;
        Ok(())
    }

    /// Trigger manual compaction
    pub async fn compact(&self) -> Result<()> {
        self.engine.compact().await?;
        Ok(())
    }

    /// Get database statistics
    pub fn stats(&self) -> DbStats {
        let stats = self.engine.stats();
        DbStats {
            total_keys: stats.total_keys,
            total_bytes: stats.total_bytes,
            wal_size: stats.wal_size,
            sstable_count: stats.sstable_count as u64,
            memtable_size: stats.memtable_size,
            wal_bytes_written: stats.wal_bytes_written,
            sstable_flush_bytes_written: stats.sstable_flush_bytes_written,
            compaction_bytes_read: stats.compaction_bytes_read,
            compaction_bytes_written: stats.compaction_bytes_written,
            immutable_memtables: stats.immutable_memtables,
            l0_sstable_count: stats.l0_sstable_count,
            write_stall_count: stats.write_stall_count,
            write_stall_micros: stats.write_stall_micros,
        }
    }
}

/// Database statistics
#[derive(Debug, Clone)]
pub struct DbStats {
    /// Total number of keys
    pub total_keys: u64,
    /// Total data size in bytes
    pub total_bytes: u64,
    /// WAL size in bytes
    pub wal_size: u64,
    /// Number of SSTable files
    pub sstable_count: u64,
    /// Current memtable size
    pub memtable_size: u64,
    /// Total WAL bytes written by this process
    pub wal_bytes_written: u64,
    /// Total SSTable bytes written by memtable flushes
    pub sstable_flush_bytes_written: u64,
    /// Total bytes read by compaction jobs
    pub compaction_bytes_read: u64,
    /// Total bytes written by compaction jobs
    pub compaction_bytes_written: u64,
    /// Immutable memtables waiting to be flushed
    pub immutable_memtables: u64,
    /// Number of level-0 SSTables
    pub l0_sstable_count: u64,
    /// Number of controlled write stalls
    pub write_stall_count: u64,
    /// Total controlled write stall time in microseconds
    pub write_stall_micros: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_basic_operations() {
        let temp = TempDir::new().unwrap();
        let db = Db::open_with_options(temp.path(), DbOptions::fast())
            .await
            .unwrap();

        // Insert
        db.insert(b"key1", b"value1").await.unwrap();
        db.insert(b"key2", b"value2").await.unwrap();

        // Get
        assert_eq!(db.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
        assert_eq!(db.get(b"key2").await.unwrap(), Some(b"value2".to_vec()));
        assert_eq!(db.get(b"key3").await.unwrap(), None);

        // Contains
        assert!(db.contains_key(b"key1").await.unwrap());
        assert!(!db.contains_key(b"key3").await.unwrap());

        // Remove
        db.remove(b"key1").await.unwrap();
        assert_eq!(db.get(b"key1").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_range_scan() {
        let temp = TempDir::new().unwrap();
        let db = Db::open_with_options(temp.path(), DbOptions::fast())
            .await
            .unwrap();

        db.insert(b"a", b"1").await.unwrap();
        db.insert(b"b", b"2").await.unwrap();
        db.insert(b"c", b"3").await.unwrap();
        db.insert(b"d", b"4").await.unwrap();

        let results = db.range(b"b", b"d").await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], (b"b".to_vec(), b"2".to_vec()));
        assert_eq!(results[1], (b"c".to_vec(), b"3".to_vec()));
    }

    #[tokio::test]
    async fn test_prefix_scan() {
        let temp = TempDir::new().unwrap();
        let db = Db::open_with_options(temp.path(), DbOptions::fast())
            .await
            .unwrap();

        db.insert(b"user:1", b"alice").await.unwrap();
        db.insert(b"user:2", b"bob").await.unwrap();
        db.insert(b"post:1", b"hello").await.unwrap();

        let users = db.scan_prefix(b"user:").await.unwrap();
        assert_eq!(users.len(), 2);
    }

    #[tokio::test]
    async fn test_fast_mode_optimized() {
        let temp = TempDir::new().unwrap();
        let db = Db::open_with_options(temp.path(), DbOptions::fast())
            .await
            .unwrap();

        // Fast mode uses sync path + thread-local buffers with shared registry
        db.insert(b"key1", b"value1").await.unwrap();
        db.insert(b"key2", b"value2").await.unwrap();

        // Flush drains ALL thread-local buffers (from all threads)
        db.flush().await.unwrap();

        // Verify data is visible
        assert_eq!(db.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
        assert_eq!(db.get(b"key2").await.unwrap(), Some(b"value2".to_vec()));
    }

    #[tokio::test]
    async fn test_fast_mode_many_inserts() {
        let temp = TempDir::new().unwrap();
        let db = Db::open_with_options(temp.path(), DbOptions::fast())
            .await
            .unwrap();

        // Insert many keys (will trigger automatic buffer flushes)
        for i in 0..1000 {
            let key = format!("key{:04}", i);
            let value = format!("value{:04}", i);
            db.insert(key.as_bytes(), value.as_bytes()).await.unwrap();
        }

        // Flush to ensure all writes are visible
        db.flush().await.unwrap();

        // Verify all data is visible
        for i in 0..1000 {
            let key = format!("key{:04}", i);
            let expected = format!("value{:04}", i);
            assert_eq!(
                db.get(key.as_bytes()).await.unwrap(),
                Some(expected.into_bytes())
            );
        }
    }

    #[tokio::test]
    async fn test_range_iter_count() {
        let temp = TempDir::new().unwrap();
        let db = Db::open_with_options(temp.path(), DbOptions::fast())
            .await
            .unwrap();

        db.insert(b"a", b"1").await.unwrap();
        db.insert(b"b", b"2").await.unwrap();
        db.insert(b"c", b"3").await.unwrap();
        db.insert(b"d", b"4").await.unwrap();

        // Count without loading values
        let count = db.range_iter(b"a", b"d").await.unwrap().count();
        assert_eq!(count, 3); // a, b, c (exclusive end)
    }

    #[tokio::test]
    async fn test_range_iter_keys_only() {
        let temp = TempDir::new().unwrap();
        let db = Db::open_with_options(temp.path(), DbOptions::fast())
            .await
            .unwrap();

        db.insert(b"user:1", b"alice").await.unwrap();
        db.insert(b"user:2", b"bob").await.unwrap();

        // Get only keys
        let keys = db.scan_prefix_iter(b"user:").await.unwrap().keys();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&b"user:1".to_vec()));
        assert!(keys.contains(&b"user:2".to_vec()));
    }

    #[tokio::test]
    async fn test_range_iter_filter_by_key() {
        let temp = TempDir::new().unwrap();
        let db = Db::open_with_options(temp.path(), DbOptions::fast())
            .await
            .unwrap();

        db.insert(b"user:1:name", b"alice").await.unwrap();
        db.insert(b"user:1:email", b"alice@example.com")
            .await
            .unwrap();
        db.insert(b"user:2:name", b"bob").await.unwrap();
        db.insert(b"user:2:email", b"bob@example.com")
            .await
            .unwrap();

        // Filter by key pattern, only load matching values
        let names: Vec<_> = db
            .scan_prefix_iter(b"user:")
            .await
            .unwrap()
            .filter(|g| g.key().ends_with(b":name"))
            .map(|g| g.into_value())
            .collect();

        assert_eq!(names.len(), 2);
        assert!(names.contains(&b"alice".to_vec()));
        assert!(names.contains(&b"bob".to_vec()));
    }

    #[tokio::test]
    async fn test_range_iter_paginate() {
        let temp = TempDir::new().unwrap();
        let db = Db::open_with_options(temp.path(), DbOptions::fast())
            .await
            .unwrap();

        for i in 0..10 {
            let key = format!("key:{:02}", i);
            let value = format!("value:{:02}", i);
            db.insert(key.as_bytes(), value.as_bytes()).await.unwrap();
        }

        // Paginate: skip 3, take 4
        let page: Vec<_> = db
            .scan_prefix_iter(b"key:")
            .await
            .unwrap()
            .paginate(3, 4)
            .map(|g| String::from_utf8_lossy(g.key()).to_string())
            .collect();

        assert_eq!(page.len(), 4);
        assert_eq!(page[0], "key:03");
        assert_eq!(page[3], "key:06");
    }
}
