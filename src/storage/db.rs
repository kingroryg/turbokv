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
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Database error: {0}")]
    Other(String),
}

/// Configuration options for the database
#[derive(Debug, Clone)]
pub struct DbOptions {
    /// Enable WAL for durability (default: true)
    pub wal_enabled: bool,
    /// Sync writes immediately (default: false for durable mode)
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
            sync_writes: false,  // Durable mode (periodic sync, not per-write)
            memtable_size: 64 * 1024 * 1024,
            block_cache_size: 64 * 1024 * 1024,
            compression: Compression::Lz4,  // Match README
        }
    }
}

impl DbOptions {
    /// Fast configuration - no WAL, no sync
    ///
    /// **Durability:** None - data may be lost on process crash
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

    /// Durable configuration - WAL enabled, no sync per write
    ///
    /// **Durability:** Survives process crashes (data in WAL is recovered)
    ///
    /// The WAL is written but not immediately synced to disk. The OS
    /// will flush buffers periodically. This provides good durability
    /// for most use cases with much better performance than paranoid mode.
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

    /// Paranoid configuration - WAL enabled, sync on every write
    ///
    /// **Durability:** Survives power loss (each write is fsync'd)
    ///
    /// Every write is immediately flushed to disk with fsync(). This
    /// guarantees no data loss even on sudden power failure, but is
    /// significantly slower than durable mode.
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

/// TurboKV database - a fast, embedded key-value store
///
/// Provides a BTreeMap-like API for storing and retrieving data.
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

    /// Open a database with custom options
    pub async fn open_with_options<P: AsRef<Path>>(path: P, options: DbOptions) -> Result<Self> {
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

    /// Insert a key-value pair
    ///
    /// If the key already exists, the value is overwritten.
    ///
    /// # Performance
    /// Automatically uses the optimal path based on configuration:
    /// - Fast mode (no WAL): Uses sync path with thread-local buffering
    /// - Durable modes: Uses async path for WAL writes
    pub async fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) -> Result<()> {
        self.engine.insert(key.as_ref(), value.as_ref()).await?;
        Ok(())
    }

    /// Get a value by key
    ///
    /// Returns `None` if the key doesn't exist.
    pub async fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>> {
        Ok(self.engine.get(key.as_ref()).await?)
    }

    /// Remove a key
    ///
    /// This is a no-op if the key doesn't exist.
    pub async fn remove<K: AsRef<[u8]>>(&self, key: K) -> Result<()> {
        self.engine.delete(key.as_ref()).await?;
        Ok(())
    }

    /// Check if a key exists
    pub async fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> Result<bool> {
        Ok(self.engine.get(key.as_ref()).await?.is_some())
    }

    /// Scan a range of keys (inclusive start, exclusive end)
    pub async fn range<K: AsRef<[u8]>>(&self, start: K, end: K) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(self.engine.range(start.as_ref(), end.as_ref()).await?)
    }

    /// Scan all keys with a given prefix
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

    /// Write multiple operations atomically
    pub async fn write_batch(&self, batch: &WriteBatch) -> Result<()> {
        self.engine.write_batch(batch).await?;
        Ok(())
    }

    /// Flush all pending writes to disk
    pub async fn flush(&self) -> Result<()> {
        // First flush ALL thread-local buffers from ALL threads
        self.engine.flush_write_buffers()?;
        self.engine.flush().await?;
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
