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

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::core::types::Compression;
use crate::core::{
    CompactionResult, DatabaseStatus, DbConfig, LogicalStats, PhysicalStats, WriteBatch,
};

use super::directory_lock::LOCKED_DIRECTORY_GUIDANCE;
use super::engine::{Engine, MaintenanceShutdownError, StorageConfig, StorageError};
use super::sstable::CompressionType;

/// Result type for database operations
pub type Result<T> = std::result::Result<T, DbError>;

/// Database errors
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("Invalid database options: {0}")]
    InvalidOptions(String),

    #[error(
        "database directory is already open for exclusive access: {}; {guidance}",
        path.display(),
        guidance = LOCKED_DIRECTORY_GUIDANCE
    )]
    DirectoryLocked { path: PathBuf },

    #[error("Storage error: {0}")]
    Storage(#[source] StorageError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Database error: {0}")]
    Other(String),
}

impl From<StorageError> for DbError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::DirectoryLocked { path } => Self::DirectoryLocked { path },
            error => Self::Storage(error),
        }
    }
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

    /// Per-mutation durable write-ahead logging.
    ///
    /// Concurrent mutations may share one ordered WAL write and sync barrier;
    /// each is acknowledged only after the group containing it is durable. This
    /// is the strongest durability mode and is intended for sudden power loss,
    /// subject to the filesystem and storage device honoring sync.
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
/// immediately after success.
///
/// Every mutation is assigned one engine-wide sequence number. Point, range,
/// and prefix reads resolve all currently visible in-memory and persisted
/// copies by that sequence before filtering tombstones. Completed flush,
/// reopen, and compaction operations preserve that ordering. A frozen
/// immutable generation remains in the live read view until its durable
/// SSTable and manifest installation are reflected there.
///
/// A write batch reserves one contiguous sequence span and is published as one
/// visibility transition. Concurrent point and scan readers observe either the
/// state before it or the complete batch. WAL recovery applies a batch only
/// after validating its complete checksummed envelope.
///
/// Creating a range or prefix scan freezes a nonempty active memtable so the
/// iterator can retain a bounded-memory, point-in-time view. Frequent scans
/// interleaved with small write bursts can therefore create more immutable
/// generations and increase later flush/compaction write amplification.
///
/// # Operational errors
///
/// Except for [`DbError::InvalidOptions`], an error can occur after storage
/// side effects. A flush failure cannot remove an uninstalled frozen
/// generation; any such generation remains live for retry while the database
/// is open. Callers must treat a failed single or bulk mutation as having an
/// indeterminate outcome and verify state before a non-idempotent retry. A
/// failed batch is not partially published, but its complete WAL envelope may
/// be recovered after reopen if failure occurred after the durable append. A
/// paranoid group-commit error poisons further writes on that open handle;
/// reopen to repair any partial tail and determine whether complete records
/// from the outcome-indeterminate failed group are recoverable. A
/// failed [`Db::close`] consumes the handle and does not promise persistence.
/// Background maintenance failures are retained in [`Db::status`]; clean close
/// fails while any compaction failure remains unresolved. Its final flush can
/// resolve an earlier flush failure when the retained FIFO and registered WAL
/// post-work both complete successfully.
///
/// # Exclusive directory ownership
///
/// Each database owns an exclusive advisory lock on its canonicalized data
/// directory. Opening the same directory through another [`Db`] or [`Engine`],
/// in this process or another process, returns [`DbError::DirectoryLocked`].
/// Shared multi-writer access is unsupported. The lock is retained while this
/// database and any of its active background mutations exist.
pub struct Db {
    engine: Arc<Engine>,
}

impl Db {
    /// Open a database at the given path with default options
    ///
    /// Creates the directory if it doesn't exist and then acquires exclusive
    /// ownership before opening mutable database state.
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_options(path, DbOptions::default()).await
    }

    /// Open a database with custom options.
    ///
    /// Returns [`DbError::InvalidOptions`] before creating database files when
    /// the requested durability combination is contradictory. Returns
    /// [`DbError::DirectoryLocked`] if another database owns the directory.
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
    /// reached the selected durability point.
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
    /// Iterator construction reports [`DbError`]; failures discovered while
    /// advancing it use the purpose-specific [`ScanError`]. Inspecting a key
    /// does not copy its value; each SSTable block is decompressed as a unit
    /// because the on-disk format interleaves keys and values.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Count keys without loading values
    /// let count = db.range_iter(b"user:", b"user:\xff").await?.count()?;
    ///
    /// // Filter by key, only load matching values
    /// for guard in db.range_iter(b"user:", b"user:\xff").await? {
    ///     let guard = guard?;
    ///     if guard.key().ends_with(b":active") {
    ///         let value = guard.value();
    ///         // process value
    ///     }
    /// }
    /// ```
    ///
    /// [`EntryGuard`]: super::iter::EntryGuard
    /// [`ScanError`]: super::iter::ScanError
    pub async fn range_iter<K: AsRef<[u8]>>(
        &self,
        start: K,
        end: K,
    ) -> Result<super::iter::RangeIter> {
        Ok(self.engine.range_iter(start.as_ref(), end.as_ref()).await?)
    }

    /// Scan keys with a prefix using guard iterator for lazy value access.
    ///
    /// Iterator construction reports [`DbError`]; failures discovered while
    /// advancing it use the purpose-specific [`ScanError`].
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Get only keys
    /// let keys = db.scan_prefix_iter(b"user:").await?.keys()?;
    ///
    /// // Paginate results
    /// let page: Vec<_> = db.scan_prefix_iter(b"user:").await?
    ///     .paginate(offset, limit)
    ///     .map(|g| g.map(|g| g.into_pair()))
    ///     .collect::<Result<Vec<_>, _>>()?;
    /// ```
    ///
    /// [`EntryGuard`]: super::iter::EntryGuard
    /// [`ScanError`]: super::iter::ScanError
    pub async fn scan_prefix_iter<K: AsRef<[u8]>>(
        &self,
        prefix: K,
    ) -> Result<super::iter::PrefixIter> {
        Ok(self.engine.scan_prefix_iter(prefix.as_ref()).await?)
    }

    /// Apply multiple operations in batch order.
    ///
    /// On success, every operation has been applied. With the WAL enabled, the
    /// full batch is one checksummed WAL record. Point, range, and prefix reads
    /// observe either the state before publication or the complete batch. A
    /// torn record is discarded during tail repair, so recovery never applies
    /// only a valid prefix. If the same key occurs more than once, its last
    /// operation wins.
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
        self.engine.flush().await?;
        Ok(())
    }

    /// Close the database cleanly after persisting pending writes.
    ///
    /// This consumes the database handle. Success means buffered writes have
    /// been flushed, background tasks have stopped, and the directory lock has
    /// been released as this handle is dropped. Dropping a [`Db`] does not
    /// provide the persistence guarantee; call `close` when pending writes must
    /// be persisted. Close returns an error when its final flush fails or a
    /// background maintenance failure remains unresolved. Use
    /// [`Self::close_with_status`] to inspect unresolved maintenance as a
    /// structured snapshot.
    pub async fn close(self) -> Result<()> {
        self.engine.shutdown().await?;
        Ok(())
    }

    /// Close cleanly and return a structured unresolved-health snapshot.
    ///
    /// This is the production monitoring form of [`Self::close`]. It consumes
    /// the database exactly like the legacy method, while distinguishing
    /// ordinary storage errors from unresolved flush or compaction work.
    pub async fn close_with_status(self) -> std::result::Result<(), MaintenanceShutdownError> {
        self.engine.shutdown_with_status().await
    }

    /// Compact every eligible SSTable in the scope captured after acquiring
    /// the shared compaction coordinator.
    ///
    /// Concurrent flushes are preserved and start outside that fixed scope,
    /// though overlap closure can pull one into a scoped job for safety. The
    /// returned result reports all actual I/O performed by the drain and sets
    /// [`CompactionResult::work_remaining`] when another job is globally
    /// selectable in the drain's final live-state observation. Like any
    /// concurrent status sample, a later flush can immediately make it stale.
    /// Obsolete input cleanup can be deferred after replacement publication;
    /// that retry remains visible through [`Self::status`] even though the
    /// logical compaction result is successful.
    pub async fn compact(&self) -> Result<CompactionResult> {
        Ok(self.engine.compact().await?)
    }

    /// Get cheap maintenance health and write-backpressure status.
    ///
    /// A failed flush, compaction, obsolete-file cleanup, or paranoid WAL
    /// barrier remains visible until a retry proves that lane's work is
    /// resolved. A poisoned WAL requires reopen. Only one bounded failure
    /// detail is kept per lane; cumulative counters retain the historical
    /// signal. Current pressure values are monitoring samples rather than a
    /// transactional snapshot with storage statistics.
    pub fn status(&self) -> DatabaseStatus {
        self.engine.status()
    }

    /// Get legacy mixed physical statistics.
    ///
    /// This compatibility API performs no scan. `total_keys` is a physical
    /// version count and `total_bytes` mixes approximate memtable bytes with
    /// SSTable file bytes; neither field describes logical live data. Use
    /// [`Self::logical_stats`] and [`Self::physical_stats`] for unambiguous
    /// statistics.
    #[deprecated(note = "use logical_stats() and physical_stats()")]
    pub fn stats(&self) -> DbStats {
        let stats = self.engine.legacy_stats();
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
            compactions_in_progress: stats.compactions_in_progress,
            immutable_memtables: stats.immutable_memtables,
            l0_sstable_count: stats.l0_sstable_count,
            write_stall_count: stats.write_stall_count,
            write_stall_micros: stats.write_stall_micros,
        }
    }

    /// Scan one coherent snapshot for exact logical live-data statistics.
    ///
    /// This fallible async operation is O(physical versions), reads SSTable
    /// blocks, and freezes a nonempty active memtable while taking the
    /// snapshot. That freeze can increase bytes written by later flushes and
    /// compactions. Tombstones and superseded versions never inflate the
    /// returned key or byte counts.
    pub async fn logical_stats(&self) -> Result<LogicalStats> {
        Ok(self.engine.logical_stats().await?)
    }

    /// Get cheap physical gauges and process-lifetime cumulative counters.
    ///
    /// This performs no logical-data scan. Current component gauges are
    /// monitoring samples rather than a transactional cross-component
    /// snapshot. Every field ending in `_since_open` resets after reopen.
    pub fn physical_stats(&self) -> PhysicalStats {
        self.engine.physical_stats()
    }
}

/// Legacy mixed physical database statistics.
///
/// Kept for source compatibility with [`Db::stats`]. New code should use
/// [`LogicalStats`] and [`PhysicalStats`].
#[derive(Debug, Clone)]
pub struct DbStats {
    /// Physical versions across memtables and SSTables, not logical live keys.
    pub total_keys: u64,
    /// Approximate memtable bytes plus SSTable file bytes.
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
    /// Compaction selections or jobs currently in progress
    pub compactions_in_progress: u64,
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
    async fn public_status_starts_healthy_with_cause_thresholds_visible() {
        let temp = TempDir::new().unwrap();
        let db = Db::open_with_options(temp.path(), DbOptions::fast())
            .await
            .unwrap();

        let status = db.status();
        assert!(status.maintenance.is_healthy());
        assert_eq!(status.write_backpressure.stalls_since_open, 0);
        assert_eq!(status.write_backpressure.immutable_memtables.threshold, 8);
        assert_eq!(status.write_backpressure.level_zero_files.threshold, 24);
        db.close().await.unwrap();
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
        let count = db.range_iter(b"a", b"d").await.unwrap().count().unwrap();
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
        let keys = db.scan_prefix_iter(b"user:").await.unwrap().keys().unwrap();
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
            .map(|entry| entry.unwrap())
            .filter(|guard| guard.key().ends_with(b":name"))
            .map(super::super::iter::EntryGuard::into_value)
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
            .map(|g| String::from_utf8_lossy(g.unwrap().key()).to_string())
            .collect();

        assert_eq!(page.len(), 4);
        assert_eq!(page[0], "key:03");
        assert_eq!(page[3], "key:06");
    }
}
