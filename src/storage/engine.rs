//! # Storage Engine for TurboKV
//!
//! LSM-tree based storage engine with:
//! - Write-ahead logging for durability
//! - In-memory buffering with concurrent skip list
//! - Sorted string tables with block-based compression
//! - Background compaction
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    Storage Engine                           │
//! ├─────────────────────────────────────────────────────────────┤
//! │                                                             │
//! │  Write Path:                                                │
//! │  ┌─────────┐    ┌─────────┐    ┌──────────┐                 │
//! │  │   KV    │───>│   WAL   │───>│ MemTable │                 │
//! │  └─────────┘    └─────────┘    └────┬─────┘                 │
//! │                                     │ Flush                 │
//! │                                     ▼                       │
//! │                                ┌──────────┐                 │
//! │                                │ SSTable  │                 │
//! │                                └──────────┘                 │
//! │                                                             │
//! │  Read Path:                                                 │
//! │  ┌─────────┐    ┌──────────┐    ┌──────────┐                │
//! │  │  Query  │───>│ MemTable │───>│ SSTables │                │
//! │  └─────────┘    └──────────┘    └──────────┘                │
//! │                                                             │
//! └─────────────────────────────────────────────────────────────┘
//! ```

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::{Mutex as AsyncMutex, RwLock};
use tokio::time::{interval, MissedTickBehavior};
use tracing::{error, info};

use crate::core::{CompactionResult, DbConfig, Error as CoreError, StorageStats, WriteBatch};

use super::{
    compaction::{CompactionConfig, Compactor},
    directory_lock::{
        AcquireError as DirectoryLockAcquireError, DirectoryLock, LOCKED_DIRECTORY_GUIDANCE,
    },
    fd::{FdConfig, FdMonitor, SSTablePool},
    iter::{RangeIter, ScanBounds, ScanSstable},
    manifest::{atomic_replace, sync_directory, Manifest, SSTableManifestEntry},
    memtable::{MemTableConfig, MemTableManager},
    sstable::{SSTableConfig, SSTableInfo, SSTableReader, SSTableWriter},
    version::VersionOrder,
    wal::{WalConfig, WriteAheadLog},
    InProgressGuard,
};

/// Result type for storage operations
pub type Result<T> = std::result::Result<T, StorageError>;

/// Storage engine error types
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("WAL error: {0}")]
    Wal(#[from] super::wal::WalError),

    #[error("MemTable error: {0}")]
    MemTable(#[from] super::memtable::MemTableError),

    #[error("SSTable error: {0}")]
    SSTable(String),

    #[error("Manifest error: {0}")]
    Manifest(String),

    #[error("Compaction error: {0}")]
    Compaction(String),

    #[error(
        "database directory is already open for exclusive access: {}; {guidance}",
        path.display(),
        guidance = LOCKED_DIRECTORY_GUIDANCE
    )]
    DirectoryLocked { path: PathBuf },

    #[error("failed to acquire the database-directory lock at {}: {source}", path.display())]
    DirectoryLockIo {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Storage error: {0}")]
    Other(String),
}

/// Configuration for the storage engine
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Directory for data files
    pub data_dir: PathBuf,
    /// WAL configuration
    pub wal_config: WalConfig,
    /// MemTable configuration
    pub memtable_config: MemTableConfig,
    /// SSTable configuration
    pub sstable_config: SSTableConfig,
    /// Compaction configuration
    pub compaction_config: CompactionConfig,
    /// File descriptor management
    pub fd_config: FdConfig,
    /// How often to check for flush
    pub flush_interval: Duration,
    /// How often to check for compaction
    pub compaction_interval: Duration,
    /// Block cache size in bytes (0 = disabled)
    pub block_cache_size: usize,
    /// Enable WAL for durability
    pub wal_enabled: bool,
    /// Immutable memtable count that triggers controlled write stalls
    pub max_immutable_memtables_before_stall: usize,
    /// L0 SSTable count that triggers controlled write stalls
    pub max_l0_files_before_stall: u64,
    /// Controlled stall duration when thresholds are exceeded
    pub write_stall_micros: u64,
    #[cfg(test)]
    pub(crate) background_tasks_enabled: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            wal_config: WalConfig::default(),
            memtable_config: MemTableConfig::default(),
            sstable_config: SSTableConfig::default(),
            compaction_config: CompactionConfig::default(),
            fd_config: FdConfig::default(),
            flush_interval: Duration::from_secs(60),
            compaction_interval: Duration::from_secs(30),
            block_cache_size: 64 * 1024 * 1024, // 64MB
            wal_enabled: true,
            max_immutable_memtables_before_stall: 8,
            max_l0_files_before_stall: 24,
            write_stall_micros: 250,
            #[cfg(test)]
            background_tasks_enabled: true,
        }
    }
}

impl StorageConfig {
    /// Create config from DbConfig
    pub fn from_db_config(db_config: &DbConfig, data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            wal_config: WalConfig {
                sync_on_write: db_config.sync_writes,
                ..Default::default()
            },
            memtable_config: MemTableConfig {
                max_size: db_config.memtable_size,
                ..Default::default()
            },
            wal_enabled: db_config.wal_enabled,
            block_cache_size: db_config.block_cache_size,
            ..Default::default()
        }
    }

    /// Fast configuration (less durability)
    pub fn fast(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            wal_config: WalConfig::fast(),
            wal_enabled: false,
            ..Default::default()
        }
    }

    /// Durable configuration - WAL enabled, no sync per write
    pub fn durable(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            wal_config: WalConfig::durable(),
            wal_enabled: true,
            ..Default::default()
        }
    }

    /// Paranoid configuration - WAL + sync on every write
    pub fn paranoid(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            wal_config: WalConfig::paranoid(),
            wal_enabled: true,
            ..Default::default()
        }
    }
}

/// Main storage engine
pub struct Engine {
    config: StorageConfig,
    directory_lock: Arc<DirectoryLock>,
    wal: Option<Arc<WriteAheadLog>>,
    memtable_manager: Arc<MemTableManager>,
    sstables: Arc<RwLock<Vec<SSTableInfo>>>,
    manifest: Arc<Mutex<Manifest>>,
    /// Serializes foreground and background ownership of the immutable FIFO.
    flush_lock: Arc<AsyncMutex<()>>,
    /// Prevents checkpoint installation from racing sequence allocation/application.
    mutation_barrier: Arc<RwLock<()>>,
    /// Preserves sequence/publication order between concurrent atomic batches.
    batch_serialization: Arc<AsyncMutex<()>>,
    /// Publishes atomic batches as one visibility transition to concurrent readers.
    batch_visibility: Arc<RwLock<()>>,
    /// Conservative floors for WAL applications that did not finish. Floors
    /// remain until reopen, when WAL replay applies them before this set starts
    /// empty again.
    unapplied_wal_sequences: Arc<Mutex<BTreeSet<u64>>>,
    shutdown: tokio::sync::watch::Sender<bool>,
    background_tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    shutdown_lock: AsyncMutex<()>,
    next_sstable_id: Arc<std::sync::atomic::AtomicU64>,
    sstable_pool: Arc<SSTablePool>,
    #[allow(dead_code)]
    fd_monitor: Arc<FdMonitor>,
    compactor: Arc<Compactor>,
    // Atomic counters for SSTable stats (updated on flush/compaction)
    sstable_total_keys: Arc<AtomicU64>,
    sstable_total_bytes: Arc<AtomicU64>,
    sstable_count: Arc<AtomicU64>,
    l0_sstable_count: Arc<AtomicU64>,
    wal_bytes_written: Arc<AtomicU64>,
    sstable_flush_bytes_written: Arc<AtomicU64>,
    compaction_bytes_read: Arc<AtomicU64>,
    compaction_bytes_written: Arc<AtomicU64>,
    compactions_in_progress: Arc<AtomicU64>,
    write_stall_count: Arc<AtomicU64>,
    write_stall_micros: Arc<AtomicU64>,
}

impl Engine {
    /// Open or create a storage engine
    pub async fn open(mut config: StorageConfig) -> Result<Self> {
        // Initialize cached timestamp
        super::cached_time::init();

        // Create directories
        tokio::fs::create_dir_all(&config.data_dir).await?;
        let directory_lock = Arc::new(DirectoryLock::acquire(&config.data_dir).map_err(
            |error| match error {
                DirectoryLockAcquireError::Locked { path } => {
                    StorageError::DirectoryLocked { path }
                }
                DirectoryLockAcquireError::Io { path, source } => {
                    StorageError::DirectoryLockIo { path, source }
                }
            },
        )?);
        config.data_dir = directory_lock.path().to_path_buf();
        let wal_dir = config.data_dir.join("wal");
        let sstable_dir = config.data_dir.join("sstables");
        tokio::fs::create_dir_all(&wal_dir).await?;
        tokio::fs::create_dir_all(&sstable_dir).await?;

        // Pre-create level directories
        for level in 0..config.compaction_config.max_levels {
            let level_dir = sstable_dir.join(format!("L{}", level));
            tokio::fs::create_dir_all(&level_dir).await?;
        }

        // Load manifest
        let manifest = Manifest::load_or_create(&config.data_dir)
            .map_err(|e| StorageError::Manifest(e.to_string()))?;
        let wal_checkpoint = manifest.wal_checkpoint;
        let persisted_next_sequence = manifest
            .sstables
            .iter()
            .map(|sstable| sstable.max_sequence)
            .max()
            .map_or(0, |sequence| sequence.saturating_add(1))
            .max(wal_checkpoint);

        let next_sstable_id = manifest.sstables.iter().map(|s| s.id).max().unwrap_or(0) + 1;

        info!(
            "Opening database: wal_checkpoint={}, sstables={}",
            wal_checkpoint,
            manifest.sstables.len()
        );

        // Create WAL if enabled
        let wal = if config.wal_enabled {
            let wal = Arc::new(
                WriteAheadLog::new_with_directory_lock(
                    &wal_dir,
                    config.wal_config.clone(),
                    Arc::downgrade(&directory_lock),
                )
                .await?,
            );
            wal.ensure_next_sequence_at_least(persisted_next_sequence);
            Some(wal)
        } else {
            None
        };

        // Create memtable manager
        let next_sequence = wal
            .as_ref()
            .map_or(persisted_next_sequence, |wal| wal.current_sequence());
        let memtable_manager = Arc::new(MemTableManager::new_with_next_sequence(
            config.memtable_config.clone(),
            next_sequence,
        ));

        // Load SSTable info
        let sstables: Vec<SSTableInfo> = manifest
            .sstables
            .iter()
            .map(|entry| SSTableInfo {
                id: entry.id,
                path: entry.path.clone(),
                file_size: entry.size,
                entry_count: entry.entry_count,
                min_key: entry.min_key.clone(),
                max_key: entry.max_key.clone(),
                creation_time: entry.creation_time,
                level: entry.level,
                min_sequence: entry.min_sequence,
                max_sequence: entry.max_sequence,
            })
            .collect();

        // Replay WAL for crash recovery
        if let Some(ref wal) = wal {
            let replayed = Self::replay_wal(wal, &memtable_manager, wal_checkpoint).await?;
            if replayed > 0 {
                info!(
                    "Crash recovery: replayed {} WAL entries from sequence {}",
                    replayed, wal_checkpoint
                );
            }
        }

        let (shutdown_tx, _) = tokio::sync::watch::channel(false);
        let next_sstable_id = Arc::new(std::sync::atomic::AtomicU64::new(next_sstable_id));

        // Block cache
        let block_cache = if config.block_cache_size > 0 {
            Some(Arc::new(super::cache::BlockCache::new(
                config.block_cache_size,
            )))
        } else {
            None
        };

        let sstable_pool = Arc::new(SSTablePool::with_cache(
            config.fd_config.clone(),
            block_cache,
        ));
        let fd_monitor = Arc::new(FdMonitor::new(config.fd_config.soft_limit_ratio));

        let compactor = Arc::new(Compactor::new(
            config.compaction_config.clone(),
            config.sstable_config.clone(),
            config.data_dir.clone(),
            Arc::clone(&next_sstable_id),
        ));

        // Initialize SSTable stats from existing SSTables
        let initial_sstable_keys: u64 = sstables.iter().map(|s| s.entry_count).sum();
        let initial_sstable_bytes: u64 = sstables.iter().map(|s| s.file_size).sum();
        let initial_sstable_count = sstables.len() as u64;
        let initial_l0_sstable_count = sstables.iter().filter(|s| s.level == 0).count() as u64;

        let engine = Self {
            config,
            directory_lock,
            wal,
            memtable_manager,
            sstables: Arc::new(RwLock::new(sstables)),
            manifest: Arc::new(Mutex::new(manifest)),
            flush_lock: Arc::new(AsyncMutex::new(())),
            mutation_barrier: Arc::new(RwLock::new(())),
            batch_serialization: Arc::new(AsyncMutex::new(())),
            batch_visibility: Arc::new(RwLock::new(())),
            unapplied_wal_sequences: Arc::new(Mutex::new(BTreeSet::new())),
            shutdown: shutdown_tx,
            background_tasks: Mutex::new(Vec::new()),
            shutdown_lock: AsyncMutex::new(()),
            next_sstable_id,
            sstable_pool,
            fd_monitor,
            compactor,
            sstable_total_keys: Arc::new(AtomicU64::new(initial_sstable_keys)),
            sstable_total_bytes: Arc::new(AtomicU64::new(initial_sstable_bytes)),
            sstable_count: Arc::new(AtomicU64::new(initial_sstable_count)),
            l0_sstable_count: Arc::new(AtomicU64::new(initial_l0_sstable_count)),
            wal_bytes_written: Arc::new(AtomicU64::new(0)),
            sstable_flush_bytes_written: Arc::new(AtomicU64::new(0)),
            compaction_bytes_read: Arc::new(AtomicU64::new(0)),
            compaction_bytes_written: Arc::new(AtomicU64::new(0)),
            compactions_in_progress: Arc::new(AtomicU64::new(0)),
            write_stall_count: Arc::new(AtomicU64::new(0)),
            write_stall_micros: Arc::new(AtomicU64::new(0)),
        };

        // Start background tasks
        #[cfg(not(test))]
        engine.start_background_tasks();
        #[cfg(test)]
        if engine.config.background_tasks_enabled {
            engine.start_background_tasks();
        }

        Ok(engine)
    }

    /// Insert a key-value pair
    ///
    /// Automatically uses optimal path based on configuration:
    /// - No WAL: Sync path with thread-local buffering (faster)
    /// - With WAL: Async path for durability
    pub async fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.maybe_stall_writes().await;
        let _mutation = self.mutation_barrier.read().await;

        if let Some(ref wal) = self.wal {
            // Durable path: WAL write then memtable
            let mut pending =
                PendingWalApplication::new(&self.unapplied_wal_sequences, wal.current_sequence());
            let sequence = wal.append(key, value).await?;
            #[cfg(test)]
            super::failpoints::check(
                &self.config.data_dir,
                super::failpoints::PersistenceBoundary::Wal,
            )?;
            self.wal_bytes_written
                .fetch_add(wal_data_entry_size(key, value), Ordering::Relaxed);
            let _publication = self.batch_visibility.read().await;
            self.memtable_manager
                .insert_with_sequence(key, value, sequence)
                .map_err(StorageError::MemTable)?;
            pending.disarm();
        } else {
            // Fast path: thread-local buffered insert, no async overhead
            self.memtable_manager
                .insert_buffered(key, value)
                .map_err(StorageError::MemTable)?;
        }

        Ok(())
    }

    /// Insert multiple key-value pairs.
    pub async fn insert_many(&self, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        self.maybe_stall_writes().await;
        let _mutation = self.mutation_barrier.read().await;

        if let Some(ref wal) = self.wal {
            let wal_entries: Vec<(&[u8], Option<&[u8]>)> = entries
                .iter()
                .map(|(key, value)| (key.as_slice(), Some(value.as_slice())))
                .collect();
            let mut pending =
                PendingWalApplication::new(&self.unapplied_wal_sequences, wal.current_sequence());
            let appended = wal.append_batch_with_metadata(&wal_entries).await?;
            #[cfg(test)]
            super::failpoints::check(
                &self.config.data_dir,
                super::failpoints::PersistenceBoundary::Wal,
            )?;
            self.wal_bytes_written
                .fetch_add(appended.bytes_written, Ordering::Relaxed);
            let _publication = self.batch_visibility.read().await;
            self.memtable_manager
                .insert_many_with_sequences(entries, &appended.sequences)
                .map_err(StorageError::MemTable)?;
            pending.disarm();
        } else {
            self.memtable_manager
                .insert_many(entries)
                .map_err(StorageError::MemTable)?;
        }

        Ok(())
    }

    /// Flush any pending writes from ALL thread-local buffers
    ///
    /// This flushes buffers from ALL threads (not just the calling thread),
    /// ensuring all concurrent writes are visible before reading.
    pub fn flush_write_buffers(&self) -> Result<()> {
        self.memtable_manager
            .flush_thread_local()
            .map_err(StorageError::MemTable)?;
        Ok(())
    }

    /// Delete a key
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        self.maybe_stall_writes().await;
        let _mutation = self.mutation_barrier.read().await;

        // Write to WAL first (if enabled)
        if let Some(ref wal) = self.wal {
            let mut pending =
                PendingWalApplication::new(&self.unapplied_wal_sequences, wal.current_sequence());
            let sequence = wal.append_delete(key).await?;
            #[cfg(test)]
            super::failpoints::check(
                &self.config.data_dir,
                super::failpoints::PersistenceBoundary::Wal,
            )?;
            self.wal_bytes_written
                .fetch_add(wal_delete_entry_size(key), Ordering::Relaxed);
            let _publication = self.batch_visibility.read().await;
            self.memtable_manager
                .delete_with_sequence(key, sequence)
                .map_err(StorageError::MemTable)?;
            pending.disarm();
        } else {
            self.memtable_manager
                .delete(key)
                .map_err(StorageError::MemTable)?;
        }

        Ok(())
    }

    /// Get a value by key
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut newest = self
            .snapshot_memtable(|manager| manager.get_entry(key))
            .await?
            .map(VersionedValue::from_memtable);

        // Resolve all physical copies by sequence; physical list order is not
        // a version order after reopen or compaction.
        for source in self.pin_sstables(|_| true).await? {
            if let Some(entry) = source
                .reader
                .get_entry(key)
                .map_err(|error| StorageError::SSTable(error.to_string()))?
            {
                let candidate = VersionedValue::from_sstable(entry, &source.info);
                retain_newest(&mut newest, candidate);
            }
        }

        Ok(newest.and_then(|entry| entry.value))
    }

    /// Check if a key exists
    pub async fn contains_key(&self, key: &[u8]) -> Result<bool> {
        Ok(self.get(key).await?.is_some())
    }

    /// Scan a range of keys
    pub async fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.range_iter(start, end)
            .await?
            .collect_pairs()
            .map_err(super::iter::ScanError::into_storage_error)
    }

    /// Scan all keys with a given prefix
    pub async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_prefix_iter(prefix)
            .await?
            .collect_pairs()
            .map_err(super::iter::ScanError::into_storage_error)
    }

    /// Scan a range of keys, returning a guard iterator for lazy value access.
    ///
    /// This allows filtering by key without loading values until needed.
    pub async fn range_iter(&self, start: &[u8], end: &[u8]) -> Result<RangeIter> {
        if start >= end {
            return Ok(RangeIter::empty(Arc::clone(&self.directory_lock)));
        }
        self.scan_iter(
            ScanBounds::range(start, end),
            Arc::clone(&self.directory_lock),
        )
        .await
    }

    /// Scan keys with a prefix, returning a guard iterator for lazy value access.
    ///
    /// This allows filtering by key without loading values until needed.
    pub async fn scan_prefix_iter(&self, prefix: &[u8]) -> Result<super::iter::PrefixIter> {
        self.scan_iter(ScanBounds::prefix(prefix), Arc::clone(&self.directory_lock))
            .await
    }

    /// Write a batch of operations atomically
    pub async fn write_batch(&self, batch: &WriteBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        self.maybe_stall_writes().await;
        let _batch_order = self.batch_serialization.lock().await;

        // Convert to WAL batch format
        let ops: Vec<(&[u8], Option<&[u8]>)> = batch
            .ops()
            .iter()
            .map(|op| match op {
                crate::core::BatchOp::Put { key, value } => {
                    (key.as_slice(), Some(value.as_slice()))
                }
                crate::core::BatchOp::Delete { key } => (key.as_slice(), None),
            })
            .collect();

        if let Some(ref wal) = self.wal {
            // Keep checkpoint installation behind the full append-to-apply
            // interval, but do not block readers during WAL I/O.
            let _mutation = self.mutation_barrier.read().await;
            let mut pending =
                PendingWalApplication::new(&self.unapplied_wal_sequences, wal.current_sequence());
            let appended = wal.append_batch_with_metadata(&ops).await?;
            #[cfg(test)]
            super::failpoints::check(
                &self.config.data_dir,
                super::failpoints::PersistenceBoundary::Wal,
            )?;
            self.wal_bytes_written
                .fetch_add(appended.bytes_written, Ordering::Relaxed);

            let _publication = self.batch_visibility.write().await;
            self.memtable_manager
                .apply_batch_with_sequences(&ops, &appended.sequences)
                .map_err(StorageError::MemTable)?;
            pending.disarm();
        } else {
            // Fast readers acquire these locks in this order while draining
            // buffers, so keep the no-WAL batch path consistent.
            let _publication = self.batch_visibility.write().await;
            let _mutation = self.mutation_barrier.read().await;
            let start = self
                .memtable_manager
                .reserve_sequence_range(batch.ops().len());
            let sequences = (start..start + batch.ops().len() as u64).collect::<Vec<_>>();
            // The manager applies into one stable generation without capacity
            // failures between operations, so an error cannot expose a prefix.
            self.memtable_manager
                .apply_batch_with_sequences(&ops, &sequences)
                .map_err(StorageError::MemTable)?;
        }

        Ok(())
    }

    /// Flush memtable to SSTable
    pub async fn flush(&self) -> Result<()> {
        let _flush = self.flush_lock.lock().await;
        {
            // Wait for every sequence already allocated by a writer to reach a
            // memtable before freezing the generation covered by this flush.
            let _mutations = self.mutation_barrier.write().await;
            if self.wal.is_none() {
                self.memtable_manager
                    .flush_thread_local()
                    .map_err(StorageError::MemTable)?;
            }
            self.memtable_manager
                .force_rotate()
                .map_err(StorageError::MemTable)?;
        }
        #[cfg(test)]
        super::failpoints::check(
            &self.config.data_dir,
            super::failpoints::PersistenceBoundary::MemtableFreeze,
        )?;

        // Flush all immutable memtables
        while let Some(memtable) = self.memtable_manager.peek_immutable_for_flush() {
            self.flush_memtable_to_sstable(&memtable).await?;
            if !self.memtable_manager.complete_immutable_flush(&memtable) {
                return Err(StorageError::Other(
                    "completed flush no longer owns the oldest immutable generation".to_string(),
                ));
            }
        }

        // Flush WAL
        if let Some(ref wal) = self.wal {
            wal.flush().await?;
        }

        Ok(())
    }

    /// Trigger compaction
    pub async fn compact(&self) -> Result<CompactionResult> {
        let _in_progress = InProgressGuard::new(Arc::clone(&self.compactions_in_progress));
        let sstables = self.sstables.read().await;

        // Convert SSTableInfo to SSTableManifestEntry for compactor
        let manifest_entries: Vec<SSTableManifestEntry> = sstables
            .iter()
            .map(|sst| SSTableManifestEntry {
                id: sst.id,
                level: sst.level,
                path: sst.path.clone(),
                size: sst.file_size,
                entry_count: sst.entry_count,
                min_key: sst.min_key.clone(),
                max_key: sst.max_key.clone(),
                min_sequence: sst.min_sequence,
                max_sequence: sst.max_sequence,
                creation_time: sst.creation_time,
            })
            .collect();

        if let Some(job) = self.compactor.pick_compaction(&manifest_entries) {
            drop(sstables);
            self.run_compaction(job).await?;

            Ok(CompactionResult {
                files_compacted: 1,
                bytes_reclaimed: 0,
                duration_ms: 0,
            })
        } else {
            Ok(CompactionResult::default())
        }
    }

    /// Get storage statistics
    pub fn stats(&self) -> StorageStats {
        let memtable_stats = self.memtable_manager.stats();

        // Count keys from active + immutable memtables
        let memtable_keys = memtable_stats.active.entry_count as u64
            + memtable_stats
                .immutable
                .iter()
                .map(|s| s.entry_count as u64)
                .sum::<u64>();

        let memtable_bytes = memtable_stats.active.size_bytes as u64
            + memtable_stats
                .immutable
                .iter()
                .map(|s| s.size_bytes as u64)
                .sum::<u64>();

        // Add SSTable stats from atomic counters
        let sstable_keys = self.sstable_total_keys.load(Ordering::Relaxed);
        let sstable_bytes = self.sstable_total_bytes.load(Ordering::Relaxed);
        let sstable_count = self.sstable_count.load(Ordering::Relaxed) as u32;

        StorageStats {
            total_keys: memtable_keys + sstable_keys,
            total_bytes: memtable_bytes + sstable_bytes,
            wal_size: self.wal.as_ref().map(|w| w.current_size()).unwrap_or(0),
            sstable_count,
            memtable_size: memtable_stats.active.size_bytes as u64,
            compaction_pending: !memtable_stats.immutable.is_empty(),
            wal_bytes_written: self.wal_bytes_written.load(Ordering::Relaxed),
            sstable_flush_bytes_written: self.sstable_flush_bytes_written.load(Ordering::Relaxed),
            compaction_bytes_read: self.compaction_bytes_read.load(Ordering::Relaxed),
            compaction_bytes_written: self.compaction_bytes_written.load(Ordering::Relaxed),
            compactions_in_progress: self.compactions_in_progress.load(Ordering::Acquire),
            immutable_memtables: memtable_stats.immutable.len() as u64,
            l0_sstable_count: self.l0_sstable_count.load(Ordering::Relaxed),
            write_stall_count: self.write_stall_count.load(Ordering::Relaxed),
            write_stall_micros: self.write_stall_micros.load(Ordering::Relaxed),
        }
    }

    /// Shutdown the engine gracefully.
    ///
    /// This stops background tasks and flushes pending writes. Because this
    /// method borrows the still-usable engine, its exclusive directory lock is
    /// retained until the [`Engine`] is dropped.
    pub async fn shutdown(&self) -> Result<()> {
        let _shutdown = self.shutdown_lock.lock().await;
        info!("Shutting down storage engine");

        // Signal background tasks to stop
        let _ = self.shutdown.send(true);

        let handles: Vec<_> = self.background_tasks.lock().drain(..).collect();
        let mut task_failure = None;
        for handle in handles {
            if let Err(error) = handle.await {
                task_failure.get_or_insert(error);
            }
        }

        // Flush pending writes
        let flush_result = self.flush().await;

        if let Some(error) = task_failure {
            return Err(StorageError::Other(format!(
                "background task failed during shutdown: {error}"
            )));
        }
        flush_result?;

        info!("Storage engine shutdown complete");
        Ok(())
    }

    // ========================================
    // Private methods
    // ========================================

    /// Replay WAL entries for crash recovery
    async fn replay_wal(
        wal: &WriteAheadLog,
        memtable_manager: &MemTableManager,
        checkpoint_sequence: u64,
    ) -> Result<usize> {
        let mut count = 0;

        for entry in wal.iter_entries_from(checkpoint_sequence).await? {
            let entry = entry?;
            if let Some((key, value)) = entry.decode_kv() {
                match value {
                    Some(v) => {
                        memtable_manager
                            .insert_with_sequence(key, v, entry.sequence)
                            .map_err(StorageError::MemTable)?;
                    }
                    None => {
                        memtable_manager
                            .delete_with_sequence(key, entry.sequence)
                            .map_err(StorageError::MemTable)?;
                    }
                }
                count += 1;
            }
        }

        Ok(count)
    }

    async fn maybe_stall_writes(&self) {
        let immutable_count = self.memtable_manager.immutable_count();
        let l0_count = self.l0_sstable_count.load(Ordering::Relaxed);
        let should_stall = immutable_count >= self.config.max_immutable_memtables_before_stall
            || l0_count >= self.config.max_l0_files_before_stall;

        if should_stall && self.config.write_stall_micros > 0 {
            self.write_stall_count.fetch_add(1, Ordering::Relaxed);
            self.write_stall_micros
                .fetch_add(self.config.write_stall_micros, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_micros(self.config.write_stall_micros)).await;
        }
    }

    async fn snapshot_memtable<T>(
        &self,
        snapshot: impl FnOnce(&MemTableManager) -> T,
    ) -> Result<T> {
        let _publication = self.batch_visibility.read().await;
        let _mutations = if self.wal.is_none() {
            // Fast readers briefly exclude sequence allocation while they drain
            // buffers and capture the in-memory state at one linearization point.
            Some(self.mutation_barrier.write().await)
        } else {
            None
        };
        if self.wal.is_none() {
            self.memtable_manager
                .flush_thread_local()
                .map_err(StorageError::MemTable)?;
        }
        Ok(snapshot(&self.memtable_manager))
    }

    /// Freeze and retain a coherent source set for a streaming scan.
    async fn scan_iter(
        &self,
        bounds: ScanBounds,
        directory_lock: Arc<DirectoryLock>,
    ) -> Result<RangeIter> {
        // WAL-backed mutations enter the publication gate only for their short
        // memtable apply. This lets a scan freeze the old state while another
        // mutation is still waiting on WAL I/O. Fast-mode scans additionally
        // exclude sequence allocation while draining all thread-local buffers.
        let memtables = {
            let _publication = self.batch_visibility.write().await;
            let _mutations = if self.wal.is_none() {
                let mutations = self.mutation_barrier.write().await;
                self.memtable_manager
                    .flush_thread_local()
                    .map_err(StorageError::MemTable)?;
                Some(mutations)
            } else {
                None
            };
            self.memtable_manager
                .force_rotate()
                .map_err(StorageError::MemTable)?;
            self.memtable_manager.snapshot_immutable_tables()
        };

        // Capture memtables before SSTables. Flush installs the SSTable before
        // removing its immutable, so this order cannot miss either side of the
        // handoff. The SSTable list itself is cloned under its lock; reader I/O
        // happens after release and retries if compaction unlinked an input.
        let sstables = self
            .pin_sstables(|info| bounds.overlaps_table(info))
            .await?;

        RangeIter::from_sources(bounds, memtables, sstables, directory_lock)
    }

    /// Pin one coherent reader set without holding the live-list lock over I/O.
    async fn pin_sstables(
        &self,
        include: impl Fn(&SSTableInfo) -> bool,
    ) -> Result<Vec<ScanSstable>> {
        loop {
            let (snapshot_identity, selected) = {
                let live_sstables = self.sstables.read().await;
                let identity = live_sstables
                    .iter()
                    .map(|info| (info.id, info.path.clone()))
                    .collect::<Vec<_>>();
                let selected = live_sstables
                    .iter()
                    .filter(|info| include(info))
                    .cloned()
                    .collect::<Vec<_>>();
                (identity, selected)
            };

            let mut pinned = Vec::with_capacity(selected.len());
            let mut missing = None;
            for info in selected {
                match self.sstable_pool.get(&info.path) {
                    Ok(reader) => pinned.push(ScanSstable { info, reader }),
                    Err(error) if sstable_open_was_not_found(&error) => {
                        missing = Some(error);
                        break;
                    }
                    Err(error) => return Err(StorageError::SSTable(error.to_string())),
                }
            }
            let Some(error) = missing else {
                return Ok(pinned);
            };

            // Compaction publishes the replacement list before unlinking its
            // inputs. If an input disappeared after capture, discard every
            // partial pin and retry from one new coherent list snapshot.
            let live_sstables = self.sstables.read().await;
            let unchanged = live_sstables.len() == snapshot_identity.len()
                && live_sstables
                    .iter()
                    .zip(&snapshot_identity)
                    .all(|(info, (id, path))| info.id == *id && info.path == *path);
            drop(live_sstables);
            if unchanged {
                return Err(StorageError::SSTable(error.to_string()));
            }
            tokio::task::yield_now().await;
        }
    }

    /// Flush a memtable to SSTable
    async fn flush_memtable_to_sstable(
        &self,
        memtable: &Arc<super::memtable::MemTable>,
    ) -> Result<SSTableInfo> {
        flush_memtable_to_sstable(
            FlushResources {
                config: &self.config,
                wal: self.wal.as_ref(),
                memtable_manager: &self.memtable_manager,
                sstables: &self.sstables,
                manifest: &self.manifest,
                mutation_barrier: &self.mutation_barrier,
                unapplied_wal_sequences: &self.unapplied_wal_sequences,
                next_sstable_id: &self.next_sstable_id,
                sstable_total_keys: &self.sstable_total_keys,
                sstable_total_bytes: &self.sstable_total_bytes,
                sstable_count: &self.sstable_count,
                l0_sstable_count: &self.l0_sstable_count,
                sstable_flush_bytes_written: &self.sstable_flush_bytes_written,
            },
            memtable,
        )
        .await
    }

    /// Run a compaction job
    async fn run_compaction(&self, job: super::compaction::CompactionJob) -> Result<()> {
        // Get paths of input files before compaction
        let input_paths: Vec<PathBuf> = job.input_sstables.iter().map(|s| s.path.clone()).collect();
        let input_ids: Vec<u64> = job.input_sstables.iter().map(|s| s.id).collect();

        // Track stats of removed SSTables for counter updates
        let removed_keys: u64 = job.input_sstables.iter().map(|s| s.entry_count).sum();
        let removed_bytes: u64 = job.input_sstables.iter().map(|s| s.size).sum();
        let removed_count = job.input_sstables.len() as u64;
        let removed_l0_count = job.input_sstables.iter().filter(|s| s.level == 0).count() as u64;

        let result = self
            .compactor
            .execute(job)
            .map_err(|e| StorageError::Compaction(e.to_string()))?;
        #[cfg(test)]
        super::failpoints::check(
            &self.config.data_dir,
            super::failpoints::PersistenceBoundary::Compaction,
        )?;

        // Update SSTable list
        let mut sstables = self.sstables.write().await;

        // Remove old files by id
        sstables.retain(|sst| !input_ids.contains(&sst.id));

        // Update atomic counters: subtract removed SSTables
        self.sstable_total_keys
            .fetch_sub(removed_keys, Ordering::Relaxed);
        self.sstable_total_bytes
            .fetch_sub(removed_bytes, Ordering::Relaxed);
        self.sstable_count
            .fetch_sub(removed_count, Ordering::Relaxed);
        self.l0_sstable_count
            .fetch_sub(removed_l0_count, Ordering::Relaxed);

        // Add new file if compaction produced output
        if let Some(ref output) = result.output_sstable {
            sstables.push(SSTableInfo {
                id: output.id,
                level: output.level,
                path: output.path.clone(),
                file_size: output.size,
                entry_count: output.entry_count,
                min_key: output.min_key.clone(),
                max_key: output.max_key.clone(),
                creation_time: output.creation_time,
                min_sequence: output.min_sequence,
                max_sequence: output.max_sequence,
            });

            // Update atomic counters: add new SSTable
            self.sstable_total_keys
                .fetch_add(output.entry_count, Ordering::Relaxed);
            self.sstable_total_bytes
                .fetch_add(output.size, Ordering::Relaxed);
            self.sstable_count.fetch_add(1, Ordering::Relaxed);
            if output.level == 0 {
                self.l0_sstable_count.fetch_add(1, Ordering::Relaxed);
            }
        }

        self.compaction_bytes_read
            .fetch_add(result.bytes_read, Ordering::Relaxed);
        self.compaction_bytes_written
            .fetch_add(result.bytes_written, Ordering::Relaxed);

        // The live source swap is complete. Manifest I/O and unlinking old
        // inputs must not hold the global reader-list lock.
        drop(sstables);

        // Update manifest
        {
            let mut manifest = self.manifest.lock();
            manifest.sstables.retain(|e| !input_ids.contains(&e.id));

            if let Some(ref output) = result.output_sstable {
                manifest.sstables.push(output.clone());
            }

            manifest
                .save(&self.config.data_dir)
                .map_err(|e| StorageError::Manifest(e.to_string()))?;
        }

        // Delete old files
        self.compactor
            .cleanup_inputs(&input_paths)
            .map_err(|e| StorageError::Compaction(e.to_string()))?;

        Ok(())
    }

    /// Start background flush and compaction tasks
    fn start_background_tasks(&self) {
        let engine = Arc::new(self.clone_for_background());

        // Flush task
        let flush_engine = engine.clone();
        let mut shutdown_rx = self.shutdown.subscribe();
        let flush_interval = self.config.flush_interval;
        let flush_task = tokio::spawn(async move {
            let mut interval = interval(flush_interval);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if flush_engine.memtable_manager.has_immutable() {
                            if let Err(e) = flush_engine.background_flush().await {
                                error!("Background flush error: {}", e);
                            }
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        break;
                    }
                }
            }
        });

        // Compaction task
        let compact_engine = engine.clone();
        let mut shutdown_rx = self.shutdown.subscribe();
        let compaction_interval = self.config.compaction_interval;
        let compaction_task = tokio::spawn(async move {
            let mut interval = interval(compaction_interval);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = compact_engine.compact().await {
                            error!("Background compaction error: {}", e);
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        break;
                    }
                }
            }
        });

        self.background_tasks
            .lock()
            .extend([flush_task, compaction_task]);
    }

    /// Clone engine state for background tasks
    fn clone_for_background(&self) -> BackgroundEngine {
        BackgroundEngine {
            wal: self.wal.clone(),
            directory_lock: Arc::downgrade(&self.directory_lock),
            memtable_manager: self.memtable_manager.clone(),
            sstables: self.sstables.clone(),
            manifest: self.manifest.clone(),
            flush_lock: self.flush_lock.clone(),
            mutation_barrier: self.mutation_barrier.clone(),
            unapplied_wal_sequences: self.unapplied_wal_sequences.clone(),
            next_sstable_id: self.next_sstable_id.clone(),
            compactor: self.compactor.clone(),
            config: self.config.clone(),
            sstable_total_keys: self.sstable_total_keys.clone(),
            sstable_total_bytes: self.sstable_total_bytes.clone(),
            sstable_count: self.sstable_count.clone(),
            l0_sstable_count: self.l0_sstable_count.clone(),
            sstable_flush_bytes_written: self.sstable_flush_bytes_written.clone(),
            compaction_bytes_read: self.compaction_bytes_read.clone(),
            compaction_bytes_written: self.compaction_bytes_written.clone(),
            compactions_in_progress: self.compactions_in_progress.clone(),
        }
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        for handle in self.background_tasks.get_mut().drain(..) {
            handle.abort();
        }
    }
}

/// Clone of engine state for background tasks
struct BackgroundEngine {
    directory_lock: std::sync::Weak<DirectoryLock>,
    memtable_manager: Arc<MemTableManager>,
    sstables: Arc<RwLock<Vec<SSTableInfo>>>,
    manifest: Arc<Mutex<Manifest>>,
    flush_lock: Arc<AsyncMutex<()>>,
    mutation_barrier: Arc<RwLock<()>>,
    unapplied_wal_sequences: Arc<Mutex<BTreeSet<u64>>>,
    next_sstable_id: Arc<std::sync::atomic::AtomicU64>,
    compactor: Arc<Compactor>,
    config: StorageConfig,
    wal: Option<Arc<WriteAheadLog>>,
    // Atomic counters for SSTable stats
    sstable_total_keys: Arc<AtomicU64>,
    sstable_total_bytes: Arc<AtomicU64>,
    sstable_count: Arc<AtomicU64>,
    l0_sstable_count: Arc<AtomicU64>,
    sstable_flush_bytes_written: Arc<AtomicU64>,
    compaction_bytes_read: Arc<AtomicU64>,
    compaction_bytes_written: Arc<AtomicU64>,
    compactions_in_progress: Arc<AtomicU64>,
}

impl BackgroundEngine {
    async fn background_flush(&self) -> Result<()> {
        let Some(_directory_lock) = self.directory_lock.upgrade() else {
            return Ok(());
        };
        let _flush = self.flush_lock.lock().await;
        while let Some(memtable) = self.memtable_manager.peek_immutable_for_flush() {
            self.flush_memtable_to_sstable(&memtable).await?;
            if !self.memtable_manager.complete_immutable_flush(&memtable) {
                return Err(StorageError::Other(
                    "completed background flush no longer owns the oldest immutable generation"
                        .to_string(),
                ));
            }
        }
        Ok(())
    }

    async fn compact(&self) -> Result<CompactionResult> {
        let Some(_directory_lock) = self.directory_lock.upgrade() else {
            return Ok(CompactionResult::default());
        };
        let _in_progress = InProgressGuard::new(Arc::clone(&self.compactions_in_progress));
        let sstables = self.sstables.read().await;
        let manifest_entries: Vec<SSTableManifestEntry> = sstables
            .iter()
            .map(|sst| SSTableManifestEntry {
                id: sst.id,
                level: sst.level,
                path: sst.path.clone(),
                size: sst.file_size,
                entry_count: sst.entry_count,
                min_key: sst.min_key.clone(),
                max_key: sst.max_key.clone(),
                min_sequence: sst.min_sequence,
                max_sequence: sst.max_sequence,
                creation_time: sst.creation_time,
            })
            .collect();

        if let Some(job) = self.compactor.pick_compaction(&manifest_entries) {
            drop(sstables);
            self.run_compaction(job).await?;

            Ok(CompactionResult {
                files_compacted: 1,
                bytes_reclaimed: 0,
                duration_ms: 0,
            })
        } else {
            Ok(CompactionResult::default())
        }
    }

    async fn flush_memtable_to_sstable(
        &self,
        memtable: &Arc<super::memtable::MemTable>,
    ) -> Result<SSTableInfo> {
        flush_memtable_to_sstable(
            FlushResources {
                config: &self.config,
                wal: self.wal.as_ref(),
                memtable_manager: &self.memtable_manager,
                sstables: &self.sstables,
                manifest: &self.manifest,
                mutation_barrier: &self.mutation_barrier,
                unapplied_wal_sequences: &self.unapplied_wal_sequences,
                next_sstable_id: &self.next_sstable_id,
                sstable_total_keys: &self.sstable_total_keys,
                sstable_total_bytes: &self.sstable_total_bytes,
                sstable_count: &self.sstable_count,
                l0_sstable_count: &self.l0_sstable_count,
                sstable_flush_bytes_written: &self.sstable_flush_bytes_written,
            },
            memtable,
        )
        .await
    }

    async fn run_compaction(&self, job: super::compaction::CompactionJob) -> Result<()> {
        let input_paths: Vec<PathBuf> = job.input_sstables.iter().map(|s| s.path.clone()).collect();
        let input_ids: Vec<u64> = job.input_sstables.iter().map(|s| s.id).collect();

        // Track stats of removed SSTables for counter updates
        let removed_keys: u64 = job.input_sstables.iter().map(|s| s.entry_count).sum();
        let removed_bytes: u64 = job.input_sstables.iter().map(|s| s.size).sum();
        let removed_count = job.input_sstables.len() as u64;
        let removed_l0_count = job.input_sstables.iter().filter(|s| s.level == 0).count() as u64;

        let result = self
            .compactor
            .execute(job)
            .map_err(|e| StorageError::Compaction(e.to_string()))?;
        #[cfg(test)]
        super::failpoints::check(
            &self.config.data_dir,
            super::failpoints::PersistenceBoundary::Compaction,
        )?;

        let mut sstables = self.sstables.write().await;
        sstables.retain(|sst| !input_ids.contains(&sst.id));

        // Update atomic counters: subtract removed SSTables
        self.sstable_total_keys
            .fetch_sub(removed_keys, Ordering::Relaxed);
        self.sstable_total_bytes
            .fetch_sub(removed_bytes, Ordering::Relaxed);
        self.sstable_count
            .fetch_sub(removed_count, Ordering::Relaxed);
        self.l0_sstable_count
            .fetch_sub(removed_l0_count, Ordering::Relaxed);

        if let Some(ref output) = result.output_sstable {
            sstables.push(SSTableInfo {
                id: output.id,
                level: output.level,
                path: output.path.clone(),
                file_size: output.size,
                entry_count: output.entry_count,
                min_key: output.min_key.clone(),
                max_key: output.max_key.clone(),
                creation_time: output.creation_time,
                min_sequence: output.min_sequence,
                max_sequence: output.max_sequence,
            });

            // Update atomic counters: add new SSTable
            self.sstable_total_keys
                .fetch_add(output.entry_count, Ordering::Relaxed);
            self.sstable_total_bytes
                .fetch_add(output.size, Ordering::Relaxed);
            self.sstable_count.fetch_add(1, Ordering::Relaxed);
            if output.level == 0 {
                self.l0_sstable_count.fetch_add(1, Ordering::Relaxed);
            }
        }

        self.compaction_bytes_read
            .fetch_add(result.bytes_read, Ordering::Relaxed);
        self.compaction_bytes_written
            .fetch_add(result.bytes_written, Ordering::Relaxed);

        // The live source swap is complete. Manifest I/O and unlinking old
        // inputs must not hold the global reader-list lock.
        drop(sstables);

        {
            let mut manifest = self.manifest.lock();
            manifest.sstables.retain(|e| !input_ids.contains(&e.id));

            if let Some(ref output) = result.output_sstable {
                manifest.sstables.push(output.clone());
            }

            manifest
                .save(&self.config.data_dir)
                .map_err(|e| StorageError::Manifest(e.to_string()))?;
        }

        self.compactor
            .cleanup_inputs(&input_paths)
            .map_err(|e| StorageError::Compaction(e.to_string()))?;

        Ok(())
    }
}

struct FlushResources<'a> {
    config: &'a StorageConfig,
    wal: Option<&'a Arc<WriteAheadLog>>,
    memtable_manager: &'a Arc<MemTableManager>,
    sstables: &'a Arc<RwLock<Vec<SSTableInfo>>>,
    manifest: &'a Arc<Mutex<Manifest>>,
    mutation_barrier: &'a Arc<RwLock<()>>,
    unapplied_wal_sequences: &'a Arc<Mutex<BTreeSet<u64>>>,
    next_sstable_id: &'a Arc<AtomicU64>,
    sstable_total_keys: &'a Arc<AtomicU64>,
    sstable_total_bytes: &'a Arc<AtomicU64>,
    sstable_count: &'a Arc<AtomicU64>,
    l0_sstable_count: &'a Arc<AtomicU64>,
    sstable_flush_bytes_written: &'a Arc<AtomicU64>,
}

async fn flush_memtable_to_sstable(
    resources: FlushResources<'_>,
    memtable: &Arc<super::memtable::MemTable>,
) -> Result<SSTableInfo> {
    let id = resources
        .memtable_manager
        .reserved_flush_id(memtable)
        .unwrap_or_else(|| {
            let proposed = resources.next_sstable_id.fetch_add(1, Ordering::SeqCst);
            resources
                .memtable_manager
                .reserve_flush_id(memtable, proposed)
        });

    // Cancellation or an injected post-install failure can leave the manifest
    // durable before the live reader list is updated. Reconcile that state
    // without rewriting or recounting the generation.
    let installed_entry = {
        let manifest = resources.manifest.lock();
        manifest
            .sstables
            .iter()
            .find(|entry| entry.id == id)
            .cloned()
    };
    if let Some(entry) = installed_entry {
        let info = sstable_info_from_manifest(&entry);
        let expected = memtable.get_all_entries();
        if !sstable_contents_match(&info.path, &expected)? {
            return Err(StorageError::SSTable(format!(
                "installed SSTable {} does not match its retained immutable generation",
                info.path.display()
            )));
        }
        install_live_sstable(&resources, &info).await;
        reclaim_wal_after_checkpoint(&resources).await?;
        return Ok(info);
    }

    let level_directory = resources.config.data_dir.join("sstables").join("L0");
    let final_path = level_directory.join(format!("{:010}.sst", id));
    let temp_path = level_directory.join(format!(".{:010}.sst.tmp", id));
    let entries = memtable.get_all_entries();
    let info = if let Some(info) = reusable_sstable_info(&final_path, id, &entries)? {
        info
    } else {
        let mut writer = SSTableWriter::new(&temp_path, resources.config.sstable_config.clone())
            .map_err(|error| {
                StorageError::SSTable(format!("Failed to create SSTable writer: {error}"))
            })?;
        for (key, entry) in &entries {
            writer
                .add_versioned(key, entry.value.as_deref(), entry.sequence)
                .map_err(|error| {
                    StorageError::SSTable(format!("Failed to write entry: {error}"))
                })?;
        }

        let mut info = writer
            .finish()
            .map_err(|error| StorageError::SSTable(format!("Failed to finish SSTable: {error}")))?;
        validate_sstable_structure(&temp_path)?;

        #[cfg(test)]
        super::failpoints::check(
            &resources.config.data_dir,
            super::failpoints::PersistenceBoundary::SstablePublication,
        )?;

        // A mismatched final file can only be an unreferenced orphan because
        // the manifest-id branch above was absent. Remove it before rename so
        // publication is retryable on platforms that cannot rename-overwrite.
        if final_path.exists() {
            std::fs::remove_file(&final_path)?;
        }
        atomic_replace(&temp_path, &final_path)?;
        info.id = id;
        info.path.clone_from(&final_path);
        info
    };
    // This also makes a reused final file discoverable before the manifest can
    // reference it, including a retry after a prior directory-sync failure.
    sync_directory(&level_directory)?;

    let checkpoint;
    {
        // No writer may hold an allocated-but-unapplied sequence while the
        // safe recovery frontier is computed and installed.
        let _mutations = resources.mutation_barrier.write().await;
        let mut live_manifest = resources.manifest.lock();
        let mut candidate = live_manifest.clone();
        candidate.sstables.push(SSTableManifestEntry {
            id,
            path: info.path.clone(),
            size: info.file_size,
            entry_count: info.entry_count,
            min_key: info.min_key.clone(),
            max_key: info.max_key.clone(),
            min_sequence: info.min_sequence,
            max_sequence: info.max_sequence,
            creation_time: info.creation_time,
            level: 0,
        });

        let minimum_unapplied_wal = resources.unapplied_wal_sequences.lock().first().copied();
        let proposed_checkpoint = safe_flush_checkpoint(
            &candidate,
            resources.memtable_manager,
            memtable,
            minimum_unapplied_wal,
        );
        candidate.wal_checkpoint = resources.wal.map_or(proposed_checkpoint, |wal| {
            wal.align_checkpoint(proposed_checkpoint)
                .max(candidate.wal_checkpoint)
        });

        #[cfg(test)]
        super::failpoints::check(
            &resources.config.data_dir,
            super::failpoints::PersistenceBoundary::ManifestInstallation,
        )?;

        candidate
            .save(&resources.config.data_dir)
            .map_err(|error| StorageError::Manifest(error.to_string()))?;
        checkpoint = candidate.wal_checkpoint;
        *live_manifest = candidate;
    }

    install_live_sstable(&resources, &info).await;
    info!("Flushed memtable to SSTable: {:?}", info.path);

    #[cfg(test)]
    super::failpoints::check(
        &resources.config.data_dir,
        super::failpoints::PersistenceBoundary::Checkpoint,
    )?;

    truncate_wal(resources.wal, checkpoint).await;
    Ok(info)
}

fn safe_flush_checkpoint(
    manifest: &Manifest,
    memtable_manager: &MemTableManager,
    installing: &Arc<super::memtable::MemTable>,
    minimum_unapplied_wal: Option<u64>,
) -> u64 {
    let durable_next_sequence = manifest
        .sstables
        .iter()
        .map(|table| table.max_sequence)
        .max()
        .map_or(0, |sequence| sequence.saturating_add(1));
    let minimum_uninstalled = memtable_manager
        .minimum_live_sequence_excluding(installing)
        .into_iter()
        .chain(minimum_unapplied_wal)
        .min();
    let safe_next_sequence = minimum_uninstalled.map_or(durable_next_sequence, |minimum_live| {
        durable_next_sequence.min(minimum_live)
    });
    manifest.wal_checkpoint.max(safe_next_sequence)
}

fn reusable_sstable_info(
    path: &std::path::Path,
    id: u64,
    expected: &[(Vec<u8>, super::memtable::MemTableEntry)],
) -> Result<Option<SSTableInfo>> {
    if !path.exists() {
        return Ok(None);
    }
    if !sstable_contents_match(path, expected)? {
        return Ok(None);
    }

    let metadata = std::fs::metadata(path)?;
    let (min_sequence, max_sequence) = expected
        .iter()
        .fold((u64::MAX, 0_u64), |(minimum, maximum), (_, entry)| {
            (minimum.min(entry.sequence), maximum.max(entry.sequence))
        });
    Ok(Some(SSTableInfo {
        id,
        path: path.to_path_buf(),
        file_size: metadata.len(),
        entry_count: expected.len() as u64,
        min_key: expected
            .first()
            .map_or_else(Vec::new, |(key, _)| key.clone()),
        max_key: expected
            .last()
            .map_or_else(Vec::new, |(key, _)| key.clone()),
        creation_time: metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
            .map_or(0, |duration| duration.as_secs()),
        level: 0,
        min_sequence: if expected.is_empty() { 0 } else { min_sequence },
        max_sequence,
    }))
}

fn validate_sstable_structure(path: &std::path::Path) -> Result<()> {
    SSTableReader::open(path)
        .map_err(|error| StorageError::SSTable(format!("Failed to validate SSTable: {error}")))?;
    Ok(())
}

fn sstable_contents_match(
    path: &std::path::Path,
    expected: &[(Vec<u8>, super::memtable::MemTableEntry)],
) -> Result<bool> {
    let reader = match SSTableReader::open(path) {
        Ok(reader) => reader,
        Err(_) => return Ok(false),
    };
    let mut actual = reader.iter();
    for (expected_key, expected_entry) in expected {
        let Some(actual_entry) = actual.next_versioned() else {
            return Ok(false);
        };
        let (actual_key, actual_entry) = match actual_entry {
            Ok(entry) => entry,
            Err(_) => return Ok(false),
        };
        if actual_key.as_ref() != expected_key
            || actual_entry.sequence != Some(expected_entry.sequence)
            || actual_entry.value.into_option().as_deref() != expected_entry.value.as_deref()
        {
            return Ok(false);
        }
    }
    Ok(actual.next_versioned().is_none())
}

fn sstable_info_from_manifest(entry: &SSTableManifestEntry) -> SSTableInfo {
    SSTableInfo {
        id: entry.id,
        path: entry.path.clone(),
        file_size: entry.size,
        entry_count: entry.entry_count,
        min_key: entry.min_key.clone(),
        max_key: entry.max_key.clone(),
        creation_time: entry.creation_time,
        level: entry.level,
        min_sequence: entry.min_sequence,
        max_sequence: entry.max_sequence,
    }
}

async fn install_live_sstable(resources: &FlushResources<'_>, info: &SSTableInfo) {
    let inserted = {
        let mut sstables = resources.sstables.write().await;
        if sstables.iter().any(|table| table.id == info.id) {
            false
        } else {
            sstables.push(info.clone());
            true
        }
    };
    if !inserted {
        return;
    }

    resources
        .sstable_total_keys
        .fetch_add(info.entry_count, Ordering::Relaxed);
    resources
        .sstable_total_bytes
        .fetch_add(info.file_size, Ordering::Relaxed);
    resources.sstable_count.fetch_add(1, Ordering::Relaxed);
    if info.level == 0 {
        resources.l0_sstable_count.fetch_add(1, Ordering::Relaxed);
    }
    resources
        .sstable_flush_bytes_written
        .fetch_add(info.file_size, Ordering::Relaxed);
}

async fn reclaim_wal_after_checkpoint(resources: &FlushResources<'_>) -> Result<()> {
    let checkpoint = resources.manifest.lock().wal_checkpoint;
    #[cfg(test)]
    super::failpoints::check(
        &resources.config.data_dir,
        super::failpoints::PersistenceBoundary::Checkpoint,
    )?;
    truncate_wal(resources.wal, checkpoint).await;
    Ok(())
}

async fn truncate_wal(wal: Option<&Arc<WriteAheadLog>>, checkpoint: u64) {
    if let Some(wal) = wal {
        if let Err(error) = wal.truncate(checkpoint).await {
            tracing::warn!("Failed to truncate WAL: {error}");
        }
    }
}

fn sstable_open_was_not_found(error: &CoreError) -> bool {
    matches!(
        error,
        CoreError::Io { source, .. } if source.kind() == std::io::ErrorKind::NotFound
    )
}

/// A present value or tombstone that remains distinct from a missing key until
/// the newest physical version has been selected.
struct VersionedValue {
    order: VersionOrder,
    value: Option<Vec<u8>>,
}

impl VersionedValue {
    fn from_memtable(entry: super::memtable::MemTableEntry) -> Self {
        Self {
            order: VersionOrder::memory(entry.sequence, u64::MAX),
            value: entry.value,
        }
    }

    fn from_sstable(entry: super::sstable::SSTableEntry, table: &SSTableInfo) -> Self {
        let value = entry.value.into_option().map(|value| value.to_vec());
        Self {
            order: VersionOrder::sstable(entry.sequence, table.id),
            value,
        }
    }

    fn is_newer_than(&self, other: &Self) -> bool {
        self.order > other.order
    }
}

fn retain_newest(current: &mut Option<VersionedValue>, candidate: VersionedValue) {
    if current
        .as_ref()
        .map_or(true, |entry| candidate.is_newer_than(entry))
    {
        *current = Some(candidate);
    }
}

/// Conservatively pins the recovery frontier if a WAL append future is
/// cancelled, panics, or returns after persistence but before memtable apply.
/// The normal success path only flips a local boolean and never takes the set
/// mutex.
struct PendingWalApplication<'a> {
    checkpoint_floors: &'a Mutex<BTreeSet<u64>>,
    floor: u64,
    armed: bool,
}

impl<'a> PendingWalApplication<'a> {
    fn new(checkpoint_floors: &'a Mutex<BTreeSet<u64>>, floor: u64) -> Self {
        Self {
            checkpoint_floors,
            floor,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PendingWalApplication<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.checkpoint_floors.lock().insert(self.floor);
        }
    }
}

fn wal_data_entry_size(key: &[u8], value: &[u8]) -> u64 {
    const WAL_ENTRY_HEADER_SIZE: usize = 32;
    (WAL_ENTRY_HEADER_SIZE + 4 + key.len() + value.len()) as u64
}

fn wal_delete_entry_size(key: &[u8]) -> u64 {
    const WAL_ENTRY_HEADER_SIZE: usize = 32;
    (WAL_ENTRY_HEADER_SIZE + 4 + key.len()) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn isolated_config(path: &std::path::Path, wal_enabled: bool) -> StorageConfig {
        StorageConfig {
            data_dir: path.to_path_buf(),
            wal_enabled,
            background_tasks_enabled: false,
            ..Default::default()
        }
    }

    async fn assert_scope_key_absent(engine: &Engine) {
        assert_eq!(engine.get(b"scope:key").await.unwrap(), None);
        assert!(engine.range(b"scope:", b"scope;").await.unwrap().is_empty());
        assert!(engine.scan_prefix(b"scope:").await.unwrap().is_empty());
    }

    fn write_legacy_table(
        path: &std::path::Path,
        id: u64,
        value: Option<&[u8]>,
        manifest_max_sequence: u64,
    ) -> SSTableManifestEntry {
        let config = SSTableConfig {
            compression: super::super::sstable::CompressionType::None,
            ..SSTableConfig::default()
        };
        let mut writer = SSTableWriter::new_legacy_v2(path, config).unwrap();
        writer.add(b"scope:key", value).unwrap();
        let info = writer.finish().unwrap();
        SSTableManifestEntry {
            id,
            level: 0,
            path: info.path,
            size: info.file_size,
            entry_count: info.entry_count,
            min_key: info.min_key,
            max_key: info.max_key,
            min_sequence: 0,
            max_sequence: manifest_max_sequence,
            creation_time: id,
        }
    }

    fn write_legacy_scan_table(
        path: &std::path::Path,
        id: u64,
        entries: &[(&[u8], Option<&[u8]>)],
        manifest_max_sequence: u64,
    ) -> SSTableManifestEntry {
        let config = SSTableConfig {
            compression: super::super::sstable::CompressionType::None,
            ..SSTableConfig::default()
        };
        let mut writer = SSTableWriter::new_legacy_v2(path, config).unwrap();
        for (key, value) in entries {
            writer.add(key, *value).unwrap();
        }
        let info = writer.finish().unwrap();
        SSTableManifestEntry {
            id,
            level: 0,
            path: info.path,
            size: info.file_size,
            entry_count: info.entry_count,
            min_key: info.min_key,
            max_key: info.max_key,
            min_sequence: 0,
            max_sequence: manifest_max_sequence,
            creation_time: id,
        }
    }

    #[test]
    fn in_progress_guard_tracks_scope_and_drop() {
        let counter = Arc::new(AtomicU64::new(0));
        {
            let _guard = InProgressGuard::new(Arc::clone(&counter));
            assert_eq!(counter.load(Ordering::Acquire), 1);
        }
        assert_eq!(counter.load(Ordering::Acquire), 0);
    }

    #[test]
    fn pending_wal_guard_records_only_an_unfinished_application() {
        let floors = Mutex::new(BTreeSet::new());
        {
            let _pending = PendingWalApplication::new(&floors, 7);
        }
        assert_eq!(floors.lock().iter().copied().collect::<Vec<_>>(), vec![7]);

        {
            let mut completed = PendingWalApplication::new(&floors, 8);
            completed.disarm();
        }
        assert_eq!(floors.lock().iter().copied().collect::<Vec<_>>(), vec![7]);
    }

    #[test]
    fn checkpoint_frontier_stops_before_a_lower_sequence_in_another_generation() {
        let manager = Arc::new(MemTableManager::new(MemTableConfig::default()));
        manager
            .insert_with_sequence(b"installed-later-sequence", b"value", 1)
            .unwrap();
        manager.force_rotate().unwrap();
        let installing = manager.peek_immutable_for_flush().unwrap();
        manager
            .insert_with_sequence(b"still-live-earlier-sequence", b"value", 0)
            .unwrap();

        let mut manifest = Manifest::new();
        manifest.sstables.push(SSTableManifestEntry {
            id: 1,
            level: 0,
            path: PathBuf::from("unused.sst"),
            size: 0,
            entry_count: 1,
            min_key: Vec::new(),
            max_key: Vec::new(),
            min_sequence: 1,
            max_sequence: 1,
            creation_time: 0,
        });

        assert_eq!(
            safe_flush_checkpoint(&manifest, &manager, &installing, None),
            0
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn foreground_and_background_flush_share_exactly_one_generation_owner() {
        let directory = TempDir::new().unwrap();
        let engine = Engine::open(isolated_config(directory.path(), true))
            .await
            .unwrap();
        let background = engine.clone_for_background();

        for generation in 0..25 {
            let key = format!("generation-{generation}");
            engine.insert(key.as_bytes(), b"value").await.unwrap();
            engine.memtable_manager.force_rotate().unwrap();

            let (foreground_result, background_result) =
                tokio::join!(engine.flush(), background.background_flush());
            foreground_result.unwrap();
            background_result.unwrap();
            assert_eq!(engine.stats().immutable_memtables, 0);
        }

        assert_eq!(engine.stats().sstable_count, 25);
        assert_eq!(
            Manifest::load_or_create(directory.path())
                .unwrap()
                .sstables
                .len(),
            25
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn background_flush_failure_retains_read_visibility_and_retries() {
        let directory = TempDir::new().unwrap();
        let engine = Engine::open(isolated_config(directory.path(), true))
            .await
            .unwrap();
        engine.insert(b"key", b"value").await.unwrap();
        engine.memtable_manager.force_rotate().unwrap();
        let background = engine.clone_for_background();
        let failure = super::super::failpoints::arm(
            directory.path(),
            super::super::failpoints::PersistenceBoundary::SstablePublication,
        );

        assert!(background.background_flush().await.is_err());
        failure.assert_hit();
        assert_eq!(engine.stats().immutable_memtables, 1);
        assert_eq!(engine.get(b"key").await.unwrap(), Some(b"value".to_vec()));

        background.background_flush().await.unwrap();
        assert_eq!(engine.stats().immutable_memtables, 0);
        assert_eq!(engine.stats().sstable_count, 1);
        assert_eq!(engine.get(b"key").await.unwrap(), Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn test_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            data_dir: temp_dir.path().to_path_buf(),
            wal_enabled: false,
            ..Default::default()
        };

        let engine = Engine::open(config).await.unwrap();

        // Insert
        engine.insert(b"key1", b"value1").await.unwrap();
        engine.insert(b"key2", b"value2").await.unwrap();

        // Get
        assert_eq!(engine.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
        assert_eq!(engine.get(b"key2").await.unwrap(), Some(b"value2".to_vec()));
        assert_eq!(engine.get(b"key3").await.unwrap(), None);

        // Delete
        engine.delete(b"key1").await.unwrap();
        assert_eq!(engine.get(b"key1").await.unwrap(), None);

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_range_scan() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            data_dir: temp_dir.path().to_path_buf(),
            wal_enabled: false,
            ..Default::default()
        };

        let engine = Engine::open(config).await.unwrap();

        engine.insert(b"a", b"1").await.unwrap();
        engine.insert(b"b", b"2").await.unwrap();
        engine.insert(b"c", b"3").await.unwrap();
        engine.insert(b"d", b"4").await.unwrap();

        let range = engine.range(b"b", b"d").await.unwrap();
        assert_eq!(range.len(), 2);

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_write_stall_counters_trigger_at_threshold() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            data_dir: temp_dir.path().to_path_buf(),
            wal_enabled: false,
            max_l0_files_before_stall: 0,
            write_stall_micros: 1,
            ..Default::default()
        };

        let engine = Engine::open(config).await.unwrap();
        engine.insert(b"stall-key", b"stall-value").await.unwrap();

        let stats = engine.stats();
        assert_eq!(stats.write_stall_count, 1);
        assert_eq!(stats.write_stall_micros, 1);

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn tombstone_wins_across_sstable_generations_reopen_and_compaction() {
        let directory = TempDir::new().unwrap();
        let config = isolated_config(directory.path(), false);
        let engine = Engine::open(config.clone()).await.unwrap();

        engine.insert(b"scope:key", b"old").await.unwrap();
        engine.flush_write_buffers().unwrap();
        engine.flush().await.unwrap();

        engine.delete(b"scope:key").await.unwrap();
        assert_scope_key_absent(&engine).await;
        engine.flush().await.unwrap();

        for generation in 0..2 {
            let key = format!("unrelated:{generation}");
            engine.insert(key.as_bytes(), b"value").await.unwrap();
            engine.flush_write_buffers().unwrap();
            engine.flush().await.unwrap();
        }
        assert_eq!(engine.sstables.read().await.len(), 4);
        assert_scope_key_absent(&engine).await;

        engine.compact().await.unwrap();
        assert_eq!(engine.sstables.read().await.len(), 1);
        assert_scope_key_absent(&engine).await;
        let next_before_reopen = engine.memtable_manager.current_sequence();
        engine.shutdown().await.unwrap();
        drop(engine);

        let reopened = Engine::open(config).await.unwrap();
        assert_scope_key_absent(&reopened).await;
        assert!(reopened.memtable_manager.current_sequence() >= next_before_reopen);

        let new_sequence = reopened.memtable_manager.current_sequence();
        reopened.insert(b"scope:key", b"new").await.unwrap();
        reopened.flush_write_buffers().unwrap();
        assert_eq!(
            reopened
                .memtable_manager
                .get_entry(b"scope:key")
                .unwrap()
                .sequence,
            new_sequence
        );
        assert_eq!(
            reopened.get(b"scope:key").await.unwrap(),
            Some(b"new".to_vec())
        );
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn wal_replay_preserves_original_mutation_sequence() {
        let directory = TempDir::new().unwrap();
        let config = isolated_config(directory.path(), true);
        let engine = Engine::open(config.clone()).await.unwrap();

        engine.insert(b"replay:key", b"value").await.unwrap();
        engine.delete(b"replay:key").await.unwrap();
        let tombstone_sequence = engine
            .memtable_manager
            .get_entry(b"replay:key")
            .unwrap()
            .sequence;
        engine.wal.as_ref().unwrap().flush().await.unwrap();
        drop(engine);

        let reopened = Engine::open(config).await.unwrap();
        let replayed = reopened.memtable_manager.get_entry(b"replay:key").unwrap();
        assert_eq!(replayed.sequence, tombstone_sequence);
        assert!(replayed.is_tombstone());
        assert!(reopened.memtable_manager.current_sequence() > tombstone_sequence);
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_scans_observe_no_atomic_batch_prefix() {
        const ROUNDS: usize = 12;
        const KEYS_PER_BATCH: usize = 512;

        let directory = TempDir::new().unwrap();
        let engine = Arc::new(
            Engine::open(isolated_config(directory.path(), false))
                .await
                .unwrap(),
        );

        for round in 0..ROUNDS {
            let prefix = format!("atomic:{round:02}:").into_bytes();
            let end = format!("atomic:{round:02};").into_bytes();
            let mut batch = WriteBatch::with_capacity(KEYS_PER_BATCH);
            for key in 0..KEYS_PER_BATCH {
                batch.put(format!("atomic:{round:02}:{key:04}"), b"value");
            }

            let scanning = Arc::new(std::sync::atomic::AtomicBool::new(true));
            let scanner_started = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let scanner_engine = Arc::clone(&engine);
            let scanner_flag = Arc::clone(&scanning);
            let scanner_started_flag = Arc::clone(&scanner_started);
            let scan_start = prefix.clone();
            let scan_end = end.clone();
            let scanner = tokio::spawn(async move {
                let mut observations = 0;
                while scanner_flag.load(Ordering::Acquire) {
                    let count = scanner_engine
                        .range(&scan_start, &scan_end)
                        .await
                        .unwrap()
                        .len();
                    assert!(
                        count == 0 || count == KEYS_PER_BATCH,
                        "reader observed {count} operations from an atomic batch"
                    );
                    observations += 1;
                    scanner_started_flag.store(true, Ordering::Release);
                    tokio::task::yield_now().await;
                }
                observations
            });

            while !scanner_started.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
            engine.write_batch(&batch).await.unwrap();
            scanning.store(false, Ordering::Release);
            assert!(scanner.await.unwrap() > 0);
            assert_eq!(
                engine.scan_prefix(&prefix).await.unwrap().len(),
                KEYS_PER_BATCH
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn publication_gate_blocks_every_reader_shape_until_batch_is_complete() {
        let directory = TempDir::new().unwrap();
        let engine = Arc::new(
            Engine::open(isolated_config(directory.path(), false))
                .await
                .unwrap(),
        );
        let publication = engine.batch_visibility.write().await;
        let start = engine.memtable_manager.reserve_sequence_range(2);
        engine
            .memtable_manager
            .insert_with_sequence(b"gate:a", b"one", start)
            .unwrap();

        let started = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let point_engine = Arc::clone(&engine);
        let point_started = Arc::clone(&started);
        let point = tokio::spawn(async move {
            point_started.fetch_add(1, Ordering::AcqRel);
            point_engine.get(b"gate:a").await.unwrap()
        });
        let range_engine = Arc::clone(&engine);
        let range_started = Arc::clone(&started);
        let range = tokio::spawn(async move {
            range_started.fetch_add(1, Ordering::AcqRel);
            range_engine.range(b"gate:", b"gate;").await.unwrap()
        });
        let prefix_engine = Arc::clone(&engine);
        let prefix_started = Arc::clone(&started);
        let prefix = tokio::spawn(async move {
            prefix_started.fetch_add(1, Ordering::AcqRel);
            prefix_engine.scan_prefix(b"gate:").await.unwrap()
        });
        while started.load(Ordering::Acquire) != 3 {
            tokio::task::yield_now().await;
        }
        tokio::task::yield_now().await;
        assert!(!point.is_finished());
        assert!(!range.is_finished());
        assert!(!prefix.is_finished());

        engine
            .memtable_manager
            .insert_with_sequence(b"gate:b", b"two", start + 1)
            .unwrap();
        drop(publication);

        assert_eq!(point.await.unwrap(), Some(b"one".to_vec()));
        let expected = vec![
            (b"gate:a".to_vec(), b"one".to_vec()),
            (b"gate:b".to_vec(), b"two".to_vec()),
        ];
        assert_eq!(range.await.unwrap(), expected);
        assert_eq!(prefix.await.unwrap(), expected);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sstable_io_after_a_reader_snapshot_does_not_block_batch_publication() {
        let directory = TempDir::new().unwrap();
        let engine = Arc::new(
            Engine::open(isolated_config(directory.path(), false))
                .await
                .unwrap(),
        );
        let sstable_write = engine.sstables.write().await;
        let range_started = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let range_engine = Arc::clone(&engine);
        let started = Arc::clone(&range_started);
        let range = tokio::spawn(async move {
            started.store(true, Ordering::Release);
            range_engine.range(b"slow:", b"slow;").await.unwrap()
        });
        while !range_started.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!range.is_finished());

        let mut batch = WriteBatch::new();
        batch.put(b"slow:key", b"published");
        tokio::time::timeout(Duration::from_secs(1), engine.write_batch(&batch))
            .await
            .expect("batch publication waited for unrelated SSTable I/O")
            .unwrap();

        drop(sstable_write);
        assert!(range.await.unwrap().is_empty());
        assert_eq!(
            engine.get(b"slow:key").await.unwrap(),
            Some(b"published".to_vec())
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn reader_observes_old_state_while_paranoid_batch_waits_on_wal() {
        let directory = TempDir::new().unwrap();
        let engine = Arc::new(
            Engine::open(isolated_config(directory.path(), true))
                .await
                .unwrap(),
        );
        engine.insert(b"blocked:key", b"old").await.unwrap();
        let wal = Arc::clone(engine.wal.as_ref().unwrap());

        let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let lock_wal = Arc::clone(&wal);
        let lock_holder = tokio::task::spawn_blocking(move || {
            let file_guard = lock_wal.lock_current_file_for_test();
            let _ = locked_tx.send(());
            release_rx.recv().unwrap();
            drop(file_guard);
        });
        locked_rx.await.unwrap();

        let batch_engine = Arc::clone(&engine);
        let batch = tokio::spawn(async move {
            let mut batch = WriteBatch::new();
            batch.put(b"blocked:key", b"new");
            batch_engine.write_batch(&batch).await
        });
        while wal.current_sequence() < 2 {
            tokio::task::yield_now().await;
        }
        assert!(!batch.is_finished());
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), engine.get(b"blocked:key"))
                .await
                .expect("reader waited for a batch WAL fsync")
                .unwrap(),
            Some(b"old".to_vec())
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), engine.scan_prefix(b"blocked:"))
                .await
                .expect("scan waited for a batch WAL fsync")
                .unwrap(),
            vec![(b"blocked:key".to_vec(), b"old".to_vec())]
        );

        release_tx.send(()).unwrap();
        lock_holder.await.unwrap();
        batch.await.unwrap().unwrap();
        assert_eq!(
            engine.get(b"blocked:key").await.unwrap(),
            Some(b"new".to_vec())
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn queued_group_commit_retains_directory_ownership_after_engine_drop() {
        let directory = TempDir::new().unwrap();
        let mut config = isolated_config(directory.path(), true);
        config.wal_config = WalConfig::paranoid();
        let engine = Arc::new(Engine::open(config.clone()).await.unwrap());
        let wal = Arc::clone(engine.wal.as_ref().unwrap());

        let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let lock_wal = Arc::clone(&wal);
        let lock_holder = tokio::task::spawn_blocking(move || {
            let file_guard = lock_wal.lock_current_file_for_test();
            let _ = locked_tx.send(());
            release_rx.recv().unwrap();
            drop(file_guard);
        });
        locked_rx.await.unwrap();

        let queued_engine = Arc::clone(&engine);
        let queued =
            tokio::spawn(async move { queued_engine.insert(b"cancelled:key", b"value").await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while wal.group_commit_in_progress_for_test() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("queued group commit did not start");

        queued.abort();
        assert!(queued.await.unwrap_err().is_cancelled());
        drop(engine);

        let error = match Engine::open(config.clone()).await {
            Err(error) => error,
            Ok(_) => panic!("queued WAL mutation unexpectedly released directory ownership"),
        };
        assert!(matches!(error, StorageError::DirectoryLocked { .. }));

        release_tx.send(()).unwrap();
        lock_holder.await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while wal.group_commit_in_progress_for_test() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("queued group commit did not finish");
        drop(wal);

        let reopened = Engine::open(config).await.unwrap();
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fast_read_drain_waits_for_an_inflight_buffered_insert() {
        let directory = TempDir::new().unwrap();
        let engine = Arc::new(
            Engine::open(isolated_config(directory.path(), false))
                .await
                .unwrap(),
        );
        let inflight_writer = engine.mutation_barrier.read().await;
        let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let reader_engine = Arc::clone(&engine);
        let reader_started = Arc::clone(&started);
        let reader = tokio::spawn(async move {
            reader_started.store(true, Ordering::Release);
            reader_engine.get(b"drain:key").await.unwrap()
        });
        while !started.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        tokio::task::yield_now().await;
        assert!(!reader.is_finished());

        engine
            .memtable_manager
            .insert_buffered(b"drain:key", b"visible")
            .unwrap();
        drop(inflight_writer);

        assert_eq!(reader.await.unwrap(), Some(b"visible".to_vec()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fast_flush_drains_a_buffered_insert_before_its_freeze_point() {
        let directory = TempDir::new().unwrap();
        let config = isolated_config(directory.path(), false);
        let engine = Arc::new(Engine::open(config.clone()).await.unwrap());
        let inflight_writer = engine.mutation_barrier.read().await;
        let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flush_engine = Arc::clone(&engine);
        let flush_started = Arc::clone(&started);
        let flush = tokio::spawn(async move {
            flush_started.store(true, Ordering::Release);
            flush_engine.flush().await
        });
        while !started.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        tokio::task::yield_now().await;
        assert!(!flush.is_finished());

        engine
            .memtable_manager
            .insert_buffered(b"flush-gap:key", b"persisted")
            .unwrap();
        drop(inflight_writer);
        flush.await.unwrap().unwrap();
        drop(engine);

        let reopened = Engine::open(config).await.unwrap();
        assert_eq!(
            reopened.get(b"flush-gap:key").await.unwrap(),
            Some(b"persisted".to_vec())
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn same_key_batch_order_survives_wal_only_recovery() {
        let directory = TempDir::new().unwrap();
        let config = isolated_config(directory.path(), true);
        let engine = Engine::open(config.clone()).await.unwrap();
        let mut batch = WriteBatch::new();
        batch.put(b"same:key", b"first");
        batch.delete(b"same:key");
        batch.put(b"same:key", b"last");

        engine.write_batch(&batch).await.unwrap();
        assert_eq!(
            engine.get(b"same:key").await.unwrap(),
            Some(b"last".to_vec())
        );
        let final_sequence = engine
            .memtable_manager
            .get_entry(b"same:key")
            .unwrap()
            .sequence;
        engine.wal.as_ref().unwrap().flush().await.unwrap();
        drop(engine);

        let reopened = Engine::open(config).await.unwrap();
        let entry = reopened.memtable_manager.get_entry(b"same:key").unwrap();
        assert_eq!(entry.value, Some(b"last".to_vec()));
        assert_eq!(entry.sequence, final_sequence);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn complete_wal_batch_recovers_after_prepublication_failure() {
        let directory = TempDir::new().unwrap();
        let config = isolated_config(directory.path(), true);
        let engine = Engine::open(config.clone()).await.unwrap();
        let failure = super::super::failpoints::arm(
            directory.path(),
            super::super::failpoints::PersistenceBoundary::Wal,
        );
        let mut batch = WriteBatch::new();
        batch.put(b"failure:a", b"one");
        batch.put(b"failure:b", b"two");

        assert!(engine.write_batch(&batch).await.is_err());
        failure.assert_hit();
        assert!(engine
            .range(b"failure:", b"failure;")
            .await
            .unwrap()
            .is_empty());
        engine.wal.as_ref().unwrap().flush().await.unwrap();
        drop(engine);

        let reopened = Engine::open(config).await.unwrap();
        assert_eq!(
            reopened.range(b"failure:", b"failure;").await.unwrap(),
            vec![
                (b"failure:a".to_vec(), b"one".to_vec()),
                (b"failure:b".to_vec(), b"two".to_vec()),
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancelled_durable_batch_stays_recoverable_before_publication() {
        let directory = TempDir::new().unwrap();
        let config = isolated_config(directory.path(), true);
        let engine = Arc::new(Engine::open(config.clone()).await.unwrap());
        let publication_reader = engine.batch_visibility.read().await;
        let batch_engine = Arc::clone(&engine);
        let batch = tokio::spawn(async move {
            let mut batch = WriteBatch::new();
            batch.put(b"cancel:a", b"one");
            batch.put(b"cancel:b", b"two");
            batch_engine.write_batch(&batch).await
        });
        let wal = engine.wal.as_ref().unwrap();
        while wal.current_sequence() < 2 || wal.align_checkpoint(1) != 0 {
            tokio::task::yield_now().await;
        }
        assert!(!batch.is_finished());

        batch.abort();
        assert!(batch.await.unwrap_err().is_cancelled());
        drop(publication_reader);
        assert!(engine
            .range(b"cancel:", b"cancel;")
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            engine
                .unapplied_wal_sequences
                .lock()
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            [0]
        );

        engine.insert(b"later:key", b"later").await.unwrap();
        engine.flush().await.unwrap();
        assert_eq!(
            Manifest::load_or_create(directory.path())
                .unwrap()
                .wal_checkpoint,
            0
        );
        drop(engine);

        let reopened = Engine::open(config).await.unwrap();
        assert_eq!(
            reopened.range(b"cancel:", b"cancel;").await.unwrap(),
            vec![
                (b"cancel:a".to_vec(), b"one".to_vec()),
                (b"cancel:b".to_vec(), b"two".to_vec()),
            ]
        );
        assert_eq!(
            reopened.get(b"later:key").await.unwrap(),
            Some(b"later".to_vec())
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn flush_checkpoint_never_splits_a_reserved_batch_span() {
        let directory = TempDir::new().unwrap();
        let config = isolated_config(directory.path(), true);
        let engine = Engine::open(config.clone()).await.unwrap();
        let wal = engine.wal.as_ref().unwrap();
        let sequences = wal
            .append_batch(&[
                (b"span:a".as_slice(), Some(b"one".as_slice())),
                (b"span:b".as_slice(), Some(b"two".as_slice())),
                (b"span:c".as_slice(), Some(b"three".as_slice())),
            ])
            .await
            .unwrap();
        for (index, sequence) in sequences.iter().copied().enumerate() {
            engine
                .memtable_manager
                .insert_with_sequence(
                    format!("span:{}", (b'a' + index as u8) as char).as_bytes(),
                    match index {
                        0 => b"one",
                        1 => b"two",
                        _ => b"three",
                    },
                    sequence,
                )
                .unwrap();
            engine.memtable_manager.force_rotate().unwrap();
        }
        let failure = super::super::failpoints::arm_on_hit(
            directory.path(),
            super::super::failpoints::PersistenceBoundary::ManifestInstallation,
            2,
        );

        assert!(engine.flush().await.is_err());
        failure.assert_hit();
        assert_eq!(
            Manifest::load_or_create(directory.path())
                .unwrap()
                .wal_checkpoint,
            sequences[0]
        );
        drop(engine);

        let reopened = Engine::open(config).await.unwrap();
        assert_eq!(reopened.scan_prefix(b"span:").await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn legacy_generations_use_table_order_until_versioned_writes_begin() {
        let directory = TempDir::new().unwrap();
        let sstable_directory = directory.path().join("sstables");
        std::fs::create_dir_all(&sstable_directory).unwrap();

        let old_value =
            write_legacy_table(&sstable_directory.join("old.sst"), 1, Some(b"old"), 100);
        // Older versions reset sequences per memtable, so this newer table can
        // legitimately have a lower manifest maximum.
        let newer_tombstone = write_legacy_table(&sstable_directory.join("new.sst"), 2, None, 0);
        let mut manifest = Manifest::new();
        manifest.sstables = vec![old_value, newer_tombstone];
        manifest.save(directory.path()).unwrap();

        let engine = Engine::open(isolated_config(directory.path(), false))
            .await
            .unwrap();
        assert_scope_key_absent(&engine).await;

        let expected_sequence = engine.memtable_manager.current_sequence();
        assert_eq!(expected_sequence, 101);
        engine.insert(b"scope:key", b"upgraded").await.unwrap();
        engine.flush_write_buffers().unwrap();
        assert_eq!(
            engine
                .memtable_manager
                .get_entry(b"scope:key")
                .unwrap()
                .sequence,
            expected_sequence
        );
        assert_eq!(
            engine.get(b"scope:key").await.unwrap(),
            Some(b"upgraded".to_vec())
        );
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn streamed_winners_match_point_reads_across_legacy_v3_and_memory_sources() {
        use std::collections::BTreeMap;

        let directory = TempDir::new().unwrap();
        let sstable_directory = directory.path().join("sstables");
        std::fs::create_dir_all(&sstable_directory).unwrap();
        let legacy_old = write_legacy_scan_table(
            &sstable_directory.join("legacy-old.sst"),
            1,
            &[
                (b"scan:a", Some(b"legacy-a")),
                (b"scan:b", Some(b"legacy-b")),
                (b"scan:c", Some(b"legacy-c-old")),
            ],
            10,
        );
        let legacy_new = write_legacy_scan_table(
            &sstable_directory.join("legacy-new.sst"),
            2,
            &[
                (b"scan:a", None),
                (b"scan:c", Some(b"legacy-c-new")),
                (b"scan:d", Some(b"legacy-d")),
            ],
            20,
        );
        let mut manifest = Manifest::new();
        manifest.sstables = vec![legacy_old, legacy_new];
        manifest.save(directory.path()).unwrap();

        let engine = Engine::open(isolated_config(directory.path(), true))
            .await
            .unwrap();
        engine.insert(b"scan:a", b"memory-a").await.unwrap();
        engine.insert(b"scan:b", b"v3-b").await.unwrap();
        engine.flush().await.unwrap();
        engine.delete(b"scan:b").await.unwrap();
        engine.insert(b"scan:e", b"memory-e").await.unwrap();

        let streamed: BTreeMap<_, _> = engine
            .scan_prefix_iter(b"scan:")
            .await
            .unwrap()
            .map(|entry| entry.unwrap().into_pair())
            .collect();
        for key in [b"scan:a", b"scan:b", b"scan:c", b"scan:d", b"scan:e"] {
            assert_eq!(
                streamed.get(key.as_slice()).cloned(),
                engine.get(key).await.unwrap()
            );
        }
        assert_eq!(
            streamed.get(b"scan:a".as_slice()),
            Some(&b"memory-a".to_vec())
        );
        assert!(!streamed.contains_key(b"scan:b".as_slice()));
        assert_eq!(
            streamed.get(b"scan:c".as_slice()),
            Some(&b"legacy-c-new".to_vec())
        );
    }

    #[tokio::test]
    async fn exact_memory_sequence_ties_match_point_read_source_order() {
        let directory = TempDir::new().unwrap();
        let engine = Engine::open(isolated_config(directory.path(), true))
            .await
            .unwrap();
        engine
            .memtable_manager
            .insert_with_sequence(b"tie:key", b"first", 7)
            .unwrap();
        engine.memtable_manager.force_rotate().unwrap();
        engine
            .memtable_manager
            .insert_with_sequence(b"tie:key", b"second", 7)
            .unwrap();
        engine.memtable_manager.force_rotate().unwrap();

        assert_eq!(
            engine.get(b"tie:key").await.unwrap(),
            Some(b"second".to_vec())
        );
        assert_eq!(
            engine.scan_prefix(b"tie:").await.unwrap(),
            vec![(b"tie:key".to_vec(), b"second".to_vec())]
        );
    }

    #[tokio::test]
    async fn scan_freeze_preserves_newest_memory_generation_on_exact_sequence_ties() {
        let directory = TempDir::new().unwrap();
        let engine = Engine::open(isolated_config(directory.path(), true))
            .await
            .unwrap();
        engine
            .memtable_manager
            .insert_with_sequence(b"freeze:value", b"old", 7)
            .unwrap();
        engine
            .memtable_manager
            .insert_with_sequence(b"freeze:deleted", b"old", 8)
            .unwrap();
        engine.memtable_manager.force_rotate().unwrap();
        engine
            .memtable_manager
            .insert_with_sequence(b"freeze:value", b"new", 7)
            .unwrap();
        engine
            .memtable_manager
            .delete_with_sequence(b"freeze:deleted", 8)
            .unwrap();

        assert_eq!(
            engine.get(b"freeze:value").await.unwrap(),
            Some(b"new".to_vec())
        );
        assert_eq!(engine.get(b"freeze:deleted").await.unwrap(), None);

        let prefix = engine.scan_prefix_iter(b"freeze:").await.unwrap();
        let range = engine.range_iter(b"freeze:", b"freeze;").await.unwrap();
        let expected = vec![(b"freeze:value".to_vec(), b"new".to_vec())];
        assert_eq!(prefix.collect_pairs().unwrap(), expected);
        assert_eq!(range.collect_pairs().unwrap(), expected);
        assert_eq!(
            engine.get(b"freeze:value").await.unwrap(),
            Some(b"new".to_vec())
        );
        assert_eq!(engine.get(b"freeze:deleted").await.unwrap(), None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn point_reads_do_not_miss_the_generation_moving_during_scan_freeze() {
        let directory = TempDir::new().unwrap();
        let engine = Engine::open(isolated_config(directory.path(), true))
            .await
            .unwrap();

        for generation in 0..32_u8 {
            let value = vec![generation];
            engine
                .memtable_manager
                .insert_with_sequence(b"handoff-tie", &value, 7)
                .unwrap();
            let (point, scan) = tokio::time::timeout(Duration::from_secs(1), async {
                tokio::join!(
                    engine.get(b"handoff-tie"),
                    engine.scan_prefix(b"handoff-tie")
                )
            })
            .await
            .expect("point read deadlocked with scan freeze");
            assert_eq!(point.unwrap(), Some(value.clone()));
            assert_eq!(
                scan.unwrap(),
                vec![(b"handoff-tie".to_vec(), value.clone())]
            );
            assert_eq!(engine.get(b"handoff-tie").await.unwrap(), Some(value));
        }
    }

    #[tokio::test]
    async fn empty_and_terminal_binary_prefixes_and_range_bounds_cross_sources() {
        let directory = TempDir::new().unwrap();
        let engine = Engine::open(isolated_config(directory.path(), false))
            .await
            .unwrap();
        for (key, value) in [
            (b"".as_slice(), b"empty".as_slice()),
            (&[0x00], b"zero"),
            (&[0x7f], b"middle"),
            (&[0xff], b"ff"),
        ] {
            engine.insert(key, value).await.unwrap();
        }
        engine.flush().await.unwrap();
        engine.insert(&[0xff, 0x00], b"ff-zero").await.unwrap();
        engine.insert(&[0xff, 0xff], b"ff-ff").await.unwrap();

        assert_eq!(engine.scan_prefix(b"").await.unwrap().len(), 6);
        assert_eq!(
            engine.scan_prefix(&[0xff]).await.unwrap(),
            vec![
                (vec![0xff], b"ff".to_vec()),
                (vec![0xff, 0x00], b"ff-zero".to_vec()),
                (vec![0xff, 0xff], b"ff-ff".to_vec()),
            ]
        );
        assert_eq!(
            engine.scan_prefix(&[0xff, 0xff]).await.unwrap(),
            vec![(vec![0xff, 0xff], b"ff-ff".to_vec())]
        );
        assert_eq!(
            engine.range(&[0x00], &[0xff]).await.unwrap(),
            vec![
                (vec![0x00], b"zero".to_vec()),
                (vec![0x7f], b"middle".to_vec()),
            ]
        );
        assert!(engine.range(b"same", b"same").await.unwrap().is_empty());
        assert!(engine.range(b"z", b"a").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn guard_values_are_lazy_and_scan_creation_records_its_freeze_cost() {
        let directory = TempDir::new().unwrap();
        let engine = Engine::open(isolated_config(directory.path(), true))
            .await
            .unwrap();
        engine.insert(b"lazy:key", b"large-value").await.unwrap();
        assert_eq!(engine.memtable_manager.immutable_count(), 0);

        let mut iterator = engine.range_iter(b"lazy:", b"lazy;").await.unwrap();
        assert_eq!(engine.memtable_manager.immutable_count(), 1);
        let guard = iterator.next().unwrap().unwrap();
        assert!(!guard.value_loaded_for_test());
        assert_eq!(guard.key(), b"lazy:key");
        assert!(!guard.value_loaded_for_test());
        assert_eq!(guard.value(), b"large-value");
        assert!(guard.value_loaded_for_test());
        assert!(iterator.next().is_none());
    }

    #[tokio::test]
    async fn large_stream_retains_only_source_heads_and_one_block_per_table() {
        let directory = TempDir::new().unwrap();
        let mut config = isolated_config(directory.path(), false);
        config.block_cache_size = 0;
        config.sstable_config = SSTableConfig {
            block_size: 1024,
            compression: super::super::sstable::CompressionType::None,
            ..SSTableConfig::default()
        };
        let engine = Engine::open(config).await.unwrap();
        const KEYS: usize = 2_000;
        const GENERATIONS: usize = 3;
        for generation in 0..GENERATIONS {
            for key in 0..KEYS {
                engine
                    .insert(
                        format!("large:{key:05}").as_bytes(),
                        format!("generation-{generation:02}-{:064}", key).as_bytes(),
                    )
                    .await
                    .unwrap();
            }
            engine.flush().await.unwrap();
        }

        let mut iterator = engine.range_iter(b"large:", b"large;").await.unwrap();
        let mut count = 0;
        while let Some(entry) = iterator.next() {
            let key = entry.unwrap().into_key();
            assert!(key.starts_with(b"large:"));
            count += 1;
            if count % 127 == 0 {
                let (sources, heads, blocks, bytes) = iterator.working_set_for_test();
                assert_eq!(sources, GENERATIONS);
                assert!(heads <= sources);
                assert!(blocks <= GENERATIONS);
                assert!(bytes <= GENERATIONS * 2048);
            }
        }
        assert_eq!(count, KEYS);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pinned_readers_survive_compaction_unlink_without_holding_the_live_list_lock() {
        let directory = TempDir::new().unwrap();
        let mut config = isolated_config(directory.path(), false);
        config.block_cache_size = 0;
        config.compaction_config.l0_compaction_trigger = 2;
        config.sstable_config = SSTableConfig {
            block_size: 256,
            compression: super::super::sstable::CompressionType::None,
            ..SSTableConfig::default()
        };
        let engine = Engine::open(config).await.unwrap();
        for generation in 0..2 {
            for key in 0..200 {
                engine
                    .insert(
                        format!("pin:{key:04}").as_bytes(),
                        format!("value-{generation}-{key:04}").as_bytes(),
                    )
                    .await
                    .unwrap();
            }
            engine.flush().await.unwrap();
        }
        let input_paths: Vec<_> = engine
            .sstables
            .read()
            .await
            .iter()
            .map(|table| table.path.clone())
            .collect();
        let iterator = engine.scan_prefix_iter(b"pin:").await.unwrap();

        tokio::time::timeout(Duration::from_secs(2), engine.compact())
            .await
            .expect("iterator retained the global SSTable list lock")
            .unwrap();
        assert!(input_paths.iter().all(|path| !path.exists()));
        let pairs = iterator.collect_pairs().unwrap();
        assert_eq!(pairs.len(), 200);
        assert!(pairs
            .iter()
            .all(|(_, value)| value.starts_with(b"value-1-")));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn source_capture_never_falls_through_the_flush_handoff() {
        let directory = TempDir::new().unwrap();
        let engine = Arc::new(
            Engine::open(isolated_config(directory.path(), true))
                .await
                .unwrap(),
        );
        for round in 0..12 {
            for key in 0..64 {
                engine
                    .insert(
                        format!("handoff:{round:02}:{key:02}").as_bytes(),
                        b"visible",
                    )
                    .await
                    .unwrap();
            }
            let flush_engine = Arc::clone(&engine);
            let scan_engine = Arc::clone(&engine);
            let prefix = format!("handoff:{round:02}:").into_bytes();
            let (flush, scan) =
                tokio::join!(async move { flush_engine.flush().await }, async move {
                    scan_engine.scan_prefix(&prefix).await
                });
            flush.unwrap();
            assert_eq!(scan.unwrap().len(), 64);
        }
    }

    #[tokio::test]
    async fn iterator_retains_sources_and_directory_ownership_after_engine_drop() {
        let directory = TempDir::new().unwrap();
        let config = isolated_config(directory.path(), false);
        let engine = Engine::open(config.clone()).await.unwrap();
        engine.insert(b"owned:key", b"value").await.unwrap();
        engine.flush().await.unwrap();
        let iterator = engine.scan_prefix_iter(b"owned:").await.unwrap();
        drop(engine);

        assert!(matches!(
            Engine::open(config.clone()).await,
            Err(StorageError::DirectoryLocked { .. })
        ));
        assert_eq!(
            iterator.collect_pairs().unwrap(),
            vec![(b"owned:key".to_vec(), b"value".to_vec())]
        );
        let reopened = Engine::open(config).await.unwrap();
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fast_batch_queued_behind_a_scan_freeze_is_seen_only_when_complete() {
        let directory = TempDir::new().unwrap();
        let engine = Arc::new(
            Engine::open(isolated_config(directory.path(), false))
                .await
                .unwrap(),
        );
        let mutation = engine.mutation_barrier.write().await;
        let batch_engine = Arc::clone(&engine);
        let batch = tokio::spawn(async move {
            let mut batch = WriteBatch::new();
            batch.put(b"queued:a", b"one");
            batch.put(b"queued:b", b"two");
            batch_engine.write_batch(&batch).await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if engine.batch_visibility.try_read().is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("fast batch did not acquire its publication gate");
        let scan_engine = Arc::clone(&engine);
        let scan = tokio::spawn(async move { scan_engine.scan_prefix(b"queued:").await });
        drop(mutation);

        batch.await.unwrap().unwrap();
        assert_eq!(scan.await.unwrap().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn range_iterator_reports_a_late_block_error_once_then_fuses() {
        use std::fs::OpenOptions;
        use std::io::{Read, Seek, SeekFrom, Write};

        let directory = TempDir::new().unwrap();
        let mut config = isolated_config(directory.path(), false);
        config.block_cache_size = 0;
        config.sstable_config = SSTableConfig {
            block_size: 40,
            compression: super::super::sstable::CompressionType::None,
            ..SSTableConfig::default()
        };
        let engine = Engine::open(config).await.unwrap();
        engine.insert(b"error:a", b"first-value").await.unwrap();
        engine.insert(b"error:b", b"second-value").await.unwrap();
        engine.flush().await.unwrap();
        let mut iterator = engine.scan_prefix_iter(b"error:").await.unwrap();

        let path = engine.sstables.read().await[0].path.clone();
        let reader = SSTableReader::open(&path).unwrap();
        assert!(reader.index().entries().len() >= 2);
        let offset = reader.index().entries()[1].block_offset;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        file.seek(SeekFrom::Start(offset)).unwrap();
        let mut byte = [0_u8; 1];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 0x80;
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&byte).unwrap();
        file.sync_all().unwrap();

        assert_eq!(
            iterator.next().unwrap().unwrap().into_key(),
            b"error:a".to_vec()
        );
        assert!(iterator.next().unwrap().is_err());
        assert!(iterator.next().is_none());
        assert!(iterator.next().is_none());
    }
}
