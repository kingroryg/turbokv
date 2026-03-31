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

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::RwLock;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{error, info};

use crate::core::{CompactionResult, DbConfig, StorageStats, WriteBatch};

use super::{
    compaction::{CompactionConfig, Compactor},
    fd::{FdConfig, FdMonitor, SSTablePool},
    manifest::{Manifest, SSTableManifestEntry},
    memtable::{MemTableConfig, MemTableManager},
    sstable::{SSTableConfig, SSTableInfo, SSTableWriter},
    wal::{WalConfig, WriteAheadLog},
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
    wal: Option<Arc<WriteAheadLog>>,
    memtable_manager: Arc<MemTableManager>,
    sstables: Arc<RwLock<Vec<SSTableInfo>>>,
    manifest: Arc<Mutex<Manifest>>,
    shutdown: tokio::sync::watch::Sender<bool>,
    next_sstable_id: Arc<std::sync::atomic::AtomicU64>,
    sstable_pool: Arc<SSTablePool>,
    #[allow(dead_code)]
    fd_monitor: Arc<FdMonitor>,
    compactor: Arc<Compactor>,
    // Atomic counters for SSTable stats (updated on flush/compaction)
    sstable_total_keys: Arc<AtomicU64>,
    sstable_total_bytes: Arc<AtomicU64>,
    sstable_count: Arc<AtomicU64>,
}

impl Engine {
    /// Open or create a storage engine
    pub async fn open(config: StorageConfig) -> Result<Self> {
        // Initialize cached timestamp
        super::cached_time::init();

        // Create directories
        tokio::fs::create_dir_all(&config.data_dir).await?;
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

        let next_sstable_id = manifest.sstables.iter().map(|s| s.id).max().unwrap_or(0) + 1;

        info!(
            "Opening database: wal_checkpoint={}, sstables={}",
            wal_checkpoint,
            manifest.sstables.len()
        );

        // Create WAL if enabled
        let wal = if config.wal_enabled {
            Some(Arc::new(
                WriteAheadLog::new(&wal_dir, config.wal_config.clone()).await?,
            ))
        } else {
            None
        };

        // Create memtable manager
        let memtable_manager = Arc::new(MemTableManager::new(config.memtable_config.clone()));

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

        let engine = Self {
            config,
            wal,
            memtable_manager,
            sstables: Arc::new(RwLock::new(sstables)),
            manifest: Arc::new(Mutex::new(manifest)),
            shutdown: shutdown_tx,
            next_sstable_id,
            sstable_pool,
            fd_monitor,
            compactor,
            sstable_total_keys: Arc::new(AtomicU64::new(initial_sstable_keys)),
            sstable_total_bytes: Arc::new(AtomicU64::new(initial_sstable_bytes)),
            sstable_count: Arc::new(AtomicU64::new(initial_sstable_count)),
        };

        // Start background tasks
        engine.start_background_tasks();

        Ok(engine)
    }

    /// Insert a key-value pair
    ///
    /// Automatically uses optimal path based on configuration:
    /// - No WAL: Sync path with thread-local buffering (faster)
    /// - With WAL: Async path for durability
    pub async fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if let Some(ref wal) = self.wal {
            // Durable path: WAL write then memtable
            wal.append(key, value).await?;
            self.memtable_manager
                .insert(key, value)
                .map_err(StorageError::MemTable)?;
        } else {
            // Fast path: thread-local buffered insert, no async overhead
            self.memtable_manager
                .insert_buffered(key, value)
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
        // Write to WAL first (if enabled)
        if let Some(ref wal) = self.wal {
            wal.append_delete(key).await?;
        }

        // Insert tombstone into memtable
        self.memtable_manager
            .delete(key)
            .map_err(StorageError::MemTable)?;

        Ok(())
    }

    /// Get a value by key
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // In fast mode, flush thread-local buffer to ensure visibility
        if self.wal.is_none() {
            self.memtable_manager
                .flush_thread_local()
                .map_err(StorageError::MemTable)?;
        }

        // Check memtable first
        if let Some(value) = self.memtable_manager.get(key) {
            return Ok(Some(value));
        }

        // Check if memtable has a tombstone
        if let Some(entry) = self.memtable_manager.get_entry(key) {
            if entry.is_tombstone() {
                return Ok(None);
            }
        }

        // Search SSTables (newest to oldest)
        let sstables = self.sstables.read().await;
        for sst in sstables.iter().rev() {
            if let Some(value) = self.read_from_sstable(sst, key).await? {
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    /// Check if a key exists
    pub async fn contains_key(&self, key: &[u8]) -> Result<bool> {
        Ok(self.get(key).await?.is_some())
    }

    /// Scan a range of keys
    pub async fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        use std::collections::BTreeMap;

        // In fast mode, flush thread-local buffer to ensure visibility
        if self.wal.is_none() {
            self.memtable_manager
                .flush_thread_local()
                .map_err(StorageError::MemTable)?;
        }

        let mut merged: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();

        // Get from SSTables first (older data)
        let sstables = self.sstables.read().await;
        for sst in sstables.iter() {
            let entries = self.range_from_sstable(sst, start, end).await?;
            for (key, value) in entries {
                merged.insert(key, value);
            }
        }
        drop(sstables);

        // Get from memtable last (newer data overrides SSTables)
        for (key, value) in self.memtable_manager.range(start, end) {
            merged.insert(key, Some(value));
        }

        // Filter out tombstones
        Ok(merged
            .into_iter()
            .filter_map(|(k, v)| v.map(|val| (k, val)))
            .collect())
    }

    /// Scan all keys with a given prefix
    pub async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        use std::collections::BTreeMap;

        // In fast mode, flush thread-local buffer to ensure visibility
        if self.wal.is_none() {
            self.memtable_manager
                .flush_thread_local()
                .map_err(StorageError::MemTable)?;
        }

        let mut merged: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();

        // Get from SSTables first (older data)
        let sstables = self.sstables.read().await;
        for sst in sstables.iter() {
            let entries = self.prefix_from_sstable(sst, prefix).await?;
            for (key, value) in entries {
                merged.insert(key, value);
            }
        }
        drop(sstables);

        // Get from memtable last (newer data overrides SSTables)
        for (key, value) in self.memtable_manager.scan_prefix(prefix) {
            merged.insert(key, Some(value));
        }

        // Filter out tombstones
        Ok(merged
            .into_iter()
            .filter_map(|(k, v)| v.map(|val| (k, val)))
            .collect())
    }

    /// Scan a range of keys, returning a guard iterator for lazy value access.
    ///
    /// This allows filtering by key without loading values until needed.
    pub async fn range_iter(&self, start: &[u8], end: &[u8]) -> Result<super::iter::RangeIter> {
        let entries = self.range(start, end).await?;
        Ok(super::iter::RangeIter::new(entries))
    }

    /// Scan keys with a prefix, returning a guard iterator for lazy value access.
    ///
    /// This allows filtering by key without loading values until needed.
    pub async fn scan_prefix_iter(&self, prefix: &[u8]) -> Result<super::iter::PrefixIter> {
        let entries = self.scan_prefix(prefix).await?;
        Ok(super::iter::RangeIter::new(entries))
    }

    /// Write a batch of operations atomically
    pub async fn write_batch(&self, batch: &WriteBatch) -> Result<()> {
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

        // Write to WAL (if enabled)
        if let Some(ref wal) = self.wal {
            wal.append_batch(&ops).await?;
        }

        // Apply to memtable
        for op in batch.ops() {
            match op {
                crate::core::BatchOp::Put { key, value } => {
                    self.memtable_manager
                        .insert(key, value)
                        .map_err(StorageError::MemTable)?;
                }
                crate::core::BatchOp::Delete { key } => {
                    self.memtable_manager
                        .delete(key)
                        .map_err(StorageError::MemTable)?;
                }
            }
        }

        Ok(())
    }

    /// Flush memtable to SSTable
    pub async fn flush(&self) -> Result<()> {
        // Force rotate memtable
        self.memtable_manager
            .force_rotate()
            .map_err(StorageError::MemTable)?;

        // Flush all immutable memtables
        while let Some(memtable) = self.memtable_manager.get_immutable_for_flush() {
            self.flush_memtable_to_sstable(&memtable).await?;
        }

        // Flush WAL
        if let Some(ref wal) = self.wal {
            wal.flush().await?;
        }

        Ok(())
    }

    /// Trigger compaction
    pub async fn compact(&self) -> Result<CompactionResult> {
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
                min_sequence: 0,
                max_sequence: 0,
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
        }
    }

    /// Shutdown the engine gracefully
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down storage engine");

        // Signal background tasks to stop
        let _ = self.shutdown.send(true);

        // Flush pending writes
        self.flush().await?;

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
                        memtable_manager.insert(key, v).map_err(StorageError::MemTable)?;
                    }
                    None => {
                        memtable_manager.delete(key).map_err(StorageError::MemTable)?;
                    }
                }
                count += 1;
            }
        }

        Ok(count)
    }

    /// Read a key from an SSTable
    async fn read_from_sstable(&self, sst: &SSTableInfo, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Check bloom filter first (if available)
        // Then search the SSTable

        let reader = self
            .sstable_pool
            .get(&sst.path)
            .map_err(|e| StorageError::SSTable(e.to_string()))?;

        match reader.get(key) {
            Ok(Some(value)) => {
                if value.is_empty() {
                    // Empty value = tombstone in SSTable
                    Ok(None)
                } else {
                    Ok(Some(value.to_vec()))
                }
            }
            Ok(None) => Ok(None),
            Err(e) => Err(StorageError::SSTable(e.to_string())),
        }
    }

    /// Range scan from an SSTable
    async fn range_from_sstable(
        &self,
        sst: &SSTableInfo,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
        // Skip SSTable if its key range doesn't overlap with query range
        if !sst.min_key.is_empty() && sst.min_key.as_slice() >= end {
            return Ok(Vec::new());
        }
        if !sst.max_key.is_empty() && sst.max_key.as_slice() < start {
            return Ok(Vec::new());
        }

        let reader = self
            .sstable_pool
            .get(&sst.path)
            .map_err(|e| StorageError::SSTable(e.to_string()))?;

        // Use iterator to scan range
        let mut results = Vec::new();
        for entry in reader.iter() {
            let (key, value) = entry.map_err(|e| StorageError::SSTable(e.to_string()))?;
            if key.as_ref() >= start && key.as_ref() < end {
                // Empty value means tombstone
                let val = if value.is_empty() {
                    None
                } else {
                    Some(value.to_vec())
                };
                results.push((key.to_vec(), val));
            } else if key.as_ref() >= end {
                break;
            }
        }
        Ok(results)
    }

    /// Prefix scan from an SSTable
    async fn prefix_from_sstable(
        &self,
        sst: &SSTableInfo,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
        // Skip SSTable if all its keys are before the prefix range
        if !sst.max_key.is_empty() && sst.max_key.as_slice() < prefix {
            return Ok(Vec::new());
        }

        let reader = self
            .sstable_pool
            .get(&sst.path)
            .map_err(|e| StorageError::SSTable(e.to_string()))?;

        // Use iterator to scan prefix
        let mut results = Vec::new();
        for entry in reader.iter() {
            let (key, value) = entry.map_err(|e| StorageError::SSTable(e.to_string()))?;
            if key.starts_with(prefix) {
                // Empty value means tombstone
                let val = if value.is_empty() {
                    None
                } else {
                    Some(value.to_vec())
                };
                results.push((key.to_vec(), val));
            } else if key.as_ref() > prefix && !key.starts_with(prefix) {
                // Past the prefix range, can stop
                break;
            }
        }
        Ok(results)
    }

    /// Flush a memtable to SSTable
    async fn flush_memtable_to_sstable(
        &self,
        memtable: &super::memtable::MemTable,
    ) -> Result<SSTableInfo> {
        let id = self
            .next_sstable_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let path = self
            .config
            .data_dir
            .join("sstables")
            .join("L0")
            .join(format!("{:010}.sst", id));

        // Get all entries from memtable
        let entries = memtable.get_all_kv();

        // Write SSTable
        let mut writer =
            SSTableWriter::new(&path, self.config.sstable_config.clone()).map_err(|e| {
                StorageError::SSTable(format!("Failed to create SSTable writer: {}", e))
            })?;

        for (key, value) in &entries {
            writer
                .add(&key, value.as_deref())
                .map_err(|e| StorageError::SSTable(format!("Failed to write entry: {}", e)))?;
        }

        let mut info = writer
            .finish()
            .map_err(|e| StorageError::SSTable(format!("Failed to finish SSTable: {}", e)))?;

        // Set the proper id (writer returns id: 0 as placeholder)
        info.id = id;

        // Update manifest
        {
            let mut manifest = self.manifest.lock();
            manifest.sstables.push(SSTableManifestEntry {
                id,
                path: info.path.clone(),
                size: info.file_size,
                entry_count: info.entry_count,
                min_key: info.min_key.clone(),
                max_key: info.max_key.clone(),
                min_sequence: 0, // Sequence tracking handled by WAL
                max_sequence: self.memtable_manager.current_sequence(),
                creation_time: info.creation_time,
                level: 0,
            });
            manifest.wal_checkpoint = if let Some(ref wal) = self.wal {
                wal.current_sequence()
            } else {
                0
            };
            manifest
                .save(&self.config.data_dir)
                .map_err(|e| StorageError::Manifest(e.to_string()))?;
        }

        // Add to SSTable list
        self.sstables.write().await.push(info.clone());

        // Update atomic stats counters
        self.sstable_total_keys
            .fetch_add(info.entry_count, Ordering::Relaxed);
        self.sstable_total_bytes
            .fetch_add(info.file_size, Ordering::Relaxed);
        self.sstable_count.fetch_add(1, Ordering::Relaxed);

        info!("Flushed memtable to SSTable: {:?}", path);

        // Truncate old WAL files
        if let Some(ref wal) = self.wal {
            let checkpoint = wal.current_sequence();
            if let Err(e) = wal.truncate(checkpoint).await {
                tracing::warn!("Failed to truncate WAL: {}", e);
            }
        }

        Ok(info)
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

        let result = self
            .compactor
            .execute(job)
            .map_err(|e| StorageError::Compaction(e.to_string()))?;

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
            });

            // Update atomic counters: add new SSTable
            self.sstable_total_keys
                .fetch_add(output.entry_count, Ordering::Relaxed);
            self.sstable_total_bytes
                .fetch_add(output.size, Ordering::Relaxed);
            self.sstable_count.fetch_add(1, Ordering::Relaxed);
        }

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
        tokio::spawn(async move {
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
        tokio::spawn(async move {
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
    }

    /// Clone engine state for background tasks
    fn clone_for_background(&self) -> BackgroundEngine {
        BackgroundEngine {
            wal: self.wal.clone(),
            memtable_manager: self.memtable_manager.clone(),
            sstables: self.sstables.clone(),
            manifest: self.manifest.clone(),
            next_sstable_id: self.next_sstable_id.clone(),
            compactor: self.compactor.clone(),
            config: self.config.clone(),
            sstable_total_keys: self.sstable_total_keys.clone(),
            sstable_total_bytes: self.sstable_total_bytes.clone(),
            sstable_count: self.sstable_count.clone(),
        }
    }
}

/// Clone of engine state for background tasks
struct BackgroundEngine {
    memtable_manager: Arc<MemTableManager>,
    sstables: Arc<RwLock<Vec<SSTableInfo>>>,
    manifest: Arc<Mutex<Manifest>>,
    next_sstable_id: Arc<std::sync::atomic::AtomicU64>,
    compactor: Arc<Compactor>,
    config: StorageConfig,
    wal: Option<Arc<WriteAheadLog>>,
    // Atomic counters for SSTable stats
    sstable_total_keys: Arc<AtomicU64>,
    sstable_total_bytes: Arc<AtomicU64>,
    sstable_count: Arc<AtomicU64>,
}

impl BackgroundEngine {
    async fn background_flush(&self) -> Result<()> {
        while let Some(memtable) = self.memtable_manager.get_immutable_for_flush() {
            self.flush_memtable_to_sstable(&memtable).await?;
        }
        Ok(())
    }

    async fn compact(&self) -> Result<CompactionResult> {
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
                min_sequence: 0,
                max_sequence: 0,
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
        memtable: &super::memtable::MemTable,
    ) -> Result<SSTableInfo> {
        let id = self
            .next_sstable_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let path = self
            .config
            .data_dir
            .join("sstables")
            .join("L0")
            .join(format!("{:010}.sst", id));

        let entries = memtable.get_all_kv();

        let mut writer =
            SSTableWriter::new(&path, self.config.sstable_config.clone()).map_err(|e| {
                StorageError::SSTable(format!("Failed to create SSTable writer: {}", e))
            })?;

        for (key, value) in &entries {
            writer
                .add(&key, value.as_deref())
                .map_err(|e| StorageError::SSTable(format!("Failed to write entry: {}", e)))?;
        }

        let mut info = writer
            .finish()
            .map_err(|e| StorageError::SSTable(format!("Failed to finish SSTable: {}", e)))?;

        // Set the proper id (writer returns id: 0 as placeholder)
        info.id = id;

        {
            let mut manifest = self.manifest.lock();
            manifest.sstables.push(SSTableManifestEntry {
                id,
                path: info.path.clone(),
                size: info.file_size,
                entry_count: info.entry_count,
                min_key: info.min_key.clone(),
                max_key: info.max_key.clone(),
                min_sequence: 0,
                max_sequence: self.memtable_manager.current_sequence(),
                creation_time: info.creation_time,
                level: 0,
            });
            manifest.wal_checkpoint = if let Some(ref wal) = self.wal {
                wal.current_sequence()
            } else {
                0
            };
            manifest
                .save(&self.config.data_dir)
                .map_err(|e| StorageError::Manifest(e.to_string()))?;
        }

        self.sstables.write().await.push(info.clone());

        // Update atomic stats counters
        self.sstable_total_keys
            .fetch_add(info.entry_count, Ordering::Relaxed);
        self.sstable_total_bytes
            .fetch_add(info.file_size, Ordering::Relaxed);
        self.sstable_count.fetch_add(1, Ordering::Relaxed);

        info!("Background flushed memtable to SSTable: {:?}", path);

        // Truncate old WAL files
        if let Some(ref wal) = self.wal {
            let checkpoint = wal.current_sequence();
            if let Err(e) = wal.truncate(checkpoint).await {
                tracing::warn!("Failed to truncate WAL: {}", e);
            }
        }

        Ok(info)
    }

    async fn run_compaction(&self, job: super::compaction::CompactionJob) -> Result<()> {
        let input_paths: Vec<PathBuf> = job.input_sstables.iter().map(|s| s.path.clone()).collect();
        let input_ids: Vec<u64> = job.input_sstables.iter().map(|s| s.id).collect();

        // Track stats of removed SSTables for counter updates
        let removed_keys: u64 = job.input_sstables.iter().map(|s| s.entry_count).sum();
        let removed_bytes: u64 = job.input_sstables.iter().map(|s| s.size).sum();
        let removed_count = job.input_sstables.len() as u64;

        let result = self
            .compactor
            .execute(job)
            .map_err(|e| StorageError::Compaction(e.to_string()))?;

        let mut sstables = self.sstables.write().await;
        sstables.retain(|sst| !input_ids.contains(&sst.id));

        // Update atomic counters: subtract removed SSTables
        self.sstable_total_keys
            .fetch_sub(removed_keys, Ordering::Relaxed);
        self.sstable_total_bytes
            .fetch_sub(removed_bytes, Ordering::Relaxed);
        self.sstable_count
            .fetch_sub(removed_count, Ordering::Relaxed);

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
            });

            // Update atomic counters: add new SSTable
            self.sstable_total_keys
                .fetch_add(output.entry_count, Ordering::Relaxed);
            self.sstable_total_bytes
                .fetch_add(output.size, Ordering::Relaxed);
            self.sstable_count.fetch_add(1, Ordering::Relaxed);
        }

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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

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
}
