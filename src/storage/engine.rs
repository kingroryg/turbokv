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

use std::collections::{BTreeSet, HashSet};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Mutex, RwLock as SyncRwLock};
use tokio::sync::{Mutex as AsyncMutex, RwLock as AsyncRwLock};
use tokio::time::{interval, MissedTickBehavior};
use tracing::{debug, error, info, warn};

use crate::core::{
    CompactionResult, DatabaseStatus, Error as CoreError, LogicalStats, MaintenanceOrigin,
    MaintenanceStatus, PhysicalCacheStats, PhysicalMemTableStats, PhysicalSSTableStats,
    PhysicalStats, PhysicalVersionStats, StorageStats, WalStats, WriteAmplificationStats,
    WriteBackpressureCauseStatus, WriteBackpressureStatus, WriteBatch, WriteStallStats,
};

#[cfg(test)]
use super::InProgressGuard;
use super::{
    compaction::{
        remove_sstable_if_present, CompactionConfig, CompactionCoordinator,
        TombstoneReclamationSources,
    },
    directory_lock::{
        AcquireError as DirectoryLockAcquireError, DirectoryLock, LOCKED_DIRECTORY_GUIDANCE,
    },
    fd::{FdConfig, SSTablePool},
    iter::{RangeIter, ScanBounds, ScanSstable},
    maintenance::{MaintenanceHealth, MaintenanceOperation},
    manifest::{atomic_replace, sync_directory, Manifest, SSTableManifestEntry, MANIFEST_VERSION},
    memtable::{MemTableConfig, MemTableManager},
    sstable::{SSTableConfig, SSTableInfo, SSTableReader, SSTableWriter},
    version::VersionOrder,
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

/// Structured graceful-shutdown failure.
///
/// Use [`Engine::shutdown_with_status`] when unresolved maintenance must be
/// inspected programmatically. The legacy [`Engine::shutdown`] method maps the
/// maintenance variant to [`StorageError::Other`] for source compatibility.
#[derive(Debug, thiserror::Error)]
pub enum MaintenanceShutdownError {
    /// A storage operation failed outside the retained maintenance report.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// Graceful shutdown could not settle all registered maintenance work.
    #[error("shutdown left unresolved maintenance failures: {0}")]
    UnresolvedMaintenance(Box<MaintenanceStatus>),
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
    sstables: Arc<AsyncRwLock<Vec<SSTableInfo>>>,
    manifest: Arc<AsyncMutex<Manifest>>,
    /// Serializes foreground and background ownership of the immutable FIFO.
    flush_lock: Arc<AsyncMutex<()>>,
    /// Prevents checkpoint installation from racing sequence allocation/application.
    mutation_barrier: Arc<AsyncRwLock<()>>,
    /// Preserves sequence/publication order between concurrent atomic batches.
    batch_serialization: Arc<AsyncMutex<()>>,
    /// Publishes atomic batches as one visibility transition to concurrent readers.
    batch_visibility: Arc<AsyncRwLock<()>>,
    /// Conservative floors for WAL applications that did not finish. Floors
    /// remain until reopen, when WAL replay applies them before this set starts
    /// empty again.
    unapplied_wal_sequences: Arc<Mutex<BTreeSet<u64>>>,
    shutdown: tokio::sync::watch::Sender<bool>,
    background_tasks: Mutex<Vec<BackgroundTask>>,
    shutdown_lock: AsyncMutex<()>,
    next_sstable_id: Arc<std::sync::atomic::AtomicU64>,
    sstable_pool: Arc<SSTablePool>,
    compaction_coordinator: Arc<CompactionCoordinator>,
    sstable_stats: Arc<SstableStatistics>,
    logical_bytes_ingested: Arc<AtomicU64>,
    compactions_in_progress: Arc<AtomicU64>,
    maintenance_health: Arc<MaintenanceHealth>,
    flush_post_work: Arc<FlushPostWork>,
    write_stalls: WriteStallStatistics,
}

struct BackgroundTask {
    operation: MaintenanceOperation,
    handle: tokio::task::JoinHandle<()>,
}

/// One lock makes the total/cause tuple coherent. It is acquired only after
/// the pressure predicate has already selected an intentionally stalled write;
/// ordinary unstalled writes never touch it.
struct WriteStallStatistics {
    counters: Mutex<WriteStallCounters>,
}

#[derive(Clone, Copy, Default)]
struct WriteStallCounters {
    count: u64,
    micros: u64,
    immutable_memtable_count: u64,
    immutable_memtable_micros: u64,
    level_zero_file_count: u64,
    level_zero_file_micros: u64,
}

impl Default for WriteStallStatistics {
    fn default() -> Self {
        Self {
            counters: Mutex::new(WriteStallCounters::default()),
        }
    }
}

impl WriteStallStatistics {
    fn record(&self, micros: u64, immutable_pressure: bool, level_zero_pressure: bool) {
        let mut counters = self.counters.lock();
        counters.count = counters.count.saturating_add(1);
        counters.micros = counters.micros.saturating_add(micros);
        if immutable_pressure {
            counters.immutable_memtable_count = counters.immutable_memtable_count.saturating_add(1);
            counters.immutable_memtable_micros =
                counters.immutable_memtable_micros.saturating_add(micros);
        }
        if level_zero_pressure {
            counters.level_zero_file_count = counters.level_zero_file_count.saturating_add(1);
            counters.level_zero_file_micros =
                counters.level_zero_file_micros.saturating_add(micros);
        }
    }

    fn snapshot(&self) -> WriteStallCounters {
        *self.counters.lock()
    }
}

#[derive(Default)]
struct FlushPostWork {
    pending: Mutex<PendingFlushPostWork>,
}

#[derive(Clone, Copy, Default)]
struct PendingFlushPostWork {
    wal_sync: bool,
    wal_reclamation_checkpoint: Option<u64>,
}

impl FlushPostWork {
    fn register_wal_sync(&self) {
        self.pending.lock().wal_sync = true;
    }

    fn complete_wal_sync(&self) {
        self.pending.lock().wal_sync = false;
    }

    fn register_wal_reclamation(&self, checkpoint: u64) {
        self.pending.lock().wal_reclamation_checkpoint = Some(checkpoint);
    }

    fn complete_wal_reclamation(&self, checkpoint: u64) {
        let mut pending = self.pending.lock();
        if pending.wal_reclamation_checkpoint == Some(checkpoint) {
            pending.wal_reclamation_checkpoint = None;
        }
    }

    fn snapshot(&self) -> PendingFlushPostWork {
        *self.pending.lock()
    }

    fn is_empty(&self) -> bool {
        let pending = self.pending.lock();
        !pending.wal_sync && pending.wal_reclamation_checkpoint.is_none()
    }
}

/// Related SSTable gauges and maintenance counters shared by foreground and
/// background paths. The publication lock covers only the short live-gauge
/// transition; SSTable and manifest I/O never holds the live-reader-list lock.
pub(super) struct SstableStatistics {
    versions: AtomicU64,
    bytes: AtomicU64,
    tombstones: AtomicU64,
    files: AtomicU64,
    level_zero_files: AtomicU64,
    publication: SyncRwLock<()>,
    flush_bytes_written: AtomicU64,
    compaction_input_bytes: AtomicU64,
    compaction_output_bytes: AtomicU64,
    versions_reclaimed: AtomicU64,
    tombstones_reclaimed: AtomicU64,
}

struct SstableStatisticsSnapshot {
    versions: u64,
    bytes: u64,
    tombstones: u64,
    files: u64,
    level_zero_files: u64,
    flush_bytes_written: u64,
    compaction_input_bytes: u64,
    compaction_output_bytes: u64,
    versions_reclaimed: u64,
    tombstones_reclaimed: u64,
}

impl SstableStatistics {
    fn new(sstables: &[SSTableInfo]) -> Self {
        Self {
            versions: AtomicU64::new(sstables.iter().map(|table| table.entry_count).sum()),
            bytes: AtomicU64::new(sstables.iter().map(|table| table.file_size).sum()),
            tombstones: AtomicU64::new(sstables.iter().map(|table| table.tombstone_count).sum()),
            files: AtomicU64::new(sstables.len() as u64),
            level_zero_files: AtomicU64::new(
                sstables.iter().filter(|table| table.level == 0).count() as u64,
            ),
            publication: SyncRwLock::new(()),
            flush_bytes_written: AtomicU64::new(0),
            compaction_input_bytes: AtomicU64::new(0),
            compaction_output_bytes: AtomicU64::new(0),
            versions_reclaimed: AtomicU64::new(0),
            tombstones_reclaimed: AtomicU64::new(0),
        }
    }

    fn sample(&self) -> SstableStatisticsSnapshot {
        let _publication = self.publication.read();
        SstableStatisticsSnapshot {
            versions: self.versions.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            tombstones: self.tombstones.load(Ordering::Relaxed),
            files: self.files.load(Ordering::Relaxed),
            level_zero_files: self.level_zero_files.load(Ordering::Relaxed),
            flush_bytes_written: self.flush_bytes_written.load(Ordering::Relaxed),
            compaction_input_bytes: self.compaction_input_bytes.load(Ordering::Relaxed),
            compaction_output_bytes: self.compaction_output_bytes.load(Ordering::Relaxed),
            versions_reclaimed: self.versions_reclaimed.load(Ordering::Relaxed),
            tombstones_reclaimed: self.tombstones_reclaimed.load(Ordering::Relaxed),
        }
    }

    fn record_flush_output(&self, bytes: u64) {
        self.flush_bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    fn level_zero_file_count(&self) -> u64 {
        self.level_zero_files.load(Ordering::Relaxed)
    }

    pub(super) fn record_compaction_attempt(&self, input_bytes: u64, output_bytes: u64) {
        self.compaction_input_bytes
            .fetch_add(input_bytes, Ordering::Relaxed);
        self.compaction_output_bytes
            .fetch_add(output_bytes, Ordering::Relaxed);
    }

    fn install_if_absent(&self, live: &mut Vec<SSTableInfo>, info: &SSTableInfo) {
        let _publication = self.publication.write();
        if live.iter().any(|table| table.id == info.id) {
            return;
        }
        live.push(info.clone());
        self.versions.fetch_add(info.entry_count, Ordering::Relaxed);
        self.bytes.fetch_add(info.file_size, Ordering::Relaxed);
        self.tombstones
            .fetch_add(info.tombstone_count, Ordering::Relaxed);
        self.files.fetch_add(1, Ordering::Relaxed);
        if info.level == 0 {
            self.level_zero_files.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(super) fn publish_compaction(
        &self,
        live: &mut Vec<SSTableInfo>,
        inputs: &[SSTableManifestEntry],
        result: &super::compaction::CompactionExecution,
    ) {
        let input_ids: Vec<u64> = inputs.iter().map(|table| table.id).collect();
        let removed_versions: u64 = inputs.iter().map(|table| table.entry_count).sum();
        let removed_bytes: u64 = inputs.iter().map(|table| table.size).sum();
        let removed_tombstones: u64 = inputs.iter().map(|table| table.tombstone_count).sum();
        let removed_level_zero = inputs.iter().filter(|table| table.level == 0).count() as u64;

        let _publication = self.publication.write();
        live.retain(|table| !input_ids.contains(&table.id));
        self.versions.fetch_sub(removed_versions, Ordering::Relaxed);
        self.bytes.fetch_sub(removed_bytes, Ordering::Relaxed);
        self.tombstones
            .fetch_sub(removed_tombstones, Ordering::Relaxed);
        self.files.fetch_sub(inputs.len() as u64, Ordering::Relaxed);
        self.level_zero_files
            .fetch_sub(removed_level_zero, Ordering::Relaxed);

        for output in &result.output_sstables {
            let info = sstable_info_from_manifest(output);
            live.push(info);
            self.versions
                .fetch_add(output.entry_count, Ordering::Relaxed);
            self.bytes.fetch_add(output.size, Ordering::Relaxed);
            self.tombstones
                .fetch_add(output.tombstone_count, Ordering::Relaxed);
            self.files.fetch_add(1, Ordering::Relaxed);
            if output.level == 0 {
                self.level_zero_files.fetch_add(1, Ordering::Relaxed);
            }
        }

        self.versions_reclaimed
            .fetch_add(result.entries_dropped, Ordering::Relaxed);
        self.tombstones_reclaimed
            .fetch_add(result.tombstones_dropped, Ordering::Relaxed);
    }
}

impl Engine {
    /// Open or create a storage engine
    pub async fn open(mut config: StorageConfig) -> Result<Self> {
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

        // Existing bytes are inspected before directory creation, manifest
        // migration, orphan cleanup, WAL header synchronization, or tail
        // repair. Acquiring the advisory lock above may create the persistent
        // empty lock file; it is the only failed-open filesystem exception.
        let mut manifest = Manifest::load_or_create(&config.data_dir)
            .map_err(|e| StorageError::Manifest(e.to_string()))?;
        // Relative paths make checked-in directory fixtures portable. Released
        // databases used absolute paths; both spellings resolve to the same
        // in-memory identity and the next manifest migration persists the
        // canonical database-root spelling.
        for entry in &mut manifest.sstables {
            entry.path = resolve_sstable_path(&config.data_dir, &entry.path)
                .map_err(StorageError::Manifest)?;
        }
        let mut validated_sstables = HashSet::new();
        for entry in &manifest.sstables {
            SSTableReader::open_validated(&entry.path)
                .map_err(|error| StorageError::SSTable(error.to_string()))?;
            validated_sstables.insert(entry.path.clone());
        }
        // Published-looking but unreferenced tables are validated as well.
        // Valid orphans can be removed after preflight; corrupt or future
        // formats must fail without cleanup deleting evidence.
        for stored_path in discover_sstable_files(&sstable_dir)? {
            let path = resolve_sstable_path(&config.data_dir, &stored_path)
                .map_err(StorageError::SSTable)?;
            if validated_sstables.insert(path.clone()) {
                SSTableReader::open_validated(&path)
                    .map_err(|error| StorageError::SSTable(error.to_string()))?;
            }
        }
        let wal_preflight = super::wal::preflight_directory(&wal_dir).await?;

        tokio::fs::create_dir_all(&wal_dir).await?;
        tokio::fs::create_dir_all(&sstable_dir).await?;

        // Pre-create level directories only after the read-only preflight.
        for level in 0..config.compaction_config.max_levels {
            let level_dir = sstable_dir.join(format!("L{}", level));
            tokio::fs::create_dir_all(&level_dir).await?;
        }

        if manifest.loaded_format_version < u64::from(MANIFEST_VERSION) {
            for entry in &mut manifest.sstables {
                entry.tombstone_count = count_sstable_tombstones(&entry.path)?;
            }
            manifest
                .persist_format_upgrade(&config.data_dir)
                .map_err(|error| StorageError::Manifest(error.to_string()))?;
        }
        let referenced_sstables = manifest
            .sstables
            .iter()
            .map(|table| table.path.clone())
            .collect::<HashSet<_>>();
        let cleanup_directory = sstable_dir.clone();
        let cleanup_data_directory = config.data_dir.clone();
        let startup_sstable_cleanup = tokio::task::spawn_blocking(move || {
            cleanup_unreferenced_sstables(
                &cleanup_data_directory,
                &cleanup_directory,
                &referenced_sstables,
            )
        })
        .await
        .map_err(|error| StorageError::Other(format!("SSTable cleanup task failed: {error}")))?;
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
                WriteAheadLog::new_preflighted_with_directory_lock(
                    &wal_dir,
                    config.wal_config.clone(),
                    Arc::downgrade(&directory_lock),
                    wal_preflight,
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
                tombstone_count: entry.tombstone_count,
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
        let sstable_stats = Arc::new(SstableStatistics::new(&sstables));
        let sstables = Arc::new(AsyncRwLock::new(sstables));
        let manifest = Arc::new(AsyncMutex::new(manifest));
        let compactions_in_progress = Arc::new(AtomicU64::new(0));
        let flush_lock = Arc::new(AsyncMutex::new(()));
        let mutation_barrier = Arc::new(AsyncRwLock::new(()));
        let unapplied_wal_sequences = Arc::new(Mutex::new(BTreeSet::new()));
        let maintenance_health = Arc::new(MaintenanceHealth::default());
        if let Some(error) = startup_sstable_cleanup.failure.as_deref() {
            maintenance_health.record_failure(
                MaintenanceOperation::Compaction,
                MaintenanceOrigin::Recovery,
                &error,
            );
        }
        let flush_post_work = Arc::new(FlushPostWork::default());
        let compaction_coordinator = Arc::new(CompactionCoordinator::new(
            config.compaction_config.clone(),
            config.sstable_config.clone(),
            config.data_dir.clone(),
            Arc::clone(&next_sstable_id),
            Arc::downgrade(&directory_lock),
            Arc::clone(&sstables),
            Arc::clone(&sstable_pool),
            Arc::clone(&manifest),
            TombstoneReclamationSources::new(
                Arc::clone(&memtable_manager),
                Arc::clone(&mutation_barrier),
                Arc::clone(&unapplied_wal_sequences),
                wal.is_some(),
            ),
            Arc::clone(&sstable_stats),
            Arc::clone(&compactions_in_progress),
            Arc::clone(&maintenance_health),
            startup_sstable_cleanup.deferred_paths,
            startup_sstable_cleanup.scan_incomplete,
        ));

        let engine = Self {
            config,
            directory_lock,
            wal,
            memtable_manager,
            sstables,
            manifest,
            flush_lock,
            mutation_barrier,
            batch_serialization: Arc::new(AsyncMutex::new(())),
            batch_visibility: Arc::new(AsyncRwLock::new(())),
            unapplied_wal_sequences,
            shutdown: shutdown_tx,
            background_tasks: Mutex::new(Vec::new()),
            shutdown_lock: AsyncMutex::new(()),
            next_sstable_id,
            sstable_pool,
            compaction_coordinator,
            sstable_stats,
            logical_bytes_ingested: Arc::new(AtomicU64::new(0)),
            compactions_in_progress,
            maintenance_health,
            flush_post_work,
            write_stalls: WriteStallStatistics::default(),
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

        self.record_logical_bytes_ingested(payload_bytes(key, Some(value)));

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

        let bytes = entries.iter().fold(0_u64, |total, (key, value)| {
            total.saturating_add(payload_bytes(key, Some(value)))
        });
        self.record_logical_bytes_ingested(bytes);

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

        self.record_logical_bytes_ingested(payload_bytes(key, None));

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

        let bytes = ops.iter().fold(0_u64, |total, (key, value)| {
            total.saturating_add(payload_bytes(key, *value))
        });
        self.record_logical_bytes_ingested(bytes);

        Ok(())
    }

    /// Flush memtable to SSTable
    pub async fn flush(&self) -> Result<()> {
        self.flush_with_origin(MaintenanceOrigin::Foreground).await
    }

    async fn flush_with_origin(&self, origin: MaintenanceOrigin) -> Result<()> {
        let mut attempt = self
            .maintenance_health
            .attempt(MaintenanceOperation::Flush, origin);
        let result = self.flush_inner().await;
        let retry_work_resolved = result.is_ok()
            && !self.memtable_manager.has_immutable()
            && self.flush_post_work.is_empty();
        attempt.finish(&result, retry_work_resolved);
        result
    }

    async fn flush_inner(&self) -> Result<()> {
        let _flush = self.flush_lock.lock().await;
        retry_flush_post_work(self.flush_resources(), &self.flush_post_work).await?;
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

        retryable_wal_sync(self.flush_resources(), &self.flush_post_work).await?;

        Ok(())
    }

    /// Drain manual compaction work captured after coordinator ownership.
    ///
    /// Manual and background requests share one fair coordinator. Concurrent
    /// requests serialize and reselect after each preceding publication. A
    /// manual request carries descendants of its initial live-file scope to a
    /// fixed point while excluding unrelated concurrent flush arrivals;
    /// overlap closure can still pull a later arrival into a scoped job. Once
    /// shutdown starts, new requests are successful no-ops.
    pub async fn compact(&self) -> Result<CompactionResult> {
        self.compaction_coordinator.request_manual().await
    }

    /// Get legacy mixed physical storage statistics.
    ///
    /// `total_keys` counts physical versions and `total_bytes` combines
    /// approximate in-memory bytes with SSTable file bytes. Use
    /// [`Self::logical_stats`] and [`Self::physical_stats`] instead.
    #[deprecated(note = "use logical_stats() and physical_stats()")]
    pub fn stats(&self) -> StorageStats {
        self.legacy_stats()
    }

    pub(crate) fn legacy_stats(&self) -> StorageStats {
        let physical = self.physical_stats();

        StorageStats {
            total_keys: physical.versions.current,
            total_bytes: physical
                .memtables
                .buffered_bytes
                .saturating_add(physical.memtables.active_bytes)
                .saturating_add(physical.memtables.immutable_bytes)
                .saturating_add(physical.sstables.bytes),
            wal_size: physical.wal.active_segment_bytes,
            sstable_count: physical.sstables.files as u32,
            memtable_size: physical.memtables.active_bytes,
            compaction_pending: physical.memtables.immutable_tables != 0,
            wal_bytes_written: physical.wal.bytes_written_since_open,
            sstable_flush_bytes_written: physical.amplification.flush_bytes_written_since_open,
            compaction_bytes_read: physical.amplification.compaction_input_bytes_since_open,
            compaction_bytes_written: physical.amplification.compaction_output_bytes_since_open,
            compactions_in_progress: physical.compactions_in_progress,
            immutable_memtables: physical.memtables.immutable_tables,
            l0_sstable_count: physical.sstables.level_zero_files,
            write_stall_count: physical.stalls.count_since_open,
            write_stall_micros: physical.stalls.micros_since_open,
        }
    }

    /// Scan one coherent database snapshot for exact logical statistics.
    ///
    /// This is an O(physical versions) operation that reads SSTable blocks and
    /// freezes a nonempty active memtable while capturing the snapshot. The
    /// freeze can cause additional bytes to be written by a later flush and
    /// compaction. Tombstones and superseded versions are resolved by the same
    /// streaming merge used for range scans and never inflate these counts.
    pub async fn logical_stats(&self) -> Result<LogicalStats> {
        let mut iterator = self.scan_prefix_iter(b"").await?;
        let mut stats = LogicalStats::default();
        for entry in &mut iterator {
            let entry = entry.map_err(super::iter::ScanError::into_storage_error)?;
            let key_bytes = entry.key().len() as u64;
            let value_bytes = entry.value_len() as u64;
            stats.live_keys = stats
                .live_keys
                .checked_add(1)
                .ok_or_else(logical_stats_overflow)?;
            stats.key_bytes = stats
                .key_bytes
                .checked_add(key_bytes)
                .ok_or_else(logical_stats_overflow)?;
            stats.value_bytes = stats
                .value_bytes
                .checked_add(value_bytes)
                .ok_or_else(logical_stats_overflow)?;
        }
        stats.total_bytes = stats
            .key_bytes
            .checked_add(stats.value_bytes)
            .ok_or_else(logical_stats_overflow)?;
        Ok(stats)
    }

    /// Get cheap operational health and write-backpressure status.
    ///
    /// Failure details are bounded and remain present until a proof-producing
    /// retry resolves the failed lane. Flush retries prove resolution by
    /// draining the immutable FIFO and completing registered WAL post-work;
    /// compaction retries require an exact final selection with no publication
    /// reconciliation, startup scan failure, or deferred cleanup remaining.
    /// A poisoned paranoid WAL also marks the flush lane unhealthy and requires
    /// reopen because its failed commit outcome may be indeterminate. Counters
    /// reset after reopen.
    pub fn status(&self) -> DatabaseStatus {
        let immutable_current =
            u64::try_from(self.memtable_manager.immutable_count()).unwrap_or(u64::MAX);
        let immutable_threshold =
            u64::try_from(self.config.max_immutable_memtables_before_stall).unwrap_or(u64::MAX);
        let level_zero_current = self.sstable_stats.level_zero_file_count();
        let level_zero_threshold = self.config.max_l0_files_before_stall;
        let immutable_active = immutable_current >= immutable_threshold;
        let level_zero_active = level_zero_current >= level_zero_threshold;
        let stall_counters = self.write_stalls.snapshot();

        DatabaseStatus {
            maintenance: self.maintenance_status(),
            write_backpressure: WriteBackpressureStatus {
                active: immutable_active || level_zero_active,
                stalls_since_open: stall_counters.count,
                stall_micros_since_open: stall_counters.micros,
                immutable_memtables: WriteBackpressureCauseStatus {
                    active: immutable_active,
                    current: immutable_current,
                    threshold: immutable_threshold,
                    count_since_open: stall_counters.immutable_memtable_count,
                    micros_since_open: stall_counters.immutable_memtable_micros,
                },
                level_zero_files: WriteBackpressureCauseStatus {
                    active: level_zero_active,
                    current: level_zero_current,
                    threshold: level_zero_threshold,
                    count_since_open: stall_counters.level_zero_file_count,
                    micros_since_open: stall_counters.level_zero_file_micros,
                },
            },
        }
    }

    fn maintenance_status(&self) -> MaintenanceStatus {
        let wal_failure = self
            .wal
            .as_ref()
            .and_then(|wal| wal.group_commit_failure_for_status())
            .map(|failure| format!("WAL error: {failure}"));
        self.maintenance_health
            .status_with_wal_failure(wal_failure.as_deref())
    }

    /// Get cheap physical gauges and process-lifetime cumulative counters.
    ///
    /// This method performs no database scan. Current gauges are sampled for
    /// monitoring and are not one transactional snapshot across components.
    /// Every `_since_open` counter resets when the engine is reopened.
    pub fn physical_stats(&self) -> PhysicalStats {
        let stall_counters = self.write_stalls.snapshot();
        let memtable_stats = self.memtable_manager.stats();
        let buffered_versions = memtable_stats.buffered_versions;
        let active_versions = memtable_stats.active.entry_count as u64;
        let immutable_versions = memtable_stats
            .immutable
            .iter()
            .map(|stats| stats.entry_count as u64)
            .sum::<u64>();
        let active_bytes = memtable_stats.active.size_bytes as u64;
        let immutable_bytes = memtable_stats
            .immutable
            .iter()
            .map(|stats| stats.size_bytes as u64)
            .sum::<u64>();
        let memtable_tombstones = memtable_stats.active.tombstone_count as u64
            + memtable_stats
                .immutable
                .iter()
                .map(|stats| stats.tombstone_count as u64)
                .sum::<u64>();
        let sstable = self.sstable_stats.sample();
        let wal_bytes_written = self
            .wal
            .as_ref()
            .map_or(0, |wal| wal.bytes_written_since_open());
        let reader_cache = self.sstable_pool.stats();
        let block_cache = self.sstable_pool.block_cache_stats();

        PhysicalStats {
            wal: WalStats {
                enabled: self.wal.is_some(),
                active_segment_bytes: self.wal.as_ref().map_or(0, |wal| wal.current_size()),
                retained_valid_bytes: self.wal.as_ref().map_or(0, |wal| wal.retained_size()),
                bytes_written_since_open: wal_bytes_written,
            },
            memtables: PhysicalMemTableStats {
                buffered_bytes: memtable_stats.buffered_bytes,
                active_bytes,
                immutable_bytes,
                active_versions,
                buffered_versions,
                immutable_versions,
                tombstones: memtable_tombstones,
                immutable_tables: memtable_stats.immutable.len() as u64,
            },
            sstables: PhysicalSSTableStats {
                bytes: sstable.bytes,
                files: sstable.files,
                level_zero_files: sstable.level_zero_files,
                versions: sstable.versions,
                tombstones: sstable.tombstones,
            },
            versions: PhysicalVersionStats {
                current: buffered_versions
                    + active_versions
                    + immutable_versions
                    + sstable.versions,
                tombstones: memtable_tombstones + sstable.tombstones,
                reclaimed_by_compaction_since_open: sstable.versions_reclaimed,
                tombstones_reclaimed_by_compaction_since_open: sstable.tombstones_reclaimed,
            },
            cache: PhysicalCacheStats {
                block_cache_enabled: block_cache.is_some(),
                block_cache_entries: block_cache.as_ref().map_or(0, |stats| stats.entries as u64),
                block_cache_bytes: block_cache.as_ref().map_or(0, |stats| stats.size_bytes),
                block_cache_hits_since_open: block_cache.as_ref().map_or(0, |stats| stats.hits),
                block_cache_misses_since_open: block_cache.as_ref().map_or(0, |stats| stats.misses),
                sstable_readers: reader_cache.open_sstables as u64,
                sstable_reader_hits_since_open: reader_cache.cache_hits,
                sstable_reader_misses_since_open: reader_cache.cache_misses,
                sstable_reader_evictions_since_open: reader_cache.evictions,
            },
            stalls: WriteStallStats {
                count_since_open: stall_counters.count,
                micros_since_open: stall_counters.micros,
            },
            amplification: WriteAmplificationStats {
                logical_bytes_ingested_since_open: self
                    .logical_bytes_ingested
                    .load(Ordering::Relaxed),
                wal_bytes_written_since_open: wal_bytes_written,
                flush_bytes_written_since_open: sstable.flush_bytes_written,
                compaction_input_bytes_since_open: sstable.compaction_input_bytes,
                compaction_output_bytes_since_open: sstable.compaction_output_bytes,
            },
            compactions_in_progress: self.compactions_in_progress.load(Ordering::Acquire),
        }
    }

    /// Shutdown the engine gracefully.
    ///
    /// This stops background tasks and flushes pending writes. Because this
    /// method borrows the still-usable engine, its exclusive directory lock is
    /// retained until the [`Engine`] is dropped. Shutdown is terminal for the
    /// compaction coordinator: later compaction requests return no work. The
    /// final flush may resolve retained flush work; any maintenance failure
    /// still unresolved afterward is returned through the legacy
    /// [`StorageError::Other`] variant. Use [`Self::shutdown_with_status`] to
    /// inspect the structured maintenance snapshot.
    pub async fn shutdown(&self) -> Result<()> {
        self.shutdown_with_status()
            .await
            .map_err(|error| match error {
                MaintenanceShutdownError::Storage(error) => error,
                MaintenanceShutdownError::UnresolvedMaintenance(status) => StorageError::Other(
                    format!("shutdown left unresolved maintenance failures: {status}"),
                ),
            })
    }

    /// Shutdown gracefully and return a structured unresolved-health snapshot.
    ///
    /// This is the production monitoring contract for callers which must
    /// distinguish ordinary storage failures from retryable maintenance left
    /// unresolved after the final flush. Like [`Self::shutdown`], this borrows
    /// the engine and retains directory ownership until the engine is dropped.
    pub async fn shutdown_with_status(&self) -> std::result::Result<(), MaintenanceShutdownError> {
        let _shutdown = self.shutdown_lock.lock().await;
        let compaction_pause = self.compaction_coordinator.pause_requests();
        info!("Shutting down storage engine");

        // Signal background tasks to stop
        let _ = self.shutdown.send(true);

        let tasks: Vec<_> = self.background_tasks.lock().drain(..).collect();
        for task in tasks {
            if let Err(error) = task.handle.await {
                if !self.maintenance_health.retry_pending(task.operation) {
                    self.maintenance_health.record_failure(
                        task.operation,
                        MaintenanceOrigin::Background,
                        &error,
                    );
                }
            }
        }
        compaction_pause.wait_until_idle().await;

        // Flush pending writes
        let flush_result = self.flush_with_origin(MaintenanceOrigin::Shutdown).await;
        let status = self.maintenance_status();

        if !status.is_healthy() {
            return Err(MaintenanceShutdownError::UnresolvedMaintenance(Box::new(
                status,
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

    fn record_logical_bytes_ingested(&self, bytes: u64) {
        let _ = self.logical_bytes_ingested.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_add(bytes)),
        );
    }

    async fn maybe_stall_writes(&self) {
        let immutable_count = self.memtable_manager.immutable_count();
        let l0_count = self.sstable_stats.level_zero_file_count();
        let immutable_pressure =
            immutable_count >= self.config.max_immutable_memtables_before_stall;
        let level_zero_pressure = l0_count >= self.config.max_l0_files_before_stall;
        let should_stall = immutable_pressure || level_zero_pressure;

        if should_stall && self.config.write_stall_micros > 0 {
            self.write_stalls.record(
                self.config.write_stall_micros,
                immutable_pressure,
                level_zero_pressure,
            );
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
            let unchanged = {
                let live_sstables = self.sstables.read().await;
                live_sstables.len() == snapshot_identity.len()
                    && live_sstables
                        .iter()
                        .zip(&snapshot_identity)
                        .all(|(info, (id, path))| info.id == *id && info.path == *path)
            };
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
        flush_memtable_to_sstable(self.flush_resources(), &self.flush_post_work, memtable).await
    }

    fn flush_resources(&self) -> FlushResources<'_> {
        FlushResources {
            config: &self.config,
            wal: self.wal.as_ref(),
            memtable_manager: &self.memtable_manager,
            sstables: &self.sstables,
            manifest: &self.manifest,
            mutation_barrier: &self.mutation_barrier,
            unapplied_wal_sequences: &self.unapplied_wal_sequences,
            next_sstable_id: &self.next_sstable_id,
            sstable_stats: &self.sstable_stats,
        }
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
                        if flush_engine.memtable_manager.has_immutable()
                            || flush_engine.maintenance_health.retry_pending(MaintenanceOperation::Flush)
                        {
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

        self.background_tasks.lock().extend([
            BackgroundTask {
                operation: MaintenanceOperation::Flush,
                handle: flush_task,
            },
            BackgroundTask {
                operation: MaintenanceOperation::Compaction,
                handle: compaction_task,
            },
        ]);
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
            compaction_coordinator: self.compaction_coordinator.clone(),
            config: self.config.clone(),
            sstable_stats: self.sstable_stats.clone(),
            maintenance_health: Arc::clone(&self.maintenance_health),
            flush_post_work: Arc::clone(&self.flush_post_work),
        }
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        for task in self.background_tasks.get_mut().drain(..) {
            task.handle.abort();
        }
    }
}

/// Clone of engine state for background tasks
struct BackgroundEngine {
    directory_lock: std::sync::Weak<DirectoryLock>,
    memtable_manager: Arc<MemTableManager>,
    sstables: Arc<AsyncRwLock<Vec<SSTableInfo>>>,
    manifest: Arc<AsyncMutex<Manifest>>,
    flush_lock: Arc<AsyncMutex<()>>,
    mutation_barrier: Arc<AsyncRwLock<()>>,
    unapplied_wal_sequences: Arc<Mutex<BTreeSet<u64>>>,
    next_sstable_id: Arc<std::sync::atomic::AtomicU64>,
    compaction_coordinator: Arc<CompactionCoordinator>,
    config: StorageConfig,
    wal: Option<Arc<WriteAheadLog>>,
    sstable_stats: Arc<SstableStatistics>,
    maintenance_health: Arc<MaintenanceHealth>,
    flush_post_work: Arc<FlushPostWork>,
}

impl BackgroundEngine {
    async fn background_flush(&self) -> Result<()> {
        let mut attempt = self
            .maintenance_health
            .attempt(MaintenanceOperation::Flush, MaintenanceOrigin::Background);
        let result = self.background_flush_inner().await;
        let retry_work_resolved = result.is_ok()
            && !self.memtable_manager.has_immutable()
            && self.flush_post_work.is_empty();
        attempt.finish(&result, retry_work_resolved);
        result
    }

    async fn background_flush_inner(&self) -> Result<()> {
        let Some(_directory_lock) = self.directory_lock.upgrade() else {
            return Ok(());
        };
        let _flush = self.flush_lock.lock().await;
        retry_flush_post_work(self.flush_resources(), &self.flush_post_work).await?;
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
        self.compaction_coordinator.request_background().await
    }

    async fn flush_memtable_to_sstable(
        &self,
        memtable: &Arc<super::memtable::MemTable>,
    ) -> Result<SSTableInfo> {
        flush_memtable_to_sstable(self.flush_resources(), &self.flush_post_work, memtable).await
    }

    fn flush_resources(&self) -> FlushResources<'_> {
        FlushResources {
            config: &self.config,
            wal: self.wal.as_ref(),
            memtable_manager: &self.memtable_manager,
            sstables: &self.sstables,
            manifest: &self.manifest,
            mutation_barrier: &self.mutation_barrier,
            unapplied_wal_sequences: &self.unapplied_wal_sequences,
            next_sstable_id: &self.next_sstable_id,
            sstable_stats: &self.sstable_stats,
        }
    }
}

#[derive(Clone, Copy)]
struct FlushResources<'a> {
    config: &'a StorageConfig,
    wal: Option<&'a Arc<WriteAheadLog>>,
    memtable_manager: &'a Arc<MemTableManager>,
    sstables: &'a Arc<AsyncRwLock<Vec<SSTableInfo>>>,
    manifest: &'a Arc<AsyncMutex<Manifest>>,
    mutation_barrier: &'a Arc<AsyncRwLock<()>>,
    unapplied_wal_sequences: &'a Arc<Mutex<BTreeSet<u64>>>,
    next_sstable_id: &'a Arc<AtomicU64>,
    sstable_stats: &'a SstableStatistics,
}

async fn flush_memtable_to_sstable(
    resources: FlushResources<'_>,
    post_work: &FlushPostWork,
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
        let manifest = resources.manifest.lock().await;
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
        reclaim_wal_after_checkpoint(resources, post_work).await?;
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
        resources.sstable_stats.record_flush_output(info.file_size);
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
        let mut live_manifest = resources.manifest.lock().await;
        let mut candidate = live_manifest.clone();
        candidate.sstables.push(SSTableManifestEntry {
            id,
            path: info.path.clone(),
            size: info.file_size,
            entry_count: info.entry_count,
            tombstone_count: info.tombstone_count,
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

    retryable_wal_reclamation(resources, post_work, checkpoint).await?;
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
        tombstone_count: expected
            .iter()
            .filter(|(_, entry)| entry.is_tombstone())
            .count() as u64,
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

fn count_sstable_tombstones(path: &std::path::Path) -> Result<u64> {
    let reader =
        SSTableReader::open(path).map_err(|error| StorageError::SSTable(error.to_string()))?;
    let mut tombstones = 0_u64;
    for entry in reader.iter() {
        let (_, value) = entry.map_err(|error| StorageError::SSTable(error.to_string()))?;
        if value.is_none() {
            tombstones = tombstones
                .checked_add(1)
                .ok_or_else(physical_tombstone_stats_overflow)?;
        }
    }
    Ok(tombstones)
}

fn sstable_info_from_manifest(entry: &SSTableManifestEntry) -> SSTableInfo {
    SSTableInfo {
        id: entry.id,
        path: entry.path.clone(),
        file_size: entry.size,
        entry_count: entry.entry_count,
        tombstone_count: entry.tombstone_count,
        min_key: entry.min_key.clone(),
        max_key: entry.max_key.clone(),
        creation_time: entry.creation_time,
        level: entry.level,
        min_sequence: entry.min_sequence,
        max_sequence: entry.max_sequence,
    }
}

async fn install_live_sstable(resources: &FlushResources<'_>, info: &SSTableInfo) {
    let mut sstables = resources.sstables.write().await;
    resources
        .sstable_stats
        .install_if_absent(&mut sstables, info);
}

async fn reclaim_wal_after_checkpoint(
    resources: FlushResources<'_>,
    post_work: &FlushPostWork,
) -> Result<()> {
    let checkpoint = resources.manifest.lock().await.wal_checkpoint;
    #[cfg(test)]
    super::failpoints::check(
        &resources.config.data_dir,
        super::failpoints::PersistenceBoundary::Checkpoint,
    )?;
    retryable_wal_reclamation(resources, post_work, checkpoint).await
}

async fn retry_flush_post_work(
    resources: FlushResources<'_>,
    post_work: &FlushPostWork,
) -> Result<()> {
    let pending = post_work.snapshot();
    if let Some(checkpoint) = pending.wal_reclamation_checkpoint {
        retryable_wal_reclamation(resources, post_work, checkpoint).await?;
    }
    if pending.wal_sync {
        retryable_wal_sync(resources, post_work).await?;
    }
    Ok(())
}

async fn retryable_wal_sync(
    resources: FlushResources<'_>,
    post_work: &FlushPostWork,
) -> Result<()> {
    let Some(wal) = resources.wal else {
        return Ok(());
    };
    post_work.register_wal_sync();
    #[cfg(test)]
    super::failpoints::check(
        &resources.config.data_dir,
        super::failpoints::PersistenceBoundary::WalFlush,
    )?;
    wal.flush().await?;
    post_work.complete_wal_sync();
    Ok(())
}

async fn retryable_wal_reclamation(
    resources: FlushResources<'_>,
    post_work: &FlushPostWork,
    checkpoint: u64,
) -> Result<()> {
    let Some(wal) = resources.wal else {
        return Ok(());
    };
    post_work.register_wal_reclamation(checkpoint);
    #[cfg(test)]
    super::failpoints::check(
        &resources.config.data_dir,
        super::failpoints::PersistenceBoundary::WalTruncation,
    )?;
    wal.truncate(checkpoint).await?;
    post_work.complete_wal_reclamation(checkpoint);
    Ok(())
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

fn payload_bytes(key: &[u8], value: Option<&[u8]>) -> u64 {
    (key.len() as u64).saturating_add(value.map_or(0, |value| value.len() as u64))
}

fn logical_stats_overflow() -> StorageError {
    StorageError::Other("logical statistics exceed u64 accounting limits".to_string())
}

fn physical_tombstone_stats_overflow() -> StorageError {
    StorageError::Other("physical tombstone statistics exceed u64 accounting limits".to_string())
}

/// Remove SSTables that cannot be authoritative because the durable manifest
/// does not reference them. A crash can leave both unpublished compaction
/// outputs and already-obsolete inputs behind. Failed unlinks are handed to
/// the coordinator's ordinary deferred-cleanup retry path.
#[derive(Default)]
struct StartupSstableCleanup {
    deferred_paths: Vec<PathBuf>,
    scan_incomplete: bool,
    failure: Option<String>,
}

impl StartupSstableCleanup {
    fn record_scan_failure(&mut self, message: String) {
        self.scan_incomplete = true;
        self.failure.get_or_insert(message);
    }

    fn defer(&mut self, path: PathBuf, message: String) {
        self.failure.get_or_insert(message);
        self.deferred_paths.push(path);
    }
}

fn cleanup_unreferenced_sstables(
    _data_directory: &std::path::Path,
    sstable_directory: &std::path::Path,
    referenced: &HashSet<PathBuf>,
) -> StartupSstableCleanup {
    let mut cleanup = StartupSstableCleanup::default();
    #[cfg(test)]
    if let Err(error) = super::failpoints::check(
        _data_directory,
        super::failpoints::PersistenceBoundary::SstableCleanupScan,
    ) {
        let message = format!("startup cleanup scan failed: {error}");
        warn!("{message}");
        cleanup.record_scan_failure(message);
        return cleanup;
    }
    // The directory lock canonicalizes the configured data directory, while
    // older manifests can contain equivalent non-canonical paths (for example,
    // macOS's /var and /private/var aliases). Compare filesystem identities in
    // the canonical namespace so those authoritative tables are retained.
    let mut referenced_identities = HashSet::with_capacity(referenced.len());
    for path in referenced {
        let canonical_path = match std::fs::canonicalize(path) {
            Ok(canonical_path) => canonical_path,
            Err(error) => {
                let message = format!(
                    "startup cleanup could not resolve manifest SSTable identity {}: {error}",
                    path.display()
                );
                warn!("{message}");
                cleanup.record_scan_failure(message);
                return cleanup;
            }
        };
        referenced_identities.insert(canonical_path);
    }
    let mut directories = vec![sstable_directory.to_path_buf()];
    match std::fs::read_dir(sstable_directory) {
        Ok(entries) => {
            for entry in entries {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(error) => {
                        let message = format!(
                            "startup cleanup could not inspect an entry in {}: {error}",
                            sstable_directory.display()
                        );
                        warn!("{message}");
                        cleanup.record_scan_failure(message);
                        continue;
                    }
                };
                match entry.file_type() {
                    Ok(file_type) if file_type.is_dir() => directories.push(entry.path()),
                    Ok(_) => {}
                    Err(error) => {
                        let message = format!(
                            "startup cleanup could not inspect file type for {}: {error}",
                            entry.path().display()
                        );
                        warn!("{message}");
                        cleanup.record_scan_failure(message);
                    }
                }
            }
        }
        Err(error) => {
            let message = format!(
                "startup cleanup could not scan SSTable directory {}: {error}",
                sstable_directory.display()
            );
            warn!("{message}");
            cleanup.record_scan_failure(message);
            return cleanup;
        }
    }

    for directory in directories {
        let entries = match std::fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) => {
                let message = format!(
                    "startup cleanup could not scan SSTable directory {}: {error}",
                    directory.display()
                );
                warn!("{message}");
                cleanup.record_scan_failure(message);
                continue;
            }
        };
        for entry in entries {
            let path = match entry {
                Ok(entry) => entry.path(),
                Err(error) => {
                    let message = format!(
                        "startup cleanup could not inspect an entry in {}: {error}",
                        directory.display()
                    );
                    warn!("{message}");
                    cleanup.record_scan_failure(message);
                    continue;
                }
            };
            if !path.extension().is_some_and(|extension| extension == "sst") {
                continue;
            }
            let canonical_path = match std::fs::canonicalize(&path) {
                Ok(canonical_path) => canonical_path,
                Err(error) => {
                    let message = format!(
                        "startup cleanup retained SSTable with unresolved identity {}: {error}",
                        path.display()
                    );
                    warn!("{message}");
                    cleanup.record_scan_failure(message);
                    continue;
                }
            };
            if referenced_identities.contains(&canonical_path) {
                continue;
            }
            #[cfg(test)]
            if let Err(error) = super::failpoints::check(
                _data_directory,
                super::failpoints::PersistenceBoundary::SstableCleanup,
            ) {
                let message = format!(
                    "startup cleanup deferred unreferenced SSTable {}: {error}",
                    path.display()
                );
                warn!("{message}");
                cleanup.defer(path, message);
                continue;
            }
            match remove_sstable_if_present(&path) {
                Ok(true) => debug!("Deleted unreferenced SSTable after reopen: {:?}", path),
                Ok(false) => {}
                Err(error) => {
                    let message = format!(
                        "startup cleanup deferred unreferenced SSTable {}: {error}",
                        path.display()
                    );
                    warn!("{message}");
                    cleanup.defer(path, message);
                }
            }
        }
    }
    cleanup
}

fn discover_sstable_files(sstable_directory: &std::path::Path) -> Result<Vec<PathBuf>> {
    if !sstable_directory.exists() {
        return Ok(Vec::new());
    }

    let mut directories = vec![sstable_directory.to_path_buf()];
    let mut root_entries =
        std::fs::read_dir(sstable_directory)?.collect::<std::io::Result<Vec<_>>>()?;
    root_entries.sort_by_key(std::fs::DirEntry::path);
    for entry in root_entries {
        if entry.file_type()?.is_dir() {
            directories.push(entry.path());
        }
    }
    directories.sort();

    let mut files = Vec::new();
    for directory in directories {
        let mut entries = std::fs::read_dir(directory)?.collect::<std::io::Result<Vec<_>>>()?;
        entries.sort_by_key(std::fs::DirEntry::path);
        files.extend(
            entries
                .into_iter()
                .map(|entry| entry.path())
                .filter(|path| path.extension().is_some_and(|extension| extension == "sst")),
        );
    }
    files.sort();
    files.dedup();
    Ok(files)
}

fn resolve_sstable_path(
    data_directory: &Path,
    stored_path: &Path,
) -> std::result::Result<PathBuf, String> {
    if stored_path
        .components()
        .any(|component| component == Component::ParentDir)
    {
        return Err(format!(
            "SSTable path contains parent traversal: {}",
            stored_path.display()
        ));
    }
    let candidate = if stored_path.is_relative() {
        data_directory.join(stored_path)
    } else {
        stored_path.to_path_buf()
    };
    let canonical = std::fs::canonicalize(&candidate).map_err(|error| {
        format!(
            "SSTable path cannot be resolved {}: {error}",
            stored_path.display()
        )
    })?;
    // A pre-existing symlink of the entire `sstables` directory is a supported
    // storage layout. Treat that directory's canonical target as a second
    // trusted root, while still rejecting a file-level symlink that escapes it.
    let canonical_sstable_directory = std::fs::canonicalize(data_directory.join("sstables")).ok();
    let within_storage_root = canonical.starts_with(data_directory)
        || canonical_sstable_directory
            .as_ref()
            .is_some_and(|root| canonical.starts_with(root));
    if !within_storage_root {
        return Err(format!(
            "SSTable path escapes database directory storage roots: {}",
            stored_path.display()
        ));
    }
    Ok(canonical)
}

#[cfg(test)]
fn wal_data_entry_size(key: &[u8], value: &[u8]) -> u64 {
    const WAL_ENTRY_HEADER_SIZE: usize = 32;
    (WAL_ENTRY_HEADER_SIZE + 4 + key.len() + value.len()) as u64
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::AtomicBool;

    use super::*;
    use rand::rngs::StdRng;
    use rand::{RngCore, SeedableRng};
    use tempfile::TempDir;

    use crate::storage::test_support::{stress_context, stress_key_value};

    type VersionedTableEntry<'a> = (&'a [u8], Option<&'a [u8]>, u64);

    fn isolated_config(path: &std::path::Path, wal_enabled: bool) -> StorageConfig {
        StorageConfig {
            data_dir: path.to_path_buf(),
            wal_enabled,
            background_tasks_enabled: false,
            ..Default::default()
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum StressMode {
        Fast,
        Durable,
        Paranoid,
    }

    impl StressMode {
        fn config(self, path: &std::path::Path) -> StorageConfig {
            let mut config = match self {
                Self::Fast => StorageConfig::fast(path.to_path_buf()),
                Self::Durable => StorageConfig::durable(path.to_path_buf()),
                Self::Paranoid => StorageConfig::paranoid(path.to_path_buf()),
            };
            config.background_tasks_enabled = false;
            config.memtable_config = MemTableConfig {
                max_size: 2_048,
                max_entries: 5,
                max_age: Duration::from_secs(3_600),
            };
            config.wal_config.max_file_size = 64 + 160;
            config.sstable_config = SSTableConfig {
                block_size: 256,
                compression: super::super::sstable::CompressionType::None,
                ..SSTableConfig::default()
            };
            config.compaction_config.l0_compaction_trigger = usize::MAX;
            config.max_l0_files_before_stall = u64::MAX;
            config
        }

        fn wal_enabled(self) -> bool {
            !matches!(self, Self::Fast)
        }

        fn failure_boundaries(self) -> &'static [super::super::failpoints::PersistenceBoundary] {
            use super::super::failpoints::PersistenceBoundary;

            const FAST: &[PersistenceBoundary] = &[
                PersistenceBoundary::MemtableFreeze,
                PersistenceBoundary::SstablePublication,
                PersistenceBoundary::ManifestInstallation,
                PersistenceBoundary::ManifestDirectorySync,
                PersistenceBoundary::Checkpoint,
            ];
            const WAL: &[PersistenceBoundary] = &[
                PersistenceBoundary::MemtableFreeze,
                PersistenceBoundary::SstablePublication,
                PersistenceBoundary::ManifestInstallation,
                PersistenceBoundary::ManifestDirectorySync,
                PersistenceBoundary::Checkpoint,
                PersistenceBoundary::WalFlush,
                PersistenceBoundary::WalTruncation,
            ];
            if self.wal_enabled() {
                WAL
            } else {
                FAST
            }
        }
    }

    #[derive(Clone, Copy)]
    struct StressProgress {
        mode: StressMode,
        seed: u64,
        step: u64,
        sequence: u64,
    }

    fn storage_stress_context(
        path: &std::path::Path,
        progress: StressProgress,
        generation: u64,
    ) -> String {
        let manifest_files = Manifest::load_or_create(path).map_or_else(
            |error| vec![format!("MANIFEST({error})")],
            |manifest| {
                manifest
                    .sstables
                    .into_iter()
                    .map(|table| format!("{}:{}", table.id, table.path.display()))
                    .collect()
            },
        );
        let wal_files = std::fs::read_dir(path.join("wal")).map_or_else(
            |_| Vec::new(),
            |entries| {
                let mut files = entries
                    .filter_map(std::result::Result::ok)
                    .filter_map(|entry| entry.file_name().into_string().ok())
                    .filter(|name| name.ends_with(".wal"))
                    .collect::<Vec<_>>();
                files.sort();
                files
            },
        );
        let identity = stress_context(progress.seed, progress.sequence, generation, "MANIFEST");
        format!(
            "mode={:?} step={} {identity} manifest_files={manifest_files:?} wal_files={wal_files:?}",
            progress.mode, progress.step
        )
    }

    async fn assert_storage_stress_model(
        engine: &Engine,
        expected: &BTreeMap<Vec<u8>, Vec<u8>>,
        progress: StressProgress,
    ) {
        let generation = engine.next_sstable_id.load(Ordering::Acquire);
        let context = storage_stress_context(&engine.config.data_dir, progress, generation);
        assert_eq!(
            engine.memtable_manager.current_sequence(),
            progress.sequence,
            "{context}"
        );
        for key_index in 0..24 {
            let key = format!("stress:key:{key_index:02}").into_bytes();
            let actual = engine
                .get(&key)
                .await
                .unwrap_or_else(|error| panic!("{context}: point read failed: {error}"));
            assert_eq!(
                actual.as_ref(),
                expected.get(&key),
                "{context}: key={key:?}"
            );
        }
        let actual = engine
            .scan_prefix(b"stress:key:")
            .await
            .unwrap_or_else(|error| panic!("{context}: scan failed: {error}"));
        assert_eq!(
            actual,
            expected.clone().into_iter().collect::<Vec<_>>(),
            "{context}"
        );
    }

    async fn reopen_storage_stress(
        engine: Engine,
        config: &StorageConfig,
        expected: &BTreeMap<Vec<u8>, Vec<u8>>,
        progress: StressProgress,
    ) -> Engine {
        if !progress.mode.wal_enabled() {
            let context = storage_stress_context(
                &config.data_dir,
                progress,
                engine.next_sstable_id.load(Ordering::Acquire),
            );
            engine
                .flush()
                .await
                .unwrap_or_else(|error| panic!("{context}: pre-reopen flush failed: {error}"));
        }
        drop(engine);
        let reopened = Engine::open(config.clone()).await.unwrap_or_else(|error| {
            panic!(
                "{}: reopen failed: {error}",
                storage_stress_context(&config.data_dir, progress, 0)
            )
        });
        assert_storage_stress_model(&reopened, expected, progress).await;
        reopened
    }

    async fn run_seeded_storage_stress(mode: StressMode, seed: u64, steps: u64) {
        let directory = TempDir::new().unwrap();
        let config = mode.config(directory.path());
        let initial_progress = StressProgress {
            mode,
            seed,
            step: 0,
            sequence: 0,
        };
        let mut engine = Engine::open(config.clone()).await.unwrap_or_else(|error| {
            panic!(
                "{}: initial open failed: {error}",
                storage_stress_context(directory.path(), initial_progress, 0)
            )
        });
        let mut expected = BTreeMap::new();
        let mut expected_sequence = 0_u64;
        let mut rng = StdRng::seed_from_u64(seed);
        let mut failure_index = 0_usize;

        for step in 0..steps {
            let (key_index, key, value) = stress_key_value(&mut rng, 192);
            let progress = StressProgress {
                mode,
                seed,
                step,
                sequence: expected_sequence,
            };
            let context = storage_stress_context(
                directory.path(),
                progress,
                engine.next_sstable_id.load(Ordering::Acquire),
            );

            if step >= 8 && (step - 8) % 13 == 0 {
                let marker_key = format!("stress:key:{:02}", (step as usize + 7) % 24).into_bytes();
                let marker_value = format!("failure:{failure_index}:{seed:016x}").into_bytes();
                engine
                    .insert(&marker_key, &marker_value)
                    .await
                    .unwrap_or_else(|error| {
                        panic!("{context}: failure marker insert failed: {error}")
                    });
                expected.insert(marker_key, marker_value);
                expected_sequence += 1;
                let failure_progress = StressProgress {
                    sequence: expected_sequence,
                    ..progress
                };
                let failure_context = storage_stress_context(
                    directory.path(),
                    failure_progress,
                    engine.next_sstable_id.load(Ordering::Acquire),
                );

                let boundaries = mode.failure_boundaries();
                let boundary = boundaries[failure_index % boundaries.len()];
                failure_index += 1;
                let failure = super::super::failpoints::arm(directory.path(), boundary);
                let result = engine.flush().await;
                assert!(
                    failure.was_hit(),
                    "{failure_context}: {boundary:?} was not reached"
                );
                assert!(
                    result.is_err(),
                    "{failure_context}: {boundary:?} did not fail"
                );
                assert_storage_stress_model(&engine, &expected, failure_progress).await;

                if !mode.wal_enabled() {
                    engine.flush().await.unwrap_or_else(|error| {
                        panic!("{failure_context}: retry after {boundary:?} failed: {error}")
                    });
                }
                engine = reopen_storage_stress(engine, &config, &expected, failure_progress).await;
                continue;
            }

            match step % 8 {
                0 | 7 => {
                    engine
                        .insert(&key, &value)
                        .await
                        .unwrap_or_else(|error| panic!("{context}: insert failed: {error}"));
                    expected.insert(key, value);
                    expected_sequence += 1;
                }
                1 => {
                    engine
                        .delete(&key)
                        .await
                        .unwrap_or_else(|error| panic!("{context}: delete failed: {error}"));
                    expected.remove(&key);
                    expected_sequence += 1;
                }
                2 => {
                    let second_key = format!("stress:key:{:02}", (key_index + 9) % 24).into_bytes();
                    let second_value = rng.next_u64().to_le_bytes().to_vec();
                    let entries = vec![
                        (key.clone(), value.clone()),
                        (second_key.clone(), second_value.clone()),
                    ];
                    engine
                        .insert_many(&entries)
                        .await
                        .unwrap_or_else(|error| panic!("{context}: insert_many failed: {error}"));
                    expected.insert(key, value);
                    expected.insert(second_key, second_value);
                    expected_sequence += 2;
                }
                3 => {
                    let deleted_key =
                        format!("stress:key:{:02}", (key_index + 5) % 24).into_bytes();
                    let mut batch = WriteBatch::new();
                    batch.put(&key, b"batch-prefix");
                    batch.delete(&key);
                    batch.put(&key, &value);
                    batch.delete(&deleted_key);
                    engine
                        .write_batch(&batch)
                        .await
                        .unwrap_or_else(|error| panic!("{context}: write_batch failed: {error}"));
                    expected.insert(key, value);
                    expected.remove(&deleted_key);
                    expected_sequence += 4;
                }
                4 => {
                    assert_storage_stress_model(&engine, &expected, progress).await;
                }
                5 => engine
                    .flush()
                    .await
                    .unwrap_or_else(|error| panic!("{context}: flush failed: {error}")),
                _ => {
                    engine = reopen_storage_stress(engine, &config, &expected, progress).await;
                }
            }
        }

        let final_progress = StressProgress {
            mode,
            seed,
            step: steps,
            sequence: expected_sequence,
        };
        let final_context = storage_stress_context(
            directory.path(),
            final_progress,
            engine.next_sstable_id.load(Ordering::Acquire),
        );
        engine
            .flush()
            .await
            .unwrap_or_else(|error| panic!("{final_context}: final flush failed: {error}"));
        assert_storage_stress_model(&engine, &expected, final_progress).await;
        let manifest = Manifest::load_or_create(directory.path())
            .unwrap_or_else(|error| panic!("{final_context}: manifest load failed: {error}"));
        let referenced = manifest
            .sstables
            .iter()
            .map(|table| {
                std::fs::canonicalize(&table.path).unwrap_or_else(|error| {
                    panic!(
                        "{final_context}: referenced file {} is unavailable: {error}",
                        table.path.display()
                    )
                })
            })
            .collect::<HashSet<_>>();
        assert!(
            referenced.iter().all(|path| path.is_file()),
            "{final_context}"
        );
        let discovered = discover_sstable_files(&directory.path().join("sstables"))
            .unwrap_or_else(|error| panic!("{final_context}: SSTable scan failed: {error}"))
            .into_iter()
            .map(|path| {
                std::fs::canonicalize(&path).unwrap_or_else(|error| {
                    panic!(
                        "{final_context}: discovered file {} is unavailable: {error}",
                        path.display()
                    )
                })
            })
            .collect::<HashSet<_>>();
        assert_eq!(
            discovered,
            referenced,
            "{}",
            storage_stress_context(
                directory.path(),
                final_progress,
                engine.next_sstable_id.load(Ordering::Acquire),
            )
        );
        engine
            .shutdown()
            .await
            .unwrap_or_else(|error| panic!("{final_context}: shutdown failed: {error}"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn seeded_mutation_rotation_flush_failure_and_reopen_model_covers_every_mode() {
        for (mode, seed) in [
            (StressMode::Fast, 0x29e1_7b84_c605_f3ad),
            (StressMode::Durable, 0x740c_a3d9_165e_82bf),
            (StressMode::Paranoid, 0xc5b8_4f20_e973_1da6),
        ] {
            run_seeded_storage_stress(mode, seed, 96).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "explicit storage stress repetitions"]
    async fn repeated_seeded_storage_stress() {
        for seed in [
            0x172d_90b4_e56a_c83f,
            0x4f8a_31e7_b2c6_950d,
            0xa630_d5f9_18be_274c,
        ] {
            for mode in [StressMode::Fast, StressMode::Durable, StressMode::Paranoid] {
                run_seeded_storage_stress(mode, seed, 192).await;
            }
        }
    }

    #[tokio::test]
    async fn missing_manifest_reference_reports_file_identity_without_cleanup_side_effects() {
        let directory = TempDir::new().unwrap();
        let config = StressMode::Durable.config(directory.path());
        let engine = Engine::open(config.clone()).await.unwrap();
        engine.insert(b"missing:key", b"value").await.unwrap();
        engine.flush().await.unwrap();
        engine.shutdown().await.unwrap();
        drop(engine);

        let manifest_path = directory.path().join("MANIFEST");
        let manifest_bytes = std::fs::read(&manifest_path).unwrap();
        let manifest = Manifest::load(&manifest_path).unwrap();
        let referenced = manifest.sstables[0].path.clone();
        let displaced = referenced.with_extension("sst.missing");
        std::fs::rename(&referenced, &displaced).unwrap();
        let context = format!(
            "seed=none sequence={} generation={} file={}",
            manifest.wal_checkpoint,
            manifest.sstables[0].id,
            referenced.display()
        );

        let error = match Engine::open(config.clone()).await {
            Ok(_) => panic!("{context}: missing manifest reference opened"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains(&referenced.display().to_string()),
            "{context}: {error}"
        );
        assert_eq!(std::fs::read(&manifest_path).unwrap(), manifest_bytes);
        assert!(displaced.is_file());

        std::fs::rename(&displaced, &referenced).unwrap();
        let reopened = Engine::open(config).await.unwrap();
        assert_eq!(
            reopened.get(b"missing:key").await.unwrap(),
            Some(b"value".to_vec())
        );
        reopened.shutdown().await.unwrap();
    }

    fn write_valid_orphan(path: &std::path::Path) {
        let mut writer = SSTableWriter::new(path, SSTableConfig::default()).unwrap();
        writer
            .add_versioned(b"orphan:key", Some(b"orphan:value"), 1)
            .unwrap();
        writer.finish().unwrap();
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
            tombstone_count: info.tombstone_count,
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
            tombstone_count: info.tombstone_count,
            min_key: info.min_key,
            max_key: info.max_key,
            min_sequence: 0,
            max_sequence: manifest_max_sequence,
            creation_time: id,
        }
    }

    fn write_versioned_scan_table(
        path: &std::path::Path,
        id: u64,
        entries: &[VersionedTableEntry<'_>],
    ) -> SSTableManifestEntry {
        let config = SSTableConfig {
            compression: super::super::sstable::CompressionType::None,
            ..SSTableConfig::default()
        };
        let mut writer = SSTableWriter::new(path, config).unwrap();
        for (key, value, sequence) in entries {
            writer.add_versioned(key, *value, *sequence).unwrap();
        }
        let info = writer.finish().unwrap();
        SSTableManifestEntry {
            id,
            level: 0,
            path: info.path,
            size: info.file_size,
            entry_count: info.entry_count,
            tombstone_count: info.tombstone_count,
            min_key: info.min_key,
            max_key: info.max_key,
            min_sequence: info.min_sequence,
            max_sequence: info.max_sequence,
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
            tombstone_count: 0,
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

        let engine = Engine::open(config.clone()).await.unwrap();

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
            max_immutable_memtables_before_stall: usize::MAX,
            max_l0_files_before_stall: 0,
            write_stall_micros: 1,
            ..Default::default()
        };

        let engine = Engine::open(config.clone()).await.unwrap();
        engine.insert(b"stall-key", b"stall-value").await.unwrap();

        let stats = engine.physical_stats();
        assert_eq!(stats.stalls.count_since_open, 1);
        assert_eq!(stats.stalls.micros_since_open, 1);
        let backpressure = engine.status().write_backpressure;
        assert!(backpressure.active);
        assert_eq!(backpressure.stalls_since_open, 1);
        assert_eq!(backpressure.stall_micros_since_open, 1);
        assert!(!backpressure.immutable_memtables.active);
        assert_eq!(backpressure.immutable_memtables.count_since_open, 0);
        assert!(backpressure.level_zero_files.active);
        assert_eq!(backpressure.level_zero_files.current, 0);
        assert_eq!(backpressure.level_zero_files.threshold, 0);
        assert_eq!(backpressure.level_zero_files.count_since_open, 1);
        assert_eq!(backpressure.level_zero_files.micros_since_open, 1);

        engine.shutdown().await.unwrap();
        drop(engine);

        let reopened = Engine::open(config).await.unwrap();
        assert_eq!(reopened.physical_stats().stalls, WriteStallStats::default());
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn write_stall_status_attributes_immutable_memtable_pressure() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            data_dir: temp_dir.path().to_path_buf(),
            wal_enabled: false,
            max_immutable_memtables_before_stall: 1,
            max_l0_files_before_stall: u64::MAX,
            write_stall_micros: 1,
            ..Default::default()
        };
        let engine = Engine::open(config).await.unwrap();
        engine.insert(b"frozen", b"value").await.unwrap();
        engine.flush_write_buffers().unwrap();
        engine.memtable_manager.force_rotate().unwrap();

        engine.insert(b"stalled", b"value").await.unwrap();

        let backpressure = engine.status().write_backpressure;
        assert!(backpressure.immutable_memtables.active);
        assert_eq!(backpressure.immutable_memtables.current, 1);
        assert_eq!(backpressure.immutable_memtables.threshold, 1);
        assert_eq!(backpressure.immutable_memtables.count_since_open, 1);
        assert_eq!(backpressure.immutable_memtables.micros_since_open, 1);
        assert!(!backpressure.level_zero_files.active);
        assert_eq!(backpressure.level_zero_files.count_since_open, 0);
        engine.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn write_stall_status_samples_total_and_cause_counters_coherently() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            data_dir: temp_dir.path().to_path_buf(),
            wal_enabled: false,
            max_immutable_memtables_before_stall: 0,
            max_l0_files_before_stall: 0,
            write_stall_micros: 1,
            ..Default::default()
        };
        let engine = Arc::new(Engine::open(config).await.unwrap());
        let writer_engine = Arc::clone(&engine);
        let writer = tokio::spawn(async move {
            for index in 0..200_u64 {
                writer_engine
                    .insert(&index.to_be_bytes(), b"value")
                    .await
                    .unwrap();
            }
        });

        while !writer.is_finished() {
            let status = engine.status().write_backpressure;
            assert_eq!(
                status.stalls_since_open,
                status.immutable_memtables.count_since_open
            );
            assert_eq!(
                status.stalls_since_open,
                status.level_zero_files.count_since_open
            );
            assert_eq!(
                status.stall_micros_since_open,
                status.immutable_memtables.micros_since_open
            );
            assert_eq!(
                status.stall_micros_since_open,
                status.level_zero_files.micros_since_open
            );
            tokio::task::yield_now().await;
        }
        writer.await.unwrap();
        let status = engine.status().write_backpressure;
        assert_eq!(status.stalls_since_open, 200);
        assert_eq!(status.immutable_memtables.count_since_open, 200);
        assert_eq!(status.level_zero_files.count_since_open, 200);
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn logical_and_physical_stats_survive_versions_compaction_and_reopen() {
        let directory = TempDir::new().unwrap();
        let mut config = isolated_config(directory.path(), false);
        config.compaction_config.l0_compaction_trigger = 4;
        let engine = Engine::open(config.clone()).await.unwrap();

        engine.insert(b"a", b"old").await.unwrap();
        engine.insert(b"b", b"bee").await.unwrap();
        assert_eq!(
            engine.logical_stats().await.unwrap(),
            LogicalStats {
                live_keys: 2,
                key_bytes: 2,
                value_bytes: 6,
                total_bytes: 8,
            }
        );

        engine.insert(b"a", b"new-value").await.unwrap();
        engine.delete(b"b").await.unwrap();
        engine.insert(b"c", b"").await.unwrap();
        assert_eq!(
            engine.logical_stats().await.unwrap(),
            LogicalStats {
                live_keys: 2,
                key_bytes: 2,
                value_bytes: 9,
                total_bytes: 11,
            }
        );

        let in_memory = engine.physical_stats();
        assert_eq!(in_memory.memtables.immutable_tables, 2);
        assert_eq!(in_memory.memtables.immutable_versions, 5);
        assert_eq!(in_memory.memtables.tombstones, 1);
        assert_eq!(in_memory.versions.current, 5);
        assert_eq!(in_memory.versions.tombstones, 1);
        assert!(!in_memory.wal.enabled);
        assert_eq!(in_memory.wal.retained_valid_bytes, 0);
        assert_eq!(
            in_memory.amplification.logical_bytes_ingested_since_open,
            1 + 3 + 1 + 3 + 1 + 9 + 1 + 1
        );

        engine.flush().await.unwrap();
        let after_first_flush = engine.physical_stats();
        assert_eq!(after_first_flush.sstables.files, 2);
        assert_eq!(after_first_flush.sstables.versions, 5);
        assert_eq!(after_first_flush.sstables.tombstones, 1);
        assert_eq!(after_first_flush.memtables.immutable_versions, 0);
        assert!(
            after_first_flush
                .amplification
                .flush_bytes_written_since_open
                > 0
        );

        engine.delete(b"c").await.unwrap();
        engine.insert(b"d", b"four").await.unwrap();
        engine.flush().await.unwrap();
        engine.insert(b"c", b"see").await.unwrap();
        engine.flush().await.unwrap();

        let before_compaction = engine.physical_stats();
        assert_eq!(before_compaction.sstables.files, 4);
        assert_eq!(before_compaction.sstables.versions, 8);
        assert_eq!(before_compaction.sstables.tombstones, 2);
        assert_eq!(
            before_compaction
                .amplification
                .logical_bytes_ingested_since_open,
            30
        );
        assert!(
            before_compaction
                .amplification
                .flush_bytes_written_since_open
                > after_first_flush
                    .amplification
                    .flush_bytes_written_since_open
        );
        let flushed_bytes = before_compaction
            .amplification
            .flush_bytes_written_since_open;

        engine.compact().await.unwrap();
        let after_compaction = engine.physical_stats();
        assert_eq!(after_compaction.sstables.files, 1);
        assert_eq!(after_compaction.sstables.versions, 3);
        assert_eq!(after_compaction.sstables.tombstones, 0);
        assert_eq!(after_compaction.versions.current, 3);
        assert_eq!(after_compaction.versions.tombstones, 0);
        assert_eq!(
            after_compaction.versions.reclaimed_by_compaction_since_open,
            5
        );
        assert_eq!(
            after_compaction
                .versions
                .tombstones_reclaimed_by_compaction_since_open,
            2
        );
        assert_eq!(
            after_compaction
                .amplification
                .flush_bytes_written_since_open,
            flushed_bytes
        );
        assert_eq!(
            after_compaction
                .amplification
                .compaction_input_bytes_since_open,
            before_compaction.sstables.bytes
        );
        assert_eq!(
            after_compaction
                .amplification
                .compaction_output_bytes_since_open,
            after_compaction.sstables.bytes
        );
        assert!(
            after_compaction
                .amplification
                .sstable_write_amplification()
                .unwrap()
                > 0.0
        );
        assert_eq!(
            engine.logical_stats().await.unwrap(),
            LogicalStats {
                live_keys: 3,
                key_bytes: 3,
                value_bytes: 16,
                total_bytes: 19,
            }
        );
        assert_eq!(engine.logical_stats().await.unwrap().live_keys, 3);
        let populated_cache = engine.physical_stats().cache;
        assert!(populated_cache.block_cache_enabled);
        assert!(populated_cache.block_cache_entries > 0);
        assert!(populated_cache.block_cache_hits_since_open > 0);
        assert!(populated_cache.block_cache_misses_since_open > 0);
        assert!(populated_cache.sstable_reader_hits_since_open > 0);
        assert!(populated_cache.sstable_reader_misses_since_open > 0);

        engine.shutdown().await.unwrap();
        drop(engine);

        let reopened = Engine::open(config).await.unwrap();
        let reopened_physical = reopened.physical_stats();
        assert_eq!(reopened_physical.sstables.files, 1);
        assert_eq!(
            reopened_physical.sstables.bytes,
            after_compaction.sstables.bytes
        );
        assert_eq!(reopened_physical.sstables.versions, 3);
        assert_eq!(reopened_physical.sstables.tombstones, 0);
        assert_eq!(reopened_physical.versions.current, 3);
        assert_eq!(reopened_physical.versions.tombstones, 0);
        assert_eq!(
            reopened_physical
                .versions
                .reclaimed_by_compaction_since_open,
            0
        );
        assert_eq!(
            reopened_physical
                .versions
                .tombstones_reclaimed_by_compaction_since_open,
            0
        );
        assert_eq!(
            reopened_physical
                .amplification
                .logical_bytes_ingested_since_open,
            0
        );
        assert_eq!(
            reopened_physical
                .amplification
                .flush_bytes_written_since_open,
            0
        );
        assert_eq!(
            reopened_physical
                .amplification
                .compaction_input_bytes_since_open,
            0
        );
        assert_eq!(
            reopened_physical
                .amplification
                .compaction_output_bytes_since_open,
            0
        );
        assert_eq!(reopened_physical.cache.block_cache_entries, 0);
        assert_eq!(reopened_physical.cache.block_cache_bytes, 0);
        assert_eq!(reopened_physical.cache.block_cache_hits_since_open, 0);
        assert_eq!(reopened_physical.cache.block_cache_misses_since_open, 0);
        assert_eq!(reopened_physical.cache.sstable_readers, 0);
        assert_eq!(reopened_physical.cache.sstable_reader_hits_since_open, 0);
        assert_eq!(reopened_physical.cache.sstable_reader_misses_since_open, 0);
        assert_eq!(
            reopened_physical.cache.sstable_reader_evictions_since_open,
            0
        );
        assert_eq!(
            reopened.logical_stats().await.unwrap(),
            LogicalStats {
                live_keys: 3,
                key_bytes: 3,
                value_bytes: 16,
                total_bytes: 19,
            }
        );
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn failed_compaction_counts_partial_output_once_and_removes_it() {
        let directory = TempDir::new().unwrap();
        let mut config = isolated_config(directory.path(), false);
        config.sstable_config = SSTableConfig {
            block_size: 256,
            compression: super::super::sstable::CompressionType::None,
            ..SSTableConfig::default()
        };
        let input_config = config.sstable_config.clone();
        let engine = Engine::open(config).await.unwrap();

        let input_path = directory.path().join("sstables/L0/failed-input.sst");
        let mut writer = SSTableWriter::new(&input_path, input_config).unwrap();
        for index in 0..256 {
            writer
                .add_versioned(format!("a:{index:04}").as_bytes(), Some(&[b'v'; 64]), index)
                .unwrap();
        }
        let mut input_info = writer.finish().unwrap();
        input_info.id = 99;
        {
            use std::io::{Read, Seek, SeekFrom, Write};

            let reader = SSTableReader::open(&input_path).unwrap();
            let corrupt_offset = reader.index().entries().last().unwrap().block_offset;
            drop(reader);
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&input_path)
                .unwrap();
            file.seek(SeekFrom::Start(corrupt_offset)).unwrap();
            let mut byte = [0_u8; 1];
            file.read_exact(&mut byte).unwrap();
            byte[0] ^= 0xff;
            file.seek(SeekFrom::Start(corrupt_offset)).unwrap();
            file.write_all(&byte).unwrap();
            file.sync_all().unwrap();
        }
        let input = SSTableManifestEntry {
            id: input_info.id,
            level: 0,
            path: input_info.path.clone(),
            size: input_info.file_size,
            entry_count: input_info.entry_count,
            tombstone_count: input_info.tombstone_count,
            min_key: input_info.min_key.clone(),
            max_key: input_info.max_key.clone(),
            min_sequence: input_info.min_sequence,
            max_sequence: input_info.max_sequence,
            creation_time: input_info.creation_time,
        };
        {
            let mut live = engine.sstables.write().await;
            engine
                .sstable_stats
                .install_if_absent(&mut live, &input_info);
        }

        let result = engine
            .compaction_coordinator
            .execute_job_for_test(super::super::compaction::CompactionSelection {
                input_sstables: vec![input],
                output_level: 1,
            })
            .await;
        assert!(matches!(result, Err(StorageError::Compaction(_))));

        let stats = engine.physical_stats();
        assert_eq!(
            stats.amplification.compaction_input_bytes_since_open,
            input_info.file_size
        );
        assert!(
            stats.amplification.compaction_output_bytes_since_open > 0,
            "partial output bytes remain accounted after cleanup"
        );
        assert_eq!(
            std::fs::read_dir(directory.path().join("sstables"))
                .unwrap()
                .filter_map(std::result::Result::ok)
                .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "sst"))
                .count(),
            0
        );
        assert_eq!(stats.sstables.files, 1);
        assert_eq!(stats.sstables.versions, input_info.entry_count);
        assert_eq!(stats.versions.reclaimed_by_compaction_since_open, 0);
    }

    #[tokio::test]
    async fn preexisting_compaction_output_is_not_charged_to_a_new_attempt() {
        let directory = TempDir::new().unwrap();
        let engine = Engine::open(isolated_config(directory.path(), false))
            .await
            .unwrap();
        let output_path = directory.path().join("sstables/L1/stale-output.sst");
        let stale_bytes = vec![0x5a; 127];
        std::fs::write(&output_path, &stale_bytes).unwrap();

        let result = engine
            .compaction_coordinator
            .execute_job_for_test(super::super::compaction::CompactionSelection {
                input_sstables: vec![SSTableManifestEntry {
                    id: 404,
                    level: 0,
                    path: directory.path().join("sstables/L0/missing-input.sst"),
                    size: 8_192,
                    entry_count: 1,
                    tombstone_count: 0,
                    min_key: b"a".to_vec(),
                    max_key: b"z".to_vec(),
                    min_sequence: 0,
                    max_sequence: 0,
                    creation_time: 0,
                }],
                output_level: 1,
            })
            .await;
        assert!(matches!(result, Err(StorageError::Compaction(_))));

        let stats = engine.physical_stats();
        assert_eq!(stats.amplification.compaction_input_bytes_since_open, 8_192);
        assert_eq!(stats.amplification.compaction_output_bytes_since_open, 0);
        assert_eq!(std::fs::read(output_path).unwrap(), stale_bytes);
    }

    #[tokio::test]
    async fn manifest_failed_compaction_keeps_inputs_live_and_retries_cleanly() {
        let directory = TempDir::new().unwrap();
        let mut config = isolated_config(directory.path(), false);
        config.compaction_config.l0_compaction_trigger = 2;
        config.sstable_config.compression = super::super::sstable::CompressionType::None;
        let engine = Engine::open(config.clone()).await.unwrap();
        for generation in 0..2 {
            for key in 0..128 {
                engine
                    .insert(
                        format!("retry:{key:04}").as_bytes(),
                        format!("generation-{generation}").as_bytes(),
                    )
                    .await
                    .unwrap();
            }
            engine.flush().await.unwrap();
        }
        let before = engine.physical_stats();
        let failure = super::super::failpoints::arm(
            directory.path(),
            super::super::failpoints::PersistenceBoundary::ManifestInstallation,
        );

        assert!(engine.compact().await.is_err());
        failure.assert_hit();
        let failed_health = engine.status().maintenance.compaction;
        assert!(failed_health.retry_pending);
        assert_eq!(failed_health.failures_since_open, 1);
        assert_eq!(failed_health.background_failures_since_open, 0);
        assert_eq!(
            failed_health.unresolved_failure.unwrap().origin,
            MaintenanceOrigin::Foreground
        );
        let failed = engine.physical_stats();
        assert_eq!(failed.sstables.files, 2);
        assert_eq!(failed.sstables.bytes, before.sstables.bytes);
        assert_eq!(failed.versions.reclaimed_by_compaction_since_open, 0);
        assert_eq!(
            failed.amplification.compaction_input_bytes_since_open,
            before.sstables.bytes
        );
        assert!(failed.amplification.compaction_output_bytes_since_open > 0);
        assert_eq!(
            Manifest::load_or_create(directory.path())
                .unwrap()
                .sstables
                .len(),
            2
        );
        assert!(std::fs::read_dir(directory.path().join("sstables"))
            .unwrap()
            .filter_map(|entry| entry.ok())
            .all(|entry| {
                entry
                    .path()
                    .extension()
                    .map_or(true, |extension| extension != "sst")
            }));
        for key in 0..128 {
            assert_eq!(
                engine
                    .get(format!("retry:{key:04}").as_bytes())
                    .await
                    .unwrap(),
                Some(b"generation-1".to_vec())
            );
        }

        assert_eq!(engine.compact().await.unwrap().input_files, 2);
        let retried_health = engine.status().maintenance.compaction;
        assert!(!retried_health.retry_pending);
        assert_eq!(retried_health.failures_since_open, 1);
        assert_eq!(retried_health.successful_retries_since_open, 1);
        let retried = engine.physical_stats();
        assert_eq!(retried.sstables.files, 1);
        assert_eq!(
            retried.amplification.compaction_input_bytes_since_open,
            before.sstables.bytes * 2
        );
        assert_eq!(retried.versions.reclaimed_by_compaction_since_open, 128);
        engine.shutdown().await.unwrap();
        drop(engine);

        let reopened = Engine::open(config).await.unwrap();
        assert_eq!(
            reopened.get(b"retry:0000").await.unwrap(),
            Some(b"generation-1".to_vec())
        );
        assert_eq!(reopened.physical_stats().sstables.files, 1);
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn background_flush_failure_is_bounded_registered_and_cleared_by_fifo_retry() {
        let directory = TempDir::new().unwrap();
        let engine = Engine::open(isolated_config(directory.path(), false))
            .await
            .unwrap();
        engine.insert(b"health:flush", b"value").await.unwrap();
        engine.flush_write_buffers().unwrap();
        engine.memtable_manager.force_rotate().unwrap();
        let failure = super::super::failpoints::arm(
            directory.path(),
            super::super::failpoints::PersistenceBoundary::ManifestInstallation,
        );
        let background = engine.clone_for_background();

        assert!(background.background_flush().await.is_err());
        failure.assert_hit();
        let failed = engine.status().maintenance.flush;
        assert!(failed.retry_pending);
        assert_eq!(failed.failures_since_open, 1);
        assert_eq!(failed.background_failures_since_open, 1);
        let detail = failed.unresolved_failure.unwrap();
        assert_eq!(detail.origin, MaintenanceOrigin::Background);
        assert!(detail.message.len() <= 512);
        assert!(!detail.message_truncated);
        assert_eq!(engine.memtable_manager.immutable_count(), 1);
        assert_eq!(
            engine.get(b"health:flush").await.unwrap(),
            Some(b"value".to_vec())
        );

        background.background_flush().await.unwrap();
        let retried = engine.status().maintenance.flush;
        assert!(!retried.retry_pending);
        assert_eq!(retried.failures_since_open, 1);
        assert_eq!(retried.background_failures_since_open, 1);
        assert_eq!(retried.successful_retries_since_open, 1);
        assert_eq!(engine.memtable_manager.immutable_count(), 0);
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn empty_background_retry_executes_failed_wal_sync_before_clearing_health() {
        let directory = TempDir::new().unwrap();
        let engine = Engine::open(isolated_config(directory.path(), true))
            .await
            .unwrap();
        engine.insert(b"health:wal-sync", b"value").await.unwrap();
        let failure = super::super::failpoints::arm(
            directory.path(),
            super::super::failpoints::PersistenceBoundary::WalFlush,
        );

        assert!(engine.flush().await.is_err());
        failure.assert_hit();
        assert_eq!(engine.memtable_manager.immutable_count(), 0);
        let failed = engine.status().maintenance.flush;
        assert!(failed.retry_pending);
        assert!(failed
            .unresolved_failure
            .as_ref()
            .is_some_and(|detail| detail.message.contains("WAL flush")));

        engine
            .clone_for_background()
            .background_flush()
            .await
            .unwrap();
        let retried = engine.status().maintenance.flush;
        assert!(!retried.retry_pending);
        assert_eq!(retried.successful_retries_since_open, 1);
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn failed_wal_reclamation_remains_registered_until_exact_retry() {
        let directory = TempDir::new().unwrap();
        let engine = Engine::open(isolated_config(directory.path(), true))
            .await
            .unwrap();
        engine
            .insert(b"health:wal-reclamation", b"value")
            .await
            .unwrap();
        let failure = super::super::failpoints::arm(
            directory.path(),
            super::super::failpoints::PersistenceBoundary::WalTruncation,
        );

        assert!(engine.flush().await.is_err());
        failure.assert_hit();
        assert_eq!(engine.memtable_manager.immutable_count(), 1);
        let failed = engine.status().maintenance.flush;
        assert!(failed.retry_pending);
        assert!(failed
            .unresolved_failure
            .as_ref()
            .is_some_and(|detail| detail.message.contains("WAL truncation")));

        engine
            .clone_for_background()
            .background_flush()
            .await
            .unwrap();
        assert_eq!(engine.memtable_manager.immutable_count(), 0);
        assert!(!engine.status().maintenance.flush.retry_pending);
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_final_flush_resolves_registered_background_flush_failure() {
        let directory = TempDir::new().unwrap();
        let config = isolated_config(directory.path(), false);
        let engine = Engine::open(config.clone()).await.unwrap();
        engine
            .insert(b"shutdown:flush-health", b"value")
            .await
            .unwrap();
        engine.flush_write_buffers().unwrap();
        engine.memtable_manager.force_rotate().unwrap();
        let failure = super::super::failpoints::arm(
            directory.path(),
            super::super::failpoints::PersistenceBoundary::ManifestInstallation,
        );

        assert!(engine
            .clone_for_background()
            .background_flush()
            .await
            .is_err());
        failure.assert_hit();
        assert!(engine.status().maintenance.flush.retry_pending);

        engine.shutdown().await.unwrap();
        let resolved = engine.status().maintenance.flush;
        assert!(!resolved.retry_pending);
        assert_eq!(resolved.successful_retries_since_open, 1);
        drop(engine);

        let reopened = Engine::open(config).await.unwrap();
        assert_eq!(
            reopened.get(b"shutdown:flush-health").await.unwrap(),
            Some(b"value".to_vec())
        );
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_flush_keeps_its_generation_registered_until_retry() {
        let directory = TempDir::new().unwrap();
        let engine = Arc::new(
            Engine::open(isolated_config(directory.path(), false))
                .await
                .unwrap(),
        );
        engine.insert(b"cancel:flush", b"value").await.unwrap();
        let manifest = engine.manifest.lock().await;
        let flushing_engine = Arc::clone(&engine);
        let flush = tokio::spawn(async move { flushing_engine.flush().await });
        tokio::time::timeout(Duration::from_secs(5), async {
            while engine.memtable_manager.immutable_count() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("flush must freeze a generation before manifest publication");

        flush.abort();
        assert!(flush.await.unwrap_err().is_cancelled());
        drop(manifest);
        assert_eq!(engine.memtable_manager.immutable_count(), 1);
        assert_eq!(
            engine.get(b"cancel:flush").await.unwrap(),
            Some(b"value".to_vec())
        );
        let cancelled = engine.status().maintenance.flush;
        assert!(cancelled.retry_pending);
        assert!(cancelled
            .unresolved_failure
            .unwrap()
            .message
            .contains("cancelled"));

        engine.flush().await.unwrap();
        assert_eq!(engine.memtable_manager.immutable_count(), 0);
        assert!(!engine.status().maintenance.flush.retry_pending);
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn physical_stats_separate_active_immutable_and_sstable_versions() {
        let directory = TempDir::new().unwrap();
        let mut config = isolated_config(directory.path(), false);
        config.block_cache_size = 0;
        let engine = Engine::open(config).await.unwrap();

        engine.insert(b"disk", b"value").await.unwrap();
        engine.flush_write_buffers().unwrap();
        engine.flush().await.unwrap();
        engine.delete(b"immutable").await.unwrap();
        engine.flush_write_buffers().unwrap();
        engine.memtable_manager.force_rotate().unwrap();
        engine.insert(b"active", b"value").await.unwrap();
        engine.flush_write_buffers().unwrap();

        let physical = engine.physical_stats();
        assert_eq!(physical.sstables.versions, 1);
        assert_eq!(physical.sstables.tombstones, 0);
        assert_eq!(physical.memtables.immutable_tables, 1);
        assert_eq!(physical.memtables.immutable_versions, 1);
        assert_eq!(physical.memtables.active_versions, 1);
        assert_eq!(physical.memtables.tombstones, 1);
        assert_eq!(physical.versions.current, 3);
        assert_eq!(physical.versions.tombstones, 1);
        assert!(!physical.cache.block_cache_enabled);
        assert_eq!(physical.cache.block_cache_entries, 0);
        assert_eq!(physical.cache.block_cache_bytes, 0);
        assert_eq!(engine.logical_stats().await.unwrap().live_keys, 2);

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn physical_memtable_stats_follow_overwrite_and_tombstone_replacements() {
        let directory = TempDir::new().unwrap();
        let engine = Engine::open(isolated_config(directory.path(), false))
            .await
            .unwrap();

        engine.delete(b"key").await.unwrap();
        let missing_delete = engine.physical_stats();
        assert_eq!(missing_delete.memtables.active_versions, 1);
        assert_eq!(missing_delete.memtables.tombstones, 1);

        engine.delete(b"key").await.unwrap();
        let repeated_delete = engine.physical_stats();
        assert_eq!(repeated_delete.memtables.active_versions, 1);
        assert_eq!(repeated_delete.memtables.tombstones, 1);

        engine.insert(b"key", b"value").await.unwrap();
        engine.flush_write_buffers().unwrap();
        let tombstone_to_value = engine.physical_stats();
        assert_eq!(tombstone_to_value.memtables.active_versions, 1);
        assert_eq!(tombstone_to_value.memtables.tombstones, 0);

        engine.insert(b"key", b"new-value").await.unwrap();
        engine.flush_write_buffers().unwrap();
        let overwritten = engine.physical_stats();
        assert_eq!(overwritten.memtables.active_versions, 1);
        assert_eq!(overwritten.memtables.tombstones, 0);

        engine.delete(b"key").await.unwrap();
        let value_to_tombstone = engine.physical_stats();
        assert_eq!(value_to_tombstone.memtables.active_versions, 1);
        assert_eq!(value_to_tombstone.memtables.tombstones, 1);
        assert_eq!(engine.logical_stats().await.unwrap().live_keys, 0);
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn fast_mode_stats_include_acknowledged_thread_buffer_mutations() {
        const BUFFER_CAPACITY: u64 = 64;

        let directory = TempDir::new().unwrap();
        let mut config = isolated_config(directory.path(), false);
        config.memtable_config.max_entries = 2;
        let engine = Engine::open(config).await.unwrap();
        let key = b"buffered";
        let value = b"value";
        let entry_bytes = (key.len() + value.len() + 32) as u64;

        for _ in 0..BUFFER_CAPACITY - 1 {
            engine.insert(key, value).await.unwrap();
        }
        let buffered = engine.physical_stats();
        assert_eq!(buffered.memtables.buffered_versions, BUFFER_CAPACITY - 1);
        assert_eq!(
            buffered.memtables.buffered_bytes,
            (BUFFER_CAPACITY - 1) * entry_bytes
        );
        assert_eq!(buffered.memtables.active_versions, 0);
        assert_eq!(buffered.versions.current, BUFFER_CAPACITY - 1);

        engine.insert(key, value).await.unwrap();
        let published = engine.physical_stats();
        assert_eq!(published.memtables.buffered_versions, 0);
        assert_eq!(published.memtables.buffered_bytes, 0);
        assert_eq!(published.memtables.active_versions, 1);
        assert_eq!(published.memtables.active_bytes, entry_bytes);
        assert_eq!(published.memtables.immutable_tables, 0);
        assert_eq!(published.versions.current, 1);
        assert_eq!(engine.logical_stats().await.unwrap().live_keys, 1);
        engine.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_fast_mode_samples_never_omit_acknowledged_buffered_work() {
        const WRITERS: usize = 4;
        const WRITES_PER_WRITER: usize = 1_024;

        let directory = TempDir::new().unwrap();
        let engine = Arc::new(
            Engine::open(isolated_config(directory.path(), false))
                .await
                .unwrap(),
        );
        engine.insert(b"anchor", b"value").await.unwrap();

        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let sampled = Arc::new(AtomicU64::new(0));
        let sampler_engine = Arc::clone(&engine);
        let sampler_done = Arc::clone(&done);
        let sampler_count = Arc::clone(&sampled);
        let sampler = tokio::spawn(async move {
            while !sampler_done.load(Ordering::Acquire) {
                let stats = sampler_engine.physical_stats();
                assert!(stats.versions.current >= 1);
                sampler_count.fetch_add(1, Ordering::Relaxed);
                tokio::task::yield_now().await;
            }
        });

        let writers = (0..WRITERS)
            .map(|writer| {
                let engine = Arc::clone(&engine);
                tokio::spawn(async move {
                    for index in 0..WRITES_PER_WRITER {
                        engine
                            .insert(format!("writer:{writer}:{index}").as_bytes(), b"value")
                            .await
                            .unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();
        for writer in writers {
            writer.await.unwrap();
        }
        done.store(true, Ordering::Release);
        sampler.await.unwrap();
        assert!(sampled.load(Ordering::Relaxed) > 0);

        let final_stats = engine.physical_stats();
        assert_eq!(
            final_stats.versions.current,
            (1 + WRITERS * WRITES_PER_WRITER) as u64
        );
        assert_eq!(
            engine.logical_stats().await.unwrap().live_keys,
            (1 + WRITERS * WRITES_PER_WRITER) as u64
        );
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn wal_replay_reconstructs_replacement_counts_without_inflation() {
        let directory = TempDir::new().unwrap();
        let config = isolated_config(directory.path(), true);
        let engine = Engine::open(config.clone()).await.unwrap();

        engine.delete(b"replayed").await.unwrap();
        engine.delete(b"replayed").await.unwrap();
        engine.insert(b"replayed", b"first").await.unwrap();
        engine.insert(b"replayed", b"second").await.unwrap();
        engine.delete(b"replayed").await.unwrap();
        engine.delete(b"replayed").await.unwrap();
        engine.insert(b"replayed", b"final").await.unwrap();

        let before = engine.physical_stats();
        assert_eq!(before.memtables.active_versions, 1);
        assert_eq!(before.memtables.tombstones, 0);
        assert!(before.wal.bytes_written_since_open > 0);
        engine.wal.as_ref().unwrap().flush().await.unwrap();
        drop(engine);

        let reopened = Engine::open(config).await.unwrap();
        let after = reopened.physical_stats();
        assert_eq!(after.memtables.active_versions, 1);
        assert_eq!(after.memtables.tombstones, 0);
        assert_eq!(after.versions.current, 1);
        assert_eq!(after.versions.tombstones, 0);
        assert_eq!(after.wal.bytes_written_since_open, 0);
        assert_eq!(
            reopened.get(b"replayed").await.unwrap(),
            Some(b"final".to_vec())
        );
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn wal_stats_separate_retained_gauges_from_counters_that_reset_on_reopen() {
        let directory = TempDir::new().unwrap();
        let config = isolated_config(directory.path(), true);
        let engine = Engine::open(config.clone()).await.unwrap();

        engine.insert(b"wal-key", b"wal-value").await.unwrap();
        let written = engine.physical_stats();
        assert!(written.wal.enabled);
        assert!(written.wal.active_segment_bytes > 0);
        assert!(written.wal.retained_valid_bytes >= written.wal.active_segment_bytes);
        assert_eq!(
            written.wal.bytes_written_since_open,
            wal_data_entry_size(b"wal-key", b"wal-value")
        );
        assert_eq!(
            written.amplification.logical_bytes_ingested_since_open,
            (b"wal-key".len() + b"wal-value".len()) as u64
        );
        assert_eq!(
            written.amplification.wal_bytes_written_since_open,
            written.wal.bytes_written_since_open
        );

        engine.shutdown().await.unwrap();
        drop(engine);

        let reopened = Engine::open(config).await.unwrap();
        let reopened_stats = reopened.physical_stats();
        assert!(reopened_stats.wal.retained_valid_bytes > 0);
        assert_eq!(reopened_stats.wal.bytes_written_since_open, 0);
        assert_eq!(
            reopened_stats
                .amplification
                .logical_bytes_ingested_since_open,
            0
        );
        assert_eq!(reopened.logical_stats().await.unwrap().live_keys, 1);
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn legacy_manifests_are_recounted_and_persistently_upgraded() {
        for legacy_version in [1_u32, 2_u32] {
            let directory = TempDir::new().unwrap();
            let sstable_directory = directory.path().join("sstables");
            std::fs::create_dir_all(&sstable_directory).unwrap();
            let path = sstable_directory.join("legacy.sst");
            let mut writer = SSTableWriter::new(
                &path,
                SSTableConfig {
                    compression: super::super::sstable::CompressionType::None,
                    ..SSTableConfig::default()
                },
            )
            .unwrap();
            writer.add_versioned(b"live", Some(b"value"), 1).unwrap();
            writer.add_versioned(b"removed", None, 2).unwrap();
            let info = writer.finish().unwrap();
            let mut manifest = Manifest::new();
            manifest.sstables.push(SSTableManifestEntry {
                id: 1,
                level: 0,
                path: info.path,
                size: info.file_size,
                entry_count: info.entry_count,
                tombstone_count: 0,
                min_key: info.min_key,
                max_key: info.max_key,
                min_sequence: info.min_sequence,
                max_sequence: info.max_sequence,
                creation_time: info.creation_time,
            });
            manifest
                .save_legacy_for_test(directory.path(), legacy_version)
                .unwrap();

            let engine = Engine::open(isolated_config(directory.path(), false))
                .await
                .unwrap();
            let stats = engine.physical_stats();
            assert_eq!(stats.sstables.versions, 2);
            assert_eq!(stats.sstables.tombstones, 1);
            engine.shutdown().await.unwrap();
            drop(engine);

            let upgraded = Manifest::load_or_create(directory.path()).unwrap();
            assert_eq!(upgraded.loaded_format_version, u64::from(MANIFEST_VERSION));
            assert_eq!(upgraded.sstables[0].tombstone_count, 1);

            let reopened = Engine::open(isolated_config(directory.path(), false))
                .await
                .unwrap();
            assert_eq!(reopened.physical_stats().sstables.tombstones, 1);
            reopened.shutdown().await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn logical_stats_never_observe_an_atomic_batch_prefix() {
        const KEYS: usize = 512;

        let directory = TempDir::new().unwrap();
        let engine = Arc::new(
            Engine::open(isolated_config(directory.path(), false))
                .await
                .unwrap(),
        );
        let mut batch = WriteBatch::with_capacity(KEYS);
        for key in 0..KEYS {
            batch.put(format!("stats:{key:04}"), b"value");
        }

        let scanning = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let scanner_started = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let scanner_engine = Arc::clone(&engine);
        let scanner_flag = Arc::clone(&scanning);
        let scanner_started_flag = Arc::clone(&scanner_started);
        let scanner = tokio::spawn(async move {
            let mut observations = 0;
            while scanner_flag.load(Ordering::Acquire) {
                let stats = scanner_engine.logical_stats().await.unwrap();
                assert!(
                    stats.live_keys == 0 || stats.live_keys == KEYS as u64,
                    "logical snapshot observed {} operations from an atomic batch",
                    stats.live_keys
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
        assert_eq!(engine.logical_stats().await.unwrap().live_keys, KEYS as u64);
        engine.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn physical_sstable_gauges_publish_consistently_during_compaction() {
        const KEYS: usize = 512;

        let directory = TempDir::new().unwrap();
        let mut config = isolated_config(directory.path(), false);
        config.compaction_config.l0_compaction_trigger = 4;
        let engine = Arc::new(Engine::open(config).await.unwrap());
        for generation in 0..4 {
            for key in 0..KEYS {
                engine
                    .insert(
                        format!("gauge:{key:04}").as_bytes(),
                        format!("generation-{generation}").as_bytes(),
                    )
                    .await
                    .unwrap();
            }
            engine.flush().await.unwrap();
        }
        assert_eq!(engine.physical_stats().sstables.versions, (4 * KEYS) as u64);

        let compacting_engine = Arc::clone(&engine);
        let compaction = tokio::spawn(async move { compacting_engine.compact().await.unwrap() });
        let mut observations = 0;
        while !compaction.is_finished() {
            let stats = engine.physical_stats();
            assert_eq!(stats.versions.current, stats.sstables.versions);
            assert_eq!(stats.versions.tombstones, stats.sstables.tombstones);
            assert!(
                (stats.sstables.files == 4 && stats.sstables.versions == (4 * KEYS) as u64)
                    || (stats.sstables.files == 1 && stats.sstables.versions == KEYS as u64)
            );
            observations += 1;
            tokio::task::yield_now().await;
        }
        compaction.await.unwrap();
        assert!(observations > 0);
        let settled = engine.physical_stats();
        assert_eq!(settled.sstables.files, 1);
        assert_eq!(settled.sstables.versions, KEYS as u64);
        assert_eq!(
            settled.versions.reclaimed_by_compaction_since_open,
            (3 * KEYS) as u64
        );
        engine.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn blocking_compaction_work_does_not_stall_a_current_thread_runtime() {
        let directory = TempDir::new().unwrap();
        let mut config = isolated_config(directory.path(), false);
        config.compaction_config.l0_compaction_trigger = 2;
        config.sstable_config.compression = super::super::sstable::CompressionType::None;
        let engine = Arc::new(Engine::open(config).await.unwrap());
        for generation in 0..2 {
            for key in 0..128 {
                engine
                    .insert(
                        format!("runtime:{key:04}").as_bytes(),
                        format!("generation-{generation}").as_bytes(),
                    )
                    .await
                    .unwrap();
            }
            engine.flush().await.unwrap();
        }

        let during_manifest = engine
            .compaction_coordinator
            .gate_during_manifest_for_test();
        let progress = Arc::new(AtomicU64::new(0));
        let stop_ticker = Arc::new(AtomicU64::new(0));
        let manifest_waiter_started = Arc::new(AtomicU64::new(0));
        let ticker_progress = Arc::clone(&progress);
        let ticker_stop = Arc::clone(&stop_ticker);
        let ticker = tokio::spawn(async move {
            while ticker_stop.load(Ordering::Acquire) == 0 {
                ticker_progress.fetch_add(1, Ordering::AcqRel);
                tokio::task::yield_now().await;
            }
        });
        tokio::task::yield_now().await;

        // An external observer always releases the synchronous test gate. It
        // samples only after another async task has tried to acquire the
        // manifest lock held by compaction, proving both the blocking-pool
        // placement and non-blocking lock contention.
        let observer_gate = Arc::clone(&during_manifest);
        let observer_progress = Arc::clone(&progress);
        let observer_waiter_started = Arc::clone(&manifest_waiter_started);
        let observer = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while !observer_gate.reached() && std::time::Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(1));
            }
            if !observer_gate.reached() {
                observer_gate.release();
                return None;
            }
            let waiter_deadline = std::time::Instant::now() + Duration::from_secs(2);
            while observer_waiter_started.load(Ordering::Acquire) == 0
                && std::time::Instant::now() < waiter_deadline
            {
                std::thread::sleep(Duration::from_millis(1));
            }
            let waiter_started = observer_waiter_started.load(Ordering::Acquire) != 0;
            let before = observer_progress.load(Ordering::Acquire);
            std::thread::sleep(Duration::from_millis(100));
            let after = observer_progress.load(Ordering::Acquire);
            observer_gate.release();
            Some((waiter_started, before, after))
        });

        let compacting_engine = Arc::clone(&engine);
        let compaction = tokio::spawn(async move { compacting_engine.compact().await });
        tokio::time::timeout(Duration::from_secs(3), async {
            while !during_manifest.reached() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("compaction must reach manifest installation");

        manifest_waiter_started.store(1, Ordering::Release);
        let manifest = Arc::clone(&engine.manifest);
        let manifest_waiter = tokio::spawn(async move {
            let _manifest = manifest.lock().await;
        });
        tokio::task::yield_now().await;
        assert!(!manifest_waiter.is_finished());

        let result = compaction.await.unwrap().unwrap();
        manifest_waiter.await.unwrap();
        stop_ticker.store(1, Ordering::Release);
        ticker.await.unwrap();
        let (waiter_started, before, after) = observer
            .join()
            .unwrap()
            .expect("compaction must reach the manifest blocking boundary");

        assert_eq!(result.input_files, 2);
        assert!(
            waiter_started,
            "the manifest waiter must contend at the gate"
        );
        assert!(
            after > before,
            "another task must progress while compaction blocks on file work"
        );
        engine.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn foreground_and_background_compaction_share_one_deterministic_claim() {
        let directory = TempDir::new().unwrap();
        let mut config = isolated_config(directory.path(), false);
        config.compaction_config.l0_compaction_trigger = 4;
        config.sstable_config.compression = super::super::sstable::CompressionType::None;
        let engine = Arc::new(Engine::open(config).await.unwrap());
        for generation in 0..4 {
            for key in 0..256 {
                engine
                    .insert(
                        format!("race:{key:04}").as_bytes(),
                        format!("generation-{generation}").as_bytes(),
                    )
                    .await
                    .unwrap();
            }
            engine.flush().await.unwrap();
        }
        let selected_bytes = engine.physical_stats().sstables.bytes;

        let manual_engine = Arc::clone(&engine);
        let manual = tokio::spawn(async move { manual_engine.compact().await.unwrap() });
        let background = engine.clone_for_background();
        let background = tokio::spawn(async move { background.compact().await.unwrap() });
        let mut outcomes = vec![
            manual.await.unwrap().input_files,
            background.await.unwrap().input_files,
        ];
        outcomes.sort_unstable();

        assert_eq!(outcomes, vec![0, 4]);
        assert_eq!(engine.physical_stats().sstables.files, 1);
        assert_eq!(
            engine
                .physical_stats()
                .amplification
                .compaction_input_bytes_since_open,
            selected_bytes
        );
        assert_eq!(engine.physical_stats().compactions_in_progress, 0);
        engine.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn flush_published_during_compaction_is_preserved_in_manifest_live_list_and_stats() {
        let directory = TempDir::new().unwrap();
        let mut config = isolated_config(directory.path(), false);
        config.compaction_config.l0_compaction_trigger = 2;
        config.sstable_config.compression = super::super::sstable::CompressionType::None;
        let engine = Arc::new(Engine::open(config).await.unwrap());
        for generation in 0..2 {
            engine
                .insert(
                    b"flush-race:old",
                    format!("generation-{generation}").as_bytes(),
                )
                .await
                .unwrap();
            engine.flush().await.unwrap();
        }
        let selected_bytes = engine.physical_stats().sstables.bytes;
        let before_manifest = engine
            .compaction_coordinator
            .gate_before_manifest_for_test();
        let compacting_engine = Arc::clone(&engine);
        let compaction = tokio::spawn(async move { compacting_engine.compact().await.unwrap() });
        tokio::time::timeout(Duration::from_secs(5), async {
            while !before_manifest.reached() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("compaction must reach manifest publication");

        engine
            .insert(b"flush-race:new", b"arrived-during-compaction")
            .await
            .unwrap();
        engine.flush().await.unwrap();
        assert_eq!(engine.sstables.read().await.len(), 3);
        before_manifest.release();
        assert_eq!(compaction.await.unwrap().input_files, 2);

        let manifest = Manifest::load_or_create(directory.path()).unwrap();
        assert_eq!(manifest.sstables.len(), 2);
        assert_eq!(engine.sstables.read().await.len(), 2);
        assert_eq!(engine.physical_stats().sstables.files, 2);
        assert_eq!(
            engine
                .physical_stats()
                .amplification
                .compaction_input_bytes_since_open,
            selected_bytes
        );
        assert_eq!(
            engine.get(b"flush-race:old").await.unwrap(),
            Some(b"generation-1".to_vec())
        );
        assert_eq!(
            engine.get(b"flush-race:new").await.unwrap(),
            Some(b"arrived-during-compaction".to_vec())
        );
        engine.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn manual_scope_terminates_while_unrelated_flushes_keep_arriving() {
        let directory = TempDir::new().unwrap();
        let mut config = isolated_config(directory.path(), false);
        config.compaction_config.l0_compaction_trigger = 4;
        config.sstable_config.compression = super::super::sstable::CompressionType::None;
        let engine = Arc::new(Engine::open(config).await.unwrap());
        for generation in 0..4 {
            engine
                .insert(
                    format!("a:initial:{generation}").as_bytes(),
                    b"initial-scope",
                )
                .await
                .unwrap();
            engine.flush().await.unwrap();
        }

        let before_manifest = engine
            .compaction_coordinator
            .gate_before_manifest_for_test();
        let compacting_engine = Arc::clone(&engine);
        let compaction = tokio::spawn(async move { compacting_engine.compact().await.unwrap() });
        tokio::time::timeout(Duration::from_secs(5), async {
            while !before_manifest.reached() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("manual compaction must reach its first publication");

        for generation in 0..4 {
            engine
                .insert(
                    format!("z:arrival:{generation:04}").as_bytes(),
                    b"outside-manual-scope",
                )
                .await
                .unwrap();
            engine.flush().await.unwrap();
        }
        let keep_flushing = Arc::new(AtomicBool::new(true));
        let flusher_flag = Arc::clone(&keep_flushing);
        let flusher_engine = Arc::clone(&engine);
        let flusher = tokio::spawn(async move {
            let mut generation = 4_u64;
            while flusher_flag.load(Ordering::Acquire) {
                flusher_engine
                    .insert(
                        format!("z:arrival:{generation:04}").as_bytes(),
                        b"outside-manual-scope",
                    )
                    .await
                    .unwrap();
                flusher_engine.flush().await.unwrap();
                generation += 1;
                tokio::task::yield_now().await;
            }
        });
        before_manifest.release();

        let result = tokio::time::timeout(Duration::from_secs(5), compaction)
            .await
            .expect("unrelated flush arrivals must not extend the manual scope")
            .unwrap();
        keep_flushing.store(false, Ordering::Release);
        flusher.await.unwrap();

        assert_eq!(result.input_files, 4);
        assert_eq!(result.output_files, 1);
        assert!(result.work_remaining);
        assert!(!result.is_complete());
        assert_eq!(engine.physical_stats().compactions_in_progress, 0);
        assert_eq!(
            engine.get(b"a:initial:0").await.unwrap(),
            Some(b"initial-scope".to_vec())
        );
        assert_eq!(
            engine.get(b"z:arrival:0000").await.unwrap(),
            Some(b"outside-manual-scope".to_vec())
        );
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn manual_drain_splits_orders_and_recompacts_outputs_without_losing_winners() {
        const KEYS: usize = 160;

        let directory = TempDir::new().unwrap();
        let mut config = isolated_config(directory.path(), false);
        config.compaction_config = super::super::compaction::CompactionConfig {
            l0_compaction_trigger: 2,
            max_levels: 3,
            level_size_multiplier: 1,
            target_file_size: 20 * 1024,
        };
        config.sstable_config = SSTableConfig {
            block_size: 512,
            compression: super::super::sstable::CompressionType::Lz4,
            ..SSTableConfig::default()
        };
        let engine = Engine::open(config.clone()).await.unwrap();

        for key_index in 0..KEYS {
            let key = [0, (key_index >> 8) as u8, key_index as u8, 0xff];
            let value = (0_usize..192)
                .map(|offset| key_index.wrapping_mul(131).wrapping_add(offset) as u8)
                .collect::<Vec<_>>();
            engine.insert(&key, &value).await.unwrap();
        }
        engine.flush().await.unwrap();
        for key_index in 0..KEYS {
            let key = [0, (key_index >> 8) as u8, key_index as u8, 0xff];
            if key_index % 17 == 0 {
                engine.delete(&key).await.unwrap();
            } else {
                let value = (0_usize..192)
                    .map(|offset| {
                        key_index
                            .wrapping_mul(197)
                            .wrapping_add(offset.wrapping_mul(7)) as u8
                    })
                    .collect::<Vec<_>>();
                engine.insert(&key, &value).await.unwrap();
            }
        }
        engine.flush().await.unwrap();

        let result = engine.compact().await.unwrap();
        assert!(
            result.input_files > 2,
            "manual drain must consume split descendants"
        );
        assert!(result.output_files > 2);
        assert_eq!(
            result.bytes_reclaimed,
            result.bytes_read.saturating_sub(result.bytes_written)
        );
        assert!(result.is_complete());

        let manifest = Manifest::load_or_create(directory.path()).unwrap();
        assert!(manifest.sstables.len() > 1);
        assert!(manifest.sstables.iter().any(|table| table.level == 2));
        assert!(manifest.sstables.iter().all(|table| table.level > 0));
        let mut ordered = manifest.sstables.clone();
        ordered.sort_by(|left, right| left.min_key.cmp(&right.min_key));
        for adjacent in ordered.windows(2) {
            assert!(adjacent[0].max_key < adjacent[1].min_key);
        }
        assert!(ordered
            .iter()
            .all(|table| table.size <= config.compaction_config.target_file_size));

        let stats = engine.physical_stats();
        assert_eq!(
            stats.amplification.compaction_input_bytes_since_open,
            result.bytes_read
        );
        assert_eq!(
            stats.amplification.compaction_output_bytes_since_open,
            result.bytes_written
        );
        assert_eq!(stats.sstables.files, ordered.len() as u64);
        let expected_pairs = (0..KEYS)
            .filter(|key_index| key_index % 17 != 0)
            .map(|key_index| {
                let key = vec![0, (key_index >> 8) as u8, key_index as u8, 0xff];
                let value = (0_usize..192)
                    .map(|offset| {
                        key_index
                            .wrapping_mul(197)
                            .wrapping_add(offset.wrapping_mul(7)) as u8
                    })
                    .collect::<Vec<_>>();
                (key, value)
            })
            .collect::<Vec<_>>();
        assert_eq!(engine.range(&[0], &[1]).await.unwrap(), expected_pairs);
        for key_index in 0..KEYS {
            let key = [0, (key_index >> 8) as u8, key_index as u8, 0xff];
            let value = engine.get(&key).await.unwrap();
            if key_index % 17 == 0 {
                assert_eq!(value, None);
            } else {
                assert_eq!(
                    value,
                    Some(expected_pairs[key_index - key_index.div_ceil(17)].1.clone())
                );
            }
        }
        engine.shutdown().await.unwrap();
        drop(engine);

        let reopened = Engine::open(config).await.unwrap();
        assert_eq!(reopened.range(&[0], &[1]).await.unwrap(), expected_pairs);
        for key_index in 0..KEYS {
            let key = [0, (key_index >> 8) as u8, key_index as u8, 0xff];
            let expected = (key_index % 17 != 0).then(|| {
                (0_usize..192)
                    .map(|offset| {
                        key_index
                            .wrapping_mul(197)
                            .wrapping_add(offset.wrapping_mul(7)) as u8
                    })
                    .collect::<Vec<_>>()
            });
            assert_eq!(reopened.get(&key).await.unwrap(), expected);
        }
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn repeated_concurrent_manual_requests_execute_one_input_set_once() {
        let directory = TempDir::new().unwrap();
        let mut config = isolated_config(directory.path(), false);
        config.compaction_config.l0_compaction_trigger = 4;
        config.sstable_config.compression = super::super::sstable::CompressionType::None;
        let engine = Arc::new(Engine::open(config).await.unwrap());
        for generation in 0..4 {
            for key in 0..256 {
                engine
                    .insert(
                        format!("manual:{key:04}").as_bytes(),
                        format!("generation-{generation}").as_bytes(),
                    )
                    .await
                    .unwrap();
            }
            engine.flush().await.unwrap();
        }
        let selected_bytes = engine.physical_stats().sstables.bytes;
        let mut requests = Vec::new();
        for _ in 0..12 {
            let request_engine = Arc::clone(&engine);
            requests.push(tokio::spawn(async move {
                request_engine.compact().await.unwrap().input_files
            }));
        }
        let mut outcomes = Vec::new();
        for request in requests {
            outcomes.push(request.await.unwrap());
        }
        outcomes.sort_unstable();

        assert_eq!(outcomes, [vec![0; 11], vec![4]].concat());
        let stats = engine.physical_stats();
        assert_eq!(stats.sstables.files, 1);
        assert_eq!(
            stats.amplification.compaction_input_bytes_since_open,
            selected_bytes
        );
        assert_eq!(stats.compactions_in_progress, 0);
        engine.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancelled_queued_requests_leave_the_coordinator_queue() {
        let directory = TempDir::new().unwrap();
        let engine = Arc::new(
            Engine::open(isolated_config(directory.path(), false))
                .await
                .unwrap(),
        );
        let ownership = engine
            .compaction_coordinator
            .hold_ownership_for_test()
            .await;
        let mut cancelled = Vec::new();
        for _ in 0..32 {
            let request_engine = Arc::clone(&engine);
            cancelled.push(tokio::spawn(async move { request_engine.compact().await }));
        }
        tokio::task::yield_now().await;
        for request in &cancelled {
            request.abort();
        }
        let surviving_engine = Arc::clone(&engine);
        let surviving = tokio::spawn(async move { surviving_engine.compact().await.unwrap() });
        drop(ownership);

        let result = tokio::time::timeout(Duration::from_secs(2), surviving)
            .await
            .expect("cancelled waiters must not starve the next request")
            .unwrap();
        assert_eq!(result.input_files, 0);
        for request in cancelled {
            match request.await {
                Ok(Ok(result)) => assert_eq!(result.input_files, 0),
                Err(error) => assert!(error.is_cancelled()),
                Ok(Err(error)) => panic!("cancelled request failed unexpectedly: {error}"),
            }
        }
        engine.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn compaction_duration_includes_time_waiting_for_coordinator_ownership() {
        let directory = TempDir::new().unwrap();
        let engine = Arc::new(
            Engine::open(isolated_config(directory.path(), false))
                .await
                .unwrap(),
        );
        let ownership = engine
            .compaction_coordinator
            .hold_ownership_for_test()
            .await;
        let waiting_engine = Arc::clone(&engine);
        let request = tokio::spawn(async move { waiting_engine.compact().await.unwrap() });
        tokio::time::sleep(Duration::from_millis(60)).await;
        drop(ownership);

        let result = request.await.unwrap();
        assert!(result.duration_ms >= 50);
        assert!(result.is_complete());
        engine.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shutdown_drains_an_owner_after_its_waiter_is_aborted_and_rejects_new_requests() {
        let directory = TempDir::new().unwrap();
        let mut config = isolated_config(directory.path(), false);
        config.compaction_config.l0_compaction_trigger = 4;
        config.sstable_config = SSTableConfig {
            block_size: 512,
            compression: super::super::sstable::CompressionType::None,
            ..SSTableConfig::default()
        };
        let engine = Arc::new(Engine::open(config.clone()).await.unwrap());
        for generation in 0..4 {
            for key in 0..128 {
                engine
                    .insert(
                        format!("shutdown:{key:05}").as_bytes(),
                        format!("generation-{generation:02}-{:064}", key).as_bytes(),
                    )
                    .await
                    .unwrap();
            }
            if generation == 0 {
                engine.insert(b"shutdown:deleted", b"old").await.unwrap();
            } else if generation == 3 {
                engine.delete(b"shutdown:deleted").await.unwrap();
            }
            engine.flush().await.unwrap();
        }
        let after_manifest = engine.compaction_coordinator.gate_after_manifest_for_test();

        let owner_engine = Arc::clone(&engine);
        let owner = tokio::spawn(async move { owner_engine.compact().await });
        tokio::time::timeout(Duration::from_secs(5), async {
            while !after_manifest.reached() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("compaction must durably publish its manifest candidate");
        assert_eq!(
            Manifest::load_or_create(directory.path())
                .unwrap()
                .sstables
                .len(),
            1
        );
        assert_eq!(engine.sstables.read().await.len(), 4);
        assert_eq!(
            engine.get(b"shutdown:00000").await.unwrap(),
            Some(format!("generation-03-{:064}", 0).into_bytes())
        );
        owner.abort();

        let shutdown_engine = Arc::clone(&engine);
        let shutdown = tokio::spawn(async move { shutdown_engine.shutdown().await });
        tokio::time::timeout(Duration::from_secs(5), async {
            while engine.compaction_coordinator.accepting_requests_for_test() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("shutdown must stop compaction admission");
        let rejected = tokio::time::timeout(Duration::from_millis(200), engine.compact())
            .await
            .expect("requests during shutdown are immediate no-ops")
            .unwrap();
        assert_eq!(rejected.input_files, 0);
        assert_eq!(
            engine
                .compaction_coordinator
                .tombstone_scan_attempts_for_test(),
            0
        );
        assert!(!shutdown.is_finished());

        after_manifest.release();
        shutdown.await.unwrap().unwrap();
        let _ = owner.await;
        assert!(engine.status().maintenance.is_healthy());
        assert_eq!(engine.physical_stats().compactions_in_progress, 0);
        assert_eq!(engine.physical_stats().sstables.files, 1);
        let engine = Arc::try_unwrap(engine).ok().unwrap();
        drop(engine);

        let reopened = Engine::open(config).await.unwrap();
        assert_eq!(
            reopened.get(b"shutdown:00000").await.unwrap(),
            Some(format!("generation-03-{:064}", 0).into_bytes())
        );
        assert_eq!(reopened.get(b"shutdown:deleted").await.unwrap(), None);
        reopened.shutdown().await.unwrap();
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
    async fn every_paranoid_mutation_entrypoint_shares_the_ordered_group_writer() {
        const CALLERS: usize = 4;
        let directory = TempDir::new().unwrap();
        let mut config = isolated_config(directory.path(), true);
        config.wal_config = WalConfig::paranoid()
            .with_group_commit_delay(Duration::from_millis(50))
            .with_max_group_size(CALLERS);
        let engine = Arc::new(Engine::open(config).await.unwrap());
        let barrier = Arc::new(tokio::sync::Barrier::new(CALLERS + 1));

        let insert = {
            let engine = Arc::clone(&engine);
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                engine.insert(b"mixed:insert", b"value").await
            })
        };
        let delete = {
            let engine = Arc::clone(&engine);
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                engine.delete(b"mixed:delete").await
            })
        };
        let insert_many = {
            let engine = Arc::clone(&engine);
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                engine
                    .insert_many(&[
                        (b"mixed:many-a".to_vec(), b"a".to_vec()),
                        (b"mixed:many-b".to_vec(), b"b".to_vec()),
                    ])
                    .await
            })
        };
        let write_batch = {
            let engine = Arc::clone(&engine);
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                let mut batch = WriteBatch::new();
                batch.put(b"mixed:batch-put", b"value");
                batch.delete(b"mixed:batch-delete");
                barrier.wait().await;
                engine.write_batch(&batch).await
            })
        };

        barrier.wait().await;
        for result in [insert, delete, insert_many, write_batch] {
            result.await.unwrap().unwrap();
        }
        let wal = engine.wal.as_ref().unwrap();
        assert_eq!(wal.group_commit_syncs_for_test(), 1);
        assert_eq!(wal.largest_group_commit_for_test(), CALLERS as u64);
        assert_eq!(wal.current_sequence(), 6);
        assert_eq!(wal.durable_sequence(), 6);
        assert_eq!(wal.read_from(0).await.unwrap().len(), 6);
        assert_eq!(
            engine.get(b"mixed:insert").await.unwrap(),
            Some(b"value".to_vec())
        );
        assert_eq!(
            engine.get(b"mixed:many-a").await.unwrap(),
            Some(b"a".to_vec())
        );
        assert_eq!(
            engine.get(b"mixed:many-b").await.unwrap(),
            Some(b"b".to_vec())
        );
        assert_eq!(
            engine.get(b"mixed:batch-put").await.unwrap(),
            Some(b"value".to_vec())
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_flush_barrier_drains_a_cancelled_collected_append() {
        let directory = TempDir::new().unwrap();
        let mut config = isolated_config(directory.path(), true);
        config.wal_config = WalConfig::paranoid()
            .with_group_commit_delay(Duration::from_secs(30))
            .with_max_group_size(8);
        let engine = Arc::new(Engine::open(config.clone()).await.unwrap());
        let wal = engine.wal.as_ref().unwrap();

        let inserting_engine = Arc::clone(&engine);
        let insertion = tokio::spawn(async move {
            inserting_engine
                .insert(b"cancelled-before-shutdown", b"recover-me")
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while wal.group_commit_in_progress_for_test() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("group writer did not collect the append");
        insertion.abort();
        assert!(insertion.await.unwrap_err().is_cancelled());

        tokio::time::timeout(Duration::from_secs(1), engine.shutdown())
            .await
            .expect("shutdown waited for the full collection window")
            .unwrap();
        assert_eq!(wal.current_sequence(), 1);
        assert_eq!(wal.durable_sequence(), 1);

        drop(engine);
        let reopened = Engine::open(config).await.unwrap();
        assert_eq!(
            reopened.get(b"cancelled-before-shutdown").await.unwrap(),
            Some(b"recover-me".to_vec())
        );
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn poisoned_paranoid_wal_is_visible_in_health_and_shutdown() {
        let directory = TempDir::new().unwrap();
        let mut config = isolated_config(directory.path(), true);
        config.wal_config = WalConfig::paranoid()
            .with_group_commit_delay(Duration::from_millis(50))
            .with_max_group_size(8);
        let engine = Arc::new(Engine::open(config.clone()).await.unwrap());
        let wal = engine.wal.as_ref().unwrap();
        let failure = super::super::failpoints::arm(
            directory.path(),
            super::super::failpoints::PersistenceBoundary::WalDataSync,
        );

        let inserting_engine = Arc::clone(&engine);
        let insertion = tokio::spawn(async move {
            inserting_engine
                .insert(b"poisoned-health", b"recover-me")
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while wal.group_commit_in_progress_for_test() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("group writer did not collect the poisoned append");
        insertion.abort();
        assert!(insertion.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(1), async {
            while wal.group_commit_in_progress_for_test() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("group writer did not publish its failure");
        failure.assert_hit();
        let status = engine.status();
        assert!(status.maintenance.flush.retry_pending);
        let reported_failure = status
            .maintenance
            .flush
            .unresolved_failure
            .as_ref()
            .expect("poisoned WAL failure was not retained");
        assert!(reported_failure.message.contains("WAL data sync"));
        assert_eq!(reported_failure.origin, MaintenanceOrigin::Foreground);
        assert_eq!(status.maintenance.flush.failures_since_open, 1);

        let empty_flush_error = engine.flush().await.unwrap_err();
        assert!(empty_flush_error.to_string().contains("WAL data sync"));
        let after_empty_flush = engine.status().maintenance.flush;
        assert_eq!(after_empty_flush.failures_since_open, 2);
        assert_eq!(
            after_empty_flush.unresolved_failure,
            status.maintenance.flush.unresolved_failure
        );

        let shutdown = engine.shutdown_with_status().await.unwrap_err();
        let shutdown_status = match shutdown {
            MaintenanceShutdownError::UnresolvedMaintenance(status) => status,
            MaintenanceShutdownError::Storage(error) => {
                panic!("expected retained health, got storage error: {error}")
            }
        };
        assert_eq!(shutdown_status.flush.failures_since_open, 3);
        assert_eq!(shutdown_status.flush.successful_retries_since_open, 0);
        assert_eq!(
            shutdown_status.flush.unresolved_failure,
            status.maintenance.flush.unresolved_failure
        );
        assert_eq!(engine.status().maintenance.flush, shutdown_status.flush);

        drop(engine);
        let reopened = Engine::open(config).await.unwrap();
        assert_eq!(
            reopened.get(b"poisoned-health").await.unwrap(),
            Some(b"recover-me".to_vec())
        );
        assert!(reopened.status().maintenance.is_healthy());
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn queued_group_commit_retains_directory_ownership_after_engine_drop() {
        let directory = TempDir::new().unwrap();
        let mut config = isolated_config(directory.path(), true);
        config.wal_config = WalConfig::paranoid();
        let engine = Arc::new(Engine::open(config.clone()).await.unwrap());
        let wal = Arc::clone(engine.wal.as_ref().unwrap());
        let acknowledged_payload_bytes = Arc::clone(&engine.logical_bytes_ingested);

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
        assert_eq!(acknowledged_payload_bytes.load(Ordering::Relaxed), 0);
        assert!(wal.bytes_written_since_open() > 0);
        drop(wal);

        let reopened = Engine::open(config).await.unwrap();
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancelled_group_commit_is_physical_wal_work_but_not_acknowledged_ingestion() {
        let directory = TempDir::new().unwrap();
        let mut config = isolated_config(directory.path(), true);
        config.wal_config = WalConfig::paranoid();
        let engine = Arc::new(Engine::open(config.clone()).await.unwrap());
        let wal = Arc::clone(engine.wal.as_ref().unwrap());
        let retained_before = wal.retained_size();

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
        let queued = tokio::spawn(async move {
            queued_engine
                .insert(b"cancelled:physical", b"persisted")
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while wal.group_commit_in_progress_for_test() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("queued group commit did not start");
        queued.abort();
        assert!(queued.await.unwrap_err().is_cancelled());

        release_tx.send(()).unwrap();
        lock_holder.await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while wal.group_commit_in_progress_for_test() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("queued group commit did not finish");

        let physical = engine.physical_stats();
        assert!(physical.wal.bytes_written_since_open > 0);
        assert_eq!(
            physical.wal.retained_valid_bytes - retained_before,
            physical.wal.bytes_written_since_open
        );
        assert_eq!(
            physical.amplification.wal_bytes_written_since_open,
            physical.wal.bytes_written_since_open
        );
        assert_eq!(physical.amplification.logical_bytes_ingested_since_open, 0);
        assert_eq!(
            engine.legacy_stats().wal_bytes_written,
            physical.wal.bytes_written_since_open
        );
        assert_eq!(engine.get(b"cancelled:physical").await.unwrap(), None);
        wal.flush().await.unwrap();
        let retained_after = wal.retained_size();
        drop(wal);
        drop(engine);

        let reopened = Engine::open(config).await.unwrap();
        assert_eq!(reopened.physical_stats().wal.bytes_written_since_open, 0);
        assert_eq!(
            reopened.physical_stats().wal.retained_valid_bytes,
            retained_after
        );
        assert_eq!(
            reopened.get(b"cancelled:physical").await.unwrap(),
            Some(b"persisted".to_vec())
        );
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

    #[cfg(unix)]
    #[tokio::test]
    async fn reopen_retains_manifest_table_through_symlinked_sstable_directory() {
        use std::os::unix::fs::symlink;

        let directory = TempDir::new().unwrap();
        let data_directory = directory.path().join("data");
        let sstable_backing = directory.path().join("sstable-backing");
        std::fs::create_dir_all(&data_directory).unwrap();
        std::fs::create_dir_all(&sstable_backing).unwrap();
        symlink(&sstable_backing, data_directory.join("sstables")).unwrap();

        let config = isolated_config(&data_directory, false);
        let engine = Engine::open(config.clone()).await.unwrap();
        engine.insert(b"symlink:key", b"preserved").await.unwrap();
        engine.flush().await.unwrap();
        let table_path = Manifest::load_or_create(&data_directory)
            .unwrap()
            .sstables
            .into_iter()
            .next()
            .unwrap()
            .path;
        assert!(table_path.exists());
        engine.shutdown().await.unwrap();
        drop(engine);

        let reopened = Engine::open(config).await.unwrap();
        assert!(table_path.exists());
        assert_eq!(reopened.physical_stats().sstables.files, 1);
        assert_eq!(
            reopened.get(b"symlink:key").await.unwrap(),
            Some(b"preserved".to_vec())
        );
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cross_level_legacy_and_versioned_winners_survive_compaction_reopen_and_new_writes() {
        let directory = TempDir::new().unwrap();
        let sstable_directory = directory.path().join("sstables");
        std::fs::create_dir_all(&sstable_directory).unwrap();

        let mut level_zero = write_legacy_scan_table(
            &sstable_directory.join("legacy-l0.sst"),
            10,
            &[
                (b"order:current-tombstone", Some(b"old-current-tombstone")),
                (b"order:current-value", Some(b"old-current-value")),
                (b"order:legacy-tombstone", Some(b"old-legacy-tombstone")),
                (b"order:legacy-value", Some(b"old-legacy-value")),
            ],
            90,
        );
        level_zero.level = 0;
        let mut level_one = write_legacy_scan_table(
            &sstable_directory.join("legacy-l1.sst"),
            20,
            &[
                (b"order:current-value", None),
                (b"order:legacy-tombstone", None),
                (b"order:legacy-value", Some(b"legacy-winner")),
            ],
            80,
        );
        level_one.level = 1;
        let mut level_two = write_legacy_scan_table(
            &sstable_directory.join("legacy-l2.sst"),
            15,
            &[
                (b"order:current-tombstone", Some(b"stale")),
                (b"order:legacy-tombstone", Some(b"stale")),
                (b"order:legacy-value", Some(b"stale")),
            ],
            70,
        );
        level_two.level = 2;
        let mut level_three = write_versioned_scan_table(
            &sstable_directory.join("versioned-l3.sst"),
            30,
            &[
                (b"order:current-tombstone", None, 100),
                (b"order:current-value", Some(b"versioned-winner"), 101),
            ],
        );
        level_three.level = 3;
        let old_paths = [
            level_zero.path.clone(),
            level_one.path.clone(),
            level_two.path.clone(),
            level_three.path.clone(),
        ];
        let mut manifest = Manifest::new();
        manifest.sstables = vec![level_zero, level_one, level_two, level_three];
        manifest.save(directory.path()).unwrap();

        let mut config = isolated_config(directory.path(), false);
        config.compaction_config.l0_compaction_trigger = 1;
        config.compaction_config.max_levels = 4;
        config.sstable_config.compression = super::super::sstable::CompressionType::None;
        let engine = Engine::open(config.clone()).await.unwrap();
        assert_eq!(engine.get(b"order:legacy-tombstone").await.unwrap(), None);
        assert_eq!(
            engine.get(b"order:legacy-value").await.unwrap(),
            Some(b"legacy-winner".to_vec())
        );
        assert_eq!(engine.get(b"order:current-tombstone").await.unwrap(), None);
        assert_eq!(
            engine.get(b"order:current-value").await.unwrap(),
            Some(b"versioned-winner".to_vec())
        );

        let result = engine.compact().await.unwrap();
        assert_eq!(result.input_files, 4);
        let compacted_manifest = Manifest::load_or_create(directory.path()).unwrap();
        assert_eq!(compacted_manifest.sstables.len(), 1);
        assert_eq!(compacted_manifest.sstables[0].level, 3);
        assert!(old_paths.iter().all(|path| !path.exists()));
        assert_eq!(engine.get(b"order:legacy-tombstone").await.unwrap(), None);
        assert_eq!(
            engine.get(b"order:legacy-value").await.unwrap(),
            Some(b"legacy-winner".to_vec())
        );
        engine.shutdown().await.unwrap();
        drop(engine);

        let reopened = Engine::open(config).await.unwrap();
        assert_eq!(
            reopened.get(b"order:current-tombstone").await.unwrap(),
            None
        );
        assert_eq!(
            reopened.get(b"order:current-value").await.unwrap(),
            Some(b"versioned-winner".to_vec())
        );
        reopened
            .insert(b"order:legacy-tombstone", b"new-versioned-value")
            .await
            .unwrap();
        reopened.delete(b"order:legacy-value").await.unwrap();
        reopened.flush().await.unwrap();
        assert_eq!(
            reopened.get(b"order:legacy-tombstone").await.unwrap(),
            Some(b"new-versioned-value".to_vec())
        );
        assert_eq!(reopened.get(b"order:legacy-value").await.unwrap(), None);
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn delayed_older_flush_retains_then_reclaims_tombstone_at_a_safe_fixed_point() {
        let directory = TempDir::new().unwrap();
        let mut config = isolated_config(directory.path(), false);
        config.compaction_config.l0_compaction_trigger = 4;
        config.compaction_config.max_levels = 3;
        config.sstable_config.compression = super::super::sstable::CompressionType::None;
        let engine = Arc::new(Engine::open(config.clone()).await.unwrap());

        engine.insert(b"delayed:key", b"original").await.unwrap();
        engine.flush().await.unwrap();
        engine.delete(b"delayed:key").await.unwrap();
        engine.flush().await.unwrap();
        for generation in 0..2 {
            engine
                .insert(format!("delayed:initial:{generation}").as_bytes(), b"value")
                .await
                .unwrap();
            engine.flush().await.unwrap();
        }

        // Model an acknowledged old, nonoverlapping generation whose flush was
        // delayed. Its global sequence floor must retain the tombstone even
        // though its eventual SSTable cannot join the overlap closure.
        engine
            .memtable_manager
            .insert_with_sequence(b"z:delayed-floor", b"late-old-copy", 0)
            .unwrap();
        let before_manifest = engine
            .compaction_coordinator
            .gate_before_manifest_for_test();
        let background = engine.clone_for_background();
        let compaction = tokio::spawn(async move { background.compact().await.unwrap() });
        tokio::time::timeout(Duration::from_secs(5), async {
            while !before_manifest.reached() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("compaction must capture the delayed floor before publication");
        engine.flush().await.unwrap();
        before_manifest.release();
        let retained = compaction.await.unwrap();
        assert_eq!(retained.input_files, 4);
        assert!(retained.work_remaining);
        assert_eq!(engine.get(b"delayed:key").await.unwrap(), None);
        assert_eq!(
            Manifest::load_or_create(directory.path())
                .unwrap()
                .sstables
                .iter()
                .map(|table| table.tombstone_count)
                .sum::<u64>(),
            1
        );

        // No level is under ordinary pressure. The later manual request must
        // nevertheless revisit the retained tombstone now that the floor is
        // durable, even though that floor's L0 table is nonoverlapping.
        engine.compact().await.unwrap();
        assert_eq!(engine.get(b"delayed:key").await.unwrap(), None);
        assert_eq!(
            Manifest::load_or_create(directory.path())
                .unwrap()
                .sstables
                .iter()
                .map(|table| table.tombstone_count)
                .sum::<u64>(),
            0
        );
        engine.shutdown().await.unwrap();
        drop(engine);

        let reopened = Engine::open(config).await.unwrap();
        assert_eq!(reopened.get(b"delayed:key").await.unwrap(), None);
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn background_reports_the_next_reclaimable_tombstone_component() {
        let directory = TempDir::new().unwrap();
        let sstable_directory = directory.path().join("sstables");
        std::fs::create_dir_all(&sstable_directory).unwrap();
        let mut manifest = Manifest::new();
        for component in 0_u64..2 {
            let prefix = format!("component:{component}");
            let deleted_key = format!("{prefix}:deleted");
            let live_key = format!("{prefix}:live");
            let entries = [
                (deleted_key.as_bytes(), None, 10),
                (live_key.as_bytes(), Some(b"newer-live".as_slice()), 20),
            ];
            let mut table = write_versioned_scan_table(
                &sstable_directory.join(format!("component-{component}.sst")),
                component + 1,
                &entries,
            );
            table.level = 1;
            manifest.sstables.push(table);
        }
        manifest.save(directory.path()).unwrap();

        let mut config = isolated_config(directory.path(), false);
        config.compaction_config.max_levels = 2;
        config.compaction_config.l0_compaction_trigger = 4;
        config.sstable_config.compression = super::super::sstable::CompressionType::None;
        let engine = Engine::open(config.clone()).await.unwrap();
        assert_eq!(engine.get(b"component:0:deleted").await.unwrap(), None);
        assert_eq!(engine.get(b"component:1:deleted").await.unwrap(), None);

        let first = engine.clone_for_background().compact().await.unwrap();
        assert_eq!((first.input_files, first.output_files), (1, 1));
        assert!(first.work_remaining);
        assert!(!first.is_complete());
        engine.shutdown().await.unwrap();
        drop(engine);

        // The exact tombstone sequence cache is intentionally in-memory. A
        // reopen must rescan the remaining mixed table and still reclaim it.
        let reopened = Engine::open(config).await.unwrap();
        let second = reopened.clone_for_background().compact().await.unwrap();
        assert_eq!((second.input_files, second.output_files), (1, 1));
        assert!(!second.work_remaining);
        assert!(second.is_complete());
        assert_eq!(
            Manifest::load_or_create(directory.path())
                .unwrap()
                .sstables
                .iter()
                .map(|table| table.tombstone_count)
                .sum::<u64>(),
            0
        );
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn unrelated_component_success_does_not_clear_failed_compaction_work() {
        let directory = TempDir::new().unwrap();
        let sstable_directory = directory.path().join("sstables");
        std::fs::create_dir_all(&sstable_directory).unwrap();
        let mut manifest = Manifest::new();
        for component in 0_u64..2 {
            let prefix = format!("proof:{component}");
            let deleted_key = format!("{prefix}:deleted");
            let live_key = format!("{prefix}:live");
            let entries = [
                (deleted_key.as_bytes(), None, 10),
                (live_key.as_bytes(), Some(b"live".as_slice()), 20),
            ];
            let mut table = write_versioned_scan_table(
                &sstable_directory.join(format!("proof-{component}.sst")),
                component + 1,
                &entries,
            );
            table.level = 1;
            manifest.sstables.push(table);
        }
        let first = manifest.sstables[0].clone();
        let second = manifest.sstables[1].clone();
        manifest.save(directory.path()).unwrap();

        let mut config = isolated_config(directory.path(), false);
        config.compaction_config.max_levels = 2;
        config.compaction_config.l0_compaction_trigger = 4;
        config.sstable_config.compression = super::super::sstable::CompressionType::None;
        let engine = Engine::open(config).await.unwrap();
        let failure = super::super::failpoints::arm(
            directory.path(),
            super::super::failpoints::PersistenceBoundary::CompactionOutputPublication,
        );
        assert!(engine
            .compaction_coordinator
            .execute_background_job_for_test(super::super::compaction::CompactionSelection {
                input_sstables: vec![first.clone()],
                output_level: 1,
            })
            .await
            .is_err());
        failure.assert_hit();
        assert!(engine.status().maintenance.compaction.retry_pending);
        assert!(Manifest::load_or_create(directory.path())
            .unwrap()
            .sstables
            .iter()
            .any(|table| table.id == first.id));

        engine
            .compaction_coordinator
            .execute_background_job_for_test(super::super::compaction::CompactionSelection {
                input_sstables: vec![second.clone()],
                output_level: 1,
            })
            .await
            .unwrap();
        let after_unrelated = engine.status().maintenance.compaction;
        assert!(after_unrelated.retry_pending);
        assert_eq!(after_unrelated.failures_since_open, 1);
        assert_eq!(after_unrelated.background_failures_since_open, 1);
        assert_eq!(after_unrelated.successful_retries_since_open, 0);
        let durable = Manifest::load_or_create(directory.path()).unwrap();
        assert!(durable.sstables.iter().any(|table| table.id == first.id));
        assert!(!durable.sstables.iter().any(|table| table.id == second.id));

        let retry = engine.clone_for_background().compact().await.unwrap();
        assert!(retry.is_complete());
        let resolved = engine.status().maintenance.compaction;
        assert!(!resolved.retry_pending);
        assert_eq!(resolved.successful_retries_since_open, 1);
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_reports_an_unresolved_background_compaction_failure() {
        let directory = TempDir::new().unwrap();
        let mut config = isolated_config(directory.path(), false);
        config.compaction_config.l0_compaction_trigger = 2;
        config.sstable_config.compression = super::super::sstable::CompressionType::None;
        let engine = Engine::open(config.clone()).await.unwrap();
        for generation in 0..2 {
            engine
                .insert(
                    b"shutdown:maintenance",
                    format!("generation-{generation}").as_bytes(),
                )
                .await
                .unwrap();
            engine.flush().await.unwrap();
        }
        let failure = super::super::failpoints::arm(
            directory.path(),
            super::super::failpoints::PersistenceBoundary::CompactionOutputPublication,
        );
        assert!(engine.clone_for_background().compact().await.is_err());
        failure.assert_hit();
        assert_eq!(
            engine
                .status()
                .maintenance
                .compaction
                .background_failures_since_open,
            1
        );

        let error = engine.shutdown_with_status().await.unwrap_err();
        let shutdown_status = match error {
            MaintenanceShutdownError::UnresolvedMaintenance(status) => status,
            error @ MaintenanceShutdownError::Storage(_) => {
                panic!("expected structured maintenance status, got {error}")
            }
        };
        assert!(shutdown_status.compaction.retry_pending);
        assert!(!shutdown_status.flush.retry_pending);
        assert!(engine.status().maintenance.compaction.retry_pending);
        let legacy_error = engine.shutdown().await.unwrap_err();
        assert!(matches!(legacy_error, StorageError::Other(_)));
        assert!(legacy_error
            .to_string()
            .contains("shutdown left unresolved maintenance failures"));
        drop(engine);

        let reopened = Engine::open(config).await.unwrap();
        assert_eq!(
            reopened.get(b"shutdown:maintenance").await.unwrap(),
            Some(b"generation-1".to_vec())
        );
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_snapshots_simultaneous_failures_without_replacing_original_detail() {
        let directory = TempDir::new().unwrap();
        let mut config = isolated_config(directory.path(), false);
        config.compaction_config.l0_compaction_trigger = 2;
        config.sstable_config.compression = super::super::sstable::CompressionType::None;
        let engine = Engine::open(config).await.unwrap();
        for generation in 0..2 {
            engine
                .insert(
                    b"shutdown:simultaneous",
                    format!("generation-{generation}").as_bytes(),
                )
                .await
                .unwrap();
            engine.flush().await.unwrap();
        }
        let compaction_failure = super::super::failpoints::arm(
            directory.path(),
            super::super::failpoints::PersistenceBoundary::CompactionOutputPublication,
        );
        assert!(engine.clone_for_background().compact().await.is_err());
        compaction_failure.assert_hit();

        engine.insert(b"shutdown:pending", b"value").await.unwrap();
        engine.flush_write_buffers().unwrap();
        engine.memtable_manager.force_rotate().unwrap();
        let original_flush_failure = super::super::failpoints::arm(
            directory.path(),
            super::super::failpoints::PersistenceBoundary::ManifestInstallation,
        );
        assert!(engine
            .clone_for_background()
            .background_flush()
            .await
            .is_err());
        original_flush_failure.assert_hit();
        let original_detail = engine
            .status()
            .maintenance
            .flush
            .unresolved_failure
            .unwrap();
        assert_eq!(original_detail.origin, MaintenanceOrigin::Background);
        drop(original_flush_failure);

        let shutdown_flush_failure = super::super::failpoints::arm(
            directory.path(),
            super::super::failpoints::PersistenceBoundary::ManifestInstallation,
        );
        let error = engine.shutdown_with_status().await.unwrap_err();
        shutdown_flush_failure.assert_hit();
        let shutdown_status = match error {
            MaintenanceShutdownError::UnresolvedMaintenance(status) => status,
            error @ MaintenanceShutdownError::Storage(_) => {
                panic!("expected structured maintenance status, got {error}")
            }
        };
        assert!(shutdown_status.compaction.retry_pending);
        assert!(shutdown_status.flush.retry_pending);
        assert_eq!(shutdown_status.flush.failures_since_open, 2);
        assert_eq!(
            shutdown_status.flush.unresolved_failure,
            Some(original_detail)
        );
        assert_eq!(engine.status().maintenance, *shutdown_status);
        assert_eq!(engine.memtable_manager.immutable_count(), 1);
        assert_eq!(
            engine.get(b"shutdown:pending").await.unwrap(),
            Some(b"value".to_vec())
        );
    }

    #[tokio::test]
    async fn installed_manifest_sync_failure_registers_inputs_until_reconciliation() {
        let directory = TempDir::new().unwrap();
        let mut config = isolated_config(directory.path(), false);
        config.compaction_config.l0_compaction_trigger = 2;
        config.sstable_config.compression = super::super::sstable::CompressionType::None;
        let engine = Engine::open(config).await.unwrap();
        for generation in 0..2 {
            engine
                .insert(
                    b"compaction:reconciliation",
                    format!("generation-{generation}").as_bytes(),
                )
                .await
                .unwrap();
            engine.flush().await.unwrap();
        }
        let input_paths = engine
            .sstables
            .read()
            .await
            .iter()
            .map(|table| table.path.clone())
            .collect::<Vec<_>>();
        let first_sync_failure = super::super::failpoints::arm(
            directory.path(),
            super::super::failpoints::PersistenceBoundary::ManifestDirectorySync,
        );
        let resync_failure = super::super::failpoints::arm(
            directory.path(),
            super::super::failpoints::PersistenceBoundary::CompactionManifestDirectoryResync,
        );

        assert!(engine.compact().await.is_err());
        first_sync_failure.assert_hit();
        resync_failure.assert_hit();
        assert!(input_paths.iter().all(|path| path.exists()));
        let pending = engine.status().maintenance.compaction;
        assert!(pending.retry_pending);
        assert!(pending
            .unresolved_failure
            .as_ref()
            .is_some_and(|failure| failure.message.contains("directory sync")));

        let retry = engine.compact().await.unwrap();
        assert_eq!(retry.input_files, 0);
        assert!(retry.is_complete());
        assert!(input_paths.iter().all(|path| !path.exists()));
        assert!(!engine.status().maintenance.compaction.retry_pending);
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn recovered_cleanup_failure_is_immediately_unhealthy_and_retries_on_reopen() {
        let directory = TempDir::new().unwrap();
        let config = isolated_config(directory.path(), false);
        let engine = Engine::open(config.clone()).await.unwrap();
        engine.shutdown().await.unwrap();
        drop(engine);
        let orphan = directory.path().join("sstables/L0/recovered-orphan.sst");
        write_valid_orphan(&orphan);
        let cleanup_failure = super::super::failpoints::arm(
            directory.path(),
            super::super::failpoints::PersistenceBoundary::SstableCleanup,
        );

        let reopened = Engine::open(config.clone()).await.unwrap();
        cleanup_failure.assert_hit();
        let pending = reopened.status().maintenance.compaction;
        assert!(pending.retry_pending);
        let failure = pending.unresolved_failure.unwrap();
        assert_eq!(failure.origin, MaintenanceOrigin::Recovery);
        assert!(failure.message.contains(&orphan.display().to_string()));
        let error = reopened.shutdown_with_status().await.unwrap_err();
        assert!(matches!(
            error,
            MaintenanceShutdownError::UnresolvedMaintenance(status)
                if status.compaction.retry_pending && !status.flush.retry_pending
        ));
        drop(reopened);

        let recovered = Engine::open(config).await.unwrap();
        assert!(!orphan.exists());
        assert!(recovered.status().maintenance.is_healthy());
        recovered.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn incomplete_startup_cleanup_scan_stays_unhealthy_until_reopen_retry() {
        let directory = TempDir::new().unwrap();
        let config = isolated_config(directory.path(), false);
        let engine = Engine::open(config.clone()).await.unwrap();
        engine.shutdown().await.unwrap();
        drop(engine);
        let orphan = directory.path().join("sstables/L0/unscanned-orphan.sst");
        write_valid_orphan(&orphan);
        let scan_failure = super::super::failpoints::arm(
            directory.path(),
            super::super::failpoints::PersistenceBoundary::SstableCleanupScan,
        );

        let reopened = Engine::open(config.clone()).await.unwrap();
        scan_failure.assert_hit();
        let pending = reopened.status().maintenance.compaction;
        assert!(pending.retry_pending);
        let failure = pending.unresolved_failure.unwrap();
        assert_eq!(failure.origin, MaintenanceOrigin::Recovery);
        assert!(failure.message.contains("cleanup scan"));
        assert!(orphan.exists());
        assert!(reopened.compact().await.unwrap().is_complete());
        assert!(reopened.status().maintenance.compaction.retry_pending);
        assert!(matches!(
            reopened.shutdown_with_status().await.unwrap_err(),
            MaintenanceShutdownError::UnresolvedMaintenance(status)
                if status.compaction.retry_pending && !status.flush.retry_pending
        ));
        drop(reopened);

        let recovered = Engine::open(config).await.unwrap();
        assert!(!orphan.exists());
        assert!(recovered.status().maintenance.is_healthy());
        recovered.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn wal_replay_floor_retains_a_tombstone_at_the_checkpoint_equality_boundary() {
        let directory = TempDir::new().unwrap();
        let mut config = isolated_config(directory.path(), true);
        config.compaction_config.l0_compaction_trigger = 2;
        config.compaction_config.max_levels = 2;
        config.sstable_config.compression = super::super::sstable::CompressionType::None;
        let engine = Engine::open(config.clone()).await.unwrap();

        let failure = super::super::failpoints::arm(
            directory.path(),
            super::super::failpoints::PersistenceBoundary::Wal,
        );
        assert!(engine
            .insert(b"wal-floor:key", b"replayable-old")
            .await
            .is_err());
        failure.assert_hit();
        engine.delete(b"wal-floor:key").await.unwrap();
        engine.flush().await.unwrap();
        engine.insert(b"wal-floor:other", b"value").await.unwrap();
        engine.flush().await.unwrap();
        assert_eq!(
            Manifest::load_or_create(directory.path())
                .unwrap()
                .wal_checkpoint,
            0
        );

        engine.compact().await.unwrap();
        let compacted = Manifest::load_or_create(directory.path()).unwrap();
        assert_eq!(compacted.wal_checkpoint, 0);
        assert_eq!(
            compacted
                .sstables
                .iter()
                .map(|table| table.tombstone_count)
                .sum::<u64>(),
            1
        );
        assert_eq!(engine.get(b"wal-floor:key").await.unwrap(), None);
        engine.wal.as_ref().unwrap().flush().await.unwrap();
        drop(engine);

        let reopened = Engine::open(config).await.unwrap();
        assert_eq!(reopened.get(b"wal-floor:key").await.unwrap(), None);
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn safe_tombstone_reclamation_survives_output_splitting_and_reopen() {
        let directory = TempDir::new().unwrap();
        let mut config = isolated_config(directory.path(), false);
        config.compaction_config.l0_compaction_trigger = 2;
        config.compaction_config.max_levels = 2;
        config.compaction_config.target_file_size = 20 * 1024;
        config.sstable_config.block_size = 512;
        config.sstable_config.compression = super::super::sstable::CompressionType::None;
        let engine = Engine::open(config.clone()).await.unwrap();

        for key_index in 0..48 {
            engine
                .insert(
                    format!("reclaim-split:{key_index:04}").as_bytes(),
                    &[b'a'; 512],
                )
                .await
                .unwrap();
        }
        engine.flush().await.unwrap();
        for key_index in 0..48 {
            let key = format!("reclaim-split:{key_index:04}");
            if key_index % 7 == 0 {
                engine.delete(key.as_bytes()).await.unwrap();
            } else {
                engine.insert(key.as_bytes(), &[b'b'; 512]).await.unwrap();
            }
        }
        engine.flush().await.unwrap();

        let result = engine.compact().await.unwrap();
        assert!(result.output_files >= 3);
        let compacted = Manifest::load_or_create(directory.path()).unwrap();
        assert!(compacted.sstables.len() >= 3);
        assert!(compacted
            .sstables
            .windows(2)
            .all(|tables| tables[0].max_key < tables[1].min_key));
        assert_eq!(
            compacted
                .sstables
                .iter()
                .map(|table| table.tombstone_count)
                .sum::<u64>(),
            0
        );
        for key_index in (0..48).step_by(7) {
            assert_eq!(
                engine
                    .get(format!("reclaim-split:{key_index:04}").as_bytes())
                    .await
                    .unwrap(),
                None
            );
        }
        engine.shutdown().await.unwrap();
        drop(engine);

        let reopened = Engine::open(config).await.unwrap();
        for key_index in (0..48).step_by(7) {
            assert_eq!(
                reopened
                    .get(format!("reclaim-split:{key_index:04}").as_bytes())
                    .await
                    .unwrap(),
                None
            );
        }
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn fully_deleted_compaction_publishes_no_empty_table_and_allows_reinsertion() {
        let directory = TempDir::new().unwrap();
        let mut config = isolated_config(directory.path(), false);
        config.compaction_config.l0_compaction_trigger = 2;
        config.compaction_config.max_levels = 2;
        config.sstable_config.compression = super::super::sstable::CompressionType::None;
        let engine = Engine::open(config.clone()).await.unwrap();

        engine.insert(b"fully-deleted", b"old").await.unwrap();
        engine.flush().await.unwrap();
        engine.delete(b"fully-deleted").await.unwrap();
        engine.flush().await.unwrap();

        let result = engine.compact().await.unwrap();
        assert_eq!(result.input_files, 2);
        assert_eq!(result.output_files, 0);
        assert!(Manifest::load_or_create(directory.path())
            .unwrap()
            .sstables
            .is_empty());
        assert_eq!(engine.get(b"fully-deleted").await.unwrap(), None);
        engine.shutdown().await.unwrap();
        drop(engine);

        let reopened = Engine::open(config).await.unwrap();
        assert_eq!(reopened.get(b"fully-deleted").await.unwrap(), None);
        reopened
            .insert(b"fully-deleted", b"new-after-reclamation")
            .await
            .unwrap();
        assert_eq!(
            reopened.get(b"fully-deleted").await.unwrap(),
            Some(b"new-after-reclamation".to_vec())
        );
        reopened.shutdown().await.unwrap();
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
        assert_eq!(guard.value_len(), b"large-value".len());
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
    async fn pinned_readers_survive_compaction_while_retired_pool_entries_are_invalidated() {
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
        assert_eq!(
            engine.get(b"pin:0000").await.unwrap(),
            Some(b"value-1-0000".to_vec())
        );
        assert_eq!(engine.sstable_pool.stats().open_sstables, 2);
        let iterator = engine.scan_prefix_iter(b"pin:").await.unwrap();

        tokio::time::timeout(Duration::from_secs(2), engine.compact())
            .await
            .expect("iterator retained the global SSTable list lock")
            .unwrap();
        assert!(input_paths.iter().all(|path| !path.exists()));
        assert_eq!(engine.sstable_pool.stats().open_sstables, 0);
        assert_eq!(
            engine.get(b"pin:0000").await.unwrap(),
            Some(b"value-1-0000".to_vec())
        );
        assert_eq!(engine.sstable_pool.stats().open_sstables, 1);
        let pairs = iterator.collect_pairs().unwrap();
        assert_eq!(pairs.len(), 200);
        assert!(pairs
            .iter()
            .all(|(_, value)| value.starts_with(b"value-1-")));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn committed_compaction_defers_failed_unlinks_and_retries_them() {
        use std::os::unix::fs::PermissionsExt;

        struct RestorePermissions(PathBuf);

        impl Drop for RestorePermissions {
            fn drop(&mut self) {
                let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o700));
            }
        }

        let directory = TempDir::new().unwrap();
        let mut config = isolated_config(directory.path(), false);
        config.compaction_config.l0_compaction_trigger = 2;
        config.sstable_config.compression = super::super::sstable::CompressionType::None;
        let engine = Engine::open(config).await.unwrap();
        for generation in 0..2 {
            engine
                .insert(
                    b"cleanup:key",
                    format!("generation-{generation}").as_bytes(),
                )
                .await
                .unwrap();
            engine.flush().await.unwrap();
        }
        let input_paths = engine
            .sstables
            .read()
            .await
            .iter()
            .map(|table| table.path.clone())
            .collect::<Vec<_>>();
        let level_zero = directory.path().join("sstables/L0");
        let restore = RestorePermissions(level_zero.clone());
        std::fs::set_permissions(&level_zero, std::fs::Permissions::from_mode(0o500)).unwrap();

        assert_eq!(engine.compact().await.unwrap().input_files, 2);
        assert_eq!(engine.physical_stats().sstables.files, 1);
        assert!(input_paths.iter().all(|path| path.exists()));
        let deferred = engine.status().maintenance.compaction;
        assert!(deferred.retry_pending);
        assert!(deferred
            .unresolved_failure
            .unwrap()
            .message
            .contains(&input_paths[0].display().to_string()));
        assert_eq!(
            engine.get(b"cleanup:key").await.unwrap(),
            Some(b"generation-1".to_vec())
        );

        drop(restore);
        assert_eq!(engine.compact().await.unwrap().input_files, 0);
        assert!(input_paths.iter().all(|path| !path.exists()));
        assert!(!engine.status().maintenance.compaction.retry_pending);
        assert_eq!(engine.physical_stats().sstables.files, 1);
        engine.shutdown().await.unwrap();
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
