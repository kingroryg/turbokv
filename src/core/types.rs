//! # Core Types
//!
//! Minimal type definitions for TurboKV - a generic key-value store.
//!
//! Keys and values are raw bytes (`&[u8]` / `Vec<u8>`).
//! Serialization is the caller's responsibility.

use std::fmt;
use std::time::Duration;

/// Compression algorithm options
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Compression {
    /// No compression
    None,
    /// LZ4 - fast compression
    #[default]
    Lz4,
    /// Snappy - balanced
    Snappy,
    /// Zstd - high compression ratio
    Zstd,
}

/// Compaction style
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompactionStyle {
    /// Size-tiered compaction - good for write-heavy workloads
    #[default]
    SizeTiered,
    /// Leveled compaction - good for read-heavy workloads
    Leveled,
}

/// Database configuration
#[derive(Debug, Clone)]
pub struct DbConfig {
    /// Enable WAL for durability (default: true)
    pub wal_enabled: bool,
    /// Compression algorithm (default: Lz4)
    pub compression: Compression,
    /// Sync writes to disk immediately (default: true)
    pub sync_writes: bool,
    /// MemTable size before flush (default: 64MB)
    pub memtable_size: usize,
    /// Block cache size in bytes (default: 64MB, 0 to disable)
    pub block_cache_size: usize,
    /// Maximum number of open files (default: 1000)
    pub max_open_files: usize,
    /// Compaction style (default: SizeTiered)
    pub compaction_style: CompactionStyle,
    /// Maximum WAL file size before rotation (default: 128MB)
    pub max_wal_size: usize,
    /// Flush interval for background flush (default: 60s)
    pub flush_interval: Duration,
    /// Compaction interval (default: 300s)
    pub compaction_interval: Duration,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            wal_enabled: true,
            compression: Compression::Lz4,
            sync_writes: true,
            memtable_size: 64 * 1024 * 1024,    // 64MB
            block_cache_size: 64 * 1024 * 1024, // 64MB
            max_open_files: 1000,
            compaction_style: CompactionStyle::SizeTiered,
            max_wal_size: 128 * 1024 * 1024, // 128MB
            flush_interval: Duration::from_secs(60),
            compaction_interval: Duration::from_secs(300),
        }
    }
}

impl DbConfig {
    /// Fast configuration - maximum speed, no durability guarantees
    ///
    /// Use for: caches, ephemeral data, benchmarks
    pub fn fast() -> Self {
        Self {
            wal_enabled: false,
            compression: Compression::None,
            sync_writes: false,
            memtable_size: 256 * 1024 * 1024, // 256MB - larger to reduce flushes
            block_cache_size: 128 * 1024 * 1024, // 128MB
            max_open_files: 1000,
            compaction_style: CompactionStyle::SizeTiered,
            max_wal_size: 256 * 1024 * 1024,
            flush_interval: Duration::from_secs(300),
            compaction_interval: Duration::from_secs(600),
        }
    }

    /// Durable configuration - data survives crashes
    ///
    /// Use for: production data that must not be lost
    pub fn durable() -> Self {
        Self {
            wal_enabled: true,
            compression: Compression::Lz4,
            sync_writes: false,
            memtable_size: 64 * 1024 * 1024,
            block_cache_size: 64 * 1024 * 1024,
            max_open_files: 1000,
            compaction_style: CompactionStyle::SizeTiered,
            max_wal_size: 128 * 1024 * 1024,
            flush_interval: Duration::from_secs(60),
            compaction_interval: Duration::from_secs(300),
        }
    }
}

/// Storage statistics
#[derive(Debug, Clone, Default)]
pub struct StorageStats {
    /// Total number of physical versions across memtables and SSTables.
    ///
    /// This legacy field is not a logical live-key count. Use
    /// `Db::logical_stats` for an exact logical snapshot.
    pub total_keys: u64,
    /// Approximate memtable bytes plus SSTable file bytes.
    ///
    /// This legacy field mixes memory estimates and on-disk bytes. Use
    /// `Db::physical_stats` for separated gauges.
    pub total_bytes: u64,
    /// Current WAL size in bytes
    pub wal_size: u64,
    /// Number of SSTable files
    pub sstable_count: u32,
    /// Current MemTable size in bytes
    pub memtable_size: u64,
    /// Whether compaction is pending
    pub compaction_pending: bool,
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

/// Exact statistics for one coherent logical database snapshot.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogicalStats {
    /// Number of unique live keys in the snapshot.
    pub live_keys: u64,
    /// Bytes occupied by the unique live keys themselves.
    pub key_bytes: u64,
    /// Bytes occupied by the values of unique live keys.
    pub value_bytes: u64,
    /// Sum of [`Self::key_bytes`] and [`Self::value_bytes`].
    pub total_bytes: u64,
}

/// Cheap physical and operational statistics.
///
/// Current gauges describe storage visible when each component is sampled.
/// Fields ending in `_since_open` are monotonic for one `Db`/`Engine` process
/// lifetime and reset when the database is reopened.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PhysicalStats {
    /// Write-ahead log state.
    pub wal: WalStats,
    /// Active and immutable memtable state.
    pub memtables: PhysicalMemTableStats,
    /// Installed SSTable state.
    pub sstables: PhysicalSSTableStats,
    /// Physical version and tombstone state.
    pub versions: PhysicalVersionStats,
    /// Block and SSTable-reader cache state.
    pub cache: PhysicalCacheStats,
    /// Controlled write-stall state.
    pub stalls: WriteStallStats,
    /// Counters used to calculate write amplification.
    pub amplification: WriteAmplificationStats,
    /// Compaction selections or jobs currently in progress.
    pub compactions_in_progress: u64,
}

/// Cheap operational health and write-backpressure status.
///
/// This is sampled for monitoring and is not a transactional snapshot across
/// maintenance and write-path components.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DatabaseStatus {
    /// Flush and compaction failure/retry state.
    pub maintenance: MaintenanceStatus,
    /// Current write-pressure causes and their counters since open.
    pub write_backpressure: WriteBackpressureStatus,
}

/// Health of flush and compaction maintenance for one open database.
///
/// A failed operation stays in `retry_pending` state until a later attempt
/// proves the failed lane is settled. Flush uses successful FIFO drainage plus
/// completion of registered WAL sync/reclamation work; compaction uses an
/// exact post-attempt selection with no publication reconciliation, startup
/// scan failure, or deferred cleanup remaining. Unrelated successful work
/// therefore cannot clear a failed component. Only the first bounded failure
/// detail in an unresolved retry sequence is retained per operation;
/// since-open counters preserve evidence for subsequent failures and after a
/// proven retry clears the detail.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MaintenanceStatus {
    /// Memtable-flush health.
    pub flush: MaintenanceOperationStatus,
    /// SSTable-compaction health.
    pub compaction: MaintenanceOperationStatus,
}

impl MaintenanceStatus {
    /// Whether neither maintenance operation has an unresolved failure.
    pub fn is_healthy(&self) -> bool {
        !self.flush.retry_pending && !self.compaction.retry_pending
    }
}

impl fmt::Display for MaintenanceStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn write_operation(
            formatter: &mut fmt::Formatter<'_>,
            name: &str,
            operation: &MaintenanceOperationStatus,
        ) -> fmt::Result {
            match &operation.unresolved_failure {
                Some(failure) => write!(
                    formatter,
                    "{name}=failure #{} from {:?}: {}{}",
                    failure.sequence_since_open,
                    failure.origin,
                    failure.message,
                    if failure.message_truncated {
                        " [truncated]"
                    } else {
                        ""
                    }
                ),
                None => write!(formatter, "{name}=healthy"),
            }
        }

        write_operation(formatter, "flush", &self.flush)?;
        formatter.write_str(", ")?;
        write_operation(formatter, "compaction", &self.compaction)
    }
}

/// Health and retry counters for one kind of database maintenance.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MaintenanceOperationStatus {
    /// Whether a failed operation still requires a successful retry.
    pub retry_pending: bool,
    /// Failed attempts since this database was opened.
    pub failures_since_open: u64,
    /// Failures originating in an automatic background task since open.
    pub background_failures_since_open: u64,
    /// Successful operations which cleared a pending failure since open.
    pub successful_retries_since_open: u64,
    /// The first unresolved failure retained for the current retry sequence.
    pub unresolved_failure: Option<MaintenanceFailure>,
}

/// Bounded detail for an unresolved maintenance failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceFailure {
    /// Monotonic ordinal among failures of this operation since open.
    pub sequence_since_open: u64,
    /// Where the failing attempt originated.
    pub origin: MaintenanceOrigin,
    /// Error text, retained up to 512 UTF-8 bytes.
    pub message: String,
    /// Whether the original error text exceeded the retained bound.
    pub message_truncated: bool,
}

/// Origin of a maintenance attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceOrigin {
    /// An explicit foreground flush or compaction request.
    Foreground,
    /// An automatic background maintenance request.
    Background,
    /// The final flush performed by graceful shutdown.
    Shutdown,
    /// Retryable cleanup discovered while reopening the database.
    Recovery,
}

/// Physical write-ahead log statistics.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WalStats {
    /// Whether the WAL is enabled for this database.
    pub enabled: bool,
    /// Bytes in the active WAL segment.
    pub active_segment_bytes: u64,
    /// Validated bytes in all retained WAL segments, including headers.
    /// A partial failed append is excluded until recovery repairs the tail.
    pub retained_valid_bytes: u64,
    /// Encoded WAL bytes appended successfully since this database was opened.
    pub bytes_written_since_open: u64,
}

/// Physical memtable statistics.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PhysicalMemTableStats {
    /// Approximate bytes in per-thread write buffers awaiting publication.
    pub buffered_bytes: u64,
    /// Approximate bytes in the writable memtable.
    pub active_bytes: u64,
    /// Approximate bytes in immutable memtables awaiting flush completion.
    pub immutable_bytes: u64,
    /// Physical versions in the writable memtable.
    pub active_versions: u64,
    /// Physical mutations in per-thread write buffers awaiting publication.
    pub buffered_versions: u64,
    /// Physical versions in immutable memtables.
    pub immutable_versions: u64,
    /// Tombstone versions across writable and immutable memtables.
    pub tombstones: u64,
    /// Immutable memtables awaiting flush completion.
    pub immutable_tables: u64,
}

/// Physical SSTable statistics.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PhysicalSSTableStats {
    /// Bytes occupied by installed SSTable files.
    pub bytes: u64,
    /// Number of installed SSTable files.
    pub files: u64,
    /// Number of installed level-zero SSTable files.
    pub level_zero_files: u64,
    /// Physical versions stored in installed SSTables.
    pub versions: u64,
    /// Tombstone versions stored in installed SSTables.
    pub tombstones: u64,
}

/// Physical version statistics.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PhysicalVersionStats {
    /// Current physical versions across memtables and installed SSTables.
    pub current: u64,
    /// Current tombstone versions across memtables and installed SSTables.
    pub tombstones: u64,
    /// Superseded versions discarded by completed compactions since open.
    pub reclaimed_by_compaction_since_open: u64,
    /// Tombstone versions among the superseded versions reclaimed by completed
    /// compactions since open. A winning tombstone remains current and is not
    /// counted as reclaimed.
    pub tombstones_reclaimed_by_compaction_since_open: u64,
}

/// Physical cache statistics.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PhysicalCacheStats {
    /// Whether the decompressed block cache is enabled.
    pub block_cache_enabled: bool,
    /// Current decompressed block-cache entry count.
    pub block_cache_entries: u64,
    /// Current decompressed block-cache bytes.
    pub block_cache_bytes: u64,
    /// Block-cache hits since open.
    pub block_cache_hits_since_open: u64,
    /// Block-cache misses since open.
    pub block_cache_misses_since_open: u64,
    /// Current cached SSTable-reader count.
    pub sstable_readers: u64,
    /// SSTable-reader cache hits since open.
    pub sstable_reader_hits_since_open: u64,
    /// SSTable-reader cache misses since open.
    pub sstable_reader_misses_since_open: u64,
    /// SSTable-reader evictions since open.
    pub sstable_reader_evictions_since_open: u64,
}

/// Controlled write-stall statistics.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteStallStats {
    /// Number of controlled stalls since open.
    pub count_since_open: u64,
    /// Total controlled stall time in microseconds since open.
    pub micros_since_open: u64,
}

/// Current write pressure and cumulative controlled-stall counters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteBackpressureStatus {
    /// Whether at least one cause is currently at its configured threshold.
    pub active: bool,
    /// Number of physical controlled stalls since open.
    pub stalls_since_open: u64,
    /// Total physical controlled-stall time in microseconds since open.
    pub stall_micros_since_open: u64,
    /// Immutable-memtable pressure and its attributed stall counters.
    pub immutable_memtables: WriteBackpressureCauseStatus,
    /// Level-zero file pressure and its attributed stall counters.
    pub level_zero_files: WriteBackpressureCauseStatus,
}

/// Current pressure and cumulative stalls attributed to one cause.
///
/// When both causes are active, each receives the event and duration while
/// [`WriteBackpressureStatus::stalls_since_open`] counts the physical sleep
/// once.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteBackpressureCauseStatus {
    /// Whether the sampled value currently meets or exceeds the threshold.
    pub active: bool,
    /// Current sampled immutable-table or level-zero-file count.
    pub current: u64,
    /// Configured count at which this cause becomes active.
    pub threshold: u64,
    /// Stall events attributed to this cause since open.
    pub count_since_open: u64,
    /// Stall time attributed to this cause since open.
    pub micros_since_open: u64,
}

/// Process-lifetime byte counters for write-amplification calculations.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteAmplificationStats {
    /// Key and value payload bytes from successful foreground mutations since open.
    /// Delete operations contribute their key bytes.
    pub logical_bytes_ingested_since_open: u64,
    /// Encoded WAL bytes appended successfully since open.
    pub wal_bytes_written_since_open: u64,
    /// SSTable file bytes produced by memtable flush attempts since open.
    pub flush_bytes_written_since_open: u64,
    /// Selected input SSTable file bytes for compaction attempts since open.
    /// Failed attempts count their selected inputs once execution begins.
    pub compaction_input_bytes_since_open: u64,
    /// Actual output-file bytes produced by compaction attempts since open.
    /// Partial files from failed attempts are included.
    pub compaction_output_bytes_since_open: u64,
}

impl WriteAmplificationStats {
    /// SSTable bytes written per logical foreground payload byte since open.
    ///
    /// WAL bytes are intentionally excluded so callers can report WAL and LSM
    /// amplification separately. Maintenance may rewrite data that predates
    /// this process, so this is an observed since-open activity ratio rather
    /// than a persisted database-lifetime ratio. Returns `None` before any
    /// foreground payload is ingested.
    pub fn sstable_write_amplification(&self) -> Option<f64> {
        (self.logical_bytes_ingested_since_open != 0).then(|| {
            self.flush_bytes_written_since_open
                .saturating_add(self.compaction_output_bytes_since_open) as f64
                / self.logical_bytes_ingested_since_open as f64
        })
    }
}

/// Result of a requested compaction operation.
///
/// Counts and byte totals describe actual completed compaction jobs, including
/// intermediate outputs consumed by a later job in the same manual drain.
/// [`Self::work_remaining`] reports whether another job was selectable in the
/// final live-state observation before the request released ownership.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactionResult {
    /// Number of SSTable inputs read.
    pub input_files: u64,
    /// Number of SSTable outputs installed.
    pub output_files: u64,
    /// Bytes read from input SSTables.
    pub bytes_read: u64,
    /// Bytes written to installed output SSTables.
    pub bytes_written: u64,
    /// Input bytes minus output bytes, saturating at zero.
    pub bytes_reclaimed: u64,
    /// Wall-clock duration of the complete request in milliseconds.
    pub duration_ms: u64,
    /// Whether another compaction job was selectable in the request's final
    /// live-state observation.
    ///
    /// Manual compaction drains the files in scope when it acquires ownership;
    /// later flushes enter that scope only when required by overlap closure.
    /// This can still be `true` when other concurrent flushes create new work.
    /// Background compaction deliberately runs at most one job.
    pub work_remaining: bool,
}

impl CompactionResult {
    /// Whether the coordinator had no selectable work when this request ended.
    pub fn is_complete(&self) -> bool {
        !self.work_remaining
    }
}

/// Write batch operation type
#[derive(Debug, Clone)]
pub enum BatchOp {
    /// Insert or update a key-value pair
    Put { key: Vec<u8>, value: Vec<u8> },
    /// Delete a key
    Delete { key: Vec<u8> },
}

/// Atomic write batch
#[derive(Debug, Clone, Default)]
pub struct WriteBatch {
    ops: Vec<BatchOp>,
}

impl WriteBatch {
    /// Create a new empty write batch
    pub fn new() -> Self {
        Self { ops: Vec::new() }
    }

    /// Create a write batch with pre-allocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            ops: Vec::with_capacity(capacity),
        }
    }

    /// Add a put operation
    pub fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, value: V) {
        self.ops.push(BatchOp::Put {
            key: key.as_ref().to_vec(),
            value: value.as_ref().to_vec(),
        });
    }

    /// Add a delete operation
    pub fn delete<K: AsRef<[u8]>>(&mut self, key: K) {
        self.ops.push(BatchOp::Delete {
            key: key.as_ref().to_vec(),
        });
    }

    /// Get the operations in this batch
    pub fn ops(&self) -> &[BatchOp] {
        &self.ops
    }

    /// Get the number of operations in this batch
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Clear all operations from the batch
    pub fn clear(&mut self) {
        self.ops.clear();
    }
}
