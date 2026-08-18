//! # Write-Ahead Log (WAL) for TurboKV
//!
//! ## Architecture
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    Write Path (Group Commit)                    │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  Writer 1 ──┐                                                   │
//! │  Writer 2 ──┼──► FIFO ──► Sequence + write ──► Shared fsync     │
//! │  Writer 3 ──┘                                                   │
//! └─────────────────────────────────────────────────────────────────┘
//!
//! ## File Format (v4)
//!
//! - Header: 64 bytes (magic, version, timestamps, sequence range)
//! - Entries: Header (32B) + Payload (variable)
//!
//! ## Zero-Allocation Write Path
//!
//! For maximum throughput, uses thread-local pre-allocated buffers
//! to avoid per-write heap allocations.

mod file;
mod iterator;
mod types;

pub use iterator::WalEntryIterator;
pub use types::{
    encode_delete, encode_kv, EntryType, Result, WalConfig, WalEntry, WalError,
    MAX_GROUP_COMMIT_DELAY_US, WAL_VERSION, WAL_VERSION_V1, WAL_VERSION_V2, WAL_VERSION_V3,
};

use std::cell::RefCell;
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use bytes::Bytes;
use parking_lot::RwLock;
use tokio::sync::{mpsc, oneshot};
use tracing::info;

use crate::core::crypto::crc32_checksum;
use crate::storage::{directory_lock::DirectoryLock, manifest::sync_directory, InProgressGuard};
use file::{
    create_file, entry_size, finalize_header, inspect_segment, open_recovered_file,
    preflight_active_segment, read_and_validate_header, read_entry_versioned,
    synchronize_segment_header, wal_sequence_from_path, write_entries_batch, write_entry, WalFile,
};
use types::{encode_batch, ENTRY_HEADER_SIZE, ENTRY_RESERVED_SIZE, WAL_HEADER_SIZE};

// Thread-local buffer for zero-allocation WAL writes
// Pre-allocated to avoid per-write heap allocations
thread_local! {
    static WAL_ENCODE_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(4096));
}

struct WriteRequest {
    record: PendingWalRecord,
    response: oneshot::Sender<Result<BatchAppend>>,
}

enum GroupCommitCommand {
    Write(WriteRequest),
    Barrier(oneshot::Sender<Result<()>>),
}

struct PendingWalRecord {
    entry_type: EntryType,
    data: Bytes,
    operation_count: u64,
}

struct PreparedWriteRequest {
    entry: WalEntry,
    sequences: Vec<u64>,
    response: oneshot::Sender<Result<BatchAppend>>,
}

pub(crate) struct BatchAppend {
    pub sequences: Vec<u64>,
}

#[derive(Clone)]
struct GroupCommitFailure(Arc<str>);

impl GroupCommitFailure {
    fn from_error(error: &WalError) -> Self {
        Self(Arc::from(format!("paranoid group commit failed: {error}")))
    }

    fn into_error(self) -> WalError {
        WalError::Io {
            message: self.0.to_string(),
            source: None,
        }
    }
}

struct GroupCommitWriter {
    current_file: Arc<RwLock<WalFile>>,
    sequence: Arc<AtomicU64>,
    durable_sequence: Arc<AtomicU64>,
    batch_ranges: Arc<RwLock<BTreeMap<u64, u64>>>,
    config: WalConfig,
    wal_dir: PathBuf,
    directory_lock: Option<Weak<DirectoryLock>>,
    failure: Arc<RwLock<Option<GroupCommitFailure>>>,
    in_progress: Arc<AtomicU64>,
    syncs: Arc<AtomicU64>,
    largest_group: Arc<AtomicU64>,
    byte_accounting: Arc<WalByteAccounting>,
}

impl GroupCommitWriter {
    async fn run(self, mut rx: mpsc::Receiver<GroupCommitCommand>) {
        let mut pending = None;
        loop {
            let command = match pending.take() {
                Some(command) => command,
                None => match rx.recv().await {
                    Some(command) => command,
                    None => break,
                },
            };
            let first = match command {
                GroupCommitCommand::Write(request) => request,
                GroupCommitCommand::Barrier(response) => {
                    let _ = response.send(Ok(()));
                    continue;
                }
            };

            let _in_progress = InProgressGuard::new(Arc::clone(&self.in_progress));
            let mut group = vec![first];
            let deadline = tokio::time::Instant::now()
                + std::time::Duration::from_micros(self.config.group_commit_delay_us);
            pending =
                collect_group(&mut rx, &mut group, self.config.max_batch_size, deadline).await;

            // Engine-owned WALs may outlive a cancelled append future. Do not
            // mutate after the Engine's directory ownership has ended; if a
            // mutation already started, retain ownership through its response.
            let _directory_lock = match self.directory_lock.as_ref() {
                Some(directory_lock) => match directory_lock.upgrade() {
                    Some(directory_lock) => Some(directory_lock),
                    None => {
                        poison_group(
                            &mut rx,
                            pending.take(),
                            group.into_iter().map(|request| request.response).collect(),
                            &self.failure,
                            WalError::ChannelClosed,
                        )
                        .await;
                        break;
                    }
                },
                None => None,
            };

            self.largest_group
                .fetch_max(group.len() as u64, Ordering::Relaxed);
            let start_sequence = match reserve_group_sequences(&self.sequence, &group) {
                Ok(start_sequence) => start_sequence,
                Err(error) => {
                    poison_group(
                        &mut rx,
                        pending.take(),
                        group.into_iter().map(|request| request.response).collect(),
                        &self.failure,
                        error,
                    )
                    .await;
                    break;
                }
            };
            let prepared = prepare_group(group, start_sequence);
            self.syncs.fetch_add(1, Ordering::Relaxed);
            match write_group_sync(
                &self.current_file,
                &prepared,
                &self.config,
                &self.wal_dir,
                &self.batch_ranges,
                &self.byte_accounting,
            ) {
                Ok(()) => {
                    let next_durable = prepared
                        .last()
                        .and_then(|request| request.sequences.last())
                        .copied()
                        .unwrap_or(start_sequence)
                        .saturating_add(1);
                    self.durable_sequence.store(next_durable, Ordering::Release);
                    for request in prepared {
                        let _ = request.response.send(Ok(BatchAppend {
                            sequences: request.sequences,
                        }));
                    }
                }
                Err(error) => {
                    poison_group(
                        &mut rx,
                        pending.take(),
                        prepared
                            .into_iter()
                            .map(|request| request.response)
                            .collect(),
                        &self.failure,
                        error,
                    )
                    .await;
                    break;
                }
            }
        }
    }
}

struct EncodedBatchRecord {
    sequences: Vec<u64>,
    encoded: Vec<u8>,
    first_sequence: u64,
    last_sequence: u64,
}

struct WalByteAccounting {
    retained_valid: AtomicU64,
    written_since_open: AtomicU64,
}

impl WalByteAccounting {
    fn new(retained_valid: u64) -> Self {
        Self {
            retained_valid: AtomicU64::new(retained_valid),
            written_since_open: AtomicU64::new(0),
        }
    }

    fn record_segment_created(&self) {
        self.retained_valid
            .fetch_add(WAL_HEADER_SIZE as u64, Ordering::Relaxed);
    }

    fn record_append(&self, bytes: u64) {
        self.retained_valid.fetch_add(bytes, Ordering::Relaxed);
        // Publish the cumulative counter after the retained gauge so a
        // sampler that observes this append can also observe its valid bytes.
        self.written_since_open.fetch_add(bytes, Ordering::Release);
    }

    fn record_reclamation(&self, bytes: u64) {
        self.retained_valid.fetch_sub(bytes, Ordering::Relaxed);
    }

    fn retained_valid(&self) -> u64 {
        self.retained_valid.load(Ordering::Relaxed)
    }

    fn written_since_open(&self) -> u64 {
        self.written_since_open.load(Ordering::Acquire)
    }
}

pub struct WriteAheadLog {
    wal_dir: PathBuf,
    config: WalConfig,
    current_file: Arc<RwLock<WalFile>>,
    sequence: Arc<AtomicU64>,
    /// Atomic batch spans used to keep durable checkpoints on batch boundaries.
    batch_ranges: Arc<RwLock<BTreeMap<u64, u64>>>,
    byte_accounting: Arc<WalByteAccounting>,
    write_tx: mpsc::Sender<GroupCommitCommand>,
    durable_sequence: Arc<AtomicU64>,
    group_commit_failure: Arc<RwLock<Option<GroupCommitFailure>>>,
    #[allow(dead_code)]
    group_commit_in_progress: Arc<AtomicU64>,
    #[allow(dead_code)]
    group_commit_syncs: Arc<AtomicU64>,
    #[allow(dead_code)]
    largest_group_commit: Arc<AtomicU64>,
}

impl WriteAheadLog {
    pub async fn new(wal_dir: impl AsRef<Path>, config: WalConfig) -> Result<Self> {
        Self::new_inner(wal_dir, config, None, None).await
    }

    pub(crate) async fn new_preflighted_with_directory_lock(
        wal_dir: impl AsRef<Path>,
        config: WalConfig,
        directory_lock: Weak<DirectoryLock>,
        preflight: WalDirectoryPreflight,
    ) -> Result<Self> {
        Self::new_inner(wal_dir, config, Some(directory_lock), Some(preflight)).await
    }

    async fn new_inner(
        wal_dir: impl AsRef<Path>,
        config: WalConfig,
        directory_lock: Option<Weak<DirectoryLock>>,
        preflight: Option<WalDirectoryPreflight>,
    ) -> Result<Self> {
        if config.max_batch_size == 0 {
            return Err(WalError::InvalidFormat(
                "paranoid group commit maximum must be greater than zero".to_string(),
            ));
        }
        if config.group_commit_delay_us > MAX_GROUP_COMMIT_DELAY_US {
            return Err(WalError::InvalidFormat(format!(
                "paranoid group commit delay must not exceed {MAX_GROUP_COMMIT_DELAY_US} microseconds"
            )));
        }

        let wal_dir = wal_dir.as_ref().to_path_buf();
        let preflight = match preflight {
            Some(preflight) if preflight.wal_dir == wal_dir => preflight,
            Some(_) => {
                return Err(WalError::InvalidFormat(
                    "WAL preflight directory does not match the open directory".to_string(),
                ))
            }
            None => preflight_directory(&wal_dir).await?,
        };
        tokio::fs::create_dir_all(&wal_dir)
            .await
            .map_err(|e| WalError::Io {
                message: format!("Failed to create WAL directory: {:?}", wal_dir),
                source: Some(e),
            })?;

        let (wal_file, initial_sequence, batch_ranges) =
            Self::open_or_create(&wal_dir, &config, preflight)?;
        sync_wal_directory(&wal_dir)?;
        let byte_accounting = Arc::new(WalByteAccounting::new(retained_wal_bytes(&wal_dir)?));
        let current_file = Arc::new(RwLock::new(wal_file));
        let queue_capacity = config.max_batch_size.saturating_mul(2).clamp(1, 1 << 20);
        let (write_tx, write_rx) = mpsc::channel::<GroupCommitCommand>(queue_capacity);

        let bg_file = Arc::clone(&current_file);
        let bg_config = config.clone();
        let bg_dir = wal_dir.clone();
        let sequence = Arc::new(AtomicU64::new(initial_sequence));
        let bg_sequence = Arc::clone(&sequence);
        let durable_sequence = Arc::new(AtomicU64::new(initial_sequence));
        let bg_durable_sequence = Arc::clone(&durable_sequence);
        let batch_ranges = Arc::new(RwLock::new(batch_ranges));
        let bg_batch_ranges = Arc::clone(&batch_ranges);
        let group_commit_in_progress = Arc::new(AtomicU64::new(0));
        let bg_group_commit_in_progress = Arc::clone(&group_commit_in_progress);
        let group_commit_failure = Arc::new(RwLock::new(None));
        let bg_group_commit_failure = Arc::clone(&group_commit_failure);
        let group_commit_syncs = Arc::new(AtomicU64::new(0));
        let bg_group_commit_syncs = Arc::clone(&group_commit_syncs);
        let largest_group_commit = Arc::new(AtomicU64::new(0));
        let bg_largest_group_commit = Arc::clone(&largest_group_commit);
        let bg_byte_accounting = Arc::clone(&byte_accounting);
        let group_commit_writer = GroupCommitWriter {
            current_file: bg_file,
            sequence: bg_sequence,
            durable_sequence: bg_durable_sequence,
            batch_ranges: bg_batch_ranges,
            config: bg_config,
            wal_dir: bg_dir,
            directory_lock,
            failure: bg_group_commit_failure,
            in_progress: bg_group_commit_in_progress,
            syncs: bg_group_commit_syncs,
            largest_group: bg_largest_group_commit,
            byte_accounting: bg_byte_accounting,
        };
        tokio::spawn(group_commit_writer.run(write_rx));

        Ok(Self {
            wal_dir,
            config,
            current_file,
            sequence,
            batch_ranges,
            byte_accounting,
            write_tx,
            durable_sequence,
            group_commit_failure,
            group_commit_in_progress,
            group_commit_syncs,
            largest_group_commit,
        })
    }

    pub async fn append(&self, key: &[u8], value: &[u8]) -> Result<u64> {
        if self.config.sync_on_write {
            let appended = self
                .enqueue_paranoid(PendingWalRecord {
                    entry_type: EntryType::Data,
                    data: Bytes::from(encode_kv(key, value)),
                    operation_count: 1,
                })
                .await?;
            Ok(appended.sequences[0])
        } else {
            // Non-sync mode (durable): use zero-allocation fast path
            self.append_zero_alloc(key, value, EntryType::Data)
        }
    }

    /// Zero-allocation append - uses thread-local buffer to avoid heap allocations
    ///
    /// This is the fast path for durable mode (WAL without fsync).
    /// Eliminates per-write allocations by reusing a thread-local buffer.
    #[inline]
    fn append_zero_alloc(&self, key: &[u8], value: &[u8], entry_type: EntryType) -> Result<u64> {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        let timestamp = super::cached_time::now_ms();

        WAL_ENCODE_BUFFER.with(|buf_cell| {
            let mut buf = buf_cell.borrow_mut();
            buf.clear();

            // Calculate data length: key_len(4) + key + value
            let data_len = 4 + key.len() + value.len();
            let total_len = ENTRY_HEADER_SIZE + data_len;

            // Ensure buffer capacity
            let cap = buf.capacity();
            if cap < total_len {
                buf.reserve(total_len - cap);
            }

            // Build entry directly in buffer

            // 1. Data length (u32) - offset 0
            buf.extend_from_slice(&(data_len as u32).to_le_bytes());
            // 2. Sequence (u64) - offset 4
            buf.extend_from_slice(&sequence.to_le_bytes());
            // 3. Timestamp (u64) - offset 12
            buf.extend_from_slice(&timestamp.to_le_bytes());
            // 4. Entry type (u8) - offset 20
            buf.push(entry_type as u8);
            // 5. Flags (u8) - offset 21
            buf.push(0);
            // 6. CRC placeholder (u32) - offset 22, will fill after encoding data
            let crc_offset = buf.len();
            buf.extend_from_slice(&[0u8; 4]);
            // 7. Reserved (6 bytes) - offset 26
            buf.extend_from_slice(&[0_u8; ENTRY_RESERVED_SIZE]);

            // 8. Encode key-value data
            let data_start = buf.len();
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(value);

            // 9. Compute CRC over data portion and fill in
            let crc = crc32_checksum(&buf[data_start..]);
            buf[crc_offset..crc_offset + 4].copy_from_slice(&crc.to_le_bytes());

            // Check rotation and write
            let entry_bytes = buf.len() as u64;
            // Single write to file
            let mut file = self.current_file.write();
            if file.should_rotate(entry_bytes, self.config.max_file_size) {
                finalize_header(&mut file)?;
                let new_seq = file.next_segment_sequence()?;
                *file = create_file(&self.wal_dir, new_seq, &self.config)?;
                self.byte_accounting.record_segment_created();
                info!("Rotated WAL file, new sequence: {}", new_seq);
            }
            file.file.write_all(&buf)?;
            self.byte_accounting.record_append(entry_bytes);
            file.record_append(entry_bytes, 1, sequence, sequence);

            Ok(sequence)
        })
    }

    pub async fn append_delete(&self, key: &[u8]) -> Result<u64> {
        if self.config.sync_on_write {
            let appended = self
                .enqueue_paranoid(PendingWalRecord {
                    entry_type: EntryType::Delete,
                    data: Bytes::from(encode_delete(key)),
                    operation_count: 1,
                })
                .await?;
            Ok(appended.sequences[0])
        } else {
            // Non-sync mode: use zero-allocation fast path
            self.append_delete_zero_alloc(key)
        }
    }

    /// Zero-allocation delete append
    #[inline]
    fn append_delete_zero_alloc(&self, key: &[u8]) -> Result<u64> {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        let timestamp = super::cached_time::now_ms();

        WAL_ENCODE_BUFFER.with(|buf_cell| {
            let mut buf = buf_cell.borrow_mut();
            buf.clear();

            // Data length: key_len(4) + key (no value for delete)
            let data_len = 4 + key.len();
            let total_len = ENTRY_HEADER_SIZE + data_len;

            let cap = buf.capacity();
            if cap < total_len {
                buf.reserve(total_len - cap);
            }

            // Build entry header
            buf.extend_from_slice(&(data_len as u32).to_le_bytes());
            buf.extend_from_slice(&sequence.to_le_bytes());
            buf.extend_from_slice(&timestamp.to_le_bytes());
            buf.push(EntryType::Delete as u8);
            buf.push(0); // flags
            let crc_offset = buf.len();
            buf.extend_from_slice(&[0u8; 4]); // CRC placeholder
            buf.extend_from_slice(&[0_u8; ENTRY_RESERVED_SIZE]);

            // Encode key only (no value for delete)
            let data_start = buf.len();
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(key);

            // Compute CRC
            let crc = crc32_checksum(&buf[data_start..]);
            buf[crc_offset..crc_offset + 4].copy_from_slice(&crc.to_le_bytes());

            // Check rotation and write
            let entry_bytes = buf.len() as u64;
            let mut file = self.current_file.write();
            if file.should_rotate(entry_bytes, self.config.max_file_size) {
                finalize_header(&mut file)?;
                let new_seq = file.next_segment_sequence()?;
                *file = create_file(&self.wal_dir, new_seq, &self.config)?;
                self.byte_accounting.record_segment_created();
                info!("Rotated WAL file, new sequence: {}", new_seq);
            }
            file.file.write_all(&buf)?;
            self.byte_accounting.record_append(entry_bytes);
            file.record_append(entry_bytes, 1, sequence, sequence);

            Ok(sequence)
        })
    }

    /// Append multiple key-value pairs as one checksummed physical record.
    ///
    /// In paranoid mode the envelope is one member of a shared commit group;
    /// the group-size limit counts this caller once, regardless of its logical
    /// operation count.
    pub async fn append_batch(&self, entries: &[(&[u8], Option<&[u8]>)]) -> Result<Vec<u64>> {
        Ok(self.append_batch_with_metadata(entries).await?.sequences)
    }

    pub(crate) async fn append_batch_with_metadata(
        &self,
        entries: &[(&[u8], Option<&[u8]>)],
    ) -> Result<BatchAppend> {
        if entries.is_empty() {
            return Ok(BatchAppend {
                sequences: Vec::new(),
            });
        }

        if self.config.sync_on_write {
            return self
                .enqueue_paranoid(PendingWalRecord {
                    entry_type: EntryType::Batch,
                    data: Bytes::from(encode_batch(entries)?),
                    operation_count: u64::try_from(entries.len()).map_err(|_| {
                        WalError::InvalidFormat(
                            "batch operation count does not fit a sequence range".to_string(),
                        )
                    })?,
                })
                .await;
        }

        let batch = self.encode_entries_batch(entries)?;
        self.write_encoded_batch(&batch).await?;
        self.batch_ranges
            .write()
            .insert(batch.first_sequence, batch.last_sequence);

        Ok(BatchAppend {
            sequences: batch.sequences,
        })
    }

    pub async fn flush(&self) -> Result<()> {
        if self.config.sync_on_write {
            self.await_group_commit_barrier().await?;
        }
        let mut file = self.current_file.write();
        finalize_header(&mut file)?;
        sync_wal_directory(&self.wal_dir)?;
        self.durable_sequence
            .fetch_max(file.next_written_sequence(), Ordering::Release);
        Ok(())
    }

    pub async fn read_from(&self, start_sequence: u64) -> Result<Vec<WalEntry>> {
        let mut entries = Vec::new();
        let mut seen = HashSet::new();

        self.flush().await?;

        let current_path = self.current_file.read().path.clone();
        let wal_files = self.list_wal_files().await?;

        for (_, path) in &wal_files {
            if *path == current_path {
                continue;
            }
            self.read_entries_from_file(path, start_sequence, &mut entries, &mut seen)?;
        }

        self.read_entries_from_file(&current_path, start_sequence, &mut entries, &mut seen)?;
        entries.sort_by_key(|e| e.sequence);

        Ok(entries)
    }

    pub async fn iter_entries(&self) -> Result<WalEntryIterator> {
        self.iter_entries_from(0).await
    }

    pub async fn iter_entries_from(&self, start_sequence: u64) -> Result<WalEntryIterator> {
        self.flush().await?;

        let wal_files = self.list_wal_files().await?;

        let paths: Vec<PathBuf> = wal_files.into_iter().map(|(_, path)| path).collect();

        WalEntryIterator::new(paths, start_sequence)
    }

    pub async fn truncate(&self, up_to_sequence: u64) -> Result<()> {
        info!("Truncating WAL up to sequence {}", up_to_sequence);

        let wal_files = self.list_wal_files().await?;
        self.truncate_files(&wal_files, up_to_sequence)
    }

    fn truncate_files(&self, wal_files: &[(u64, PathBuf)], up_to_sequence: u64) -> Result<()> {
        // Validate immutable candidates without blocking appends. A file that
        // is non-active here cannot become active later because rotation only
        // advances filenames. If the observed active file rotates meanwhile,
        // leaving it for the next reclamation pass is conservative.
        let observed_current_path = self.current_file.read().path.clone();
        let mut eligible = Vec::new();

        for (_, path) in wal_files {
            if *path == observed_current_path {
                continue;
            }
            let metadata = inspect_segment(path, false)?;
            if metadata
                .last_sequence
                .map_or(true, |last| last < up_to_sequence)
            {
                eligible.push((path, metadata.valid_end));
            }
        }

        // Keep the active identity stable only for the final recheck/unlink
        // window. Rotation and appends require the write side of this lock.
        let current_file = self.current_file.read();
        for (path, valid_bytes) in eligible {
            if *path != current_file.path {
                info!("Deleting WAL file: {:?}", path);
                std::fs::remove_file(path)?;
                self.byte_accounting.record_reclamation(valid_bytes);
            }
        }
        self.batch_ranges
            .write()
            .retain(|_, last| *last >= up_to_sequence);
        Ok(())
    }

    pub fn current_sequence(&self) -> u64 {
        self.sequence.load(Ordering::SeqCst)
    }

    /// Exclusive upper bound of WAL sequences covered by a successful sync.
    ///
    /// Unsynced durable-mode appends do not advance this frontier. A failed
    /// paranoid group leaves it unchanged even though recovery may later find
    /// complete records from the failed, outcome-indeterminate attempt. Reopen
    /// validates and syncs every retained segment before initializing the
    /// recovered frontier.
    pub fn durable_sequence(&self) -> u64 {
        self.durable_sequence.load(Ordering::Acquire)
    }

    /// Move a proposed checkpoint back to the start of any batch it splits.
    pub(crate) fn align_checkpoint(&self, proposed: u64) -> u64 {
        let ranges = self.batch_ranges.read();
        ranges
            .range(..proposed)
            .next_back()
            .and_then(|(&start, &end)| (proposed <= end).then_some(start))
            .unwrap_or(proposed)
    }

    /// Ensure future WAL entries do not reuse sequences already persisted in
    /// SSTables (including databases created without a WAL).
    pub(crate) fn ensure_next_sequence_at_least(&self, next_sequence: u64) {
        self.sequence.fetch_max(next_sequence, Ordering::SeqCst);
    }

    /// Returns the current WAL file size in bytes (synchronous).
    pub fn current_size(&self) -> u64 {
        let file = self.current_file.read();
        file.size
    }

    /// Returns validated bytes in all retained WAL segments, including headers.
    /// A partial failed append is excluded until recovery repairs the tail.
    pub fn retained_size(&self) -> u64 {
        self.byte_accounting.retained_valid()
    }

    /// Returns encoded WAL entry bytes appended successfully since open.
    /// Segment headers and recovered pre-open bytes are excluded.
    pub fn bytes_written_since_open(&self) -> u64 {
        self.byte_accounting.written_since_open()
    }

    #[cfg(test)]
    pub(crate) fn lock_current_file_for_test(&self) -> parking_lot::RwLockWriteGuard<'_, WalFile> {
        self.current_file.write()
    }

    #[cfg(test)]
    pub(crate) fn group_commit_in_progress_for_test(&self) -> u64 {
        self.group_commit_in_progress.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn group_commit_syncs_for_test(&self) -> u64 {
        self.group_commit_syncs.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn largest_group_commit_for_test(&self) -> u64 {
        self.largest_group_commit.load(Ordering::Acquire)
    }

    // ========================================
    // Private methods
    // ========================================

    async fn enqueue_paranoid(&self, record: PendingWalRecord) -> Result<BatchAppend> {
        self.submit_group_command(|response| {
            GroupCommitCommand::Write(WriteRequest { record, response })
        })
        .await
    }

    async fn await_group_commit_barrier(&self) -> Result<()> {
        self.submit_group_command(GroupCommitCommand::Barrier).await
    }

    async fn submit_group_command<T>(
        &self,
        command: impl FnOnce(oneshot::Sender<Result<T>>) -> GroupCommitCommand,
    ) -> Result<T> {
        if let Some(failure) = self.group_commit_failure.read().clone() {
            return Err(failure.into_error());
        }

        let (response, receive) = oneshot::channel();
        if self.write_tx.send(command(response)).await.is_err() {
            return Err(self
                .group_commit_failure
                .read()
                .clone()
                .map_or(WalError::ChannelClosed, GroupCommitFailure::into_error));
        }

        receive.await.map_err(|_| {
            self.group_commit_failure
                .read()
                .clone()
                .map_or(WalError::ChannelClosed, GroupCommitFailure::into_error)
        })?
    }

    pub(crate) fn group_commit_failure_for_status(&self) -> Option<String> {
        self.group_commit_failure
            .read()
            .clone()
            .map(|failure| failure.into_error().to_string())
    }

    fn encode_entries_batch(
        &self,
        entries: &[(&[u8], Option<&[u8]>)],
    ) -> Result<EncodedBatchRecord> {
        let start_sequence = self
            .sequence
            .fetch_add(entries.len() as u64, Ordering::SeqCst);
        let last_sequence = start_sequence
            .checked_add(entries.len() as u64 - 1)
            .ok_or_else(|| WalError::InvalidFormat("batch sequence range overflows".to_string()))?;
        let sequences = (start_sequence..=last_sequence).collect();
        let entry = WalEntry {
            sequence: start_sequence,
            timestamp: super::cached_time::now_ms(),
            entry_type: EntryType::Batch,
            data: Bytes::from(encode_batch(entries)?),
        };
        let mut encoded = Vec::with_capacity(entry_size(&entry));
        write_entry(&mut encoded, &entry)?;
        Ok(EncodedBatchRecord {
            sequences,
            encoded,
            first_sequence: start_sequence,
            last_sequence,
        })
    }

    async fn write_encoded_batch(&self, batch: &EncodedBatchRecord) -> Result<()> {
        let total_batch_size = batch.encoded.len() as u64;
        let needs_rotation = {
            let f = self.current_file.read();
            f.should_rotate(total_batch_size, self.config.max_file_size)
        };
        if needs_rotation {
            self.rotate().await?;
        }

        let mut f = self.current_file.write();
        f.file.write_all(&batch.encoded)?;
        self.byte_accounting.record_append(total_batch_size);
        f.record_append(
            total_batch_size,
            batch.sequences.len() as u64,
            batch.first_sequence,
            batch.last_sequence,
        );

        if self.config.sync_on_write {
            f.file.sync_all()?;
        }
        Ok(())
    }

    async fn rotate(&self) -> Result<()> {
        rotate_sync(
            &self.current_file,
            &self.wal_dir,
            &self.config,
            &self.byte_accounting,
        )
    }

    fn open_or_create(
        wal_dir: &Path,
        config: &WalConfig,
        mut preflight: WalDirectoryPreflight,
    ) -> Result<(WalFile, u64, BTreeMap<u64, u64>)> {
        if let Some(latest) = preflight.segments.pop() {
            let mut next_sequence = 0;
            let mut batch_ranges = BTreeMap::new();
            for segment in preflight.segments {
                synchronize_segment_header(&segment.path, &segment.metadata)?;
                next_sequence = next_sequence.max(segment.metadata.next_sequence());
                batch_ranges.extend(segment.metadata.batch_ranges.iter().copied());
            }
            let (mut file, latest_next_sequence) =
                open_recovered_file(&latest.path, &latest.metadata)?;
            next_sequence = next_sequence.max(latest_next_sequence);
            batch_ranges.extend(latest.metadata.batch_ranges.iter().copied());

            // Older segments remain readable, but current writes use v4. Start
            // a new segment rather than adding v4 batch records to an old one.
            if !latest.metadata.format.is_current() {
                finalize_header(&mut file)?;
                let new_sequence = next_sequence.max(latest.filename_sequence.saturating_add(1));
                return Ok((
                    create_file(wal_dir, new_sequence, config)?,
                    new_sequence,
                    batch_ranges,
                ));
            }

            if file.entry_count == 0 {
                file.first_sequence = next_sequence;
                file.last_sequence = next_sequence;
                finalize_header(&mut file)?;
            }

            Ok((file, next_sequence, batch_ranges))
        } else {
            Ok((create_file(wal_dir, 0, config)?, 0, BTreeMap::new()))
        }
    }

    async fn list_wal_files(&self) -> Result<Vec<(u64, PathBuf)>> {
        discover_wal_files(&self.wal_dir).await
    }

    fn read_entries_from_file(
        &self,
        path: &Path,
        start_sequence: u64,
        entries: &mut Vec<WalEntry>,
        seen: &mut HashSet<u64>,
    ) -> Result<()> {
        let file = File::open(path)?;
        let file_end = file.metadata()?.len();
        let mut reader = BufReader::new(file);
        let (format, _) = read_and_validate_header(&mut reader)?;
        reader.seek(SeekFrom::Start(WAL_HEADER_SIZE as u64))?;

        loop {
            let remaining = file_end.saturating_sub(reader.stream_position()?);
            match read_entry_versioned(&mut reader, format, remaining) {
                Ok(entry) => {
                    for entry in entry.into_logical_entries()? {
                        if entry.sequence >= start_sequence && !seen.contains(&entry.sequence) {
                            seen.insert(entry.sequence);
                            entries.push(entry);
                        }
                    }
                }
                Err(WalError::Eof) => break,
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}

struct PreflightSegment {
    filename_sequence: u64,
    path: PathBuf,
    metadata: file::SegmentMetadata,
}

/// Proof that every retained segment passed read-only validation. Construction
/// consumes this token before any header rewrite, tail repair, or new segment.
pub(crate) struct WalDirectoryPreflight {
    wal_dir: PathBuf,
    segments: Vec<PreflightSegment>,
}

/// Validate every retained segment without rewriting headers, truncating a
/// recoverable active tail, creating a v4 segment, or syncing the directory.
pub(crate) async fn preflight_directory(wal_dir: &Path) -> Result<WalDirectoryPreflight> {
    let wal_files = discover_wal_files(wal_dir).await?;
    let mut segments = Vec::with_capacity(wal_files.len());
    let last_index = wal_files.len().checked_sub(1);
    for (index, (filename_sequence, path)) in wal_files.into_iter().enumerate() {
        let metadata = if Some(index) == last_index {
            preflight_active_segment(&path)?
        } else {
            inspect_segment(&path, false)?
        };
        segments.push(PreflightSegment {
            filename_sequence,
            path,
            metadata,
        });
    }
    Ok(WalDirectoryPreflight {
        wal_dir: wal_dir.to_path_buf(),
        segments,
    })
}

async fn discover_wal_files(wal_dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    if !wal_dir.exists() {
        return Ok(Vec::new());
    }

    let mut entries = tokio::fs::read_dir(wal_dir).await?;
    let mut wal_files = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if entry.file_type().await?.is_file() {
            if let Some(sequence) = wal_sequence_from_path(&path) {
                wal_files.push((sequence, path));
            }
        }
    }
    wal_files.sort_by_key(|(sequence, _)| *sequence);
    Ok(wal_files)
}

// ========================================
// Synchronous helper routines
// ========================================

async fn collect_group(
    rx: &mut mpsc::Receiver<GroupCommitCommand>,
    group: &mut Vec<WriteRequest>,
    maximum: usize,
    deadline: tokio::time::Instant,
) -> Option<GroupCommitCommand> {
    while group.len() < maximum {
        match rx.try_recv() {
            Ok(GroupCommitCommand::Write(request)) => {
                group.push(request);
                continue;
            }
            Ok(command @ GroupCommitCommand::Barrier(_)) => return Some(command),
            Err(mpsc::error::TryRecvError::Disconnected) => return None,
            Err(mpsc::error::TryRecvError::Empty) => {}
        }

        if deadline <= tokio::time::Instant::now() {
            return None;
        }
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(GroupCommitCommand::Write(request))) => group.push(request),
            Ok(Some(command @ GroupCommitCommand::Barrier(_))) => return Some(command),
            Ok(None) | Err(_) => return None,
        }
    }
    None
}

fn reserve_group_sequences(sequence: &AtomicU64, group: &[WriteRequest]) -> Result<u64> {
    let operation_count = group.iter().try_fold(0_u64, |total, request| {
        total
            .checked_add(request.record.operation_count)
            .ok_or_else(|| WalError::InvalidFormat("group sequence range overflows".to_string()))
    })?;
    sequence
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
            current.checked_add(operation_count)
        })
        .map_err(|_| WalError::InvalidFormat("group sequence range overflows".to_string()))
}

fn prepare_group(group: Vec<WriteRequest>, start_sequence: u64) -> Vec<PreparedWriteRequest> {
    let mut next_sequence = start_sequence;
    group
        .into_iter()
        .map(|request| {
            let end_sequence = next_sequence + request.record.operation_count;
            let sequences = (next_sequence..end_sequence).collect();
            let entry = WalEntry {
                sequence: next_sequence,
                timestamp: super::cached_time::now_ms(),
                entry_type: request.record.entry_type,
                data: request.record.data,
            };
            next_sequence = end_sequence;
            PreparedWriteRequest {
                entry,
                sequences,
                response: request.response,
            }
        })
        .collect()
}

async fn poison_group(
    rx: &mut mpsc::Receiver<GroupCommitCommand>,
    pending: Option<GroupCommitCommand>,
    responses: Vec<oneshot::Sender<Result<BatchAppend>>>,
    shared_failure: &RwLock<Option<GroupCommitFailure>>,
    error: WalError,
) {
    let failure = install_group_failure(shared_failure, &error);
    rx.close();
    for response in responses {
        let _ = response.send(Err(failure.clone().into_error()));
    }
    if let Some(command) = pending {
        fail_group_command(command, &failure);
    }
    while let Some(command) = rx.recv().await {
        fail_group_command(command, &failure);
    }
}

fn fail_group_command(command: GroupCommitCommand, failure: &GroupCommitFailure) {
    match command {
        GroupCommitCommand::Write(request) => {
            let _ = request.response.send(Err(failure.clone().into_error()));
        }
        GroupCommitCommand::Barrier(response) => {
            let _ = response.send(Err(failure.clone().into_error()));
        }
    }
}

fn install_group_failure(
    shared_failure: &RwLock<Option<GroupCommitFailure>>,
    error: &WalError,
) -> GroupCommitFailure {
    let mut stored = shared_failure.write();
    stored
        .get_or_insert_with(|| GroupCommitFailure::from_error(error))
        .clone()
}

fn write_group_sync(
    current_file: &Arc<RwLock<WalFile>>,
    group: &[PreparedWriteRequest],
    config: &WalConfig,
    wal_dir: &Path,
    batch_ranges: &RwLock<BTreeMap<u64, u64>>,
    byte_accounting: &WalByteAccounting,
) -> Result<()> {
    let mut f = current_file.write();
    let mut rotated = false;
    let mut start = 0;
    while start < group.len() {
        let first_size = entry_size(&group[start].entry) as u64;
        if f.should_rotate(first_size, config.max_file_size) {
            rotate_group_segment(&mut f, wal_dir, config, byte_accounting)?;
            rotated = true;
        }

        let mut end = start;
        let mut chunk_bytes = 0_u64;
        while end < group.len() {
            let request_bytes = entry_size(&group[end].entry) as u64;
            let candidate_bytes = chunk_bytes
                .checked_add(request_bytes)
                .ok_or_else(|| WalError::InvalidFormat("group byte size overflows".to_string()))?;
            let has_prior_entry = f.entry_count > 0 || end > start;
            if has_prior_entry && f.size.saturating_add(candidate_bytes) > config.max_file_size {
                break;
            }
            chunk_bytes = candidate_bytes;
            end += 1;
        }

        write_group_chunk(&mut f, &group[start..end], chunk_bytes, byte_accounting)?;
        start = end;
        if start < group.len() {
            rotate_group_segment(&mut f, wal_dir, config, byte_accounting)?;
            rotated = true;
        }
    }

    {
        let mut ranges = batch_ranges.write();
        for request in group {
            if request.entry.entry_type == EntryType::Batch {
                let last_sequence = request
                    .sequences
                    .last()
                    .copied()
                    .expect("paranoid batch has at least one operation");
                ranges.insert(request.entry.sequence, last_sequence);
            }
        }
    }

    sync_wal_data(&f.file, wal_dir)?;
    if rotated {
        sync_wal_directory(wal_dir)?;
    }
    Ok(())
}

fn write_group_chunk(
    file: &mut WalFile,
    group: &[PreparedWriteRequest],
    encoded_bytes: u64,
    byte_accounting: &WalByteAccounting,
) -> Result<()> {
    let entries: Vec<&WalEntry> = group.iter().map(|request| &request.entry).collect();
    write_entries_batch(&mut file.file, &entries)?;
    byte_accounting.record_append(encoded_bytes);

    let first = group
        .first()
        .expect("group-commit segment chunk is nonempty");
    let last = group
        .last()
        .expect("group-commit segment chunk is nonempty");
    let last_sequence = last
        .sequences
        .last()
        .copied()
        .unwrap_or(last.entry.sequence);
    let operation_count = group.iter().fold(0_u64, |total, request| {
        total.saturating_add(request.sequences.len() as u64)
    });
    file.record_append(
        encoded_bytes,
        operation_count,
        first.entry.sequence,
        last_sequence,
    );
    Ok(())
}

fn rotate_group_segment(
    file: &mut WalFile,
    wal_dir: &Path,
    config: &WalConfig,
    byte_accounting: &WalByteAccounting,
) -> Result<()> {
    finalize_header(file)?;
    let new_sequence = file.next_segment_sequence()?;
    *file = create_file(wal_dir, new_sequence, config)?;
    byte_accounting.record_segment_created();
    info!("Rotated WAL file, new sequence: {}", new_sequence);
    Ok(())
}

fn sync_wal_data(file: &File, _wal_dir: &Path) -> Result<()> {
    #[cfg(test)]
    check_wal_failpoint(
        _wal_dir,
        super::failpoints::PersistenceBoundary::WalDataSync,
    )?;
    file.sync_all()?;
    Ok(())
}

fn sync_wal_directory(wal_dir: &Path) -> Result<()> {
    #[cfg(test)]
    check_wal_failpoint(
        wal_dir,
        super::failpoints::PersistenceBoundary::WalDirectorySync,
    )?;
    sync_directory(wal_dir)?;
    Ok(())
}

#[cfg(test)]
fn check_wal_failpoint(
    wal_dir: &Path,
    boundary: super::failpoints::PersistenceBoundary,
) -> Result<()> {
    super::failpoints::check(wal_dir, boundary).map_err(|error| WalError::Io {
        message: error.to_string(),
        source: None,
    })?;
    if let Some(data_dir) = wal_dir.parent() {
        super::failpoints::check(data_dir, boundary).map_err(|error| WalError::Io {
            message: error.to_string(),
            source: None,
        })?;
    }
    Ok(())
}

fn rotate_sync(
    current_file: &Arc<RwLock<WalFile>>,
    wal_dir: &Path,
    config: &WalConfig,
    byte_accounting: &WalByteAccounting,
) -> Result<()> {
    let mut current = current_file.write();
    rotate_group_segment(&mut current, wal_dir, config, byte_accounting)
}

fn retained_wal_bytes(wal_dir: &Path) -> Result<u64> {
    std::fs::read_dir(wal_dir)?.try_fold(0_u64, |total, entry| {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_file() && wal_sequence_from_path(&path).is_some() {
            Ok(total.saturating_add(entry.metadata()?.len()))
        } else {
            Ok(total)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::types::{LEGACY_ENTRY_EXTENSION_SIZE, WAL_FIRST_SEQUENCE_OFFSET};
    use super::*;
    use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
    use rand::rngs::StdRng;
    use rand::{Rng, RngCore, SeedableRng};
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};
    use tempfile::TempDir;

    use crate::storage::test_support::stress_context;

    fn wal_paths_result(directory: &Path) -> std::io::Result<Vec<PathBuf>> {
        let mut paths = std::fs::read_dir(directory)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<std::io::Result<Vec<_>>>()?
            .into_iter()
            .filter(|path| path.extension().is_some_and(|extension| extension == "wal"))
            .collect::<Vec<_>>();
        paths.sort();
        Ok(paths)
    }

    fn wal_paths(directory: &Path) -> Vec<PathBuf> {
        wal_paths_result(directory).unwrap()
    }

    fn header_bounds(path: &Path) -> (u64, u64, u64) {
        let mut file = File::open(path).unwrap();
        file.seek(SeekFrom::Start(WAL_FIRST_SEQUENCE_OFFSET))
            .unwrap();
        (
            file.read_u64::<LittleEndian>().unwrap(),
            file.read_u64::<LittleEndian>().unwrap(),
            file.read_u64::<LittleEndian>().unwrap(),
        )
    }

    fn stress_wal_paths(seed: u64, sequence: u64, directory: &Path) -> Vec<PathBuf> {
        let context = stress_context(seed, sequence, "unknown", directory.display());
        wal_paths_result(directory)
            .unwrap_or_else(|error| panic!("{context}: WAL directory scan failed: {error}"))
    }

    fn wal_case(seed: u64, sequence: u64, directory: &Path) -> String {
        let files = stress_wal_paths(seed, sequence, directory)
            .into_iter()
            .filter_map(|path| path.file_name().map(|name| name.to_owned()))
            .collect::<Vec<_>>();
        stress_context(
            seed,
            sequence,
            files.len().saturating_sub(1),
            format!("{files:?}"),
        )
    }

    async fn run_seeded_arbitrary_wal_model(seed: u64) {
        let directory = TempDir::new().unwrap();
        let config = WalConfig {
            max_file_size: WAL_HEADER_SIZE as u64 + 144,
            ..WalConfig::durable()
        };
        let mut rng = StdRng::seed_from_u64(seed);
        let boundary_lengths = [0, 1, 31, 32, 33, 127, 128, 255, 1_024, 4_096];
        let operations = (0..32)
            .map(|index| {
                let key_length = if index < boundary_lengths.len() {
                    boundary_lengths[index]
                } else {
                    rng.gen_range(0..=256)
                };
                let value_length = if index < boundary_lengths.len() {
                    boundary_lengths[boundary_lengths.len() - index - 1]
                } else {
                    rng.gen_range(0..=512)
                };
                let mut key = vec![0; key_length];
                let mut value = vec![0; value_length];
                rng.fill_bytes(&mut key);
                rng.fill_bytes(&mut value);
                (key, (rng.next_u32() % 4 != 0).then_some(value))
            })
            .collect::<Vec<_>>();

        let wal = WriteAheadLog::new(directory.path(), config.clone())
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "{}: open failed: {error}",
                    wal_case(seed, 0, directory.path())
                )
            });
        let mut expected = Vec::new();
        let mut index = 0;
        while index < operations.len() {
            let context = wal_case(seed, expected.len() as u64, directory.path());
            if index % 5 == 0 {
                let end = (index + 3).min(operations.len());
                let batch = operations[index..end]
                    .iter()
                    .map(|(key, value)| (key.as_slice(), value.as_deref()))
                    .collect::<Vec<_>>();
                let sequences = wal
                    .append_batch(&batch)
                    .await
                    .unwrap_or_else(|error| panic!("{context}: batch append failed: {error}"));
                let wanted = (expected.len() as u64..expected.len() as u64 + batch.len() as u64)
                    .collect::<Vec<_>>();
                assert_eq!(sequences, wanted, "{context}");
                for (sequence, (key, value)) in sequences.into_iter().zip(&operations[index..end]) {
                    expected.push((sequence, key.clone(), value.clone()));
                }
                index = end;
            } else {
                let (key, value) = &operations[index];
                let sequence = match value {
                    Some(value) => wal.append(key, value).await,
                    None => wal.append_delete(key).await,
                }
                .unwrap_or_else(|error| panic!("{context}: single append failed: {error}"));
                assert_eq!(sequence, expected.len() as u64, "{context}");
                expected.push((sequence, key.clone(), value.clone()));
                index += 1;
            }
        }
        wal.flush().await.unwrap_or_else(|error| {
            panic!(
                "{}: flush failed: {error}",
                wal_case(seed, expected.len() as u64, directory.path())
            )
        });
        assert!(
            stress_wal_paths(seed, expected.len() as u64, directory.path()).len() > 1,
            "{}",
            wal_case(seed, expected.len() as u64, directory.path())
        );
        let active_path = wal.current_file.read().path.clone();
        let tail_context = wal_case(seed, expected.len() as u64, directory.path());
        let valid_active_length = active_path
            .metadata()
            .unwrap_or_else(|error| {
                panic!(
                    "{tail_context} file={}: metadata failed: {error}",
                    active_path.display()
                )
            })
            .len();
        drop(wal);

        let mut active = OpenOptions::new()
            .append(true)
            .open(&active_path)
            .unwrap_or_else(|error| {
                panic!(
                    "{tail_context} file={}: tail open failed: {error}",
                    active_path.display()
                )
            });
        active.write_all(&[7, 0, 0]).unwrap_or_else(|error| {
            panic!(
                "{tail_context} file={}: tail write failed: {error}",
                active_path.display()
            )
        });
        drop(active);
        let repaired = WriteAheadLog::new(directory.path(), config.clone())
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "{}: tail repair failed: {error}",
                    wal_case(seed, expected.len() as u64, directory.path())
                )
            });
        assert_eq!(
            active_path
                .metadata()
                .unwrap_or_else(|error| {
                    panic!(
                        "{tail_context} file={}: repaired metadata failed: {error}",
                        active_path.display()
                    )
                })
                .len(),
            valid_active_length,
            "{}",
            wal_case(seed, expected.len() as u64, directory.path())
        );
        let replayed = repaired.read_from(0).await.unwrap_or_else(|error| {
            panic!(
                "{}: replay failed: {error}",
                wal_case(seed, expected.len() as u64, directory.path())
            )
        });
        assert_eq!(
            replayed.len(),
            expected.len(),
            "{}",
            wal_case(seed, expected.len() as u64, directory.path())
        );
        for (actual, (sequence, key, value)) in replayed.iter().zip(&expected) {
            let context = wal_case(seed, *sequence, directory.path());
            assert_eq!(actual.sequence, *sequence, "{context}");
            assert_eq!(actual.decode_key(), Some(key.as_slice()), "{context}");
            assert_eq!(actual.decode_value(), value.as_deref(), "{context}");
        }
        let marker_sequence = repaired
            .append(&[0, 0xff, 0], &[0xff, 0, 0xff])
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "{}: post-repair append failed: {error}",
                    wal_case(seed, expected.len() as u64, directory.path())
                )
            });
        assert_eq!(
            marker_sequence,
            expected.len() as u64,
            "{}",
            wal_case(seed, marker_sequence, directory.path())
        );
        repaired.flush().await.unwrap_or_else(|error| {
            panic!(
                "{}: post-repair flush failed: {error}",
                wal_case(seed, marker_sequence, directory.path())
            )
        });
        drop(repaired);

        let paths = stress_wal_paths(seed, marker_sequence, directory.path());
        let corruption_context = wal_case(seed, marker_sequence, directory.path());
        let corrupted_path = paths
            .first()
            .unwrap_or_else(|| panic!("{corruption_context}: no WAL file to corrupt"))
            .clone();
        let mut corrupted = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&corrupted_path)
            .unwrap_or_else(|error| {
                panic!(
                    "{corruption_context} file={}: corruption open failed: {error}",
                    corrupted_path.display()
                )
            });
        let offset = (WAL_HEADER_SIZE + ENTRY_HEADER_SIZE + 1) as u64;
        corrupted
            .seek(SeekFrom::Start(offset))
            .unwrap_or_else(|error| panic!("{corruption_context}: seek failed: {error}"));
        let byte = corrupted
            .read_u8()
            .unwrap_or_else(|error| panic!("{corruption_context}: read failed: {error}"));
        corrupted
            .seek(SeekFrom::Start(offset))
            .unwrap_or_else(|error| panic!("{corruption_context}: reseek failed: {error}"));
        corrupted
            .write_all(&[byte ^ 0x80])
            .unwrap_or_else(|error| panic!("{corruption_context}: corrupt failed: {error}"));
        corrupted
            .sync_all()
            .unwrap_or_else(|error| panic!("{corruption_context}: corrupt sync failed: {error}"));
        drop(corrupted);

        let error = match WriteAheadLog::new(directory.path(), config).await {
            Ok(_) => panic!(
                "{}: corrupted file {} reopened successfully",
                wal_case(seed, marker_sequence, directory.path()),
                corrupted_path.display()
            ),
            Err(error) => error,
        };
        assert!(
            matches!(error, WalError::CrcMismatch)
                || error.to_string().contains("CRC mismatch: data corrupted"),
            "{} file={}: unexpected checksum error: {error}",
            wal_case(seed, marker_sequence, directory.path()),
            corrupted_path.display()
        );
    }

    #[tokio::test]
    async fn seeded_arbitrary_bytes_sizes_rotation_repair_batch_and_checksum_model() {
        for seed in [
            0x01d7_7f9b_4ac3_e281,
            0x783c_92e1_05bf_a64d,
            0xfe10_4b68_d927_3ca5,
        ] {
            run_seeded_arbitrary_wal_model(seed).await;
        }
    }

    #[tokio::test]
    async fn test_wal_append_and_read() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            sync_on_write: true,
            ..Default::default()
        };

        let wal = WriteAheadLog::new(temp_dir.path(), config).await.unwrap();

        // Append some entries
        wal.append(b"key1", b"value1").await.unwrap();
        wal.append(b"key2", b"value2").await.unwrap();
        wal.append_delete(b"key1").await.unwrap();

        // Read back
        let entries = wal.read_from(0).await.unwrap();
        assert_eq!(entries.len(), 3);

        // Verify key-value decoding
        let (key, value) = entries[0].decode_kv().unwrap();
        assert_eq!(key, b"key1");
        assert_eq!(value, Some(b"value1".as_slice()));

        let (key, value) = entries[2].decode_kv().unwrap();
        assert_eq!(key, b"key1");
        assert_eq!(value, None); // Delete entry
    }

    #[tokio::test]
    async fn retained_size_includes_every_valid_segment_and_survives_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            max_file_size: WAL_HEADER_SIZE as u64 + 48,
            sync_on_write: true,
            ..Default::default()
        };
        let wal = WriteAheadLog::new(temp_dir.path(), config.clone())
            .await
            .unwrap();

        for key in 0..4 {
            wal.append(format!("key-{key}").as_bytes(), b"value")
                .await
                .unwrap();
        }
        wal.flush().await.unwrap();

        let paths = wal_paths(temp_dir.path());
        assert!(paths.len() > 1);
        let filesystem_bytes = paths
            .iter()
            .map(|path| std::fs::metadata(path).unwrap().len())
            .sum::<u64>();
        assert_eq!(wal.retained_size(), filesystem_bytes);
        assert!(wal.retained_size() > wal.current_size());
        let before_reclamation = wal.retained_size();
        wal.truncate(u64::MAX).await.unwrap();
        let after_reclamation = wal.retained_size();
        assert!(after_reclamation < before_reclamation);
        assert_eq!(after_reclamation, wal.current_size());
        assert_eq!(
            after_reclamation,
            wal_paths(temp_dir.path())
                .iter()
                .map(|path| std::fs::metadata(path).unwrap().len())
                .sum::<u64>()
        );
        drop(wal);

        let reopened = WriteAheadLog::new(temp_dir.path(), config).await.unwrap();
        assert_eq!(reopened.retained_size(), after_reclamation);
    }

    #[tokio::test]
    async fn test_wal_batch() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            sync_on_write: true,
            ..Default::default()
        };

        let wal = WriteAheadLog::new(temp_dir.path(), config).await.unwrap();

        let batch: Vec<(&[u8], Option<&[u8]>)> = vec![
            (b"key1", Some(b"value1")),
            (b"key2", Some(b"value2")),
            (b"key3", None), // Delete
        ];

        let sequences = wal.append_batch(&batch).await.unwrap();
        assert_eq!(sequences.len(), 3);

        let entries = wal.read_from(0).await.unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.sequence)
                .collect::<Vec<_>>(),
            sequences
        );
        assert_eq!(wal.align_checkpoint(sequences[1]), sequences[0]);
        assert_eq!(wal.align_checkpoint(sequences[2] + 1), sequences[2] + 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn queued_group_commit_single_and_atomic_batch_follow_sequence_order() {
        let directory = TempDir::new().unwrap();
        let wal = Arc::new(
            WriteAheadLog::new(directory.path(), WalConfig::paranoid())
                .await
                .unwrap(),
        );

        let (locked_tx, locked_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let lock_wal = Arc::clone(&wal);
        let lock_holder = tokio::task::spawn_blocking(move || {
            let file_guard = lock_wal.current_file.write();
            let _ = locked_tx.send(());
            release_rx.recv().unwrap();
            drop(file_guard);
        });
        locked_rx.await.unwrap();
        let queued_wal = Arc::clone(&wal);
        let queued = tokio::spawn(async move { queued_wal.append(b"key", b"old").await });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while wal.group_commit_in_progress_for_test() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first commit group did not reach the WAL lock");

        let batch_wal = Arc::clone(&wal);
        let batch = tokio::spawn(async move {
            batch_wal
                .append_batch(&[
                    (b"key".as_slice(), Some(b"new".as_slice())),
                    (b"key".as_slice(), None),
                ])
                .await
        });
        release_tx.send(()).unwrap();
        lock_holder.await.unwrap();

        assert_eq!(queued.await.unwrap().unwrap(), 0);
        assert_eq!(batch.await.unwrap().unwrap(), [1, 2]);
        assert_eq!(
            wal.read_from(0)
                .await
                .unwrap()
                .iter()
                .map(|entry| entry.sequence)
                .collect::<Vec<_>>(),
            [0, 1, 2]
        );
    }

    #[tokio::test]
    async fn zero_group_size_is_rejected_instead_of_hanging() {
        let directory = TempDir::new().unwrap();
        let error = match WriteAheadLog::new(
            directory.path(),
            WalConfig::paranoid().with_max_group_size(0),
        )
        .await
        {
            Ok(_) => panic!("zero group size unexpectedly opened a WAL"),
            Err(error) => error,
        };
        assert!(matches!(error, WalError::InvalidFormat(_)));
        assert!(error.to_string().contains("greater than zero"));
    }

    #[tokio::test]
    async fn excessive_collection_window_is_rejected_instead_of_overflowing_deadline() {
        let directory = TempDir::new().unwrap();
        let config = WalConfig {
            group_commit_delay_us: MAX_GROUP_COMMIT_DELAY_US + 1,
            ..WalConfig::paranoid()
        };
        let error = match WriteAheadLog::new(directory.path(), config).await {
            Ok(_) => panic!("excessive collection window unexpectedly opened a WAL"),
            Err(error) => error,
        };
        assert!(matches!(error, WalError::InvalidFormat(_)));
        assert!(error.to_string().contains("must not exceed"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_paranoid_callers_share_one_bounded_durability_barrier() {
        const CALLERS: usize = 16;
        let directory = TempDir::new().unwrap();
        let config = WalConfig::paranoid()
            .with_group_commit_delay(std::time::Duration::from_millis(50))
            .with_max_group_size(CALLERS);
        let wal = Arc::new(WriteAheadLog::new(directory.path(), config).await.unwrap());
        let barrier = Arc::new(tokio::sync::Barrier::new(CALLERS + 1));
        let mut callers = Vec::with_capacity(CALLERS);
        for index in 0..CALLERS {
            let caller_wal = Arc::clone(&wal);
            let caller_barrier = Arc::clone(&barrier);
            callers.push(tokio::spawn(async move {
                caller_barrier.wait().await;
                caller_wal
                    .append(format!("key-{index}").as_bytes(), b"value")
                    .await
            }));
        }
        barrier.wait().await;

        let mut sequences = Vec::with_capacity(CALLERS);
        for caller in callers {
            sequences.push(caller.await.unwrap().unwrap());
        }
        sequences.sort_unstable();
        assert_eq!(sequences, (0..CALLERS as u64).collect::<Vec<_>>());
        assert_eq!(wal.group_commit_syncs_for_test(), 1);
        assert_eq!(wal.largest_group_commit_for_test(), CALLERS as u64);
        assert_eq!(wal.durable_sequence(), CALLERS as u64);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn maximum_group_size_counts_callers_and_zero_delay_is_supported() {
        const CALLERS: usize = 10;
        const MAXIMUM: usize = 3;
        let directory = TempDir::new().unwrap();
        let config = WalConfig::paranoid()
            .with_group_commit_delay(std::time::Duration::ZERO)
            .with_max_group_size(MAXIMUM);
        let wal = Arc::new(WriteAheadLog::new(directory.path(), config).await.unwrap());
        let barrier = Arc::new(tokio::sync::Barrier::new(CALLERS + 1));
        let mut callers = Vec::with_capacity(CALLERS);
        for index in 0..CALLERS {
            let caller_wal = Arc::clone(&wal);
            let caller_barrier = Arc::clone(&barrier);
            callers.push(tokio::spawn(async move {
                caller_barrier.wait().await;
                caller_wal
                    .append(format!("zero-delay-{index}").as_bytes(), b"value")
                    .await
            }));
        }
        barrier.wait().await;
        for caller in callers {
            caller.await.unwrap().unwrap();
        }

        assert!(wal.group_commit_syncs_for_test() >= CALLERS.div_ceil(MAXIMUM) as u64);
        assert!(wal.largest_group_commit_for_test() <= MAXIMUM as u64);
        assert_eq!(wal.durable_sequence(), CALLERS as u64);
    }

    #[tokio::test]
    async fn one_large_atomic_batch_is_one_group_member_and_one_wal_envelope() {
        const OPERATIONS: usize = 4_096;
        let directory = TempDir::new().unwrap();
        let config = WalConfig {
            max_file_size: WAL_HEADER_SIZE as u64 + 128,
            ..WalConfig::paranoid().with_max_group_size(1)
        };
        let wal = WriteAheadLog::new(directory.path(), config).await.unwrap();
        let keys = (0..OPERATIONS)
            .map(|index| format!("large-batch-{index:04}").into_bytes())
            .collect::<Vec<_>>();
        let entries = keys
            .iter()
            .map(|key| (key.as_slice(), Some(b"value".as_slice())))
            .collect::<Vec<_>>();

        let sequences = wal.append_batch(&entries).await.unwrap();
        assert_eq!(sequences, (0..OPERATIONS as u64).collect::<Vec<_>>());
        assert_eq!(wal.group_commit_syncs_for_test(), 1);
        assert_eq!(wal.largest_group_commit_for_test(), 1);

        let path = wal.current_file.read().path.clone();
        let metadata = inspect_segment(&path, false).unwrap();
        assert_eq!(metadata.entry_count, OPERATIONS as u64);
        assert_eq!(metadata.batch_ranges, [(0, OPERATIONS as u64 - 1)]);
        assert_eq!(wal_paths(directory.path()).len(), 1);
        assert!(metadata.valid_end > WAL_HEADER_SIZE as u64 + 128);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn commit_group_splits_small_records_across_bounded_segments() {
        const CALLERS: usize = 5;
        const MAX_FILE_SIZE: u64 = WAL_HEADER_SIZE as u64 + 72;
        let directory = TempDir::new().unwrap();
        let config = WalConfig {
            max_file_size: MAX_FILE_SIZE,
            ..WalConfig::paranoid()
                .with_group_commit_delay(std::time::Duration::from_millis(50))
                .with_max_group_size(CALLERS)
        };
        let wal = Arc::new(WriteAheadLog::new(directory.path(), config).await.unwrap());
        let barrier = Arc::new(tokio::sync::Barrier::new(CALLERS + 1));
        let mut callers = Vec::with_capacity(CALLERS);
        for index in 0..CALLERS {
            let caller_wal = Arc::clone(&wal);
            let caller_barrier = Arc::clone(&barrier);
            callers.push(tokio::spawn(async move {
                caller_barrier.wait().await;
                caller_wal
                    .append(format!("split-{index}").as_bytes(), b"0123456789abcdef")
                    .await
            }));
        }
        barrier.wait().await;
        for caller in callers {
            caller.await.unwrap().unwrap();
        }

        assert_eq!(wal.group_commit_syncs_for_test(), 1);
        assert_eq!(wal.largest_group_commit_for_test(), CALLERS as u64);
        let paths = wal_paths(directory.path());
        assert_eq!(paths.len(), CALLERS);
        for path in paths {
            assert!(inspect_segment(&path, false).unwrap().valid_end <= MAX_FILE_SIZE);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn paranoid_caller_waits_until_its_group_is_durable() {
        let directory = TempDir::new().unwrap();
        let wal = Arc::new(
            WriteAheadLog::new(directory.path(), WalConfig::paranoid())
                .await
                .unwrap(),
        );
        let (locked_tx, locked_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let lock_wal = Arc::clone(&wal);
        let lock_holder = tokio::task::spawn_blocking(move || {
            let file_guard = lock_wal.lock_current_file_for_test();
            let _ = locked_tx.send(());
            release_rx.recv().unwrap();
            drop(file_guard);
        });
        locked_rx.await.unwrap();

        let caller_wal = Arc::clone(&wal);
        let caller = tokio::spawn(async move { caller_wal.append(b"key", b"value").await });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while wal.group_commit_in_progress_for_test() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("commit group did not reach the WAL lock");
        assert!(!caller.is_finished());
        assert_eq!(wal.durable_sequence(), 0);

        release_tx.send(()).unwrap();
        lock_holder.await.unwrap();
        assert_eq!(caller.await.unwrap().unwrap(), 0);
        assert_eq!(wal.durable_sequence(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn data_sync_failure_fans_out_drains_queue_and_prevents_later_writes() {
        const CALLERS: usize = 12;
        const MAXIMUM: usize = 4;
        let directory = TempDir::new().unwrap();
        let config = WalConfig::paranoid()
            .with_group_commit_delay(std::time::Duration::from_millis(200))
            .with_max_group_size(MAXIMUM);
        let wal = Arc::new(
            WriteAheadLog::new(directory.path(), config.clone())
                .await
                .unwrap(),
        );
        let failure = super::super::failpoints::arm(
            directory.path(),
            super::super::failpoints::PersistenceBoundary::WalDataSync,
        );
        let barrier = Arc::new(tokio::sync::Barrier::new(CALLERS + 1));
        let mut callers = Vec::with_capacity(CALLERS);
        for index in 0..CALLERS {
            let caller_wal = Arc::clone(&wal);
            let caller_barrier = Arc::clone(&barrier);
            callers.push(tokio::spawn(async move {
                caller_barrier.wait().await;
                caller_wal
                    .append(format!("failed-{index}").as_bytes(), b"value")
                    .await
            }));
        }
        barrier.wait().await;

        let mut messages = Vec::with_capacity(CALLERS);
        for caller in callers {
            messages.push(caller.await.unwrap().unwrap_err().to_string());
        }
        failure.assert_hit();
        assert!(messages.windows(2).all(|pair| pair[0] == pair[1]));
        assert!(messages[0].contains("WAL data sync"));
        assert_eq!(wal.group_commit_syncs_for_test(), 1);
        assert_eq!(wal.largest_group_commit_for_test(), MAXIMUM as u64);
        assert_eq!(wal.current_sequence(), MAXIMUM as u64);
        assert_eq!(wal.durable_sequence(), 0);

        let later = wal.append(b"later", b"not-written").await.unwrap_err();
        assert_eq!(later.to_string(), messages[0]);
        assert_eq!(wal.flush().await.unwrap_err().to_string(), messages[0]);
        assert_eq!(wal.current_sequence(), MAXIMUM as u64);
        assert_eq!(wal.durable_sequence(), 0);
        assert_eq!(wal.group_commit_syncs_for_test(), 1);

        drop(wal);
        let reopened = WriteAheadLog::new(directory.path(), config).await.unwrap();
        assert_eq!(reopened.durable_sequence(), MAXIMUM as u64);
        assert_eq!(reopened.read_from(0).await.unwrap().len(), MAXIMUM);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn later_segment_failure_keeps_the_whole_group_frontier_indeterminate() {
        const CALLERS: usize = 3;
        const MAX_FILE_SIZE: u64 = WAL_HEADER_SIZE as u64 + 72;
        let directory = TempDir::new().unwrap();
        let config = WalConfig {
            max_file_size: MAX_FILE_SIZE,
            ..WalConfig::paranoid()
                .with_group_commit_delay(std::time::Duration::from_millis(50))
                .with_max_group_size(CALLERS)
        };
        let wal = Arc::new(
            WriteAheadLog::new(directory.path(), config.clone())
                .await
                .unwrap(),
        );
        assert_eq!(wal.append(b"prior", b"durable").await.unwrap(), 0);
        assert_eq!(wal.durable_sequence(), 1);

        let failure = super::super::failpoints::arm(
            directory.path(),
            super::super::failpoints::PersistenceBoundary::WalDataSync,
        );
        let barrier = Arc::new(tokio::sync::Barrier::new(CALLERS + 1));
        let mut callers = Vec::with_capacity(CALLERS);
        for index in 0..CALLERS {
            let caller_wal = Arc::clone(&wal);
            let caller_barrier = Arc::clone(&barrier);
            callers.push(tokio::spawn(async move {
                caller_barrier.wait().await;
                caller_wal
                    .append(format!("failed-segment-{index}").as_bytes(), b"value")
                    .await
            }));
        }
        barrier.wait().await;

        let mut messages = Vec::with_capacity(CALLERS);
        for caller in callers {
            messages.push(caller.await.unwrap().unwrap_err().to_string());
        }
        failure.assert_hit();
        assert!(messages.windows(2).all(|pair| pair[0] == pair[1]));
        assert_eq!(wal.group_commit_syncs_for_test(), 2);
        assert_eq!(wal.largest_group_commit_for_test(), CALLERS as u64);
        assert_eq!(wal.current_sequence(), 1 + CALLERS as u64);
        assert_eq!(wal.durable_sequence(), 1);
        assert_eq!(wal_paths(directory.path()).len(), 1 + CALLERS);

        drop(wal);
        let reopened = WriteAheadLog::new(directory.path(), config).await.unwrap();
        assert_eq!(reopened.durable_sequence(), 1 + CALLERS as u64);
        assert_eq!(reopened.read_from(0).await.unwrap().len(), 1 + CALLERS);
    }

    #[tokio::test]
    async fn rotation_directory_sync_failure_does_not_advance_durable_sequence() {
        let directory = TempDir::new().unwrap();
        let config = WalConfig {
            max_file_size: WAL_HEADER_SIZE as u64 + 40,
            ..WalConfig::paranoid()
        };
        let wal = WriteAheadLog::new(directory.path(), config).await.unwrap();
        assert_eq!(wal.append(b"first", b"value").await.unwrap(), 0);
        assert_eq!(wal.durable_sequence(), 1);
        let failure = super::super::failpoints::arm(
            directory.path(),
            super::super::failpoints::PersistenceBoundary::WalDirectorySync,
        );

        let error = wal.append(b"second", b"value").await.unwrap_err();
        failure.assert_hit();
        assert!(error.to_string().contains("WAL directory sync"));
        assert_eq!(wal.durable_sequence(), 1);
        assert_eq!(wal_paths(directory.path()).len(), 2);
        assert_eq!(
            wal.append(b"later", b"value")
                .await
                .unwrap_err()
                .to_string(),
            error.to_string()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancelled_group_commit_counts_bytes_at_physical_append_and_resets_on_reopen() {
        let directory = TempDir::new().unwrap();
        let config = WalConfig::paranoid();
        let wal = Arc::new(
            WriteAheadLog::new(directory.path(), config.clone())
                .await
                .unwrap(),
        );
        let retained_before = wal.retained_size();
        assert_eq!(wal.bytes_written_since_open(), 0);

        let (locked_tx, locked_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let lock_wal = Arc::clone(&wal);
        let lock_holder = tokio::task::spawn_blocking(move || {
            let file_guard = lock_wal.lock_current_file_for_test();
            let _ = locked_tx.send(());
            release_rx.recv().unwrap();
            drop(file_guard);
        });
        locked_rx.await.unwrap();

        let queued_wal = Arc::clone(&wal);
        let queued = tokio::spawn(async move {
            queued_wal
                .append(b"cancelled:key", b"persisted-value")
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
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
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while wal.group_commit_in_progress_for_test() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("queued group commit did not finish");

        let written = wal.bytes_written_since_open();
        assert!(written > 0);
        assert_eq!(wal.retained_size() - retained_before, written);
        assert_eq!(wal.read_from(0).await.unwrap().len(), 1);
        let retained_after = wal.retained_size();
        drop(wal);

        let reopened = WriteAheadLog::new(directory.path(), config).await.unwrap();
        assert_eq!(reopened.retained_size(), retained_after);
        assert_eq!(reopened.bytes_written_since_open(), 0);
    }

    #[tokio::test]
    async fn recovery_and_retained_size_ignore_noncanonical_wal_paths() {
        let directory = TempDir::new().unwrap();
        let aliases = [
            "1.wal".to_string(),
            format!("{:019}.wal", 1),
            format!("{:021}.wal", 1),
            "0000000000000000000000001.wal".to_string(),
            "unrecognized.wal".to_string(),
            format!("{:020}.tmp", 999),
        ];
        for (index, alias) in aliases.iter().enumerate() {
            std::fs::write(directory.path().join(alias), vec![0_u8; index + 1]).unwrap();
        }
        std::fs::create_dir(directory.path().join("00000000000000000998.wal")).unwrap();

        let wal = WriteAheadLog::new(directory.path(), WalConfig::durable())
            .await
            .unwrap();

        assert_eq!(wal.retained_size(), wal.current_size());
        assert_eq!(
            wal_sequence_from_path(&wal.current_file.read().path),
            Some(0)
        );
        let retained = wal.retained_size();
        drop(wal);

        let reopened = WriteAheadLog::new(directory.path(), WalConfig::durable())
            .await
            .unwrap();
        assert_eq!(reopened.retained_size(), retained);
        for alias in aliases {
            assert!(directory.path().join(alias).is_file());
        }
        assert!(directory.path().join("00000000000000000998.wal").is_dir());
    }

    #[tokio::test]
    async fn reclaimed_batch_ranges_do_not_accumulate_in_memory() {
        let directory = TempDir::new().unwrap();
        let wal = WriteAheadLog::new(directory.path(), WalConfig::durable())
            .await
            .unwrap();
        let sequences = wal
            .append_batch(&[(b"key".as_slice(), Some(b"value".as_slice()))])
            .await
            .unwrap();
        assert_eq!(wal.batch_ranges.read().len(), 1);

        wal.truncate(sequences[0] + 1).await.unwrap();
        assert!(wal.batch_ranges.read().is_empty());
    }

    #[tokio::test]
    async fn repaired_tail_accepts_reachable_appends_across_a_second_recovery() {
        let directory = TempDir::new().unwrap();
        let config = WalConfig::paranoid();
        let wal = WriteAheadLog::new(directory.path(), config.clone())
            .await
            .unwrap();
        assert_eq!(wal.append(b"before", b"repair").await.unwrap(), 0);
        wal.flush().await.unwrap();
        let path = wal.current_file.read().path.clone();
        let valid_end = wal.current_size();
        assert_eq!(wal.retained_size(), valid_end);
        {
            let mut current = wal.lock_current_file_for_test();
            current.file.seek(SeekFrom::End(0)).unwrap();
            current.file.write_all(&[8, 0, 0, 0, 1, 2, 3]).unwrap();
            current.file.sync_all().unwrap();
        }
        assert_eq!(wal.retained_size(), valid_end);
        assert_eq!(path.metadata().unwrap().len(), valid_end + 7);
        drop(wal);

        let repaired = WriteAheadLog::new(directory.path(), config.clone())
            .await
            .unwrap();
        assert_eq!(repaired.current_size(), valid_end);
        assert_eq!(repaired.retained_size(), valid_end);
        assert_eq!(path.metadata().unwrap().len(), valid_end);
        assert_eq!(repaired.current_sequence(), 1);
        assert_eq!(repaired.append(b"after", b"repair").await.unwrap(), 1);
        repaired.flush().await.unwrap();
        assert_eq!(header_bounds(&path), (0, 1, 2));
        drop(repaired);

        let reopened = WriteAheadLog::new(directory.path(), config).await.unwrap();
        let entries = reopened.read_from(0).await.unwrap();
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.sequence)
                .collect::<Vec<_>>(),
            [0, 1]
        );
        assert_eq!(reopened.append(b"second", b"reopen").await.unwrap(), 2);
    }

    #[tokio::test]
    async fn checkpoint_reclamation_retains_a_segment_spanning_the_frontier() {
        let directory = TempDir::new().unwrap();
        let config = WalConfig {
            max_file_size: 140,
            sync_on_write: true,
            ..WalConfig::default()
        };
        let wal = WriteAheadLog::new(directory.path(), config).await.unwrap();
        for key in [b"a", b"b", b"c"] {
            wal.append(key, b"v").await.unwrap();
        }
        wal.flush().await.unwrap();
        assert_eq!(wal_paths(directory.path()).len(), 2);

        wal.truncate(1).await.unwrap();
        assert_eq!(wal_paths(directory.path()).len(), 2);

        wal.truncate(2).await.unwrap();
        assert_eq!(wal_paths(directory.path()).len(), 1);
        assert_eq!(wal.read_from(0).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn reclamation_never_deletes_the_active_segment() {
        let directory = TempDir::new().unwrap();
        let config = WalConfig {
            max_file_size: 110,
            sync_on_write: true,
            ..WalConfig::default()
        };
        let wal = WriteAheadLog::new(directory.path(), config).await.unwrap();
        wal.append(b"a", b"v").await.unwrap();
        wal.append(b"b", b"v").await.unwrap();
        let active_path = wal.current_file.read().path.clone();
        assert_eq!(wal_paths(directory.path()).len(), 2);

        wal.truncate(u64::MAX).await.unwrap();
        assert!(active_path.exists());
        assert_eq!(wal_paths(directory.path()), [active_path]);
        assert_eq!(wal.read_from(0).await.unwrap()[0].sequence, 1);
    }

    #[tokio::test]
    async fn concurrent_rotation_and_reclamation_keep_later_appends_reachable() {
        let directory = TempDir::new().unwrap();
        let config = WalConfig {
            max_file_size: 110,
            sync_on_write: false,
            ..WalConfig::default()
        };
        let wal = Arc::new(
            WriteAheadLog::new(directory.path(), config.clone())
                .await
                .unwrap(),
        );
        let writer_wal = Arc::clone(&wal);
        let writer = tokio::spawn(async move {
            for index in 0_u64..100 {
                writer_wal
                    .append(&index.to_le_bytes(), b"value")
                    .await
                    .unwrap();
                tokio::task::yield_now().await;
            }
        });
        let reclaim_wal = Arc::clone(&wal);
        let reclaimer = tokio::spawn(async move {
            for _ in 0..100 {
                reclaim_wal.truncate(u64::MAX).await.unwrap();
                tokio::task::yield_now().await;
            }
        });
        writer.await.unwrap();
        reclaimer.await.unwrap();

        let marker_sequence = wal.append(b"marker", b"reachable").await.unwrap();
        wal.flush().await.unwrap();
        let active_path = wal.current_file.read().path.clone();
        assert!(active_path.exists());
        drop(wal);

        let reopened = WriteAheadLog::new(directory.path(), config).await.unwrap();
        let marker = reopened.read_from(marker_sequence).await.unwrap();
        assert_eq!(marker.len(), 1);
        assert_eq!(marker[0].decode_key(), Some(b"marker".as_slice()));
    }

    #[tokio::test]
    async fn reclamation_uses_validated_record_bounds_not_the_filename() {
        let directory = TempDir::new().unwrap();
        let config = WalConfig::paranoid();
        let mut rotated = create_file(directory.path(), 0, &config).unwrap();
        write_entry(
            &mut rotated.file,
            &WalEntry {
                sequence: 10,
                timestamp: 0,
                entry_type: EntryType::Data,
                data: Bytes::from(encode_kv(b"key", b"value")),
            },
        )
        .unwrap();
        let rotated_path = rotated.path.clone();
        drop(rotated);
        drop(create_file(directory.path(), 11, &config).unwrap());

        let wal = WriteAheadLog::new(directory.path(), config).await.unwrap();
        assert_eq!(wal.current_sequence(), 11);
        assert_eq!(header_bounds(&rotated_path), (10, 10, 1));
        wal.truncate(5).await.unwrap();
        assert!(rotated_path.exists());
        wal.truncate(11).await.unwrap();
        assert!(!rotated_path.exists());
    }

    #[tokio::test]
    async fn empty_current_segment_recovers_the_exact_next_sequence() {
        let directory = TempDir::new().unwrap();
        let config = WalConfig::paranoid();
        let mut rotated = create_file(directory.path(), 0, &config).unwrap();
        write_entry(
            &mut rotated.file,
            &WalEntry {
                sequence: 10,
                timestamp: 0,
                entry_type: EntryType::Delete,
                data: Bytes::from(encode_delete(b"key")),
            },
        )
        .unwrap();
        drop(rotated);
        let mut active = create_file(directory.path(), 8, &config).unwrap();
        let active_path = active.path.clone();
        active
            .file
            .seek(SeekFrom::Start(WAL_FIRST_SEQUENCE_OFFSET))
            .unwrap();
        active.file.write_u64::<LittleEndian>(999).unwrap();
        active.file.write_u64::<LittleEndian>(999).unwrap();
        active.file.write_u64::<LittleEndian>(999).unwrap();
        drop(active);

        let wal = WriteAheadLog::new(directory.path(), config).await.unwrap();
        assert_eq!(wal.current_sequence(), 11);
        assert_eq!(header_bounds(&active_path), (11, 11, 0));
        assert_eq!(wal.append(b"next", b"value").await.unwrap(), 11);
        wal.flush().await.unwrap();
        assert_eq!(header_bounds(&active_path), (11, 11, 1));
    }

    #[tokio::test]
    async fn rotation_keeps_filenames_forward_when_record_sequences_are_lower() {
        let directory = TempDir::new().unwrap();
        let config = WalConfig {
            max_file_size: 108,
            sync_on_write: true,
            ..WalConfig::default()
        };
        let mut first = create_file(directory.path(), 10, &config).unwrap();
        write_entry(
            &mut first.file,
            &WalEntry {
                sequence: 5,
                timestamp: 0,
                entry_type: EntryType::Data,
                data: Bytes::from(encode_kv(b"old", b"value")),
            },
        )
        .unwrap();
        drop(first);

        let wal = WriteAheadLog::new(directory.path(), config.clone())
            .await
            .unwrap();
        assert_eq!(wal.current_sequence(), 6);
        assert_eq!(wal.append(b"new", b"value").await.unwrap(), 6);
        wal.flush().await.unwrap();
        let active_path = wal.current_file.read().path.clone();
        assert_eq!(active_path.file_stem().unwrap(), "00000000000000000011");
        let valid_end = active_path.metadata().unwrap().len();
        drop(wal);

        OpenOptions::new()
            .append(true)
            .open(&active_path)
            .unwrap()
            .write_all(&[1, 2])
            .unwrap();
        let reopened = WriteAheadLog::new(directory.path(), config).await.unwrap();
        assert_eq!(active_path.metadata().unwrap().len(), valid_end);
        assert_eq!(
            reopened
                .read_from(0)
                .await
                .unwrap()
                .iter()
                .map(|entry| entry.sequence)
                .collect::<Vec<_>>(),
            [5, 6]
        );
    }

    #[tokio::test]
    async fn oversized_first_sync_entry_does_not_create_phantom_bounds() {
        let directory = TempDir::new().unwrap();
        let config = WalConfig {
            max_file_size: WAL_HEADER_SIZE as u64,
            sync_on_write: true,
            ..WalConfig::default()
        };
        let wal = WriteAheadLog::new(directory.path(), config).await.unwrap();
        assert_eq!(wal.append(b"oversized", b"value").await.unwrap(), 0);
        wal.flush().await.unwrap();
        let paths = wal_paths(directory.path());
        assert_eq!(paths.len(), 1);
        assert_eq!(header_bounds(&paths[0]), (0, 0, 1));
    }

    #[test]
    fn iterator_is_fused_after_reporting_corruption() {
        let directory = TempDir::new().unwrap();
        let config = WalConfig::durable();
        let mut wal_file = create_file(directory.path(), 0, &config).unwrap();
        let entry = WalEntry {
            sequence: 0,
            timestamp: 0,
            entry_type: EntryType::Data,
            data: Bytes::from(encode_kv(b"key", b"value")),
        };
        write_entry(&mut wal_file.file, &entry).unwrap();
        wal_file.file.write_all(&[1, 0, 0, 0, 1]).unwrap();
        let path = wal_file.path.clone();
        drop(wal_file);

        let mut iterator = WalEntryIterator::new(vec![path], 0).unwrap();
        assert_eq!(iterator.next().unwrap().unwrap().sequence, 0);
        assert!(iterator.next().unwrap().is_err());
        assert!(iterator.next().is_none());
    }

    #[tokio::test]
    async fn read_helpers_reject_a_header_corrupted_after_open() {
        let directory = TempDir::new().unwrap();
        let wal = WriteAheadLog::new(directory.path(), WalConfig::paranoid())
            .await
            .unwrap();
        wal.append(b"key", b"value").await.unwrap();
        wal.flush().await.unwrap();
        let path = wal.current_file.read().path.clone();

        let mut file = OpenOptions::new().write(true).open(path).unwrap();
        file.write_all(b"BADMAGIC").unwrap();
        drop(file);

        assert!(matches!(
            wal.read_from(0).await,
            Err(WalError::InvalidFormat(_))
        ));
        assert!(matches!(
            wal.iter_entries().await,
            Err(WalError::InvalidFormat(_))
        ));
    }

    #[tokio::test]
    async fn read_helpers_report_torn_and_oversized_lengths_without_scanning_past_them() {
        for tail in [&[1_u8, 2][..], &u32::MAX.to_le_bytes()[..]] {
            let directory = TempDir::new().unwrap();
            let wal = WriteAheadLog::new(directory.path(), WalConfig::paranoid())
                .await
                .unwrap();
            wal.append(b"key", b"value").await.unwrap();
            wal.flush().await.unwrap();
            let path = wal.current_file.read().path.clone();
            OpenOptions::new()
                .append(true)
                .open(path)
                .unwrap()
                .write_all(tail)
                .unwrap();

            assert!(wal.read_from(0).await.is_err());
            let mut iterator = wal.iter_entries().await.unwrap();
            assert_eq!(iterator.next().unwrap().unwrap().sequence, 0);
            assert!(iterator.next().unwrap().is_err());
            assert!(iterator.next().is_none());
        }
    }

    #[tokio::test]
    async fn legacy_active_segment_rotates_to_v4_before_append() {
        let directory = TempDir::new().unwrap();
        let config = WalConfig::paranoid();
        let mut legacy = create_file(directory.path(), 0, &config).unwrap();
        legacy.file.seek(SeekFrom::Start(8)).unwrap();
        legacy.file.write_u32::<LittleEndian>(2).unwrap();
        legacy
            .file
            .seek(SeekFrom::Start(WAL_HEADER_SIZE as u64))
            .unwrap();
        let payload = encode_kv(b"legacy", b"value");
        legacy
            .file
            .write_u32::<LittleEndian>(payload.len() as u32)
            .unwrap();
        legacy.file.write_u64::<LittleEndian>(0).unwrap();
        legacy.file.write_u64::<LittleEndian>(0).unwrap();
        legacy.file.write_u8(EntryType::Data as u8).unwrap();
        legacy.file.write_u8(0).unwrap();
        legacy
            .file
            .write_u32::<LittleEndian>(crc32_checksum(&payload))
            .unwrap();
        legacy.file.write_all(&[0_u8; ENTRY_RESERVED_SIZE]).unwrap();
        legacy
            .file
            .write_all(&[0_u8; LEGACY_ENTRY_EXTENSION_SIZE])
            .unwrap();
        legacy.file.write_all(&payload).unwrap();
        drop(legacy);

        let wal = WriteAheadLog::new(directory.path(), config).await.unwrap();
        assert_eq!(wal_paths(directory.path()).len(), 2);
        assert_eq!(wal.current_sequence(), 1);
        assert_eq!(wal.append(b"current", b"value").await.unwrap(), 1);
        let entries = wal.read_from(0).await.unwrap();
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.sequence)
                .collect::<Vec<_>>(),
            [0, 1]
        );
    }
}
