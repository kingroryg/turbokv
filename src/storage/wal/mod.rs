//! # Write-Ahead Log (WAL) for TurboKV
//!
//! ## Architecture
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    Write Path (Group Commit)                    │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  Writer 1 ──┐                                                   │
//! │  Writer 2 ──┼──► Channel ──► Background Task ──► Batch fsync    │
//! │  Writer 3 ──┘                                                   │
//! │                                                                 │
//! │  append_batch() ──────────► Direct Write (bypasses group commit)│
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
pub use types::{encode_delete, encode_kv, EntryType, Result, WalConfig, WalEntry, WalError};

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
use crate::storage::{directory_lock::DirectoryLock, InProgressGuard};
use file::{
    create_file, entry_size, finalize_header, inspect_segment, open_recovered_file,
    read_and_validate_header, read_entry_versioned, synchronize_segment_header,
    wal_sequence_from_path, write_entries_batch, write_entry, WalFile,
};
use types::{encode_batch, ENTRY_HEADER_SIZE, ENTRY_RESERVED_SIZE, WAL_HEADER_SIZE};

// Thread-local buffer for zero-allocation WAL writes
// Pre-allocated to avoid per-write heap allocations
thread_local! {
    static WAL_ENCODE_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(4096));
}

struct WriteRequest {
    entry: WalEntry,
    response: oneshot::Sender<Result<()>>,
}

pub(crate) struct BatchAppend {
    pub sequences: Vec<u64>,
    pub bytes_written: u64,
}

struct EncodedBatchRecord {
    sequences: Vec<u64>,
    encoded: Vec<u8>,
    first_sequence: u64,
    last_sequence: u64,
}

pub struct WriteAheadLog {
    wal_dir: PathBuf,
    config: WalConfig,
    current_file: Arc<RwLock<WalFile>>,
    sequence: Arc<AtomicU64>,
    /// Atomic batch spans used to keep durable checkpoints on batch boundaries.
    batch_ranges: Arc<RwLock<BTreeMap<u64, u64>>>,
    write_tx: mpsc::Sender<WriteRequest>,
    #[allow(dead_code)]
    group_commit_in_progress: Arc<AtomicU64>,
}

impl WriteAheadLog {
    pub async fn new(wal_dir: impl AsRef<Path>, config: WalConfig) -> Result<Self> {
        Self::new_inner(wal_dir, config, None).await
    }

    pub(crate) async fn new_with_directory_lock(
        wal_dir: impl AsRef<Path>,
        config: WalConfig,
        directory_lock: Weak<DirectoryLock>,
    ) -> Result<Self> {
        Self::new_inner(wal_dir, config, Some(directory_lock)).await
    }

    async fn new_inner(
        wal_dir: impl AsRef<Path>,
        config: WalConfig,
        directory_lock: Option<Weak<DirectoryLock>>,
    ) -> Result<Self> {
        let wal_dir = wal_dir.as_ref().to_path_buf();
        tokio::fs::create_dir_all(&wal_dir)
            .await
            .map_err(|e| WalError::Io {
                message: format!("Failed to create WAL directory: {:?}", wal_dir),
                source: Some(e),
            })?;

        let (wal_file, sequence, batch_ranges) = Self::open_or_create(&wal_dir, &config).await?;
        let current_file = Arc::new(RwLock::new(wal_file));
        let (write_tx, write_rx) = mpsc::channel::<WriteRequest>(config.max_batch_size * 2);

        // Spawn background group commit loop
        let bg_file = Arc::clone(&current_file);
        let bg_config = config.clone();
        let bg_dir = wal_dir.clone();
        let group_commit_in_progress = Arc::new(AtomicU64::new(0));
        let bg_group_commit_in_progress = Arc::clone(&group_commit_in_progress);
        tokio::spawn(async move {
            Self::group_commit_loop(
                write_rx,
                bg_file,
                bg_config,
                bg_dir,
                directory_lock,
                bg_group_commit_in_progress,
            )
            .await;
        });

        Ok(Self {
            wal_dir,
            config,
            current_file,
            sequence: Arc::new(AtomicU64::new(sequence)),
            batch_ranges: Arc::new(RwLock::new(batch_ranges)),
            write_tx,
            group_commit_in_progress,
        })
    }

    pub async fn append(&self, key: &[u8], value: &[u8]) -> Result<u64> {
        if self.config.sync_on_write {
            // Sync mode (paranoid): use traditional path with fsync
            let entry = self.create_entry(key, value, EntryType::Data)?;
            let sequence = entry.sequence;

            // Try direct path if lock is free, otherwise use group commit
            let lock_available = self.current_file.try_write().is_some();

            if lock_available {
                self.write_entry_direct(&entry, true)?;
            } else {
                // Lock contended - use group commit to share fsync
                let (tx, rx) = oneshot::channel();
                let req = WriteRequest {
                    entry,
                    response: tx,
                };
                self.write_tx
                    .send(req)
                    .await
                    .map_err(|_| WalError::ChannelClosed)?;
                rx.await.map_err(|_| WalError::ChannelClosed)??;
            }
            Ok(sequence)
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
                info!("Rotated WAL file, new sequence: {}", new_seq);
            }
            file.file.write_all(&buf)?;
            file.record_append(entry_bytes, 1, sequence, sequence);

            Ok(sequence)
        })
    }

    /// Write entry directly to buffered file
    /// This bypasses the channel overhead that causes convoy effect
    /// If `sync` is true, flushes and fsyncs after write (paranoid mode)
    fn write_entry_direct(&self, entry: &WalEntry, sync: bool) -> Result<()> {
        let entry_bytes = entry_size(entry) as u64;

        // Check if rotation needed (check while holding read lock, then upgrade if needed)
        let needs_rotation = {
            let file = self.current_file.read();
            file.should_rotate(entry_bytes, self.config.max_file_size)
        };

        if needs_rotation {
            rotate_sync(&self.current_file, &self.wal_dir, &self.config)?;
        }

        let mut file = self.current_file.write();
        write_entry(&mut file.file, entry)?;
        file.record_append(entry_bytes, 1, entry.sequence, entry.sequence);

        if sync {
            // Paranoid mode: fsync to disk (survives power loss)
            file.file.sync_all()?;
        }
        Ok(())
    }

    pub async fn append_delete(&self, key: &[u8]) -> Result<u64> {
        if self.config.sync_on_write {
            // Sync mode: use traditional path with fsync
            let entry = self.create_delete_entry(key)?;
            let sequence = entry.sequence;

            let lock_available = self.current_file.try_write().is_some();

            if lock_available {
                self.write_entry_direct(&entry, true)?;
            } else {
                let (tx, rx) = oneshot::channel();
                let req = WriteRequest {
                    entry,
                    response: tx,
                };
                self.write_tx
                    .send(req)
                    .await
                    .map_err(|_| WalError::ChannelClosed)?;
                rx.await.map_err(|_| WalError::ChannelClosed)??;
            }
            Ok(sequence)
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
                info!("Rotated WAL file, new sequence: {}", new_seq);
            }
            file.file.write_all(&buf)?;
            file.record_append(entry_bytes, 1, sequence, sequence);

            Ok(sequence)
        })
    }

    /// Append multiple key-value pairs in a single batch (bypasses group commit)
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
                bytes_written: 0,
            });
        }

        let batch = self.encode_entries_batch(entries)?;
        self.write_encoded_batch(&batch).await?;
        self.batch_ranges
            .write()
            .insert(batch.first_sequence, batch.last_sequence);

        Ok(BatchAppend {
            sequences: batch.sequences,
            bytes_written: batch.encoded.len() as u64,
        })
    }

    pub async fn flush(&self) -> Result<()> {
        let mut file = self.current_file.write();
        finalize_header(&mut file)?;
        Ok(())
    }

    pub async fn read_from(&self, start_sequence: u64) -> Result<Vec<WalEntry>> {
        let mut entries = Vec::new();
        let mut seen = HashSet::new();

        self.flush().await?;

        let current_path = self.current_file.read().path.clone();
        let mut wal_files = self.list_wal_files().await?;
        wal_files.sort_by_key(|f| f.0);

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

        let mut wal_files = self.list_wal_files().await?;
        wal_files.sort_by_key(|f| f.0);

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
                eligible.push(path);
            }
        }

        // Keep the active identity stable only for the final recheck/unlink
        // window. Rotation and appends require the write side of this lock.
        let current_file = self.current_file.read();
        for path in eligible {
            if *path != current_file.path {
                info!("Deleting WAL file: {:?}", path);
                std::fs::remove_file(path)?;
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

    #[cfg(test)]
    pub(crate) fn lock_current_file_for_test(&self) -> parking_lot::RwLockWriteGuard<'_, WalFile> {
        self.current_file.write()
    }

    #[cfg(test)]
    pub(crate) fn group_commit_in_progress_for_test(&self) -> u64 {
        self.group_commit_in_progress.load(Ordering::Acquire)
    }

    // ========================================
    // Private methods
    // ========================================

    fn create_entry(&self, key: &[u8], value: &[u8], entry_type: EntryType) -> Result<WalEntry> {
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
        let timestamp = super::cached_time::now_ms();
        let data = encode_kv(key, value);

        Ok(WalEntry {
            sequence,
            timestamp,
            entry_type,
            data: Bytes::from(data),
        })
    }

    fn create_delete_entry(&self, key: &[u8]) -> Result<WalEntry> {
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
        let timestamp = super::cached_time::now_ms();
        let data = encode_delete(key);

        Ok(WalEntry {
            sequence,
            timestamp,
            entry_type: EntryType::Delete,
            data: Bytes::from(data),
        })
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
        rotate_sync(&self.current_file, &self.wal_dir, &self.config)
    }

    async fn group_commit_loop(
        mut rx: mpsc::Receiver<WriteRequest>,
        current_file: Arc<RwLock<WalFile>>,
        config: WalConfig,
        wal_dir: PathBuf,
        directory_lock: Option<Weak<DirectoryLock>>,
        group_commit_in_progress: Arc<AtomicU64>,
    ) {
        // Adaptive group commit: no artificial delay for single writers,
        // but batches concurrent writers efficiently.
        //
        // Key insight: during fsync of batch N, writes for batch N+1 accumulate
        // in the channel. When fsync completes, we grab all pending writes immediately.
        // The fsync latency itself provides the batching window.

        loop {
            // Wait for first write (blocking)
            let first = match rx.recv().await {
                Some(req) => req,
                None => break,
            };

            let mut batch = vec![first];

            // Immediately grab ALL other pending writes (non-blocking)
            // This is the key optimization: no artificial delay
            while batch.len() < config.max_batch_size {
                match rx.try_recv() {
                    Ok(req) => batch.push(req),
                    Err(_) => break, // No more pending writes
                }
            }

            // If batch is small and we expect high concurrency, optionally wait briefly
            // This helps batch writes that arrive during the write (not fsync) phase
            if batch.len() < 4 && config.group_commit_delay_us > 0 {
                let brief_wait = std::time::Duration::from_micros(
                    config.group_commit_delay_us.min(100), // Cap at 100μs
                );
                let deadline = tokio::time::Instant::now() + brief_wait;
                while batch.len() < config.max_batch_size {
                    match tokio::time::timeout_at(deadline, rx.recv()).await {
                        Ok(Some(req)) => batch.push(req),
                        _ => break,
                    }
                }
            }

            // Engine-owned WALs may outlive a cancelled append future. Do not
            // mutate after the Engine's directory ownership has ended; if a
            // mutation already started, retain ownership through its response.
            let _directory_lock = match directory_lock.as_ref() {
                Some(directory_lock) => match directory_lock.upgrade() {
                    Some(directory_lock) => Some(directory_lock),
                    None => break,
                },
                None => None,
            };
            let _in_progress = InProgressGuard::new(Arc::clone(&group_commit_in_progress));

            let result = write_batch_sync(&current_file, &batch, &config, &wal_dir);
            let ok = result.is_ok();

            for req in batch {
                let _ = req.response.send(if ok {
                    Ok(())
                } else {
                    Err(WalError::Io {
                        message: "Batch write failed".to_string(),
                        source: None,
                    })
                });
            }
        }
    }

    async fn open_or_create(
        wal_dir: &Path,
        config: &WalConfig,
    ) -> Result<(WalFile, u64, BTreeMap<u64, u64>)> {
        let mut entries = tokio::fs::read_dir(wal_dir).await?;
        let mut wal_files = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension() == Some(std::ffi::OsStr::new("wal")) {
                if let Some(sequence) = wal_sequence_from_path(&path) {
                    wal_files.push((sequence, path));
                }
            }
        }
        wal_files.sort_by_key(|(sequence, _)| *sequence);

        if let Some((latest_filename_sequence, latest)) = wal_files.last() {
            let mut next_sequence = 0;
            let mut batch_ranges = BTreeMap::new();
            for (_, path) in &wal_files[..wal_files.len() - 1] {
                let metadata = inspect_segment(path, false)?;
                synchronize_segment_header(path, &metadata)?;
                next_sequence = next_sequence.max(metadata.next_sequence());
                batch_ranges.extend(metadata.batch_ranges.iter().copied());
            }
            let latest_metadata = inspect_segment(latest, true)?;
            let (mut file, latest_next_sequence) = open_recovered_file(latest, &latest_metadata)?;
            next_sequence = next_sequence.max(latest_next_sequence);
            batch_ranges.extend(latest_metadata.batch_ranges.iter().copied());

            // Older segments remain readable, but current writes use v4. Start
            // a new segment rather than adding v4 batch records to an old one.
            if !latest_metadata.format.is_current() {
                finalize_header(&mut file)?;
                let new_sequence = next_sequence.max(latest_filename_sequence.saturating_add(1));
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
        let mut files = Vec::new();
        let mut entries = tokio::fs::read_dir(&self.wal_dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension() == Some(std::ffi::OsStr::new("wal")) {
                if let Some(sequence) = wal_sequence_from_path(&path) {
                    files.push((sequence, path));
                }
            }
        }
        Ok(files)
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

// ========================================
// Synchronous helper routines
// ========================================

fn write_batch_sync(
    current_file: &Arc<RwLock<WalFile>>,
    batch: &[WriteRequest],
    config: &WalConfig,
    wal_dir: &Path,
) -> Result<()> {
    let entries: Vec<&WalEntry> = batch.iter().map(|req| &req.entry).collect();

    let total_batch_size: u64 = entries.iter().map(|e| entry_size(e) as u64).sum();
    let needs_rotation = {
        let f = current_file.read();
        f.should_rotate(total_batch_size, config.max_file_size)
    };
    if needs_rotation {
        rotate_sync(current_file, wal_dir, config)?;
    }

    let mut f = current_file.write();
    write_entries_batch(&mut f.file, &entries)?;
    if let (Some(min_sequence), Some(max_sequence)) = (
        entries.iter().map(|entry| entry.sequence).min(),
        entries.iter().map(|entry| entry.sequence).max(),
    ) {
        f.record_append(
            total_batch_size,
            entries.len() as u64,
            min_sequence,
            max_sequence,
        );
    }

    if config.sync_on_write {
        f.file.sync_all()?;
    }
    Ok(())
}

fn rotate_sync(
    current_file: &Arc<RwLock<WalFile>>,
    wal_dir: &Path,
    config: &WalConfig,
) -> Result<()> {
    let mut current = current_file.write();
    finalize_header(&mut current)?;

    let new_seq = current.next_segment_sequence()?;
    *current = create_file(wal_dir, new_seq, config)?;

    info!("Rotated WAL file, new sequence: {}", new_seq);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::types::{LEGACY_ENTRY_EXTENSION_SIZE, WAL_FIRST_SEQUENCE_OFFSET};
    use super::*;
    use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};
    use tempfile::TempDir;

    fn wal_paths(directory: &Path) -> Vec<PathBuf> {
        let mut paths: Vec<_> = std::fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|extension| extension == "wal"))
            .collect();
        paths.sort();
        paths
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
    async fn queued_group_commit_single_and_direct_batch_follow_sequence_order() {
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
        while wal.current_sequence() < 1 {
            tokio::task::yield_now().await;
        }

        let batch_wal = Arc::clone(&wal);
        let batch = tokio::spawn(async move {
            batch_wal
                .append_batch(&[
                    (b"key".as_slice(), Some(b"new".as_slice())),
                    (b"key".as_slice(), None),
                ])
                .await
        });
        while wal.current_sequence() < 3 {
            tokio::task::yield_now().await;
        }
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
        drop(wal);

        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&[8, 0, 0, 0, 1, 2, 3])
            .unwrap();

        let repaired = WriteAheadLog::new(directory.path(), config.clone())
            .await
            .unwrap();
        assert_eq!(repaired.current_size(), valid_end);
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
