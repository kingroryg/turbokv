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
//! ## File Format (v3)
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
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;
use tokio::sync::{mpsc, oneshot};
use tracing::info;

use crate::core::crypto::crc32_checksum;
use file::{
    create_file, entry_size, finalize_header, read_entry, read_header_last_sequence, recover_file,
    write_entries_batch, write_entry, WalFile,
};
use types::{ENTRY_HEADER_SIZE, WAL_HEADER_SIZE};

// Thread-local buffer for zero-allocation WAL writes
// Pre-allocated to avoid per-write heap allocations
thread_local! {
    static WAL_ENCODE_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(4096));
}

struct WriteRequest {
    entry: WalEntry,
    response: oneshot::Sender<Result<()>>,
}

pub struct WriteAheadLog {
    wal_dir: PathBuf,
    config: WalConfig,
    current_file: Arc<RwLock<WalFile>>,
    sequence: Arc<AtomicU64>,
    write_tx: mpsc::Sender<WriteRequest>,
}

impl WriteAheadLog {
    pub async fn new(wal_dir: impl AsRef<Path>, config: WalConfig) -> Result<Self> {
        let wal_dir = wal_dir.as_ref().to_path_buf();
        tokio::fs::create_dir_all(&wal_dir)
            .await
            .map_err(|e| WalError::Io {
                message: format!("Failed to create WAL directory: {:?}", wal_dir),
                source: Some(e),
            })?;

        let (wal_file, sequence) = Self::open_or_create(&wal_dir, &config).await?;
        let current_file = Arc::new(RwLock::new(wal_file));
        let (write_tx, write_rx) = mpsc::channel::<WriteRequest>(config.max_batch_size * 2);

        // Spawn background group commit loop
        let bg_file = Arc::clone(&current_file);
        let bg_config = config.clone();
        let bg_dir = wal_dir.clone();
        tokio::spawn(async move {
            Self::group_commit_loop(write_rx, bg_file, bg_config, bg_dir).await;
        });

        Ok(Self {
            wal_dir,
            config,
            current_file,
            sequence: Arc::new(AtomicU64::new(sequence)),
            write_tx,
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
            buf.extend_from_slice(&[0u8; 6]);

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
            let needs_rotation = {
                let file = self.current_file.read();
                file.size + entry_bytes > self.config.max_file_size
            };

            if needs_rotation {
                rotate_sync(&self.current_file, &self.wal_dir, &self.config)?;
            }

            // Single write to file
            let mut file = self.current_file.write();
            file.file.write_all(&buf)?;
            file.file.flush()?; // Ensure data reaches OS (survives process crash)
            file.size += entry_bytes;
            file.entry_count += 1;
            file.last_sequence = sequence;

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
            file.size + entry_bytes > self.config.max_file_size
        };

        if needs_rotation {
            rotate_sync(&self.current_file, &self.wal_dir, &self.config)?;
        }

        let mut file = self.current_file.write();
        write_entry(&mut file.file, entry)?;
        file.file.flush()?; // Ensure data reaches OS (survives process crash)
        file.size += entry_bytes;
        file.entry_count += 1;
        file.last_sequence = entry.sequence;

        if sync {
            // Paranoid mode: also fsync to disk (survives power loss)
            file.file.get_ref().sync_all()?;
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
            buf.extend_from_slice(&[0u8; 6]); // reserved

            // Encode key only (no value for delete)
            let data_start = buf.len();
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(key);

            // Compute CRC
            let crc = crc32_checksum(&buf[data_start..]);
            buf[crc_offset..crc_offset + 4].copy_from_slice(&crc.to_le_bytes());

            // Check rotation and write
            let entry_bytes = buf.len() as u64;
            let needs_rotation = {
                let file = self.current_file.read();
                file.size + entry_bytes > self.config.max_file_size
            };

            if needs_rotation {
                rotate_sync(&self.current_file, &self.wal_dir, &self.config)?;
            }

            let mut file = self.current_file.write();
            file.file.write_all(&buf)?;
            file.size += entry_bytes;
            file.entry_count += 1;
            file.last_sequence = sequence;

            Ok(sequence)
        })
    }

    /// Append multiple key-value pairs in a single batch (bypasses group commit)
    pub async fn append_batch(&self, entries: &[(&[u8], Option<&[u8]>)]) -> Result<Vec<u64>> {
        if entries.is_empty() {
            return Ok(vec![]);
        }

        let wal_entries = self.create_entries_batch(entries)?;
        let sequences: Vec<u64> = wal_entries.iter().map(|e| e.sequence).collect();

        self.write_batch(&wal_entries).await?;

        Ok(sequences)
    }

    pub async fn flush(&self) -> Result<()> {
        let mut file = self.current_file.write();
        file.file.flush()?;
        file.file.get_ref().sync_all()?;
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

        let current_path = self.current_file.read().path.clone();
        let mut wal_files = self.list_wal_files().await?;
        wal_files.sort_by_key(|f| f.0);

        let paths: Vec<PathBuf> = wal_files
            .into_iter()
            .filter(|(_, path)| {
                if start_sequence == 0 || *path == current_path {
                    return true;
                }
                read_header_last_sequence(path)
                    .map(|last| last >= start_sequence)
                    .unwrap_or(true)
            })
            .map(|(_, p)| p)
            .collect();

        WalEntryIterator::new(paths, start_sequence)
    }

    pub async fn truncate(&self, up_to_sequence: u64) -> Result<()> {
        info!("Truncating WAL up to sequence {}", up_to_sequence);

        let current_path = self.current_file.read().path.clone();

        for (seq, path) in self.list_wal_files().await? {
            // Never delete the current active file
            if path == current_path {
                continue;
            }
            if seq < up_to_sequence {
                info!("Deleting WAL file: {:?}", path);
                tokio::fs::remove_file(path).await?;
            }
        }
        Ok(())
    }

    pub fn current_sequence(&self) -> u64 {
        self.sequence.load(Ordering::SeqCst)
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

    fn create_entries_batch(&self, entries: &[(&[u8], Option<&[u8]>)]) -> Result<Vec<WalEntry>> {
        use rayon::prelude::*;

        let start_sequence = self
            .sequence
            .fetch_add(entries.len() as u64, Ordering::SeqCst);
        let timestamp = super::cached_time::now_ms();

        // Parallel encoding (CPU-bound, parallelizable)
        let wal_entries: Vec<WalEntry> = entries
            .par_iter()
            .enumerate()
            .map(|(i, (key, value))| {
                let sequence = start_sequence + i as u64;
                let (data, entry_type) = match value {
                    Some(v) => (encode_kv(key, v), EntryType::Data),
                    None => (encode_delete(key), EntryType::Delete),
                };
                WalEntry {
                    sequence,
                    timestamp,
                    entry_type,
                    data: Bytes::from(data),
                }
            })
            .collect();

        Ok(wal_entries)
    }

    async fn write_batch(&self, entries: &[WalEntry]) -> Result<()> {
        let total_batch_size: u64 = entries.iter().map(|e| entry_size(e) as u64).sum();
        let needs_rotation = {
            let f = self.current_file.read();
            f.size + total_batch_size > self.config.max_file_size
        };
        if needs_rotation {
            self.rotate().await?;
        }

        let mut f = self.current_file.write();
        write_entries_batch(&mut f.file, entries)?;
        f.size += total_batch_size;
        f.entry_count += entries.len() as u64;
        if let Some(last_entry) = entries.last() {
            f.last_sequence = last_entry.sequence;
        }

        if self.config.sync_on_write {
            f.file.flush()?;
            f.file.get_ref().sync_all()?;
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

    async fn open_or_create(wal_dir: &Path, config: &WalConfig) -> Result<(WalFile, u64)> {
        let mut entries = tokio::fs::read_dir(wal_dir).await?;
        let mut wal_files = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension() == Some(std::ffi::OsStr::new("wal")) {
                wal_files.push(path);
            }
        }
        wal_files.sort();

        if let Some(latest) = wal_files.last() {
            recover_file(latest, config)
        } else {
            Ok((create_file(wal_dir, 0, config)?, 0))
        }
    }

    async fn list_wal_files(&self) -> Result<Vec<(u64, PathBuf)>> {
        let mut files = Vec::new();
        let mut entries = tokio::fs::read_dir(&self.wal_dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension() == Some(std::ffi::OsStr::new("wal")) {
                if let Some(name) = path.file_stem() {
                    if let Ok(seq) = name.to_string_lossy().parse::<u64>() {
                        files.push((seq, path));
                    }
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
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(WAL_HEADER_SIZE as u64))?;

        while let Ok(entry) = read_entry(&mut reader) {
            if entry.sequence >= start_sequence && !seen.contains(&entry.sequence) {
                seen.insert(entry.sequence);
                entries.push(entry);
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
        f.size + total_batch_size > config.max_file_size
    };
    if needs_rotation {
        rotate_sync(current_file, wal_dir, config)?;
    }

    let mut f = current_file.write();
    write_entries_batch(&mut f.file, &entries)?;
    f.size += total_batch_size;
    f.entry_count += entries.len() as u64;
    if let Some(last_entry) = entries.last() {
        f.last_sequence = last_entry.sequence;
    }

    if config.sync_on_write {
        f.file.flush()?;
        f.file.get_ref().sync_all()?;
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

    let new_seq = current.last_sequence + 1;
    *current = create_file(wal_dir, new_seq, config)?;

    info!("Rotated WAL file, new sequence: {}", new_seq);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

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
    }
}
