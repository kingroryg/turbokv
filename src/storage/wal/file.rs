//! WAL file handling for TurboKV
//!
//! ## Write path
//!
//! - Durable mode publishes each record's tag and the header cursor through a
//!   bounded, physically reserved mapping, then acknowledges page-cache arrival.
//! - Paranoid mode and the unsupported-mapping fallback publish tags last with
//!   direct `File` writes; TurboKV does not use `O_DIRECT`.
//!
//! ## WAL Entry Format (v5)
//!
//! Entry header: 32 bytes
//! - length: u32 (4 bytes)
//! - sequence: u64 (8 bytes)
//! - timestamp: u64 (8 bytes)
//! - entry_type: u8 (1 byte)
//! - flags: u8 (1 byte)
//! - crc: u32 (4 bytes)
//! - reserved: 4 zero bytes + 2-byte commit tag (published last)
//! Payload: variable length

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{compiler_fence, fence, Ordering};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use bytes::Bytes;
use memmap2::{MmapMut, MmapOptions};

use crate::core::crypto::crc32_checksum;

use super::reservation::{allocation_api_unsupported, reserve_file_space, AllocationResult};
use super::types::*;

const MAX_SUFFIX_CANDIDATE_OFFSETS: u64 = 64 * 1024;
const MAX_SUFFIX_RECORD_BYTES: u64 = 8 * 1024 * 1024;
const MAPPED_GROWTH_BYTES: u64 = 8 * 1024 * 1024;
const CURSOR_CRC64_POLYNOMIAL: u64 = 0x42f0_e1eb_a9ea_3693;
const CURSOR_CRC64_TABLE: [u64; 256] = make_crc64_table();
#[cfg(test)]
const INJECT_CREATE_FAILURE_MAX_SIZE: u64 = u64::MAX - 17;
#[cfg(test)]
const CREATE_CRASH_DIRECTORY: &str = "TURBOKV_TEST_WAL_CREATE_CRASH_DIRECTORY";
#[cfg(test)]
const CREATE_CRASH_SEQUENCE: &str = "TURBOKV_TEST_WAL_CREATE_CRASH_SEQUENCE";
#[cfg(test)]
const CREATE_CRASH_HEADER_BYTES: &str = "TURBOKV_TEST_WAL_CREATE_CRASH_HEADER_BYTES";
#[cfg(test)]
const CREATE_CRASH_EXIT_CODE: i32 = 91;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WalFormat {
    Legacy,
    V3,
    V4,
    Current,
}

impl WalFormat {
    fn from_version(version: u32) -> Result<Self> {
        match version {
            WAL_VERSION_V1 | WAL_VERSION_V2 => Ok(Self::Legacy),
            WAL_VERSION_V3 => Ok(Self::V3),
            WAL_VERSION_V4 => Ok(Self::V4),
            WAL_VERSION => Ok(Self::Current),
            _ => Err(WalError::InvalidFormat(format!(
                "Unsupported WAL version: {version}"
            ))),
        }
    }

    pub fn has_legacy_extension(self) -> bool {
        self == Self::Legacy
    }

    pub fn is_current(self) -> bool {
        self == Self::Current
    }

    fn supports_batches(self) -> bool {
        matches!(self, Self::V4 | Self::Current)
    }

    fn entry_header_size(self) -> usize {
        ENTRY_HEADER_SIZE
            + if self.has_legacy_extension() {
                LEGACY_ENTRY_EXTENSION_SIZE
            } else {
                0
            }
    }
}

/// In-memory representation of an open WAL file
#[derive(Debug)]
pub(crate) struct WalFile {
    pub path: PathBuf,
    pub file: File,
    pub size: u64,
    pub entry_count: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub format: WalFormat,
    mapping: Option<MmapMut>,
    mapping_disabled: bool,
    allocation_capacity: u64,
    #[cfg(test)]
    growth_view_flushes: u64,
}

impl WalFile {
    pub fn should_rotate(&self, additional_bytes: u64, maximum_size: u64) -> bool {
        self.entry_count > 0 && self.size.saturating_add(additional_bytes) > maximum_size
    }

    pub fn record_append(
        &mut self,
        bytes: u64,
        count: u64,
        first_sequence: u64,
        last_sequence: u64,
    ) -> Result<()> {
        debug_assert!(count > 0);
        let new_size = self
            .size
            .checked_add(bytes)
            .ok_or_else(|| WalError::InvalidFormat("WAL logical size overflows".to_string()))?;
        let new_entry_count = self
            .entry_count
            .checked_add(count)
            .ok_or_else(|| WalError::InvalidFormat("WAL entry count overflows".to_string()))?;
        if self.entry_count == 0 {
            self.first_sequence = first_sequence;
            self.last_sequence = last_sequence;
        } else {
            self.first_sequence = self.first_sequence.min(first_sequence);
            self.last_sequence = self.last_sequence.max(last_sequence);
        }
        self.size = new_size;
        self.entry_count = new_entry_count;
        if self.mapping.is_none() {
            self.allocation_capacity = self.allocation_capacity.max(self.size);
        }
        if self.format == WalFormat::Current {
            self.publish_acknowledged_end()?;
        }
        Ok(())
    }

    pub fn next_segment_sequence(&self) -> Result<u64> {
        let filename_sequence = wal_sequence_from_path(&self.path).ok_or_else(|| {
            WalError::InvalidFormat(format!(
                "WAL filename is not a numeric sequence: {}",
                self.path.display()
            ))
        })?;
        Ok(self
            .last_sequence
            .saturating_add(1)
            .max(filename_sequence.saturating_add(1)))
    }

    pub fn next_written_sequence(&self) -> u64 {
        if self.entry_count == 0 {
            self.first_sequence
        } else {
            self.last_sequence.saturating_add(1)
        }
    }

    /// Write one already-encoded v5 record through the mapped durable path or
    /// through the direct compatibility fallback.
    pub fn write_current_record(&mut self, encoded: &[u8], maximum_size: u64) -> Result<()> {
        debug_assert_eq!(self.format, WalFormat::Current);
        validate_current_encoding(encoded)?;
        if self.ensure_mapped_capacity(encoded.len() as u64, maximum_size)? {
            let start = usize::try_from(self.size).map_err(|_| {
                WalError::InvalidFormat("mapped WAL offset does not fit usize".to_string())
            })?;
            let end = start.checked_add(encoded.len()).ok_or_else(|| {
                WalError::InvalidFormat("mapped WAL record range overflows".to_string())
            })?;
            let tag_start = start.checked_add(V5_COMMIT_TAG_OFFSET).ok_or_else(|| {
                WalError::InvalidFormat("mapped WAL tag offset overflows".to_string())
            })?;
            let mapping = self
                .mapping
                .as_mut()
                .expect("mapped capacity was installed");
            if end > mapping.len() {
                return Err(WalError::InvalidFormat(
                    "mapped WAL record exceeds reserved capacity".to_string(),
                ));
            }

            mapping[start..tag_start].copy_from_slice(&encoded[..V5_COMMIT_TAG_OFFSET]);
            mapping[tag_start..tag_start + V5_COMMIT_TAG.len()].fill(0);
            mapping[tag_start + V5_COMMIT_TAG.len()..end]
                .copy_from_slice(&encoded[V5_COMMIT_TAG_OFFSET + V5_COMMIT_TAG.len()..]);
            publish_fence();
            write_mapped_bytes_volatile(mapping, tag_start, &V5_COMMIT_TAG);
            publish_fence();
        } else {
            self.write_current_record_direct(encoded)?;
        }
        Ok(())
    }

    /// Write a v5 record without mmap while preserving the tag-last commit
    /// protocol. A crash before the final two-byte write therefore leaves an
    /// uncommitted, repairable active tail instead of a committed CRC failure.
    fn write_current_record_direct(&mut self, encoded: &[u8]) -> Result<()> {
        self.write_current_record_direct_at(self.size, encoded)
    }

    fn write_current_record_direct_at(&mut self, record_start: u64, encoded: &[u8]) -> Result<()> {
        validate_current_encoding(encoded)?;
        let tag_end = V5_COMMIT_TAG_OFFSET + V5_COMMIT_TAG.len();
        self.file.seek(SeekFrom::Start(record_start))?;
        self.file.write_all(&encoded[..V5_COMMIT_TAG_OFFSET])?;
        self.file.write_all(&[0_u8; V5_COMMIT_TAG.len()])?;
        self.file.write_all(&encoded[tag_end..])?;
        publish_fence();
        self.file.seek(SeekFrom::Start(
            record_start
                .checked_add(V5_COMMIT_TAG_OFFSET as u64)
                .ok_or_else(|| {
                    WalError::InvalidFormat("v5 commit tag offset overflows".to_string())
                })?,
        ))?;
        self.file.write_all(&V5_COMMIT_TAG)?;
        publish_fence();
        self.file.seek(SeekFrom::Start(
            record_start
                .checked_add(encoded.len() as u64)
                .ok_or_else(|| WalError::InvalidFormat("v5 record end overflows".to_string()))?,
        ))?;
        Ok(())
    }

    /// Direct-write a paranoid group one record at a time so no record's tag
    /// can become visible before its own payload is complete.
    pub fn write_current_entries_direct<T: AsRef<WalEntry>>(
        &mut self,
        entries: &[T],
    ) -> Result<()> {
        debug_assert_eq!(self.format, WalFormat::Current);
        let mut offset = self.size;
        let mut encoded = Vec::new();
        for entry in entries {
            encoded.clear();
            write_entry(&mut encoded, entry.as_ref())?;
            self.write_current_record_direct_at(offset, &encoded)?;
            offset = offset.checked_add(encoded.len() as u64).ok_or_else(|| {
                WalError::InvalidFormat("v5 direct group end overflows".to_string())
            })?;
        }
        Ok(())
    }

    /// Ensure the active durable segment has physically reserved mapped space.
    /// Unsupported allocation APIs disable mmap and retain the direct writer.
    fn ensure_mapped_capacity(&mut self, additional_bytes: u64, maximum_size: u64) -> Result<bool> {
        if self.mapping_disabled {
            return Ok(false);
        }
        let required = self
            .size
            .checked_add(additional_bytes)
            .ok_or_else(|| WalError::InvalidFormat("mapped WAL size overflows".to_string()))?;
        if self
            .mapping
            .as_ref()
            .is_some_and(|mapping| mapping.len() as u64 >= required)
        {
            return Ok(true);
        }

        let rounded = required
            .checked_add(MAPPED_GROWTH_BYTES - 1)
            .map(|value| value / MAPPED_GROWTH_BYTES * MAPPED_GROWTH_BYTES)
            .ok_or_else(|| WalError::InvalidFormat("mapped WAL capacity overflows".to_string()))?;
        let target = rounded.min(maximum_size.max(required));
        let mapping_length = match usize::try_from(target) {
            Ok(length) => length,
            Err(_) => {
                self.disable_mapping()?;
                return Ok(false);
            }
        };

        // Publish the old Windows view before growth unmaps it. A later
        // explicit file sync can then complete the required FlushViewOfFile +
        // FlushFileBuffers pair even though this view no longer exists.
        if let Some(mapping) = self.mapping.take() {
            // Windows does not include dirty mapped views in a later
            // FlushFileBuffers call after the view has been unmapped. Unix
            // fsync/F_FULLFSYNC does include prior mmap writes to the file, so
            // avoid an otherwise measurable msync on that path.
            #[cfg(any(windows, test))]
            mapping.flush_async()?;
            #[cfg(all(not(windows), not(test)))]
            drop(mapping);
            #[cfg(test)]
            {
                self.growth_view_flushes += 1;
            }
        }
        match reserve_file_space(&self.file, target)? {
            AllocationResult::Reserved => {}
            AllocationResult::Unsupported => {
                self.disable_mapping()?;
                return Ok(false);
            }
        }
        self.file.set_len(target)?;
        self.allocation_capacity = target;
        // SAFETY: the file is exclusively owned by this WAL, was extended to
        // `mapping_length`, and cannot be truncated while this lock-owned
        // `WalFile` is being accessed.
        match unsafe { MmapOptions::new().len(mapping_length).map_mut(&self.file) } {
            Ok(mapping) => {
                self.mapping = Some(mapping);
                Ok(true)
            }
            Err(error) if allocation_api_unsupported(&error) => {
                self.disable_mapping()?;
                Ok(false)
            }
            Err(error) => Err(error.into()),
        }
    }

    fn publish_acknowledged_end(&mut self) -> Result<()> {
        let segment_sequence = wal_sequence_from_path(&self.path).ok_or_else(|| {
            WalError::InvalidFormat(format!(
                "WAL filename is not a numeric sequence: {}",
                self.path.display()
            ))
        })?;
        let checksum = acknowledged_end_crc(segment_sequence, self.size);
        if let Some(mapping) = self.mapping.as_mut() {
            write_mapped_bytes_volatile(
                mapping,
                WAL_ACKNOWLEDGED_END_OFFSET as usize,
                &self.size.to_le_bytes(),
            );
            publish_fence();
            write_mapped_bytes_volatile(
                mapping,
                WAL_ACKNOWLEDGED_END_CRC_OFFSET as usize,
                &checksum.to_le_bytes(),
            );
            publish_fence();
        } else {
            let end_position = self.file.stream_position()?;
            self.file
                .seek(SeekFrom::Start(WAL_ACKNOWLEDGED_END_OFFSET))?;
            self.file.write_u64::<LittleEndian>(self.size)?;
            self.file.write_u64::<LittleEndian>(checksum)?;
            self.file.seek(SeekFrom::Start(end_position))?;
        }
        Ok(())
    }

    fn disable_mapping(&mut self) -> Result<()> {
        if let Some(mapping) = self.mapping.take() {
            mapping.flush()?;
        }
        if self.file.metadata()?.len() != self.size {
            self.file.set_len(self.size)?;
        }
        self.file.seek(SeekFrom::Start(self.size))?;
        self.mapping_disabled = true;
        self.allocation_capacity = self.size;
        Ok(())
    }

    fn finish_mapped_extent(&mut self) -> Result<()> {
        if let Some(mapping) = self.mapping.take() {
            mapping.flush()?;
        }
        let physical_len = self.file.metadata()?.len();
        if physical_len > self.allocation_capacity
            && !range_is_zero(&mut self.file, self.allocation_capacity, physical_len)?
        {
            return Err(WalError::Corruption(format!(
                "nonzero bytes follow active WAL allocation at byte {} in {}",
                self.allocation_capacity,
                self.path.display()
            )));
        }
        if physical_len != self.size {
            self.file.set_len(self.size)?;
        }
        self.allocation_capacity = self.size;
        self.file.seek(SeekFrom::Start(self.size))?;
        Ok(())
    }

    #[cfg(test)]
    pub fn allocation_capacity_for_test(&self) -> u64 {
        self.allocation_capacity
    }

    #[cfg(test)]
    pub fn disable_mapping_for_test(&mut self) -> Result<()> {
        self.disable_mapping()
    }

    #[cfg(test)]
    pub fn growth_view_flushes_for_test(&self) -> u64 {
        self.growth_view_flushes
    }

    pub fn enable_mapped_writes(&mut self, maximum_size: u64) -> Result<()> {
        if self.format != WalFormat::Current {
            return Ok(());
        }
        self.mapping_disabled = false;
        let _ = self.ensure_mapped_capacity(0, maximum_size)?;
        Ok(())
    }
}

fn validate_current_encoding(encoded: &[u8]) -> Result<()> {
    let tag_end = V5_COMMIT_TAG_OFFSET + V5_COMMIT_TAG.len();
    if encoded.len() < ENTRY_HEADER_SIZE || encoded[V5_COMMIT_TAG_OFFSET..tag_end] != V5_COMMIT_TAG
    {
        return Err(WalError::InvalidFormat(
            "encoded v5 record has no complete commit tag".to_string(),
        ));
    }
    let payload_length = u32::from_le_bytes(
        encoded[..4]
            .try_into()
            .expect("validated v5 record has four length bytes"),
    ) as usize;
    let expected_length = ENTRY_HEADER_SIZE
        .checked_add(payload_length)
        .ok_or_else(|| WalError::InvalidFormat("encoded v5 record length overflows".to_string()))?;
    if encoded.len() != expected_length {
        return Err(WalError::InvalidFormat(format!(
            "encoded v5 record has {} bytes but declares {expected_length}",
            encoded.len()
        )));
    }
    Ok(())
}

fn publish_fence() {
    compiler_fence(Ordering::Release);
    fence(Ordering::Release);
}

fn write_mapped_bytes_volatile(mapping: &mut MmapMut, offset: usize, bytes: &[u8]) {
    debug_assert!(offset.saturating_add(bytes.len()) <= mapping.len());
    for (index, byte) in bytes.iter().copied().enumerate() {
        // SAFETY: the range is bounds-checked above and the WAL owns the only
        // mutable mapping while its active-file write lock is held.
        unsafe { std::ptr::write_volatile(mapping.as_mut_ptr().add(offset + index), byte) };
    }
}

const fn make_crc64_table() -> [u64; 256] {
    let mut table = [0_u64; 256];
    let mut index = 0;
    while index < table.len() {
        let mut crc = (index as u64) << 56;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & (1_u64 << 63) != 0 {
                (crc << 1) ^ CURSOR_CRC64_POLYNOMIAL
            } else {
                crc << 1
            };
            bit += 1;
        }
        table[index] = crc;
        index += 1;
    }
    table
}

fn acknowledged_end_crc(segment_sequence: u64, acknowledged_end: u64) -> u64 {
    let mut crc = u64::MAX;
    for byte in WAL_VERSION
        .to_le_bytes()
        .into_iter()
        .chain(segment_sequence.to_le_bytes())
        .chain(acknowledged_end.to_le_bytes())
    {
        let index = ((crc >> 56) as u8 ^ byte) as usize;
        crc = (crc << 8) ^ CURSOR_CRC64_TABLE[index];
    }
    crc ^ u64::MAX
}

#[derive(Debug, Clone)]
pub(crate) struct SegmentMetadata {
    pub first_sequence: Option<u64>,
    pub last_sequence: Option<u64>,
    pub entry_count: u64,
    pub valid_end: u64,
    pub format: WalFormat,
    pub empty_sequence: u64,
    /// Atomic logical batch spans discovered during the validation scan.
    pub batch_ranges: Vec<(u64, u64)>,
}

impl SegmentMetadata {
    pub fn next_sequence(&self) -> u64 {
        self.last_sequence
            .map_or(self.empty_sequence, |sequence| sequence.saturating_add(1))
    }
}

pub(crate) fn wal_sequence_from_path(path: &Path) -> Option<u64> {
    wal_sequence_with_suffix(path, ".wal")
}

pub(crate) fn temporary_wal_sequence_from_path(path: &Path) -> Option<u64> {
    wal_sequence_with_suffix(path, ".wal.tmp")
}

fn wal_sequence_with_suffix(path: &Path, suffix: &str) -> Option<u64> {
    let filename = path.file_name()?.to_str()?;
    let stem = filename.strip_suffix(suffix)?;
    if stem.len() != 20 || !stem.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }

    let sequence = stem.parse::<u64>().ok()?;
    (filename == format!("{sequence:020}{suffix}")).then_some(sequence)
}

fn temporary_wal_path(wal_dir: &Path, sequence: u64) -> PathBuf {
    wal_dir.join(format!("{sequence:020}.wal.tmp"))
}

fn current_segment_header(sequence: u64) -> [u8; WAL_HEADER_SIZE] {
    let mut header = [0_u8; WAL_HEADER_SIZE];
    let mut writer = std::io::Cursor::new(&mut header[..]);
    writer.write_all(WAL_MAGIC).expect("fixed header buffer");
    writer
        .write_u32::<LittleEndian>(WAL_VERSION)
        .expect("fixed header buffer");
    writer
        .write_u64::<LittleEndian>(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        )
        .expect("fixed header buffer");
    writer
        .write_u64::<LittleEndian>(sequence)
        .expect("fixed header buffer");
    writer
        .write_u64::<LittleEndian>(sequence)
        .expect("fixed header buffer");
    writer
        .write_u64::<LittleEndian>(0)
        .expect("fixed header buffer");
    writer
        .write_u32::<LittleEndian>(0)
        .expect("fixed header buffer");
    writer
        .write_u64::<LittleEndian>(WAL_HEADER_SIZE as u64)
        .expect("fixed header buffer");
    writer
        .write_u64::<LittleEndian>(acknowledged_end_crc(sequence, WAL_HEADER_SIZE as u64))
        .expect("fixed header buffer");
    header
}

/// Create a new WAL file
pub(crate) fn create_file(wal_dir: &Path, sequence: u64, config: &WalConfig) -> Result<WalFile> {
    let filename = format!("{:020}.wal", sequence);
    let path = wal_dir.join(&filename);
    let temporary_path = temporary_wal_path(wal_dir, sequence);
    match std::fs::remove_file(&temporary_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    let mut installed = false;
    let result = (|| -> Result<WalFile> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(&temporary_path)?;
        let header = current_segment_header(sequence);

        #[cfg(test)]
        if std::env::var_os(CREATE_CRASH_DIRECTORY).is_some() {
            let cut = std::env::var(CREATE_CRASH_HEADER_BYTES)
                .expect("create-crash child has a header cut")
                .parse::<usize>()
                .expect("create-crash header cut is numeric");
            file.write_all(&header[..cut.min(header.len())])?;
            file.sync_all()?;
            std::process::exit(CREATE_CRASH_EXIT_CODE);
        }

        file.write_all(&header)?;

        #[cfg(test)]
        if config.max_file_size == INJECT_CREATE_FAILURE_MAX_SIZE {
            return Err(WalError::Io {
                message: "injected post-create allocation failure".to_string(),
                source: Some(std::io::Error::other("injected ENOSPC-like failure")),
            });
        }

        if path.try_exists()? {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("WAL segment already exists: {}", path.display()),
            )
            .into());
        }
        std::fs::rename(&temporary_path, &path)?;
        installed = true;

        let mut wal_file = WalFile {
            path: path.clone(),
            file,
            size: WAL_HEADER_SIZE as u64,
            entry_count: 0,
            first_sequence: sequence,
            last_sequence: sequence,
            format: WalFormat::Current,
            mapping: None,
            mapping_disabled: config.sync_on_write,
            allocation_capacity: WAL_HEADER_SIZE as u64,
            #[cfg(test)]
            growth_view_flushes: 0,
        };
        if !config.sync_on_write {
            wal_file.ensure_mapped_capacity(0, config.max_file_size)?;
        }
        Ok(wal_file)
    })();

    if result.is_err() {
        for cleanup_path in [&temporary_path, &path]
            .into_iter()
            .take(if installed { 2 } else { 1 })
        {
            if let Err(cleanup_error) = std::fs::remove_file(cleanup_path) {
                if cleanup_error.kind() == std::io::ErrorKind::NotFound {
                    continue;
                }
                tracing::warn!(
                    path = %cleanup_path.display(),
                    error = %cleanup_error,
                    "failed to remove an uninstalled WAL segment after creation failed"
                );
            }
        }
    }
    result
}

/// Recover a WAL file, returning the file handle and last sequence
#[cfg(test)]
pub(crate) fn recover_file(path: &Path, _config: &WalConfig) -> Result<(WalFile, u64)> {
    tracing::info!("Recovering from WAL file: {:?}", path);

    let metadata = inspect_segment(path, true)?;
    open_recovered_file(path, &metadata)
}

pub(crate) fn open_recovered_file(
    path: &Path,
    metadata: &SegmentMetadata,
) -> Result<(WalFile, u64)> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    if file.metadata()?.len() != metadata.valid_end {
        file.set_len(metadata.valid_end)?;
    }
    rewrite_header(&mut file, metadata)?;
    file.sync_all()?;
    file.seek(SeekFrom::Start(metadata.valid_end))?;

    let first_sequence = metadata.first_sequence.unwrap_or(metadata.empty_sequence);
    let last_sequence = metadata.last_sequence.unwrap_or(metadata.empty_sequence);
    Ok((
        WalFile {
            path: path.to_path_buf(),
            file,
            size: metadata.valid_end,
            entry_count: metadata.entry_count,
            first_sequence,
            last_sequence,
            format: metadata.format,
            mapping: None,
            mapping_disabled: true,
            allocation_capacity: metadata.valid_end,
            #[cfg(test)]
            growth_view_flushes: 0,
        },
        metadata.next_sequence(),
    ))
}

/// Validate a segment and derive sequence bounds exclusively from valid records.
///
/// Recovery repairs only damage confined to the physical end of the active
/// segment. A bad record with bytes after its declared end, or with a later
/// independently valid record, is interior corruption and is never skipped.
pub(crate) fn inspect_segment(path: &Path, repair_tail: bool) -> Result<SegmentMetadata> {
    inspect_segment_with_tail_policy(
        path,
        if repair_tail {
            TailPolicy::Repair
        } else {
            TailPolicy::Reject
        },
    )
}

/// Perform the same tail classification as recovery without changing bytes.
pub(crate) fn preflight_active_segment(path: &Path) -> Result<SegmentMetadata> {
    inspect_segment_with_tail_policy(path, TailPolicy::AllowRecoverable)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TailPolicy {
    Reject,
    AllowRecoverable,
    Repair,
}

fn inspect_segment_with_tail_policy(
    path: &Path,
    tail_policy: TailPolicy,
) -> Result<SegmentMetadata> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(tail_policy == TailPolicy::Repair)
        .open(path)?;
    let file_len = file.metadata()?.len();
    let (format, _header_first_sequence) = read_and_validate_header(&mut file)?;
    let empty_sequence = wal_sequence_from_path(path).ok_or_else(|| {
        WalError::InvalidFormat(format!(
            "WAL filename is not a numeric sequence: {}",
            path.display()
        ))
    })?;
    let entry_header_size = format.entry_header_size() as u64;
    let acknowledged_end = if format == WalFormat::Current {
        read_v5_acknowledged_end(&mut file, empty_sequence, file_len)?
    } else {
        None
    };

    let mut metadata = SegmentMetadata {
        first_sequence: None,
        last_sequence: None,
        entry_count: 0,
        valid_end: WAL_HEADER_SIZE as u64,
        format,
        empty_sequence,
        batch_ranges: Vec::new(),
    };
    let mut offset = WAL_HEADER_SIZE as u64;

    while offset < file_len {
        if format == WalFormat::Current && v5_header_is_zero(&mut file, offset, file_len)? {
            if acknowledged_end.is_some_and(|end| offset < end) {
                return Err(WalError::Corruption(format!(
                    "zero v5 record header before acknowledged end {acknowledged_end:?} at byte {offset} in {}",
                    path.display()
                )));
            }
            if tail_policy == TailPolicy::Reject {
                return Err(WalError::Corruption(format!(
                    "non-active v5 segment retains a preallocated tail at byte {offset} in {}",
                    path.display()
                )));
            }
            // The commit tag, not the order in which ordinary mapped body
            // stores reached memory, is the publication boundary. A process
            // can stop after payload cache lines changed but before any header
            // bytes became observable. The checked cursor is the writer's
            // last completed append boundary, and ordered publication makes a
            // zero header exactly there an unpublished tail regardless of its
            // arbitrary body bytes. With a stale or invalid cursor, prove that
            // the suffix is terminal instead of truncating interior damage.
            if acknowledged_end == Some(offset) {
                return finish_recoverable_tail(&mut file, &metadata, tail_policy);
            }
            if let Some(last_nonzero) = last_nonzero_offset(&mut file, offset, file_len)? {
                let suffix = find_valid_record_after(
                    &mut file,
                    offset.saturating_add(1),
                    last_nonzero.saturating_add(1),
                    file_len,
                    format,
                    entry_header_size,
                )?;
                if suffix != SuffixScan::Exhausted {
                    let ambiguity = if suffix == SuffixScan::Ambiguous {
                        "; plausible-suffix search exceeded its recovery budget"
                    } else {
                        ""
                    };
                    return Err(WalError::Corruption(format!(
                        "zero v5 record header before a plausible committed suffix at byte {offset} in {}{ambiguity}",
                        path.display()
                    )));
                }
            }
            return finish_recoverable_tail(&mut file, &metadata, tail_policy);
        }

        match read_record_at(&mut file, offset, file_len, format) {
            Ok((entry, end)) => {
                let (record_first, record_last) = entry.sequence_bounds()?;
                metadata.first_sequence = Some(
                    metadata
                        .first_sequence
                        .map_or(record_first, |first| first.min(record_first)),
                );
                metadata.last_sequence = Some(
                    metadata
                        .last_sequence
                        .map_or(record_last, |last| last.max(record_last)),
                );
                metadata.entry_count += record_last - record_first + 1;
                if entry.entry_type == EntryType::Batch {
                    metadata.batch_ranges.push((record_first, record_last));
                }
                metadata.valid_end = end;
                offset = end;
            }
            Err(RecordReadError::Io(error)) => return Err(error),
            Err(RecordReadError::Damage(failure)) => {
                if format == WalFormat::Current
                    && (!failure.repairable_v5_tail
                        || acknowledged_end.is_some_and(|end| offset < end))
                {
                    return Err(WalError::Corruption(format!(
                        "{} at byte {} in {}",
                        failure.reason,
                        offset,
                        path.display()
                    )));
                }

                let suffix = if format == WalFormat::Current {
                    let search_start = failure
                        .declared_end
                        .unwrap_or_else(|| offset.saturating_add(1));
                    match last_nonzero_offset(&mut file, search_start, file_len)? {
                        Some(last_nonzero) => find_valid_record_after(
                            &mut file,
                            offset.saturating_add(1),
                            last_nonzero.saturating_add(1),
                            file_len,
                            format,
                            entry_header_size,
                        )?,
                        None => SuffixScan::Exhausted,
                    }
                } else if failure.declared_end.is_some_and(|end| end < file_len) {
                    SuffixScan::Found
                } else {
                    find_valid_record_after(
                        &mut file,
                        offset.saturating_add(1),
                        file_len,
                        file_len,
                        format,
                        entry_header_size,
                    )?
                };

                if suffix != SuffixScan::Exhausted || tail_policy == TailPolicy::Reject {
                    let ambiguity = if suffix == SuffixScan::Ambiguous {
                        "; plausible-suffix search exceeded its recovery budget"
                    } else {
                        ""
                    };
                    return Err(WalError::Corruption(format!(
                        "{} at byte {} in {}{}",
                        failure.reason,
                        offset,
                        path.display(),
                        ambiguity
                    )));
                }

                if tail_policy == TailPolicy::AllowRecoverable {
                    return Ok(metadata);
                }
                return finish_recoverable_tail(&mut file, &metadata, tail_policy);
            }
        }
    }

    if acknowledged_end.is_some_and(|end| metadata.valid_end < end) {
        return Err(WalError::Corruption(format!(
            "v5 segment ends at {} before acknowledged end {acknowledged_end:?} in {}",
            metadata.valid_end,
            path.display()
        )));
    }

    Ok(metadata)
}

fn finish_recoverable_tail(
    file: &mut File,
    metadata: &SegmentMetadata,
    tail_policy: TailPolicy,
) -> Result<SegmentMetadata> {
    if tail_policy == TailPolicy::AllowRecoverable {
        return Ok(metadata.clone());
    }
    debug_assert!(tail_policy == TailPolicy::Repair);
    file.set_len(metadata.valid_end)?;
    file.seek(SeekFrom::Start(metadata.valid_end))?;
    rewrite_header(file, metadata)?;
    file.sync_all()?;
    Ok(metadata.clone())
}

fn read_v5_acknowledged_end(
    file: &mut File,
    segment_sequence: u64,
    file_len: u64,
) -> Result<Option<u64>> {
    file.seek(SeekFrom::Start(WAL_ACKNOWLEDGED_END_OFFSET))?;
    let end = file.read_u64::<LittleEndian>()?;
    let stored_crc = file.read_u64::<LittleEndian>()?;
    if stored_crc != acknowledged_end_crc(segment_sequence, end) {
        return Ok(None);
    }
    if end < WAL_HEADER_SIZE as u64 || end > file_len {
        return Err(WalError::Corruption(format!(
            "v5 acknowledged end {end} is outside physical extent {file_len}"
        )));
    }
    Ok(Some(end))
}

fn v5_header_is_zero(file: &mut File, offset: u64, file_len: u64) -> Result<bool> {
    if file_len.saturating_sub(offset) < ENTRY_HEADER_SIZE as u64 {
        return Ok(false);
    }
    file.seek(SeekFrom::Start(offset))?;
    let mut header = [0_u8; ENTRY_HEADER_SIZE];
    file.read_exact(&mut header)?;
    Ok(header == [0_u8; ENTRY_HEADER_SIZE])
}

fn range_is_zero(file: &mut File, start: u64, end: u64) -> Result<bool> {
    Ok(last_nonzero_offset(file, start, end)?.is_none())
}

fn last_nonzero_offset(file: &mut File, start: u64, end: u64) -> Result<Option<u64>> {
    if start >= end {
        return Ok(None);
    }
    let mut buffer = [0_u8; 16 * 1024];
    let mut offset = start;
    let mut last = None;
    file.seek(SeekFrom::Start(start))?;
    while offset < end {
        let bytes = usize::try_from((end - offset).min(buffer.len() as u64))
            .expect("bounded scan chunk fits usize");
        file.read_exact(&mut buffer[..bytes])?;
        if let Some(index) = buffer[..bytes].iter().rposition(|byte| *byte != 0) {
            last = Some(offset + index as u64);
        }
        offset += bytes as u64;
    }
    Ok(last)
}

struct RecordFailure {
    reason: String,
    declared_end: Option<u64>,
    repairable_v5_tail: bool,
}

enum RecordReadError {
    Damage(RecordFailure),
    Io(WalError),
}

fn read_record_at(
    file: &mut File,
    offset: u64,
    file_len: u64,
    format: WalFormat,
) -> std::result::Result<(WalEntry, u64), RecordReadError> {
    let header_size = format.entry_header_size() as u64;
    let remaining = file_len.saturating_sub(offset);
    if remaining < header_size {
        return Err(RecordReadError::Damage(RecordFailure {
            reason: "partial record header at physical tail".to_string(),
            declared_end: None,
            repairable_v5_tail: format == WalFormat::Current,
        }));
    }

    file.seek(SeekFrom::Start(offset))
        .map_err(|error| RecordReadError::Io(error.into()))?;
    let mut base_header = [0_u8; ENTRY_HEADER_SIZE];
    file.read_exact(&mut base_header)
        .map_err(|error| RecordReadError::Io(error.into()))?;
    let length = u32::from_le_bytes(
        base_header[..4]
            .try_into()
            .expect("four-byte record length"),
    ) as u64;
    let v5_tag_published = format == WalFormat::Current
        && base_header[V5_COMMIT_TAG_OFFSET..ENTRY_HEADER_SIZE] == V5_COMMIT_TAG;
    let declared_end = offset
        .checked_add(header_size)
        .and_then(|end| end.checked_add(length));
    let Some(end) = declared_end else {
        return Err(RecordReadError::Damage(RecordFailure {
            reason: "record length overflows the file offset".to_string(),
            declared_end: None,
            repairable_v5_tail: format == WalFormat::Current && !v5_tag_published,
        }));
    };
    if end > file_len {
        return Err(RecordReadError::Damage(RecordFailure {
            reason: "partial record body at physical tail".to_string(),
            // A tag-clear direct write cannot publish a later record before
            // this declared body completes. Retain the end even past EOF so
            // recovery does not search inside a large, terminal partial body.
            declared_end: Some(end),
            repairable_v5_tail: format == WalFormat::Current && !v5_tag_published,
        }));
    }
    if format == WalFormat::Current && !v5_tag_published {
        return Err(RecordReadError::Damage(RecordFailure {
            reason: "v5 record commit tag is absent".to_string(),
            declared_end: Some(end),
            repairable_v5_tail: true,
        }));
    }

    file.seek(SeekFrom::Start(offset))
        .map_err(|error| RecordReadError::Io(error.into()))?;
    let mut bounded = file.take(end - offset);
    match read_entry_versioned(&mut bounded, format, end - offset) {
        Ok(entry) => Ok((entry, end)),
        Err(error) => Err(classify_record_error(error, end, format)),
    }
}

fn classify_record_error(error: WalError, declared_end: u64, format: WalFormat) -> RecordReadError {
    if matches!(error, WalError::Io { .. }) {
        RecordReadError::Io(error)
    } else {
        RecordReadError::Damage(RecordFailure {
            reason: error.to_string(),
            declared_end: Some(declared_end),
            repairable_v5_tail: format != WalFormat::Current,
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SuffixScan {
    Found,
    Exhausted,
    Ambiguous,
}

fn find_valid_record_after(
    file: &mut File,
    start: u64,
    scan_end: u64,
    physical_end: u64,
    format: WalFormat,
    entry_header_size: u64,
) -> Result<SuffixScan> {
    if format == WalFormat::Current {
        return find_valid_v5_record_after(file, start, scan_end, physical_end);
    }

    let Some(last_start) = scan_end.checked_sub(entry_header_size) else {
        return Ok(SuffixScan::Exhausted);
    };
    let mut candidate_count = 0_u64;
    let mut record_byte_budget = MAX_SUFFIX_RECORD_BYTES;
    for candidate in start..=last_start {
        candidate_count += 1;
        if candidate_count > MAX_SUFFIX_CANDIDATE_OFFSETS {
            return Ok(SuffixScan::Ambiguous);
        }

        let Some(record_size) = plausible_record_size(file, candidate, physical_end, format)?
        else {
            continue;
        };
        if record_size > record_byte_budget {
            return Ok(SuffixScan::Ambiguous);
        }
        record_byte_budget -= record_size;
        match read_record_at(file, candidate, physical_end, format) {
            Ok(_) => return Ok(SuffixScan::Found),
            Err(RecordReadError::Damage(_)) => {}
            Err(RecordReadError::Io(error)) => return Err(error),
        }
    }
    Ok(SuffixScan::Exhausted)
}

/// Search a v5 suffix by its two-byte publication tag instead of issuing one
/// seek/read per possible byte offset. This keeps recovery linear in the
/// active segment size while allowing a large unpublished mapped payload to be
/// proven terminal. Structurally plausible headers remain bounded so arbitrary
/// payload tag bytes do not consume the decode budget, while adversarial header
/// candidates still fail closed instead of causing unbounded decode work.
fn find_valid_v5_record_after(
    file: &mut File,
    start: u64,
    scan_end: u64,
    physical_end: u64,
) -> Result<SuffixScan> {
    let Some(mut scan_offset) = start.checked_add(V5_COMMIT_TAG_OFFSET as u64) else {
        return Ok(SuffixScan::Exhausted);
    };
    if scan_end.saturating_sub(scan_offset) < V5_COMMIT_TAG.len() as u64 {
        return Ok(SuffixScan::Exhausted);
    }

    let mut buffer = [0_u8; 16 * 1024];
    let mut previous = None;
    let mut candidate_count = 0_u64;
    let mut record_byte_budget = MAX_SUFFIX_RECORD_BYTES;
    while scan_offset < scan_end {
        let bytes = usize::try_from((scan_end - scan_offset).min(buffer.len() as u64))
            .expect("bounded suffix scan chunk fits usize");
        file.seek(SeekFrom::Start(scan_offset))?;
        file.read_exact(&mut buffer[..bytes])?;

        for (index, byte) in buffer[..bytes].iter().copied().enumerate() {
            let absolute = scan_offset + index as u64;
            if previous == Some(V5_COMMIT_TAG[0]) && byte == V5_COMMIT_TAG[1] {
                let tag_start = absolute - 1;
                let candidate = tag_start - V5_COMMIT_TAG_OFFSET as u64;
                if candidate >= start {
                    let Some(record_size) =
                        plausible_record_size(file, candidate, physical_end, WalFormat::Current)?
                    else {
                        previous = Some(byte);
                        continue;
                    };
                    candidate_count += 1;
                    if candidate_count > MAX_SUFFIX_CANDIDATE_OFFSETS {
                        return Ok(SuffixScan::Ambiguous);
                    }
                    if record_size > record_byte_budget {
                        return Ok(SuffixScan::Ambiguous);
                    }
                    record_byte_budget -= record_size;
                    match read_record_at(file, candidate, physical_end, WalFormat::Current) {
                        Ok(_) => return Ok(SuffixScan::Found),
                        Err(RecordReadError::Damage(_)) => {}
                        Err(RecordReadError::Io(error)) => return Err(error),
                    }
                }
            }
            previous = Some(byte);
        }
        scan_offset += bytes as u64;
    }
    Ok(SuffixScan::Exhausted)
}

fn plausible_record_size(
    file: &mut File,
    offset: u64,
    file_len: u64,
    format: WalFormat,
) -> Result<Option<u64>> {
    file.seek(SeekFrom::Start(offset))?;
    let mut header = [0_u8; ENTRY_HEADER_SIZE];
    file.read_exact(&mut header)?;
    let entry_type = EntryType::try_from(header[ENTRY_TYPE_OFFSET]);
    if entry_type.is_err()
        || matches!(entry_type, Ok(EntryType::Batch)) && !format.supports_batches()
        || header[ENTRY_FLAGS_OFFSET] != 0
        || if format == WalFormat::Current {
            header[ENTRY_RESERVED_START..V5_COMMIT_TAG_OFFSET]
                != [0_u8; V5_COMMIT_TAG_OFFSET - ENTRY_RESERVED_START]
                || header[V5_COMMIT_TAG_OFFSET..ENTRY_HEADER_SIZE] != V5_COMMIT_TAG
        } else {
            header[ENTRY_RESERVED_START..ENTRY_HEADER_SIZE]
                != [0_u8; ENTRY_HEADER_SIZE - ENTRY_RESERVED_START]
        }
    {
        return Ok(None);
    }
    let length = u32::from_le_bytes(header[..4].try_into().expect("four-byte length")) as u64;
    let Some(record_size) = (format.entry_header_size() as u64).checked_add(length) else {
        return Ok(None);
    };
    if record_size > file_len.saturating_sub(offset) {
        return Ok(None);
    }
    Ok(Some(record_size))
}

pub(crate) fn read_and_validate_header(
    reader: &mut (impl Read + Seek),
) -> Result<(WalFormat, u64)> {
    // Released WAL headers carry a zero checksum placeholder. Treat their
    // sequence/count fields as repairable metadata: record checksums and
    // payload structure are authoritative, and headers are rewritten only
    // after the complete database has passed read-only preflight.
    reader.seek(SeekFrom::Start(0))?;
    let mut header = [0_u8; WAL_HEADER_SIZE];
    if let Err(error) = reader.read_exact(&mut header) {
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            return Err(WalError::InvalidFormat(format!(
                "WAL header is truncated: expected {WAL_HEADER_SIZE} bytes"
            )));
        }
        return Err(error.into());
    }
    let mut header = &header[..];
    let mut magic = [0_u8; 8];
    header.read_exact(&mut magic)?;
    if &magic != WAL_MAGIC && &magic != b"HANSHIRO" {
        return Err(WalError::InvalidFormat(
            "Invalid WAL file magic number".to_string(),
        ));
    }

    let version = header.read_u32::<LittleEndian>()?;
    let format = WalFormat::from_version(version)?;
    let _creation_time = header.read_u64::<LittleEndian>()?;
    let first_sequence = header.read_u64::<LittleEndian>()?;
    let _last_sequence = header.read_u64::<LittleEndian>()?;
    let _entry_count = header.read_u64::<LittleEndian>()?;
    let _checksum = header.read_u32::<LittleEndian>()?;
    let mut reserved = [0_u8; 16];
    header.read_exact(&mut reserved)?;
    if format != WalFormat::Current && reserved != [0_u8; 16] {
        return Err(WalError::InvalidFormat(
            "nonzero reserved WAL header bytes".to_string(),
        ));
    }
    Ok((format, first_sequence))
}

fn rewrite_header(file: &mut File, metadata: &SegmentMetadata) -> Result<()> {
    let first = metadata.first_sequence.unwrap_or(metadata.empty_sequence);
    let last = metadata.last_sequence.unwrap_or(metadata.empty_sequence);
    file.seek(SeekFrom::Start(WAL_FIRST_SEQUENCE_OFFSET))?;
    file.write_u64::<LittleEndian>(first)?;
    file.write_u64::<LittleEndian>(last)?;
    file.write_u64::<LittleEndian>(metadata.entry_count)?;
    if metadata.format == WalFormat::Current {
        file.seek(SeekFrom::Start(WAL_ACKNOWLEDGED_END_OFFSET))?;
        file.write_u64::<LittleEndian>(metadata.valid_end)?;
        file.write_u64::<LittleEndian>(acknowledged_end_crc(
            metadata.empty_sequence,
            metadata.valid_end,
        ))?;
    }
    file.seek(SeekFrom::Start(metadata.valid_end))?;
    Ok(())
}

pub(crate) fn synchronize_segment_header(path: &Path, metadata: &SegmentMetadata) -> Result<()> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    rewrite_header(&mut file, metadata)?;
    file.sync_all()?;
    Ok(())
}

/// Update header with final sequence/count before rotation
pub(crate) fn finalize_header(wal_file: &mut WalFile) -> Result<()> {
    // Flush dirty mapped pages before unmapping/truncating. On Windows this is
    // the required FlushViewOfFile half of the mapped-view + FlushFileBuffers
    // durability pair; `sync_all` below supplies the file-handle half.
    wal_file.finish_mapped_extent()?;
    let file = &mut wal_file.file;

    file.seek(SeekFrom::Start(WAL_FIRST_SEQUENCE_OFFSET))?;
    file.write_u64::<LittleEndian>(wal_file.first_sequence)?;
    file.write_u64::<LittleEndian>(wal_file.last_sequence)?;
    file.write_u64::<LittleEndian>(wal_file.entry_count)?;
    if wal_file.format == WalFormat::Current {
        let segment_sequence = wal_sequence_from_path(&wal_file.path).ok_or_else(|| {
            WalError::InvalidFormat(format!(
                "WAL filename is not a numeric sequence: {}",
                wal_file.path.display()
            ))
        })?;
        file.seek(SeekFrom::Start(WAL_ACKNOWLEDGED_END_OFFSET))?;
        file.write_u64::<LittleEndian>(wal_file.size)?;
        file.write_u64::<LittleEndian>(acknowledged_end_crc(segment_sequence, wal_file.size))?;
    }
    file.sync_all()?;
    file.seek(SeekFrom::Start(wal_file.size))?;

    Ok(())
}

pub(crate) fn v5_record_crc_fields(
    payload_length: u32,
    sequence: u64,
    timestamp: u64,
    entry_type: EntryType,
    flags: u8,
    data: &[u8],
) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&payload_length.to_le_bytes());
    hasher.update(&sequence.to_le_bytes());
    hasher.update(&timestamp.to_le_bytes());
    hasher.update(&[entry_type as u8, flags]);
    hasher.update(data);
    hasher.finalize()
}

/// Write a single entry in the current v5 physical format.
#[allow(dead_code)]
pub(crate) fn write_entry(writer: &mut impl Write, entry: &WalEntry) -> Result<()> {
    let payload_length = checked_record_payload_length(entry.data.len())?;
    writer.write_u32::<LittleEndian>(payload_length)?;
    writer.write_u64::<LittleEndian>(entry.sequence)?;
    writer.write_u64::<LittleEndian>(entry.timestamp)?;
    writer.write_u8(entry.entry_type as u8)?;
    writer.write_u8(0)?; // Flags
    writer.write_u32::<LittleEndian>(v5_record_crc_fields(
        payload_length,
        entry.sequence,
        entry.timestamp,
        entry.entry_type,
        0,
        &entry.data,
    ))?;
    writer.write_all(&[0_u8; V5_COMMIT_TAG_OFFSET - ENTRY_RESERVED_START])?;
    writer.write_all(&V5_COMMIT_TAG)?;
    writer.write_all(&entry.data)?;

    Ok(())
}

/// Read a single entry with version-aware format.
///
/// Versions 1 and 2 contain a 96-byte extension between the header and payload;
/// versions 3 through 5 do not. The extension is ignored during migration reads.
pub(crate) fn read_entry_versioned(
    reader: &mut impl Read,
    format: WalFormat,
    available_bytes: u64,
) -> Result<WalEntry> {
    let mut length_bytes = [0_u8; 4];
    match reader.read(&mut length_bytes[..1]) {
        Ok(0) => return Err(WalError::Eof),
        Ok(_) => {}
        Err(error) => return Err(error.into()),
    }
    reader.read_exact(&mut length_bytes[1..])?;
    let length = u32::from_le_bytes(length_bytes) as usize;
    let record_size = format
        .entry_header_size()
        .checked_add(length)
        .ok_or_else(|| WalError::InvalidFormat("record length overflows".to_string()))?;
    if record_size as u64 > available_bytes {
        return Err(WalError::InvalidFormat(format!(
            "record declares {record_size} bytes with only {available_bytes} remaining"
        )));
    }

    let sequence = reader.read_u64::<LittleEndian>()?;
    let timestamp = reader.read_u64::<LittleEndian>()?;
    let entry_type = EntryType::try_from(reader.read_u8()?)?;
    if entry_type == EntryType::Batch && !format.supports_batches() {
        return Err(WalError::InvalidFormat(
            "batch record appears in a WAL version without batch support".to_string(),
        ));
    }
    let flags = reader.read_u8()?;
    if flags != 0 {
        return Err(WalError::InvalidFormat(format!(
            "unsupported entry flags: {flags}"
        )));
    }
    let crc = reader.read_u32::<LittleEndian>()?;
    let mut reserved = [0_u8; ENTRY_RESERVED_SIZE];
    reader.read_exact(&mut reserved)?;
    let reserved_valid = if format == WalFormat::Current {
        reserved[..V5_COMMIT_TAG_OFFSET - ENTRY_RESERVED_START]
            == [0_u8; V5_COMMIT_TAG_OFFSET - ENTRY_RESERVED_START]
            && reserved[V5_COMMIT_TAG_OFFSET - ENTRY_RESERVED_START..] == V5_COMMIT_TAG
    } else {
        reserved == [0_u8; ENTRY_RESERVED_SIZE]
    };
    if !reserved_valid {
        return Err(WalError::InvalidFormat(
            "nonzero reserved entry header bytes".to_string(),
        ));
    }

    if format.has_legacy_extension() {
        reader.read_exact(&mut [0_u8; LEGACY_ENTRY_EXTENSION_SIZE])?;
    }

    let mut data = vec![0u8; length];
    reader.read_exact(&mut data)?;

    // Verify CRC
    let computed_crc = if format == WalFormat::Current {
        v5_record_crc_fields(length as u32, sequence, timestamp, entry_type, flags, &data)
    } else {
        crc32_checksum(&data)
    };
    if computed_crc != crc {
        return Err(WalError::CrcMismatch);
    }
    validate_payload(entry_type, &data)?;

    let entry = WalEntry {
        sequence,
        timestamp,
        entry_type,
        data: Bytes::from(data),
    };
    entry.sequence_bounds()?;
    Ok(entry)
}

fn validate_payload(entry_type: EntryType, data: &[u8]) -> Result<()> {
    if entry_type == EntryType::Batch {
        return validate_batch(data);
    }
    if !matches!(entry_type, EntryType::Data | EntryType::Delete) {
        return Ok(());
    }
    let key_length_bytes: [u8; 4] = data
        .get(..4)
        .ok_or_else(|| WalError::InvalidFormat("entry payload has no key length".to_string()))?
        .try_into()
        .expect("four-byte key length");
    let key_length = u32::from_le_bytes(key_length_bytes) as usize;
    let key_end = 4_usize
        .checked_add(key_length)
        .ok_or_else(|| WalError::InvalidFormat("entry key length overflows".to_string()))?;
    if key_end > data.len() {
        return Err(WalError::InvalidFormat(
            "entry key length exceeds its payload".to_string(),
        ));
    }
    if entry_type == EntryType::Delete && key_end != data.len() {
        return Err(WalError::InvalidFormat(
            "delete entry contains trailing value bytes".to_string(),
        ));
    }
    Ok(())
}

/// Calculate the size of an entry on disk (v3-v5 physical format).
pub(crate) fn entry_size(entry: &WalEntry) -> usize {
    ENTRY_HEADER_SIZE + entry.data.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Seek, SeekFrom, Write};
    use tempfile::TempDir;

    fn entry(sequence: u64) -> WalEntry {
        WalEntry {
            sequence,
            timestamp: 0,
            entry_type: EntryType::Data,
            data: Bytes::from(encode_kv(b"key", b"value")),
        }
    }

    fn encoded_entry(sequence: u64) -> Vec<u8> {
        let mut encoded = Vec::new();
        write_entry(&mut encoded, &entry(sequence)).unwrap();
        encoded
    }

    fn batch_entry(sequence: u64) -> WalEntry {
        WalEntry {
            sequence,
            timestamp: 0,
            entry_type: EntryType::Batch,
            data: Bytes::from(
                encode_batch(&[
                    (b"first".as_slice(), Some(b"one".as_slice())),
                    (b"second".as_slice(), None),
                    (b"third".as_slice(), Some(b"three".as_slice())),
                ])
                .unwrap(),
            ),
        }
    }

    fn encoded_batch_entry(sequence: u64) -> Vec<u8> {
        let mut encoded = Vec::new();
        write_entry(&mut encoded, &batch_entry(sequence)).unwrap();
        encoded
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

    fn invalidate_acknowledged_end(file: &mut File) {
        file.seek(SeekFrom::Start(WAL_ACKNOWLEDGED_END_CRC_OFFSET))
            .unwrap();
        let checksum = file.read_u64::<LittleEndian>().unwrap();
        file.seek(SeekFrom::Start(WAL_ACKNOWLEDGED_END_CRC_OFFSET))
            .unwrap();
        file.write_u64::<LittleEndian>(checksum ^ 1).unwrap();
    }

    fn append_current(wal_file: &mut WalFile, entry: &WalEntry, maximum_size: u64) -> u64 {
        let mut encoded = Vec::new();
        write_entry(&mut encoded, entry).unwrap();
        let start = wal_file.size;
        wal_file
            .write_current_record(&encoded, maximum_size)
            .unwrap();
        let (first, last) = entry.sequence_bounds().unwrap();
        wal_file
            .record_append(encoded.len() as u64, last - first + 1, first, last)
            .unwrap();
        start
    }

    #[test]
    fn wal_sequence_requires_the_writer_canonical_filename() {
        assert_eq!(
            wal_sequence_from_path(Path::new("00000000000000000042.wal")),
            Some(42)
        );
        assert_eq!(wal_sequence_from_path(Path::new("1.wal")), None);
        assert_eq!(
            wal_sequence_from_path(Path::new("0000000000000000001.wal")),
            None
        );
        assert_eq!(
            wal_sequence_from_path(Path::new("000000000000000000001.wal")),
            None
        );
        assert_eq!(
            wal_sequence_from_path(Path::new("00000000000000000042.tmp")),
            None
        );
        assert_eq!(
            temporary_wal_sequence_from_path(Path::new("00000000000000000042.wal.tmp")),
            Some(42)
        );
        assert_eq!(
            temporary_wal_sequence_from_path(Path::new("42.wal.tmp")),
            None
        );
        assert_eq!(
            wal_sequence_from_path(Path::new("not-a-wal-segment.wal")),
            None
        );
        assert_eq!(
            wal_sequence_from_path(Path::new("18446744073709551616.wal")),
            None
        );
    }

    #[test]
    fn reads_legacy_entry_extension_without_exposing_it() {
        let payload = encode_kv(b"key", b"value");
        let mut bytes = Vec::new();
        bytes
            .write_u32::<LittleEndian>(payload.len() as u32)
            .unwrap();
        bytes.write_u64::<LittleEndian>(7).unwrap();
        bytes.write_u64::<LittleEndian>(11).unwrap();
        bytes.write_u8(EntryType::Data as u8).unwrap();
        bytes.write_u8(0).unwrap();
        bytes
            .write_u32::<LittleEndian>(crc32_checksum(&payload))
            .unwrap();
        bytes.extend_from_slice(&[0_u8; ENTRY_RESERVED_SIZE]);
        bytes.extend_from_slice(&[0xAA; LEGACY_ENTRY_EXTENSION_SIZE]);
        bytes.extend_from_slice(&payload);

        let entry =
            read_entry_versioned(&mut bytes.as_slice(), WalFormat::Legacy, bytes.len() as u64)
                .unwrap();
        assert_eq!(entry.sequence, 7);
        assert_eq!(entry.data.as_ref(), payload);
    }

    #[test]
    fn recovery_uses_maximum_sequence_when_physical_order_differs() {
        let directory = TempDir::new().unwrap();
        let config = WalConfig::durable();
        let mut file = create_file(directory.path(), 3, &config).unwrap();

        for sequence in [5, 3] {
            let entry = WalEntry {
                sequence,
                timestamp: 0,
                entry_type: EntryType::Data,
                data: Bytes::from(encode_kv(b"key", b"value")),
            };
            write_entry(&mut file.file, &entry).unwrap();
        }
        let path = file.path.clone();
        drop(file);

        let (recovered, next_sequence) = recover_file(&path, &config).unwrap();
        assert_eq!(recovered.first_sequence, 3);
        assert_eq!(recovered.last_sequence, 5);
        assert_eq!(recovered.entry_count, 2);
        assert_eq!(next_sequence, 6);
        assert_eq!(header_bounds(&path), (3, 5, 2));
    }

    #[test]
    fn recovery_truncates_only_damage_confined_to_the_physical_tail() {
        let complete = encoded_entry(1);
        let mut partial_body = complete[..complete.len() - 1].to_vec();
        partial_body[V5_COMMIT_TAG_OFFSET..V5_COMMIT_TAG_OFFSET + V5_COMMIT_TAG.len()].fill(0);
        let cases = [
            ("partial-length", complete[..2].to_vec()),
            ("partial-header", complete[..20].to_vec()),
            ("partial-body", partial_body),
        ];

        for (name, damaged_tail) in cases {
            let directory = TempDir::new().unwrap();
            let config = WalConfig::durable();
            let mut wal_file = create_file(directory.path(), 0, &config).unwrap();
            write_entry(&mut wal_file.file, &entry(0)).unwrap();
            let valid_end = wal_file.file.stream_position().unwrap();
            wal_file.file.write_all(&damaged_tail).unwrap();
            let path = wal_file.path.clone();
            drop(wal_file);

            let (recovered, next_sequence) =
                recover_file(&path, &config).unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(recovered.size, valid_end, "{name}");
            assert_eq!(
                recovered.file.metadata().unwrap().len(),
                valid_end,
                "{name}"
            );
            assert_eq!(recovered.entry_count, 1, "{name}");
            assert_eq!(next_sequence, 1, "{name}");
            assert_eq!(header_bounds(&path), (0, 0, 1), "{name}");
        }
    }

    #[test]
    fn recovery_discards_every_logical_operation_from_a_torn_batch_envelope() {
        let complete = encoded_batch_entry(1);
        let mut partial_body = complete[..complete.len() - 1].to_vec();
        partial_body[V5_COMMIT_TAG_OFFSET..V5_COMMIT_TAG_OFFSET + V5_COMMIT_TAG.len()].fill(0);
        let cases = [
            ("partial-header", complete[..20].to_vec()),
            ("partial-body", partial_body),
        ];

        for (name, damaged_batch) in cases {
            let directory = TempDir::new().unwrap();
            let config = WalConfig::durable();
            let mut wal_file = create_file(directory.path(), 0, &config).unwrap();
            write_entry(&mut wal_file.file, &entry(0)).unwrap();
            let valid_end = wal_file.file.stream_position().unwrap();
            wal_file.file.write_all(&damaged_batch).unwrap();
            let path = wal_file.path.clone();
            drop(wal_file);

            let (recovered, next_sequence) =
                recover_file(&path, &config).unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(recovered.size, valid_end, "{name}");
            assert_eq!(recovered.entry_count, 1, "{name}");
            assert_eq!(next_sequence, 1, "{name}");
            assert_eq!(header_bounds(&path), (0, 0, 1), "{name}");
        }
    }

    #[test]
    fn segment_metadata_counts_the_complete_logical_batch_sequence_span() {
        let directory = TempDir::new().unwrap();
        let config = WalConfig::durable();
        let mut wal_file = create_file(directory.path(), 0, &config).unwrap();
        write_entry(&mut wal_file.file, &entry(0)).unwrap();
        write_entry(&mut wal_file.file, &batch_entry(1)).unwrap();
        let path = wal_file.path.clone();
        drop(wal_file);

        let (recovered, next_sequence) = recover_file(&path, &config).unwrap();
        assert_eq!((recovered.first_sequence, recovered.last_sequence), (0, 3));
        assert_eq!(recovered.entry_count, 4);
        assert_eq!(next_sequence, 4);
        assert_eq!(header_bounds(&path), (0, 3, 4));
    }

    #[test]
    fn recovery_rejects_a_corrupt_complete_record_before_more_bytes() {
        for corruption in ["checksum", "length", "flags"] {
            let directory = TempDir::new().unwrap();
            let config = WalConfig::durable();
            let mut wal_file = create_file(directory.path(), 0, &config).unwrap();
            write_entry(&mut wal_file.file, &entry(0)).unwrap();
            let corrupt_offset = wal_file.file.stream_position().unwrap();
            write_entry(&mut wal_file.file, &entry(1)).unwrap();
            write_entry(&mut wal_file.file, &entry(2)).unwrap();
            let path = wal_file.path.clone();

            wal_file
                .file
                .seek(SeekFrom::Start(match corruption {
                    "length" => corrupt_offset,
                    "flags" => corrupt_offset + ENTRY_FLAGS_OFFSET as u64,
                    _ => corrupt_offset + ENTRY_CRC_OFFSET,
                }))
                .unwrap();
            match corruption {
                "length" => wal_file.file.write_all(&u32::MAX.to_le_bytes()).unwrap(),
                "flags" => wal_file.file.write_all(&[1]).unwrap(),
                _ => {
                    let mut byte = [0_u8; 1];
                    wal_file.file.read_exact(&mut byte).unwrap();
                    wal_file.file.seek(SeekFrom::Current(-1)).unwrap();
                    wal_file.file.write_all(&[byte[0] ^ 0x80]).unwrap();
                }
            }
            drop(wal_file);

            let error = recover_file(&path, &config).unwrap_err();
            assert!(matches!(error, WalError::Corruption(_)));
            assert!(path.metadata().unwrap().len() > corrupt_offset);
        }
    }

    #[test]
    fn non_active_segment_tail_damage_is_explicit_corruption() {
        let directory = TempDir::new().unwrap();
        let config = WalConfig::durable();
        let mut wal_file = create_file(directory.path(), 0, &config).unwrap();
        write_entry(&mut wal_file.file, &entry(0)).unwrap();
        wal_file.file.write_all(&[1, 2, 3]).unwrap();
        let path = wal_file.path.clone();
        drop(wal_file);

        let error = inspect_segment(&path, false).unwrap_err();
        assert!(matches!(error, WalError::Corruption(_)));
    }

    #[test]
    fn tag_dense_unpublished_payload_is_repaired_without_spending_candidate_budget() {
        let directory = TempDir::new().unwrap();
        let config = WalConfig::durable();
        let mut wal_file = create_file(directory.path(), 0, &config).unwrap();
        append_current(&mut wal_file, &entry(0), config.max_file_size);
        let valid_end = wal_file.size;
        wal_file.file.seek(SeekFrom::Start(valid_end)).unwrap();
        wal_file.file.write_all(&[0_u8; ENTRY_HEADER_SIZE]).unwrap();
        wal_file
            .file
            .write_all(&V5_COMMIT_TAG.repeat(MAX_SUFFIX_CANDIDATE_OFFSETS as usize + 1))
            .unwrap();
        let path = wal_file.path.clone();
        drop(wal_file);

        let (recovered, next) = recover_file(&path, &config).unwrap();
        assert_eq!(recovered.size, valid_end);
        assert_eq!(next, 1);
        assert_eq!(path.metadata().unwrap().len(), valid_end);
    }

    #[test]
    fn acknowledged_boundary_repairs_unpublished_payload_containing_valid_record_bytes() {
        let directory = TempDir::new().unwrap();
        let config = WalConfig::durable();
        let mut wal_file = create_file(directory.path(), 0, &config).unwrap();
        append_current(&mut wal_file, &entry(0), config.max_file_size);
        let valid_end = wal_file.size;
        wal_file.file.seek(SeekFrom::Start(valid_end)).unwrap();
        wal_file.file.write_all(&[0_u8; ENTRY_HEADER_SIZE]).unwrap();
        wal_file.file.write_all(&encoded_entry(1)).unwrap();
        let path = wal_file.path.clone();
        drop(wal_file);

        let (recovered, next) = recover_file(&path, &config).unwrap();
        assert_eq!(recovered.size, valid_end);
        assert_eq!(next, 1);
        assert_eq!(path.metadata().unwrap().len(), valid_end);
    }

    #[test]
    fn plausible_header_budget_exhaustion_is_not_truncated() {
        let directory = TempDir::new().unwrap();
        let config = WalConfig::durable();
        let mut wal_file = create_file(directory.path(), 0, &config).unwrap();
        append_current(&mut wal_file, &entry(0), config.max_file_size);
        wal_file.file.seek(SeekFrom::Start(wal_file.size)).unwrap();
        wal_file.file.write_all(&[0_u8; ENTRY_HEADER_SIZE]).unwrap();
        let mut corrupt_record = encoded_entry(1);
        corrupt_record[ENTRY_CRC_OFFSET as usize] ^= 0x80;
        for _ in 0..=MAX_SUFFIX_CANDIDATE_OFFSETS {
            wal_file.file.write_all(&corrupt_record).unwrap();
        }
        invalidate_acknowledged_end(&mut wal_file.file);
        let path = wal_file.path.clone();
        let original_len = wal_file.file.metadata().unwrap().len();
        drop(wal_file);

        let error = recover_file(&path, &config).unwrap_err();
        assert!(
            matches!(error, WalError::Corruption(message) if message.contains("recovery budget"))
        );
        assert_eq!(path.metadata().unwrap().len(), original_len);
    }

    #[test]
    fn generic_io_errors_are_never_classified_as_repairable_damage() {
        let io_error = WalError::Io {
            message: "injected read failure".to_string(),
            source: Some(std::io::Error::other("injected read failure")),
        };
        assert!(matches!(
            classify_record_error(io_error, 100, WalFormat::Current),
            RecordReadError::Io(_)
        ));
        assert!(matches!(
            classify_record_error(WalError::CrcMismatch, 100, WalFormat::Current),
            RecordReadError::Damage(RecordFailure {
                declared_end: Some(100),
                ..
            })
        ));
    }

    #[test]
    fn every_v5_direct_body_cut_and_partial_commit_tag_repairs_to_the_prior_record() {
        let torn = encoded_entry(1);
        let mut unpublished = torn.clone();
        unpublished[V5_COMMIT_TAG_OFFSET..V5_COMMIT_TAG_OFFSET + V5_COMMIT_TAG.len()].fill(0);
        let mut cuts: Vec<Vec<u8>> = (0..=unpublished.len())
            .map(|cut| unpublished[..cut].to_vec())
            .collect();
        let mut one_tag_byte = unpublished;
        one_tag_byte[V5_COMMIT_TAG_OFFSET] = V5_COMMIT_TAG[0];
        cuts.push(one_tag_byte);

        for (cut, bytes) in cuts.into_iter().enumerate() {
            let directory = TempDir::new().unwrap();
            let config = WalConfig::paranoid();
            let mut wal_file = create_file(directory.path(), 0, &config).unwrap();
            append_current(&mut wal_file, &entry(0), config.max_file_size);
            let valid_end = wal_file.size;
            wal_file.file.seek(SeekFrom::Start(valid_end)).unwrap();
            wal_file.file.write_all(&bytes).unwrap();
            let path = wal_file.path.clone();
            drop(wal_file);

            let (recovered, next) = recover_file(&path, &config)
                .unwrap_or_else(|error| panic!("direct cut {cut}: {error}"));
            assert_eq!(recovered.size, valid_end, "direct cut {cut}");
            assert_eq!(recovered.entry_count, 1, "direct cut {cut}");
            assert_eq!(next, 1, "direct cut {cut}");
        }
    }

    #[test]
    fn large_v5_direct_partial_body_repairs_without_scanning_inside_the_record() {
        let directory = TempDir::new().unwrap();
        let config = WalConfig::paranoid();
        let mut wal_file = create_file(directory.path(), 0, &config).unwrap();
        append_current(&mut wal_file, &entry(0), config.max_file_size);
        let valid_end = wal_file.size;

        let large = WalEntry {
            sequence: 1,
            timestamp: 0,
            entry_type: EntryType::Data,
            data: Bytes::from(encode_kv(
                b"large",
                &vec![0x5a; MAX_SUFFIX_CANDIDATE_OFFSETS as usize + 1024],
            )),
        };
        let mut unpublished = Vec::new();
        write_entry(&mut unpublished, &large).unwrap();
        unpublished[V5_COMMIT_TAG_OFFSET..V5_COMMIT_TAG_OFFSET + V5_COMMIT_TAG.len()].fill(0);
        let partial_end = ENTRY_HEADER_SIZE + MAX_SUFFIX_CANDIDATE_OFFSETS as usize + 1;
        wal_file.file.seek(SeekFrom::Start(valid_end)).unwrap();
        wal_file
            .file
            .write_all(&unpublished[..partial_end])
            .unwrap();
        let path = wal_file.path.clone();
        drop(wal_file);

        let (recovered, next) = recover_file(&path, &config).unwrap();
        assert_eq!(recovered.size, valid_end);
        assert_eq!(recovered.entry_count, 1);
        assert_eq!(next, 1);
    }

    #[test]
    fn large_unpublished_mapped_payload_after_a_zero_header_repairs_to_the_prior_record() {
        let directory = TempDir::new().unwrap();
        let config = WalConfig::durable();
        let mut wal_file = create_file(directory.path(), 0, &config).unwrap();
        append_current(&mut wal_file, &entry(0), config.max_file_size);
        let valid_end = wal_file.size;
        wal_file.file.seek(SeekFrom::Start(valid_end)).unwrap();
        wal_file.file.write_all(&[0_u8; ENTRY_HEADER_SIZE]).unwrap();
        wal_file
            .file
            .write_all(&vec![0_u8; MAX_SUFFIX_CANDIDATE_OFFSETS as usize + 1])
            .unwrap();
        wal_file
            .file
            .write_all(b"partially-observable-mapped-payload")
            .unwrap();
        let path = wal_file.path.clone();
        drop(wal_file);

        let (recovered, next) = recover_file(&path, &config).unwrap();
        assert_eq!(recovered.size, valid_end);
        assert_eq!(recovered.entry_count, 1);
        assert_eq!(next, 1);
        assert_eq!(path.metadata().unwrap().len(), valid_end);
    }

    #[test]
    fn zeroed_v5_header_before_a_later_committed_record_is_not_repaired() {
        let directory = TempDir::new().unwrap();
        let config = WalConfig::durable();
        let mut wal_file = create_file(directory.path(), 0, &config).unwrap();
        append_current(&mut wal_file, &entry(0), config.max_file_size);
        let corrupt_start = wal_file.size;
        wal_file.file.seek(SeekFrom::Start(corrupt_start)).unwrap();
        wal_file.file.write_all(&[0_u8; ENTRY_HEADER_SIZE]).unwrap();
        wal_file.file.write_all(&encoded_entry(1)).unwrap();
        invalidate_acknowledged_end(&mut wal_file.file);
        let original_len = wal_file.file.metadata().unwrap().len();
        let path = wal_file.path.clone();
        drop(wal_file);

        let error = recover_file(&path, &config).unwrap_err();
        assert!(
            matches!(error, WalError::Corruption(message) if message.contains("plausible committed suffix"))
        );
        assert_eq!(path.metadata().unwrap().len(), original_len);
    }

    #[test]
    fn committed_suffix_with_trailing_zero_value_is_never_truncated() {
        let directory = TempDir::new().unwrap();
        let config = WalConfig::durable();
        let mut wal_file = create_file(directory.path(), 0, &config).unwrap();
        append_current(&mut wal_file, &entry(0), config.max_file_size);
        let corrupt_start = wal_file.size;
        let later = WalEntry {
            sequence: 1,
            timestamp: 0,
            entry_type: EntryType::Data,
            data: Bytes::from(encode_kv(b"later", &[0_u8; 256])),
        };
        let mut committed = Vec::new();
        write_entry(&mut committed, &later).unwrap();
        assert_eq!(committed.last(), Some(&0));
        wal_file.file.seek(SeekFrom::Start(corrupt_start)).unwrap();
        wal_file.file.write_all(&[0_u8; ENTRY_HEADER_SIZE]).unwrap();
        wal_file.file.write_all(&committed).unwrap();
        invalidate_acknowledged_end(&mut wal_file.file);
        let original_len = wal_file.file.metadata().unwrap().len();
        let path = wal_file.path.clone();
        drop(wal_file);

        let error = recover_file(&path, &config).unwrap_err();
        assert!(
            matches!(error, WalError::Corruption(message) if message.contains("plausible committed suffix"))
        );
        assert_eq!(path.metadata().unwrap().len(), original_len);
    }

    #[test]
    fn stale_or_partially_published_v5_cursor_never_hides_a_committed_record() {
        let committed = encoded_entry(1);
        for cursor_prefix in 0..=16 {
            let directory = TempDir::new().unwrap();
            let config = WalConfig::paranoid();
            let mut wal_file = create_file(directory.path(), 0, &config).unwrap();
            append_current(&mut wal_file, &entry(0), config.max_file_size);
            let committed_end = wal_file.size + committed.len() as u64;
            wal_file.file.seek(SeekFrom::Start(wal_file.size)).unwrap();
            wal_file.file.write_all(&committed).unwrap();

            let segment_sequence = wal_sequence_from_path(&wal_file.path).unwrap();
            let mut cursor = Vec::with_capacity(16);
            cursor.extend_from_slice(&committed_end.to_le_bytes());
            cursor.extend_from_slice(
                &acknowledged_end_crc(segment_sequence, committed_end).to_le_bytes(),
            );
            wal_file
                .file
                .seek(SeekFrom::Start(WAL_ACKNOWLEDGED_END_OFFSET))
                .unwrap();
            wal_file.file.write_all(&cursor[..cursor_prefix]).unwrap();
            let path = wal_file.path.clone();
            drop(wal_file);

            let (recovered, next) = recover_file(&path, &config)
                .unwrap_or_else(|error| panic!("cursor prefix {cursor_prefix}: {error}"));
            assert_eq!(recovered.entry_count, 2, "cursor prefix {cursor_prefix}");
            assert_eq!(
                recovered.size, committed_end,
                "cursor prefix {cursor_prefix}"
            );
            assert_eq!(next, 2, "cursor prefix {cursor_prefix}");
        }
    }

    #[test]
    fn every_committed_v5_record_byte_is_covered_by_crc_or_structure_validation() {
        let encoded = encoded_entry(1);
        for byte in 0..encoded.len() {
            let directory = TempDir::new().unwrap();
            let config = WalConfig::paranoid();
            let mut wal_file = create_file(directory.path(), 0, &config).unwrap();
            append_current(&mut wal_file, &entry(0), config.max_file_size);
            let corrupt_start = append_current(&mut wal_file, &entry(1), config.max_file_size);
            wal_file
                .file
                .seek(SeekFrom::Start(corrupt_start + byte as u64))
                .unwrap();
            wal_file.file.write_all(&[encoded[byte] ^ 1]).unwrap();
            let path = wal_file.path.clone();
            drop(wal_file);

            assert!(
                matches!(recover_file(&path, &config), Err(WalError::Corruption(_))),
                "committed byte {byte} was not rejected"
            );
        }
    }

    #[test]
    fn direct_fallback_and_reopened_mapped_path_preserve_v5_recovery() {
        let directory = TempDir::new().unwrap();
        let config = WalConfig {
            max_file_size: 1024 * 1024,
            ..WalConfig::durable()
        };
        let mut wal_file = create_file(directory.path(), 0, &config).unwrap();
        assert!(wal_file.mapping.is_some());
        assert_eq!(
            wal_file.allocation_capacity_for_test(),
            config.max_file_size
        );
        wal_file.disable_mapping_for_test().unwrap();
        assert!(wal_file.mapping.is_none());
        append_current(&mut wal_file, &entry(0), config.max_file_size);
        let path = wal_file.path.clone();
        finalize_header(&mut wal_file).unwrap();
        drop(wal_file);

        let metadata = inspect_segment(&path, true).unwrap();
        let (mut reopened, next) = open_recovered_file(&path, &metadata).unwrap();
        assert_eq!(next, 1);
        reopened.enable_mapped_writes(config.max_file_size).unwrap();
        assert!(reopened.mapping.is_some());
        assert!(reopened.allocation_capacity_for_test() <= config.max_file_size);
        append_current(&mut reopened, &entry(1), config.max_file_size);
        finalize_header(&mut reopened).unwrap();
        drop(reopened);

        let final_metadata = inspect_segment(&path, false).unwrap();
        assert_eq!(final_metadata.entry_count, 2);
        assert_eq!(final_metadata.last_sequence, Some(1));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_mapped_capacity_has_physical_blocks_reserved() {
        use std::os::unix::fs::MetadataExt;

        let directory = TempDir::new().unwrap();
        let config = WalConfig {
            max_file_size: 1024 * 1024,
            ..WalConfig::durable()
        };
        let wal_file = create_file(directory.path(), 0, &config).unwrap();
        let metadata = wal_file.file.metadata().unwrap();
        let allocated_bytes = metadata.blocks().saturating_mul(512);
        assert_eq!(metadata.len(), config.max_file_size);
        assert!(
            allocated_bytes >= config.max_file_size,
            "filesystem reports only {allocated_bytes} allocated bytes for {} bytes of mapped capacity",
            config.max_file_size
        );
    }

    #[test]
    fn failed_segment_construction_removes_its_filename_before_retry() {
        let directory = TempDir::new().unwrap();
        let failed_config = WalConfig {
            max_file_size: INJECT_CREATE_FAILURE_MAX_SIZE,
            ..WalConfig::durable()
        };
        let path = directory.path().join("00000000000000000007.wal");
        let temporary_path = temporary_wal_path(directory.path(), 7);
        let error = create_file(directory.path(), 7, &failed_config).unwrap_err();
        assert!(matches!(error, WalError::Io { .. }));
        assert!(!path.exists());
        assert!(!temporary_path.exists());

        let retried = create_file(directory.path(), 7, &WalConfig::paranoid()).unwrap();
        assert_eq!(retried.path, path);
    }

    #[test]
    fn segment_header_install_crash_child() {
        let Some(directory) = std::env::var_os(CREATE_CRASH_DIRECTORY) else {
            return;
        };
        let sequence = std::env::var(CREATE_CRASH_SEQUENCE)
            .expect("create-crash child has a sequence")
            .parse::<u64>()
            .expect("create-crash sequence is numeric");
        let _ = create_file(Path::new(&directory), sequence, &WalConfig::paranoid());
        panic!("create-crash child returned instead of terminating");
    }

    #[test]
    fn every_partial_segment_header_crash_leaves_no_final_name_and_retries() {
        use std::process::Command;

        let directory = TempDir::new().unwrap();
        let config = WalConfig::paranoid();
        let mut previous = create_file(directory.path(), 0, &config).unwrap();
        append_current(&mut previous, &entry(0), config.max_file_size);
        finalize_header(&mut previous).unwrap();
        let previous_path = previous.path.clone();
        drop(previous);

        for cut in 0..=WAL_HEADER_SIZE {
            let sequence = cut as u64 + 1;
            let final_path = directory.path().join(format!("{sequence:020}.wal"));
            let temporary_path = temporary_wal_path(directory.path(), sequence);
            let status = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "storage::wal::file::tests::segment_header_install_crash_child",
                    "--nocapture",
                ])
                .env(CREATE_CRASH_DIRECTORY, directory.path())
                .env(CREATE_CRASH_SEQUENCE, sequence.to_string())
                .env(CREATE_CRASH_HEADER_BYTES, cut.to_string())
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(CREATE_CRASH_EXIT_CODE), "cut {cut}");
            assert!(!final_path.exists(), "cut {cut}");
            assert!(temporary_path.exists(), "cut {cut}");

            let retried = create_file(directory.path(), sequence, &config).unwrap();
            assert_eq!(retried.path, final_path, "cut {cut}");
            assert!(!temporary_path.exists(), "cut {cut}");
            assert_eq!(
                preflight_active_segment(&final_path).unwrap().valid_end,
                WAL_HEADER_SIZE as u64,
                "cut {cut}"
            );
        }

        assert_eq!(
            inspect_segment(&previous_path, false).unwrap().entry_count,
            1
        );
    }

    #[test]
    fn mapped_growth_flushes_the_old_view_before_remapping() {
        let directory = TempDir::new().unwrap();
        let config = WalConfig {
            max_file_size: 2 * MAPPED_GROWTH_BYTES,
            ..WalConfig::durable()
        };
        let mut wal_file = create_file(directory.path(), 0, &config).unwrap();
        assert_eq!(wal_file.allocation_capacity_for_test(), MAPPED_GROWTH_BYTES);
        assert_eq!(wal_file.growth_view_flushes_for_test(), 0);

        assert!(wal_file
            .ensure_mapped_capacity(MAPPED_GROWTH_BYTES, config.max_file_size)
            .unwrap());
        assert_eq!(
            wal_file.allocation_capacity_for_test(),
            2 * MAPPED_GROWTH_BYTES
        );
        assert_eq!(wal_file.growth_view_flushes_for_test(), 1);
        finalize_header(&mut wal_file).unwrap();
        assert_eq!(
            wal_file.file.metadata().unwrap().len(),
            WAL_HEADER_SIZE as u64
        );
    }
}
