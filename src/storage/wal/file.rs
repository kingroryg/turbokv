//! WAL file handling for TurboKV
//!
//! ## Write path
//!
//! - `write_entries_batch` preallocates one buffer and writes a group at once.
//! - Durable-mode `File` writes bypass a userspace `BufWriter` but still use the
//!   operating-system page cache; TurboKV does not use `O_DIRECT`.
//!
//! ## WAL Entry Format (v4)
//!
//! Entry header: 32 bytes
//! - length: u32 (4 bytes)
//! - sequence: u64 (8 bytes)
//! - timestamp: u64 (8 bytes)
//! - entry_type: u8 (1 byte)
//! - flags: u8 (1 byte)
//! - crc: u32 (4 bytes)
//! - reserved: 6 bytes
//! Payload: variable length

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use bytes::Bytes;

use crate::core::crypto::crc32_checksum;

use super::types::*;

const MAX_SUFFIX_CANDIDATE_OFFSETS: u64 = 64 * 1024;
const MAX_SUFFIX_RECORD_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WalFormat {
    Legacy,
    V3,
    Current,
}

impl WalFormat {
    fn from_version(version: u32) -> Result<Self> {
        match version {
            WAL_VERSION_V1 | WAL_VERSION_V2 => Ok(Self::Legacy),
            WAL_VERSION_V3 => Ok(Self::V3),
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
        self == Self::Current
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
    ) {
        debug_assert!(count > 0);
        if self.entry_count == 0 {
            self.first_sequence = first_sequence;
            self.last_sequence = last_sequence;
        } else {
            self.first_sequence = self.first_sequence.min(first_sequence);
            self.last_sequence = self.last_sequence.max(last_sequence);
        }
        self.size += bytes;
        self.entry_count += count;
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
    let filename = path.file_name()?.to_str()?;
    let stem = filename.strip_suffix(".wal")?;
    if stem.len() != 20 || !stem.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }

    let sequence = stem.parse::<u64>().ok()?;
    (filename == format!("{sequence:020}.wal")).then_some(sequence)
}

/// Create a new WAL file
pub(crate) fn create_file(wal_dir: &Path, sequence: u64, _config: &WalConfig) -> Result<WalFile> {
    let filename = format!("{:020}.wal", sequence);
    let path = wal_dir.join(&filename);

    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .read(true)
        .open(&path)?;

    let mut file = file;

    // Write header
    file.write_all(WAL_MAGIC)?;
    file.write_u32::<LittleEndian>(WAL_VERSION)?;
    file.write_u64::<LittleEndian>(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    )?;
    file.write_u64::<LittleEndian>(sequence)?; // First sequence
    file.write_u64::<LittleEndian>(sequence)?; // Last sequence (updated on finalize)
    file.write_u64::<LittleEndian>(0)?; // Entry count
    file.write_u32::<LittleEndian>(0)?; // Checksum placeholder
    file.write_all(&[0u8; 16])?; // Reserved

    Ok(WalFile {
        path,
        file,
        size: WAL_HEADER_SIZE as u64,
        entry_count: 0,
        first_sequence: sequence,
        last_sequence: sequence,
    })
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
                let has_following_bytes = failure.declared_end.is_some_and(|end| end < file_len);
                let suffix = if has_following_bytes {
                    SuffixScan::Found
                } else {
                    find_valid_record_after(
                        &mut file,
                        offset.saturating_add(1),
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

                file.set_len(metadata.valid_end)?;
                file.seek(SeekFrom::Start(metadata.valid_end))?;
                rewrite_header(&mut file, &metadata)?;
                file.sync_all()?;
                return Ok(metadata);
            }
        }
    }

    Ok(metadata)
}

struct RecordFailure {
    reason: String,
    declared_end: Option<u64>,
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
        }));
    }

    file.seek(SeekFrom::Start(offset))
        .map_err(|error| RecordReadError::Io(error.into()))?;
    let length = file
        .read_u32::<LittleEndian>()
        .map_err(|error| RecordReadError::Io(error.into()))? as u64;
    let declared_end = offset
        .checked_add(header_size)
        .and_then(|end| end.checked_add(length));
    let Some(end) = declared_end else {
        return Err(RecordReadError::Damage(RecordFailure {
            reason: "record length overflows the file offset".to_string(),
            declared_end: None,
        }));
    };
    if end > file_len {
        return Err(RecordReadError::Damage(RecordFailure {
            reason: "partial record body at physical tail".to_string(),
            declared_end: None,
        }));
    }

    file.seek(SeekFrom::Start(offset))
        .map_err(|error| RecordReadError::Io(error.into()))?;
    let mut bounded = file.take(end - offset);
    match read_entry_versioned(&mut bounded, format, end - offset) {
        Ok(entry) => Ok((entry, end)),
        Err(error) => Err(classify_record_error(error, end)),
    }
}

fn classify_record_error(error: WalError, declared_end: u64) -> RecordReadError {
    if matches!(error, WalError::Io { .. }) {
        RecordReadError::Io(error)
    } else {
        RecordReadError::Damage(RecordFailure {
            reason: error.to_string(),
            declared_end: Some(declared_end),
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
    file_len: u64,
    format: WalFormat,
    entry_header_size: u64,
) -> Result<SuffixScan> {
    let Some(last_start) = file_len.checked_sub(entry_header_size) else {
        return Ok(SuffixScan::Exhausted);
    };
    let mut candidate_count = 0_u64;
    let mut record_byte_budget = MAX_SUFFIX_RECORD_BYTES;
    for candidate in start..=last_start {
        candidate_count += 1;
        if candidate_count > MAX_SUFFIX_CANDIDATE_OFFSETS {
            return Ok(SuffixScan::Ambiguous);
        }

        let Some(record_size) = plausible_record_size(file, candidate, file_len, format)? else {
            continue;
        };
        if record_size > record_byte_budget {
            return Ok(SuffixScan::Ambiguous);
        }
        record_byte_budget -= record_size;
        match read_record_at(file, candidate, file_len, format) {
            Ok(_) => return Ok(SuffixScan::Found),
            Err(RecordReadError::Damage(_)) => {}
            Err(RecordReadError::Io(error)) => return Err(error),
        }
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
        || header[ENTRY_RESERVED_START..ENTRY_HEADER_SIZE]
            != [0_u8; ENTRY_HEADER_SIZE - ENTRY_RESERVED_START]
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
    if reserved != [0_u8; 16] {
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
    let file = &mut wal_file.file;

    file.seek(SeekFrom::Start(WAL_FIRST_SEQUENCE_OFFSET))?;
    file.write_u64::<LittleEndian>(wal_file.first_sequence)?;
    file.write_u64::<LittleEndian>(wal_file.last_sequence)?;
    file.write_u64::<LittleEndian>(wal_file.entry_count)?;
    file.sync_all()?;
    file.seek(SeekFrom::End(0))?;

    Ok(())
}

/// Write a single entry to the WAL (v3/v4 physical format).
#[allow(dead_code)]
pub(crate) fn write_entry(writer: &mut impl Write, entry: &WalEntry) -> Result<()> {
    writer.write_u32::<LittleEndian>(entry.data.len() as u32)?;
    writer.write_u64::<LittleEndian>(entry.sequence)?;
    writer.write_u64::<LittleEndian>(entry.timestamp)?;
    writer.write_u8(entry.entry_type as u8)?;
    writer.write_u8(0)?; // Flags
    writer.write_u32::<LittleEndian>(crc32_checksum(&entry.data))?;
    writer.write_all(&[0_u8; ENTRY_RESERVED_SIZE])?;
    writer.write_all(&entry.data)?;

    Ok(())
}

/// Read a single entry with version-aware format.
///
/// Versions 1 and 2 contain a 96-byte extension between the header and payload;
/// versions 3 and 4 do not. The extension is ignored during migration reads.
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
    if reserved != [0_u8; ENTRY_RESERVED_SIZE] {
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
    if crc32_checksum(&data) != crc {
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

/// Calculate the size of an entry on disk (v3/v4 physical format).
pub(crate) fn entry_size(entry: &WalEntry) -> usize {
    ENTRY_HEADER_SIZE + entry.data.len()
}

/// **OPTIMIZED** - Batch write that minimizes syscalls
///
/// Pre-allocates a buffer for all entries and writes them in a single syscall.
/// This is a critical optimization for high-throughput workloads.
pub(crate) fn write_entries_batch<T: AsRef<WalEntry>>(
    writer: &mut impl Write,
    entries: &[T],
) -> Result<()> {
    // Pre-allocate buffer for all entries
    let total_size: usize = entries.iter().map(|e| entry_size(e.as_ref())).sum();
    let mut buffer = Vec::with_capacity(total_size);

    for entry in entries {
        let entry = entry.as_ref();

        // Entry header (32 bytes)
        buffer.extend_from_slice(&(entry.data.len() as u32).to_le_bytes());
        buffer.extend_from_slice(&entry.sequence.to_le_bytes());
        buffer.extend_from_slice(&entry.timestamp.to_le_bytes());
        buffer.push(entry.entry_type as u8);
        buffer.push(0); // Flags
        buffer.extend_from_slice(&crc32_checksum(&entry.data).to_le_bytes());
        buffer.extend_from_slice(&[0_u8; ENTRY_RESERVED_SIZE]);

        // Payload
        buffer.extend_from_slice(&entry.data);
    }

    // One buffered write_all call for the segment chunk. write_all retries
    // short writes so every encoded entry reaches the file before syncing.
    writer.write_all(&buffer)?;
    Ok(())
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
        let cases = [
            ("partial-length", complete[..2].to_vec()),
            ("partial-header", complete[..20].to_vec()),
            ("partial-body", complete[..complete.len() - 1].to_vec()),
            ("checksum", {
                let mut bytes = complete.clone();
                bytes[ENTRY_CRC_OFFSET as usize] ^= 0x80;
                bytes
            }),
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
        let cases = [
            ("partial-header", complete[..20].to_vec()),
            ("partial-body", complete[..complete.len() - 1].to_vec()),
            ("checksum", {
                let mut bytes = complete.clone();
                bytes[ENTRY_CRC_OFFSET as usize] ^= 0x80;
                bytes
            }),
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
    fn ambiguous_tail_beyond_suffix_budget_is_not_truncated() {
        let directory = TempDir::new().unwrap();
        let config = WalConfig::durable();
        let mut wal_file = create_file(directory.path(), 0, &config).unwrap();
        write_entry(&mut wal_file.file, &entry(0)).unwrap();
        wal_file.file.write_all(&u32::MAX.to_le_bytes()).unwrap();
        wal_file
            .file
            .write_all(&vec![
                0_u8;
                MAX_SUFFIX_CANDIDATE_OFFSETS as usize
                    + ENTRY_HEADER_SIZE
                    + 1
            ])
            .unwrap();
        let path = wal_file.path.clone();
        let original_len = wal_file.file.metadata().unwrap().len();
        drop(wal_file);

        let error = recover_file(&path, &config).unwrap_err();
        assert!(matches!(error, WalError::Corruption(_)));
        assert_eq!(path.metadata().unwrap().len(), original_len);
    }

    #[test]
    fn generic_io_errors_are_never_classified_as_repairable_damage() {
        let io_error = WalError::Io {
            message: "injected read failure".to_string(),
            source: Some(std::io::Error::other("injected read failure")),
        };
        assert!(matches!(
            classify_record_error(io_error, 100),
            RecordReadError::Io(_)
        ));
        assert!(matches!(
            classify_record_error(WalError::CrcMismatch, 100),
            RecordReadError::Damage(RecordFailure {
                declared_end: Some(100),
                ..
            })
        ));
    }
}
