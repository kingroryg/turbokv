//! WAL file handling for TurboKV
//!
//! ## Key Optimizations (PRESERVED)
//!
//! - **write_entries_batch()** - Vectorized batch write that pre-allocates buffer
//!   and writes all entries in a single syscall
//! - **BufWriter::with_capacity()** - Configurable write buffering
//!
//! ## WAL Entry Format (v3)
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
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use bytes::Bytes;
use tracing::info;

use crate::core::crypto::crc32_checksum;

use super::types::*;

/// In-memory representation of an open WAL file
pub(crate) struct WalFile {
    pub path: PathBuf,
    pub file: BufWriter<File>,
    pub size: u64,
    pub entry_count: u64,
    #[allow(dead_code)]
    pub first_sequence: u64,
    pub last_sequence: u64,
}

/// Create a new WAL file
pub(crate) fn create_file(wal_dir: &Path, sequence: u64, config: &WalConfig) -> Result<WalFile> {
    let filename = format!("{:020}.wal", sequence);
    let path = wal_dir.join(&filename);

    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .open(&path)?;

    let mut writer = BufWriter::with_capacity(config.buffer_size, file);

    // Write header
    writer.write_all(WAL_MAGIC)?;
    writer.write_u32::<LittleEndian>(WAL_VERSION)?;
    writer.write_u64::<LittleEndian>(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    )?;
    writer.write_u64::<LittleEndian>(sequence)?; // First sequence
    writer.write_u64::<LittleEndian>(sequence)?; // Last sequence (updated on finalize)
    writer.write_u64::<LittleEndian>(0)?; // Entry count
    writer.write_u32::<LittleEndian>(0)?; // Checksum placeholder
    writer.write_all(&[0u8; 16])?; // Reserved
    writer.flush()?;

    Ok(WalFile {
        path,
        file: writer,
        size: WAL_HEADER_SIZE as u64,
        entry_count: 0,
        first_sequence: sequence,
        last_sequence: sequence,
    })
}

/// Recover a WAL file, returning the file handle and last sequence
pub(crate) fn recover_file(
    path: &Path,
    config: &WalConfig,
) -> Result<(WalFile, u64)> {
    info!("Recovering from WAL file: {:?}", path);

    let file = OpenOptions::new().read(true).write(true).open(path)?;
    let mut reader = BufReader::new(file);

    // Validate header magic
    let mut magic = [0u8; 8];
    reader.read_exact(&mut magic)?;
    if &magic != WAL_MAGIC {
        // Try legacy magic for backward compatibility
        if &magic != b"HANSHIRO" {
            return Err(WalError::InvalidFormat(
                "Invalid WAL file magic number".to_string(),
            ));
        }
    }

    let version = reader.read_u32::<LittleEndian>()?;
    // Support v1, v2 (legacy with Merkle), and v3 (current, no Merkle)
    if version != WAL_VERSION && version != 1 && version != 2 {
        return Err(WalError::InvalidFormat(format!(
            "Unsupported WAL version: {}",
            version
        )));
    }

    let _creation_time = reader.read_u64::<LittleEndian>()?;
    let first_sequence = reader.read_u64::<LittleEndian>()?;
    let mut last_sequence = reader.read_u64::<LittleEndian>()?;
    let entry_count = reader.read_u64::<LittleEndian>()?;
    let _checksum = reader.read_u32::<LittleEndian>()?;
    reader.read_exact(&mut [0u8; 16])?;

    // Read entries to find last sequence
    // Use version to determine format (v2 has Merkle bytes, v3 doesn't)
    let has_merkle = version <= 2;
    loop {
        match read_entry_versioned(&mut reader, has_merkle) {
            Ok(entry) => {
                last_sequence = entry.sequence;
            }
            Err(WalError::Eof) => break,
            Err(_) => break,
        }
    }

    let file_size = reader.seek(SeekFrom::End(0))?;
    let mut file = reader.into_inner();
    file.seek(SeekFrom::End(0))?;
    let writer = BufWriter::with_capacity(config.buffer_size, file);

    Ok((
        WalFile {
            path: path.to_path_buf(),
            file: writer,
            size: file_size,
            entry_count,
            first_sequence,
            last_sequence,
        },
        last_sequence + 1,
    ))
}

/// Update header with final sequence/count before rotation
pub(crate) fn finalize_header(wal_file: &mut WalFile) -> Result<()> {
    wal_file.file.flush()?;
    let file = wal_file.file.get_mut();

    file.seek(SeekFrom::Start(28))?; // Offset of last_sequence
    file.write_u64::<LittleEndian>(wal_file.last_sequence)?;
    file.write_u64::<LittleEndian>(wal_file.entry_count)?;
    file.sync_all()?;
    file.seek(SeekFrom::End(0))?;

    Ok(())
}

/// Read the last sequence from a WAL file header
pub(crate) fn read_header_last_sequence(path: &Path) -> Result<u64> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(28))?;
    Ok(file.read_u64::<LittleEndian>()?)
}

/// Write a single entry to the WAL (v3 format - no Merkle)
#[allow(dead_code)]
pub(crate) fn write_entry(writer: &mut impl Write, entry: &WalEntry) -> Result<()> {
    writer.write_u32::<LittleEndian>(entry.data.len() as u32)?;
    writer.write_u64::<LittleEndian>(entry.sequence)?;
    writer.write_u64::<LittleEndian>(entry.timestamp)?;
    writer.write_u8(entry.entry_type as u8)?;
    writer.write_u8(0)?; // Flags
    writer.write_u32::<LittleEndian>(crc32_checksum(&entry.data))?;
    writer.write_all(&[0u8; 6])?; // Reserved
    writer.write_all(&entry.data)?;

    Ok(())
}

/// Read a single entry from the WAL (v3 format - no Merkle)
pub(crate) fn read_entry(reader: &mut impl Read) -> Result<WalEntry> {
    read_entry_versioned(reader, false)
}

/// Read a single entry with version-aware format
/// has_merkle: true for v1/v2 format (96 bytes Merkle), false for v3
pub(crate) fn read_entry_versioned(reader: &mut impl Read, has_merkle: bool) -> Result<WalEntry> {
    let length = match reader.read_u32::<LittleEndian>() {
        Ok(len) => len as usize,
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Err(WalError::Eof);
        }
        Err(e) => return Err(e.into()),
    };

    let sequence = reader.read_u64::<LittleEndian>()?;
    let timestamp = reader.read_u64::<LittleEndian>()?;
    let entry_type = EntryType::try_from(reader.read_u8()?)?;
    let _flags = reader.read_u8()?;
    let crc = reader.read_u32::<LittleEndian>()?;
    reader.read_exact(&mut [0u8; 6])?;

    // Skip Merkle bytes if reading legacy format
    if has_merkle {
        reader.read_exact(&mut [0u8; 96])?;
    }

    let mut data = vec![0u8; length];
    reader.read_exact(&mut data)?;

    // Verify CRC
    if crc32_checksum(&data) != crc {
        return Err(WalError::CrcMismatch);
    }

    Ok(WalEntry {
        sequence,
        timestamp,
        entry_type,
        data: Bytes::from(data),
    })
}

/// Calculate the size of an entry on disk (v3 format - no Merkle)
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
        buffer.extend_from_slice(&[0u8; 6]); // Reserved

        // Payload (no Merkle overhead in v3)
        buffer.extend_from_slice(&entry.data);
    }

    // Single syscall for all entries
    writer.write_all(&buffer)?;
    Ok(())
}
