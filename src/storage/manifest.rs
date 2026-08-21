//! The manifest is the durable authority for:
//! - All SSTables and their metadata
//! - WAL checkpoint (first sequence that recovery must replay)
//! - Manifest format version for compatibility
//!
//! Current manifest file format (v3):
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    MANIFEST File                            │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Magic: "HNSHMNFT" (8 bytes)                                │
//! │  Version: u32                                               │
//! │  WAL Checkpoint: u64 (next replay sequence)                 │
//! │  SSTable Count: u32                                         │
//! │  Entries: id, level, path, sizes/counts, key/sequence bounds│
//! │  Checksum: u32 (CRC32)                                      │
//! └─────────────────────────────────────────────────────────────┘
//! ```

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::core::crypto::crc32_checksum;
use crate::core::error::{Error, Result};

const MANIFEST_MAGIC: &[u8; 8] = b"HNSHMNFT";
/// Stable identifier for the released manifest layout with a checkpoint
/// extension and without per-table tombstone counts.
pub const MANIFEST_VERSION_V1: u32 = 1;
/// Stable identifier for the manifest layout that removed the checkpoint
/// extension.
pub const MANIFEST_VERSION_V2: u32 = 2;
/// Stable identifier written by this release.
///
/// Readers accept versions 1 through 3. A validated v1/v2 manifest is migrated
/// atomically to v3 only after every referenced SSTable and retained WAL
/// segment has passed a read-only startup preflight. SSTables remain readable
/// in place and WAL migration starts a new current-format segment.
pub const MANIFEST_VERSION: u32 = 3;

/// Database manifest - tracks persistent state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// On-disk format version loaded, or the current format for a new manifest.
    pub loaded_format_version: u64,
    /// First WAL sequence not known to be represented by installed SSTables.
    /// On recovery, replay WAL entries greater than or equal to this sequence.
    pub wal_checkpoint: u64,
    /// All SSTable metadata
    pub sstables: Vec<SSTableManifestEntry>,
}

/// SSTable metadata stored in manifest
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableManifestEntry {
    /// Engine-wide table identifier.
    pub id: u64,
    /// LSM level containing the table.
    pub level: u32,
    /// Table path, resolved against the database directory when relative.
    pub path: PathBuf,
    /// Encoded file length in bytes.
    pub size: u64,
    /// Number of physical key versions in the table.
    pub entry_count: u64,
    /// Number of tombstone versions in the table.
    pub tombstone_count: u64,
    /// Smallest raw key, or empty for an empty table.
    pub min_key: Vec<u8>,
    /// Largest raw key, or empty for an empty table.
    pub max_key: Vec<u8>,
    /// Smallest engine sequence represented by the table.
    pub min_sequence: u64,
    /// Largest engine sequence represented by the table.
    pub max_sequence: u64,
    /// Unix timestamp in seconds recorded when the table was finished.
    pub creation_time: u64,
}

impl Manifest {
    /// Create new empty manifest
    pub fn new() -> Self {
        Self {
            loaded_format_version: u64::from(MANIFEST_VERSION),
            wal_checkpoint: 0,
            sstables: Vec::new(),
        }
    }

    /// Load `MANIFEST` from `data_dir`, or return a new in-memory manifest.
    ///
    /// A missing file is not created by this function. Existing bytes are read
    /// synchronously, fully allocated in memory, checksummed, and structurally
    /// validated before a value is returned.
    pub fn load_or_create(data_dir: &Path) -> Result<Self> {
        let manifest_path = data_dir.join("MANIFEST");

        if manifest_path.exists() {
            Self::load(&manifest_path)
        } else {
            info!("No manifest found, creating new database");
            Ok(Self::new())
        }
    }

    /// Synchronously read, allocate, checksum, and decode a manifest file.
    pub fn load(path: &Path) -> Result<Self> {
        // Read entire file into memory for checksum verification
        let file = File::open(path).map_err(|e| Error::Io {
            message: format!("Failed to open manifest: {:?}", path),
            source: e,
        })?;
        let mut reader = BufReader::new(file);
        let mut file_data = Vec::new();
        reader.read_to_end(&mut file_data)?;

        if file_data.len() < 12 {
            return Err(manifest_corruption("file is shorter than its header"));
        }

        if &file_data[..8] != MANIFEST_MAGIC {
            return Err(manifest_corruption("invalid magic number"));
        }
        let version = u32::from_le_bytes(
            file_data[8..12]
                .try_into()
                .expect("manifest version slice is exactly four bytes"),
        );
        if !matches!(
            version,
            MANIFEST_VERSION_V1 | MANIFEST_VERSION_V2 | MANIFEST_VERSION
        ) {
            return Err(Error::Internal {
                message: format!("Unsupported manifest version: {version}"),
            });
        }

        if file_data.len() < 16 {
            return Err(manifest_corruption("file is missing its checksum"));
        }

        // Verify checksum: last 4 bytes are the CRC32, rest is the payload
        let (payload, checksum_bytes) = file_data.split_at(file_data.len() - 4);
        let stored_checksum = u32::from_le_bytes(
            checksum_bytes
                .try_into()
                .expect("checksum slice is exactly 4 bytes"),
        );
        let computed_checksum = crc32_checksum(payload);
        // v0.2.0 and v0.2.1 wrote a zero placeholder without changing the v1
        // identifier. That exact legacy spelling remains readable; all other
        // manifests, including later v1 files, require the checksum to match.
        // A placeholder manifest has no payload authentication; its magic,
        // lengths, count, UTF-8 path encoding, and exact end are still checked.
        let released_v1_placeholder = version == MANIFEST_VERSION_V1 && stored_checksum == 0;
        if !released_v1_placeholder && stored_checksum != computed_checksum {
            return Err(Error::Internal {
                message: format!(
                    "Manifest checksum mismatch: stored={:#010x}, computed={:#010x}",
                    stored_checksum, computed_checksum
                ),
            });
        }

        // Now parse the payload
        let mut reader = std::io::Cursor::new(payload);

        // The fixed header was checked before the checksum so a future format
        // is classified as unsupported even though its checksum scheme is not
        // known to this reader.
        reader.set_position(12);

        // Read WAL checkpoint
        let wal_checkpoint = read_manifest_u64(&mut reader, "WAL checkpoint")?;

        if version == MANIFEST_VERSION_V1 {
            let extension_len = read_manifest_u32(&mut reader, "checkpoint extension length")?;
            skip_manifest_bytes(&mut reader, extension_len as usize, "checkpoint extension")?;
        }

        // Read SSTable count
        let sstable_count = read_manifest_u32(&mut reader, "SSTable count")? as usize;
        let minimum_entry_size = if version == MANIFEST_VERSION { 72 } else { 64 };
        if sstable_count > remaining_manifest_bytes(&reader) / minimum_entry_size {
            return Err(manifest_corruption(
                "SSTable count exceeds the remaining manifest payload",
            ));
        }

        // Read SSTable entries
        let mut sstables = Vec::with_capacity(sstable_count);
        for _ in 0..sstable_count {
            let entry = Self::read_sstable_entry(&mut reader, version)?;
            sstables.push(entry);
        }

        if reader.position() as usize != payload.len() {
            return Err(manifest_corruption(
                "trailing bytes remain after the declared SSTable entries",
            ));
        }

        info!(
            "Loaded manifest: version={}, wal_checkpoint={}, sstables={}",
            version,
            wal_checkpoint,
            sstables.len()
        );

        Ok(Self {
            loaded_format_version: u64::from(version),
            wal_checkpoint,
            sstables,
        })
    }

    /// Durably replace the manifest through a temporary file.
    ///
    /// The complete encoded manifest is allocated in memory, written and
    /// synced, atomically renamed, and followed by a directory sync where the
    /// platform exposes one. Failure can occur after the rename; callers that
    /// need to distinguish that outcome must reload and compare the manifest.
    pub fn save(&self, data_dir: &Path) -> Result<()> {
        let manifest_path = data_dir.join("MANIFEST");
        let temp_path = data_dir.join("MANIFEST.tmp");

        // Write all content to a buffer, compute CRC32, then write to file
        {
            let mut buf: Vec<u8> = Vec::new();

            // Write magic
            buf.write_all(MANIFEST_MAGIC)?;

            // Write version
            buf.write_u32::<LittleEndian>(MANIFEST_VERSION)?;

            // Write WAL checkpoint
            buf.write_u64::<LittleEndian>(self.wal_checkpoint)?;

            // Write SSTable count
            buf.write_u32::<LittleEndian>(self.sstables.len() as u32)?;

            // Write SSTable entries
            for entry in &self.sstables {
                Self::write_sstable_entry(&mut buf, entry, MANIFEST_VERSION)?;
            }

            // Compute CRC32 over all content and append it
            let checksum = crc32_checksum(&buf);
            buf.write_u32::<LittleEndian>(checksum)?;

            // Write the complete buffer to the temp file
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&temp_path)?;
            let mut writer = BufWriter::new(file);
            writer.write_all(&buf)?;
            writer.flush()?;

            // Fsync the temp file before rename to ensure data is durable
            let file = writer.into_inner().map_err(|e| e.into_error())?;
            file.sync_all()?;
        }

        atomic_replace(&temp_path, &manifest_path)?;
        #[cfg(test)]
        super::failpoints::check(
            data_dir,
            super::failpoints::PersistenceBoundary::ManifestDirectorySync,
        )
        .map_err(|error| crate::core::Error::Internal {
            message: error.to_string(),
        })?;
        sync_directory(data_dir)?;

        info!(
            "Saved manifest: wal_checkpoint={}, sstables={}",
            self.wal_checkpoint,
            self.sstables.len()
        );

        Ok(())
    }

    /// Persist an already validated legacy manifest in the current format.
    pub(crate) fn persist_format_upgrade(&mut self, data_dir: &Path) -> Result<()> {
        self.loaded_format_version = u64::from(MANIFEST_VERSION);
        self.save(data_dir)
    }

    /// Update the first WAL sequence that recovery must replay.
    ///
    /// This mutates only the in-memory value; call [`Self::save`] to persist it.
    pub fn update_checkpoint(&mut self, sequence: u64) {
        self.wal_checkpoint = sequence;
    }

    /// Append an SSTable to this in-memory manifest.
    pub fn add_sstable(&mut self, entry: SSTableManifestEntry) {
        self.sstables.push(entry);
    }

    /// Remove matching SSTable identifiers from this in-memory manifest.
    pub fn remove_sstables(&mut self, ids: &[u64]) {
        self.sstables.retain(|e| !ids.contains(&e.id));
    }

    fn read_sstable_entry(
        reader: &mut std::io::Cursor<&[u8]>,
        version: u32,
    ) -> Result<SSTableManifestEntry> {
        let id = read_manifest_u64(reader, "SSTable id")?;
        let level = read_manifest_u32(reader, "SSTable level")?;

        // Read path
        let path_bytes = read_manifest_bytes(reader, "SSTable path")?;
        let path = PathBuf::from(
            String::from_utf8(path_bytes)
                .map_err(|_| manifest_corruption("SSTable path is not valid UTF-8"))?,
        );

        let size = read_manifest_u64(reader, "SSTable size")?;
        let entry_count = read_manifest_u64(reader, "SSTable entry count")?;
        let tombstone_count = if version >= MANIFEST_VERSION {
            read_manifest_u64(reader, "SSTable tombstone count")?
        } else {
            0
        };

        // Read min_key
        let min_key = read_manifest_bytes(reader, "minimum key")?;

        // Read max_key
        let max_key = read_manifest_bytes(reader, "maximum key")?;

        let min_sequence = read_manifest_u64(reader, "minimum sequence")?;
        let max_sequence = read_manifest_u64(reader, "maximum sequence")?;
        let creation_time = read_manifest_u64(reader, "creation time")?;

        Ok(SSTableManifestEntry {
            id,
            level,
            path,
            size,
            entry_count,
            tombstone_count,
            min_key,
            max_key,
            min_sequence,
            max_sequence,
            creation_time,
        })
    }

    fn write_sstable_entry(
        writer: &mut impl Write,
        entry: &SSTableManifestEntry,
        version: u32,
    ) -> Result<()> {
        writer.write_u64::<LittleEndian>(entry.id)?;
        writer.write_u32::<LittleEndian>(entry.level)?;

        // Write path
        let path_str = entry.path.to_string_lossy();
        writer.write_u32::<LittleEndian>(path_str.len() as u32)?;
        writer.write_all(path_str.as_bytes())?;

        writer.write_u64::<LittleEndian>(entry.size)?;
        writer.write_u64::<LittleEndian>(entry.entry_count)?;
        if version >= MANIFEST_VERSION {
            writer.write_u64::<LittleEndian>(entry.tombstone_count)?;
        }

        // Write min_key
        writer.write_u32::<LittleEndian>(entry.min_key.len() as u32)?;
        writer.write_all(&entry.min_key)?;

        // Write max_key
        writer.write_u32::<LittleEndian>(entry.max_key.len() as u32)?;
        writer.write_all(&entry.max_key)?;

        writer.write_u64::<LittleEndian>(entry.min_sequence)?;
        writer.write_u64::<LittleEndian>(entry.max_sequence)?;
        writer.write_u64::<LittleEndian>(entry.creation_time)?;

        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn save_legacy_for_test(&self, data_dir: &Path, version: u32) -> Result<()> {
        assert!(version == MANIFEST_VERSION_V1 || version == MANIFEST_VERSION_V2);
        let mut buffer = Vec::new();
        buffer.write_all(MANIFEST_MAGIC)?;
        buffer.write_u32::<LittleEndian>(version)?;
        buffer.write_u64::<LittleEndian>(self.wal_checkpoint)?;
        if version == MANIFEST_VERSION_V1 {
            buffer.write_u32::<LittleEndian>(0)?;
        }
        buffer.write_u32::<LittleEndian>(self.sstables.len() as u32)?;
        for entry in &self.sstables {
            Self::write_sstable_entry(&mut buffer, entry, version)?;
        }
        let checksum = crc32_checksum(&buffer);
        buffer.write_u32::<LittleEndian>(checksum)?;
        std::fs::write(data_dir.join("MANIFEST"), buffer)?;
        Ok(())
    }
}

fn manifest_corruption(message: &str) -> Error {
    Error::Internal {
        message: format!("Manifest corruption: {message}"),
    }
}

fn remaining_manifest_bytes(reader: &std::io::Cursor<&[u8]>) -> usize {
    reader
        .get_ref()
        .len()
        .saturating_sub(reader.position() as usize)
}

fn read_manifest_u32(reader: &mut std::io::Cursor<&[u8]>, field: &str) -> Result<u32> {
    if remaining_manifest_bytes(reader) < std::mem::size_of::<u32>() {
        return Err(manifest_corruption(&format!("truncated {field}")));
    }
    reader
        .read_u32::<LittleEndian>()
        .map_err(|_| manifest_corruption(&format!("truncated {field}")))
}

fn read_manifest_u64(reader: &mut std::io::Cursor<&[u8]>, field: &str) -> Result<u64> {
    if remaining_manifest_bytes(reader) < std::mem::size_of::<u64>() {
        return Err(manifest_corruption(&format!("truncated {field}")));
    }
    reader
        .read_u64::<LittleEndian>()
        .map_err(|_| manifest_corruption(&format!("truncated {field}")))
}

fn read_manifest_bytes(reader: &mut std::io::Cursor<&[u8]>, field: &str) -> Result<Vec<u8>> {
    let length = read_manifest_u32(reader, &format!("{field} length"))? as usize;
    if length > remaining_manifest_bytes(reader) {
        return Err(manifest_corruption(&format!(
            "{field} length exceeds the remaining payload"
        )));
    }
    let start = reader.position() as usize;
    let end = start + length;
    reader.set_position(end as u64);
    Ok(reader.get_ref()[start..end].to_vec())
}

fn skip_manifest_bytes(
    reader: &mut std::io::Cursor<&[u8]>,
    length: usize,
    field: &str,
) -> Result<()> {
    if length > remaining_manifest_bytes(reader) {
        return Err(manifest_corruption(&format!(
            "{field} length exceeds the remaining payload"
        )));
    }
    reader.set_position(reader.position() + length as u64);
    Ok(())
}

#[cfg(not(windows))]
pub(crate) fn atomic_replace(source: &Path, destination: &Path) -> std::io::Result<()> {
    std::fs::rename(source, destination)
}

#[cfg(windows)]
pub(crate) fn atomic_replace(source: &Path, destination: &Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;

    use windows_sys::Win32::Storage::FileSystem::{
        MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
    };

    let source: Vec<u16> = source.as_os_str().encode_wide().chain(Some(0)).collect();
    let destination: Vec<u16> = destination
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect();
    // SAFETY: both vectors are valid, NUL-terminated UTF-16 paths and remain
    // alive for the duration of the system call.
    let result = unsafe {
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if result == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// Persist a directory entry update where the platform exposes directory fsync.
#[cfg(unix)]
pub(crate) fn sync_directory(path: &Path) -> std::io::Result<()> {
    File::open(path)?.sync_all()
}

/// Rust has no portable directory-sync operation on these targets. Atomic
/// replacement and file fsync still preserve the strongest available ordering.
#[cfg(not(unix))]
pub(crate) fn sync_directory(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

impl Default for Manifest {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, RngCore, SeedableRng};
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;

    use crate::storage::test_support::stress_context;

    fn wait_for_manifest_reader(
        reads: &AtomicU64,
        previous_reads: u64,
        failures: &Mutex<Vec<String>>,
        context: &str,
    ) -> u64 {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            let observed = reads.load(Ordering::Acquire);
            if observed > previous_reads {
                return observed;
            }
            let failures = failures.lock().unwrap();
            assert!(
                failures.is_empty(),
                "{context}: reader failed: {failures:?}"
            );
            drop(failures);
            assert!(
                std::time::Instant::now() < deadline,
                "{context}: reader did not observe a replacement"
            );
            std::thread::yield_now();
        }
    }

    #[test]
    fn test_manifest_save_load() {
        let temp_dir = TempDir::new().unwrap();

        let mut manifest = Manifest::new();
        manifest.wal_checkpoint = 12345;
        manifest.add_sstable(SSTableManifestEntry {
            id: 1,
            level: 0,
            path: PathBuf::from("/data/sstables/1.sst"),
            size: 1024,
            entry_count: 100,
            tombstone_count: 7,
            min_key: vec![0, 1, 2],
            max_key: vec![9, 9, 9],
            min_sequence: 0,
            max_sequence: 99,
            creation_time: 1234567890,
        });

        // Save
        manifest.save(temp_dir.path()).unwrap();

        // Load
        let loaded = Manifest::load_or_create(temp_dir.path()).unwrap();

        assert_eq!(loaded.wal_checkpoint, 12345);
        assert_eq!(loaded.sstables.len(), 1);
        assert_eq!(loaded.sstables[0].id, 1);
        assert_eq!(loaded.sstables[0].entry_count, 100);
        assert_eq!(loaded.sstables[0].tombstone_count, 7);
    }

    #[test]
    fn test_manifest_new_database() {
        let temp_dir = TempDir::new().unwrap();

        let manifest = Manifest::load_or_create(temp_dir.path()).unwrap();

        assert_eq!(manifest.wal_checkpoint, 0);
        assert!(manifest.sstables.is_empty());
    }

    #[test]
    fn current_manifest_does_not_write_legacy_extension() {
        let temp_dir = TempDir::new().unwrap();
        Manifest::new().save(temp_dir.path()).unwrap();

        let bytes = std::fs::read(temp_dir.path().join("MANIFEST")).unwrap();
        assert_eq!(u32::from_le_bytes(bytes[8..12].try_into().unwrap()), 3);
        assert_eq!(u32::from_le_bytes(bytes[20..24].try_into().unwrap()), 0);
        assert_eq!(bytes.len(), 28);
    }

    #[test]
    fn loads_v1_manifest_with_legacy_extension() {
        let temp_dir = TempDir::new().unwrap();
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MANIFEST_MAGIC);
        bytes
            .write_u32::<LittleEndian>(MANIFEST_VERSION_V1)
            .unwrap();
        bytes.write_u64::<LittleEndian>(42).unwrap();
        bytes.write_u32::<LittleEndian>(6).unwrap();
        bytes.extend_from_slice(b"legacy");
        bytes.write_u32::<LittleEndian>(0).unwrap();
        let checksum = crc32_checksum(&bytes);
        bytes.write_u32::<LittleEndian>(checksum).unwrap();
        std::fs::write(temp_dir.path().join("MANIFEST"), bytes).unwrap();

        let manifest = Manifest::load_or_create(temp_dir.path()).unwrap();
        assert_eq!(manifest.wal_checkpoint, 42);
        assert!(manifest.sstables.is_empty());
    }

    #[test]
    fn loads_v2_manifest_without_tombstone_counts() {
        let temp_dir = TempDir::new().unwrap();
        let mut manifest = Manifest::new();
        manifest.add_sstable(SSTableManifestEntry {
            id: 9,
            level: 0,
            path: PathBuf::from("legacy.sst"),
            size: 2048,
            entry_count: 3,
            tombstone_count: 2,
            min_key: b"a".to_vec(),
            max_key: b"z".to_vec(),
            min_sequence: 1,
            max_sequence: 3,
            creation_time: 99,
        });
        manifest
            .save_legacy_for_test(temp_dir.path(), MANIFEST_VERSION_V2)
            .unwrap();

        let loaded = Manifest::load_or_create(temp_dir.path()).unwrap();
        assert_eq!(loaded.loaded_format_version, 2);
        assert_eq!(loaded.sstables.len(), 1);
        assert_eq!(loaded.sstables[0].entry_count, 3);
        assert_eq!(loaded.sstables[0].tombstone_count, 0);
    }

    #[test]
    fn atomically_replaces_an_existing_manifest() {
        let temp_dir = TempDir::new().unwrap();
        let mut manifest = Manifest::new();
        manifest.wal_checkpoint = 1;
        manifest.save(temp_dir.path()).unwrap();

        manifest.wal_checkpoint = 2;
        manifest.save(temp_dir.path()).unwrap();

        assert_eq!(
            Manifest::load_or_create(temp_dir.path())
                .unwrap()
                .wal_checkpoint,
            2
        );
    }

    #[test]
    fn seeded_atomic_replacement_and_checksum_model_never_exposes_a_partial_manifest() {
        const SEED: u64 = 0x65a8_f2c1_904d_7be3;

        let directory = TempDir::new().unwrap();
        let manifest_path = directory.path().join("MANIFEST");
        let initial_context = stress_context(SEED, 0, 0, manifest_path.display());
        Manifest::new()
            .save(directory.path())
            .unwrap_or_else(|error| panic!("{initial_context}: initial save failed: {error}"));
        let reading = Arc::new(AtomicBool::new(true));
        let reads = Arc::new(AtomicU64::new(0));
        let failures = Arc::new(Mutex::new(Vec::new()));
        let reader_path = manifest_path.clone();
        let reader_running = Arc::clone(&reading);
        let reader_reads = Arc::clone(&reads);
        let reader_failures = Arc::clone(&failures);
        let reader = std::thread::spawn(move || {
            while reader_running.load(Ordering::Acquire) {
                match Manifest::load(&reader_path) {
                    Ok(_) => {
                        reader_reads.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(error) => {
                        reader_failures.lock().unwrap().push(error.to_string());
                        break;
                    }
                }
                std::thread::yield_now();
            }
        });
        let mut observed_reads = wait_for_manifest_reader(&reads, 0, &failures, &initial_context);

        let mut rng = StdRng::seed_from_u64(SEED);
        for generation in 0..64_u64 {
            let mut min_key = vec![0; rng.gen_range(0..=128)];
            let mut max_key = vec![0; rng.gen_range(0..=128)];
            rng.fill_bytes(&mut min_key);
            rng.fill_bytes(&mut max_key);
            let file_identity = format!("sstables/L0/{generation:010}.sst");
            let entry_count = rng.gen_range(0..=1_024);
            let manifest = Manifest {
                loaded_format_version: u64::from(MANIFEST_VERSION),
                wal_checkpoint: generation.saturating_mul(3),
                sstables: vec![SSTableManifestEntry {
                    id: generation,
                    level: (generation % 4) as u32,
                    path: PathBuf::from(&file_identity),
                    size: rng.next_u64(),
                    entry_count,
                    tombstone_count: rng.gen_range(0..=entry_count),
                    min_key: min_key.clone(),
                    max_key: max_key.clone(),
                    min_sequence: generation.saturating_mul(3),
                    max_sequence: generation.saturating_mul(3).saturating_add(2),
                    creation_time: rng.next_u64(),
                }],
            };
            let context = stress_context(SEED, manifest.wal_checkpoint, generation, &file_identity);
            manifest
                .save(directory.path())
                .unwrap_or_else(|error| panic!("{context}: save failed: {error}"));
            assert!(!directory.path().join("MANIFEST.tmp").exists(), "{context}");
            observed_reads = wait_for_manifest_reader(&reads, observed_reads, &failures, &context);

            let bytes = std::fs::read(&manifest_path)
                .unwrap_or_else(|error| panic!("{context}: manifest read failed: {error}"));
            assert!(
                bytes.len() >= 4,
                "{context}: manifest checksum is truncated"
            );
            let (payload, checksum) = bytes.split_at(bytes.len() - 4);
            assert_eq!(
                u32::from_le_bytes(
                    checksum
                        .try_into()
                        .unwrap_or_else(|_| panic!("{context}: checksum was not four bytes")),
                ),
                crc32_checksum(payload),
                "{context}"
            );
            let loaded = Manifest::load(&manifest_path)
                .unwrap_or_else(|error| panic!("{context}: load failed: {error}"));
            assert_eq!(loaded.wal_checkpoint, manifest.wal_checkpoint, "{context}");
            assert_eq!(loaded.sstables.len(), 1, "{context}");
            let loaded_entry = &loaded.sstables[0];
            assert_eq!(loaded_entry.id, generation, "{context}");
            assert_eq!(loaded_entry.path, PathBuf::from(file_identity), "{context}");
            assert_eq!(loaded_entry.min_key, min_key, "{context}");
            assert_eq!(loaded_entry.max_key, max_key, "{context}");
        }

        reading.store(false, Ordering::Release);
        reader
            .join()
            .unwrap_or_else(|_| panic!("{initial_context}: manifest reader panicked"));
        let failures = failures.lock().unwrap().clone();
        assert!(
            failures.is_empty(),
            "{}: {:?}",
            stress_context(SEED, 63 * 3, "concurrent", manifest_path.display()),
            failures
        );

        let corrupt_path = directory.path().join("MANIFEST.corrupt");
        let corrupt_context = stress_context(SEED, 63 * 3, 63, corrupt_path.display());
        let mut corrupt = std::fs::read(&manifest_path)
            .unwrap_or_else(|error| panic!("{corrupt_context}: source read failed: {error}"));
        let payload_offset = corrupt.len() / 2;
        corrupt[payload_offset] ^= 0x80;
        std::fs::write(&corrupt_path, corrupt)
            .unwrap_or_else(|error| panic!("{corrupt_context}: corrupt write failed: {error}"));
        let error = match Manifest::load(&corrupt_path) {
            Ok(_) => panic!("{corrupt_context}: corrupted manifest loaded successfully"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("Manifest checksum mismatch"),
            "{corrupt_context}: {error}"
        );
    }
}
