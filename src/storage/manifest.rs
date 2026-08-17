//! The manifest tracks:
//! - All SSTables and their metadata
//! - WAL checkpoint (first sequence that recovery must replay)
//! - Database version for compatibility
//!
//! Manifest File Format
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    MANIFEST File                            │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Magic: "HNSHMNFT" (8 bytes)                                │
//! │  Version: u32                                               │
//! │  WAL Checkpoint: u64 (next replay sequence)                 │
//! │  SSTable Count: u32                                         │
//! │  SSTable Entries: [SSTableManifestEntry...]                 │
//! │  Checksum: u32 (CRC32)                                      │
//! └─────────────────────────────────────────────────────────────┘

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::core::crypto::crc32_checksum;
use crate::core::error::{Error, Result};

const MANIFEST_MAGIC: &[u8; 8] = b"HNSHMNFT";
/// Format v3 persists per-SSTable tombstone counts for cheap physical gauges.
/// Readers still accept v1 and v2 so existing databases can be upgraded in
/// place after their tables are inspected once at open.
pub(crate) const MANIFEST_VERSION: u32 = 3;
const MANIFEST_VERSION_WITHOUT_TOMBSTONE_COUNTS: u32 = 2;
const LEGACY_MANIFEST_VERSION: u32 = 1;

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
    pub id: u64,
    pub level: u32,
    pub path: PathBuf,
    pub size: u64,
    pub entry_count: u64,
    pub tombstone_count: u64,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub min_sequence: u64,
    pub max_sequence: u64,
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

    /// Load manifest from disk, or create new if doesn't exist
    pub fn load_or_create(data_dir: &Path) -> Result<Self> {
        let manifest_path = data_dir.join("MANIFEST");

        if manifest_path.exists() {
            Self::load(&manifest_path)
        } else {
            info!("No manifest found, creating new database");
            Ok(Self::new())
        }
    }

    /// Load manifest from file
    pub fn load(path: &Path) -> Result<Self> {
        // Read entire file into memory for checksum verification
        let file = File::open(path).map_err(|e| Error::Io {
            message: format!("Failed to open manifest: {:?}", path),
            source: e,
        })?;
        let mut reader = BufReader::new(file);
        let mut file_data = Vec::new();
        reader.read_to_end(&mut file_data)?;

        if file_data.len() < 4 {
            return Err(Error::Internal {
                message: "Manifest file too small".to_string(),
            });
        }

        // Verify checksum: last 4 bytes are the CRC32, rest is the payload
        let (payload, checksum_bytes) = file_data.split_at(file_data.len() - 4);
        let stored_checksum = u32::from_le_bytes(
            checksum_bytes
                .try_into()
                .expect("checksum slice is exactly 4 bytes"),
        );
        let computed_checksum = crc32_checksum(payload);
        if stored_checksum != computed_checksum {
            return Err(Error::Internal {
                message: format!(
                    "Manifest checksum mismatch: stored={:#010x}, computed={:#010x}",
                    stored_checksum, computed_checksum
                ),
            });
        }

        // Now parse the payload
        let mut reader = std::io::Cursor::new(payload);

        // Read and verify magic
        let mut magic = [0u8; 8];
        reader.read_exact(&mut magic)?;
        if &magic != MANIFEST_MAGIC {
            return Err(Error::Internal {
                message: "Invalid manifest magic number".to_string(),
            });
        }

        // Read version
        let version = reader.read_u32::<LittleEndian>()?;
        if version != MANIFEST_VERSION
            && version != MANIFEST_VERSION_WITHOUT_TOMBSTONE_COUNTS
            && version != LEGACY_MANIFEST_VERSION
        {
            return Err(Error::Internal {
                message: format!("Unsupported manifest version: {}", version),
            });
        }

        // Read WAL checkpoint
        let wal_checkpoint = reader.read_u64::<LittleEndian>()?;

        if version == LEGACY_MANIFEST_VERSION {
            let extension_len = reader.read_u32::<LittleEndian>()? as usize;
            let mut extension = vec![0u8; extension_len];
            reader.read_exact(&mut extension)?;
        }

        // Read SSTable count
        let sstable_count = reader.read_u32::<LittleEndian>()? as usize;

        // Read SSTable entries
        let mut sstables = Vec::with_capacity(sstable_count);
        for _ in 0..sstable_count {
            let entry = Self::read_sstable_entry(&mut reader, version)?;
            sstables.push(entry);
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

    /// Save manifest to disk (atomic write via rename)
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

    /// Update the exclusive durable frontier after a successful flush.
    pub fn update_checkpoint(&mut self, sequence: u64) {
        self.wal_checkpoint = sequence;
    }

    /// Add SSTable to manifest
    pub fn add_sstable(&mut self, entry: SSTableManifestEntry) {
        self.sstables.push(entry);
    }

    /// Remove SSTables (after compaction)
    pub fn remove_sstables(&mut self, ids: &[u64]) {
        self.sstables.retain(|e| !ids.contains(&e.id));
    }

    fn read_sstable_entry(reader: &mut impl Read, version: u32) -> Result<SSTableManifestEntry> {
        let id = reader.read_u64::<LittleEndian>()?;
        let level = reader.read_u32::<LittleEndian>()?;

        // Read path
        let path_len = reader.read_u32::<LittleEndian>()? as usize;
        let mut path_bytes = vec![0u8; path_len];
        reader.read_exact(&mut path_bytes)?;
        let path = PathBuf::from(String::from_utf8_lossy(&path_bytes).to_string());

        let size = reader.read_u64::<LittleEndian>()?;
        let entry_count = reader.read_u64::<LittleEndian>()?;
        let tombstone_count = if version >= MANIFEST_VERSION {
            reader.read_u64::<LittleEndian>()?
        } else {
            0
        };

        // Read min_key
        let min_key_len = reader.read_u32::<LittleEndian>()? as usize;
        let mut min_key = vec![0u8; min_key_len];
        reader.read_exact(&mut min_key)?;

        // Read max_key
        let max_key_len = reader.read_u32::<LittleEndian>()? as usize;
        let mut max_key = vec![0u8; max_key_len];
        reader.read_exact(&mut max_key)?;

        let min_sequence = reader.read_u64::<LittleEndian>()?;
        let max_sequence = reader.read_u64::<LittleEndian>()?;
        let creation_time = reader.read_u64::<LittleEndian>()?;

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
        assert!(
            version == LEGACY_MANIFEST_VERSION
                || version == MANIFEST_VERSION_WITHOUT_TOMBSTONE_COUNTS
        );
        let mut buffer = Vec::new();
        buffer.write_all(MANIFEST_MAGIC)?;
        buffer.write_u32::<LittleEndian>(version)?;
        buffer.write_u64::<LittleEndian>(self.wal_checkpoint)?;
        if version == LEGACY_MANIFEST_VERSION {
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
    use tempfile::TempDir;

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
            .write_u32::<LittleEndian>(LEGACY_MANIFEST_VERSION)
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
            .save_legacy_for_test(temp_dir.path(), MANIFEST_VERSION_WITHOUT_TOMBSTONE_COUNTS)
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
}
