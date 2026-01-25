//! The manifest tracks:
//! - All SSTables and their metadata
//! - WAL checkpoint (last flushed sequence)
//! - Database version for compatibility
//!
//! Manifest File Format
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    MANIFEST File                            │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Magic: "HNSHMNFT" (8 bytes)                                │
//! │  Version: u32                                               │
//! │  WAL Checkpoint: u64 (last flushed sequence)                │
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

use crate::core::error::{Error, Result};

const MANIFEST_MAGIC: &[u8; 8] = b"HNSHMNFT";
const MANIFEST_VERSION: u32 = 1;

/// Database manifest - tracks persistent state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Manifest version for compatibility
    pub version: u64,
    /// Last WAL sequence that was flushed to SSTable
    /// On recovery, replay WAL entries > this sequence
    pub wal_checkpoint: u64,
    /// Merkle hash at checkpoint - used to verify chain continuity
    pub checkpoint_hash: Option<String>,
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
            version: 0,
            wal_checkpoint: 0,
            checkpoint_hash: None,
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
        let file = File::open(path).map_err(|e| Error::Io {
            message: format!("Failed to open manifest: {:?}", path),
            source: e,
        })?;
        let mut reader = BufReader::new(file);

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
        if version != MANIFEST_VERSION {
            return Err(Error::Internal {
                message: format!("Unsupported manifest version: {}", version),
            });
        }

        // Read WAL checkpoint
        let wal_checkpoint = reader.read_u64::<LittleEndian>()?;
        
        // Read checkpoint hash length and data
        let hash_len = reader.read_u32::<LittleEndian>()? as usize;
        let checkpoint_hash = if hash_len > 0 {
            let mut hash_bytes = vec![0u8; hash_len];
            reader.read_exact(&mut hash_bytes)?;
            Some(String::from_utf8_lossy(&hash_bytes).to_string())
        } else {
            None
        };

        // Read SSTable count
        let sstable_count = reader.read_u32::<LittleEndian>()? as usize;

        // Read SSTable entries
        let mut sstables = Vec::with_capacity(sstable_count);
        for _ in 0..sstable_count {
            let entry = Self::read_sstable_entry(&mut reader)?;
            sstables.push(entry);
        }

        // Read and verify checksum
        let _stored_checksum = reader.read_u32::<LittleEndian>()?;
        // TODO: Verify checksum

        info!(
            "Loaded manifest: version={}, wal_checkpoint={}, sstables={}",
            version, wal_checkpoint, sstables.len()
        );

        Ok(Self {
            version: version as u64,
            wal_checkpoint,
            checkpoint_hash,
            sstables,
        })
    }

    /// Save manifest to disk (atomic write via rename)
    pub fn save(&self, data_dir: &Path) -> Result<()> {
        let manifest_path = data_dir.join("MANIFEST");
        let temp_path = data_dir.join("MANIFEST.tmp");

        // Write to temp file
        {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&temp_path)?;
            let mut writer = BufWriter::new(file);

            // Write magic
            writer.write_all(MANIFEST_MAGIC)?;

            // Write version
            writer.write_u32::<LittleEndian>(MANIFEST_VERSION)?;

            // Write WAL checkpoint
            writer.write_u64::<LittleEndian>(self.wal_checkpoint)?;
            
            // Write checkpoint hash
            if let Some(ref hash) = self.checkpoint_hash {
                let hash_bytes = hash.as_bytes();
                writer.write_u32::<LittleEndian>(hash_bytes.len() as u32)?;
                writer.write_all(hash_bytes)?;
            } else {
                writer.write_u32::<LittleEndian>(0)?;
            }

            // Write SSTable count
            writer.write_u32::<LittleEndian>(self.sstables.len() as u32)?;

            // Write SSTable entries
            for entry in &self.sstables {
                Self::write_sstable_entry(&mut writer, entry)?;
            }

            // Write checksum (placeholder)
            writer.write_u32::<LittleEndian>(0)?;

            writer.flush()?;
        }

        // Atomic rename
        std::fs::rename(&temp_path, &manifest_path)?;

        info!(
            "Saved manifest: wal_checkpoint={}, sstables={}",
            self.wal_checkpoint,
            self.sstables.len()
        );

        Ok(())
    }

    /// Update WAL checkpoint after successful flush
    pub fn update_checkpoint(&mut self, sequence: u64, hash: Option<String>) {
        self.wal_checkpoint = sequence;
        self.checkpoint_hash = hash;
        self.version += 1;
    }

    /// Add SSTable to manifest
    pub fn add_sstable(&mut self, entry: SSTableManifestEntry) {
        self.sstables.push(entry);
        self.version += 1;
    }

    /// Remove SSTables (after compaction)
    pub fn remove_sstables(&mut self, ids: &[u64]) {
        self.sstables.retain(|e| !ids.contains(&e.id));
        self.version += 1;
    }

    fn read_sstable_entry(reader: &mut impl Read) -> Result<SSTableManifestEntry> {
        let id = reader.read_u64::<LittleEndian>()?;
        let level = reader.read_u32::<LittleEndian>()?;
        
        // Read path
        let path_len = reader.read_u32::<LittleEndian>()? as usize;
        let mut path_bytes = vec![0u8; path_len];
        reader.read_exact(&mut path_bytes)?;
        let path = PathBuf::from(String::from_utf8_lossy(&path_bytes).to_string());

        let size = reader.read_u64::<LittleEndian>()?;
        let entry_count = reader.read_u64::<LittleEndian>()?;

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
            min_key,
            max_key,
            min_sequence,
            max_sequence,
            creation_time,
        })
    }

    fn write_sstable_entry(writer: &mut impl Write, entry: &SSTableManifestEntry) -> Result<()> {
        writer.write_u64::<LittleEndian>(entry.id)?;
        writer.write_u32::<LittleEndian>(entry.level)?;

        // Write path
        let path_str = entry.path.to_string_lossy();
        writer.write_u32::<LittleEndian>(path_str.len() as u32)?;
        writer.write_all(path_str.as_bytes())?;

        writer.write_u64::<LittleEndian>(entry.size)?;
        writer.write_u64::<LittleEndian>(entry.entry_count)?;

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
    }

    #[test]
    fn test_manifest_new_database() {
        let temp_dir = TempDir::new().unwrap();
        
        let manifest = Manifest::load_or_create(temp_dir.path()).unwrap();
        
        assert_eq!(manifest.wal_checkpoint, 0);
        assert!(manifest.sstables.is_empty());
    }
}
