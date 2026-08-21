//! SSTable types and configuration

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub const SSTABLE_MAGIC: &[u8; 8] = b"HANSHIRO";
/// Stable identifier for the released value-only entry layout. Version 1 did
/// not encode tombstones or sequence numbers.
pub const SSTABLE_VERSION_V1: u32 = 1;
/// Stable identifier for entries with an explicit value/tombstone marker.
pub const SSTABLE_VERSION_V2: u32 = 2;
/// Stable identifier for entries with an engine-wide sequence number.
///
/// Readers retain v1/v2 tables in place. New flush and compaction output uses
/// v3, so legacy tables migrate naturally when they are rewritten.
pub const SSTABLE_VERSION: u32 = 3;
pub const DEFAULT_BLOCK_SIZE: usize = 16 * 1024; // 16KB, better for larger security events
pub const FOOTER_SIZE: usize = 40;

#[derive(Debug, Clone)]
pub struct SSTableConfig {
    pub block_size: usize,
    pub compression: super::CompressionType,
    pub bloom_bits_per_key: usize,
}

impl Default for SSTableConfig {
    fn default() -> Self {
        Self {
            block_size: DEFAULT_BLOCK_SIZE,
            compression: super::CompressionType::Lz4,
            bloom_bits_per_key: 10,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableInfo {
    pub id: u64,
    pub path: PathBuf,
    pub file_size: u64,
    pub entry_count: u64,
    /// Number of entries whose value is a tombstone.
    #[serde(default)]
    pub tombstone_count: u64,
    #[serde(with = "serde_bytes")]
    pub min_key: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub max_key: Vec<u8>,
    pub creation_time: u64,
    pub level: u32,
    /// Lowest sequence retained by this table.
    #[serde(default)]
    pub min_sequence: u64,
    /// Highest sequence retained by this table.
    #[serde(default)]
    pub max_sequence: u64,
}
