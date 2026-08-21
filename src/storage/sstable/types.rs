//! SSTable types and configuration

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Magic bytes in the current and legacy SSTable footers.
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
/// Default target size of one uncompressed data block.
pub const DEFAULT_BLOCK_SIZE: usize = 16 * 1024;
/// Encoded current-format footer size in bytes.
pub const FOOTER_SIZE: usize = 40;

/// Low-level SSTable encoding configuration.
#[derive(Debug, Clone)]
pub struct SSTableConfig {
    /// Target uncompressed data-block size; one oversized entry is still allowed.
    pub block_size: usize,
    /// Per-block compression codec.
    pub compression: super::CompressionType,
    /// Bloom-filter bit budget per inserted key.
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

/// Metadata for an SSTable file known to an open engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableInfo {
    /// Engine-wide table identifier; direct writers return zero for callers to set.
    pub id: u64,
    /// Filesystem path to the table.
    pub path: PathBuf,
    /// Encoded file length in bytes.
    pub file_size: u64,
    /// Number of physical key versions in the table.
    pub entry_count: u64,
    /// Number of entries whose value is a tombstone.
    #[serde(default)]
    pub tombstone_count: u64,
    /// Smallest raw key, or empty for an empty table.
    #[serde(with = "serde_bytes")]
    pub min_key: Vec<u8>,
    /// Largest raw key, or empty for an empty table.
    #[serde(with = "serde_bytes")]
    pub max_key: Vec<u8>,
    /// Unix timestamp in seconds recorded when writing finished.
    pub creation_time: u64,
    /// LSM level assigned by the caller or engine.
    pub level: u32,
    /// Lowest sequence retained by this table.
    #[serde(default)]
    pub min_sequence: u64,
    /// Highest sequence retained by this table.
    #[serde(default)]
    pub max_sequence: u64,
}
