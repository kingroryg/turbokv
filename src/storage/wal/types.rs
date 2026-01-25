//! WAL types for TurboKV
//!
//! Defines the entry format and configuration for the Write-Ahead Log.

use bytes::Bytes;
use crate::core::crypto::MerkleNode;
use thiserror::Error;

/// Magic bytes for WAL file header
pub const WAL_MAGIC: &[u8; 8] = b"TURBOKV\0";
pub const WAL_VERSION: u32 = 2;
pub const WAL_HEADER_SIZE: usize = 64;
pub const ENTRY_HEADER_SIZE: usize = 32;
pub const MERKLE_INFO_SIZE: usize = 96;

/// WAL-specific error types
#[derive(Debug, Error)]
pub enum WalError {
    #[error("I/O error: {message}")]
    Io {
        message: String,
        #[source]
        source: Option<std::io::Error>,
    },

    #[error("Invalid WAL format: {0}")]
    InvalidFormat(String),

    #[error("CRC mismatch: data corrupted")]
    CrcMismatch,

    #[error("Merkle validation failed: expected {expected}, got {actual}")]
    MerkleValidation { expected: String, actual: String },

    #[error("Channel closed")]
    ChannelClosed,

    #[error("EOF reached")]
    Eof,
}

impl From<std::io::Error> for WalError {
    fn from(e: std::io::Error) -> Self {
        WalError::Io {
            message: e.to_string(),
            source: Some(e),
        }
    }
}

pub type Result<T> = std::result::Result<T, WalError>;

/// Type of WAL entry
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EntryType {
    /// Normal key-value data
    Data = 1,
    /// Checkpoint marker (safe point for recovery)
    Checkpoint = 2,
    /// Truncation marker
    Truncate = 3,
    /// Tombstone (key deletion)
    Delete = 4,
}

impl TryFrom<u8> for EntryType {
    type Error = WalError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(EntryType::Data),
            2 => Ok(EntryType::Checkpoint),
            3 => Ok(EntryType::Truncate),
            4 => Ok(EntryType::Delete),
            _ => Err(WalError::InvalidFormat(format!(
                "Invalid entry type: {}",
                value
            ))),
        }
    }
}

/// A single WAL entry
///
/// The `data` field contains encoded key-value pair:
/// - For Data: `[key_len: u32][key][value]`
/// - For Delete: `[key_len: u32][key]`
#[derive(Debug, Clone)]
pub struct WalEntry {
    /// Sequence number (monotonically increasing)
    pub sequence: u64,
    /// Timestamp in milliseconds since epoch
    pub timestamp: u64,
    /// Type of entry
    pub entry_type: EntryType,
    /// Encoded key-value data
    pub data: Bytes,
    /// Merkle chain node (may be empty if merkle disabled)
    pub merkle: MerkleNode,
}

impl WalEntry {
    /// Decode the key from the entry data
    pub fn decode_key(&self) -> Option<&[u8]> {
        if self.data.len() < 4 {
            return None;
        }
        let key_len = u32::from_le_bytes([
            self.data[0],
            self.data[1],
            self.data[2],
            self.data[3],
        ]) as usize;

        if self.data.len() < 4 + key_len {
            return None;
        }
        Some(&self.data[4..4 + key_len])
    }

    /// Decode the value from the entry data (None for Delete entries)
    pub fn decode_value(&self) -> Option<&[u8]> {
        if self.entry_type == EntryType::Delete {
            return None;
        }
        if self.data.len() < 4 {
            return None;
        }
        let key_len = u32::from_le_bytes([
            self.data[0],
            self.data[1],
            self.data[2],
            self.data[3],
        ]) as usize;

        if self.data.len() < 4 + key_len {
            return None;
        }
        Some(&self.data[4 + key_len..])
    }

    /// Decode both key and value
    pub fn decode_kv(&self) -> Option<(&[u8], Option<&[u8]>)> {
        let key = self.decode_key()?;
        let value = if self.entry_type == EntryType::Delete {
            None
        } else {
            self.decode_value()
        };
        Some((key, value))
    }
}

impl AsRef<WalEntry> for WalEntry {
    fn as_ref(&self) -> &WalEntry {
        self
    }
}

/// WAL configuration
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Maximum file size before rotation (bytes)
    pub max_file_size: u64,
    /// Sync to disk after each write
    pub sync_on_write: bool,
    /// Enable compression (not yet implemented)
    pub compression: bool,
    /// Write buffer size
    pub buffer_size: usize,
    /// Group commit delay in microseconds
    pub group_commit_delay_us: u64,
    /// Maximum batch size for group commit
    pub max_batch_size: usize,
    /// Enable Merkle chain integrity (tamper-proof)
    /// Disable for higher throughput
    pub merkle_enabled: bool,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            max_file_size: 1024 * 1024 * 1024, // 1GB
            sync_on_write: true,
            compression: false,
            buffer_size: 64 * 1024, // 64KB
            group_commit_delay_us: 2000,
            max_batch_size: 512,
            merkle_enabled: false, // Disabled by default for speed
        }
    }
}

impl WalConfig {
    /// Configuration for maximum durability
    pub fn durable() -> Self {
        Self {
            sync_on_write: true,
            merkle_enabled: false,
            ..Default::default()
        }
    }

    /// Configuration for maximum speed (less durability)
    pub fn fast() -> Self {
        Self {
            sync_on_write: false,
            merkle_enabled: false,
            group_commit_delay_us: 500,
            max_batch_size: 1024,
            ..Default::default()
        }
    }

    /// Configuration for tamper-proof storage
    pub fn tamper_proof() -> Self {
        Self {
            sync_on_write: true,
            merkle_enabled: true,
            ..Default::default()
        }
    }
}

/// Encode a key-value pair for WAL storage
#[inline]
pub fn encode_kv(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + key.len() + value.len());
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);
    buf
}

/// Encode a key for deletion
#[inline]
pub fn encode_delete(key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + key.len());
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
    buf
}
